//! The always-on process: single-instance lock, control socket, tick loop.
//!
//! Concurrency: one mutex around `State`. Every mutation is saved to disk
//! before the lock is released, so the process can be killed at any moment.
//! Children are spawned and waited on outside the lock, one thread per run.
//! Every run thread is also in `Daemon::live` from before it spawns until its
//! outcome is saved, so shutdown can find (and wait for) all of them. Lock
//! order: `state` before `live`, never the reverse.

use crate::job::{Decision, Job, JobSpec, NOTIFY_AFTER_FAILURES, RunRecord, Running, Status, Trigger};
use crate::runner;
use crate::store::State;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

const MAX_IDLE_WAIT: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const NOTIFY_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Paths {
    pub home: PathBuf,
    pub state: PathBuf,
    pub socket: PathBuf,
    pub lock: PathBuf,
}

impl Paths {
    pub fn new(home: &Path) -> Paths {
        Paths {
            home: home.to_path_buf(),
            state: home.join("state.json"),
            socket: home.join("scheduler.sock"),
            lock: home.join("scheduler.lock"),
        }
    }
}

struct Daemon {
    paths: Paths,
    state: Mutex<State>,
    wake: Condvar,
    shutting_down: AtomicBool,
    started_at: DateTime<Utc>,
    /// run id (or `<run id>:notify`) -> its process group once spawned.
    live: Mutex<HashMap<String, Live>>,
}

#[derive(Default)]
struct Live {
    pgid: Option<i32>,
    /// Set when the scheduler (not the job) ended the run, and why.
    stopped: Option<&'static str>,
}

const STOPPED_SHUTDOWN: &str = "scheduler shut down during the run; process group terminated";
const STOPPED_REMOVED: &str = "job was removed during the run; process group terminated";

static SIGNALED: AtomicBool = AtomicBool::new(false);
static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

/// Take the single-instance lock. `Ok(None)` means another process holds it.
/// The fd is close-on-exec (Rust default), so children never inherit it.
pub fn acquire_lock(path: &Path) -> std::io::Result<Option<File>> {
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(path)?;
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = std::io::Error::last_os_error();
        return if e.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(None)
        } else {
            Err(e)
        };
    }
    f.set_len(0)?;
    writeln!(f, "{}", std::process::id())?;
    Ok(Some(f))
}

/// Never panics (unlike `eprintln!`), so it is safe in `Drop` while unwinding.
fn log(msg: impl AsRef<str>) {
    let _ = writeln!(
        std::io::stderr().lock(),
        "{} boss-sched: {}",
        Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
        msg.as_ref()
    );
}

fn new_run_id() -> String {
    let n = RUN_SEQ.fetch_add(1, Ordering::SeqCst);
    format!("r{}-{}-{n}", Utc::now().format("%Y%m%d%H%M%S"), std::process::id())
}

/// Run the daemon until SIGTERM/SIGINT. Returns the process exit code.
pub fn run(home: &Path) -> Result<i32, String> {
    let paths = Paths::new(home);
    fs::create_dir_all(home).map_err(|e| format!("create {}: {e}", home.display()))?;
    fs::set_permissions(home, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
    let Some(_lock) = acquire_lock(&paths.lock).map_err(|e| format!("lock: {e}"))? else {
        log(format!("another instance holds {}; exiting", paths.lock.display()));
        // Exit 0: the agents restart only on failure, so this does not loop.
        return Ok(0);
    };
    let mut state = State::load(&paths.state)?;
    recover(&mut state, Utc::now(), Duration::from_secs(10));
    state.save(&paths.state)?;

    let _ = fs::remove_file(&paths.socket);
    let listener = UnixListener::bind(&paths.socket).map_err(|e| format!("bind {}: {e}", paths.socket.display()))?;
    fs::set_permissions(&paths.socket, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    // Block TERM/INT in every thread (children get a clean mask from std's
    // spawn) and take them synchronously on one thread, so the main loop can
    // sleep until the next due slot instead of polling for signals.
    let mut sigs: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut sigs);
        libc::sigaddset(&mut sigs, libc::SIGTERM);
        libc::sigaddset(&mut sigs, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &sigs, std::ptr::null_mut());
    }
    let d = Arc::new(Daemon {
        paths,
        state: Mutex::new(state),
        wake: Condvar::new(),
        shutting_down: AtomicBool::new(false),
        started_at: Utc::now(),
        live: Mutex::new(HashMap::new()),
    });
    log(format!("started pid {} home {}", std::process::id(), home.display()));
    let waiter = d.clone();
    thread::spawn(move || {
        let mut sig = 0;
        unsafe { libc::sigwait(&sigs, &mut sig) };
        SIGNALED.store(true, Ordering::SeqCst);
        let _guard = waiter.state.lock().unwrap();
        waiter.wake.notify_all();
    });
    let acceptor = d.clone();
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let d = acceptor.clone();
            thread::spawn(move || serve(&d, conn));
        }
    });
    // The loop holds the state lock except while waiting, so a notify from
    // add/enable/a finished run (all sent under the lock) is never lost.
    let mut st = d.state.lock().unwrap();
    loop {
        if SIGNALED.load(Ordering::SeqCst) {
            break;
        }
        let wait = tick(&d, &mut st, Utc::now());
        if SIGNALED.load(Ordering::SeqCst) {
            break;
        }
        st = d.wake.wait_timeout(st, wait).unwrap().0;
    }
    drop(st);
    shutdown(&d);
    let _ = fs::remove_file(&d.paths.socket);
    log("stopped");
    Ok(0)
}

/// Startup recovery: runs that were live when the previous process died are
/// recorded as interrupted, and a leftover process group is terminated so it
/// cannot outlive its timeout or overlap the next run. A pid is only signalled
/// when its start time matches the one recorded, so a reused pid is never hit.
/// Payloads are never replayed.
pub fn recover(state: &mut State, now: DateTime<Utc>, grace: Duration) {
    let mut records = vec![];
    for job in &mut state.jobs {
        if let Some(r) = job.state.running.take() {
            let ours = r
                .pid
                .filter(|&p| r.pid_start.is_some() && runner::process_start(p) == r.pid_start);
            let detail = match (r.pid, ours) {
                (Some(pid), Some(_)) => {
                    runner::terminate_group(pid as i32, grace.min(Duration::from_secs(job.spec.kill_grace_seconds)));
                    format!(
                        "scheduler restarted while run was live (pid {pid} still alive, process group terminated); not replayed"
                    )
                }
                // Alive (kill(0) succeeds, or EPERM but its start time is
                // readable) yet unverified: reused, or an old state file
                // without `pid_start`. Never signalled.
                (Some(pid), None) if runner::pid_alive(pid) || runner::process_start(pid).is_some() => format!(
                    "scheduler restarted while run was live (pid {pid} no longer ours: reused or unverifiable, left untouched); not replayed"
                ),
                (Some(pid), None) => format!("scheduler restarted while run was live (pid {pid} gone); not replayed"),
                // A crash between spawn and saving the pid lands here; that
                // child cannot be identified, so it is left alone.
                (None, _) => "scheduler restarted before the run recorded its pid; not replayed".into(),
            };
            job.state.last_status = Some(Status::Interrupted);
            records.push(record(&job.spec.id, &r, now, Status::Interrupted, Some(detail)));
        }
        if job.state.next_run_at.is_none() && !job.spec.is_one_shot() {
            job.state.next_run_at = job.spec.next_after(now);
        }
    }
    for r in records {
        state.push_run(r);
    }
}

fn record(job_id: &str, r: &Running, now: DateTime<Utc>, status: Status, detail: Option<String>) -> RunRecord {
    RunRecord {
        run_id: r.run_id.clone(),
        job_id: job_id.into(),
        trigger: r.trigger,
        slot: r.slot,
        missed_slots: r.missed_slots,
        started_at: r.started_at,
        finished_at: now,
        status,
        exit_code: None,
        signal: None,
        detail,
        stdout: String::new(),
        stderr: String::new(),
    }
}

/// Start everything that is due. Returns how long the loop may sleep.
fn tick(d: &Arc<Daemon>, st: &mut State, now: DateTime<Utc>) -> Duration {
    let mut starts = vec![];
    let mut changed = false;
    let mut skipped = vec![];
    for job in st
        .jobs
        .iter_mut()
        .filter(|j| j.spec.enabled && j.state.running.is_none())
    {
        let (decision, next) = crate::job::plan(&job.spec, job.state.next_run_at, now);
        if decision == Decision::NotDue {
            continue;
        }
        job.state.next_run_at = next;
        changed = true;
        match decision {
            Decision::Skip { slot, missed, reason } => {
                let r = Running {
                    run_id: new_run_id(),
                    pid: None,
                    pid_start: None,
                    started_at: now,
                    trigger: Trigger::Schedule,
                    slot: Some(slot),
                    missed_slots: missed,
                };
                log(format!("job {}: {reason}", job.spec.id));
                job.state.last_status = Some(Status::Skipped);
                skipped.push(record(&job.spec.id, &r, now, Status::Skipped, Some(reason)));
            }
            Decision::Run { slot, missed } => {
                let trigger = if missed > 1 {
                    Trigger::CatchUp
                } else {
                    Trigger::Schedule
                };
                let r = Running {
                    run_id: new_run_id(),
                    pid: None,
                    pid_start: None,
                    started_at: now,
                    trigger,
                    slot: Some(slot),
                    missed_slots: missed,
                };
                job.state.running = Some(r.clone());
                starts.push((job.spec.clone(), r));
            }
            Decision::NotDue => unreachable!(),
        }
    }
    for r in skipped {
        st.push_run(r);
    }
    if changed {
        save(d, st);
    }
    let soonest = st
        .jobs
        .iter()
        .filter(|j| j.spec.enabled && j.state.running.is_none())
        .filter_map(|j| j.state.next_run_at)
        .min();
    for (spec, r) in starts {
        spawn_run(d.clone(), spec, r, None);
    }
    match soonest {
        Some(t) => (t - Utc::now()).to_std().unwrap_or(Duration::ZERO).min(MAX_IDLE_WAIT),
        None => MAX_IDLE_WAIT,
    }
}

fn save(d: &Daemon, st: &State) {
    if let Err(e) = st.save(&d.paths.state) {
        log(format!("ERROR {e}"));
    }
}

/// Record a just-spawned process group in `live`. Returns why the scheduler
/// already wants it stopped (shutdown or remove raced the spawn), if so.
fn track_pgid(d: &Daemon, key: &str, pgid: i32) -> Option<&'static str> {
    let mut live = d.live.lock().unwrap();
    let e = live.entry(key.to_string()).or_default();
    e.pgid = Some(pgid);
    e.stopped
}

/// Mark a tracked run as stopped by the scheduler and SIGTERM its group.
/// A run that has not spawned yet sees the mark and never starts.
fn stop_run(d: &Daemon, key: &str, why: &'static str) {
    let pgid = {
        let mut live = d.live.lock().unwrap();
        let Some(e) = live.get_mut(key) else { return };
        if e.stopped.is_some() {
            return;
        }
        e.stopped = Some(why);
        e.pgid
    };
    if let Some(p) = pgid {
        runner::signal_group(p, libc::SIGTERM);
    }
}

/// Removes a `live` entry when dropped, so a run thread that panics still
/// leaves `live` and shutdown does not wait out its full grace for it. Takes
/// only the `live` lock (fine with or without `state` held, per the lock
/// order) and tolerates a poisoned one, so it cannot panic while unwinding.
/// If the run thread is panicking, its process group (if one was spawned) is
/// SIGKILLed after the lock is released, so no group is left untracked.
struct LiveEntry<'a> {
    d: &'a Daemon,
    key: String,
}

impl Drop for LiveEntry<'_> {
    fn drop(&mut self) {
        let removed = self.d.live.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.key);
        if thread::panicking()
            && let Some(pgid) = removed.and_then(|e| e.pgid)
        {
            runner::signal_group(pgid, libc::SIGKILL);
        }
    }
}

const ABANDONED: &str = "scheduler run thread crashed; run abandoned";

/// Records a run as failed if its thread unwinds from a panic before saving
/// an outcome, so the job is not left marked running (never scheduled again,
/// `run-now` refused) until the next daemon restart. Declared before the
/// run's `LiveEntry`, so it drops after it: the `live` lock is released and
/// the group killed before this takes `state`, keeping the lock order. Takes
/// a poisoned `state` lock too, so it cannot panic while unwinding.
struct AbandonGuard<'a> {
    d: &'a Daemon,
    job_id: String,
    run: Running,
}

impl Drop for AbandonGuard<'_> {
    fn drop(&mut self) {
        if !thread::panicking() {
            return;
        }
        let mut st = self.d.state.lock().unwrap_or_else(|e| e.into_inner());
        if abandon_run(&mut st, &self.job_id, &self.run, Utc::now()) {
            save(self.d, &st);
        }
        self.d.wake.notify_all();
    }
}

/// Record `run` of `job_id` as failed with `ABANDONED` if it is still the
/// job's current run. A removed job, or one re-added and running under a new
/// run id, is left alone. Returns whether anything changed. Any `on_failure`
/// notify is dropped: a crashed run thread cannot run it.
fn abandon_run(st: &mut State, job_id: &str, run: &Running, now: DateTime<Utc>) -> bool {
    let current = st
        .job(job_id)
        .and_then(|j| j.state.running.as_ref())
        .is_some_and(|r| r.run_id == run.run_id);
    if !current {
        return false;
    }
    log(format!("job {job_id} run {}: {ABANDONED}", run.run_id));
    let rec = record(job_id, run, now, Status::Failed, Some(ABANDONED.into()));
    let _ = apply_outcome(st, &rec, now);
    true
}

/// Call with the state lock held (or before the run is visible to anyone),
/// so `live` is filled in before shutdown could look for it.
fn spawn_run(d: Arc<Daemon>, spec: JobSpec, r: Running, done: Option<Sender<RunRecord>>) {
    d.live.lock().unwrap().insert(r.run_id.clone(), Live::default());
    thread::spawn(move || {
        let _abandon = AbandonGuard {
            d: &d,
            job_id: spec.id.clone(),
            run: r.clone(),
        };
        let run_entry = LiveEntry {
            d: &d,
            key: r.run_id.clone(),
        };
        let env = [
            ("BOSS_SCHED_JOB_ID", spec.id.clone()),
            ("BOSS_SCHED_RUN_ID", r.run_id.clone()),
            ("BOSS_SCHED_SLOT", r.slot.map(|s| s.to_rfc3339()).unwrap_or_default()),
        ];
        let mut rec = record(&spec.id, &r, Utc::now(), Status::SpawnError, None);
        let stopped_early = d.live.lock().unwrap().get(&r.run_id).and_then(|e| e.stopped);
        if d.shutting_down.load(Ordering::SeqCst) || stopped_early.is_some() {
            rec.status = Status::Interrupted;
            rec.detail = Some(if stopped_early == Some(STOPPED_REMOVED) {
                "job was removed before the run started".into()
            } else {
                "scheduler was shutting down; run not started".into()
            });
        } else {
            match runner::start(&spec, &env) {
                Err(e) => rec.detail = Some(format!("spawn {:?}: {e}", spec.command[0])),
                Ok(started) => {
                    let pid = started.pid();
                    let pid_start = runner::process_start(pid);
                    if track_pgid(&d, &r.run_id, pid as i32).is_some() {
                        runner::signal_group(pid as i32, libc::SIGTERM);
                    }
                    {
                        let mut st = d.state.lock().unwrap();
                        if let Some(run) = st
                            .job_mut(&spec.id)
                            .and_then(|j| j.state.running.as_mut())
                            .filter(|run| run.run_id == r.run_id)
                        {
                            run.pid = Some(pid);
                            run.pid_start = pid_start;
                        }
                        save(&d, &st);
                    }
                    log(format!(
                        "job {} run {} started pid {pid} ({:?})",
                        spec.id, r.run_id, r.trigger
                    ));
                    let o = runner::wait(
                        started,
                        Duration::from_secs(spec.timeout_seconds),
                        Duration::from_secs(spec.kill_grace_seconds),
                    );
                    rec.status = o.status;
                    rec.exit_code = o.exit_code;
                    rec.signal = o.signal;
                    rec.detail = o.detail;
                    rec.stdout = o.stdout;
                    rec.stderr = o.stderr;
                    // Only a run the scheduler ended counts as interrupted; one
                    // that failed or timed out on its own keeps its real status.
                    let stopped = d.live.lock().unwrap().get(&r.run_id).and_then(|e| e.stopped);
                    if let Some(why) = stopped
                        && o.status != Status::Success
                    {
                        rec.status = Status::Interrupted;
                        rec.detail = Some(why.into());
                    }
                }
            }
        }
        rec.finished_at = Utc::now();
        log(format!("job {} run {} finished: {:?}", spec.id, rec.run_id, rec.status));
        let notify = {
            let mut st = d.state.lock().unwrap();
            let n = apply_outcome(&mut st, &rec, Utc::now());
            save(&d, &st);
            // Under the lock: the main loop is either waiting (and wakes) or
            // has not yet computed its next sleep (and sees this run gone).
            d.wake.notify_all();
            n
        };
        // Register the notifier before this run leaves `live`, so shutdown
        // never sees a gap with neither tracked.
        let notify_key = format!("{}:notify", rec.run_id);
        let notify = notify.filter(|_| !d.shutting_down.load(Ordering::SeqCst));
        let notify_entry = notify.is_some().then(|| {
            d.live.lock().unwrap().insert(notify_key.clone(), Live::default());
            LiveEntry {
                d: &d,
                key: notify_key.clone(),
            }
        });
        drop(run_entry);
        if let Some((argv, reason)) = notify {
            run_notify(&d, &notify_key, &spec, argv, &rec, reason);
        }
        drop(notify_entry);
        if let Some(tx) = done {
            let _ = tx.send(rec);
        }
    });
}

/// Fold a finished run into job state. Returns an `on_failure` command to run, if any.
/// A run is matched by run id, not job id: if its job was removed (and maybe
/// re-added under the same id) the result is dropped rather than applied to
/// a job it does not belong to.
pub fn apply_outcome(st: &mut State, rec: &RunRecord, now: DateTime<Utc>) -> Option<(Vec<String>, String)> {
    let current = st
        .job(&rec.job_id)
        .and_then(|j| j.state.running.as_ref())
        .is_some_and(|r| r.run_id == rec.run_id);
    if !current {
        log(format!(
            "job {} run {}: job was removed; result not recorded",
            rec.job_id, rec.run_id
        ));
        return None;
    }
    st.push_run(rec.clone());
    let job = st.job_mut(&rec.job_id)?;
    job.state.running = None;
    job.state.last_status = Some(rec.status);
    job.state.last_finished_at = Some(rec.finished_at);
    let mut notify = None;
    if rec.status == Status::Success {
        job.state.consecutive_failures = 0;
    } else if rec.status.is_failure() {
        job.state.consecutive_failures += 1;
        let n = job.state.consecutive_failures;
        if n >= job.spec.max_consecutive_failures {
            job.spec.enabled = false;
            let why = format!(
                "auto-disabled after {n} consecutive failures (last: {:?}{})",
                rec.status,
                rec.exit_code.map(|c| format!(" exit {c}")).unwrap_or_default()
            );
            log(format!("job {}: {why}", job.spec.id));
            job.state.disabled_reason = Some(why.clone());
            notify = job.spec.on_failure.clone().map(|a| (a, why));
        } else if n == NOTIFY_AFTER_FAILURES {
            notify = job
                .spec
                .on_failure
                .clone()
                .map(|a| (a, format!("{n} consecutive failures")));
        }
    }
    // Slots that passed while this run was live are dropped, not queued.
    if job.state.next_run_at.is_some_and(|t| t <= now) && !job.spec.is_one_shot() {
        job.state.next_run_at = job.spec.next_after(now);
    }
    if job.spec.is_one_shot()
        && rec.trigger != Trigger::Manual
        && rec.status == Status::Success
        && job.spec.delete_after_success
    {
        let id = rec.job_id.clone();
        st.jobs.retain(|j| j.spec.id != id);
    }
    notify
}

fn run_notify(d: &Daemon, key: &str, spec: &JobSpec, argv: Vec<String>, rec: &RunRecord, reason: String) {
    let mut n = spec.clone();
    n.command = argv;
    let env = [
        ("BOSS_SCHED_JOB_ID", spec.id.clone()),
        ("BOSS_SCHED_JOB_NAME", spec.name.clone()),
        ("BOSS_SCHED_RUN_ID", rec.run_id.clone()),
        ("BOSS_SCHED_STATUS", format!("{:?}", rec.status).to_lowercase()),
        ("BOSS_SCHED_REASON", reason),
    ];
    if d.live.lock().unwrap().get(key).and_then(|e| e.stopped).is_some() {
        return;
    }
    match runner::start(&n, &env) {
        Ok(s) => {
            if track_pgid(d, key, s.pid() as i32).is_some() {
                runner::signal_group(s.pid() as i32, libc::SIGTERM);
            }
            let o = runner::wait(s, NOTIFY_TIMEOUT, Duration::from_secs(5));
            log(format!("job {} on_failure finished: {:?}", spec.id, o.status));
        }
        Err(e) => log(format!("job {} on_failure spawn failed: {e}", spec.id)),
    }
}

/// SIGTERM: stop every tracked run and notifier (including ones still
/// spawning, which then never start), wait for their outcomes to be saved as
/// interrupted, then exit. Updates leave no orphans.
fn shutdown(d: &Arc<Daemon>) {
    {
        // Under the state lock: a request handler either registered its run
        // in `live` before this point, or sees the flag and refuses.
        let _st = d.state.lock().unwrap();
        d.shutting_down.store(true, Ordering::SeqCst);
    }
    let keys = |d: &Daemon| -> Vec<String> { d.live.lock().unwrap().keys().cloned().collect() };
    let n = keys(d).len();
    if n == 0 {
        return;
    }
    log(format!("shutting down; terminating {n} live run(s)"));
    // Re-sweep while waiting: a run mid-spawn gets its pgid after the first pass.
    let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
    while !keys(d).is_empty() && std::time::Instant::now() < deadline {
        for k in keys(d) {
            stop_run(d, &k, STOPPED_SHUTDOWN);
        }
        thread::sleep(Duration::from_millis(100));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !keys(d).is_empty() && std::time::Instant::now() < deadline {
        let pgids: Vec<i32> = d.live.lock().unwrap().values().filter_map(|e| e.pgid).collect();
        for p in pgids {
            runner::signal_group(p, libc::SIGKILL);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn serve(d: &Arc<Daemon>, conn: UnixStream) {
    let Ok(read_half) = conn.try_clone() else { return };
    let mut out = conn;
    for line in BufReader::new(read_half).lines() {
        let Ok(line) = line else { return };
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Value>(&line) {
            Ok(req) => match handle(d, &req) {
                Ok(v) => json!({"ok": true, "result": v}),
                Err(e) => json!({"ok": false, "error": e}),
            },
            Err(e) => json!({"ok": false, "error": format!("bad request: {e}")}),
        };
        if writeln!(out, "{resp}").is_err() {
            return;
        }
    }
}

fn id_arg(req: &Value) -> Result<String, String> {
    req["id"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| "missing \"id\"".into())
}

fn handle(d: &Arc<Daemon>, req: &Value) -> Result<Value, String> {
    let cmd = req["cmd"].as_str().ok_or("missing \"cmd\"")?;
    let now = Utc::now();
    let mut st = d.state.lock().unwrap();
    let result = match cmd {
        "status" => json!({
            "pid": std::process::id(), "version": env!("CARGO_PKG_VERSION"),
            "home": d.paths.home, "started_at": d.started_at, "jobs": st.jobs.len(),
            "running": st.jobs.iter().filter(|j| j.state.running.is_some()).count(),
        }),
        "list" => to_v(&st.jobs),
        "get" => to_v(st.job(&id_arg(req)?).ok_or("no such job")?),
        "history" => {
            let id = id_arg(req)?;
            let limit = req["limit"].as_u64().unwrap_or(20) as usize;
            let runs = st.history.get(&id).map(|v| v.as_slice()).unwrap_or(&[]);
            to_v(&runs[runs.len().saturating_sub(limit)..].to_vec())
        }
        "add" => {
            let mut spec: JobSpec = serde_json::from_value(req["job"].clone()).map_err(|e| format!("bad job: {e}"))?;
            if spec.id.is_empty() {
                spec.id = unique_id(&st, &spec.name);
            }
            spec.validate()?;
            if st.job(&spec.id).is_some() {
                return Err(format!("job '{}' already exists", spec.id));
            }
            let mut job = Job {
                state: Default::default(),
                spec,
            };
            job.state.next_run_at = job.spec.initial_next(now);
            let v = to_v(&job);
            st.jobs.push(job);
            v
        }
        "remove" => {
            let id = id_arg(req)?;
            let job = st.job(&id).ok_or("no such job")?;
            // A live run goes with its job: SIGTERM now, SIGKILL after the
            // job's grace if it is still tracked. Shutdown also still sees it.
            if let Some(r) = &job.state.running {
                let (key, grace) = (r.run_id.clone(), Duration::from_secs(job.spec.kill_grace_seconds));
                stop_run(d, &key, STOPPED_REMOVED);
                let d2 = d.clone();
                thread::spawn(move || {
                    thread::sleep(grace);
                    let pgid = d2.live.lock().unwrap().get(&key).and_then(|e| e.pgid);
                    if let Some(p) = pgid {
                        runner::signal_group(p, libc::SIGKILL);
                    }
                });
            }
            st.jobs.retain(|j| j.spec.id != id);
            st.history.remove(&id);
            json!({"removed": id})
        }
        "enable" | "disable" => {
            let job = st.job_mut(&id_arg(req)?).ok_or("no such job")?;
            job.spec.enabled = cmd == "enable";
            if job.spec.enabled {
                // Re-enabling resets the failure streak and does not replay disabled-time slots.
                job.state.consecutive_failures = 0;
                job.state.disabled_reason = None;
                if !job.spec.is_one_shot() {
                    job.state.next_run_at = job.spec.next_after(now);
                }
            }
            to_v(&*job)
        }
        "run_now" => {
            if d.shutting_down.load(Ordering::SeqCst) || SIGNALED.load(Ordering::SeqCst) {
                return Err("scheduler is shutting down".into());
            }
            let job = st.job_mut(&id_arg(req)?).ok_or("no such job")?;
            if job.state.running.is_some() {
                return Err("job is already running".into());
            }
            let r = Running {
                run_id: new_run_id(),
                pid: None,
                pid_start: None,
                started_at: now,
                trigger: Trigger::Manual,
                slot: None,
                missed_slots: 0,
            };
            job.state.running = Some(r.clone());
            let spec = job.spec.clone();
            save(d, &st);
            let wait = req["wait"].as_bool().unwrap_or(false);
            let (tx, rx) = mpsc::channel();
            spawn_run(d.clone(), spec, r.clone(), wait.then_some(tx));
            drop(st);
            return if wait {
                rx.recv()
                    .map(|rec| to_v(&rec))
                    .map_err(|_| "run thread vanished".into())
            } else {
                Ok(json!({"run_id": r.run_id}))
            };
        }
        other => return Err(format!("unknown cmd '{other}'")),
    };
    if matches!(cmd, "add" | "remove" | "enable" | "disable") {
        save(d, &st);
        d.wake.notify_all();
    }
    Ok(result)
}

fn unique_id(st: &State, name: &str) -> String {
    let mut base: String = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    base = base.split('-').filter(|s| !s.is_empty()).collect::<Vec<_>>().join("-");
    if base.is_empty() {
        base = "job".into();
    }
    let mut id = base.clone();
    let mut n = 2;
    while st.job(&id).is_some() || st.history.contains_key(&id) {
        id = format!("{base}-{n}");
        n += 1;
    }
    id
}

fn to_v<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(on_failure: bool) -> State {
        let mut st = State {
            version: 1,
            ..Default::default()
        };
        let mut spec = json!({"id": "j", "name": "j", "command": ["false"],
                              "schedule": {"kind": "cron", "expr": "0 * * * *", "tz": "UTC"}});
        if on_failure {
            spec["on_failure"] = json!(["notify"]);
        }
        st.jobs.push(Job {
            spec: serde_json::from_value(spec).unwrap(),
            state: Default::default(),
        });
        st
    }

    fn running() -> Running {
        Running {
            run_id: new_run_id(),
            pid: None,
            pid_start: None,
            started_at: Utc::now(),
            trigger: Trigger::Schedule,
            slot: None,
            missed_slots: 1,
        }
    }

    /// Start a run of job "j" and return the record it finishes with.
    fn rec(st: &mut State, status: Status) -> RunRecord {
        let r = running();
        st.jobs[0].state.running = Some(r.clone());
        record("j", &r, Utc::now(), status, None)
    }

    #[test]
    fn stale_run_does_not_touch_a_re_added_job() {
        let mut st = state_with(false);
        let old = rec(&mut st, Status::Failed);
        // Removed and re-added under the same id; the new job is running.
        st.jobs = state_with(false).jobs;
        let new = running();
        st.jobs[0].state.running = Some(new.clone());
        assert!(apply_outcome(&mut st, &old, Utc::now()).is_none());
        let j = &st.jobs[0].state;
        assert_eq!(j.running.as_ref().map(|r| r.run_id.as_str()), Some(new.run_id.as_str()));
        assert_eq!((j.consecutive_failures, j.last_status), (0, None));
        assert!(st.history.is_empty(), "removed job's history must not come back");
        // A run whose job is gone entirely is dropped too.
        st.jobs.clear();
        assert!(apply_outcome(&mut st, &old, Utc::now()).is_none());
        assert!(st.history.is_empty());
    }

    #[test]
    fn abandoned_run_is_recorded_failed_and_frees_the_job() {
        let mut st = state_with(true);
        let r = running();
        st.jobs[0].state.running = Some(r.clone());
        assert!(abandon_run(&mut st, "j", &r, Utc::now()));
        let j = &st.jobs[0].state;
        assert!(j.running.is_none(), "job must not stay running");
        assert_eq!((j.last_status, j.consecutive_failures), (Some(Status::Failed), 1));
        let h = &st.history["j"];
        assert_eq!((h.len(), h[0].status), (1, Status::Failed));
        assert_eq!(h[0].run_id, r.run_id);
        assert_eq!(h[0].detail.as_deref(), Some(ABANDONED));
        // A second cleanup of the same run is a no-op.
        assert!(!abandon_run(&mut st, "j", &r, Utc::now()));
        assert_eq!(st.history["j"].len(), 1);
        // Removed and re-added, running under a new run id: left untouched.
        st.jobs = state_with(true).jobs;
        st.history.clear();
        let new = running();
        st.jobs[0].state.running = Some(new.clone());
        assert!(!abandon_run(&mut st, "j", &r, Utc::now()));
        let j = &st.jobs[0].state;
        assert_eq!(j.running.as_ref().map(|r| r.run_id.as_str()), Some(new.run_id.as_str()));
        assert_eq!((j.consecutive_failures, j.last_status), (0, None));
        assert!(st.history.is_empty());
    }

    #[test]
    fn panicking_run_thread_does_not_leave_the_job_running() {
        // Drive the real guard: a thread that panics with it armed, then a
        // poisoned state lock that the guard must still take.
        let dir = std::env::temp_dir().join(format!("bsched-abandon-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let mut st = state_with(false);
        let r = running();
        st.jobs[0].state.running = Some(r.clone());
        let d = Arc::new(Daemon {
            paths: Paths::new(&dir),
            state: Mutex::new(st),
            wake: Condvar::new(),
            shutting_down: AtomicBool::new(false),
            started_at: Utc::now(),
            live: Mutex::new(HashMap::new()),
        });
        let d2 = d.clone();
        let (job_id, run) = ("j".to_string(), r.clone());
        let res = thread::spawn(move || {
            let _abandon = AbandonGuard { d: &d2, job_id, run };
            // Panic while holding the state lock, so it is poisoned too.
            let _st = d2.state.lock().unwrap();
            panic!("simulated run thread crash");
        })
        .join();
        assert!(res.is_err());
        assert!(d.state.is_poisoned());
        let st = d.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(st.jobs[0].state.running.is_none());
        assert_eq!(st.history["j"][0].detail.as_deref(), Some(ABANDONED));
        assert!(
            State::load(&d.paths.state).unwrap().jobs[0].state.running.is_none(),
            "saved"
        );
        drop(st);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn failure_counter_notifies_at_two_and_auto_disables_at_five() {
        let mut st = state_with(true);
        let mut notified = vec![];
        for i in 1..=5 {
            if let Some((_, why)) = {
                let r = rec(&mut st, Status::Failed);
                apply_outcome(&mut st, &r, Utc::now())
            } {
                notified.push((i, why));
            }
            assert_eq!(st.jobs[0].spec.enabled, i < 5, "after failure {i}");
        }
        assert_eq!(notified.len(), 2);
        assert_eq!(notified[0].0, 2);
        assert_eq!(notified[1].0, 5);
        assert!(
            st.jobs[0]
                .state
                .disabled_reason
                .as_deref()
                .unwrap()
                .contains("auto-disabled after 5")
        );
        assert_eq!(st.history["j"].len(), 5);
    }

    #[test]
    fn success_resets_counter_and_skips_do_not_count() {
        let mut st = state_with(false);
        for s in [Status::Failed, Status::Timeout, Status::Skipped, Status::Interrupted] {
            let r = rec(&mut st, s);
            apply_outcome(&mut st, &r, Utc::now());
        }
        assert_eq!(st.jobs[0].state.consecutive_failures, 2);
        let r = rec(&mut st, Status::Success);
        apply_outcome(&mut st, &r, Utc::now());
        assert_eq!(st.jobs[0].state.consecutive_failures, 0);
        assert!(st.jobs[0].spec.enabled);
    }

    #[test]
    fn recover_marks_live_runs_interrupted_without_replay() {
        let mut st = state_with(false);
        let next = Utc::now() + chrono::Duration::hours(1);
        st.jobs[0].state.next_run_at = Some(next);
        st.jobs[0].state.running = Some(Running {
            pid: Some(999_999),
            ..running()
        });
        recover(&mut st, Utc::now(), Duration::from_secs(1));
        assert!(st.jobs[0].state.running.is_none());
        assert_eq!(st.jobs[0].state.next_run_at, Some(next));
        let h = &st.history["j"];
        assert_eq!((h.len(), h[0].status), (1, Status::Interrupted));
        assert!(h[0].detail.as_deref().unwrap().contains("pid 999999 gone"));
    }

    #[test]
    fn recover_reports_dead_pid_without_start_time_as_gone() {
        // A reaped child's pid is dead; with no recorded start time it is
        // neither verifiable nor alive, so it must read as gone.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        let mut st = state_with(false);
        st.jobs[0].state.running = Some(Running {
            pid: Some(pid),
            pid_start: None,
            ..running()
        });
        recover(&mut st, Utc::now(), Duration::from_secs(1));
        let d = st.history["j"][0].detail.clone().unwrap();
        assert!(d.contains(&format!("pid {pid} gone")), "{d}");
    }

    #[test]
    fn recover_never_signals_a_reused_pid() {
        // A live pid (this test process) whose start time does not match the
        // recorded one is someone else's process; signalling it would kill us.
        let mut st = state_with(false);
        let me = std::process::id();
        st.jobs[0].state.running = Some(Running {
            pid: Some(me),
            pid_start: runner::process_start(me).map(|s| s + 1),
            ..running()
        });
        recover(&mut st, Utc::now(), Duration::from_secs(1));
        let d = st.history["j"][0].detail.clone().unwrap();
        assert!(d.contains(&format!("pid {me} no longer ours")), "{d}");
        assert!(!d.contains("gone"), "{d}");
        // An old state file with no recorded start time: alive but unverifiable.
        st.jobs[0].state.running = Some(Running {
            pid: Some(me),
            pid_start: None,
            ..running()
        });
        recover(&mut st, Utc::now(), Duration::from_secs(1));
        let d = st.history["j"][1].detail.clone().unwrap();
        assert!(
            d.contains(&format!(
                "pid {me} no longer ours: reused or unverifiable, left untouched"
            )),
            "{d}"
        );
    }

    #[test]
    fn single_instance_lock() {
        let dir = std::env::temp_dir().join(format!("bsched-lock-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("scheduler.lock");
        let held = acquire_lock(&p).unwrap();
        assert!(held.is_some());
        assert!(acquire_lock(&p).unwrap().is_none(), "second open must not get the lock");
        drop(held);
        // Other tests fork concurrently; a forked child shares the lock's open
        // file description until its exec closes it (CLOEXEC), so allow a moment.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut again = acquire_lock(&p).unwrap();
        while again.is_none() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
            again = acquire_lock(&p).unwrap();
        }
        assert!(again.is_some(), "lock is released with the fd");
        fs::remove_dir_all(&dir).unwrap();
    }
}
