//! The always-on process: single-instance lock, control socket, tick loop.
//!
//! Concurrency: one mutex around `State`. Every mutation is saved to disk
//! before the lock is released, so the process can be killed at any moment.
//! Children are spawned and waited on outside the lock, one thread per run.

use crate::job::{Decision, Job, JobSpec, NOTIFY_AFTER_FAILURES, RunRecord, Running, Status, Trigger};
use crate::runner;
use crate::store::State;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
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
}

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

fn log(msg: impl AsRef<str>) {
    eprintln!(
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
        return Ok(0); // exit 0 so launchd KeepAlive does not loop on us
    };
    let mut state = State::load(&paths.state)?;
    recover(&mut state, Utc::now());
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
    loop {
        let wait = tick(&d, Utc::now());
        let guard = d.state.lock().unwrap();
        if SIGNALED.load(Ordering::SeqCst) {
            break;
        }
        let _ = d.wake.wait_timeout(guard, wait).unwrap();
    }
    shutdown(&d);
    let _ = fs::remove_file(&d.paths.socket);
    log("stopped");
    Ok(0)
}

/// Startup recovery: runs that were live when the previous process died are
/// recorded as interrupted. Their payloads are never replayed.
pub fn recover(state: &mut State, now: DateTime<Utc>) {
    let mut records = vec![];
    for job in &mut state.jobs {
        if let Some(r) = job.state.running.take() {
            let alive = r.pid.is_some_and(runner::pid_alive);
            let detail = match r.pid {
                Some(pid) => format!(
                    "scheduler restarted while run was live (pid {pid} {}); not replayed",
                    if alive { "still alive, left untouched" } else { "gone" }
                ),
                None => "scheduler restarted before the run spawned; not replayed".into(),
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
fn tick(d: &Arc<Daemon>, now: DateTime<Utc>) -> Duration {
    let mut starts = vec![];
    let mut st = d.state.lock().unwrap();
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
        save(d, &st);
    }
    let soonest = st
        .jobs
        .iter()
        .filter(|j| j.spec.enabled && j.state.running.is_none())
        .filter_map(|j| j.state.next_run_at)
        .min();
    drop(st);
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

fn spawn_run(d: Arc<Daemon>, spec: JobSpec, r: Running, done: Option<Sender<RunRecord>>) {
    thread::spawn(move || {
        let env = [
            ("BOSS_SCHED_JOB_ID", spec.id.clone()),
            ("BOSS_SCHED_RUN_ID", r.run_id.clone()),
            ("BOSS_SCHED_SLOT", r.slot.map(|s| s.to_rfc3339()).unwrap_or_default()),
        ];
        let mut rec = record(&spec.id, &r, Utc::now(), Status::SpawnError, None);
        match runner::start(&spec, &env) {
            Err(e) => rec.detail = Some(format!("spawn {:?}: {e}", spec.command[0])),
            Ok(started) => {
                let pid = started.pid();
                {
                    let mut st = d.state.lock().unwrap();
                    if let Some(run) = st.job_mut(&spec.id).and_then(|j| j.state.running.as_mut()) {
                        run.pid = Some(pid);
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
                if d.shutting_down.load(Ordering::SeqCst) && o.status != Status::Success {
                    rec.status = Status::Interrupted;
                    rec.detail = Some("scheduler shut down during the run; process group terminated".into());
                }
            }
        }
        rec.finished_at = Utc::now();
        log(format!("job {} run {} finished: {:?}", spec.id, rec.run_id, rec.status));
        let notify = {
            let mut st = d.state.lock().unwrap();
            let n = apply_outcome(&mut st, &rec, Utc::now());
            save(&d, &st);
            n
        };
        d.wake.notify_all();
        if let Some((argv, reason)) = notify {
            run_notify(&spec, argv, &rec, reason);
        }
        if let Some(tx) = done {
            let _ = tx.send(rec);
        }
    });
}

/// Fold a finished run into job state. Returns an `on_failure` command to run, if any.
pub fn apply_outcome(st: &mut State, rec: &RunRecord, now: DateTime<Utc>) -> Option<(Vec<String>, String)> {
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

fn run_notify(spec: &JobSpec, argv: Vec<String>, rec: &RunRecord, reason: String) {
    let mut n = spec.clone();
    n.command = argv;
    let env = [
        ("BOSS_SCHED_JOB_ID", spec.id.clone()),
        ("BOSS_SCHED_JOB_NAME", spec.name.clone()),
        ("BOSS_SCHED_RUN_ID", rec.run_id.clone()),
        ("BOSS_SCHED_STATUS", format!("{:?}", rec.status).to_lowercase()),
        ("BOSS_SCHED_REASON", reason),
    ];
    match runner::start(&n, &env) {
        Ok(s) => {
            let o = runner::wait(s, NOTIFY_TIMEOUT, Duration::from_secs(5));
            log(format!("job {} on_failure finished: {:?}", spec.id, o.status));
        }
        Err(e) => log(format!("job {} on_failure spawn failed: {e}", spec.id)),
    }
}

/// SIGTERM: stop every live run's process group (so updates leave no
/// orphans), record them as interrupted, then exit.
fn shutdown(d: &Arc<Daemon>) {
    d.shutting_down.store(true, Ordering::SeqCst);
    let live = |d: &Daemon| -> Vec<i32> {
        d.state
            .lock()
            .unwrap()
            .jobs
            .iter()
            .filter_map(|j| j.state.running.as_ref().and_then(|r| r.pid))
            .map(|p| p as i32)
            .collect()
    };
    let pids = live(d);
    if pids.is_empty() {
        return;
    }
    log(format!("shutting down; terminating {} live run(s)", pids.len()));
    for p in &pids {
        runner::signal_group(*p, libc::SIGTERM);
    }
    let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
    while !live(d).is_empty() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(100));
    }
    for p in live(d) {
        runner::signal_group(p, libc::SIGKILL);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !live(d).is_empty() && std::time::Instant::now() < deadline {
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
            let before = st.jobs.len();
            st.jobs.retain(|j| j.spec.id != id);
            if st.jobs.len() == before {
                return Err("no such job".into());
            }
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
            let job = st.job_mut(&id_arg(req)?).ok_or("no such job")?;
            if job.state.running.is_some() {
                return Err("job is already running".into());
            }
            let r = Running {
                run_id: new_run_id(),
                pid: None,
                started_at: now,
                trigger: Trigger::Manual,
                slot: None,
                missed_slots: 0,
            };
            job.state.running = Some(r.clone());
            let spec = job.spec.clone();
            save(d, &st);
            drop(st);
            let wait = req["wait"].as_bool().unwrap_or(false);
            let (tx, rx) = mpsc::channel();
            spawn_run(d.clone(), spec, r.clone(), wait.then_some(tx));
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

    fn rec(status: Status) -> RunRecord {
        let r = Running {
            run_id: new_run_id(),
            pid: None,
            started_at: Utc::now(),
            trigger: Trigger::Schedule,
            slot: None,
            missed_slots: 1,
        };
        record("j", &r, Utc::now(), status, None)
    }

    #[test]
    fn failure_counter_notifies_at_two_and_auto_disables_at_five() {
        let mut st = state_with(true);
        let mut notified = vec![];
        for i in 1..=5 {
            if let Some((_, why)) = apply_outcome(&mut st, &rec(Status::Failed), Utc::now()) {
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
            apply_outcome(&mut st, &rec(s), Utc::now());
        }
        assert_eq!(st.jobs[0].state.consecutive_failures, 2);
        apply_outcome(&mut st, &rec(Status::Success), Utc::now());
        assert_eq!(st.jobs[0].state.consecutive_failures, 0);
        assert!(st.jobs[0].spec.enabled);
    }

    #[test]
    fn recover_marks_live_runs_interrupted_without_replay() {
        let mut st = state_with(false);
        let next = Utc::now() + chrono::Duration::hours(1);
        st.jobs[0].state.next_run_at = Some(next);
        st.jobs[0].state.running = Some(Running {
            run_id: "r1".into(),
            pid: Some(999_999),
            started_at: Utc::now(),
            trigger: Trigger::Schedule,
            slot: None,
            missed_slots: 1,
        });
        recover(&mut st, Utc::now());
        assert!(st.jobs[0].state.running.is_none());
        assert_eq!(st.jobs[0].state.next_run_at, Some(next));
        let h = &st.history["j"];
        assert_eq!((h.len(), h[0].status), (1, Status::Interrupted));
        assert!(h[0].detail.as_deref().unwrap().contains("pid 999999 gone"));
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
