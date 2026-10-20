//! End-to-end: real daemon process on a temp home, driven over its socket.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_boss-sched");

fn temp_home(tag: &str) -> PathBuf {
    // Short /tmp path: unix socket paths are limited to ~104 bytes on macOS.
    let p = PathBuf::from(format!("/tmp/bsched-it-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// Owns a daemon process; kills it on drop so a failed assertion cannot leave
/// an orphan holding the test harness's stdout open.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(home: &Path) -> Daemon {
    std::fs::create_dir_all(home).unwrap();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.join("daemon.log"))
        .unwrap();
    let c = Command::new(BIN)
        .args(["--home", home.to_str().unwrap(), "daemon"])
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    let d = Daemon(c);
    wait_for(|| UnixStream::connect(home.join("scheduler.sock")).is_ok(), 5, "socket");
    d
}

fn wait_for(mut f: impl FnMut() -> bool, secs: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn req(home: &Path, r: Value) -> Value {
    let mut s = UnixStream::connect(home.join("scheduler.sock")).unwrap();
    writeln!(s, "{r}").unwrap();
    let mut line = String::new();
    BufReader::new(&s).read_line(&mut line).unwrap();
    let v: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["ok"], true, "request {r} failed: {v}");
    v["result"].clone()
}

fn history(home: &Path, id: &str) -> Vec<Value> {
    req(home, json!({"cmd": "history", "id": id}))
        .as_array()
        .unwrap()
        .clone()
}

fn pid_alive(pid: i64) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn stop(mut d: Daemon) {
    unsafe { libc::kill(d.0.id() as i32, libc::SIGTERM) };
    let _ = d.0.wait();
}

#[test]
fn add_run_history_over_socket() {
    let home = temp_home("basic");
    let d = start(&home);
    let sock_mode = std::fs::metadata(home.join("scheduler.sock"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(sock_mode, 0o600);

    // A second daemon loses the lock and exits 0 promptly.
    let st = Command::new(BIN)
        .args(["--home", home.to_str().unwrap(), "daemon"])
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(st.success());

    let job = req(
        &home,
        json!({"cmd": "add", "job": {
        "name": "Hello Job", "command": ["/bin/sh", "-c", "echo hi from $BOSS_SCHED_JOB_ID; echo $GREETING >&2"],
        "env": {"GREETING": "yo"}, "cwd": "/tmp",
        "schedule": {"kind": "cron", "expr": "0 0 1 1 *", "tz": "America/Los_Angeles"}}}),
    );
    assert_eq!(job["spec"]["id"], "hello-job");
    let rec = req(&home, json!({"cmd": "run_now", "id": "hello-job", "wait": true}));
    assert_eq!(rec["status"], "success");
    let h = history(&home, "hello-job");
    assert_eq!(h.len(), 1);
    assert_eq!(h[0]["stdout"], "hi from hello-job\n");
    assert_eq!(h[0]["stderr"], "yo\n");
    assert_eq!(h[0]["trigger"], "manual");

    // A scheduled one-shot fires on its own and deletes itself on success.
    let at = (chrono::Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    req(
        &home,
        json!({"cmd": "add", "job": {"id": "once", "name": "once", "command": ["/usr/bin/true"],
        "delete_after_success": true, "schedule": {"kind": "at", "at": at}}}),
    );
    wait_for(|| !history(&home, "once").is_empty(), 10, "one-shot run");
    let h = history(&home, "once");
    assert_eq!(
        (h[0]["status"].as_str(), h[0]["trigger"].as_str()),
        (Some("success"), Some("schedule"))
    );
    wait_for(
        || req(&home, json!({"cmd": "list"})).as_array().unwrap().len() == 1,
        5,
        "one-shot deletion",
    );

    // The CLI talks to the same socket.
    let out = Command::new(BIN)
        .args(["--home", home.to_str().unwrap(), "history", "hello-job"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("success"));
    stop(d);
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn downtime_coalesces_into_one_catch_up_run() {
    let home = temp_home("catchup");
    let d = start(&home);
    req(
        &home,
        json!({"cmd": "add", "job": {"id": "every-min", "name": "m", "command": ["/usr/bin/true"],
        "max_lateness_seconds": 3600,
        "schedule": {"kind": "cron", "expr": "* * * * *", "tz": "UTC"}}}),
    );
    stop(d);
    // Pretend we were down for ~10 minutes.
    let path = home.join("state.json");
    let mut st: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let past = chrono::Utc::now() - chrono::Duration::minutes(10);
    st["jobs"][0]["state"]["next_run_at"] = json!(past.format("%Y-%m-%dT%H:%M:00Z").to_string());
    std::fs::write(&path, serde_json::to_vec(&st).unwrap()).unwrap();

    let d = start(&home);
    wait_for(|| !history(&home, "every-min").is_empty(), 5, "catch-up run");
    std::thread::sleep(Duration::from_millis(1500));
    let h = history(&home, "every-min");
    assert_eq!(h[0]["trigger"], "catch_up");
    assert!(h[0]["missed_slots"].as_u64().unwrap() >= 10, "{}", h[0]);
    // One catch-up only. A minute boundary during the wait can add one
    // ordinary on-time run, which is correct, not a storm.
    assert!(
        h[1..]
            .iter()
            .all(|r| r["trigger"] == "schedule" && r["missed_slots"] == 1)
            && h.len() <= 2,
        "expected one catch-up run, got {h:?}"
    );
    stop(d);
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn restart_records_interrupted_and_sigterm_leaves_no_orphans() {
    let home = temp_home("restart");
    let d = start(&home);
    req(
        &home,
        json!({"cmd": "add", "job": {"id": "slow", "name": "slow",
        "command": ["/bin/sh", "-c", "sleep 30 & echo $! > child.pid; wait"], "cwd": home,
        "schedule": {"kind": "cron", "expr": "0 0 1 1 *", "tz": "UTC"}}}),
    );
    let pidfile = home.join("child.pid");
    let grandchild = |p: &Path| -> i64 { std::fs::read_to_string(p).unwrap().trim().parse().unwrap() };

    // Graceful stop (what `launchctl kickstart -k` sends): the run's group is killed.
    req(&home, json!({"cmd": "run_now", "id": "slow"}));
    wait_for(
        || std::fs::read_to_string(&pidfile).is_ok_and(|s| s.ends_with('\n')),
        5,
        "child pid",
    );
    let g1 = grandchild(&pidfile);
    let t = Instant::now();
    stop(d);
    assert!(
        t.elapsed() < Duration::from_secs(5),
        "SIGTERM to children should suffice, took {:?}\n{}",
        t.elapsed(),
        std::fs::read_to_string(home.join("daemon.log")).unwrap()
    );
    wait_for(|| !pid_alive(g1), 3, "grandchild death after daemon SIGTERM");
    let d = start(&home);
    let h = history(&home, "slow");
    assert_eq!(h.len(), 1);
    assert_eq!(h[0]["status"], "interrupted");

    // Hard kill: the run survives the daemon, and the next process finds it,
    // terminates its group and records it, with no replay.
    std::fs::remove_file(&pidfile).unwrap();
    req(&home, json!({"cmd": "run_now", "id": "slow"}));
    wait_for(
        || std::fs::read_to_string(&pidfile).is_ok_and(|s| s.ends_with('\n')),
        5,
        "child pid",
    );
    let g2 = grandchild(&pidfile);
    let pgid = req(&home, json!({"cmd": "get", "id": "slow"}))["state"]["running"]["pid"]
        .as_i64()
        .unwrap();
    let mut d = d;
    d.0.kill().unwrap();
    d.0.wait().unwrap();
    assert!(pid_alive(g2), "a hard kill cannot take the run with it");
    let d = start(&home);
    let h = history(&home, "slow");
    assert_eq!(h.len(), 2);
    assert_eq!(h[1]["status"], "interrupted");
    assert!(
        h[1]["detail"].as_str().unwrap().contains("process group terminated"),
        "{}",
        h[1]
    );
    let job = &req(&home, json!({"cmd": "get", "id": "slow"}));
    assert!(job["state"]["running"].is_null());
    let reaped = Instant::now() + Duration::from_secs(3);
    while pid_alive(g2) && Instant::now() < reaped {
        std::thread::sleep(Duration::from_millis(50));
    }
    if pid_alive(g2) {
        unsafe { libc::killpg(pgid as i32, libc::SIGKILL) };
        panic!("recovery left the previous daemon's run alive");
    }
    stop(d);
    let _ = std::fs::remove_dir_all(&home);
}

fn slow_job(home: &Path, id: &str, secs: u32) {
    req(
        home,
        json!({"cmd": "add", "job": {"id": id, "name": id,
        "command": ["/bin/sh", "-c", format!("sleep {secs} & echo $! > {id}.pid; wait")], "cwd": home,
        "schedule": {"kind": "cron", "expr": "0 0 1 1 *", "tz": "UTC"}}}),
    );
}

#[test]
fn sigterm_right_after_run_now_leaves_no_orphan() {
    // The run is still spawning when SIGTERM lands; it must be stopped (or
    // never started), not left running with PPID 1.
    let home = temp_home("racestop");
    for i in 0..10 {
        let d = start(&home);
        let id = format!("s{i}");
        slow_job(&home, &id, 30);
        req(&home, json!({"cmd": "run_now", "id": id}));
        stop(d);
        // If it spawned, its grandchild wrote a pid; give it a moment to.
        std::thread::sleep(Duration::from_millis(200));
        if let Ok(s) = std::fs::read_to_string(home.join(format!("{id}.pid")))
            && let Ok(pid) = s.trim().parse::<i64>()
        {
            assert!(!pid_alive(pid), "iteration {i}: run {pid} outlived the daemon");
        }
        let d = start(&home);
        let h = history(&home, &id);
        assert_eq!(h.len(), 1, "iteration {i}: {h:?}");
        assert_eq!(h[0]["status"], "interrupted");
        stop(d);
    }
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn remove_stops_the_run_and_a_re_added_job_is_unaffected() {
    let home = temp_home("readd");
    let d = start(&home);
    slow_job(&home, "x", 30);
    req(&home, json!({"cmd": "run_now", "id": "x"}));
    let pidfile = home.join("x.pid");
    wait_for(
        || std::fs::read_to_string(&pidfile).is_ok_and(|s| s.ends_with('\n')),
        5,
        "first run",
    );
    let first: i64 = std::fs::read_to_string(&pidfile).unwrap().trim().parse().unwrap();
    std::fs::remove_file(&pidfile).unwrap();
    req(&home, json!({"cmd": "remove", "id": "x"}));
    wait_for(|| !pid_alive(first), 5, "removed job's run to stop");

    slow_job(&home, "x", 30);
    req(&home, json!({"cmd": "run_now", "id": "x"}));
    wait_for(
        || std::fs::read_to_string(&pidfile).is_ok_and(|s| s.ends_with('\n')),
        5,
        "second run",
    );
    // Give the first run's thread time to finish and (not) apply its outcome.
    std::thread::sleep(Duration::from_millis(500));
    let job = req(&home, json!({"cmd": "get", "id": "x"}));
    assert!(!job["state"]["running"].is_null(), "new run lost its tracking: {job}");
    assert!(job["state"]["last_status"].is_null(), "{job}");
    assert!(history(&home, "x").is_empty());
    let mut s = UnixStream::connect(home.join("scheduler.sock")).unwrap();
    writeln!(s, "{}", json!({"cmd": "run_now", "id": "x"})).unwrap();
    let mut line = String::new();
    BufReader::new(&s).read_line(&mut line).unwrap();
    assert!(line.contains("already running"), "{line}");
    stop(d);
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn closed_stdout_exits_quietly() {
    // A pipe whose read end is already closed: every write gets EPIPE, as when
    // the CLI is piped into `head` that has exited.
    let (r, w) = std::io::pipe().unwrap();
    drop(r);
    let out = Command::new(BIN)
        .arg("--help")
        .stdout(w)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("panicked"), "panicked on closed stdout: {stderr}");
    assert_eq!(out.status.code(), Some(0), "stderr: {stderr}");
}

#[test]
fn version_works_without_a_daemon() {
    let home = std::env::temp_dir().join(format!("bsched-version-{}", std::process::id()));
    for args in [vec!["--version"], vec!["-V"], vec!["version"]] {
        let out = Command::new(BIN).arg("--home").arg(&home).args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{args:?}");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            format!("boss-sched {}", env!("CARGO_PKG_VERSION"))
        );
    }
    let out = Command::new(BIN).args(["--json", "--version"]).output().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
}
