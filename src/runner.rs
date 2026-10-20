//! Spawns one job run as a child in its own process group, enforces the
//! timeout on the whole group, and captures bounded stdout/stderr.

use crate::job::{JobSpec, Status, expand_tilde};
use std::io::Read;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

/// Bytes of output kept per stream (the tail is kept).
pub const OUTPUT_CAP: usize = 16 * 1024;
const POLL: Duration = Duration::from_millis(50);

pub struct Started {
    child: Child,
    out: Receiver<String>,
    err: Receiver<String>,
}

impl Started {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

#[derive(Debug)]
pub struct Outcome {
    pub status: Status,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub detail: Option<String>,
    pub stdout: String,
    pub stderr: String,
}

pub fn start(spec: &JobSpec, extra_env: &[(&str, String)]) -> std::io::Result<Started> {
    let mut cmd = Command::new(&spec.command[0]);
    cmd.args(&spec.command[1..])
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(&spec.env)
        .envs(extra_env.iter().map(|(k, v)| (*k, v)));
    if let Some(cwd) = &spec.cwd {
        cmd.current_dir(expand_tilde(cwd));
    }
    // The daemon blocks TERM/INT in all threads (see daemon::run) and std's
    // spawn does not reliably clear the inherited mask, so a child would be
    // deaf to the SIGTERM we send on timeout. Clear it before exec.
    unsafe {
        cmd.pre_exec(|| {
            let mut empty: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut empty);
            libc::sigprocmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut());
            Ok(())
        });
    }
    let mut child = cmd.spawn()?;
    let out = capture(child.stdout.take().expect("piped"));
    let err = capture(child.stderr.take().expect("piped"));
    Ok(Started { child, out, err })
}

/// Wait for the run, killing the whole process group on timeout. Any group
/// members left behind after the leader exits are also terminated.
pub fn wait(mut s: Started, timeout: Duration, grace: Duration) -> Outcome {
    let pgid = s.child.id() as i32;
    let began = Instant::now();
    let mut status: Option<ExitStatus> = None;
    let mut timed_out = false;
    while status.is_none() {
        match s.child.try_wait() {
            Ok(Some(st)) => status = Some(st),
            Ok(None) if began.elapsed() >= timeout => {
                timed_out = true;
                kill_group(&mut s.child, pgid, &mut status, grace);
            }
            Ok(None) => thread::sleep(POLL),
            Err(e) => {
                kill_group(&mut s.child, pgid, &mut status, grace);
                return outcome(Status::Failed, None, Some(format!("wait failed: {e}")), &s);
            }
        }
    }
    let mut detail = None;
    if !timed_out && group_alive(pgid) {
        kill_group(&mut s.child, pgid, &mut status, grace);
        detail = Some("leader exited; leftover processes in its group were terminated".to_string());
    }
    let Some(st) = status else {
        return outcome(
            Status::Failed,
            None,
            Some("could not reap the run's process".into()),
            &s,
        );
    };
    let (code, sig) = (st.code(), st.signal());
    let (kind, detail) = if timed_out {
        (
            Status::Timeout,
            Some(format!("timed out after {}s; process group killed", timeout.as_secs())),
        )
    } else if st.success() {
        (Status::Success, detail)
    } else {
        (Status::Failed, detail)
    };
    let mut o = outcome(kind, code, detail, &s);
    o.signal = sig;
    o
}

fn outcome(status: Status, exit_code: Option<i32>, detail: Option<String>, s: &Started) -> Outcome {
    // A process that escaped the group (setsid) may still hold the pipes; do not hang on it.
    let take = |rx: &Receiver<String>| {
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|_| "[output incomplete: a process outside the job group holds the pipe]".into())
    };
    Outcome {
        status,
        exit_code,
        signal: None,
        detail,
        stdout: take(&s.out),
        stderr: take(&s.err),
    }
}

/// SIGTERM the group, wait up to `grace` for leader and group to go, then SIGKILL.
fn kill_group(child: &mut Child, pgid: i32, status: &mut Option<ExitStatus>, grace: Duration) {
    signal_group(pgid, libc::SIGTERM);
    let deadline = Instant::now() + grace;
    loop {
        if status.is_none() {
            *status = child.try_wait().ok().flatten();
        }
        if status.is_some() && !group_alive(pgid) {
            return;
        }
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(POLL);
    }
    signal_group(pgid, libc::SIGKILL);
    if status.is_none() {
        *status = child.wait().ok();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while group_alive(pgid) && Instant::now() < deadline {
        thread::sleep(POLL);
    }
}

pub fn signal_group(pgid: i32, sig: i32) {
    if pgid > 1 {
        unsafe { libc::killpg(pgid, sig) };
    }
}

pub fn group_alive(pgid: i32) -> bool {
    pgid > 1 && unsafe { libc::killpg(pgid, 0) } == 0
}

pub fn pid_alive(pid: u32) -> bool {
    pid > 1 && unsafe { libc::kill(pid as i32, 0) } == 0
}

/// SIGTERM a group that is not our child (e.g. left over from a previous
/// daemon), wait up to `grace` for it to go, then SIGKILL.
pub fn terminate_group(pgid: i32, grace: Duration) {
    signal_group(pgid, libc::SIGTERM);
    let deadline = Instant::now() + grace;
    while group_alive(pgid) && Instant::now() < deadline {
        thread::sleep(POLL);
    }
    signal_group(pgid, libc::SIGKILL);
    let deadline = Instant::now() + Duration::from_secs(2);
    while group_alive(pgid) && Instant::now() < deadline {
        thread::sleep(POLL);
    }
}

/// When a process started, in an opaque OS unit. Recorded next to a run's
/// pid so a restarted daemon can tell its old child from a reused pid.
#[cfg(target_os = "linux")]
pub fn process_start(pid: u32) -> Option<u64> {
    // Field 22 of /proc/PID/stat (clock ticks since boot). The command name
    // (field 2) may contain spaces, so count from the last ')'.
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
}

#[cfg(target_os = "macos")]
pub fn process_start(pid: u32) -> Option<u64> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
    let n = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    (n == size).then(|| info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn process_start(_pid: u32) -> Option<u64> {
    None
}

/// Drain a pipe on a thread, keeping only the last OUTPUT_CAP bytes.
fn capture<R: Read + Send + 'static>(mut r: R) -> Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let (mut buf, mut dropped, mut chunk) = (Vec::new(), 0usize, [0u8; 8192]);
        loop {
            match r.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
            if buf.len() > 2 * OUTPUT_CAP {
                let cut = buf.len() - OUTPUT_CAP;
                buf.drain(..cut);
                dropped += cut;
            }
        }
        if buf.len() > OUTPUT_CAP {
            let cut = buf.len() - OUTPUT_CAP;
            buf.drain(..cut);
            dropped += cut;
        }
        let text = String::from_utf8_lossy(&buf).into_owned();
        let _ = tx.send(if dropped > 0 {
            format!("[... {dropped} bytes truncated ...]\n{text}")
        } else {
            text
        });
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(argv: &[&str], timeout: u64) -> JobSpec {
        serde_json::from_value(serde_json::json!({
            "id": "t", "name": "t", "command": argv, "timeout_seconds": timeout,
            "schedule": {"kind": "at", "at": "2030-01-01T00:00:00Z"},
        }))
        .unwrap()
    }

    fn run(argv: &[&str], timeout: u64, grace: u64) -> (Outcome, Duration) {
        let t = Instant::now();
        let s = start(&spec(argv, timeout), &[("BOSS_SCHED_JOB_ID", "t".into())]).unwrap();
        (
            wait(s, Duration::from_secs(timeout), Duration::from_secs(grace)),
            t.elapsed(),
        )
    }

    fn assert_dead(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while pid_alive(pid) && Instant::now() < deadline {
            thread::sleep(POLL);
        }
        assert!(!pid_alive(pid), "grandchild {pid} survived");
    }

    #[test]
    fn success_and_output_and_env() {
        let (o, _) = run(&["/bin/sh", "-c", "echo out $BOSS_SCHED_JOB_ID; echo err >&2"], 10, 1);
        assert_eq!(o.status, Status::Success);
        assert_eq!(o.stdout, "out t\n");
        assert_eq!(o.stderr, "err\n");
    }

    #[test]
    fn failure_exit_code() {
        let (o, _) = run(&["/bin/sh", "-c", "exit 3"], 10, 1);
        assert_eq!((o.status, o.exit_code), (Status::Failed, Some(3)));
    }

    #[test]
    fn output_is_truncated_to_tail() {
        let (o, _) = run(
            &["/bin/sh", "-c", "head -c 100000 /dev/zero | tr '\\0' a; echo END"],
            10,
            1,
        );
        assert!(o.stdout.starts_with("[... "), "{}", &o.stdout[..40]);
        assert!(o.stdout.ends_with("END\n"));
        assert!(o.stdout.len() < OUTPUT_CAP + 64);
    }

    #[test]
    fn timeout_kills_whole_group_with_sigterm() {
        // Child shell spawns a grandchild sleeper and prints its pid.
        let (o, took) = run(&["/bin/sh", "-c", "sleep 300 & echo $!; wait"], 1, 5);
        assert_eq!(o.status, Status::Timeout);
        assert!(
            took < Duration::from_secs(4),
            "SIGTERM should have sufficed, took {took:?}"
        );
        assert_dead(o.stdout.trim().parse().unwrap());
    }

    #[test]
    fn timeout_escalates_to_sigkill() {
        // TERM is ignored by the shell and (inherited) by the grandchild.
        let (o, took) = run(&["/bin/sh", "-c", "trap '' TERM; sleep 300 & echo $!; wait"], 1, 1);
        assert_eq!((o.status, o.signal), (Status::Timeout, Some(libc::SIGKILL)));
        assert!(took >= Duration::from_secs(2));
        assert_dead(o.stdout.trim().parse().unwrap());
    }

    #[test]
    fn leftover_background_process_is_reaped() {
        let (o, _) = run(&["/bin/sh", "-c", "sleep 300 >/dev/null 2>&1 & echo $!"], 10, 1);
        assert_eq!(o.status, Status::Success);
        assert!(o.detail.unwrap().contains("leftover"));
        assert_dead(o.stdout.trim().parse().unwrap());
    }

    #[test]
    fn process_start_identifies_a_live_process() {
        let me = std::process::id();
        let a = process_start(me);
        assert!(a.is_some());
        assert_eq!(a, process_start(me), "stable for the same process");
        let mut c = Command::new("/bin/sleep").arg("5").spawn().unwrap();
        assert_ne!(process_start(c.id()), None);
        c.kill().unwrap();
        c.wait().unwrap();
        assert_eq!(process_start(c.id()), None, "gone once reaped");
    }

    #[test]
    fn missing_binary_is_spawn_error() {
        assert!(start(&spec(&["/nonexistent/boss-sched-x"], 1), &[]).is_err());
    }
}
