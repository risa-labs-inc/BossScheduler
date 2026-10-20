//! `boss-sched` CLI: the daemon itself plus thin socket clients.

use boss_scheduler::{agent, daemon, daemon::Paths};
use chrono::{DateTime, Local, Utc};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

/// `println!` that exits quietly when stdout is closed (e.g. piped into `head`)
/// instead of panicking with "failed printing to stdout: Broken pipe".
macro_rules! outln {
    ($($t:tt)*) => { out_write(format_args!("{}\n", format_args!($($t)*))) };
}

/// `print!` with the same broken-pipe handling as `outln!`.
macro_rules! out {
    ($($t:tt)*) => { out_write(format_args!($($t)*)) };
}

fn out_write(args: std::fmt::Arguments) {
    let mut o = std::io::stdout().lock();
    if let Err(e) = o.write_fmt(args).and_then(|_| o.flush()) {
        if e.kind() == std::io::ErrorKind::BrokenPipe {
            std::process::exit(0);
        }
        eprintln!("boss-sched: write stdout: {e}");
        std::process::exit(1);
    }
}

const USAGE: &str = "\
boss-sched - tiny always-on job scheduler

usage: boss-sched [--home DIR] [--json] <command>

  daemon                     run the scheduler (single instance; extra copies exit 0)
  version                    print the binary version (also --version, -V); no daemon needed
  status                     daemon pid, uptime, job counts (exit 3 if not running)
  list                       all jobs
  add --file JOB.json [--test-run]
  add --name NAME (--cron EXPR --tz ZONE | --at RFC3339) [--id ID] [--cwd DIR]
      [--env K=V]... [--timeout SECS] [--max-lateness SECS] [--kill-grace SECS]
      [--max-failures N] [--on-failure JSON_ARGV] [--delete-after-success]
      [--disabled] [--test-run] -- CMD [ARG]...
  remove <id> | enable <id> | disable <id>
  run-now <id> [--wait]      start a run immediately (does not move the schedule)
  history <id> [--limit N] [--full]
  install-agent              write the login service (does not start it): macOS
                             ~/Library/LaunchAgents/ai.rever.boss.scheduler.plist,
                             Linux ~/.config/systemd/user/boss-scheduler.service
  uninstall-agent            remove that file (does not stop the service)
  print-agent                print what install-agent would write

home defaults to $BOSS_SCHED_HOME or ~/.boss/scheduler";

fn main() {
    let code = match real_main() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("boss-sched: {e}");
            1
        }
    };
    std::process::exit(code);
}

struct Args(Vec<String>);

impl Args {
    fn flag(&mut self, name: &str) -> bool {
        let before = self.0.len();
        self.0.retain(|a| a != name);
        self.0.len() != before
    }
    fn opt(&mut self, name: &str) -> Result<Option<String>, String> {
        let Some(i) = self.0.iter().position(|a| a == name) else {
            return Ok(None);
        };
        if i + 1 >= self.0.len() {
            return Err(format!("{name} needs a value"));
        }
        self.0.remove(i);
        Ok(Some(self.0.remove(i)))
    }
    fn all(&mut self, name: &str) -> Result<Vec<String>, String> {
        let mut v = vec![];
        while let Some(x) = self.opt(name)? {
            v.push(x);
        }
        Ok(v)
    }
    fn num(&mut self, name: &str) -> Result<Option<u64>, String> {
        self.opt(name)?
            .map(|v| v.parse().map_err(|_| format!("{name}: not a number: {v}")))
            .transpose()
    }
    fn id(&mut self) -> Result<String, String> {
        if self.0.is_empty() {
            Err("missing job id".into())
        } else {
            Ok(self.0.remove(0))
        }
    }
}

fn real_main() -> Result<i32, String> {
    let mut raw: Vec<String> = std::env::args().skip(1).collect();
    let tail = raw.iter().position(|a| a == "--").map(|i| {
        let t = raw.split_off(i);
        t[1..].to_vec()
    });
    let mut a = Args(raw);
    let custom_home = a.opt("--home")?.or_else(|| std::env::var("BOSS_SCHED_HOME").ok());
    let home = match &custom_home {
        Some(h) => PathBuf::from(h),
        None => PathBuf::from(std::env::var("HOME").map_err(|_| "HOME is not set")?).join(".boss/scheduler"),
    };
    let json_out = a.flag("--json");
    if a.flag("--version") || a.flag("-V") || a.0.first().is_some_and(|c| c == "version") {
        if json_out {
            outln!("{}", json!({"version": env!("CARGO_PKG_VERSION")}));
        } else {
            outln!("boss-sched {}", env!("CARGO_PKG_VERSION"));
        }
        return Ok(0);
    }
    if a.0.is_empty() || a.flag("--help") || a.flag("-h") {
        outln!("{USAGE}");
        return Ok(0);
    }
    let cmd = a.0.remove(0);
    let call = |req: Value| client(&home, req);
    let show = |v: &Value, human: &dyn Fn(&Value)| {
        if json_out {
            outln!("{}", serde_json::to_string_pretty(v).unwrap())
        } else {
            human(v)
        }
    };
    match cmd.as_str() {
        "daemon" => return daemon::run(&home),
        "status" => match call(json!({"cmd": "status"})) {
            Ok(v) => show(&v, &|v| {
                outln!(
                    "running  pid {}  version {}  since {}  jobs {}  running {}  home {}",
                    v["pid"],
                    v["version"].as_str().unwrap_or(""),
                    when(&v["started_at"]),
                    v["jobs"],
                    v["running"],
                    v["home"].as_str().unwrap_or("")
                )
            }),
            Err(e) => {
                outln!("not running ({e})");
                return Ok(3);
            }
        },
        "list" => show(&call(json!({"cmd": "list"}))?, &print_jobs),
        "add" => {
            let test_run = a.flag("--test-run");
            let job = match a.opt("--file")? {
                Some(f) => serde_json::from_slice::<Value>(&std::fs::read(&f).map_err(|e| format!("read {f}: {e}"))?)
                    .map_err(|e| format!("{f}: {e}"))?,
                None => job_from_flags(&mut a, tail.unwrap_or_default())?,
            };
            let added = call(json!({"cmd": "add", "job": job}))?;
            let id = added["spec"]["id"].as_str().unwrap_or_default().to_string();
            show(&added, &|v| {
                outln!("added {id}, next run {}", when(&v["state"]["next_run_at"]))
            });
            if test_run {
                eprintln!("test run of {id}...");
                let rec = call(json!({"cmd": "run_now", "id": id, "wait": true}))?;
                show(&rec, &|r| print_run(r, true));
                return Ok(if rec["status"] == "success" { 0 } else { 1 });
            }
        }
        "remove" | "enable" | "disable" => {
            let v = call(json!({"cmd": cmd, "id": a.id()?}))?;
            show(&v, &|_| outln!("{cmd}d"));
        }
        "run-now" => {
            let id = a.id()?;
            let wait = a.flag("--wait");
            let v = call(json!({"cmd": "run_now", "id": id, "wait": wait}))?;
            show(&v, &|v| {
                if wait {
                    print_run(v, true)
                } else {
                    outln!("started {}", v["run_id"])
                }
            });
            if wait && v["status"] != "success" {
                return Ok(1);
            }
        }
        "history" => {
            let id = a.id()?;
            let full = a.flag("--full");
            let limit = a.num("--limit")?.unwrap_or(20);
            let v = call(json!({"cmd": "history", "id": id, "limit": limit}))?;
            show(&v, &|v| {
                for r in v.as_array().into_iter().flatten() {
                    print_run(r, full);
                }
            });
        }
        "install-agent" => outln!("{}", agent::install(&home, custom_home.is_some())?),
        "uninstall-agent" => outln!("{}", agent::uninstall()?),
        "print-agent" => out!("{}", agent::render(&home, custom_home.is_some())?),
        other => return Err(format!("unknown command '{other}' (try --help)")),
    }
    Ok(0)
}

fn job_from_flags(a: &mut Args, command: Vec<String>) -> Result<Value, String> {
    let name = a.opt("--name")?.ok_or("--name is required (or use --file)")?;
    let schedule = match (a.opt("--cron")?, a.opt("--tz")?, a.opt("--at")?) {
        (Some(expr), Some(tz), None) => json!({"kind": "cron", "expr": expr, "tz": tz}),
        (Some(_), None, None) => return Err("--cron needs --tz (IANA zone, e.g. America/Los_Angeles)".into()),
        (None, None, Some(at)) => json!({"kind": "at", "at": at}),
        _ => return Err("give exactly one of --cron EXPR --tz ZONE, or --at RFC3339".into()),
    };
    if command.is_empty() {
        return Err("command missing: put it after `--`".into());
    }
    let mut env = serde_json::Map::new();
    for kv in a.all("--env")? {
        let (k, v) = kv.split_once('=').ok_or_else(|| format!("--env wants K=V, got {kv}"))?;
        env.insert(k.into(), v.into());
    }
    let mut job = json!({"name": name, "schedule": schedule, "command": command, "env": env,
                         "delete_after_success": a.flag("--delete-after-success"),
                         "enabled": !a.flag("--disabled")});
    for (flag, key) in [
        ("--timeout", "timeout_seconds"),
        ("--max-lateness", "max_lateness_seconds"),
        ("--kill-grace", "kill_grace_seconds"),
        ("--max-failures", "max_consecutive_failures"),
    ] {
        if let Some(n) = a.num(flag)? {
            job[key] = json!(n);
        }
    }
    if let Some(id) = a.opt("--id")? {
        job["id"] = json!(id);
    }
    if let Some(cwd) = a.opt("--cwd")? {
        job["cwd"] = json!(cwd);
    }
    if let Some(f) = a.opt("--on-failure")? {
        job["on_failure"] =
            serde_json::from_str(&f).map_err(|e| format!("--on-failure wants a JSON argv array: {e}"))?;
    }
    if !a.0.is_empty() {
        return Err(format!("unexpected arguments: {:?}", a.0));
    }
    Ok(job)
}

fn client(home: &std::path::Path, req: Value) -> Result<Value, String> {
    let sock = Paths::new(home).socket;
    let mut s = UnixStream::connect(&sock).map_err(|e| format!("daemon not reachable at {}: {e}", sock.display()))?;
    if req["wait"] != true {
        let _ = s.set_read_timeout(Some(Duration::from_secs(30)));
    }
    writeln!(s, "{req}").map_err(|e| e.to_string())?;
    let mut line = String::new();
    BufReader::new(&s)
        .read_line(&mut line)
        .map_err(|e| format!("read reply: {e}"))?;
    let resp: Value = serde_json::from_str(&line).map_err(|e| format!("bad reply: {e}"))?;
    if resp["ok"] == true {
        Ok(resp["result"].clone())
    } else {
        Err(resp["error"].as_str().unwrap_or("error").into())
    }
}

fn when(v: &Value) -> String {
    v.as_str()
        .and_then(|s| s.parse::<DateTime<Utc>>().ok())
        .map(|t| t.with_timezone(&Local).format("%a %Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".into())
}

fn print_jobs(v: &Value) {
    outln!(
        "{:<24} {:<8} {:<25} {:<12} {:<5} {:<34} NAME",
        "ID",
        "ENABLED",
        "NEXT",
        "LAST",
        "FAILS",
        "SCHEDULE"
    );
    for j in v.as_array().into_iter().flatten() {
        let (spec, st) = (&j["spec"], &j["state"]);
        let sched = match spec["schedule"]["kind"].as_str() {
            Some("cron") => format!(
                "{} {}",
                spec["schedule"]["expr"].as_str().unwrap_or(""),
                spec["schedule"]["tz"].as_str().unwrap_or("")
            ),
            _ => format!("at {}", spec["schedule"]["at"].as_str().unwrap_or("")),
        };
        let last = if st["running"].is_object() {
            "running".into()
        } else {
            st["last_status"].as_str().unwrap_or("-").to_string()
        };
        outln!(
            "{:<24} {:<8} {:<25} {:<12} {:<5} {:<34} {}",
            spec["id"].as_str().unwrap_or(""),
            if spec["enabled"] == true { "yes" } else { "no" },
            when(&st["next_run_at"]),
            last,
            st["consecutive_failures"],
            sched,
            spec["name"].as_str().unwrap_or("")
        );
        if let Some(why) = st["disabled_reason"].as_str() {
            outln!("{:<24} ^ {why}", "");
        }
    }
}

fn print_run(r: &Value, full: bool) {
    let code = r["exit_code"]
        .as_i64()
        .map(|c| format!("exit {c}"))
        .or_else(|| r["signal"].as_i64().map(|s| format!("signal {s}")))
        .unwrap_or_else(|| "-".into());
    outln!(
        "{}  {}  {:<12} {:<10} {:<9} missed={}  {}",
        r["run_id"].as_str().unwrap_or(""),
        when(&r["started_at"]),
        r["status"].as_str().unwrap_or(""),
        code,
        r["trigger"].as_str().unwrap_or(""),
        r["missed_slots"],
        r["detail"].as_str().unwrap_or("")
    );
    if full {
        for k in ["stdout", "stderr"] {
            if let Some(s) = r[k].as_str().filter(|s| !s.is_empty()) {
                outln!("--- {k} ---\n{}", s.trim_end());
            }
        }
    }
}
