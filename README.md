# Boss Scheduler

A tiny, always-on job scheduler for the BOSS desktop workspace. One native
binary, `boss-sched`, that knows when jobs are due, spawns them as child
processes, and records what happened. Runs on macOS (launchd) and Linux
(systemd). Windows is not supported.

## The one-job rule

Boss Scheduler does exactly one thing: decide when a job is due, run its argv,
record the outcome. It has no terminals, no MCP server, no tray icon, no UI and
links no BOSS code. Anything a job needs (an agent CLI, a browser, a chat
channel) is the job's business, reached through the command it runs.

This is deliberate. The previous always-on BOSS service (BossTerm's session
daemon) held live terminal sessions, MCP, sharing, a tray icon and its own
login-item install. Because live in-memory state had to survive updates, it
could not start from the packaged app, raced itself into duplicate instances,
and had update and exit bugs. A scheduler that only decides schedules, plus
children that do the work, has none of that state to protect.

Rules that keep it small:

- All state is on disk. The process can be killed at any moment and restarted
  without losing a job, a schedule position or a run record.
- One instance per home directory, enforced by an `flock` held for the life of
  the process. A second copy exits 0, and the service files restart only on
  failure, so a duplicate does not loop.
- Children never inherit the lock or the socket (close-on-exec).

## Install

Prebuilt binaries for macOS and Linux are attached to each
[GitHub release](https://github.com/risa-labs-inc/BossScheduler/releases)
(see "Release artifacts" below). Or build from source.

## Build and test

```bash
cargo build --release        # target/release/boss-sched
cargo test                   # unit + integration tests
cargo fmt --check && cargo clippy --all-targets -- -D warnings
```

Dependencies: `serde`, `serde_json`, `chrono`, `chrono-tz`, `libc`. Cron
parsing is in-tree (`src/cron.rs`) so DST behaviour is ours to define and test.

## Storage

State lives in `~/.boss/scheduler/state.json` (directory mode 0700, file 0600),
rewritten atomically on every change: write `state.json.tmp`, fsync, rename,
fsync the directory. JSON over SQLite because the data set is tiny (a handful
of jobs, 50 runs each, output capped at 16 KiB per stream), it needs no C
dependency or schema migrations, and it is readable with `cat` when debugging.
If the file is ever unreadable the daemon refuses to start rather than
silently discarding jobs.

Files in the home directory:

| File | Purpose |
|------|---------|
| `state.json` | jobs, their schedule state, run history |
| `scheduler.lock` | single-instance `flock`; contains the holder's pid |
| `scheduler.sock` | control socket, mode 0600 |
| `daemon.log` | stdout/stderr when run by launchd or systemd |

The home directory defaults to `~/.boss/scheduler`; override with `--home DIR`
or `BOSS_SCHED_HOME`.

## Job model

```json
{
  "id": "nightly-backup",
  "name": "Nightly backup",
  "schedule": { "kind": "cron", "expr": "30 2 * * *", "tz": "America/Los_Angeles" },
  "command": ["/usr/local/bin/backup", "--quiet"],
  "cwd": "~/work",
  "env": { "FOO": "bar" },
  "timeout_seconds": 600,
  "max_lateness_seconds": 3600,
  "kill_grace_seconds": 10,
  "delete_after_success": false,
  "enabled": true,
  "on_failure": ["/usr/local/bin/notify", "backup failed"],
  "max_consecutive_failures": 5
}
```

- `schedule`: either `{"kind":"cron","expr":"<5 fields>","tz":"<IANA zone>"}`
  (the zone is required) or `{"kind":"at","at":"<RFC 3339 instant>"}` for a
  one-shot. `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` are accepted.
- `command`: an argv array, executed directly with no shell. If you want a
  shell, say so: `["/bin/sh", "-c", "..."]`.
- `id` is optional on `add`; one is derived from the name.
- Unknown fields are rejected, so typos fail loudly.

Defaults: `timeout_seconds` 600, `max_lateness_seconds` 3600,
`kill_grace_seconds` 10, `max_consecutive_failures` 5, `enabled` true.

Each run gets `BOSS_SCHED_JOB_ID`, `BOSS_SCHED_RUN_ID` and `BOSS_SCHED_SLOT`
in its environment, stdin from `/dev/null`, and the daemon's environment plus
the job's `env`.

## Behaviour

**Catch-up without storms.** Each job stores its next due instant. When the
daemon looks at a job whose due instant has passed (laptop asleep, daemon
down), all missed slots coalesce into one decision about the most recent one:
if that slot is within `max_lateness_seconds`, one catch-up run happens
(recorded with `trigger: catch_up` and `missed_slots: N`); otherwise one
`skipped: too late` record is written. Either way the schedule moves to the
next future slot. Slots that pass while a run is still live are dropped, so a
long run never queues a backlog behind it. Re-enabling a disabled job starts
from the next future slot rather than replaying the disabled period.

**DST, wall-clock semantics.** A slot in a spring-forward gap (02:30 on a night
that jumps 02:00 to 03:00) fires once at the first valid minute after the gap.
A slot in a fall-back fold fires once, at its first occurrence, unless the hour
field is a wildcard (`*` or `*/n`), in which case it fires in both passes so
frequent jobs keep their cadence. Day-of-month and day-of-week are OR'd when
both are restricted, as in Vixie cron.

**Timeouts kill the whole tree.** Every run is its own process group. On
timeout the group gets SIGTERM, then SIGKILL after `kill_grace_seconds`. If the
leader exits normally but left background processes in its group, those are
terminated too. A process that deliberately escapes with `setsid` is outside
the group and is not tracked.

**Output.** stdout and stderr are captured separately; the last 16 KiB of each
is kept, with a marker saying how much was dropped.

**Failures.** `failed` (non-zero exit or signal), `timeout` and `spawn_error`
count toward a consecutive-failure counter; `success` resets it; `skipped` and
`interrupted` do not touch it. At 2 consecutive failures the optional
`on_failure` argv runs (with `BOSS_SCHED_JOB_ID`, `BOSS_SCHED_JOB_NAME`,
`BOSS_SCHED_RUN_ID`, `BOSS_SCHED_STATUS`, `BOSS_SCHED_REASON`; 60 s timeout).
At `max_consecutive_failures` the job is disabled, the reason is recorded and
shown by `list`, and `on_failure` runs once more. `enable` resets the counter.

**One-shots.** An `at` job runs once (or is skipped if it is more than
`max_lateness_seconds` late), then has no next run. With
`delete_after_success` it is removed after a successful scheduled run; its
history is kept.

**Restarts.** The pid of every live run, and that process's start time, are
in `state.json`. On SIGTERM or SIGINT (what `launchctl kickstart -k`,
`bootout` and `systemctl stop` send) the daemon stops accepting `run-now`,
SIGTERMs every live run's process group (including runs still spawning and
`on_failure` commands), escalates to SIGKILL after 10 s, records those runs as
`interrupted`, and exits. A run that failed or timed out on its own just
before keeps its real status. If the daemon dies hard (SIGKILL, crash), the
next start finds the stale entries, terminates any leftover process group
whose pid and start time still match (a reused pid is never signalled), and
records them as `interrupted`. Payloads are never replayed; the slot is
considered consumed. If the daemon thread watching a run crashes instead, its
process group (if spawned) is SIGKILLed and the run is recorded as `failed` ("scheduler run
thread crashed; run abandoned"), so the job is scheduled again as usual.

**Removing a running job** stops its run too (SIGTERM, then SIGKILL after
`kill_grace_seconds`). Results are matched to runs by run id, so a run of a
removed job never lands on a new job added under the same id.

## CLI

```
boss-sched [--home DIR] [--json] <command>

  daemon                     run the scheduler (extra copies exit 0)
  version                    binary version (also --version, -V); works without the daemon
  status                     pid, uptime, job counts (exit 3 if not running)
  list                       all jobs with next run, last status, failures
  add --file JOB.json [--test-run]
  add --name NAME (--cron EXPR --tz ZONE | --at RFC3339) [--id ID] [--cwd DIR]
      [--env K=V]... [--timeout SECS] [--max-lateness SECS] [--kill-grace SECS]
      [--max-failures N] [--on-failure JSON_ARGV] [--delete-after-success]
      [--disabled] [--test-run] -- CMD [ARG]...
  remove <id> | enable <id> | disable <id>
  run-now <id> [--wait]      run immediately; does not move the schedule
  history <id> [--limit N] [--full]
  install-agent | uninstall-agent | print-agent
```

Examples:

```bash
boss-sched add --name "disk report" --cron "0 8 * * 1-5" --tz America/Los_Angeles \
    --timeout 120 -- /usr/bin/df -h
boss-sched add --file examples/morning-digest.json
boss-sched add --name ping --at 2026-10-20T17:00:00Z --delete-after-success -- /sbin/ping -c1 1.1.1.1
boss-sched run-now disk-report --wait
boss-sched history disk-report --full
```

`--test-run` adds the job, runs it once immediately, prints the result and
exits non-zero if it did not succeed. The job stays added either way.
`run-now` works on disabled jobs too; it refuses if the job is already running.

Every command except `daemon` and the agent commands is a client of the
socket, so the daemon must be running.

### Socket protocol

`scheduler.sock` speaks line-delimited JSON, one request per line, one reply
per line, several requests per connection allowed:

```
{"cmd":"status"}
{"cmd":"list"}
{"cmd":"get","id":"x"}
{"cmd":"add","job":{...job spec...}}
{"cmd":"remove","id":"x"}   {"cmd":"enable","id":"x"}   {"cmd":"disable","id":"x"}
{"cmd":"run_now","id":"x","wait":true}
{"cmd":"history","id":"x","limit":20}
```

Replies are `{"ok":true,"result":...}` or `{"ok":false,"error":"..."}`.

## Running it under launchd (macOS)

```bash
boss-sched install-agent     # writes ~/Library/LaunchAgents/ai.rever.boss.scheduler.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/ai.rever.boss.scheduler.plist
```

`install-agent` only writes the plist: the absolute binary path (plus
`--home` with an absolute path only when you use a custom home from `--home`
or `BOSS_SCHED_HOME`), `RunAtLoad`, `KeepAlive` with `SuccessfulExit` false
so only a failure restarts it, logging to `<home>/daemon.log` (by default
`~/.boss/scheduler/daemon.log`), and your current `PATH` so jobs can find
tools (entries starting with `~` are expanded to your home, since launchd
does not expand them). It never calls `launchctl`; loading is your step.
`print-agent` shows what it would write, and
`launchd/ai.rever.boss.scheduler.plist.template` is the same thing with
placeholders. `uninstall-agent` removes the file; unload with
`launchctl bootout gui/$(id -u)/ai.rever.boss.scheduler`.

## Update and restart story

The binary holds no state worth protecting, so updating is: replace the file,
restart the service.

```bash
dest=/path/the/plist/points/at/boss-sched
cp target/release/boss-sched "$dest.new" && mv "$dest.new" "$dest"
launchctl kickstart -k gui/$UID/ai.rever.boss.scheduler
```

Copy to a new file and rename it over the old one; do not `cp` straight onto
the installed binary. On macOS, overwriting a signed binary in place leaves
the kernel's cached code signature stale, and every later launch of that file
is killed (exit 137) until it is replaced by a new file.

`kickstart -k` sends SIGTERM; the old process terminates live runs (recorded
as `interrupted`), releases the lock and exits; launchd starts the new binary,
which reloads `state.json` and carries on, applying the catch-up rules to any
slot that came due in between. Restarting at any time is safe. The cost is that
a run in flight during an update is interrupted rather than finished; it is not
retried until its next slot (or a manual `run-now`).

## Running it under systemd (Linux)

```bash
boss-sched install-agent     # writes ~/.config/systemd/user/boss-scheduler.service
systemctl --user daemon-reload
systemctl --user enable --now boss-scheduler.service
loginctl enable-linger $USER # optional: keep running while logged out
```

The unit has the absolute binary path (plus `--home` with an absolute path
only for a custom home from `--home` or `BOSS_SCHED_HOME`), your current `PATH`
(entries starting with `~` expanded to your home),
`Restart=on-failure` and appends to `<home>/daemon.log` (by default
`~/.boss/scheduler/daemon.log`). It uses
`KillMode=mixed`: `systemctl stop` sends SIGTERM to the daemon alone, which
stops its job groups and records them as `interrupted` itself; systemd only
SIGKILLs whatever is left after that. To update, replace the binary and run
`systemctl --user restart boss-scheduler.service`. `uninstall-agent` removes
the file; stop it with `systemctl --user disable --now boss-scheduler.service`.

## Example job

`examples/morning-digest.json` is an example only; nothing installs it. It runs
weekdays at 08:00 America/Los_Angeles with `max_lateness_seconds` 1800 (a
laptop that wakes after 08:30 skips the day rather than sending a stale
digest) and `timeout_seconds` 900. Its command is a placeholder:

```
/bin/sh -c 'exec claude -p "$(cat morning-digest-prompt.txt)"'
```

run from `~/.boss/scheduler/jobs/morning-digest/`, so copy
`examples/morning-digest-prompt.txt` there first. The prompt asks an agent CLI
to summarise today's calendar and unread mail and post a short digest; it is
read only and never sends, replies to or deletes anything. Swap in whichever
headless agent CLI and flags you use. The `PATH` in its `env` is a guess for
Homebrew installs; adjust it to wherever your CLI lives.

## What it deliberately does not do

- No terminals, PTYs, MCP server, tray icon, UI, or login-item install of its own.
- No shell unless the job's argv asks for one.
- No retries or backoff within a slot; a failed run waits for its next slot.
- No replay of interrupted runs, and no queue of missed slots (at most one
  catch-up run).
- No concurrent runs of the same job.
- No distributed or multi-user scheduling; one user, one machine, one home dir.
- No secrets handling. Jobs fetch their own credentials.
- No log rotation of `daemon.log` (it is small: one line per run event).

## Release artifacts

Pushing a tag `v<version>` (where `<version>` matches `version` in
`Cargo.toml`; the workflow fails otherwise) builds release binaries and
publishes a GitHub release named `v<version>` with the assets below. Running
the workflow manually (`workflow_dispatch`) releases the dispatched commit as
`v<version>`. A version that already has a release is never rebuilt; bump
`Cargo.toml` instead.


```
https://github.com/risa-labs-inc/BossScheduler/releases/download/v<version>/boss-sched-<version>-<target>.tar.gz
https://github.com/risa-labs-inc/BossScheduler/releases/download/v<version>/SHA256SUMS
```

`<target>` is one of:

| Target | Built on |
|--------|----------|
| `aarch64-apple-darwin` | macOS (Apple silicon) |
| `x86_64-apple-darwin` | macOS (Intel) |
| `x86_64-unknown-linux-gnu` | Linux x86_64 |
| `aarch64-unknown-linux-gnu` | Linux arm64 |

Each archive contains exactly one file at its root: the `boss-sched`
executable. `SHA256SUMS` lists every archive in `sha256sum` format
(`<hex>  <file name>`); verify with `shasum -a 256 -c SHA256SUMS` or
`sha256sum -c SHA256SUMS`. This layout is a contract: BossConsole downloads
these exact URLs to bundle the scheduler, so changing names or layout is a
breaking change.

The macOS binaries are **unsigned and not notarized**. BossConsole re-signs and
notarizes them as part of its own packaging. If you download one directly,
Gatekeeper may quarantine it; clear that with
`xattr -d com.apple.quarantine boss-sched` or build from source.

## License

Apache-2.0. See [LICENSE](LICENSE).
