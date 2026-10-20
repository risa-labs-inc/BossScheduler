//! Job model and the pure scheduling decision (what is due, catch-up, lateness).

use crate::cron::Cron;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Schedule {
    /// Five-field cron expression; `tz` is a required IANA zone.
    Cron { expr: String, tz: String },
    /// One-shot at an absolute instant.
    At { at: DateTime<Utc> },
}

/// What the user asked for. Everything here comes from `add`.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub schedule: Schedule,
    /// argv; executed directly, no shell.
    pub command: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "d_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "d_lateness")]
    pub max_lateness_seconds: u64,
    #[serde(default = "d_grace")]
    pub kill_grace_seconds: u64,
    #[serde(default)]
    pub delete_after_success: bool,
    #[serde(default = "d_true")]
    pub enabled: bool,
    /// argv run after 2 consecutive failures and on auto-disable.
    #[serde(default)]
    pub on_failure: Option<Vec<String>>,
    #[serde(default = "d_max_failures")]
    pub max_consecutive_failures: u32,
}

fn d_timeout() -> u64 {
    600
}
fn d_lateness() -> u64 {
    3600
}
fn d_grace() -> u64 {
    10
}
fn d_true() -> bool {
    true
}
fn d_max_failures() -> u32 {
    5
}

/// Failures in a row that trigger the `on_failure` notify command.
pub const NOTIFY_AFTER_FAILURES: u32 = 2;

/// What the scheduler tracks about a job. Owned by the daemon.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct JobState {
    pub next_run_at: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub disabled_reason: Option<String>,
    pub running: Option<Running>,
    pub last_status: Option<Status>,
    pub last_finished_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Running {
    pub run_id: String,
    pub pid: Option<u32>,
    /// `runner::process_start` of `pid`, so recovery never signals a reused pid.
    #[serde(default)]
    pub pid_start: Option<u64>,
    pub started_at: DateTime<Utc>,
    pub trigger: Trigger,
    pub slot: Option<DateTime<Utc>>,
    pub missed_slots: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Job {
    pub spec: JobSpec,
    #[serde(default)]
    pub state: JobState,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    Schedule,
    CatchUp,
    Manual,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Success,
    Failed,
    Timeout,
    SpawnError,
    Skipped,
    Interrupted,
}

impl Status {
    /// Statuses that count toward the consecutive-failure counter.
    pub fn is_failure(self) -> bool {
        matches!(self, Status::Failed | Status::Timeout | Status::SpawnError)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RunRecord {
    pub run_id: String,
    pub job_id: String,
    pub trigger: Trigger,
    pub slot: Option<DateTime<Utc>>,
    /// Scheduled slots coalesced into this run (1 = on time, >1 = catch-up).
    pub missed_slots: u32,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: Status,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub detail: Option<String>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
}

impl JobSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("name is required".into());
        }
        if !self.id.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)) || self.id.is_empty() {
            return Err(format!("bad id '{}' (use letters, digits, - _ .)", self.id));
        }
        if self.command.is_empty() || self.command[0].is_empty() {
            return Err("command must be a non-empty argv array".into());
        }
        if self.timeout_seconds == 0 {
            return Err("timeout_seconds must be > 0".into());
        }
        if self.max_consecutive_failures == 0 {
            return Err("max_consecutive_failures must be > 0".into());
        }
        if let Schedule::Cron { expr, tz } = &self.schedule {
            Cron::parse(expr, tz)?;
        }
        if let Some(cwd) = &self.cwd
            && !std::path::Path::new(&expand_tilde(cwd)).is_dir()
        {
            return Err(format!("cwd '{cwd}' is not a directory"));
        }
        if self.on_failure.as_ref().is_some_and(|a| a.is_empty()) {
            return Err("on_failure must be a non-empty argv array".into());
        }
        Ok(())
    }

    /// First fire instant strictly after `after` (for one-shots: the `at` time, once).
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match &self.schedule {
            Schedule::Cron { expr, tz } => Cron::parse(expr, tz).ok()?.next_after(after),
            Schedule::At { .. } => None,
        }
    }

    pub fn initial_next(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match &self.schedule {
            Schedule::Cron { .. } => self.next_after(now),
            Schedule::At { at } => Some(*at),
        }
    }

    pub fn is_one_shot(&self) -> bool {
        matches!(self.schedule, Schedule::At { .. })
    }
}

pub fn expand_tilde(p: &str) -> String {
    match (p.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => format!("{home}/{rest}"),
        _ if p == "~" => std::env::var("HOME").unwrap_or_else(|_| p.into()),
        _ => p.to_string(),
    }
}

#[derive(Debug, PartialEq)]
pub enum Decision {
    NotDue,
    Run {
        slot: DateTime<Utc>,
        missed: u32,
    },
    Skip {
        slot: DateTime<Utc>,
        missed: u32,
        reason: String,
    },
}

/// Cap on slot walking so a years-long gap on an every-minute job stays cheap.
const MAX_SLOT_WALK: u32 = 100_000;

/// Decide what to do with a job at `now`. Returns the decision and the new
/// `next_run_at`. All missed slots coalesce into one decision about the most
/// recent missed slot; lateness is measured from that slot.
pub fn plan(
    spec: &JobSpec,
    next_run_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (Decision, Option<DateTime<Utc>>) {
    let Some(first) = next_run_at else {
        return (Decision::NotDue, None);
    };
    if first > now {
        return (Decision::NotDue, Some(first));
    }
    let cron = match &spec.schedule {
        Schedule::Cron { expr, tz } => Cron::parse(expr, tz).ok(),
        Schedule::At { .. } => None,
    };
    let step = |after| cron.as_ref().and_then(|c| c.next_after(after));
    let mut latest = first;
    let mut missed = 1u32;
    let mut upcoming = step(latest);
    while let Some(n) = upcoming {
        if n > now {
            break;
        }
        latest = n;
        missed += 1;
        if missed >= MAX_SLOT_WALK {
            // Measure lateness from the real latest slot, not where the walk
            // stopped; `missed` is then a lower bound.
            if let Some(l) = last_slot_at_or_before(&step, latest, now) {
                latest = l;
            }
            upcoming = step(now);
            break;
        }
        upcoming = step(latest);
    }
    let late = now - latest;
    let decision = if late > Duration::seconds(spec.max_lateness_seconds as i64) {
        Decision::Skip {
            slot: latest,
            missed,
            reason: format!(
                "skipped: too late ({}s past slot, max_lateness {}s)",
                late.num_seconds(),
                spec.max_lateness_seconds
            ),
        }
    } else {
        Decision::Run { slot: latest, missed }
    };
    (decision, upcoming)
}

/// Most recent slot in (`floor`, `now`], found by widening a window back from
/// `now` so the walk stays short however far behind `floor` is.
fn last_slot_at_or_before(
    step: &impl Fn(DateTime<Utc>) -> Option<DateTime<Utc>>,
    floor: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let mut back = Duration::minutes(1);
    while now - back > floor {
        if let Some(mut s) = step(now - back).filter(|s| *s <= now) {
            while let Some(n) = step(s).filter(|n| *n <= now) {
                s = n;
            }
            return Some(s);
        }
        back = back * 2;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(expr: &str, lateness: u64) -> JobSpec {
        serde_json::from_value(serde_json::json!({
            "id": "t", "name": "t", "command": ["true"],
            "schedule": {"kind": "cron", "expr": expr, "tz": "UTC"},
            "max_lateness_seconds": lateness,
        }))
        .unwrap()
    }
    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn not_due_before_slot() {
        let s = spec("0 * * * *", 60);
        let (d, n) = plan(&s, Some(t("2026-10-19T10:00:00Z")), t("2026-10-19T09:59:59Z"));
        assert_eq!(d, Decision::NotDue);
        assert_eq!(n, Some(t("2026-10-19T10:00:00Z")));
    }

    #[test]
    fn on_time_run() {
        let s = spec("0 * * * *", 60);
        let (d, n) = plan(&s, Some(t("2026-10-19T10:00:00Z")), t("2026-10-19T10:00:01Z"));
        assert_eq!(
            d,
            Decision::Run {
                slot: t("2026-10-19T10:00:00Z"),
                missed: 1
            }
        );
        assert_eq!(n, Some(t("2026-10-19T11:00:00Z")));
    }

    #[test]
    fn missed_slots_coalesce_into_one_catch_up() {
        // Asleep from 10:00 to 15:20: six hourly slots, one run for the 15:00 slot.
        let s = spec("0 * * * *", 1800);
        let (d, n) = plan(&s, Some(t("2026-10-19T10:00:00Z")), t("2026-10-19T15:20:00Z"));
        assert_eq!(
            d,
            Decision::Run {
                slot: t("2026-10-19T15:00:00Z"),
                missed: 6
            }
        );
        assert_eq!(n, Some(t("2026-10-19T16:00:00Z")));
        // Planning again at the same instant with the new next is a no-op: no storm.
        assert_eq!(plan(&s, n, t("2026-10-19T15:20:00Z")).0, Decision::NotDue);
    }

    #[test]
    fn catch_up_beyond_max_lateness_is_skipped() {
        let s = spec("17 9 * * *", 2400);
        // Woke at 10:00, 43 minutes after the 09:17 slot: too late.
        let (d, n) = plan(&s, Some(t("2026-10-17T09:17:00Z")), t("2026-10-19T10:00:00Z"));
        match d {
            Decision::Skip { slot, missed, reason } => {
                assert_eq!(slot, t("2026-10-19T09:17:00Z"));
                assert_eq!(missed, 3);
                assert!(reason.starts_with("skipped: too late"), "{reason}");
            }
            other => panic!("expected skip, got {other:?}"),
        }
        assert_eq!(n, Some(t("2026-10-20T09:17:00Z")));
        // Woke at 09:40: within 2400s of today's slot, so one catch-up run.
        let (d, _) = plan(&s, Some(t("2026-10-17T09:17:00Z")), t("2026-10-19T09:40:00Z"));
        assert_eq!(
            d,
            Decision::Run {
                slot: t("2026-10-19T09:17:00Z"),
                missed: 3
            }
        );
    }

    #[test]
    fn one_shot() {
        let mut s = spec("0 * * * *", 60);
        s.schedule = Schedule::At {
            at: t("2026-10-19T10:00:00Z"),
        };
        let (d, n) = plan(&s, s.initial_next(t("2026-10-19T09:00:00Z")), t("2026-10-19T10:00:30Z"));
        assert_eq!(
            d,
            Decision::Run {
                slot: t("2026-10-19T10:00:00Z"),
                missed: 1
            }
        );
        assert_eq!(n, None);
        let (d, _) = plan(&s, Some(t("2026-10-19T10:00:00Z")), t("2026-10-19T11:00:00Z"));
        assert!(matches!(d, Decision::Skip { .. }));
    }

    #[test]
    fn example_job_file_parses() {
        let s: JobSpec = serde_json::from_str(include_str!("../examples/morning-digest.json")).unwrap();
        assert_eq!((s.timeout_seconds, s.max_lateness_seconds), (900, 1800));
        // Friday 2026-10-16 after the slot -> Monday 08:00 PDT.
        assert_eq!(s.next_after(t("2026-10-16T17:00:00Z")), Some(t("2026-10-19T15:00:00Z")));
    }

    #[test]
    fn huge_gap_is_bounded() {
        let s = spec("* * * * *", 120);
        let (d, n) = plan(&s, Some(t("2020-01-01T00:00:00Z")), t("2026-10-19T10:00:30Z"));
        // The walk is capped, but lateness is still measured from the 10:00
        // slot (30s ago), so it runs rather than being skipped.
        assert_eq!(
            d,
            Decision::Run {
                slot: t("2026-10-19T10:00:00Z"),
                missed: MAX_SLOT_WALK
            }
        );
        assert_eq!(n, Some(t("2026-10-19T10:01:00Z")));
        // A sparse schedule far behind finds its latest slot too.
        let s = spec("17 9 1 * *", 3600);
        let (d, _) = plan(&s, Some(t("2026-10-01T09:17:00Z")), t("2026-10-19T10:00:00Z"));
        assert!(matches!(d, Decision::Skip { slot, .. } if slot == t("2026-10-01T09:17:00Z")));
    }
}
