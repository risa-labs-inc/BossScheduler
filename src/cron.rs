//! Five-field cron expressions evaluated in an IANA timezone.
//!
//! DST rules (wall-clock semantics, like Vixie cron):
//! - A slot that falls in a spring-forward gap fires once, at the first
//!   valid minute after the gap (02:30 on a 02:00->03:00 night fires at 03:00).
//! - A slot in a fall-back fold fires once, at its first occurrence, unless the
//!   hour field is a wildcard (`*` or `*/n`), in which case it fires in both
//!   passes so frequent jobs keep their cadence.

use chrono::{DateTime, Datelike, Duration, LocalResult, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
/// How far ahead to look before declaring that an expression never fires.
const HORIZON_YEARS: i32 = 6;

#[derive(Debug, Clone)]
pub struct Cron {
    minutes: u64,
    hours: u64,
    dom: u64,
    months: u64,
    dow: u64,
    dom_star: bool,
    dow_star: bool,
    hour_star: bool,
    tz: Tz,
}

pub fn parse_tz(tz: &str) -> Result<Tz, String> {
    tz.parse::<Tz>().map_err(|_| format!("unknown IANA timezone '{tz}'"))
}

impl Cron {
    pub fn parse(expr: &str, tz: &str) -> Result<Cron, String> {
        let tz = parse_tz(tz)?;
        let expr = match expr.trim() {
            "@hourly" => "0 * * * *",
            "@daily" | "@midnight" => "0 0 * * *",
            "@weekly" => "0 0 * * 0",
            "@monthly" => "0 0 1 * *",
            "@yearly" | "@annually" => "0 0 1 1 *",
            e => e,
        };
        let f: Vec<&str> = expr.split_whitespace().collect();
        if f.len() != 5 {
            return Err(format!("cron needs 5 fields (min hour dom month dow), got {}", f.len()));
        }
        let mut dow = field(f[4], 0, 7, &DAYS)?;
        if dow & (1 << 7) != 0 {
            dow = (dow | 1) & !(1 << 7); // 7 is also Sunday
        }
        let cron = Cron {
            minutes: field(f[0], 0, 59, &[])?,
            hours: field(f[1], 0, 23, &[])?,
            dom: field(f[2], 1, 31, &[])?,
            months: field(f[3], 1, 12, &MONTHS)?,
            dow,
            dom_star: f[2].starts_with('*'),
            dow_star: f[4].starts_with('*'),
            hour_star: f[1].starts_with('*'),
            tz,
        };
        if cron.next_after(Utc::now()).is_none() {
            return Err(format!("cron '{expr}' never fires"));
        }
        Ok(cron)
    }

    fn day_matches(&self, t: &NaiveDateTime) -> bool {
        let dom = self.dom & (1 << t.day()) != 0;
        let dow = self.dow & (1 << t.weekday().num_days_from_sunday()) != 0;
        if self.dom_star || self.dow_star {
            dom && dow
        } else {
            dom || dow
        }
    }

    /// Smallest matching wall-clock minute >= `t`.
    fn next_naive(&self, mut t: NaiveDateTime, limit_year: i32) -> Option<NaiveDateTime> {
        while t.year() <= limit_year {
            let day_start = t.date().and_hms_opt(0, 0, 0)?;
            if self.months & (1 << t.month()) == 0 {
                let (y, m) = if t.month() == 12 {
                    (t.year() + 1, 1)
                } else {
                    (t.year(), t.month() + 1)
                };
                t = chrono::NaiveDate::from_ymd_opt(y, m, 1)?.and_hms_opt(0, 0, 0)?;
            } else if !self.day_matches(&t) {
                t = day_start + Duration::days(1);
            } else if self.hours & (1 << t.hour()) == 0 {
                t = day_start + Duration::hours(t.hour() as i64 + 1);
            } else if self.minutes & (1 << t.minute()) == 0 {
                t += Duration::minutes(1);
            } else {
                return Some(t);
            }
        }
        None
    }

    /// Earliest instant for a wall time, plus the second instant of a fold.
    fn resolve(&self, n: NaiveDateTime) -> (DateTime<Utc>, Option<DateTime<Utc>>) {
        match self.tz.from_local_datetime(&n) {
            LocalResult::Single(d) => (d.with_timezone(&Utc), None),
            LocalResult::Ambiguous(a, b) => {
                let (a, b) = (a.with_timezone(&Utc), b.with_timezone(&Utc));
                (a.min(b), Some(a.max(b)))
            }
            LocalResult::None => {
                let mut m = n;
                for _ in 0..24 * 60 {
                    m += Duration::minutes(1);
                    if let Some(d) = self.tz.from_local_datetime(&m).earliest() {
                        return (d.with_timezone(&Utc), None);
                    }
                }
                (self.tz.from_utc_datetime(&n).with_timezone(&Utc), None)
            }
        }
    }

    /// Next fire instant strictly after `after`.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local = after.with_timezone(&self.tz).naive_local();
        let local = local.with_second(0)?.with_nanosecond(0)?;
        // Start an hour early so the second pass of a fold is not skipped.
        let mut n = local - Duration::hours(1);
        let limit = local.year() + HORIZON_YEARS;
        let mut best: Option<DateTime<Utc>> = None;
        loop {
            let Some(cand) = self.next_naive(n, limit) else {
                return best;
            };
            let (early, late) = self.resolve(cand);
            if best.is_some_and(|b| early >= b) {
                return best;
            }
            let late = if self.hour_star { late } else { None };
            for c in [Some(early), late].into_iter().flatten() {
                if c > after && best.is_none_or(|b| c < b) {
                    best = Some(c);
                }
            }
            n = cand + Duration::minutes(1);
        }
    }
}

fn field(s: &str, lo: u32, hi: u32, names: &[&str]) -> Result<u64, String> {
    let bad = |p: &str| format!("bad cron field '{p}' (allowed {lo}-{hi})");
    let val = |v: &str| -> Result<u32, String> {
        match names.iter().position(|n| n.eq_ignore_ascii_case(v)) {
            Some(i) => Ok(i as u32 + lo),
            None => v.parse::<u32>().map_err(|_| bad(v)),
        }
    };
    let mut bits = 0u64;
    for part in s.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, st)) => (r, st.parse::<u32>().map_err(|_| bad(part))?),
            None => (part, 1),
        };
        let (a, b) = if range == "*" {
            (lo, hi)
        } else if let Some((a, b)) = range.split_once('-') {
            (val(a)?, val(b)?)
        } else {
            let v = val(range)?;
            (v, if part.contains('/') { hi } else { v })
        };
        if step == 0 || a > b || a < lo || b > hi {
            return Err(bad(part));
        }
        let mut v = a;
        while v <= b {
            bits |= 1 << v;
            v += step;
        }
    }
    Ok(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }
    fn next(expr: &str, tz: &str, after: &str) -> String {
        Cron::parse(expr, tz)
            .unwrap()
            .next_after(utc(after))
            .unwrap()
            .to_rfc3339()
    }

    #[test]
    fn weekday_morning_in_la() {
        // Friday 2026-10-16 10:00 PDT -> next weekday 09:17 is Monday 19th.
        assert_eq!(
            next("17 9 * * 1-5", "America/Los_Angeles", "2026-10-16T17:00:00Z"),
            "2026-10-19T16:17:00+00:00"
        );
        // Exactly at the slot: strictly after, so next day.
        assert_eq!(
            next("17 9 * * 1-5", "America/Los_Angeles", "2026-10-19T16:17:00Z"),
            "2026-10-20T16:17:00+00:00"
        );
    }

    #[test]
    fn same_wall_time_different_offsets_across_dst() {
        // 09:17 PST (UTC-8) after the November switch.
        assert_eq!(
            next("17 9 * * *", "America/Los_Angeles", "2026-11-01T18:00:00Z"),
            "2026-11-02T17:17:00+00:00"
        );
        assert_eq!(
            next("0 9 * * *", "Asia/Kolkata", "2026-10-19T00:00:00Z"),
            "2026-10-19T03:30:00+00:00"
        );
    }

    #[test]
    fn spring_forward_gap_fires_once_after_gap() {
        // 2026-03-08 02:00 PST jumps to 03:00 PDT; 02:30 does not exist.
        assert_eq!(
            next("30 2 * * *", "America/Los_Angeles", "2026-03-08T08:00:00Z"),
            "2026-03-08T10:00:00+00:00"
        );
        // And the day after it is back at 02:30 PDT.
        assert_eq!(
            next("30 2 * * *", "America/Los_Angeles", "2026-03-08T10:00:00Z"),
            "2026-03-09T09:30:00+00:00"
        );
        // Every-15 inside the gap collapses to one 03:00 fire, then 03:15.
        let c = Cron::parse("*/15 * * * *", "America/Los_Angeles").unwrap();
        let a = c.next_after(utc("2026-03-08T09:50:00Z")).unwrap();
        assert_eq!(a.to_rfc3339(), "2026-03-08T10:00:00+00:00");
        assert_eq!(c.next_after(a).unwrap().to_rfc3339(), "2026-03-08T10:15:00+00:00");
    }

    #[test]
    fn fall_back_fold() {
        // 2026-11-01 01:00-01:59 happens twice (PDT 08:xxZ then PST 09:xxZ).
        let fixed = Cron::parse("30 1 * * *", "America/Los_Angeles").unwrap();
        let first = fixed.next_after(utc("2026-11-01T07:00:00Z")).unwrap();
        assert_eq!(first.to_rfc3339(), "2026-11-01T08:30:00+00:00");
        assert_eq!(
            fixed.next_after(first).unwrap().to_rfc3339(),
            "2026-11-02T09:30:00+00:00"
        );
        // A wildcard-hour job keeps its cadence through both passes.
        let freq = Cron::parse("*/30 * * * *", "America/Los_Angeles").unwrap();
        let mut t = utc("2026-11-01T07:45:00Z");
        let mut seen = vec![];
        for _ in 0..5 {
            t = freq.next_after(t).unwrap();
            seen.push(t.format("%H:%M").to_string());
        }
        assert_eq!(seen, ["08:00", "08:30", "09:00", "09:30", "10:00"]);
    }

    #[test]
    fn dom_dow_or_semantics_and_names() {
        // "1st of month OR Monday": from Wed 2026-10-28 -> Sun 1 Nov.
        assert_eq!(
            next("0 12 1 * mon", "UTC", "2026-10-28T00:00:00Z"),
            "2026-11-01T12:00:00+00:00"
        );
        assert_eq!(
            next("0 0 * jan sun", "UTC", "2026-10-28T00:00:00Z"),
            "2027-01-03T00:00:00+00:00"
        );
        assert_eq!(
            next("0 0 * * 7", "UTC", "2026-10-28T00:00:00Z"),
            "2026-11-01T00:00:00+00:00"
        );
        assert_eq!(
            next("@hourly", "UTC", "2026-10-28T00:00:00Z"),
            "2026-10-28T01:00:00+00:00"
        );
        assert_eq!(
            next("0 0 29 2 *", "UTC", "2026-10-28T00:00:00Z"),
            "2028-02-29T00:00:00+00:00"
        );
    }

    #[test]
    fn rejects_bad_input() {
        for e in [
            "* * * *",
            "60 * * * *",
            "*/0 * * * *",
            "5-1 * * * *",
            "0 0 30 2 *",
            "x * * * *",
        ] {
            assert!(Cron::parse(e, "UTC").is_err(), "{e} should be rejected");
        }
        assert!(Cron::parse("0 0 * * *", "Mars/Olympus").is_err());
    }
}
