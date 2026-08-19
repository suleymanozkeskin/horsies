use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

use crate::core::config::schedule::{expand_enum_field, expand_numeric_field};
use crate::core::config::{
    CronSchedule, DailySchedule, DaySelector, HourlySchedule, IntervalSchedule, MonthlySchedule,
    SchedulePattern, WeeklySchedule,
};

/// One 400-year Gregorian cycle, as an exclusive day-offset bound. The calendar
/// repeats identically every 146097 days, so any satisfiable cron schedule
/// resolves within this many forward steps.
const MAX_CRON_SCAN_DAYS: i64 = 146097;

/// Forward-scan bounds for the calendar patterns. Each is far larger than the
/// worst case (a DST gap removes at most one candidate), so a satisfiable
/// pattern always resolves while a genuinely unsatisfiable one still terminates.
const HOURLY_SCAN_HOURS: i64 = 48;
const DAILY_SCAN_DAYS: i64 = 8;
const WEEKLY_SCAN_DAYS: i64 = 15;
const MONTHLY_SCAN_MONTHS: u32 = 24;

/// Calculate the next run time for a schedule pattern after a given reference time.
///
/// Panics if `timezone` is not a valid IANA timezone name. Callers must
/// validate timezones upfront via `ScheduleConfig::validate()`.
pub fn next_run_at(
    pattern: &SchedulePattern,
    after: DateTime<Utc>,
    timezone: &str,
) -> Option<DateTime<Utc>> {
    let tz: Tz = timezone.parse().expect("timezone validated at config time");
    let local = after.with_timezone(&tz);

    match pattern {
        SchedulePattern::Interval(interval) => next_interval(interval, after),
        SchedulePattern::Hourly(hourly) => next_hourly(hourly, local, tz),
        SchedulePattern::Daily(daily) => next_daily(daily, local, tz),
        SchedulePattern::Weekly(weekly) => next_weekly(weekly, local, tz),
        SchedulePattern::Monthly(monthly) => next_monthly(monthly, local, tz),
        SchedulePattern::Cron(cron) => next_cron(cron, local, tz),
    }
}

fn next_interval(interval: &IntervalSchedule, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let secs = interval.total_seconds();
    if secs == 0 {
        return None;
    }
    Some(after + chrono::Duration::seconds(secs as i64))
}

fn next_hourly(hourly: &HourlySchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    let target_minute = hourly.minute;
    let target_second = hourly.second;

    // Scan forward hour by hour in WALL-CLOCK terms (HH:mm:ss -> next HH:mm:ss),
    // skipping any candidate that lands in a DST gap (`earliest()` is `None`).
    // Incrementing the naive wall-clock time keeps the target minute aligned,
    // unlike advancing the instant, which would distort it across a transition.
    let mut naive = local
        .date_naive()
        .and_hms_opt(local.hour(), target_minute, target_second)?;
    for _ in 0..HOURLY_SCAN_HOURS {
        if let Some(candidate) = tz.from_local_datetime(&naive).earliest() {
            if candidate > local {
                return Some(candidate.with_timezone(&Utc));
            }
        }
        naive += chrono::Duration::hours(1);
    }
    None
}

fn next_daily(daily: &DailySchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    let target = daily.time;

    // Scan forward day by day, skipping days whose target wall-clock time falls
    // in a DST gap (`earliest()` is `None`), until a candidate strictly after
    // `local` resolves. A satisfiable daily pattern resolves within a day or two.
    for day_offset in 0..DAILY_SCAN_DAYS {
        let day = local.date_naive() + chrono::Duration::days(day_offset);
        let naive = day.and_time(target);
        let Some(candidate) = tz.from_local_datetime(&naive).earliest() else {
            continue;
        };
        if candidate > local {
            return Some(candidate.with_timezone(&Utc));
        }
    }
    None
}

fn next_weekly(weekly: &WeeklySchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    if weekly.days.is_empty() {
        return None;
    }

    let target_time = weekly.time;

    // Scan forward day by day, skipping any target that falls in a DST gap
    // (`earliest()` is `None`). The window spans more than two weeks so a
    // spring-forward Sunday still resolves to the following week's occurrence
    // (parity with the widened hourly/daily/monthly scans; C1).
    for offset in 0..WEEKLY_SCAN_DAYS {
        let day = local.date_naive() + chrono::Duration::days(offset);
        let weekday = day.weekday();

        if !weekly.days.iter().any(|d| to_chrono_weekday(d) == weekday) {
            continue;
        }

        let candidate = day.and_time(target_time);
        let Some(candidate) = tz.from_local_datetime(&candidate).earliest() else {
            continue;
        };

        if candidate > local {
            return Some(candidate.with_timezone(&Utc));
        }
    }

    None
}

fn next_monthly(monthly: &MonthlySchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    let target_day = monthly.day;
    let target_time = monthly.time;

    // Scan forward month by month, skipping a month whose target wall-clock time
    // falls in a DST gap (`try_monthly_candidate` is `None`), until a candidate
    // strictly after `local` resolves. A satisfiable monthly pattern resolves
    // within a month or two.
    let mut year = local.year();
    let mut month = local.month();
    for _ in 0..MONTHLY_SCAN_MONTHS {
        if let Some(candidate) = try_monthly_candidate(year, month, target_day, target_time, tz) {
            if candidate > local {
                return Some(candidate.with_timezone(&Utc));
            }
        }
        (year, month) = if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        };
    }
    None
}

fn try_monthly_candidate(
    year: i32,
    month: u32,
    day: u32,
    time: NaiveTime,
    tz: Tz,
) -> Option<DateTime<Tz>> {
    // Clamp day to the last day of the month.
    let days_in_month = days_in_month(year, month);
    let actual_day = day.min(days_in_month);
    let date = chrono::NaiveDate::from_ymd_opt(year, month, actual_day)?;
    let datetime = date.and_time(time);
    tz.from_local_datetime(&datetime).earliest()
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_of_next =
        chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1).expect("valid date");
    let first_of_current = chrono::NaiveDate::from_ymd_opt(year, month, 1).expect("valid date");
    (first_of_next - first_of_current).num_days() as u32
}

/// Build a per-date predicate from a [`DaySelector`].
///
/// Term sets are expanded once here (not per candidate day). Weekday ordinals
/// use Monday=0..Sunday=6 (`num_days_from_monday`), matching the model layer's
/// `CronOrdinal`; day-of-month uses the calendar day.
fn build_day_matcher(day: &DaySelector) -> Box<dyn Fn(NaiveDate) -> bool> {
    match day {
        DaySelector::EveryDay => Box::new(|_| true),
        DaySelector::ByMonthDay { day_of_month } => {
            let dom = expand_numeric_field(day_of_month, 1, 31);
            Box::new(move |date| dom.contains(&(date.day() as i64)))
        }
        DaySelector::ByWeekday { day_of_week } => {
            let dow = expand_enum_field(day_of_week, 0, 6);
            Box::new(move |date| dow.contains(&(date.weekday().num_days_from_monday() as i64)))
        }
        DaySelector::EitherDay {
            day_of_month,
            day_of_week,
        } => {
            let dom = expand_numeric_field(day_of_month, 1, 31);
            let dow = expand_enum_field(day_of_week, 0, 6);
            Box::new(move |date| {
                dom.contains(&(date.day() as i64))
                    || dow.contains(&(date.weekday().num_days_from_monday() as i64))
            })
        }
        DaySelector::BothDays {
            day_of_month,
            day_of_week,
        } => {
            let dom = expand_numeric_field(day_of_month, 1, 31);
            let dow = expand_enum_field(day_of_week, 0, 6);
            Box::new(move |date| {
                dom.contains(&(date.day() as i64))
                    && dow.contains(&(date.weekday().num_days_from_monday() as i64))
            })
        }
    }
}

/// Calculate next run for a cron-style schedule.
///
/// Scans forward day by day (bounded by one Gregorian cycle), resolving each
/// candidate wall-clock time via `from_local_datetime(..).earliest()` so DST
/// gaps (nonexistent local times) are skipped. Fires at second :00.
fn next_cron(cron: &CronSchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    let mut minutes: Vec<i64> = expand_numeric_field(&cron.minute, 0, 59)
        .into_iter()
        .collect();
    minutes.sort_unstable();
    let mut hours: Vec<i64> = expand_numeric_field(&cron.hour, 0, 23)
        .into_iter()
        .collect();
    hours.sort_unstable();
    let month_set = expand_enum_field(&cron.month, 1, 12);
    let day_matches = build_day_matcher(&cron.day);

    let base_date = local.date_naive();
    for day_offset in 0..MAX_CRON_SCAN_DAYS {
        let candidate_date = base_date + chrono::Duration::days(day_offset);
        if !month_set.contains(&(candidate_date.month() as i64)) {
            continue;
        }
        if !day_matches(candidate_date) {
            continue;
        }
        for &hour in &hours {
            for &minute in &minutes {
                let Some(naive) = candidate_date.and_hms_opt(hour as u32, minute as u32, 0) else {
                    continue;
                };
                let Some(candidate) = tz.from_local_datetime(&naive).earliest() else {
                    continue;
                };
                if candidate <= local {
                    continue;
                }
                return Some(candidate.with_timezone(&Utc));
            }
        }
    }

    None
}

/// Determine whether a schedule should fire at the given check time.
///
/// This mirrors Python's `should_run_now(next_run_at, check_time)`:
/// - Returns `true` if `next_run_at` is `None` (first run — should execute).
/// - Returns `true` if `next_run_at <= check_time` (schedule is due).
/// - Returns `false` otherwise (schedule is in the future).
pub fn should_run_now(next_run_at: Option<DateTime<Utc>>, check_time: DateTime<Utc>) -> bool {
    match next_run_at {
        None => true,
        Some(next) => next <= check_time,
    }
}

fn to_chrono_weekday(day: &crate::core::config::Weekday) -> chrono::Weekday {
    match day {
        crate::core::config::Weekday::Monday => chrono::Weekday::Mon,
        crate::core::config::Weekday::Tuesday => chrono::Weekday::Tue,
        crate::core::config::Weekday::Wednesday => chrono::Weekday::Wed,
        crate::core::config::Weekday::Thursday => chrono::Weekday::Thu,
        crate::core::config::Weekday::Friday => chrono::Weekday::Fri,
        crate::core::config::Weekday::Saturday => chrono::Weekday::Sat,
        crate::core::config::Weekday::Sunday => chrono::Weekday::Sun,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, min, sec)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn interval_30s() {
        let pattern = SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(30),
            minutes: None,
            hours: None,
            days: None,
        });
        let after = utc(2025, 1, 1, 12, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 1, 12, 0, 30));
    }

    #[test]
    fn interval_zero_returns_none() {
        let pattern = SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(0),
            minutes: None,
            hours: None,
            days: None,
        });
        let after = utc(2025, 1, 1, 12, 0, 0);
        assert!(next_run_at(&pattern, after, "UTC").is_none());
    }

    #[test]
    fn hourly_future_minute_same_hour() {
        let pattern = SchedulePattern::Hourly(HourlySchedule {
            minute: 30,
            second: 0,
        });
        let after = utc(2025, 1, 1, 12, 15, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 1, 12, 30, 0));
    }

    #[test]
    fn hourly_past_minute_goes_to_next_hour() {
        let pattern = SchedulePattern::Hourly(HourlySchedule {
            minute: 10,
            second: 0,
        });
        let after = utc(2025, 1, 1, 12, 30, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 1, 13, 10, 0));
    }

    #[test]
    fn daily_future_time_same_day() {
        let pattern = SchedulePattern::Daily(DailySchedule {
            time: NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
        });
        let after = utc(2025, 1, 1, 12, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 1, 18, 0, 0));
    }

    #[test]
    fn daily_past_time_goes_to_tomorrow() {
        let pattern = SchedulePattern::Daily(DailySchedule {
            time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
        });
        let after = utc(2025, 1, 1, 12, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 2, 8, 0, 0));
    }

    #[test]
    fn weekly_correct_day() {
        let pattern = SchedulePattern::Weekly(WeeklySchedule {
            days: vec![crate::core::config::Weekday::Wednesday],
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        });
        // 2025-01-06 is Monday
        let after = utc(2025, 1, 6, 12, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        // Next Wednesday is Jan 8
        assert_eq!(next, utc(2025, 1, 8, 9, 0, 0));
    }

    #[test]
    fn monthly_future_day() {
        let pattern = SchedulePattern::Monthly(MonthlySchedule {
            day: 15,
            time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        });
        let after = utc(2025, 1, 10, 12, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 15, 10, 0, 0));
    }

    #[test]
    fn monthly_past_day_goes_to_next_month() {
        let pattern = SchedulePattern::Monthly(MonthlySchedule {
            day: 5,
            time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        });
        let after = utc(2025, 1, 10, 12, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 2, 5, 10, 0, 0));
    }

    #[test]
    fn monthly_day_31_in_february_clamps() {
        let pattern = SchedulePattern::Monthly(MonthlySchedule {
            day: 31,
            time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        });
        // After Jan 31 → Feb should clamp to 28
        let after = utc(2025, 2, 1, 0, 0, 0);
        let next = next_run_at(&pattern, after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 2, 28, 10, 0, 0));
    }

    #[test]
    fn days_in_month_february_non_leap() {
        assert_eq!(days_in_month(2025, 2), 28);
    }

    #[test]
    fn days_in_month_february_leap() {
        assert_eq!(days_in_month(2024, 2), 29);
    }

    #[test]
    fn days_in_month_december() {
        assert_eq!(days_in_month(2025, 12), 31);
    }

    // --- should_run_now tests ---

    #[test]
    fn should_run_now_none_returns_true() {
        // First run (next_run_at is None) should always fire.
        let check_time = utc(2025, 6, 15, 12, 0, 0);
        assert!(should_run_now(None, check_time));
    }

    #[test]
    fn should_run_now_past_returns_true() {
        // next_run_at is in the past relative to check_time.
        let next = utc(2025, 6, 15, 11, 0, 0);
        let check = utc(2025, 6, 15, 12, 0, 0);
        assert!(should_run_now(Some(next), check));
    }

    #[test]
    fn should_run_now_equal_returns_true() {
        // next_run_at is exactly check_time — should fire.
        let t = utc(2025, 6, 15, 12, 0, 0);
        assert!(should_run_now(Some(t), t));
    }

    #[test]
    fn should_run_now_future_returns_false() {
        // next_run_at is in the future — should NOT fire.
        let next = utc(2025, 6, 15, 13, 0, 0);
        let check = utc(2025, 6, 15, 12, 0, 0);
        assert!(!should_run_now(Some(next), check));
    }

    // --- cron matcher tests (ported from Python TestCalculateNextRunCron) ---

    use crate::core::config::{
        CronEnumTerm, CronNumericTerm, CronSchedule, DaySelector, Month, Weekday,
    };

    fn num_values(values: Vec<i64>) -> CronNumericTerm {
        CronNumericTerm::Values { values }
    }

    /// A cron schedule with every-minute/every-hour/every-month and the given
    /// day selector (mirrors Python's `_every_minute_hour`).
    fn every_minute_hour(day: DaySelector) -> CronSchedule {
        CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day,
        }
    }

    #[test]
    fn cron_wall_clock_alignment_every_four_hours() {
        let pattern = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![CronNumericTerm::Step { step: 4 }],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        });
        let next = next_run_at(&pattern, utc(2026, 5, 31, 9, 13, 0), "UTC").unwrap();
        assert_eq!(next, utc(2026, 5, 31, 12, 0, 0));
    }

    #[test]
    fn cron_minute_step_within_hour() {
        let pattern = SchedulePattern::Cron(CronSchedule {
            minute: vec![CronNumericTerm::Step { step: 15 }],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        });
        let next = next_run_at(&pattern, utc(2026, 5, 31, 9, 7, 0), "UTC").unwrap();
        assert_eq!(next, utc(2026, 5, 31, 9, 15, 0));
    }

    #[test]
    fn cron_month_restriction_skips_to_next_year() {
        let pattern = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: vec![CronEnumTerm::EnumValues {
                values: vec![Month::February],
            }],
            day: DaySelector::EveryDay,
        });
        let next = next_run_at(&pattern, utc(2026, 5, 31, 9, 0, 0), "UTC").unwrap();
        assert_eq!(next, utc(2027, 2, 1, 0, 0, 0));
    }

    #[test]
    fn cron_either_day_takes_earliest() {
        let pattern = SchedulePattern::Cron(every_minute_hour(DaySelector::EitherDay {
            day_of_month: vec![num_values(vec![13])],
            day_of_week: vec![CronEnumTerm::EnumValues {
                values: vec![Weekday::Friday],
            }],
        }));
        // base is Sunday 2026-05-31; next Friday is 2026-06-05 (before the 13th).
        let next = next_run_at(&pattern, utc(2026, 5, 31, 12, 0, 0), "UTC").unwrap();
        assert_eq!(
            next.date_naive(),
            NaiveDate::from_ymd_opt(2026, 6, 5).unwrap()
        );
    }

    #[test]
    fn cron_both_days_requires_friday_the_thirteenth() {
        let pattern = SchedulePattern::Cron(every_minute_hour(DaySelector::BothDays {
            day_of_month: vec![num_values(vec![13])],
            day_of_week: vec![CronEnumTerm::EnumValues {
                values: vec![Weekday::Friday],
            }],
        }));
        let next = next_run_at(&pattern, utc(2026, 5, 31, 12, 0, 0), "UTC").unwrap();
        assert_eq!(
            next.date_naive(),
            NaiveDate::from_ymd_opt(2026, 11, 13).unwrap()
        );
    }

    #[test]
    fn cron_leap_day_resolves_to_next_leap_year() {
        let pattern = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: vec![CronEnumTerm::EnumValues {
                values: vec![Month::February],
            }],
            day: DaySelector::ByMonthDay {
                day_of_month: vec![num_values(vec![29])],
            },
        });
        let next = next_run_at(&pattern, utc(2026, 5, 31, 9, 0, 0), "UTC").unwrap();
        assert_eq!(next, utc(2028, 2, 29, 0, 0, 0));
    }

    #[test]
    fn cron_result_is_utc_aware_for_non_utc_tz() {
        let pattern = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![12])],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        });
        // 12:00 America/New_York on 2026-05-31 (EDT, UTC-4) == 16:00 UTC.
        let next = next_run_at(&pattern, utc(2026, 5, 31, 9, 0, 0), "America/New_York").unwrap();
        assert_eq!(next, utc(2026, 5, 31, 16, 0, 0));
    }

    #[test]
    fn cron_dst_spring_forward_gap_skipped() {
        // A 02:30 cron in America/New_York: 2026-03-08 02:30 is the nonexistent
        // spring-forward instant, so the matcher skips it and fires on 03-09 at
        // 02:30 EDT (== 06:30 UTC). `next_daily` now scans forward and skips the
        // gap identically (C5), so the Python equivalence cron_run == daily_run
        // holds in this port too.
        let cron = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![30])],
            hour: vec![num_values(vec![2])],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        });
        let daily = SchedulePattern::Daily(DailySchedule {
            time: NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
        });
        let base = utc(2026, 3, 7, 12, 0, 0);
        let cron_run = next_run_at(&cron, base, "America/New_York").unwrap();
        let daily_run = next_run_at(&daily, base, "America/New_York").unwrap();
        assert_eq!(cron_run, utc(2026, 3, 9, 6, 30, 0));
        assert_eq!(daily_run, cron_run);
    }

    #[test]
    fn daily_dst_spring_forward_gap_not_disabled() {
        // C5: a daily 02:30 schedule in America/New_York whose target lands in
        // the 2026-03-08 spring-forward gap must not return `None`; it fires the
        // next day at 02:30 EDT (== 06:30 UTC) instead of permanently stopping.
        let pattern = SchedulePattern::Daily(DailySchedule {
            time: NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
        });
        // Reference just before the gap day's target time.
        let after = utc(2026, 3, 8, 6, 0, 0); // 01:00 EST
        let next = next_run_at(&pattern, after, "America/New_York").unwrap();
        assert_eq!(next, utc(2026, 3, 9, 6, 30, 0));
    }

    #[test]
    fn monthly_dst_spring_forward_gap_not_disabled() {
        // C6: a monthly schedule whose target day/time lands in a DST gap must
        // not return `None`. day=8 02:30 America/New_York: 2026-03-08 02:30 is
        // the gap, so it skips March and fires 2026-04-08 02:30 EDT (06:30 UTC).
        let pattern = SchedulePattern::Monthly(MonthlySchedule {
            day: 8,
            time: NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
        });
        // Reference after Feb 8 so "this month" (Feb) is past.
        let after = utc(2026, 2, 9, 0, 0, 0);
        let next = next_run_at(&pattern, after, "America/New_York").unwrap();
        assert_eq!(next, utc(2026, 4, 8, 6, 30, 0));
    }

    #[test]
    fn weekly_dst_spring_forward_gap_not_disabled() {
        // C1: a weekly Sunday 02:30 schedule in America/New_York whose next
        // occurrence lands in the 2026-03-08 spring-forward gap must not return
        // `None`. From the fired slot Sun 2026-03-01 02:30 EST, offset 0 is the
        // fired slot (rejected as non-strict), offset 7 is the gap Sunday
        // (skipped), and offset 14 (Sun 2026-03-15 02:30 EDT == 06:30 UTC) must
        // resolve. An 8-day scan returned `None` and stopped the schedule forever.
        let pattern = SchedulePattern::Weekly(WeeklySchedule {
            days: vec![crate::core::config::Weekday::Sunday],
            time: NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
        });
        // Fired slot Sun 2026-03-01 02:30 EST (== 07:30 UTC).
        let after = utc(2026, 3, 1, 7, 30, 0);
        let next = next_run_at(&pattern, after, "America/New_York").unwrap();
        assert_eq!(next, utc(2026, 3, 15, 6, 30, 0));
    }

    #[test]
    fn hourly_dst_spring_forward_gap_not_disabled() {
        // C11: a sub-hour DST zone (Australia/Lord_Howe, +30min spring-forward on
        // 2025-10-05, local 02:00:00-02:29:59 nonexistent). An hourly schedule at
        // minute 15 must skip the nonexistent 02:15 and fire at 03:15, not stop.
        let pattern = SchedulePattern::Hourly(HourlySchedule {
            minute: 15,
            second: 0,
        });
        // 01:20 Lord Howe (pre-transition, +10:30) == 2025-10-04 14:50 UTC.
        let after = utc(2025, 10, 4, 14, 50, 0);
        let next = next_run_at(&pattern, after, "Australia/Lord_Howe").unwrap();
        // 03:15 Lord Howe (post-transition, +11:00) == 2025-10-04 16:15 UTC.
        assert_eq!(next, utc(2025, 10, 4, 16, 15, 0));
    }

    // --- equivalence with calendar patterns at second=0 ---

    fn equiv_base() -> DateTime<Utc> {
        utc(2026, 5, 31, 9, 13, 0)
    }

    #[test]
    fn cron_matches_hourly() {
        let cron = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![30])],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        });
        let other = SchedulePattern::Hourly(HourlySchedule {
            minute: 30,
            second: 0,
        });
        assert_eq!(
            next_run_at(&cron, equiv_base(), "UTC"),
            next_run_at(&other, equiv_base(), "UTC"),
        );
    }

    #[test]
    fn cron_matches_daily() {
        let cron = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![3])],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        });
        let other = SchedulePattern::Daily(DailySchedule {
            time: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
        });
        assert_eq!(
            next_run_at(&cron, equiv_base(), "UTC"),
            next_run_at(&other, equiv_base(), "UTC"),
        );
    }

    #[test]
    fn cron_matches_weekly() {
        let cron = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![9])],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::ByWeekday {
                day_of_week: vec![CronEnumTerm::EnumValues {
                    values: vec![Weekday::Monday, Weekday::Friday],
                }],
            },
        });
        let other = SchedulePattern::Weekly(WeeklySchedule {
            days: vec![Weekday::Monday, Weekday::Friday],
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        });
        assert_eq!(
            next_run_at(&cron, equiv_base(), "UTC"),
            next_run_at(&other, equiv_base(), "UTC"),
        );
    }

    #[test]
    fn cron_matches_monthly() {
        // Day 15 exists in every month, so cron (no clamp) and monthly agree.
        let cron = SchedulePattern::Cron(CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::ByMonthDay {
                day_of_month: vec![num_values(vec![15])],
            },
        });
        let other = SchedulePattern::Monthly(MonthlySchedule {
            day: 15,
            time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        });
        assert_eq!(
            next_run_at(&cron, equiv_base(), "UTC"),
            next_run_at(&other, equiv_base(), "UTC"),
        );
    }
}
