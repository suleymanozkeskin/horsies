use chrono::{DateTime, Datelike, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

use crate::core::config::{
    DailySchedule, HourlySchedule, IntervalSchedule, MonthlySchedule, SchedulePattern,
    WeeklySchedule,
};

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

    // Try this hour first.
    let candidate = local
        .date_naive()
        .and_hms_opt(local.hour(), target_minute, target_second)?;
    let candidate = tz.from_local_datetime(&candidate).earliest()?;

    if candidate > local {
        return Some(candidate.with_timezone(&Utc));
    }

    // Next hour.
    let next = local + chrono::Duration::hours(1);
    let candidate = next
        .date_naive()
        .and_hms_opt(next.hour(), target_minute, target_second)?;
    let candidate = tz.from_local_datetime(&candidate).earliest()?;
    Some(candidate.with_timezone(&Utc))
}

fn next_daily(daily: &DailySchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    let target = daily.time;

    // Try today.
    let candidate = local.date_naive().and_time(target);
    let candidate = tz.from_local_datetime(&candidate).earliest()?;

    if candidate > local {
        return Some(candidate.with_timezone(&Utc));
    }

    // Tomorrow.
    let tomorrow = local.date_naive() + chrono::Duration::days(1);
    let candidate = tomorrow.and_time(target);
    let candidate = tz.from_local_datetime(&candidate).earliest()?;
    Some(candidate.with_timezone(&Utc))
}

fn next_weekly(weekly: &WeeklySchedule, local: DateTime<Tz>, tz: Tz) -> Option<DateTime<Utc>> {
    if weekly.days.is_empty() {
        return None;
    }

    let target_time = weekly.time;

    // Check up to 8 days ahead (covers wrapping around the week).
    for offset in 0..8i64 {
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

    // Try this month.
    if let Some(candidate) =
        try_monthly_candidate(local.year(), local.month(), target_day, target_time, tz)
    {
        if candidate > local {
            return Some(candidate.with_timezone(&Utc));
        }
    }

    // Next month.
    let (year, month) = if local.month() == 12 {
        (local.year() + 1, 1)
    } else {
        (local.year(), local.month() + 1)
    };

    try_monthly_candidate(year, month, target_day, target_time, tz).map(|c| c.with_timezone(&Utc))
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
}
