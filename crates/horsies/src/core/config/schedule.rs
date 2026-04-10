use std::collections::HashSet;

use chrono::NaiveTime;
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

/// Days of the week.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

/// Run task every N seconds/minutes/hours/days.
///
/// At least one time unit must be specified.
/// Total interval is the sum of all specified units.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntervalSchedule {
    /// Seconds component (1–86400).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds: Option<u32>,
    /// Minutes component (1–1440).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minutes: Option<u32>,
    /// Hours component (1–168).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hours: Option<u32>,
    /// Days component (1–365).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,
}

impl Default for IntervalSchedule {
    fn default() -> Self {
        Self {
            seconds: None,
            minutes: None,
            hours: None,
            days: None,
        }
    }
}

impl IntervalSchedule {
    /// Calculate total interval in seconds.
    pub fn total_seconds(&self) -> u64 {
        let mut total: u64 = 0;
        if let Some(s) = self.seconds {
            total += s as u64;
        }
        if let Some(m) = self.minutes {
            total += m as u64 * 60;
        }
        if let Some(h) = self.hours {
            total += h as u64 * 3600;
        }
        if let Some(d) = self.days {
            total += d as u64 * 86400;
        }
        total
    }

    /// Validate that at least one time unit is specified and all values
    /// are within acceptable upper bounds.
    ///
    /// Upper bounds match the Python implementation:
    /// - seconds: 1–86400
    /// - minutes: 1–1440
    /// - hours: 1–168
    /// - days: 1–365
    pub fn validate(&self) -> Result<(), String> {
        if self.seconds.is_none()
            && self.minutes.is_none()
            && self.hours.is_none()
            && self.days.is_none()
        {
            return Err("IntervalSchedule requires at least one time unit".to_owned());
        }

        if let Some(s) = self.seconds {
            if s == 0 || s > 86400 {
                return Err(format!(
                    "IntervalSchedule seconds must be between 1 and 86400, got {}",
                    s,
                ));
            }
        }
        if let Some(m) = self.minutes {
            if m == 0 || m > 1440 {
                return Err(format!(
                    "IntervalSchedule minutes must be between 1 and 1440, got {}",
                    m,
                ));
            }
        }
        if let Some(h) = self.hours {
            if h == 0 || h > 168 {
                return Err(format!(
                    "IntervalSchedule hours must be between 1 and 168, got {}",
                    h,
                ));
            }
        }
        if let Some(d) = self.days {
            if d == 0 || d > 365 {
                return Err(format!(
                    "IntervalSchedule days must be between 1 and 365, got {}",
                    d,
                ));
            }
        }

        Ok(())
    }
}

/// Run task every hour at a specific minute and second.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlySchedule {
    /// Minute of the hour (0–59).
    pub minute: u32,
    /// Second of the minute (0–59).
    #[serde(default)]
    pub second: u32,
}

impl HourlySchedule {
    /// Validate that minute is 0–59 and second is 0–59.
    ///
    /// Matches Python's `Field(ge=0, le=59)` constraints.
    pub fn validate(&self) -> Result<(), String> {
        if self.minute > 59 {
            return Err(format!(
                "HourlySchedule minute must be between 0 and 59, got {}",
                self.minute,
            ));
        }
        if self.second > 59 {
            return Err(format!(
                "HourlySchedule second must be between 0 and 59, got {}",
                self.second,
            ));
        }
        Ok(())
    }
}

/// Run task every day at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailySchedule {
    /// Time of day to run (HH:MM:SS).
    pub time: NaiveTime,
}

/// Run task on specific days of the week at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklySchedule {
    /// Days of week to run (at least one).
    pub days: Vec<Weekday>,
    /// Time of day to run.
    pub time: NaiveTime,
}

impl WeeklySchedule {
    /// Validate that the days list is non-empty and contains no duplicates.
    pub fn validate(&self) -> Result<(), String> {
        if self.days.is_empty() {
            return Err("WeeklySchedule requires at least one day".to_owned());
        }
        let unique: HashSet<&Weekday> = self.days.iter().collect();
        if unique.len() != self.days.len() {
            return Err(format!(
                "WeeklySchedule has duplicate days: {:?}",
                self.days,
            ));
        }
        Ok(())
    }
}

/// Run task on a specific day of the month at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlySchedule {
    /// Day of month (1–31).
    pub day: u32,
    /// Time of day to run.
    pub time: NaiveTime,
}

impl MonthlySchedule {
    /// Validate that day is 1–31.
    ///
    /// Matches Python's `Field(ge=1, le=31)` constraint.
    pub fn validate(&self) -> Result<(), String> {
        if self.day == 0 || self.day > 31 {
            return Err(format!(
                "MonthlySchedule day must be between 1 and 31, got {}",
                self.day,
            ));
        }
        Ok(())
    }
}

/// Union of all schedule pattern types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchedulePattern {
    Interval(IntervalSchedule),
    Hourly(HourlySchedule),
    Daily(DailySchedule),
    Weekly(WeeklySchedule),
    Monthly(MonthlySchedule),
}

/// Definition of a scheduled task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSchedule {
    /// Unique schedule identifier.
    pub name: String,
    /// Task to execute (must be registered).
    pub task_name: String,
    /// Schedule pattern defining when the task runs.
    pub pattern: SchedulePattern,
    /// Task arguments (serialized).
    #[serde(default)]
    pub args: serde_json::Value,
    /// Task keyword arguments (serialized).
    #[serde(default)]
    pub kwargs: serde_json::Value,
    /// Target queue name (None = default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    /// Whether this schedule is active.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Timezone for schedule evaluation.
    #[serde(default = "default_timezone")]
    pub timezone: String,
    /// Execute missed runs if scheduler was down.
    #[serde(default)]
    pub catch_up_missed: bool,

    /// Maximum runs to enqueue per scheduler tick when catch_up_missed is true.
    #[serde(default = "default_max_catch_up_runs")]
    pub max_catch_up_runs: u32,
}

impl TaskSchedule {
    /// Create a scheduled task with the required fields and library defaults
    /// for everything else.
    pub fn new(
        name: impl Into<String>,
        task_name: impl Into<String>,
        pattern: SchedulePattern,
    ) -> Self {
        Self {
            name: name.into(),
            task_name: task_name.into(),
            pattern,
            args: serde_json::Value::Null,
            kwargs: serde_json::Value::Null,
            queue_name: None,
            enabled: default_true(),
            timezone: default_timezone(),
            catch_up_missed: false,
            max_catch_up_runs: default_max_catch_up_runs(),
        }
    }

    pub fn args(mut self, args: serde_json::Value) -> Self {
        self.args = args;
        self
    }

    pub fn kwargs(mut self, kwargs: serde_json::Value) -> Self {
        self.kwargs = kwargs;
        self
    }

    pub fn queue(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = Some(queue_name.into());
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn timezone(mut self, timezone: impl Into<String>) -> Self {
        self.timezone = timezone.into();
        self
    }

    pub fn catch_up_missed(mut self, catch_up_missed: bool) -> Self {
        self.catch_up_missed = catch_up_missed;
        self
    }

    pub fn max_catch_up_runs(mut self, max_catch_up_runs: u32) -> Self {
        self.max_catch_up_runs = max_catch_up_runs;
        self
    }
}

fn default_max_catch_up_runs() -> u32 {
    100
}

fn default_true() -> bool {
    true
}
fn default_timezone() -> String {
    "UTC".to_owned()
}

/// Scheduler configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConfig {
    /// Master scheduler enable switch.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// List of scheduled tasks.
    #[serde(default)]
    pub schedules: Vec<TaskSchedule>,
    /// Scheduler check interval in seconds (1–60).
    #[serde(default = "default_check_interval")]
    pub check_interval_seconds: u32,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            schedules: Vec::new(),
            check_interval_seconds: default_check_interval(),
        }
    }
}

impl ScheduleConfig {
    /// Create a `ScheduleConfig` with the provided schedules and default
    /// scheduler settings.
    pub fn new(schedules: Vec<TaskSchedule>) -> Self {
        Self {
            schedules,
            ..Self::default()
        }
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn check_interval_seconds(mut self, seconds: u32) -> Self {
        self.check_interval_seconds = seconds;
        self
    }

    /// Validate the schedule configuration.
    ///
    /// Checks:
    /// - `check_interval_seconds` is between 1 and 60 (matching Python `Field(ge=1, le=60)`)
    /// - All schedule names are unique
    /// - Individual schedule patterns are valid (interval bounds, hourly/monthly ranges,
    ///   weekly day uniqueness)
    pub fn validate(&self) -> Result<(), String> {
        // check_interval_seconds bounds (Python: ge=1, le=60)
        if self.check_interval_seconds == 0 || self.check_interval_seconds > 60 {
            return Err(format!(
                "check_interval_seconds must be between 1 and 60, got {}",
                self.check_interval_seconds,
            ));
        }

        // Unique schedule names
        let mut seen_names = HashSet::with_capacity(self.schedules.len());
        for schedule in &self.schedules {
            if !seen_names.insert(&schedule.name) {
                return Err(format!(
                    "duplicate schedule name '{}' — each schedule must have a unique name",
                    schedule.name,
                ));
            }
        }

        // Validate individual schedule patterns and timezones
        for schedule in &self.schedules {
            match &schedule.pattern {
                SchedulePattern::Interval(interval) => interval.validate()?,
                SchedulePattern::Hourly(hourly) => hourly.validate()?,
                SchedulePattern::Weekly(weekly) => weekly.validate()?,
                SchedulePattern::Monthly(monthly) => monthly.validate()?,
                SchedulePattern::Daily(_) => {} // NaiveTime handles its own bounds
            }

            if schedule.timezone.parse::<Tz>().is_err() {
                return Err(format!(
                    "schedule '{}': invalid timezone '{}' — \
                     use an IANA timezone name (e.g. 'America/New_York', 'UTC')",
                    schedule.name, schedule.timezone,
                ));
            }
        }

        Ok(())
    }
}

fn default_check_interval() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_total_seconds() {
        let schedule = IntervalSchedule {
            hours: Some(1),
            minutes: Some(30),
            seconds: None,
            days: None,
        };
        assert_eq!(schedule.total_seconds(), 5400);
    }

    #[test]
    fn interval_default_is_empty() {
        let schedule = IntervalSchedule::default();
        assert!(schedule.seconds.is_none());
        assert!(schedule.minutes.is_none());
        assert!(schedule.hours.is_none());
        assert!(schedule.days.is_none());
    }

    #[test]
    fn interval_requires_at_least_one_unit() {
        let empty = IntervalSchedule {
            seconds: None,
            minutes: None,
            hours: None,
            days: None,
        };
        assert!(empty.validate().is_err());
    }

    #[test]
    fn interval_upper_bound_seconds() {
        let schedule = IntervalSchedule {
            seconds: Some(86401),
            minutes: None,
            hours: None,
            days: None,
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("seconds"));
        assert!(err.contains("86401"));
    }

    #[test]
    fn interval_upper_bound_minutes() {
        let schedule = IntervalSchedule {
            seconds: None,
            minutes: Some(1441),
            hours: None,
            days: None,
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("minutes"));
    }

    #[test]
    fn interval_upper_bound_hours() {
        let schedule = IntervalSchedule {
            seconds: None,
            minutes: None,
            hours: Some(169),
            days: None,
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("hours"));
    }

    #[test]
    fn interval_upper_bound_days() {
        let schedule = IntervalSchedule {
            seconds: None,
            minutes: None,
            hours: None,
            days: Some(366),
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("days"));
    }

    #[test]
    fn interval_zero_value_rejected() {
        let schedule = IntervalSchedule {
            seconds: Some(0),
            minutes: None,
            hours: None,
            days: None,
        };
        assert!(schedule.validate().is_err());
    }

    #[test]
    fn interval_valid_bounds_accepted() {
        let schedule = IntervalSchedule {
            seconds: Some(86400),
            minutes: Some(1440),
            hours: Some(168),
            days: Some(365),
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn weekly_validate_no_duplicates() {
        let schedule = WeeklySchedule {
            days: vec![Weekday::Monday, Weekday::Friday],
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn weekly_validate_rejects_duplicates() {
        let schedule = WeeklySchedule {
            days: vec![Weekday::Monday, Weekday::Monday],
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn weekly_validate_rejects_empty() {
        let schedule = WeeklySchedule {
            days: vec![],
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        };
        assert!(schedule.validate().is_err());
    }

    #[test]
    fn schedule_config_validate_unique_names() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![
                TaskSchedule {
                    name: "job-a".to_owned(),
                    task_name: "my_task".to_owned(),
                    pattern: SchedulePattern::Interval(IntervalSchedule {
                        seconds: Some(30),
                        minutes: None,
                        hours: None,
                        days: None,
                    }),
                    args: serde_json::Value::Null,
                    kwargs: serde_json::Value::Null,
                    queue_name: None,
                    enabled: true,
                    timezone: "UTC".to_owned(),
                    catch_up_missed: false,
                    max_catch_up_runs: 100,
                },
                TaskSchedule {
                    name: "job-b".to_owned(),
                    task_name: "my_task".to_owned(),
                    pattern: SchedulePattern::Interval(IntervalSchedule {
                        seconds: Some(60),
                        minutes: None,
                        hours: None,
                        days: None,
                    }),
                    args: serde_json::Value::Null,
                    kwargs: serde_json::Value::Null,
                    queue_name: None,
                    enabled: true,
                    timezone: "UTC".to_owned(),
                    catch_up_missed: false,
                    max_catch_up_runs: 100,
                },
            ],
            check_interval_seconds: 1,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn schedule_config_validate_rejects_duplicate_names() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![
                TaskSchedule {
                    name: "same-name".to_owned(),
                    task_name: "task_a".to_owned(),
                    pattern: SchedulePattern::Interval(IntervalSchedule {
                        seconds: Some(30),
                        minutes: None,
                        hours: None,
                        days: None,
                    }),
                    args: serde_json::Value::Null,
                    kwargs: serde_json::Value::Null,
                    queue_name: None,
                    enabled: true,
                    timezone: "UTC".to_owned(),
                    catch_up_missed: false,
                    max_catch_up_runs: 100,
                },
                TaskSchedule {
                    name: "same-name".to_owned(),
                    task_name: "task_b".to_owned(),
                    pattern: SchedulePattern::Interval(IntervalSchedule {
                        seconds: Some(60),
                        minutes: None,
                        hours: None,
                        days: None,
                    }),
                    args: serde_json::Value::Null,
                    kwargs: serde_json::Value::Null,
                    queue_name: None,
                    enabled: true,
                    timezone: "UTC".to_owned(),
                    catch_up_missed: false,
                    max_catch_up_runs: 100,
                },
            ],
            check_interval_seconds: 1,
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("duplicate schedule name"));
        assert!(err.contains("same-name"));
    }

    #[test]
    fn schedule_config_validate_propagates_pattern_errors() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "bad-schedule".to_owned(),
                task_name: "task_a".to_owned(),
                pattern: SchedulePattern::Weekly(WeeklySchedule {
                    days: vec![Weekday::Monday, Weekday::Monday],
                    time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                }),
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn schedule_pattern_tagged_serde() {
        let pattern = SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(30),
            minutes: None,
            hours: None,
            days: None,
        });
        let json = serde_json::to_string(&pattern).unwrap();
        assert!(json.contains("\"type\":\"interval\""));
        let back: SchedulePattern = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, SchedulePattern::Interval(_)));
    }

    // --- HourlySchedule validation tests ---

    #[test]
    fn hourly_validate_ok() {
        let schedule = HourlySchedule {
            minute: 30,
            second: 0,
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn hourly_validate_minute_59_ok() {
        let schedule = HourlySchedule {
            minute: 59,
            second: 59,
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn hourly_validate_minute_0_ok() {
        let schedule = HourlySchedule {
            minute: 0,
            second: 0,
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn hourly_validate_minute_over_59_rejected() {
        let schedule = HourlySchedule {
            minute: 60,
            second: 0,
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("minute"));
        assert!(err.contains("60"));
    }

    #[test]
    fn hourly_validate_second_over_59_rejected() {
        let schedule = HourlySchedule {
            minute: 0,
            second: 60,
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("second"));
        assert!(err.contains("60"));
    }

    // --- MonthlySchedule validation tests ---

    #[test]
    fn monthly_validate_ok() {
        let schedule = MonthlySchedule {
            day: 15,
            time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn monthly_validate_day_1_ok() {
        let schedule = MonthlySchedule {
            day: 1,
            time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn monthly_validate_day_31_ok() {
        let schedule = MonthlySchedule {
            day: 31,
            time: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        };
        assert!(schedule.validate().is_ok());
    }

    #[test]
    fn monthly_validate_day_0_rejected() {
        let schedule = MonthlySchedule {
            day: 0,
            time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("day"));
        assert!(err.contains("0"));
    }

    #[test]
    fn monthly_validate_day_32_rejected() {
        let schedule = MonthlySchedule {
            day: 32,
            time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        };
        let err = schedule.validate().unwrap_err();
        assert!(err.contains("day"));
        assert!(err.contains("32"));
    }

    // --- ScheduleConfig.check_interval_seconds validation tests ---

    #[test]
    fn schedule_config_check_interval_zero_rejected() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![],
            check_interval_seconds: 0,
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("check_interval_seconds"));
    }

    #[test]
    fn schedule_config_check_interval_61_rejected() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![],
            check_interval_seconds: 61,
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("check_interval_seconds"));
    }

    #[test]
    fn schedule_config_check_interval_1_ok() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![],
            check_interval_seconds: 1,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn schedule_config_check_interval_60_ok() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![],
            check_interval_seconds: 60,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn schedule_config_default_matches_serde_defaults() {
        let config = ScheduleConfig::default();
        assert!(config.enabled);
        assert!(config.schedules.is_empty());
        assert_eq!(config.check_interval_seconds, 1);
    }

    #[test]
    fn schedule_config_new_uses_defaults() {
        let config = ScheduleConfig::new(vec![]);
        assert!(config.enabled);
        assert!(config.schedules.is_empty());
        assert_eq!(config.check_interval_seconds, 1);
    }

    #[test]
    fn schedule_config_validate_propagates_hourly_errors() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "bad-hourly".to_owned(),
                task_name: "task_a".to_owned(),
                pattern: SchedulePattern::Hourly(HourlySchedule {
                    minute: 60,
                    second: 0,
                }),
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn schedule_config_validate_propagates_monthly_errors() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "bad-monthly".to_owned(),
                task_name: "task_a".to_owned(),
                pattern: SchedulePattern::Monthly(MonthlySchedule {
                    day: 0,
                    time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                }),
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn schedule_config_rejects_invalid_timezone() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "bad-tz".to_owned(),
                task_name: "task_a".to_owned(),
                pattern: SchedulePattern::Daily(DailySchedule {
                    time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                }),
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "Not/A_Timezone".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("invalid timezone"));
        assert!(err.contains("Not/A_Timezone"));
    }

    #[test]
    fn schedule_config_accepts_valid_timezone() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "ny-schedule".to_owned(),
                task_name: "task_a".to_owned(),
                pattern: SchedulePattern::Daily(DailySchedule {
                    time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                }),
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "America/New_York".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        };
        assert!(config.validate().is_ok());
    }

    // -- TaskSchedule defaults & construction (ported from Python test_schedule_models.py) --

    #[test]
    fn task_schedule_defaults() {
        let sched: TaskSchedule = serde_json::from_str(
            r#"{
                "name": "daily-task",
                "task_name": "my_task",
                "pattern": { "type": "interval", "seconds": 30 }
            }"#,
        )
        .unwrap();
        assert_eq!(sched.name, "daily-task");
        assert_eq!(sched.task_name, "my_task");
        assert!(sched.enabled);
        assert_eq!(sched.timezone, "UTC");
        assert!(!sched.catch_up_missed);
        assert_eq!(sched.max_catch_up_runs, 100);
        assert!(sched.queue_name.is_none());
        assert_eq!(sched.args, serde_json::Value::default());
        assert_eq!(sched.kwargs, serde_json::Value::default());
    }

    #[test]
    fn task_schedule_new_populates_defaults() {
        let sched = TaskSchedule::new(
            "daily-task",
            "my_task",
            SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(30),
                ..Default::default()
            }),
        );
        assert_eq!(sched.name, "daily-task");
        assert_eq!(sched.task_name, "my_task");
        assert!(sched.enabled);
        assert_eq!(sched.timezone, "UTC");
        assert!(!sched.catch_up_missed);
        assert_eq!(sched.max_catch_up_runs, 100);
        assert!(sched.queue_name.is_none());
        assert_eq!(sched.args, serde_json::Value::Null);
        assert_eq!(sched.kwargs, serde_json::Value::Null);
    }

    #[test]
    fn task_schedule_builder_methods_override_defaults() {
        let sched = TaskSchedule::new(
            "full",
            "my_task",
            SchedulePattern::Hourly(HourlySchedule {
                minute: 15,
                second: 0,
            }),
        )
        .args(serde_json::json!([1, 2]))
        .kwargs(serde_json::json!({"key": "val"}))
        .queue("high")
        .enabled(false)
        .timezone("Europe/London")
        .catch_up_missed(true)
        .max_catch_up_runs(50);

        assert_eq!(sched.queue_name.as_deref(), Some("high"));
        assert!(!sched.enabled);
        assert_eq!(sched.timezone, "Europe/London");
        assert!(sched.catch_up_missed);
        assert_eq!(sched.max_catch_up_runs, 50);
        assert_eq!(sched.args, serde_json::json!([1, 2]));
        assert_eq!(sched.kwargs, serde_json::json!({"key": "val"}));
    }

    #[test]
    fn task_schedule_all_fields() {
        let sched: TaskSchedule = serde_json::from_str(
            r#"{
                "name": "full",
                "task_name": "my_task",
                "pattern": { "type": "hourly", "minute": 15 },
                "args": [1, 2],
                "kwargs": { "key": "val" },
                "queue_name": "high",
                "enabled": false,
                "timezone": "Europe/London",
                "catch_up_missed": true,
                "max_catch_up_runs": 50
            }"#,
        )
        .unwrap();
        assert_eq!(sched.name, "full");
        assert!(!sched.enabled);
        assert_eq!(sched.timezone, "Europe/London");
        assert!(sched.catch_up_missed);
        assert_eq!(sched.max_catch_up_runs, 50);
        assert_eq!(sched.queue_name.as_deref(), Some("high"));
        assert_eq!(sched.args, serde_json::json!([1, 2]));
        assert_eq!(sched.kwargs, serde_json::json!({"key": "val"}));
    }

    #[test]
    fn task_schedule_serde_round_trip() {
        let sched = TaskSchedule {
            name: "rt".to_owned(),
            task_name: "t".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            args: serde_json::json!([1]),
            kwargs: serde_json::json!({"k": "v"}),
            queue_name: Some("q".to_owned()),
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: true,
            max_catch_up_runs: 10,
        };
        let json = serde_json::to_string(&sched).unwrap();
        let back: TaskSchedule = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "rt");
        assert_eq!(back.max_catch_up_runs, 10);
        assert!(back.catch_up_missed);
    }

    // -- Weekday enum (ported from Python test_schedule_models.py) --

    #[test]
    fn weekday_all_seven_values() {
        let days = [
            Weekday::Monday,
            Weekday::Tuesday,
            Weekday::Wednesday,
            Weekday::Thursday,
            Weekday::Friday,
            Weekday::Saturday,
            Weekday::Sunday,
        ];
        assert_eq!(days.len(), 7);
        // All distinct
        let set: HashSet<Weekday> = days.iter().copied().collect();
        assert_eq!(set.len(), 7);
    }

    #[test]
    fn weekday_serde_snake_case() {
        let json = serde_json::to_string(&Weekday::Monday).unwrap();
        assert_eq!(json, r#""monday""#);
        let back: Weekday = serde_json::from_str(r#""friday""#).unwrap();
        assert_eq!(back, Weekday::Friday);
    }

    // -- ScheduleConfig defaults --

    #[test]
    fn schedule_config_defaults() {
        let config: ScheduleConfig = serde_json::from_str(r#"{}"#).unwrap();
        assert!(config.enabled);
        assert!(config.schedules.is_empty());
        assert_eq!(config.check_interval_seconds, 1);
    }
}
