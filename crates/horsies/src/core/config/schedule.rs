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

/// Months of the year.
///
/// Snake-case string values keep the serialized form stable, mirroring
/// [`Weekday`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Month {
    January,
    February,
    March,
    April,
    May,
    June,
    July,
    August,
    September,
    October,
    November,
    December,
}

/// Canonical ordinal for enum-domain cron fields.
///
/// Month is January=1..December=12; Weekday is Monday=0..Sunday=6 (matching
/// `chrono::Weekday::num_days_from_monday`). A single source of truth shared by
/// validation and the scheduler matcher.
pub trait CronOrdinal: Copy {
    /// Ordinal value used for range/step semantics.
    fn cron_ordinal(self) -> i64;
}

impl CronOrdinal for Month {
    fn cron_ordinal(self) -> i64 {
        match self {
            Month::January => 1,
            Month::February => 2,
            Month::March => 3,
            Month::April => 4,
            Month::May => 5,
            Month::June => 6,
            Month::July => 7,
            Month::August => 8,
            Month::September => 9,
            Month::October => 10,
            Month::November => 11,
            Month::December => 12,
        }
    }
}

impl CronOrdinal for Weekday {
    fn cron_ordinal(self) -> i64 {
        match self {
            Weekday::Monday => 0,
            Weekday::Tuesday => 1,
            Weekday::Wednesday => 2,
            Weekday::Thursday => 3,
            Weekday::Friday => 4,
            Weekday::Saturday => 5,
            Weekday::Sunday => 6,
        }
    }
}

/// Leap-aware maximum day for a month ordinal (February allows 29). Used by the
/// construction-time satisfiability check.
fn month_max_days(month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => 29,
        _ => 0,
    }
}

fn default_cron_step() -> u32 {
    1
}

/// Cron term over an integer field (minute, hour, day-of-month).
///
/// Per-field domain bounds are enforced by [`CronSchedule::validate`], which
/// names the offending field — the variants are domain-agnostic. Wire tags
/// match the Python implementation (`every`/`step`/`values`/`range`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CronNumericTerm {
    /// `*` — every value in the field's domain.
    Every,
    /// `*/n` — every nth value across the field's full domain (anchored at the
    /// domain start). `step` must be <= the field span.
    Step { step: u32 },
    /// `a,b,c` — an explicit set of integer values.
    Values { values: Vec<i64> },
    /// `a-b` or `a-b/n` — an inclusive integer range with an optional step.
    Range {
        start: i64,
        end: i64,
        #[serde(default = "default_cron_step")]
        step: u32,
    },
}

/// Cron term over an enum field (`Month` or `Weekday`), generic over the enum
/// type so month and day-of-week stay distinct on the type level.
///
/// Wire tags match the Python implementation
/// (`every`/`enum_values`/`enum_range`/`enum_step`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CronEnumTerm<T> {
    /// `*` — every value in the field's domain.
    Every,
    /// `a,b,c` — explicit enum values.
    EnumValues { values: Vec<T> },
    /// `a-b` or `a-b/n` — inclusive range by canonical ordinal, optional step.
    EnumRange {
        start: T,
        end: T,
        #[serde(default = "default_cron_step")]
        step: u32,
    },
    /// `*/n` — every nth value across the field's full domain, by ordinal.
    EnumStep { step: u32 },
}

/// Day dimension selector.
///
/// A single sum type forces an explicit choice when both day-of-month and
/// day-of-week are restricted, eliminating cron's silent OR ambiguity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaySelector {
    /// Both day-of-month and day-of-week unrestricted.
    EveryDay,
    /// Restrict by day-of-month only.
    ByMonthDay { day_of_month: Vec<CronNumericTerm> },
    /// Restrict by day-of-week only.
    ByWeekday {
        day_of_week: Vec<CronEnumTerm<Weekday>>,
    },
    /// Fire when day-of-month OR day-of-week matches (cron's "13th or Friday").
    EitherDay {
        day_of_month: Vec<CronNumericTerm>,
        day_of_week: Vec<CronEnumTerm<Weekday>>,
    },
    /// Fire only when day-of-month AND day-of-week both match (intersection).
    BothDays {
        day_of_month: Vec<CronNumericTerm>,
        day_of_week: Vec<CronEnumTerm<Weekday>>,
    },
}

/// Typed 5-field cron-style schedule (with step extensions).
///
/// Expresses what a `minute hour day-of-month month day-of-week` crontab line
/// can, through typed fields instead of a cron-expression string. Fires at
/// second :00 (minute granularity). The `day` field collapses day-of-month and
/// day-of-week into an explicit [`DaySelector`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronSchedule {
    /// Minute field terms (0–59).
    pub minute: Vec<CronNumericTerm>,
    /// Hour field terms (0–23).
    pub hour: Vec<CronNumericTerm>,
    /// Month field terms.
    pub month: Vec<CronEnumTerm<Month>>,
    /// Day-of-month / day-of-week selector.
    pub day: DaySelector,
}

/// Build a field-scoped validation error message.
fn invalid_cron_field(field: &str, detail: &str) -> String {
    format!("invalid cron '{}' field: {}", field, detail)
}

/// Validate integer-domain terms against `[lo, hi]`.
fn validate_numeric_terms(
    terms: &[CronNumericTerm],
    field: &str,
    lo: i64,
    hi: i64,
) -> Result<(), String> {
    let span = hi - lo;
    for term in terms {
        match term {
            CronNumericTerm::Every => {}
            CronNumericTerm::Step { step } => {
                if *step == 0 {
                    return Err(invalid_cron_field(field, "step must be >= 1"));
                }
                if *step as i64 > span {
                    return Err(invalid_cron_field(
                        field,
                        &format!(
                            "step {} exceeds the {} span {}; use explicit values instead",
                            step, field, span,
                        ),
                    ));
                }
            }
            CronNumericTerm::Values { values } => {
                if values.is_empty() {
                    return Err(invalid_cron_field(field, "values must not be empty"));
                }
                for value in values {
                    if *value < lo || *value > hi {
                        return Err(invalid_cron_field(
                            field,
                            &format!("value {} out of range {}-{}", value, lo, hi),
                        ));
                    }
                }
            }
            CronNumericTerm::Range { start, end, step } => {
                if *step == 0 {
                    return Err(invalid_cron_field(field, "range step must be >= 1"));
                }
                if *start < lo || *start > hi {
                    return Err(invalid_cron_field(
                        field,
                        &format!("range start {} out of range {}-{}", start, lo, hi),
                    ));
                }
                if *end < lo || *end > hi {
                    return Err(invalid_cron_field(
                        field,
                        &format!("range end {} out of range {}-{}", end, lo, hi),
                    ));
                }
                if start > end {
                    return Err(invalid_cron_field(
                        field,
                        &format!("range start {} exceeds end {}", start, end),
                    ));
                }
                if *step as i64 > span {
                    return Err(invalid_cron_field(
                        field,
                        &format!("range step {} exceeds span {}", step, span),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Validate enum-domain terms: range ordering (no wrap-around) and step span.
///
/// Enum values are in-domain by construction, so only ordering and step bounds
/// need checking. `span` is the field's ordinal span (11 for month, 6 for
/// day-of-week).
fn validate_enum_terms<T: CronOrdinal + std::fmt::Debug>(
    terms: &[CronEnumTerm<T>],
    field: &str,
    span: i64,
) -> Result<(), String> {
    for term in terms {
        match term {
            CronEnumTerm::Every => {}
            CronEnumTerm::EnumStep { step } => {
                if *step == 0 {
                    return Err(invalid_cron_field(field, "step must be >= 1"));
                }
                if *step as i64 > span {
                    return Err(invalid_cron_field(
                        field,
                        &format!("step {} exceeds the {} span {}", step, field, span),
                    ));
                }
            }
            CronEnumTerm::EnumValues { values } => {
                if values.is_empty() {
                    return Err(invalid_cron_field(field, "values must not be empty"));
                }
            }
            CronEnumTerm::EnumRange { start, end, step } => {
                if *step == 0 {
                    return Err(invalid_cron_field(field, "range step must be >= 1"));
                }
                if start.cron_ordinal() > end.cron_ordinal() {
                    return Err(invalid_cron_field(
                        field,
                        &format!(
                            "range start {:?} comes after end {:?}; \
                             wrap-around ranges are not allowed, use explicit values",
                            start, end,
                        ),
                    ));
                }
                if *step as i64 > span {
                    return Err(invalid_cron_field(
                        field,
                        &format!("range step {} exceeds span {}", step, span),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Validate the day dimension's contained day-of-month / day-of-week terms.
fn validate_day_selector(day: &DaySelector) -> Result<(), String> {
    match day {
        DaySelector::EveryDay => Ok(()),
        DaySelector::ByMonthDay { day_of_month } => {
            if day_of_month.is_empty() {
                return Err(invalid_cron_field(
                    "day_of_month",
                    "must have at least one term",
                ));
            }
            validate_numeric_terms(day_of_month, "day_of_month", 1, 31)
        }
        DaySelector::ByWeekday { day_of_week } => {
            if day_of_week.is_empty() {
                return Err(invalid_cron_field(
                    "day_of_week",
                    "must have at least one term",
                ));
            }
            validate_enum_terms(day_of_week, "day_of_week", 6)
        }
        DaySelector::EitherDay {
            day_of_month,
            day_of_week,
        }
        | DaySelector::BothDays {
            day_of_month,
            day_of_week,
        } => {
            if day_of_month.is_empty() {
                return Err(invalid_cron_field(
                    "day_of_month",
                    "must have at least one term",
                ));
            }
            if day_of_week.is_empty() {
                return Err(invalid_cron_field(
                    "day_of_week",
                    "must have at least one term",
                ));
            }
            validate_numeric_terms(day_of_month, "day_of_month", 1, 31)?;
            validate_enum_terms(day_of_week, "day_of_week", 6)
        }
    }
}

/// Reject schedules whose month × day-of-month combination can never occur.
///
/// Only `ByMonthDay` and `BothDays` constrain day-of-month such that the
/// schedule can be empty (e.g. February + day 30). `EitherDay` always fires via
/// its weekday branch; `ByWeekday`/`EveryDay` are always satisfiable.
fn validate_satisfiable(month: &[CronEnumTerm<Month>], day: &DaySelector) -> Result<(), String> {
    let day_of_month = match day {
        DaySelector::ByMonthDay { day_of_month } | DaySelector::BothDays { day_of_month, .. } => {
            day_of_month
        }
        DaySelector::EveryDay | DaySelector::ByWeekday { .. } | DaySelector::EitherDay { .. } => {
            return Ok(())
        }
    };
    let month_set = expand_enum_field(month, 1, 12);
    let dom_set = expand_numeric_field(day_of_month, 1, 31);
    let feasible = month_set
        .iter()
        .any(|m| dom_set.iter().any(|d| *d <= month_max_days(*m)));
    if !feasible {
        let mut months: Vec<i64> = month_set.iter().copied().collect();
        months.sort_unstable();
        let mut days: Vec<i64> = dom_set.iter().copied().collect();
        days.sort_unstable();
        return Err(format!(
            "cron schedule can never fire: no selected day-of-month exists in any \
             selected month (months: {:?}, days-of-month: {:?})",
            months, days,
        ));
    }
    Ok(())
}

/// Expand numeric terms into the concrete set of matched values in `[lo, hi]`.
///
/// Shared by validation (satisfiability) and the scheduler matcher so term
/// expansion is never reimplemented. `step` is clamped to >= 1 defensively;
/// validation rejects step 0 before this is reachable on a real config.
pub fn expand_numeric_field(terms: &[CronNumericTerm], lo: i64, hi: i64) -> HashSet<i64> {
    let mut result = HashSet::new();
    for term in terms {
        match term {
            CronNumericTerm::Every => {
                result.extend(lo..=hi);
            }
            CronNumericTerm::Step { step } => {
                let step = (*step as i64).max(1);
                let mut value = lo;
                while value <= hi {
                    result.insert(value);
                    value += step;
                }
            }
            CronNumericTerm::Values { values } => {
                result.extend(values.iter().copied().filter(|v| *v >= lo && *v <= hi));
            }
            CronNumericTerm::Range { start, end, step } => {
                let step = (*step as i64).max(1);
                let mut value = *start;
                while value <= *end {
                    if value >= lo && value <= hi {
                        result.insert(value);
                    }
                    value += step;
                }
            }
        }
    }
    result
}

/// Expand enum terms into the concrete set of ordinals in `[lo, hi]`.
///
/// Month ordinals are 1–12, weekday ordinals 0–6. Shared by validation and the
/// scheduler matcher.
pub fn expand_enum_field<T: CronOrdinal>(
    terms: &[CronEnumTerm<T>],
    lo: i64,
    hi: i64,
) -> HashSet<i64> {
    let mut result = HashSet::new();
    for term in terms {
        match term {
            CronEnumTerm::Every => {
                result.extend(lo..=hi);
            }
            CronEnumTerm::EnumStep { step } => {
                let step = (*step as i64).max(1);
                let mut value = lo;
                while value <= hi {
                    result.insert(value);
                    value += step;
                }
            }
            CronEnumTerm::EnumValues { values } => {
                result.extend(values.iter().map(|v| v.cron_ordinal()));
            }
            CronEnumTerm::EnumRange { start, end, step } => {
                let step = (*step as i64).max(1);
                let mut value = start.cron_ordinal();
                let end = end.cron_ordinal();
                while value <= end {
                    result.insert(value);
                    value += step;
                }
            }
        }
    }
    result
}

impl CronSchedule {
    /// Validate per-field domains, range ordering, step span, and month ×
    /// day-of-month satisfiability.
    ///
    /// Returns the first error encountered (single-error style, consistent with
    /// the other schedule pattern validators in this module).
    pub fn validate(&self) -> Result<(), String> {
        if self.minute.is_empty() {
            return Err(invalid_cron_field("minute", "must have at least one term"));
        }
        if self.hour.is_empty() {
            return Err(invalid_cron_field("hour", "must have at least one term"));
        }
        if self.month.is_empty() {
            return Err(invalid_cron_field("month", "must have at least one term"));
        }
        validate_numeric_terms(&self.minute, "minute", 0, 59)?;
        validate_numeric_terms(&self.hour, "hour", 0, 23)?;
        validate_enum_terms(&self.month, "month", 11)?;
        validate_day_selector(&self.day)?;
        validate_satisfiable(&self.month, &self.day)?;
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
    Cron(CronSchedule),
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
                SchedulePattern::Cron(cron) => cron.validate()?,
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

    // ----------------------------------------------------------------------
    // CronSchedule (ported from Python test_schedule_cron.py)
    // ----------------------------------------------------------------------

    fn num_values(values: Vec<i64>) -> CronNumericTerm {
        CronNumericTerm::Values { values }
    }

    // -- construction --

    #[test]
    fn cron_minimal_valid() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_mixed_terms_in_one_field() {
        let cron = CronSchedule {
            minute: vec![
                num_values(vec![0, 30]),
                CronNumericTerm::Range {
                    start: 10,
                    end: 20,
                    step: 5,
                },
            ],
            hour: vec![CronNumericTerm::Step { step: 4 }],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_enum_range_and_step() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![
                CronEnumTerm::EnumRange {
                    start: Month::March,
                    end: Month::September,
                    step: 2,
                },
                CronEnumTerm::EnumStep { step: 3 },
            ],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().is_ok());
    }

    // -- single-error domain validation --

    #[test]
    fn cron_minute_out_of_domain() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![60])],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        let err = cron.validate().unwrap_err();
        assert!(err.contains("minute"));
        assert!(err.contains("60"));
    }

    #[test]
    fn cron_hour_out_of_domain() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![num_values(vec![24])],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        let err = cron.validate().unwrap_err();
        assert!(err.contains("hour"));
        assert!(err.contains("24"));
    }

    #[test]
    fn cron_day_of_month_out_of_domain() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::ByMonthDay {
                day_of_month: vec![num_values(vec![32])],
            },
        };
        let err = cron.validate().unwrap_err();
        assert!(err.contains("day_of_month"));
        assert!(err.contains("32"));
    }

    #[test]
    fn cron_numeric_range_start_after_end() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Range {
                start: 30,
                end: 10,
                step: 1,
            }],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        let err = cron.validate().unwrap_err();
        assert!(err.contains("minute"));
        assert!(err.contains("exceeds end"));
    }

    #[test]
    fn cron_weekday_range_wraparound_rejected() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::ByWeekday {
                day_of_week: vec![CronEnumTerm::EnumRange {
                    start: Weekday::Friday,
                    end: Weekday::Monday,
                    step: 1,
                }],
            },
        };
        let err = cron.validate().unwrap_err();
        assert!(err.contains("day_of_week"));
        assert!(err.contains("wrap-around"));
    }

    #[test]
    fn cron_month_range_wraparound_rejected() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::EnumRange {
                start: Month::December,
                end: Month::January,
                step: 1,
            }],
            day: DaySelector::EveryDay,
        };
        let err = cron.validate().unwrap_err();
        assert!(err.contains("month"));
        assert!(err.contains("wrap-around"));
    }

    // -- step span --

    #[test]
    fn cron_minute_step_at_span_allowed() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Step { step: 59 }],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_minute_step_over_span_rejected() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Step { step: 60 }],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().unwrap_err().contains("minute"));
    }

    #[test]
    fn cron_day_of_month_step_at_span_allowed() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::ByMonthDay {
                day_of_month: vec![CronNumericTerm::Step { step: 30 }],
            },
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_day_of_month_step_over_span_rejected() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::ByMonthDay {
                day_of_month: vec![CronNumericTerm::Step { step: 31 }],
            },
        };
        assert!(cron.validate().unwrap_err().contains("day_of_month"));
    }

    #[test]
    fn cron_month_step_at_span_allowed() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::EnumStep { step: 11 }],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_month_step_over_span_rejected() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Every],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::EnumStep { step: 12 }],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().unwrap_err().contains("month"));
    }

    // -- satisfiability --

    fn feb_only() -> Vec<CronEnumTerm<Month>> {
        vec![CronEnumTerm::EnumValues {
            values: vec![Month::February],
        }]
    }

    #[test]
    fn cron_february_30_rejected() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: feb_only(),
            day: DaySelector::ByMonthDay {
                day_of_month: vec![num_values(vec![30])],
            },
        };
        assert!(cron.validate().unwrap_err().contains("never fire"));
    }

    #[test]
    fn cron_february_29_allowed() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: feb_only(),
            day: DaySelector::ByMonthDay {
                day_of_month: vec![num_values(vec![29])],
            },
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_april_31_rejected() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: vec![CronEnumTerm::EnumValues {
                values: vec![Month::April],
            }],
            day: DaySelector::ByMonthDay {
                day_of_month: vec![num_values(vec![31])],
            },
        };
        assert!(cron.validate().unwrap_err().contains("never fire"));
    }

    #[test]
    fn cron_either_day_impossible_dom_still_valid() {
        // EitherDay always fires via the weekday branch, so an impossible
        // day-of-month does not make it unsatisfiable.
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: feb_only(),
            day: DaySelector::EitherDay {
                day_of_month: vec![num_values(vec![30])],
                day_of_week: vec![CronEnumTerm::EnumValues {
                    values: vec![Weekday::Monday],
                }],
            },
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn cron_both_days_impossible_dom_rejected() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![num_values(vec![0])],
            month: feb_only(),
            day: DaySelector::BothDays {
                day_of_month: vec![num_values(vec![30])],
                day_of_week: vec![CronEnumTerm::EnumValues {
                    values: vec![Weekday::Monday],
                }],
            },
        };
        assert!(cron.validate().unwrap_err().contains("never fire"));
    }

    // -- empty fields & step zero --

    #[test]
    fn cron_empty_minute_rejected() {
        let cron = CronSchedule {
            minute: vec![],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().unwrap_err().contains("minute"));
    }

    #[test]
    fn cron_step_zero_rejected() {
        let cron = CronSchedule {
            minute: vec![CronNumericTerm::Step { step: 0 }],
            hour: vec![CronNumericTerm::Every],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        assert!(cron.validate().unwrap_err().contains("step"));
    }

    // -- serde --

    #[test]
    fn cron_serde_carries_discriminators() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![15])],
            hour: vec![CronNumericTerm::Step { step: 4 }],
            month: vec![CronEnumTerm::Every],
            day: DaySelector::EveryDay,
        };
        let json = serde_json::to_string(&cron).unwrap();
        assert!(json.contains(r#""kind":"values""#));
        assert!(json.contains(r#""kind":"step""#));
        assert!(json.contains(r#""kind":"every""#));
        assert!(json.contains(r#""kind":"every_day""#));
    }

    #[test]
    fn cron_direct_round_trip() {
        let cron = CronSchedule {
            minute: vec![num_values(vec![0])],
            hour: vec![CronNumericTerm::Range {
                start: 8,
                end: 18,
                step: 2,
            }],
            month: vec![CronEnumTerm::EnumValues {
                values: vec![Month::June, Month::July],
            }],
            day: DaySelector::BothDays {
                day_of_month: vec![num_values(vec![1, 15])],
                day_of_week: vec![CronEnumTerm::EnumValues {
                    values: vec![Weekday::Monday],
                }],
            },
        };
        let json = serde_json::to_string(&cron).unwrap();
        let back: CronSchedule = serde_json::from_str(&json).unwrap();
        assert_eq!(cron, back);
    }

    #[test]
    fn cron_month_enum_serializes_as_string() {
        let json = serde_json::to_string(&Month::February).unwrap();
        assert_eq!(json, r#""february""#);
        let back: Month = serde_json::from_str(r#""december""#).unwrap();
        assert_eq!(back, Month::December);
    }

    #[test]
    fn cron_task_schedule_round_trip() {
        let sched = TaskSchedule::new(
            "cron-job",
            "my_task",
            SchedulePattern::Cron(CronSchedule {
                minute: vec![num_values(vec![0])],
                hour: vec![CronNumericTerm::Step { step: 6 }],
                month: vec![CronEnumTerm::Every],
                day: DaySelector::EveryDay,
            }),
        );
        let json = serde_json::to_string(&sched).unwrap();
        assert!(json.contains(r#""type":"cron""#));
        let back: TaskSchedule = serde_json::from_str(&json).unwrap();
        assert!(matches!(back.pattern, SchedulePattern::Cron(_)));
    }

    #[test]
    fn cron_config_validate_propagates_errors() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "bad-cron".to_owned(),
                task_name: "task_a".to_owned(),
                pattern: SchedulePattern::Cron(CronSchedule {
                    minute: vec![num_values(vec![99])],
                    hour: vec![CronNumericTerm::Every],
                    month: vec![CronEnumTerm::Every],
                    day: DaySelector::EveryDay,
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
}
