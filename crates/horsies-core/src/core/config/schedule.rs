use std::collections::HashSet;

use chrono::{NaiveTime, Timelike};
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// Check that a pattern's time of day is a whole second. Slots fire at whole
/// seconds (Python refuses a `time` with microseconds at config), so the
/// calculator never sees a fraction of a second. A leap second
/// (`23:59:60`, a nanosecond count of one second or more) has no Python form
/// and is refused too. Pure.
///
/// Errors: the pattern, the time and the fix, as text.
fn validate_whole_second(pattern: &str, time: NaiveTime) -> Result<(), String> {
    match time.nanosecond() {
        0 => Ok(()),
        _ => Err(format!(
            "{pattern} time must be a whole second, got {time}: schedules fire at whole seconds; \
             give the time without a fraction of a second",
        )),
    }
}

/// Run task every day at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DailySchedule {
    /// Time of day to run (HH:MM:SS), in whole seconds, in the schedule's
    /// timezone.
    pub time: NaiveTime,
}

impl DailySchedule {
    /// Validate that the time is a whole second.
    pub fn validate(&self) -> Result<(), String> {
        validate_whole_second("DailySchedule", self.time)
    }
}

/// Run task on specific days of the week at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeeklySchedule {
    /// Days of week to run (at least one).
    pub days: Vec<Weekday>,
    /// Time of day to run, in whole seconds, in the schedule's timezone.
    pub time: NaiveTime,
}

impl WeeklySchedule {
    /// Validate that the days list is non-empty and contains no duplicates,
    /// and that the time is a whole second.
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
        validate_whole_second("WeeklySchedule", self.time)
    }
}

/// Run task on a specific day of the month at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonthlySchedule {
    /// Day of month (1–31).
    pub day: u32,
    /// Time of day to run, in whole seconds, in the schedule's timezone.
    pub time: NaiveTime,
}

impl MonthlySchedule {
    /// Validate that day is 1–31 and that the time is a whole second.
    ///
    /// Matches Python's `Field(ge=1, le=31)` constraint.
    pub fn validate(&self) -> Result<(), String> {
        if self.day == 0 || self.day > 31 {
            return Err(format!(
                "MonthlySchedule day must be between 1 and 31, got {}",
                self.day,
            ));
        }
        validate_whole_second("MonthlySchedule", self.time)
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

/// The body of a tagged unit variant ([`CronNumericTerm::Every`],
/// [`CronEnumTerm::Every`], [`DaySelector::EveryDay`]): the members after the
/// tag, which must be none. serde reads a unit variant of an internally
/// tagged enum and skips its other members, also under
/// `deny_unknown_fields`; Python's `CronEvery` and `EveryDay` refuse them
/// (`extra='forbid'`).
///
/// Errors: serde's unknown-field error, which names the first member.
fn no_member_after_the_tag<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    /// A body with no members.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct NoMembers {}

    NoMembers::deserialize(deserializer).map(|NoMembers {}| ())
}

/// Cron term over an integer field (minute, hour, day-of-month).
///
/// Per-field domain bounds are enforced by [`CronSchedule::validate`], which
/// names the offending field — the variants are domain-agnostic. Wire tags
/// match the Python implementation (`every`/`step`/`values`/`range`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CronNumericTerm {
    /// `*` — every value in the field's domain.
    #[serde(deserialize_with = "no_member_after_the_tag")]
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CronEnumTerm<T> {
    /// `*` — every value in the field's domain.
    #[serde(deserialize_with = "no_member_after_the_tag")]
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DaySelector {
    /// Both day-of-month and day-of-week unrestricted.
    #[serde(deserialize_with = "no_member_after_the_tag")]
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
#[serde(deny_unknown_fields)]
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
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchedulePattern {
    Interval(IntervalSchedule),
    Hourly(HourlySchedule),
    Daily(DailySchedule),
    Weekly(WeeklySchedule),
    Monthly(MonthlySchedule),
    Cron(CronSchedule),
}

impl SchedulePattern {
    /// Check the bounds and the rules of the pattern: interval units,
    /// hourly and monthly ranges, unique weekly days, cron terms, and a
    /// daily, weekly or monthly time of day in whole seconds. Pure.
    ///
    /// Errors: the first broken rule, as text.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            SchedulePattern::Interval(interval) => interval.validate(),
            SchedulePattern::Hourly(hourly) => hourly.validate(),
            SchedulePattern::Daily(daily) => daily.validate(),
            SchedulePattern::Weekly(weekly) => weekly.validate(),
            SchedulePattern::Monthly(monthly) => monthly.validate(),
            SchedulePattern::Cron(cron) => cron.validate(),
        }
    }
}

/// Definition of a scheduled task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSchedule {
    /// Unique schedule identifier.
    pub name: String,
    /// Task to execute (must be registered).
    pub task_name: String,
    /// Schedule pattern defining when the task runs.
    pub pattern: SchedulePattern,
    /// The task input as one JSON value, stored as the task row's `input`.
    /// Absent is `null` (a task without input). It replaced `args` and
    /// `kwargs`; a config that names either is refused as an unknown key.
    #[serde(default)]
    pub input: serde_json::Value,
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
            input: serde_json::Value::Null,
            queue_name: None,
            enabled: default_true(),
            timezone: default_timezone(),
            catch_up_missed: false,
            max_catch_up_runs: default_max_catch_up_runs(),
        }
    }

    pub fn input(mut self, input: serde_json::Value) -> Self {
        self.input = input;
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
#[serde(deny_unknown_fields)]
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

    /// Validate the schedule configuration and return the first error.
    ///
    /// The same checks as [`ScheduleConfig::errors`].
    pub fn validate(&self) -> Result<(), String> {
        match self.errors().into_iter().next() {
            Some(first) => Err(first),
            None => Ok(()),
        }
    }

    /// Every error of the schedule configuration, in config order.
    ///
    /// Checks:
    /// - `check_interval_seconds` is between 1 and 60 (matching Python `Field(ge=1, le=60)`)
    /// - All schedule names are unique
    /// - Individual schedule patterns are valid (interval bounds, hourly/monthly ranges,
    ///   weekly day uniqueness)
    /// - Each timezone is an IANA name
    /// - Each `max_catch_up_runs` is between 1 and 10,000 (Python `Field(ge=1, le=10000)`)
    pub fn errors(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // check_interval_seconds bounds (Python: ge=1, le=60)
        if !(1..=60).contains(&self.check_interval_seconds) {
            errors.push(format!(
                "check_interval_seconds must be between 1 and 60, got {}",
                self.check_interval_seconds,
            ));
        }

        // Unique schedule names
        let mut seen_names = HashSet::with_capacity(self.schedules.len());
        for schedule in &self.schedules {
            if !seen_names.insert(&schedule.name) {
                errors.push(format!(
                    "duplicate schedule name '{}' — each schedule must have a unique name",
                    schedule.name,
                ));
            }
        }

        // Validate individual schedule patterns, timezones and catch-up bounds
        for schedule in &self.schedules {
            if let Err(pattern_error) = schedule.pattern.validate() {
                errors.push(format!("schedule '{}': {}", schedule.name, pattern_error));
            }

            if schedule.timezone.parse::<Tz>().is_err() {
                errors.push(format!(
                    "schedule '{}': invalid timezone '{}' — \
                     use an IANA timezone name (e.g. 'America/New_York', 'UTC')",
                    schedule.name, schedule.timezone,
                ));
            }

            if !(MAX_CATCH_UP_RUNS_MIN..=MAX_CATCH_UP_RUNS_MAX)
                .contains(&schedule.max_catch_up_runs)
            {
                errors.push(format!(
                    "schedule '{}': max_catch_up_runs must be between {} and {}, got {}",
                    schedule.name,
                    MAX_CATCH_UP_RUNS_MIN,
                    MAX_CATCH_UP_RUNS_MAX,
                    schedule.max_catch_up_runs,
                ));
            }
        }

        errors
    }
}

/// Smallest `TaskSchedule::max_catch_up_runs` (Python `Field(ge=1)`).
pub const MAX_CATCH_UP_RUNS_MIN: u32 = 1;

/// Largest `TaskSchedule::max_catch_up_runs` (Python `Field(le=10000)`).
pub const MAX_CATCH_UP_RUNS_MAX: u32 = 10_000;

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

    /// Python refuses a `time` with microseconds at config; the core refuses
    /// a time of day with any fraction of a second, and a leap second, in
    /// each pattern that has a time.
    #[test]
    fn a_time_of_day_with_a_fraction_of_a_second_is_refused() {
        let fractions = [
            NaiveTime::from_hms_micro_opt(9, 0, 0, 500).unwrap(),
            NaiveTime::from_hms_nano_opt(9, 0, 0, 1).unwrap(),
            NaiveTime::from_hms_milli_opt(23, 59, 59, 1_500).unwrap(),
        ];
        for time in fractions {
            let patterns = [
                (
                    "DailySchedule",
                    SchedulePattern::Daily(DailySchedule { time }),
                ),
                (
                    "WeeklySchedule",
                    SchedulePattern::Weekly(WeeklySchedule {
                        days: vec![Weekday::Monday],
                        time,
                    }),
                ),
                (
                    "MonthlySchedule",
                    SchedulePattern::Monthly(MonthlySchedule { day: 1, time }),
                ),
            ];
            for (name, pattern) in patterns {
                let error = pattern.validate().unwrap_err();
                assert!(
                    error.starts_with(&format!("{name} time must be a whole second")),
                    "{error}"
                );
                assert!(error.contains("schedules fire at whole seconds"), "{error}");
            }
        }
    }

    #[test]
    fn a_time_of_day_in_whole_seconds_is_valid() {
        let time = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        assert!(SchedulePattern::Daily(DailySchedule { time })
            .validate()
            .is_ok());
        assert!(SchedulePattern::Monthly(MonthlySchedule { day: 31, time })
            .validate()
            .is_ok());
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
                    input: serde_json::Value::Null,
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
                    input: serde_json::Value::Null,
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
                    input: serde_json::Value::Null,
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
                    input: serde_json::Value::Null,
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
                input: serde_json::Value::Null,
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
                input: serde_json::Value::Null,
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
                input: serde_json::Value::Null,
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
                input: serde_json::Value::Null,
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
                input: serde_json::Value::Null,
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
        assert_eq!(sched.input, serde_json::Value::default());
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
        assert_eq!(sched.input, serde_json::Value::Null);
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
        .input(serde_json::json!({"key": "val"}))
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
        assert_eq!(sched.input, serde_json::json!({"key": "val"}));
    }

    #[test]
    fn removed_args_and_kwargs_keys_are_refused_by_name() {
        // The one `input` value replaced `args` and `kwargs`. A config that
        // still names either key fails closed when it is read, with the key
        // named, as every unknown schedule key is.
        for key in ["args", "kwargs"] {
            let mut raw = serde_json::json!({
                "name": "s",
                "task_name": "t",
                "pattern": { "type": "interval", "seconds": 30 },
            });
            raw[key] = serde_json::json!({"x": 1});
            let refused = serde_json::from_value::<TaskSchedule>(raw).unwrap_err();
            assert!(
                refused
                    .to_string()
                    .contains(&format!("unknown field `{key}`")),
                "{refused}"
            );
        }
    }

    #[test]
    fn task_schedule_all_fields() {
        let sched: TaskSchedule = serde_json::from_str(
            r#"{
                "name": "full",
                "task_name": "my_task",
                "pattern": { "type": "hourly", "minute": 15 },
                "input": { "key": "val" },
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
        assert_eq!(sched.input, serde_json::json!({"key": "val"}));
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
            input: serde_json::json!({"k": "v"}),
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
                input: serde_json::Value::Null,
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

    // -- Bounds and collected errors (Python test_schedule_models.py) --

    fn interval_schedule(name: &str) -> TaskSchedule {
        TaskSchedule::new(
            name,
            "my_task",
            SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(10),
                ..IntervalSchedule::default()
            }),
        )
    }

    /// Python `test_schedule_models.py::test_max_catch_up_runs_validation`:
    /// `max_catch_up_runs` is between 1 and 10,000.
    #[test]
    fn max_catch_up_runs_validation() {
        for accepted in [MAX_CATCH_UP_RUNS_MIN, MAX_CATCH_UP_RUNS_MAX] {
            let config = ScheduleConfig::new(vec![
                interval_schedule("bounded").max_catch_up_runs(accepted)
            ]);
            assert!(
                config.errors().is_empty(),
                "{accepted}: {:?}",
                config.errors()
            );
        }
        for refused in [0, MAX_CATCH_UP_RUNS_MAX + 1] {
            let config =
                ScheduleConfig::new(vec![interval_schedule("bounded").max_catch_up_runs(refused)]);
            let errors = config.errors();
            assert_eq!(errors.len(), 1, "{refused}: {errors:?}");
            assert!(errors[0].contains("max_catch_up_runs"), "{}", errors[0]);
        }
    }

    /// Every schedule error is collected, not only the first (Python's
    /// `ValidationReport` collects the errors of the schedule section).
    #[test]
    fn errors_collects_every_schedule_error() {
        let config = ScheduleConfig {
            enabled: true,
            schedules: vec![
                interval_schedule("twice"),
                interval_schedule("twice"),
                interval_schedule("bad_tz").timezone("Not/A_Timezone"),
                interval_schedule("no_catch_up").max_catch_up_runs(0),
                TaskSchedule::new(
                    "empty_interval",
                    "my_task",
                    SchedulePattern::Interval(IntervalSchedule::default()),
                ),
            ],
            check_interval_seconds: 0,
        };
        let errors = config.errors();
        assert_eq!(errors.len(), 5, "{errors:?}");
        assert!(errors[0].contains("check_interval_seconds"));
        assert!(errors[1].contains("duplicate schedule name 'twice'"));
        assert!(errors[2].contains("invalid timezone 'Not/A_Timezone'"));
        assert!(errors[3].contains("schedule 'no_catch_up': max_catch_up_runs"));
        assert!(errors[4].contains("schedule 'empty_interval'"));
        assert_eq!(config.validate(), Err(errors[0].clone()));
    }

    /// Python `test_config_models_forbid_extra.py`: a misspelled key in a
    /// schedule config struct is refused, not dropped.
    #[test]
    fn unknown_keys_in_schedule_config_are_refused() {
        let refused = [
            r#"{"enabled": true, "check_intervals": 5}"#,
            r#"{"schedules": [{"name": "a", "task_name": "t",
                "pattern": {"type": "interval", "seconds": 5}, "queue": "q"}]}"#,
            r#"{"schedules": [{"name": "a", "task_name": "t",
                "pattern": {"type": "interval", "second": 5}}]}"#,
            r#"{"schedules": [{"name": "a", "task_name": "t",
                "pattern": {"type": "monthly", "day": 1, "time": "00:00:00", "days": 2}}]}"#,
            r#"{"schedules": [{"name": "a", "task_name": "t",
                "pattern": {"type": "cron", "minute": [{"kind": "step", "step": 5, "steps": 1}],
                "hour": [{"kind": "every"}], "month": [{"kind": "every"}],
                "day": {"kind": "every_day"}}}]}"#,
        ];
        for json in refused {
            let error = serde_json::from_str::<ScheduleConfig>(json)
                .expect_err("an unknown key must be refused");
            assert!(
                error.to_string().contains("unknown field"),
                "{json}: {error}"
            );
        }

        let accepted = r#"{"schedules": [{"name": "a", "task_name": "t",
            "pattern": {"type": "cron", "minute": [{"kind": "step", "step": 5}],
            "hour": [{"kind": "every"}], "month": [{"kind": "every"}],
            "day": {"kind": "every_day"}}}]}"#;
        let config = serde_json::from_str::<ScheduleConfig>(accepted).expect("known keys only");
        assert_eq!(config.schedules.len(), 1);
    }

    /// The schedule config of one schedule with the TOML `pattern`.
    fn one_schedule_toml(pattern: &str) -> String {
        format!("[[schedules]]\nname = \"a\"\ntask_name = \"t\"\npattern = {pattern}\n")
    }

    /// A TOML cron pattern with `minute`, every hour, `month` and `day`.
    fn cron_toml(minute: &str, month: &str, day: &str) -> String {
        format!(
            "{{ type = \"cron\", minute = [{minute}], hour = [{{ kind = \"every\" }}], \
             month = [{month}], day = {day} }}"
        )
    }

    /// Python `test_config_models_forbid_extra.py` for the schedule models:
    /// each variant of each tagged schedule enum ([`SchedulePattern`],
    /// [`CronNumericTerm`], [`CronEnumTerm`], [`DaySelector`]) refuses a
    /// member that it does not declare, and the error names the member. The
    /// unit variants `every` and `every_day` refuse it too, as Python's
    /// `CronEvery` and `EveryDay` do. The config is read from TOML, as
    /// `horsies web <CONFIG>` reads it, and it loads without the member.
    /// [`CronEnumTerm`] is generic over the month and the weekday; the rows
    /// use the month.
    #[test]
    fn every_variant_of_the_tagged_schedule_enums_refuses_an_unknown_member() {
        let every = r#"{ kind = "every" }"#;
        let every_day = r#"{ kind = "every_day" }"#;
        let both_lists =
            r#"day_of_month = [{ kind = "every" }], day_of_week = [{ kind = "every" }]"#;
        // Each pattern holds `EXTRA` before the closing brace of the table
        // of one variant; the row's key goes there.
        let rows: Vec<(&str, String)> = vec![
            (
                "extra_interval",
                r#"{ type = "interval", seconds = 5 EXTRA}"#.to_owned(),
            ),
            (
                "extra_hourly",
                r#"{ type = "hourly", minute = 0 EXTRA}"#.to_owned(),
            ),
            (
                "extra_daily",
                r#"{ type = "daily", time = "09:00:00" EXTRA}"#.to_owned(),
            ),
            (
                "extra_weekly",
                r#"{ type = "weekly", days = ["monday"], time = "09:00:00" EXTRA}"#.to_owned(),
            ),
            (
                "extra_monthly",
                r#"{ type = "monthly", day = 1, time = "09:00:00" EXTRA}"#.to_owned(),
            ),
            (
                "extra_cron",
                format!(
                    "{{ type = \"cron\", minute = [{every}], hour = [{every}], \
                     month = [{every}], day = {every_day} EXTRA}}"
                ),
            ),
            (
                "extra_every",
                cron_toml(r#"{ kind = "every" EXTRA}"#, every, every_day),
            ),
            (
                "extra_step",
                cron_toml(r#"{ kind = "step", step = 5 EXTRA}"#, every, every_day),
            ),
            (
                "extra_values",
                cron_toml(
                    r#"{ kind = "values", values = [0] EXTRA}"#,
                    every,
                    every_day,
                ),
            ),
            (
                "extra_range",
                cron_toml(
                    r#"{ kind = "range", start = 0, end = 5 EXTRA}"#,
                    every,
                    every_day,
                ),
            ),
            (
                "extra_enum_every",
                cron_toml(every, r#"{ kind = "every" EXTRA}"#, every_day),
            ),
            (
                "extra_enum_values",
                cron_toml(
                    every,
                    r#"{ kind = "enum_values", values = ["january"] EXTRA}"#,
                    every_day,
                ),
            ),
            (
                "extra_enum_range",
                cron_toml(
                    every,
                    r#"{ kind = "enum_range", start = "january", end = "march" EXTRA}"#,
                    every_day,
                ),
            ),
            (
                "extra_enum_step",
                cron_toml(every, r#"{ kind = "enum_step", step = 2 EXTRA}"#, every_day),
            ),
            (
                "extra_every_day",
                cron_toml(every, every, r#"{ kind = "every_day" EXTRA}"#),
            ),
            (
                "extra_by_month_day",
                cron_toml(
                    every,
                    every,
                    r#"{ kind = "by_month_day", day_of_month = [{ kind = "every" }] EXTRA}"#,
                ),
            ),
            (
                "extra_by_weekday",
                cron_toml(
                    every,
                    every,
                    r#"{ kind = "by_weekday", day_of_week = [{ kind = "every" }] EXTRA}"#,
                ),
            ),
            (
                "extra_either_day",
                cron_toml(
                    every,
                    every,
                    &format!("{{ kind = \"either_day\", {both_lists} EXTRA}}"),
                ),
            ),
            (
                "extra_both_days",
                cron_toml(
                    every,
                    every,
                    &format!("{{ kind = \"both_days\", {both_lists} EXTRA}}"),
                ),
            ),
        ];
        for (key, pattern) in rows {
            let accepted = one_schedule_toml(&pattern.replace("EXTRA", ""));
            let _config: ScheduleConfig = toml::from_str(&accepted).unwrap_or_else(|error| {
                panic!("{key}: the pattern without the member must load: {error}")
            });
            let refused = one_schedule_toml(&pattern.replace("EXTRA", &format!(", {key} = 1")));
            let error = toml::from_str::<ScheduleConfig>(&refused)
                .expect_err("an unknown member must be refused");
            let unknown_field = format!("unknown field `{key}`");
            assert!(error.to_string().contains(&unknown_field), "{key}: {error}");
        }
    }
}
