//! Data-lifecycle configuration: how long records live and how they age.
//!
//! A finite retention class is identified by its period in whole days
//! (`standard_<days>d`). A queue maps to a period, or to `None` for forever;
//! `retention_periods` lists the other periods a send may choose. The
//! maintenance owner registers the class of every period this config can
//! stamp.

use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::Duration;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

use crate::core::history::ddl::classes::{
    DEFAULT_RETENTION_CLASS_KEY, DEFAULT_RETENTION_DAYS, FOREVER_CLASS_KEY,
};
use crate::core::history::periods::{
    PeriodRefused, RetentionDays, MAX_RETENTION_DAYS, MIN_RETENTION_DAYS, PERIOD_CLASS_PREFIX,
};

/// The send-time retention choice that overrides the queue mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionChoice {
    /// A finite period; it must be one of `RetentionConfig::sendable_periods`.
    Period(RetentionDays),
    Forever,
}

impl RetentionChoice {
    /// The choice of a finite period, or why the duration is not a period.
    pub fn period(period: Duration) -> Result<Self, PeriodRefused> {
        RetentionDays::parse_period(period).map(Self::Period)
    }
}

/// The retention class of one enqueue (Python's broker rule: a call that
/// names no class stores the default class `standard_30d`; an explicit `None`
/// stores forever).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueRetention {
    /// The call names no class: the default class, `standard_30d`.
    Default,
    /// The record is kept forever.
    Forever,
    /// A finite period of whole days.
    Period(RetentionDays),
}

impl EnqueueRetention {
    /// The class key that the task row stores.
    pub fn class_key(self) -> String {
        match self {
            Self::Default => DEFAULT_RETENTION_CLASS_KEY.to_owned(),
            Self::Forever => FOREVER_CLASS_KEY.to_owned(),
            Self::Period(days) => days.class_key(),
        }
    }

    /// The retention of a class key, by the rule that derives each key
    /// ([`EnqueueRetention::class_key`]): `forever`, or `standard_<days>d` of
    /// a period of whole days within the period bounds, written as
    /// [`RetentionDays::class_key`] writes it (no sign, no leading zero). The
    /// baseline CHECK `horsies_tasks_retention_class_key_form` holds the same
    /// form. `standard_30d` is the period of the default class. Pure.
    ///
    /// Errors: [`RetentionClassKeyRefused`] for any other text.
    pub fn from_class_key(key: &str) -> Result<Self, RetentionClassKeyRefused> {
        if key == FOREVER_CLASS_KEY {
            return Ok(Self::Forever);
        }
        let period = key
            .strip_prefix(PERIOD_CLASS_PREFIX)
            .and_then(|rest| rest.strip_suffix('d'))
            .and_then(|days| days.parse::<u16>().ok())
            .and_then(|days| RetentionDays::parse_period(Duration::days(i64::from(days))).ok())
            .filter(|days| days.class_key() == key);
        match period {
            Some(days) => Ok(Self::Period(days)),
            None => Err(RetentionClassKeyRefused {
                key: key.to_owned(),
            }),
        }
    }
}

/// A text that is not the key of a retention class. Nothing was written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "retention class key {key:?} is neither {FOREVER_CLASS_KEY:?} nor standard_<days>d for a period of {MIN_RETENTION_DAYS} to {MAX_RETENTION_DAYS} whole days"
)]
pub struct RetentionClassKeyRefused {
    pub key: String,
}

/// The age past which a PAUSED workflow expires: a whole number of
/// microseconds from 1 to [`PausedWorkflowAge::MAX_MICROS`] (Python
/// `RetentionConfig.paused_workflow_cancel_after`, a positive
/// `timedelta`, which holds whole microseconds only).
///
/// Only [`PausedWorkflowAge::parse`] builds it, and the config reader calls
/// it when the value enters. The expiry sweep sends the age to the database
/// as a count of microseconds, so it takes this type and has no refusal of
/// its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PausedWorkflowAge {
    micros: i64,
}

impl PausedWorkflowAge {
    /// The longest age, in days: 365,000 days (1,000 years of 365 days).
    ///
    /// The expiry reads `updated_at < now() - <age>`. PostgreSQL's lowest
    /// `timestamptz` is 4714-11-24 BC, so with this bound `now() - <age>` stays
    /// inside the timestamp range for any `now()` from year 1 AD on, and the
    /// interval stays far inside the interval range: the expiry has no
    /// out-of-range failure.
    pub const MAX_DAYS: i64 = 365_000;

    /// [`PausedWorkflowAge::MAX_DAYS`] in microseconds.
    pub const MAX_MICROS: i64 = Self::MAX_DAYS * MICROS_PER_DAY;

    /// The age of `age`. Pure.
    ///
    /// Errors: [`PausedAgeRefused`] for an age of zero or less, an age with
    /// a part below one microsecond, or an age longer than
    /// [`PausedWorkflowAge::MAX_DAYS`].
    pub fn parse(age: Duration) -> Result<Self, PausedAgeRefused> {
        if age <= Duration::zero() {
            return Err(PausedAgeRefused::NotPositive { age });
        }
        if age.subsec_nanos() % NANOS_PER_MICROSECOND != 0 {
            return Err(PausedAgeRefused::NotWholeMicroseconds { age });
        }
        match age.num_microseconds() {
            Some(micros) if micros <= Self::MAX_MICROS => Ok(Self { micros }),
            Some(_) | None => Err(PausedAgeRefused::TooLong { age }),
        }
    }

    /// The age in microseconds, above zero.
    pub fn micros(self) -> i64 {
        self.micros
    }

    /// The age as a duration.
    pub fn duration(self) -> Duration {
        Duration::microseconds(self.micros)
    }
}

impl Serialize for PausedWorkflowAge {
    /// The duration form that the config reader reads.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.duration().serialize(serializer)
    }
}

/// Nanoseconds in one microsecond.
const NANOS_PER_MICROSECOND: i32 = 1_000;

/// Microseconds in one day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Why a duration is not a paused-workflow age. Nothing reads the value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PausedAgeRefused {
    #[error(
        "paused_workflow_cancel_after must be positive, got {age}; use None to disable the sweep"
    )]
    NotPositive { age: Duration },
    #[error(
        "paused_workflow_cancel_after ({age}) has a part below one microsecond; the expiry compares ages in whole microseconds"
    )]
    NotWholeMicroseconds { age: Duration },
    #[error(
        "paused_workflow_cancel_after ({age}) is longer than {max_days} days; the expiry subtracts the age from now(), and the result must stay inside PostgreSQL's timestamp range. Use at most {max_days} days, or None to disable the sweep",
        max_days = PausedWorkflowAge::MAX_DAYS
    )]
    TooLong { age: Duration },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RetentionConfig {
    pub worker_state_retention_hours: Option<u32>,
    pub terminal_record_retention_hours: Option<u32>,
    /// `None` turns the expiry sweep off.
    pub paused_workflow_cancel_after: Option<PausedWorkflowAge>,
    pub history_leaf_horizon_days: u32,
    pub heartbeat_leaf_horizon_hours: u32,
    /// Finite periods a send may choose besides the 30-day default and the
    /// mapped periods. Whole days, 1 to 1825.
    pub retention_periods: Vec<Duration>,
    /// Per-queue retention: a period in whole days, 1 to 1825, or `None` to
    /// keep the history record forever. An unmapped queue uses the default.
    pub queue_retention: HashMap<String, Option<Duration>>,
    pub partition_maintenance_interval_seconds: u64,
    pub retention_sweep_interval_seconds: u64,
    pub retention_delete_batch_size: u32,
}

fn default_worker_state_retention_hours() -> Option<u32> {
    Some(24 * 7)
}

fn default_terminal_record_retention_hours() -> Option<u32> {
    Some(24 * 30)
}

fn default_history_leaf_horizon_days() -> u32 {
    3
}

fn default_heartbeat_leaf_horizon_hours() -> u32 {
    6
}

fn default_partition_maintenance_interval_seconds() -> u64 {
    900
}

fn default_retention_sweep_interval_seconds() -> u64 {
    300
}

fn default_retention_delete_batch_size() -> u32 {
    500
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            worker_state_retention_hours: default_worker_state_retention_hours(),
            terminal_record_retention_hours: default_terminal_record_retention_hours(),
            paused_workflow_cancel_after: None,
            history_leaf_horizon_days: default_history_leaf_horizon_days(),
            heartbeat_leaf_horizon_hours: default_heartbeat_leaf_horizon_hours(),
            retention_periods: Vec::new(),
            queue_retention: HashMap::new(),
            partition_maintenance_interval_seconds: default_partition_maintenance_interval_seconds(
            ),
            retention_sweep_interval_seconds: default_retention_sweep_interval_seconds(),
            retention_delete_batch_size: default_retention_delete_batch_size(),
        }
    }
}

/// The refusal of the named classes that periods replaced.
const RETENTION_CLASSES_REMOVED: &str = "retention_classes was removed: a retention class is identified by its period in whole days. Map a queue to the period in queue_retention, or list a period that sends choose explicitly in retention_periods";

/// Every field of `RetentionConfig`. An unknown key is refused, as Python's
/// config models refuse extra keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionConfigWire {
    #[serde(default = "default_worker_state_retention_hours")]
    worker_state_retention_hours: Option<u32>,
    #[serde(default = "default_terminal_record_retention_hours")]
    terminal_record_retention_hours: Option<u32>,
    #[serde(default)]
    paused_workflow_cancel_after: Option<Duration>,
    #[serde(default = "default_history_leaf_horizon_days")]
    history_leaf_horizon_days: u32,
    #[serde(default = "default_heartbeat_leaf_horizon_hours")]
    heartbeat_leaf_horizon_hours: u32,
    #[serde(default)]
    retention_periods: Vec<Duration>,
    #[serde(default)]
    queue_retention: HashMap<String, Option<Duration>>,
    #[serde(default = "default_partition_maintenance_interval_seconds")]
    partition_maintenance_interval_seconds: u64,
    #[serde(default = "default_retention_sweep_interval_seconds")]
    retention_sweep_interval_seconds: u64,
    #[serde(default = "default_retention_delete_batch_size")]
    retention_delete_batch_size: u32,
}

impl<'de> Deserialize<'de> for RetentionConfig {
    /// Refuses `retention_classes`: a configuration that still declares named
    /// classes is corrected, never silently ignored. Refuses a paused age
    /// that [`PausedWorkflowAge::parse`] refuses
    /// ([`RetentionConfigError::PausedExpiryRefused`]).
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value
            .as_object()
            .is_some_and(|fields| fields.contains_key("retention_classes"))
        {
            return Err(D::Error::custom(RETENTION_CLASSES_REMOVED));
        }
        let wire: RetentionConfigWire = serde_json::from_value(value).map_err(D::Error::custom)?;
        let paused_workflow_cancel_after = match wire.paused_workflow_cancel_after {
            None => None,
            Some(age) => Some(PausedWorkflowAge::parse(age).map_err(|refused| {
                D::Error::custom(RetentionConfigError::PausedExpiryRefused { refused })
            })?),
        };
        Ok(Self {
            worker_state_retention_hours: wire.worker_state_retention_hours,
            terminal_record_retention_hours: wire.terminal_record_retention_hours,
            paused_workflow_cancel_after,
            history_leaf_horizon_days: wire.history_leaf_horizon_days,
            heartbeat_leaf_horizon_hours: wire.heartbeat_leaf_horizon_hours,
            retention_periods: wire.retention_periods,
            queue_retention: wire.queue_retention,
            partition_maintenance_interval_seconds: wire.partition_maintenance_interval_seconds,
            retention_sweep_interval_seconds: wire.retention_sweep_interval_seconds,
            retention_delete_batch_size: wire.retention_delete_batch_size,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RetentionConfigError {
    #[error("{field} ({value}) must be within {min}..={max}")]
    OutOfRange {
        field: &'static str,
        value: u64,
        min: u64,
        max: u64,
    },
    /// The config reader refused the paused-workflow age.
    #[error("{refused}")]
    PausedExpiryRefused { refused: PausedAgeRefused },
    #[error(
        "queue {queue:?} maps to a period that is not one: {refused}; use whole days from 1 to 1825, or None to keep records forever"
    )]
    QueuePeriodRefused {
        queue: String,
        refused: PeriodRefused,
    },
    #[error(
        "retention period refused: {refused}; use whole days from 1 to 1825, or None to keep records forever"
    )]
    PeriodRefused { refused: PeriodRefused },
    #[error("retention period of {days} days listed twice: a period has exactly one class")]
    DuplicatePeriod { days: u16 },
    #[error("queue_retention references unknown queue {queue:?}")]
    UnknownQueue { queue: String },
    #[error(
        "retention of {days} days is not a period this deployment registers; omit it for the queue mapping or the 30-day default, choose forever, or choose one of {allowed:?} days. Add the period to retention_periods to send with it"
    )]
    UnregisteredPeriod { days: u16, allowed: Vec<u16> },
}

impl RetentionConfig {
    pub fn validate(&self) -> Vec<RetentionConfigError> {
        let mut errors = Vec::new();
        validate_optional_range(
            &mut errors,
            "worker_state_retention_hours",
            self.worker_state_retention_hours,
            1,
            24 * 365,
        );
        validate_optional_range(
            &mut errors,
            "terminal_record_retention_hours",
            self.terminal_record_retention_hours,
            1,
            24 * 365 * 5,
        );
        validate_range(
            &mut errors,
            "history_leaf_horizon_days",
            u64::from(self.history_leaf_horizon_days),
            2,
            14,
        );
        validate_range(
            &mut errors,
            "heartbeat_leaf_horizon_hours",
            u64::from(self.heartbeat_leaf_horizon_hours),
            2,
            48,
        );
        validate_range(
            &mut errors,
            "partition_maintenance_interval_seconds",
            self.partition_maintenance_interval_seconds,
            60,
            3_600,
        );
        validate_range(
            &mut errors,
            "retention_sweep_interval_seconds",
            self.retention_sweep_interval_seconds,
            30,
            86_400,
        );
        validate_range(
            &mut errors,
            "retention_delete_batch_size",
            u64::from(self.retention_delete_batch_size),
            50,
            10_000,
        );
        // A queue name is only a lookup key: the class key holds the period
        // alone, so any configured queue name can be mapped.
        let mut mappings: Vec<_> = self.queue_retention.iter().collect();
        mappings.sort_by_key(|(queue, _)| queue.as_str());
        for (queue, period) in mappings {
            let Some(period) = period else {
                continue;
            };
            if let Err(refused) = RetentionDays::parse_period(*period) {
                errors.push(RetentionConfigError::QueuePeriodRefused {
                    queue: queue.clone(),
                    refused,
                });
            }
        }

        let mut listed = HashSet::new();
        for period in &self.retention_periods {
            match RetentionDays::parse_period(*period) {
                Err(refused) => errors.push(RetentionConfigError::PeriodRefused { refused }),
                Ok(days) if !listed.insert(days) => {
                    errors.push(RetentionConfigError::DuplicatePeriod { days: days.get() });
                }
                Ok(_) => {}
            }
        }
        errors
    }

    pub fn validate_queues<'a>(
        &self,
        declared_queues: impl IntoIterator<Item = &'a str>,
    ) -> Vec<RetentionConfigError> {
        let declared: HashSet<&str> = declared_queues.into_iter().collect();
        let mut unknown: Vec<_> = self
            .queue_retention
            .keys()
            .filter(|queue| !declared.contains(queue.as_str()))
            .cloned()
            .collect();
        unknown.sort();
        unknown
            .into_iter()
            .map(|queue| RetentionConfigError::UnknownQueue { queue })
            .collect()
    }

    /// The class a send on this queue stamps when it chooses none: the mapped
    /// period's class, `None` for a queue mapped to forever, or the default
    /// class for an unmapped queue.
    ///
    /// The periods come from a config that `validate` accepted; a period that
    /// fails to parse here was changed after validation, which is a defect.
    pub fn resolve_queue_retention_class(&self, queue: &str) -> Option<String> {
        match self.queue_retention.get(queue) {
            None => Some(DEFAULT_RETENTION_CLASS_KEY.to_owned()),
            Some(None) => None,
            Some(Some(period)) => Some(validated_days(*period).class_key()),
        }
    }

    /// The retention of one enqueue on this queue when the send chooses
    /// none: the default class for an unmapped queue, forever for a queue
    /// mapped to `None`, or the mapped period.
    pub fn resolve_queue_retention(&self, queue_name: &str) -> EnqueueRetention {
        match self.queue_retention.get(queue_name) {
            None => EnqueueRetention::Default,
            Some(None) => EnqueueRetention::Forever,
            Some(Some(period)) => EnqueueRetention::Period(validated_days(*period)),
        }
    }

    /// The class one send stamps: the queue mapping when the send chooses
    /// none, `None` for forever, or the class of a sendable period.
    pub fn resolve_choice(
        &self,
        queue_name: &str,
        choice: Option<&RetentionChoice>,
    ) -> Result<Option<String>, RetentionConfigError> {
        match choice {
            None => Ok(self.resolve_queue_retention_class(queue_name)),
            Some(RetentionChoice::Forever) => Ok(None),
            Some(RetentionChoice::Period(days)) => {
                let allowed = self.sendable_periods();
                match allowed.contains(days) {
                    true => Ok(Some(days.class_key())),
                    false => Err(RetentionConfigError::UnregisteredPeriod {
                        days: days.get(),
                        allowed: allowed.iter().map(|days| days.get()).collect(),
                    }),
                }
            }
        }
    }

    /// The finite periods a send may choose explicitly: the default, every
    /// mapped period, and `retention_periods`. These are the periods whose
    /// classes this process's maintenance owner registers.
    pub fn sendable_periods(&self) -> BTreeSet<RetentionDays> {
        let mut periods = self.mapped_and_listed_periods();
        periods.insert(DEFAULT_RETENTION_DAYS);
        periods
    }

    /// Every finite period the maintenance owner must register besides the
    /// default, which the library registers: the distinct mapped and listed
    /// periods, in day order. Queues with the same period share one class.
    pub fn registrable_periods(&self) -> Vec<RetentionDays> {
        self.mapped_and_listed_periods()
            .into_iter()
            .filter(|days| *days != DEFAULT_RETENTION_DAYS)
            .collect()
    }

    fn mapped_and_listed_periods(&self) -> BTreeSet<RetentionDays> {
        self.queue_retention
            .values()
            .flatten()
            .chain(self.retention_periods.iter())
            .map(|period| validated_days(*period))
            .collect()
    }
}

/// The days of a period that `RetentionConfig::validate` accepted.
fn validated_days(period: Duration) -> RetentionDays {
    match RetentionDays::parse_period(period) {
        Ok(days) => days,
        Err(refused) => panic!("retention period changed after validation: {refused}"),
    }
}

fn validate_optional_range(
    errors: &mut Vec<RetentionConfigError>,
    field: &'static str,
    value: Option<u32>,
    min: u64,
    max: u64,
) {
    if let Some(value) = value {
        validate_range(errors, field, u64::from(value), min, max);
    }
}

fn validate_range(
    errors: &mut Vec<RetentionConfigError>,
    field: &'static str,
    value: u64,
    min: u64,
    max: u64,
) {
    if !(min..=max).contains(&value) {
        errors.push(RetentionConfigError::OutOfRange {
            field,
            value,
            min,
            max,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_class_key_reads_back_as_the_retention_that_derives_it() {
        assert_eq!(
            EnqueueRetention::from_class_key("forever"),
            Ok(EnqueueRetention::Forever)
        );
        for days in [1, 30, 1825] {
            let key = format!("standard_{days}d");
            let retention = EnqueueRetention::from_class_key(&key).expect("a period key reads");
            assert_eq!(retention.class_key(), key);
        }
        assert_eq!(
            EnqueueRetention::from_class_key("standard_30d").map(EnqueueRetention::class_key),
            Ok(EnqueueRetention::Default.class_key())
        );
    }

    #[test]
    fn a_text_that_no_class_derives_is_refused() {
        for key in [
            "",
            "Forever",
            "standard_0d",
            "standard_1826d",
            "standard_030d",
            "standard_+30d",
            "standard_30",
            "standard_d",
            "standard_99999999999d",
            "STANDARD_30d",
            "standard_30d ",
        ] {
            let refused = EnqueueRetention::from_class_key(key).expect_err(key);
            assert_eq!(refused.key, key);
        }
    }

    #[test]
    fn defaults_and_all_bounds_match_the_retention_contract() {
        let config = RetentionConfig::default();
        assert!(config.validate().is_empty());
        assert_eq!(config.worker_state_retention_hours, Some(168));
        assert_eq!(config.terminal_record_retention_hours, Some(720));
        assert_eq!(config.history_leaf_horizon_days, 3);
        assert_eq!(config.heartbeat_leaf_horizon_hours, 6);
        assert_eq!(config.partition_maintenance_interval_seconds, 900);
        assert_eq!(config.retention_sweep_interval_seconds, 300);
        assert_eq!(config.retention_delete_batch_size, 500);
        assert_eq!(config.paused_workflow_cancel_after, None);

        // The paused age is not here: its type holds its bound, and the
        // config reader refuses an age outside it
        // (`a_paused_age_outside_its_bound_is_refused_when_the_config_is_read`).
        let invalid = RetentionConfig {
            worker_state_retention_hours: Some(0),
            terminal_record_retention_hours: Some(43_801),
            history_leaf_horizon_days: 1,
            heartbeat_leaf_horizon_hours: 49,
            partition_maintenance_interval_seconds: 59,
            retention_sweep_interval_seconds: 86_401,
            retention_delete_batch_size: 49,
            ..Default::default()
        };
        assert_eq!(invalid.validate().len(), 7);
    }

    /// A paused age is a whole number of microseconds above zero, kept
    /// exact: a part below one millisecond stays, one day is
    /// 86 400 000 000 microseconds, and the bound itself is an age.
    #[test]
    fn a_paused_age_is_whole_microseconds_above_zero() {
        for (age, micros) in [
            (Duration::microseconds(1), 1),
            (Duration::microseconds(500), 500),
            (Duration::microseconds(1_500), 1_500),
            (Duration::hours(1), 3_600_000_000),
            (Duration::days(1), 86_400_000_000),
            (
                Duration::days(PausedWorkflowAge::MAX_DAYS),
                31_536_000_000_000_000,
            ),
        ] {
            let parsed = PausedWorkflowAge::parse(age).expect("an age of whole microseconds");
            assert_eq!(parsed.micros(), micros, "{age}");
            assert_eq!(parsed.duration(), age, "{age}");
        }
        assert_eq!(PausedWorkflowAge::MAX_MICROS, 31_536_000_000_000_000);
    }

    /// The bound keeps `now() - <age>` inside PostgreSQL's timestamp range
    /// for any `now()` from year 1 AD on. PostgreSQL's lowest `timestamptz`
    /// is 4714-11-24 BC (Julian day 0); chrono counts years
    /// astronomically, so 4714 BC is year -4713.
    #[test]
    fn the_longest_paused_age_stays_inside_the_timestamp_range() {
        let lowest_postgres_date =
            chrono::NaiveDate::from_ymd_opt(-4713, 11, 24).expect("a proleptic Gregorian date");
        let year_one = chrono::NaiveDate::from_ymd_opt(1, 1, 1).expect("a date");
        let earliest_cutoff = year_one - Duration::days(PausedWorkflowAge::MAX_DAYS);
        assert!(
            earliest_cutoff > lowest_postgres_date,
            "{earliest_cutoff} is before {lowest_postgres_date}"
        );
    }

    /// An age that the expiry cannot compare exactly in microseconds, or
    /// that is longer than the bound, is refused, each with its own reason.
    #[test]
    fn a_paused_age_outside_its_bound_is_refused() {
        for age in [Duration::zero(), Duration::microseconds(-1)] {
            assert_eq!(
                PausedWorkflowAge::parse(age),
                Err(PausedAgeRefused::NotPositive { age }),
            );
        }
        for age in [
            Duration::nanoseconds(1),
            Duration::microseconds(1_500) + Duration::nanoseconds(1),
            Duration::hours(1) + Duration::nanoseconds(999),
        ] {
            assert_eq!(
                PausedWorkflowAge::parse(age),
                Err(PausedAgeRefused::NotWholeMicroseconds { age }),
            );
        }
        for age in [
            Duration::days(PausedWorkflowAge::MAX_DAYS) + Duration::microseconds(1),
            Duration::days(PausedWorkflowAge::MAX_DAYS + 1),
            Duration::MAX,
        ] {
            assert_eq!(
                PausedWorkflowAge::parse(age),
                Err(PausedAgeRefused::TooLong { age }),
            );
        }
    }

    /// The config reader is where the paused age enters: an age with a part
    /// of 1 ns is refused there, with the named error, before any worker
    /// starts. A whole-microsecond age reads back exactly, and the written
    /// config reads back equal.
    #[test]
    fn a_paused_age_outside_its_bound_is_refused_when_the_config_is_read() {
        // chrono's duration form is [seconds, nanoseconds].
        let refused = serde_json::from_value::<RetentionConfig>(serde_json::json!({
            "paused_workflow_cancel_after": [3_600, 1],
        }))
        .unwrap_err();
        let expected = RetentionConfigError::PausedExpiryRefused {
            refused: PausedAgeRefused::NotWholeMicroseconds {
                age: Duration::hours(1) + Duration::nanoseconds(1),
            },
        };
        assert!(
            refused.to_string().contains(&expected.to_string()),
            "{refused}"
        );
        assert!(
            refused.to_string().contains("below one microsecond"),
            "{refused}"
        );

        let zero = serde_json::from_value::<RetentionConfig>(serde_json::json!({
            "paused_workflow_cancel_after": [0, 0],
        }))
        .unwrap_err();
        assert!(zero.to_string().contains("must be positive"), "{zero}");

        // One microsecond above the bound of 365,000 days.
        let too_long = serde_json::from_value::<RetentionConfig>(serde_json::json!({
            "paused_workflow_cancel_after": [31_536_000_000_i64, 1_000],
        }))
        .unwrap_err();
        let expected = RetentionConfigError::PausedExpiryRefused {
            refused: PausedAgeRefused::TooLong {
                age: Duration::days(PausedWorkflowAge::MAX_DAYS) + Duration::microseconds(1),
            },
        };
        assert!(
            too_long.to_string().contains(&expected.to_string()),
            "{too_long}"
        );
        assert!(
            too_long.to_string().contains("longer than 365000 days"),
            "{too_long}"
        );
        let longest: RetentionConfig = serde_json::from_value(serde_json::json!({
            "paused_workflow_cancel_after": [31_536_000_000_i64, 0],
        }))
        .expect("the longest age");
        assert_eq!(
            longest
                .paused_workflow_cancel_after
                .map(PausedWorkflowAge::micros),
            Some(PausedWorkflowAge::MAX_MICROS)
        );

        let read: RetentionConfig = serde_json::from_value(serde_json::json!({
            "paused_workflow_cancel_after": [3_600, 250_000],
        }))
        .expect("an age of whole microseconds");
        let age = read.paused_workflow_cancel_after.expect("the age is set");
        assert_eq!(age.micros(), 3_600_000_250);
        let written = serde_json::to_value(&read).expect("a config writes");
        let read_again: RetentionConfig =
            serde_json::from_value(written).expect("the written config reads");
        assert_eq!(read_again, read);
    }

    #[test]
    fn every_numeric_boundary_is_inclusive_and_one_step_out_is_refused() {
        let cases: &[(&str, u64, u64)] = &[
            ("worker_state_retention_hours", 1, 8_760),
            ("terminal_record_retention_hours", 1, 43_800),
            ("history_leaf_horizon_days", 2, 14),
            ("heartbeat_leaf_horizon_hours", 2, 48),
            ("partition_maintenance_interval_seconds", 60, 3_600),
            ("retention_sweep_interval_seconds", 30, 86_400),
            ("retention_delete_batch_size", 50, 10_000),
        ];
        for &(field, min, max) in cases {
            for value in [min, max] {
                let mut config = RetentionConfig::default();
                set_numeric(&mut config, field, value);
                assert!(
                    config.validate().is_empty(),
                    "{field}={value} must be accepted",
                );
            }
            for value in [min - 1, max + 1] {
                let mut config = RetentionConfig::default();
                set_numeric(&mut config, field, value);
                assert!(config.validate().iter().any(|error| matches!(
                    error,
                    RetentionConfigError::OutOfRange { field: found, value: found_value, .. }
                        if *found == field && *found_value == value
                )));
            }
        }

        let disabled = RetentionConfig {
            worker_state_retention_hours: None,
            terminal_record_retention_hours: None,
            ..RetentionConfig::default()
        };
        assert!(disabled.validate().is_empty());
    }

    fn set_numeric(config: &mut RetentionConfig, field: &str, value: u64) {
        match field {
            "worker_state_retention_hours" => {
                config.worker_state_retention_hours = Some(value as u32);
            }
            "terminal_record_retention_hours" => {
                config.terminal_record_retention_hours = Some(value as u32);
            }
            "history_leaf_horizon_days" => config.history_leaf_horizon_days = value as u32,
            "heartbeat_leaf_horizon_hours" => config.heartbeat_leaf_horizon_hours = value as u32,
            "partition_maintenance_interval_seconds" => {
                config.partition_maintenance_interval_seconds = value;
            }
            "retention_sweep_interval_seconds" => config.retention_sweep_interval_seconds = value,
            "retention_delete_batch_size" => config.retention_delete_batch_size = value as u32,
            unknown => panic!("unhandled retention boundary field {unknown}"),
        }
    }

    fn days(count: i64) -> RetentionDays {
        RetentionDays::parse_period(Duration::days(count)).unwrap()
    }

    #[test]
    fn periods_and_queue_mappings_fail_closed() {
        let config = RetentionConfig {
            retention_periods: vec![Duration::days(7), Duration::days(7), Duration::days(1826)],
            queue_retention: HashMap::from([
                ("bulk".to_owned(), Some(Duration::hours(36))),
                ("empty".to_owned(), Some(Duration::zero())),
                ("forever_queue".to_owned(), None),
                ("week".to_owned(), Some(Duration::days(7))),
            ]),
            ..Default::default()
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 4, "{errors:?}");
        assert!(errors.contains(&RetentionConfigError::QueuePeriodRefused {
            queue: "bulk".to_owned(),
            refused: PeriodRefused::NotWholeDays {
                period: Duration::hours(36)
            },
        }));
        assert!(errors.contains(&RetentionConfigError::QueuePeriodRefused {
            queue: "empty".to_owned(),
            refused: PeriodRefused::TooShort {
                period: Duration::zero()
            },
        }));
        assert!(errors.contains(&RetentionConfigError::DuplicatePeriod { days: 7 }));
        assert!(errors.contains(&RetentionConfigError::PeriodRefused {
            refused: PeriodRefused::TooLong {
                period: Duration::days(1826)
            },
        }));
    }

    #[test]
    fn precedence_sendable_and_registrable_periods_are_exact() {
        let config = RetentionConfig {
            retention_periods: vec![Duration::days(7)],
            queue_retention: HashMap::from([
                ("bulk".to_owned(), Some(Duration::days(2))),
                ("shared".to_owned(), Some(Duration::days(2))),
                ("audit".to_owned(), None),
            ]),
            ..Default::default()
        };
        assert!(config.validate().is_empty());
        assert_eq!(
            config.resolve_choice("default", None).unwrap().as_deref(),
            Some(DEFAULT_RETENTION_CLASS_KEY)
        );
        assert_eq!(
            config.resolve_choice("bulk", None).unwrap().as_deref(),
            Some("standard_2d")
        );
        assert_eq!(config.resolve_choice("audit", None).unwrap(), None);
        assert_eq!(
            config
                .resolve_choice("bulk", Some(&RetentionChoice::Period(days(30))))
                .unwrap()
                .as_deref(),
            Some(DEFAULT_RETENTION_CLASS_KEY)
        );
        assert_eq!(
            config
                .resolve_choice("bulk", Some(&RetentionChoice::Forever))
                .unwrap(),
            None
        );
        for (period, key) in [(7, "standard_7d"), (2, "standard_2d")] {
            assert_eq!(
                config
                    .resolve_choice("default", Some(&RetentionChoice::Period(days(period))))
                    .unwrap()
                    .as_deref(),
                Some(key),
            );
        }
        assert_eq!(
            config.resolve_choice("default", Some(&RetentionChoice::Period(days(9)))),
            Err(RetentionConfigError::UnregisteredPeriod {
                days: 9,
                allowed: vec![2, 7, 30],
            })
        );
        assert_eq!(
            RetentionChoice::period(Duration::hours(12)),
            Err(PeriodRefused::NotWholeDays {
                period: Duration::hours(12)
            })
        );
        assert_eq!(
            config
                .sendable_periods()
                .into_iter()
                .map(RetentionDays::get)
                .collect::<Vec<_>>(),
            vec![2, 7, 30]
        );
        assert_eq!(config.registrable_periods(), vec![days(2), days(7)]);
    }

    #[test]
    fn unknown_keys_are_refused_when_the_config_is_read() {
        let refused = serde_json::from_value::<RetentionConfig>(serde_json::json!({
            "history_leaf_horizon_dayz": 3,
        }))
        .unwrap_err();
        assert!(
            refused.to_string().contains("history_leaf_horizon_dayz"),
            "{refused}"
        );
    }

    #[test]
    fn named_classes_are_refused_when_the_config_is_read() {
        let refused = serde_json::from_value::<RetentionConfig>(serde_json::json!({
            "retention_classes": [],
        }))
        .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("retention_classes was removed"),
            "{refused}"
        );
        let read: RetentionConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(read, RetentionConfig::default());
    }
}
