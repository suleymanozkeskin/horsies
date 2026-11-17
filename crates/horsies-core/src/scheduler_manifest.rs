//! The typed manifest that a client passes to the core to run one
//! scheduler.
//!
//! The manifest holds the scheduler switches and every configured schedule.
//! An enabled schedule holds its pattern, its timezone, its catch-up rule and
//! its send facts, which the client resolved: the task, the queue and its
//! priority, the input text, the task's options text, the retention class
//! key and the rerun-input posture. A disabled schedule holds its name only:
//! the start reads it only to tell a state row of this config from a state
//! row of another scheduler.
//!
//! As the other manifests ([`crate::manifest`]), it holds values only: no
//! field has a default, and an unknown field is refused. The parser refuses
//! what the client never builds, with the field path and never a value: a
//! pattern text that is not a pattern of the core or that breaks the
//! pattern's rules, a retention class key that is not a class key, a
//! repeated schedule name. A timezone that the core does not know is not a
//! parse refusal: the client checked the name with its own zone database, so
//! the start refuses that schedule
//! (`SchedulerStartError::UnknownTimezone`).
//!
//! `pattern_json` is the JSON text of the client's pattern model, in the
//! form of Python's `pattern.model_dump(mode='json')`: an object with a
//! `type` key and every field of the pattern, `null` for an unset optional
//! field, a time of day as `HH:MM:SS`. The zone of a schedule comes only
//! from its `timezone`, and slots fire at whole seconds: the parser refuses
//! a time of day with a UTC offset or with a fraction of a second, as the
//! Python config refuses a `time` with `tzinfo` or with microseconds.

use std::collections::HashSet;

use serde::Deserialize;

use crate::core::config::bounded::BoundedU32;
use crate::core::config::schedule::{MAX_CATCH_UP_RUNS_MAX, MAX_CATCH_UP_RUNS_MIN};
use crate::core::config::{EnqueueRetention, SchedulePattern};
use crate::manifest::{parse_manifest, ManifestRefused};
use crate::manifest_schema::{IntBound, ManifestValue, PythonValue};
use crate::worker_manifest::QueuePriority;

/// The one scheduler manifest version this core reads.
pub const SCHEDULER_MANIFEST_VERSION: u32 = 1;

/// The shortest wait between two ticks, in seconds (Python
/// `ScheduleConfig.check_interval_seconds`, `Field(ge=1, le=60)`).
pub const MIN_CHECK_INTERVAL_SECONDS: u32 = 1;

/// The longest wait between two ticks, in seconds.
pub const MAX_CHECK_INTERVAL_SECONDS: u32 = 60;

/// The manifest version. Only the parser constructs it, and it accepts only
/// [`SCHEDULER_MANIFEST_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "u32")]
pub struct SchedulerManifestVersion(u32);

impl SchedulerManifestVersion {
    /// The version this core reads.
    pub const CURRENT: Self = Self(SCHEDULER_MANIFEST_VERSION);

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for SchedulerManifestVersion {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            SCHEDULER_MANIFEST_VERSION => Ok(Self(value)),
            other => Err(format!(
                "scheduler manifest version {other} is not supported; this core reads version \
                 {SCHEDULER_MANIFEST_VERSION}"
            )),
        }
    }
}

impl ManifestValue for SchedulerManifestVersion {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "MANIFEST_VERSION",
        min: SCHEDULER_MANIFEST_VERSION as u64,
        max: SCHEDULER_MANIFEST_VERSION as u64,
    });
}

/// The wait between two ticks, in whole seconds: 1 to 60.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct CheckIntervalSeconds(
    pub BoundedU32<MIN_CHECK_INTERVAL_SECONDS, MAX_CHECK_INTERVAL_SECONDS>,
);

impl ManifestValue for CheckIntervalSeconds {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "CHECK_INTERVAL_SECONDS",
        min: MIN_CHECK_INTERVAL_SECONDS as u64,
        max: MAX_CHECK_INTERVAL_SECONDS as u64,
    });
}

/// The most slots that one fire of a catch-up schedule sends: 1 to 10,000
/// (Python `TaskSchedule.max_catch_up_runs`, `Field(ge=1, le=10000)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct CatchUpRuns(pub BoundedU32<MAX_CATCH_UP_RUNS_MIN, MAX_CATCH_UP_RUNS_MAX>);

impl ManifestValue for CatchUpRuns {
    const PYTHON: PythonValue = PythonValue::Int(IntBound {
        name: "MAX_CATCH_UP_RUNS",
        min: MAX_CATCH_UP_RUNS_MIN as u64,
        max: MAX_CATCH_UP_RUNS_MAX as u64,
    });
}

/// A schedule pattern, parsed from its JSON text. The parser refuses text
/// that is not a pattern of the core (a time of day with a UTC offset
/// included), that has a field the pattern does not have, or whose pattern
/// breaks its rules ([`SchedulePattern::validate`]; a time of day with a
/// fraction of a second included).
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "String")]
pub struct PatternJson(SchedulePattern);

impl PatternJson {
    /// The parsed pattern.
    pub fn pattern(&self) -> &SchedulePattern {
        &self.0
    }

    /// The parsed pattern, by value.
    pub fn into_pattern(self) -> SchedulePattern {
        self.0
    }
}

impl TryFrom<String> for PatternJson {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        let pattern: SchedulePattern = serde_json::from_str(&text)
            .map_err(|error| format!("the text is not a schedule pattern: {error}"))?;
        pattern
            .validate()
            .map_err(|broken| format!("the pattern breaks its rules: {broken}"))?;
        Ok(Self(pattern))
    }
}

impl ManifestValue for PatternJson {
    const PYTHON: PythonValue = PythonValue::Str;
}

/// A retention class key: `forever`, or `standard_<days>d` of a period of
/// whole days within the period bounds. The parser applies the one rule of
/// [`EnqueueRetention::from_class_key`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct RetentionClassKey(EnqueueRetention);

impl RetentionClassKey {
    /// The retention that the key names.
    pub fn retention(self) -> EnqueueRetention {
        self.0
    }
}

impl TryFrom<String> for RetentionClassKey {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        EnqueueRetention::from_class_key(&text)
            .map(Self)
            .map_err(|refused| refused.to_string())
    }
}

impl ManifestValue for RetentionClassKey {
    const PYTHON: PythonValue = PythonValue::Str;
}

/// The options of the task that each slot sends.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_macros::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ScheduleTaskOptions {
    /// The options text that the client encoded when the task was defined:
    /// the text that a plain send of the task stores.
    Encoded { json: String },
    /// The task has no options.
    NoOptions,
}

impl ScheduleTaskOptions {
    /// The options text, in the form that the send command takes.
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Encoded { json } => Some(json),
            Self::NoOptions => None,
        }
    }
}

/// What each slot of an enabled schedule sends.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSendManifest {
    /// The task that each slot sends.
    pub task_name: String,
    /// The queue of each slot: the schedule's queue, else the task's queue.
    pub queue_name: String,
    /// The priority of that queue: 1 is claimed first, 100 last.
    pub priority: QueuePriority,
    /// The input of each slot as one JSON value; the row stores the text as
    /// written.
    pub input_json: String,
    pub task_options: ScheduleTaskOptions,
    /// The retention class of each slot's task.
    pub retention_class_key: RetentionClassKey,
    /// Keep each slot's input for a rerun of its task.
    pub retain_rerun_input: bool,
}

/// Whether a schedule fires.
#[derive(Debug, Clone, Deserialize, horsies_macros::ManifestSchema)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum ScheduleActivation {
    /// The schedule fires.
    Enabled {
        /// The pattern as the JSON text of the client's pattern model.
        pattern_json: PatternJson,
        /// The IANA name of the zone of the pattern's times.
        timezone: String,
        /// Fire every missed slot, at most `max_catch_up_runs` per tick;
        /// else fire only the latest due slot.
        catch_up_missed: bool,
        max_catch_up_runs: CatchUpRuns,
        send: ScheduleSendManifest,
    },
    /// The schedule does not fire.
    Disabled,
}

/// One configured schedule.
#[derive(Debug, Clone, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleManifest {
    /// The name of the schedule, unique in the manifest: the key of its
    /// state row and of its slots' task ids.
    pub name: String,
    pub activation: ScheduleActivation,
}

/// The manifest of a scheduler process.
#[derive(Debug, Clone, Deserialize, horsies_macros::ManifestSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerManifest {
    /// Always `SCHEDULER_MANIFEST_VERSION`, the one version this core reads.
    pub manifest_version: SchedulerManifestVersion,
    /// The master switch (`ScheduleConfig.enabled`). Off, the start writes
    /// nothing and the scheduler fires nothing.
    pub enabled: bool,
    /// The wait between two ticks.
    pub check_interval_seconds: CheckIntervalSeconds,
    /// Every configured schedule, in the order of the config.
    pub schedules: Vec<ScheduleManifest>,
}

/// Parse a scheduler manifest from its JSON text.
///
/// Pure: no I/O. Refuses unknown fields, missing fields, values outside
/// their bounds, every manifest version but [`SCHEDULER_MANIFEST_VERSION`],
/// a pattern text that is not a valid pattern, a retention class key that
/// is not a class key, and a schedule name that an earlier schedule has.
/// It does not check timezones: the start does.
pub fn parse_scheduler_manifest(text: &str) -> Result<SchedulerManifest, ManifestRefused> {
    let manifest: SchedulerManifest = parse_manifest(text)?;
    match repeated_name(&manifest.schedules) {
        Some(position) => Err(ManifestRefused {
            field_path: format!("schedules[{position}].name"),
            cause: "the name of an earlier schedule repeats".to_owned(),
        }),
        None => Ok(manifest),
    }
}

/// The position of the first schedule whose name an earlier schedule has.
/// Pure.
fn repeated_name(schedules: &[ScheduleManifest]) -> Option<usize> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(schedules.len());
    schedules
        .iter()
        .position(|schedule| !seen.insert(schedule.name.as_str()))
}

#[cfg(test)]
mod tests;
