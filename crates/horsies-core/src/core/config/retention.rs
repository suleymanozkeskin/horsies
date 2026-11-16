use std::collections::{HashMap, HashSet};

use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::ddl::classes::{DEFAULT_RETENTION_CLASS_KEY, FOREVER_CLASS_KEY};
use crate::core::history::names::{HEARTBEAT_CLASS_KEY, MAX_RETENTION_CLASS_KEY_LENGTH};

pub const QUEUE_DERIVED_CLASS_PREFIX: &str = "q_";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionClassConfig {
    pub key: String,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionChoice {
    Class(String),
    Forever,
}

impl RetentionChoice {
    pub fn class(key: impl Into<String>) -> Self {
        Self::Class(key.into())
    }

    pub fn as_class_key(&self) -> Option<&str> {
        match self {
            Self::Class(key) => Some(key),
            Self::Forever => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionConfig {
    #[serde(default = "default_worker_state_retention_hours")]
    pub worker_state_retention_hours: Option<u32>,
    #[serde(default = "default_terminal_record_retention_hours")]
    pub terminal_record_retention_hours: Option<u32>,
    #[serde(default)]
    pub paused_workflow_auto_cancel_after: Option<Duration>,
    #[serde(default = "default_history_leaf_horizon_days")]
    pub history_leaf_horizon_days: u32,
    #[serde(default = "default_heartbeat_leaf_horizon_hours")]
    pub heartbeat_leaf_horizon_hours: u32,
    #[serde(default)]
    pub retention_classes: Vec<RetentionClassConfig>,
    #[serde(default)]
    pub queue_retention: HashMap<String, Option<Duration>>,
    #[serde(default = "default_partition_maintenance_interval_s")]
    pub partition_maintenance_interval_s: u64,
    #[serde(default = "default_retention_sweep_interval_s")]
    pub retention_sweep_interval_s: u64,
    #[serde(default = "default_retention_delete_batch_size")]
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

fn default_partition_maintenance_interval_s() -> u64 {
    900
}

fn default_retention_sweep_interval_s() -> u64 {
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
            paused_workflow_auto_cancel_after: None,
            history_leaf_horizon_days: default_history_leaf_horizon_days(),
            heartbeat_leaf_horizon_hours: default_heartbeat_leaf_horizon_hours(),
            retention_classes: Vec::new(),
            queue_retention: HashMap::new(),
            partition_maintenance_interval_s: default_partition_maintenance_interval_s(),
            retention_sweep_interval_s: default_retention_sweep_interval_s(),
            retention_delete_batch_size: default_retention_delete_batch_size(),
        }
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
    #[error("paused_workflow_auto_cancel_after must be positive; use None to disable the sweep")]
    NonPositivePausedExpiry,
    #[error("retention class {key:?} is reserved")]
    ReservedClass { key: String },
    #[error("retention class {key:?} uses the reserved {QUEUE_DERIVED_CLASS_PREFIX:?} prefix")]
    QueueDerivedPrefix { key: String },
    #[error("retention class {key:?} is not a usable identifier")]
    InvalidClassIdentifier { key: String },
    #[error(
        "retention class key {key:?} is {length} characters; the limit is {MAX_RETENTION_CLASS_KEY_LENGTH}"
    )]
    ClassKeyTooLong { key: String, length: usize },
    #[error("retention class {key:?} declared twice")]
    DuplicateClass { key: String },
    #[error("retention class {key:?} has a non-positive duration")]
    NonPositiveClassDuration { key: String },
    #[error("queue {queue:?} cannot be mapped: the name is not a usable identifier")]
    InvalidQueueIdentifier { queue: String },
    #[error("queue {queue:?} maps to a non-positive retention")]
    NonPositiveQueueDuration { queue: String },
    #[error("queue {queue:?} retention is not a whole number of seconds")]
    QueueDurationNotWholeSeconds { queue: String },
    #[error("queue_retention references unknown queue {queue:?}")]
    UnknownQueue { queue: String },
    #[error("unknown retention class {key:?}")]
    UnknownClass { key: String },
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
            "partition_maintenance_interval_s",
            self.partition_maintenance_interval_s,
            60,
            3_600,
        );
        validate_range(
            &mut errors,
            "retention_sweep_interval_s",
            self.retention_sweep_interval_s,
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
        if self
            .paused_workflow_auto_cancel_after
            .is_some_and(|duration| duration <= Duration::zero())
        {
            errors.push(RetentionConfigError::NonPositivePausedExpiry);
        }

        let reserved = [
            DEFAULT_RETENTION_CLASS_KEY,
            FOREVER_CLASS_KEY,
            HEARTBEAT_CLASS_KEY,
        ];
        let mut seen = HashSet::new();
        for declared in &self.retention_classes {
            let key = &declared.key;
            if reserved.contains(&key.as_str()) {
                errors.push(RetentionConfigError::ReservedClass { key: key.clone() });
            } else if key.starts_with(QUEUE_DERIVED_CLASS_PREFIX) {
                errors.push(RetentionConfigError::QueueDerivedPrefix { key: key.clone() });
            } else if !is_safe_identifier(key) {
                errors.push(RetentionConfigError::InvalidClassIdentifier { key: key.clone() });
            } else if key.len() > MAX_RETENTION_CLASS_KEY_LENGTH {
                errors.push(RetentionConfigError::ClassKeyTooLong {
                    key: key.clone(),
                    length: key.len(),
                });
            }
            if !seen.insert(key.as_str()) {
                errors.push(RetentionConfigError::DuplicateClass { key: key.clone() });
            }
            if declared.duration <= Duration::zero() {
                errors.push(RetentionConfigError::NonPositiveClassDuration { key: key.clone() });
            }
        }

        let mut mappings: Vec<_> = self.queue_retention.iter().collect();
        mappings.sort_by_key(|(queue, _)| queue.as_str());
        for (queue, duration) in mappings {
            if !is_safe_identifier(queue) {
                errors.push(RetentionConfigError::InvalidQueueIdentifier {
                    queue: queue.clone(),
                });
                continue;
            }
            let Some(duration) = duration else {
                continue;
            };
            if *duration <= Duration::zero() {
                errors.push(RetentionConfigError::NonPositiveQueueDuration {
                    queue: queue.clone(),
                });
                continue;
            }
            match derived_queue_class_key(queue, *duration) {
                Ok(key) if key.len() > MAX_RETENTION_CLASS_KEY_LENGTH => {
                    errors.push(RetentionConfigError::ClassKeyTooLong {
                        length: key.len(),
                        key,
                    });
                }
                Ok(_) => {}
                Err(DurationRenderError::NotWholeSeconds) => {
                    errors.push(RetentionConfigError::QueueDurationNotWholeSeconds {
                        queue: queue.clone(),
                    });
                }
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

    pub fn declared_class_keys(&self) -> HashSet<&str> {
        self.retention_classes
            .iter()
            .map(|class| class.key.as_str())
            .collect()
    }

    pub fn known_finite_class_keys(&self) -> HashSet<String> {
        let mut keys: HashSet<String> = self
            .retention_classes
            .iter()
            .map(|class| class.key.clone())
            .collect();
        keys.extend(self.queue_retention.iter().filter_map(|(queue, duration)| {
            duration.map(|duration| {
                derived_queue_class_key(queue, duration)
                    .expect("validated retention mappings have whole-second durations")
            })
        }));
        keys
    }

    pub fn resolve_queue_class(&self, queue_name: &str) -> Option<String> {
        match self.queue_retention.get(queue_name) {
            None => Some(DEFAULT_RETENTION_CLASS_KEY.to_owned()),
            Some(None) => None,
            Some(Some(duration)) => Some(
                derived_queue_class_key(queue_name, *duration)
                    .expect("validated retention mappings have whole-second durations"),
            ),
        }
    }

    pub fn resolve_choice(
        &self,
        queue_name: &str,
        choice: Option<&RetentionChoice>,
    ) -> Result<Option<String>, RetentionConfigError> {
        match choice {
            None => Ok(self.resolve_queue_class(queue_name)),
            Some(RetentionChoice::Forever) => Ok(None),
            Some(RetentionChoice::Class(key)) if key == DEFAULT_RETENTION_CLASS_KEY => {
                Ok(Some(key.clone()))
            }
            Some(RetentionChoice::Class(key)) if self.known_finite_class_keys().contains(key) => {
                Ok(Some(key.clone()))
            }
            Some(RetentionChoice::Class(key)) => {
                Err(RetentionConfigError::UnknownClass { key: key.clone() })
            }
        }
    }

    pub fn registrable_classes(&self) -> Vec<RetentionClassConfig> {
        let mut classes = self.retention_classes.clone();
        let mut mappings: Vec<_> = self.queue_retention.iter().collect();
        mappings.sort_by_key(|(queue, _)| queue.as_str());
        classes.extend(mappings.into_iter().filter_map(|(queue, duration)| {
            duration.map(|duration| RetentionClassConfig {
                key: derived_queue_class_key(queue, duration)
                    .expect("validated retention mappings have whole-second durations"),
                duration,
            })
        }));
        classes
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DurationRenderError {
    #[error("duration is not a whole number of seconds")]
    NotWholeSeconds,
}

pub fn render_duration(duration: Duration) -> Result<String, DurationRenderError> {
    if duration.num_nanoseconds().is_none()
        || duration.num_nanoseconds().expect("checked above") % 1_000_000_000 != 0
    {
        return Err(DurationRenderError::NotWholeSeconds);
    }
    let total = duration.num_seconds();
    for (unit, seconds) in [('d', 86_400), ('h', 3_600), ('m', 60)] {
        if total % seconds == 0 {
            return Ok(format!("{}{unit}", total / seconds));
        }
    }
    Ok(format!("{total}s"))
}

pub fn derived_queue_class_key(
    queue_name: &str,
    duration: Duration,
) -> Result<String, DurationRenderError> {
    Ok(format!(
        "{QUEUE_DERIVED_CLASS_PREFIX}{queue_name}_{}",
        render_duration(duration)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_rendering_and_queue_derivation_are_exact() {
        for (duration, expected) in [
            (Duration::days(7), "7d"),
            (Duration::hours(36), "36h"),
            (Duration::minutes(90), "90m"),
            (Duration::seconds(45), "45s"),
        ] {
            assert_eq!(render_duration(duration).unwrap(), expected);
        }
        assert_eq!(
            derived_queue_class_key("bulk", Duration::hours(36)).unwrap(),
            "q_bulk_36h"
        );
        assert!(render_duration(Duration::nanoseconds(1)).is_err());
    }

    #[test]
    fn defaults_and_all_bounds_match_the_retention_contract() {
        let config = RetentionConfig::default();
        assert!(config.validate().is_empty());
        assert_eq!(config.worker_state_retention_hours, Some(168));
        assert_eq!(config.terminal_record_retention_hours, Some(720));
        assert_eq!(config.history_leaf_horizon_days, 3);
        assert_eq!(config.heartbeat_leaf_horizon_hours, 6);
        assert_eq!(config.partition_maintenance_interval_s, 900);
        assert_eq!(config.retention_sweep_interval_s, 300);
        assert_eq!(config.retention_delete_batch_size, 500);

        let invalid = RetentionConfig {
            worker_state_retention_hours: Some(0),
            terminal_record_retention_hours: Some(43_801),
            paused_workflow_auto_cancel_after: Some(Duration::zero()),
            history_leaf_horizon_days: 1,
            heartbeat_leaf_horizon_hours: 49,
            partition_maintenance_interval_s: 59,
            retention_sweep_interval_s: 86_401,
            retention_delete_batch_size: 49,
            ..Default::default()
        };
        assert_eq!(invalid.validate().len(), 8);
    }

    #[test]
    fn every_numeric_boundary_is_inclusive_and_one_step_out_is_refused() {
        let cases: &[(&str, u64, u64)] = &[
            ("worker_state_retention_hours", 1, 8_760),
            ("terminal_record_retention_hours", 1, 43_800),
            ("history_leaf_horizon_days", 2, 14),
            ("heartbeat_leaf_horizon_hours", 2, 48),
            ("partition_maintenance_interval_s", 60, 3_600),
            ("retention_sweep_interval_s", 30, 86_400),
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

        let mut disabled = RetentionConfig::default();
        disabled.worker_state_retention_hours = None;
        disabled.terminal_record_retention_hours = None;
        assert!(disabled.validate().is_empty());
    }

    fn set_numeric(config: &mut RetentionConfig, field: &str, value: u64) {
        match field {
            "worker_state_retention_hours" => {
                config.worker_state_retention_hours = Some(value as u32)
            }
            "terminal_record_retention_hours" => {
                config.terminal_record_retention_hours = Some(value as u32)
            }
            "history_leaf_horizon_days" => config.history_leaf_horizon_days = value as u32,
            "heartbeat_leaf_horizon_hours" => config.heartbeat_leaf_horizon_hours = value as u32,
            "partition_maintenance_interval_s" => config.partition_maintenance_interval_s = value,
            "retention_sweep_interval_s" => config.retention_sweep_interval_s = value,
            "retention_delete_batch_size" => config.retention_delete_batch_size = value as u32,
            unknown => panic!("unhandled retention boundary field {unknown}"),
        }
    }

    #[test]
    fn declarations_and_queue_mappings_fail_closed() {
        let config = RetentionConfig {
            retention_classes: vec![
                RetentionClassConfig {
                    key: "forever".to_owned(),
                    duration: Duration::days(1),
                },
                RetentionClassConfig {
                    key: "q_owned".to_owned(),
                    duration: Duration::days(1),
                },
                RetentionClassConfig {
                    key: "nineteen_chars_keyx".to_owned(),
                    duration: Duration::zero(),
                },
            ],
            queue_retention: HashMap::from([
                ("bad-name".to_owned(), Some(Duration::days(1))),
                ("bulk".to_owned(), Some(Duration::nanoseconds(1))),
                ("forever_queue".to_owned(), None),
            ]),
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|error| matches!(
            error,
            RetentionConfigError::ReservedClass { key } if key == "forever"
        )));
        assert!(errors.iter().any(|error| matches!(
            error,
            RetentionConfigError::QueueDerivedPrefix { key } if key == "q_owned"
        )));
        assert!(errors
            .iter()
            .any(|error| matches!(error, RetentionConfigError::ClassKeyTooLong { .. })));
        assert!(errors
            .iter()
            .any(|error| matches!(error, RetentionConfigError::NonPositiveClassDuration { .. })));
        assert!(errors
            .iter()
            .any(|error| matches!(error, RetentionConfigError::InvalidQueueIdentifier { .. })));
        assert!(errors.iter().any(|error| matches!(
            error,
            RetentionConfigError::QueueDurationNotWholeSeconds { .. }
        )));
    }

    #[test]
    fn class_key_budget_identifier_rules_duplicates_and_collision_band_are_exact() {
        let at_budget = "k".repeat(MAX_RETENTION_CLASS_KEY_LENGTH);
        let valid = RetentionConfig {
            retention_classes: vec![RetentionClassConfig {
                key: at_budget.clone(),
                duration: Duration::hours(1),
            }],
            ..Default::default()
        };
        assert!(valid.validate().is_empty());
        assert_eq!(valid.retention_classes[0].key, at_budget);

        for key_length in [MAX_RETENTION_CLASS_KEY_LENGTH + 1, 30, 31] {
            let key = "k".repeat(key_length);
            let errors = RetentionConfig {
                retention_classes: vec![RetentionClassConfig {
                    key: key.clone(),
                    duration: Duration::days(5),
                }],
                ..Default::default()
            }
            .validate();
            assert!(errors.iter().any(|error| matches!(
                error,
                RetentionConfigError::ClassKeyTooLong { key: found, length }
                    if found == &key && *length == key_length
            )));
        }

        for unsafe_key in ["", "Upper", "has-dash", "has space", "_leading"] {
            let errors = RetentionConfig {
                retention_classes: vec![RetentionClassConfig {
                    key: unsafe_key.to_owned(),
                    duration: Duration::days(1),
                }],
                ..Default::default()
            }
            .validate();
            assert!(errors.iter().any(|error| matches!(
                error,
                RetentionConfigError::InvalidClassIdentifier { key } if key == unsafe_key
            )));
        }

        let duplicate = RetentionConfig {
            retention_classes: vec![
                RetentionClassConfig {
                    key: "audit".to_owned(),
                    duration: Duration::days(1),
                },
                RetentionClassConfig {
                    key: "audit".to_owned(),
                    duration: Duration::days(2),
                },
            ],
            ..Default::default()
        };
        assert!(duplicate.validate().iter().any(|error| matches!(
            error,
            RetentionConfigError::DuplicateClass { key } if key == "audit"
        )));
    }

    #[test]
    fn precedence_and_registrable_classes_are_exact() {
        let config = RetentionConfig {
            retention_classes: vec![RetentionClassConfig {
                key: "audit_7d".to_owned(),
                duration: Duration::days(7),
            }],
            queue_retention: HashMap::from([
                ("bulk".to_owned(), Some(Duration::hours(36))),
                ("audit".to_owned(), None),
            ]),
            ..Default::default()
        };
        assert_eq!(
            config.resolve_choice("default", None).unwrap().as_deref(),
            Some(DEFAULT_RETENTION_CLASS_KEY)
        );
        assert_eq!(
            config.resolve_choice("bulk", None).unwrap().as_deref(),
            Some("q_bulk_36h")
        );
        assert_eq!(config.resolve_choice("audit", None).unwrap(), None);
        assert_eq!(
            config
                .resolve_choice(
                    "bulk",
                    Some(&RetentionChoice::class(DEFAULT_RETENTION_CLASS_KEY)),
                )
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
        for key in ["audit_7d", "q_bulk_36h"] {
            assert_eq!(
                config
                    .resolve_choice("default", Some(&RetentionChoice::class(key)))
                    .unwrap()
                    .as_deref(),
                Some(key),
            );
        }
        assert!(matches!(
            config.resolve_choice("default", Some(&RetentionChoice::class("q_other_9d")),),
            Err(RetentionConfigError::UnknownClass { .. })
        ));
        assert_eq!(
            config
                .registrable_classes()
                .into_iter()
                .map(|class| class.key)
                .collect::<Vec<_>>(),
            vec!["audit_7d", "q_bulk_36h"]
        );
    }
}
