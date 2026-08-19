use serde::{Deserialize, Serialize};

/// Queue routing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueMode {
    /// Single "default" queue, no custom routing.
    Default,
    /// User-defined queues with priority and concurrency settings.
    Custom,
}

/// Configuration for a custom queue.
///
/// - `name`: queue identifier, used in task registration (e.g. `"high_priority"`)
/// - `priority`: 1 = highest priority, 100 = lowest
/// - `max_concurrency`: max concurrent tasks for this queue (app-level cap still applies)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomQueueConfig {
    pub name: String,
    /// Queue priority (1 = first executed, 100 = last).
    #[serde(default = "default_priority")]
    pub priority: u32,
    /// Max concurrent tasks for this queue (app- and cluster-level caps still apply).
    ///
    /// `None` is the explicit uncapped sentinel (mirrors `cluster_wide_cap=None`):
    /// the queue is omitted from the worker's per-queue concurrency map, so the
    /// claim pass enforces no per-queue limit and skips that queue's in-flight
    /// count query entirely. `Some(0)` is valid and pauses claiming from the
    /// queue. Defaults to `Some(5)`.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: Option<u32>,
}

/// Longest allowed queue name in bytes. The insert trigger fires
/// `pg_notify('task_queue_' || NEW.queue_name, ..)`; Postgres rejects channel
/// names over 63 bytes, and `"task_queue_"` is 11, leaving 52 for the name. A
/// longer name makes every INSERT for that queue fail with an opaque trigger
/// error (C18).
const MAX_QUEUE_NAME_BYTES: usize = 52;

/// Validation error for CustomQueueConfig.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CustomQueueConfigError {
    #[error("queue name must not be empty")]
    EmptyName,
    #[error(
        "queue '{name}' name is {len} bytes; the NOTIFY channel 'task_queue_' + name \
         must fit Postgres's 63-byte limit, so the name must be at most {max} bytes"
    )]
    NameTooLong {
        name: String,
        len: usize,
        max: usize,
    },
    #[error("queue '{name}' priority must be between 1 and 100, got {value}")]
    PriorityOutOfRange { name: String, value: u32 },
}

impl CustomQueueConfig {
    /// Validate the queue configuration.
    ///
    /// Checks:
    /// - `name` is not empty
    /// - `name` is at most 52 bytes (the NOTIFY channel-name limit)
    /// - `priority` is between 1 and 100 (matching Python `Field(ge=1, le=100)`)
    pub fn validate(&self) -> Vec<CustomQueueConfigError> {
        let mut errors = Vec::new();

        if self.name.is_empty() {
            errors.push(CustomQueueConfigError::EmptyName);
        }

        if self.name.len() > MAX_QUEUE_NAME_BYTES {
            errors.push(CustomQueueConfigError::NameTooLong {
                name: self.name.clone(),
                len: self.name.len(),
                max: MAX_QUEUE_NAME_BYTES,
            });
        }

        if self.priority == 0 || self.priority > 100 {
            errors.push(CustomQueueConfigError::PriorityOutOfRange {
                name: self.name.clone(),
                value: self.priority,
            });
        }

        errors
    }
}

fn default_priority() -> u32 {
    1
}

fn default_max_concurrency() -> Option<u32> {
    Some(5)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_mode_serde() {
        let mode = QueueMode::Custom;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"custom\"");
        let back: QueueMode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, mode);
    }

    #[test]
    fn custom_queue_defaults() {
        let json = r#"{"name": "high"}"#;
        let config: CustomQueueConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "high");
        assert_eq!(config.priority, 1);
        assert_eq!(config.max_concurrency, Some(5));
    }

    #[test]
    fn max_concurrency_none_deserializes() {
        let json = r#"{"name": "bulk", "max_concurrency": null}"#;
        let config: CustomQueueConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_concurrency, None);
    }

    #[test]
    fn max_concurrency_zero_accepted() {
        let json = r#"{"name": "drained", "max_concurrency": 0}"#;
        let config: CustomQueueConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_concurrency, Some(0));
        assert!(config.validate().is_empty());
    }

    #[test]
    fn custom_queue_validate_ok() {
        let config = CustomQueueConfig {
            name: "fast".to_owned(),
            priority: 1,
            max_concurrency: Some(10),
        };
        assert!(config.validate().is_empty());
    }

    #[test]
    fn custom_queue_validate_priority_at_100() {
        let config = CustomQueueConfig {
            name: "slow".to_owned(),
            priority: 100,
            max_concurrency: Some(5),
        };
        assert!(config.validate().is_empty());
    }

    #[test]
    fn custom_queue_name_at_52_bytes_ok() {
        // 52 bytes → channel 'task_queue_' + name = 63 bytes, exactly the limit.
        let config = CustomQueueConfig {
            name: "a".repeat(52),
            priority: 1,
            max_concurrency: Some(5),
        };
        assert!(
            config.validate().is_empty(),
            "52-byte name must be accepted"
        );
    }

    #[test]
    fn custom_queue_name_over_52_bytes_rejected() {
        // C18: a 53-byte name overflows the 63-byte NOTIFY channel limit and
        // would make every INSERT for the queue fail in the trigger. Reject it
        // at config time instead.
        let config = CustomQueueConfig {
            name: "a".repeat(53),
            priority: 1,
            max_concurrency: Some(5),
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            CustomQueueConfigError::NameTooLong {
                len: 53,
                max: 52,
                ..
            }
        ));
    }

    #[test]
    fn custom_queue_validate_priority_zero_rejected() {
        let config = CustomQueueConfig {
            name: "bad".to_owned(),
            priority: 0,
            max_concurrency: Some(5),
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            CustomQueueConfigError::PriorityOutOfRange { .. }
        ));
    }

    #[test]
    fn custom_queue_validate_priority_over_100_rejected() {
        let config = CustomQueueConfig {
            name: "bad".to_owned(),
            priority: 101,
            max_concurrency: Some(5),
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            CustomQueueConfigError::PriorityOutOfRange { .. }
        ));
    }

    #[test]
    fn custom_queue_validate_empty_name_rejected() {
        let config = CustomQueueConfig {
            name: "".to_owned(),
            priority: 1,
            max_concurrency: Some(5),
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], CustomQueueConfigError::EmptyName));
    }
}
