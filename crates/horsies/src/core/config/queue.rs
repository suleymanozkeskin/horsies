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
    /// Max concurrent tasks for this queue.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
}

/// Validation error for CustomQueueConfig.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CustomQueueConfigError {
    #[error("queue name must not be empty")]
    EmptyName,
    #[error("queue '{name}' priority must be between 1 and 100, got {value}")]
    PriorityOutOfRange { name: String, value: u32 },
}

impl CustomQueueConfig {
    /// Validate the queue configuration.
    ///
    /// Checks:
    /// - `name` is not empty
    /// - `priority` is between 1 and 100 (matching Python `Field(ge=1, le=100)`)
    pub fn validate(&self) -> Vec<CustomQueueConfigError> {
        let mut errors = Vec::new();

        if self.name.is_empty() {
            errors.push(CustomQueueConfigError::EmptyName);
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

fn default_max_concurrency() -> u32 {
    5
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
        assert_eq!(config.max_concurrency, 5);
    }

    #[test]
    fn custom_queue_validate_ok() {
        let config = CustomQueueConfig {
            name: "fast".to_owned(),
            priority: 1,
            max_concurrency: 10,
        };
        assert!(config.validate().is_empty());
    }

    #[test]
    fn custom_queue_validate_priority_at_100() {
        let config = CustomQueueConfig {
            name: "slow".to_owned(),
            priority: 100,
            max_concurrency: 5,
        };
        assert!(config.validate().is_empty());
    }

    #[test]
    fn custom_queue_validate_priority_zero_rejected() {
        let config = CustomQueueConfig {
            name: "bad".to_owned(),
            priority: 0,
            max_concurrency: 5,
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
            max_concurrency: 5,
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
            max_concurrency: 5,
        };
        let errors = config.validate();
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], CustomQueueConfigError::EmptyName));
    }
}
