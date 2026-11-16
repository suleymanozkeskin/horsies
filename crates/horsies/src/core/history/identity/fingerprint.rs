//! Version-1 canonical enqueue-command fingerprint.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const COMMAND_FINGERPRINT_VERSION: i16 = 1;

#[derive(Debug, thiserror::Error)]
pub enum FingerprintError {
    #[error("task_name must be non-empty")]
    EmptyTaskName,
    #[error("queue_name must be non-empty")]
    EmptyQueueName,
    #[error("priority must be between 1 and 100")]
    InvalidPriority,
    #[error("enqueue_delay_seconds must be non-negative")]
    InvalidEnqueueDelay,
    #[error("retention_class_key must be non-empty")]
    EmptyRetentionClass,
    #[error("rerun source and root must be present together")]
    IncompleteRerunLineage,
    #[error("canonical JSON serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueCommandV1 {
    task_name: String,
    queue_name: String,
    priority: i32,
    args_json: Option<String>,
    kwargs_json: Option<String>,
    good_until: Option<DateTime<Utc>>,
    enqueue_delay_seconds: Option<i64>,
    task_options_json: Option<String>,
    retention_class_key: String,
    retain_rerun_input: bool,
    rerun_of_task_id: Option<Uuid>,
    rerun_root_task_id: Option<Uuid>,
}

impl EnqueueCommandV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_name: impl Into<String>,
        queue_name: impl Into<String>,
        priority: i32,
        args_json: Option<String>,
        kwargs_json: Option<String>,
        good_until: Option<DateTime<Utc>>,
        enqueue_delay_seconds: Option<i64>,
        task_options_json: Option<String>,
        retention_class_key: impl Into<String>,
        retain_rerun_input: bool,
        rerun_of_task_id: Option<Uuid>,
        rerun_root_task_id: Option<Uuid>,
    ) -> Result<Self, FingerprintError> {
        let command = Self {
            task_name: task_name.into(),
            queue_name: queue_name.into(),
            priority,
            args_json,
            kwargs_json,
            good_until,
            enqueue_delay_seconds,
            task_options_json,
            retention_class_key: retention_class_key.into(),
            retain_rerun_input,
            rerun_of_task_id,
            rerun_root_task_id,
        };
        command.validate()?;
        Ok(command)
    }

    fn validate(&self) -> Result<(), FingerprintError> {
        if self.task_name.is_empty() {
            return Err(FingerprintError::EmptyTaskName);
        }
        if self.queue_name.is_empty() {
            return Err(FingerprintError::EmptyQueueName);
        }
        if !(1..=100).contains(&self.priority) {
            return Err(FingerprintError::InvalidPriority);
        }
        if self
            .enqueue_delay_seconds
            .is_some_and(|seconds| seconds < 0)
        {
            return Err(FingerprintError::InvalidEnqueueDelay);
        }
        if self.retention_class_key.is_empty() {
            return Err(FingerprintError::EmptyRetentionClass);
        }
        if self.rerun_of_task_id.is_some() != self.rerun_root_task_id.is_some() {
            return Err(FingerprintError::IncompleteRerunLineage);
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, FingerprintError> {
        self.validate()?;
        serde_json::to_vec(&Value::Array(vec![
            Value::from(COMMAND_FINGERPRINT_VERSION),
            Value::from(self.task_name.clone()),
            Value::from(self.queue_name.clone()),
            Value::from(self.priority),
            option_string_value(&self.args_json),
            option_string_value(&self.kwargs_json),
            self.good_until
                .map(|value| Value::from(value.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()))
                .unwrap_or(Value::Null),
            self.enqueue_delay_seconds
                .map(Value::from)
                .unwrap_or(Value::Null),
            option_string_value(&self.task_options_json),
            Value::from(self.retention_class_key.clone()),
            Value::from(self.retain_rerun_input),
            self.rerun_of_task_id
                .map(|value| Value::from(value.to_string()))
                .unwrap_or(Value::Null),
            self.rerun_root_task_id
                .map(|value| Value::from(value.to_string()))
                .unwrap_or(Value::Null),
        ]))
        .map_err(FingerprintError::from)
    }

    pub fn fingerprint(&self) -> Result<[u8; 32], FingerprintError> {
        let digest = Sha256::digest(self.canonical_bytes()?);
        Ok(digest.into())
    }
}

fn option_string_value(value: &Option<String>) -> Value {
    value.clone().map(Value::from).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    fn command(
        task_name: &str,
        queue_name: &str,
        priority: i32,
        delay: Option<i64>,
        class_key: &str,
        source: Option<Uuid>,
        root: Option<Uuid>,
    ) -> Result<EnqueueCommandV1, FingerprintError> {
        EnqueueCommandV1::new(
            task_name,
            queue_name,
            priority,
            Some("[1,2]".to_owned()),
            Some("{\"a\":true}".to_owned()),
            DateTime::parse_from_rfc3339("2026-08-07T12:00:00Z")
                .ok()
                .map(|value| value.with_timezone(&Utc)),
            delay,
            Some("{}".to_owned()),
            class_key,
            true,
            source,
            root,
        )
    }

    #[test]
    fn invalid_fingerprint_commands_cannot_be_constructed() {
        assert!(matches!(
            command("", "default", 50, Some(0), "finite", None, None),
            Err(FingerprintError::EmptyTaskName)
        ));
        assert!(matches!(
            command("task", "", 50, Some(0), "finite", None, None),
            Err(FingerprintError::EmptyQueueName)
        ));
        for priority in [0, 101] {
            assert!(matches!(
                command("task", "default", priority, Some(0), "finite", None, None),
                Err(FingerprintError::InvalidPriority)
            ));
        }
        assert!(matches!(
            command("task", "default", 50, Some(-1), "finite", None, None),
            Err(FingerprintError::InvalidEnqueueDelay)
        ));
        assert!(matches!(
            command("task", "default", 50, Some(0), "", None, None),
            Err(FingerprintError::EmptyRetentionClass)
        ));
        assert!(matches!(
            command(
                "task",
                "default",
                50,
                Some(0),
                "finite",
                Some(Uuid::nil()),
                None,
            ),
            Err(FingerprintError::IncompleteRerunLineage)
        ));
    }

    #[test]
    fn every_fingerprint_field_changes_the_digest() {
        let base = command("task", "default", 50, Some(0), "finite", None, None).unwrap();
        let base_digest = base.fingerprint().unwrap();
        let changed = [
            command("other", "default", 50, Some(0), "finite", None, None).unwrap(),
            command("task", "bulk", 50, Some(0), "finite", None, None).unwrap(),
            command("task", "default", 51, Some(0), "finite", None, None).unwrap(),
            command("task", "default", 50, Some(1), "finite", None, None).unwrap(),
            command("task", "default", 50, Some(0), "other", None, None).unwrap(),
            command(
                "task",
                "default",
                50,
                Some(0),
                "finite",
                Some(Uuid::nil()),
                Some(Uuid::nil()),
            )
            .unwrap(),
        ];
        for command in changed {
            assert_ne!(command.fingerprint().unwrap(), base_digest);
        }
    }
}
