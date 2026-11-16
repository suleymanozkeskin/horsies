//! Finite retention-class naming and registration.

use chrono::Duration;
use sqlx::PgConnection;

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{RETENTION_CLASSES, TASK_HISTORY_PARENT};

pub const FOREVER_CLASS_KEY: &str = "forever";
pub const DEFAULT_RETENTION_CLASS_KEY: &str = "standard_30d";
pub const DEFAULT_RETENTION_DURATION_DAYS: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassRegistration {
    Registered {
        class_key: String,
        finite_parent_name: String,
    },
    AlreadyRegistered {
        class_key: String,
    },
    Conflict {
        class_key: String,
        existing_duration_microseconds: Option<i64>,
        existing_partition_interval_microseconds: Option<i64>,
    },
}

pub fn resolve_retention_class_key(requested: Option<&str>) -> &str {
    requested.unwrap_or(FOREVER_CLASS_KEY)
}

pub fn finite_class_parent_name(class_key: &str) -> Result<String, HistoryError> {
    let name = format!("{TASK_HISTORY_PARENT}_{class_key}");
    if !is_safe_identifier(&name) {
        return Err(HistoryError::contract(format!(
            "class key {class_key:?} does not form a safe relation name"
        )));
    }
    Ok(name)
}

pub fn render_finite_class_parent_ddl(
    class_key: &str,
    parent_name: &str,
) -> Result<String, HistoryError> {
    let expected_parent = finite_class_parent_name(class_key)?;
    if parent_name != expected_parent {
        return Err(HistoryError::contract(
            "finite class parent does not match the class-key derivation",
        ));
    }
    Ok(format!(
        "CREATE TABLE {parent_name}\n    PARTITION OF {TASK_HISTORY_PARENT}\n    FOR VALUES IN ('{class_key}')\n    PARTITION BY RANGE (retention_anchor_at)"
    ))
}

pub async fn register_finite_retention_class(
    connection: &mut PgConnection,
    class_key: &str,
    duration: Duration,
) -> Result<ClassRegistration, HistoryError> {
    if duration <= Duration::zero() {
        return Err(HistoryError::contract(
            "retention duration must be positive",
        ));
    }
    let parent_name = finite_class_parent_name(class_key)?;
    let duration_microseconds = duration.num_microseconds().ok_or_else(|| {
        HistoryError::contract("retention duration is outside the supported interval range")
    })?;
    let read_class_sql = format!(
        "SELECT (EXTRACT(epoch FROM duration) * 1000000)::bigint,
                (EXTRACT(epoch FROM partition_interval) * 1000000)::bigint,
                finite_parent_name
         FROM {RETENTION_CLASSES}
         WHERE class_key = $1"
    );
    let existing: Option<(Option<i64>, Option<i64>, Option<String>)> =
        sqlx::query_as(&read_class_sql)
            .bind(class_key)
            .fetch_optional(&mut *connection)
            .await?;

    match existing {
        Some((Some(existing_duration), Some(86_400_000_000), Some(existing_parent)))
            if existing_duration == duration_microseconds && existing_parent == parent_name =>
        {
            return Ok(ClassRegistration::AlreadyRegistered {
                class_key: class_key.to_owned(),
            });
        }
        Some((existing_duration, existing_interval, _)) => {
            return Ok(ClassRegistration::Conflict {
                class_key: class_key.to_owned(),
                existing_duration_microseconds: existing_duration,
                existing_partition_interval_microseconds: existing_interval,
            });
        }
        None => {}
    }

    let insert_class_sql = format!(
        "INSERT INTO {RETENTION_CLASSES} (
             class_key, duration, partition_interval,
             finite_parent_name, created_at
         ) VALUES ($1, $2::bigint * interval '1 microsecond',
                   interval '1 day', $3, statement_timestamp())"
    );
    sqlx::query(&insert_class_sql)
        .bind(class_key)
        .bind(duration_microseconds)
        .bind(&parent_name)
        .execute(&mut *connection)
        .await?;

    sqlx::query(&render_finite_class_parent_ddl(class_key, &parent_name)?)
        .execute(&mut *connection)
        .await?;

    Ok(ClassRegistration::Registered {
        class_key: class_key.to_owned(),
        finite_parent_name: parent_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_defaults_resolution_and_parent_ddl_are_pinned() {
        assert_eq!(DEFAULT_RETENTION_CLASS_KEY, "standard_30d");
        assert_eq!(DEFAULT_RETENTION_DURATION_DAYS, 30);
        assert_eq!(resolve_retention_class_key(None), "forever");
        assert_eq!(resolve_retention_class_key(Some("finite_7d")), "finite_7d");
        let parent = finite_class_parent_name("finite_30d_v1").unwrap();
        assert_eq!(parent, "horsies_task_history_finite_30d_v1");
        assert_eq!(
            render_finite_class_parent_ddl("finite_30d_v1", &parent).unwrap(),
            "CREATE TABLE horsies_task_history_finite_30d_v1\n    PARTITION OF horsies_task_history\n    FOR VALUES IN ('finite_30d_v1')\n    PARTITION BY RANGE (retention_anchor_at)"
        );
        assert!(finite_class_parent_name("30-days!").is_err());
        assert!(render_finite_class_parent_ddl("finite", "wrong_parent").is_err());
    }
}
