//! Retention-class naming and registration.
//!
//! A finite class is identified by its period in whole days, and its key is
//! derived from the period (`standard_<days>d`). The baseline CHECK
//! `horsies_retention_classes_kind_shape` ties the key to the period, so a
//! registered key can never mean another duration. A class is immutable:
//! registration creates the class row and its RANGE parent, or finds both
//! already there.

use sqlx::PgConnection;

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{RETENTION_CLASSES, TASK_HISTORY_PARENT};
use crate::core::history::periods::RetentionDays;

pub const FOREVER_CLASS_KEY: &str = "forever";
pub const DEFAULT_RETENTION_CLASS_KEY: &str = "standard_30d";
pub const DEFAULT_RETENTION_DAYS: RetentionDays = RetentionDays::DEFAULT;

/// `horsies_retention_classes.class_kind` of a finite period class.
pub const PERIOD_CLASS_KIND: &str = "PERIOD";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassRegistration {
    /// The class row and its RANGE parent now exist.
    Registered {
        class_key: String,
        finite_parent_name: String,
    },
    /// The class row already exists. Its key fixes its period, so nothing
    /// changed.
    AlreadyRegistered { class_key: String },
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
        "CREATE TABLE {parent_name}\n    PARTITION OF {TASK_HISTORY_PARENT}\n    FOR VALUES IN ('{class_key}')\n    PARTITION BY RANGE (terminal_at)"
    ))
}

/// Create or find the class of one period inside the caller's transaction.
///
/// Reads the class row by key; when absent, inserts it and creates its RANGE
/// parent. Does not commit.
pub async fn register_period_class(
    connection: &mut PgConnection,
    days: RetentionDays,
) -> Result<ClassRegistration, HistoryError> {
    let class_key = days.class_key();
    let parent_name = finite_class_parent_name(&class_key)?;
    let exists_sql =
        format!("SELECT EXISTS (SELECT 1 FROM {RETENTION_CLASSES} WHERE class_key = $1)");
    let exists: bool = sqlx::query_scalar(&exists_sql)
        .bind(&class_key)
        .fetch_one(&mut *connection)
        .await?;
    if exists {
        return Ok(ClassRegistration::AlreadyRegistered { class_key });
    }

    let insert_class_sql = format!(
        "INSERT INTO {RETENTION_CLASSES} (
             class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
         ) VALUES ($1, '{PERIOD_CLASS_KIND}', $2, NULL, statement_timestamp())"
    );
    sqlx::query(&insert_class_sql)
        .bind(&class_key)
        .bind(days.as_stored())
        .execute(&mut *connection)
        .await?;

    sqlx::query(&render_finite_class_parent_ddl(&class_key, &parent_name)?)
        .execute(&mut *connection)
        .await?;

    Ok(ClassRegistration::Registered {
        class_key,
        finite_parent_name: parent_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_defaults_resolution_and_parent_ddl_are_pinned() {
        assert_eq!(DEFAULT_RETENTION_CLASS_KEY, "standard_30d");
        assert_eq!(
            DEFAULT_RETENTION_DAYS.class_key(),
            DEFAULT_RETENTION_CLASS_KEY
        );
        assert_eq!(resolve_retention_class_key(None), "forever");
        assert_eq!(
            resolve_retention_class_key(Some("standard_7d")),
            "standard_7d"
        );
        let parent = finite_class_parent_name("standard_30d").unwrap();
        assert_eq!(parent, "horsies_task_history_standard_30d");
        assert_eq!(
            render_finite_class_parent_ddl("standard_30d", &parent).unwrap(),
            "CREATE TABLE horsies_task_history_standard_30d\n    PARTITION OF horsies_task_history\n    FOR VALUES IN ('standard_30d')\n    PARTITION BY RANGE (terminal_at)"
        );
        assert!(finite_class_parent_name("30-days!").is_err());
        assert!(render_finite_class_parent_ddl("standard_30d", "wrong_parent").is_err());
    }
}
