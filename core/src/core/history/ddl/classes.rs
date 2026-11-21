//! Retention-class naming and registration.
//!
//! A finite class is identified by its period in whole days, and its key is
//! derived from the period (`standard_<days>d`). The baseline CHECK
//! `horsies_retention_classes_kind_shape` ties the key to the period, so a
//! registered key can never mean another duration. A class is immutable:
//! registration creates the class row and its RANGE parent, or finds both
//! already there.

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::TASK_HISTORY_PARENT;
use crate::core::history::periods::RetentionDays;

pub const FOREVER_CLASS_KEY: &str = "forever";
pub const DEFAULT_RETENTION_CLASS_KEY: &str = "standard_30d";
pub const DEFAULT_RETENTION_DAYS: RetentionDays = RetentionDays::DEFAULT;

/// `horsies_retention_classes.class_kind` of a finite period class.
pub const PERIOD_CLASS_KIND: &str = "PERIOD";

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
