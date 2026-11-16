//! Canonical names for runtime-created history relations and indexes.

use chrono::{DateTime, Utc};

use crate::core::history::commands::{is_safe_identifier, HistoryCommandError, LeafRef};
use crate::core::history::errors::HistoryError;

pub const ORDERING_INDEX_COLUMN: &str = "enqueued_at";

pub fn daily_leaf_name(
    parent_name: &str,
    lower: DateTime<Utc>,
) -> Result<String, HistoryCommandError> {
    let name = format!("{parent_name}_{}", lower.format("%Y_%m_%d"));
    if !is_safe_identifier(&name) {
        return Err(HistoryCommandError::Invalid(
            "derived leaf name must be a safe PostgreSQL identifier",
        ));
    }
    Ok(name)
}

pub fn leaf_id_index_name(leaf_name: &str) -> String {
    format!("{leaf_name}_task_idx")
}

pub fn leaf_enqueued_index_name(leaf_name: &str) -> String {
    format!("{leaf_name}_enqueued_idx")
}

pub fn render_daily_leaf_ddl(parent_name: &str, leaf: &LeafRef) -> Result<String, HistoryError> {
    require_safe_identifier(parent_name, "daily-leaf parent")?;
    require_safe_identifier(leaf.leaf_name(), "daily leaf")?;
    Ok(format!(
        "CREATE TABLE {}\n    PARTITION OF {}\n    FOR VALUES FROM ('{}') TO ('{}')",
        leaf.leaf_name(),
        parent_name,
        python_isoformat_utc(leaf.bounds().lower()),
        python_isoformat_utc(leaf.bounds().upper()),
    ))
}

pub fn render_leaf_id_index_ddl(leaf_name: &str) -> Result<String, HistoryError> {
    render_leaf_index_ddl(leaf_name, &leaf_id_index_name(leaf_name), "task_id")
}

pub fn render_leaf_enqueued_index_ddl(leaf_name: &str) -> Result<String, HistoryError> {
    render_leaf_index_ddl(
        leaf_name,
        &leaf_enqueued_index_name(leaf_name),
        ORDERING_INDEX_COLUMN,
    )
}

fn render_leaf_index_ddl(
    leaf_name: &str,
    index_name: &str,
    column: &str,
) -> Result<String, HistoryError> {
    require_safe_identifier(leaf_name, "index leaf")?;
    require_safe_identifier(index_name, "derived index")?;
    Ok(format!(
        "CREATE INDEX {index_name} ON {leaf_name} ({column})"
    ))
}

fn require_safe_identifier(value: &str, label: &str) -> Result<(), HistoryError> {
    if is_safe_identifier(value) {
        Ok(())
    } else {
        Err(HistoryError::contract(format!(
            "{label} {value:?} is not a safe PostgreSQL identifier"
        )))
    }
}

fn python_isoformat_utc(value: DateTime<Utc>) -> String {
    if value.timestamp_subsec_nanos() == 0 {
        value.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()
    } else {
        value.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::history::commands::{LeafBounds, LeafRef};

    #[test]
    fn runtime_ddl_is_canonical_and_rejects_unsafe_derived_names() {
        let lower = DateTime::parse_from_rfc3339("2026-08-11T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let upper = lower + chrono::Duration::days(1);
        let parent = "horsies_task_history_standard_30d";
        let leaf_name = daily_leaf_name(parent, lower).unwrap();
        let leaf = LeafRef::new(
            &leaf_name,
            "standard_30d",
            LeafBounds::new(lower, upper).unwrap(),
        )
        .unwrap();
        assert_eq!(
            render_daily_leaf_ddl(parent, &leaf).unwrap(),
            "CREATE TABLE horsies_task_history_standard_30d_2026_08_11\n    PARTITION OF horsies_task_history_standard_30d\n    FOR VALUES FROM ('2026-08-11T00:00:00+00:00') TO ('2026-08-12T00:00:00+00:00')"
        );
        assert_eq!(
            render_leaf_id_index_ddl(&leaf_name).unwrap(),
            "CREATE INDEX horsies_task_history_standard_30d_2026_08_11_task_idx ON horsies_task_history_standard_30d_2026_08_11 (task_id)"
        );
        assert_eq!(
            render_leaf_enqueued_index_ddl(&leaf_name).unwrap(),
            "CREATE INDEX horsies_task_history_standard_30d_2026_08_11_enqueued_idx ON horsies_task_history_standard_30d_2026_08_11 (enqueued_at)"
        );
        assert!(render_leaf_enqueued_index_ddl(&"a".repeat(55)).is_err());
    }
}
