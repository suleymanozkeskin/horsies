use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

/// Row from the `horsies_heartbeats` table.
///
/// Heartbeats are written by workers while tasks are CLAIMED or RUNNING,
/// and read by the recovery system to detect stale tasks (worker crashes).
///
/// Python equivalent: `TaskHeartbeatModel` in `horsies/core/models/task_pg.py`.
#[derive(Debug, Clone, FromRow)]
pub struct HeartbeatRow {
    /// BIGINT since migration 0020 (`horsies_heartbeats.id`).
    pub id: i64,
    pub task_id: Uuid,
    pub sender_id: String,
    pub role: String,
    pub sent_at: DateTime<Utc>,
    pub hostname: Option<String>,
    pub pid: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the struct can be constructed with all fields (compile-time check
    /// that field names and types are correct relative to the migration schema).
    #[test]
    fn heartbeat_row_field_types() {
        let row = HeartbeatRow {
            id: 1,
            task_id: Uuid::nil(),
            sender_id: "worker-1".to_owned(),
            role: "runner".to_owned(),
            sent_at: Utc::now(),
            hostname: Some("host1".to_owned()),
            pid: Some(1234),
        };
        assert_eq!(row.task_id, Uuid::nil());
        assert_eq!(row.role, "runner");
    }

    /// Verify optional fields can be None.
    #[test]
    fn heartbeat_row_optional_fields() {
        let row = HeartbeatRow {
            id: 2,
            task_id: Uuid::max(),
            sender_id: "worker-2".to_owned(),
            role: "claimer".to_owned(),
            sent_at: Utc::now(),
            hostname: None,
            pid: None,
        };
        assert!(row.hostname.is_none());
        assert!(row.pid.is_none());
    }

    /// HeartbeatRow must implement Debug and Clone.
    #[test]
    fn heartbeat_row_traits() {
        fn assert_debug_clone<T: std::fmt::Debug + Clone>() {}
        assert_debug_clone::<HeartbeatRow>();
    }
}
