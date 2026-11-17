use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

/// Row from the `horsies_heartbeats` table.
///
/// Heartbeats are written by workers while tasks are CLAIMED or RUNNING,
/// and read by the recovery system to detect stale tasks (worker crashes).
///
/// Python equivalent: `TaskHeartbeatModel` in `python/horsies/core/models/task_pg.py`.
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
