use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

/// Row from the `horsies_heartbeats` table (`baseline/0001_tables.sql`): one
/// field per column, in the table's column order. Every column is NOT NULL.
///
/// Heartbeats are written by workers while tasks are CLAIMED or RUNNING,
/// and read by the recovery system to detect stale tasks (worker crashes).
/// The table has no key: no column set is held unique.
///
/// Python equivalent: `TaskHeartbeatModel` in `python/horsies/core/models/task_pg.py`.
#[derive(Debug, Clone, FromRow)]
pub struct HeartbeatRow {
    pub task_id: Uuid,
    /// `claimer` or `runner` (CHECK `horsies_heartbeats_role_known`).
    pub role: String,
    pub sent_at: DateTime<Utc>,
    pub sender_id: String,
    pub hostname: String,
    pub pid: i32,
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use sqlx::postgres::PgRow;
    use sqlx::{Column, FromRow, Row};
    use uuid::Uuid;

    use super::HeartbeatRow;
    use crate::broker::terminalization_matrix::migrated_pool;

    /// The columns of `horsies_heartbeats`, in table order.
    const HEARTBEAT_COLUMNS: [&str; 6] =
        ["task_id", "role", "sent_at", "sender_id", "hostname", "pid"];

    /// A heartbeat that the baseline table holds decodes into
    /// [`HeartbeatRow`], and the table has no column that the type leaves
    /// out. The database has the heartbeat leaves of the coverage pass, so
    /// a beat sent now has its leaf.
    #[tokio::test]
    async fn a_baseline_heartbeat_row_decodes_into_heartbeat_row() {
        let pool = migrated_pool().await;
        let task_id = Uuid::new_v4();
        let inserted = sqlx::query(
            "INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid) \
             VALUES ($1, 'runner', NOW(), 'heartbeat-row-worker', 'heartbeat-row-host', 4242) \
             RETURNING sent_at",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("insert a heartbeat");
        let sent_at: DateTime<Utc> = inserted.try_get("sent_at").expect("read sent_at");

        let row: PgRow = sqlx::query("SELECT * FROM horsies_heartbeats WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the heartbeat");
        let columns: Vec<&str> = row.columns().iter().map(Column::name).collect();
        assert_eq!(columns, HEARTBEAT_COLUMNS);

        let heartbeat = HeartbeatRow::from_row(&row).expect("a baseline heartbeat row decodes");
        assert_eq!(heartbeat.task_id, task_id);
        assert_eq!(heartbeat.role, "runner");
        assert_eq!(heartbeat.sent_at, sent_at);
        assert_eq!(heartbeat.sender_id, "heartbeat-row-worker");
        assert_eq!(heartbeat.hostname, "heartbeat-row-host");
        assert_eq!(heartbeat.pid, 4242);
    }
}
