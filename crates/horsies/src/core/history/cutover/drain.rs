//! Read-only proof that the old fleet has stopped moving work.

use sqlx::{FromRow, PgConnection};

use crate::core::history::errors::HistoryError;
use crate::core::history::names::{HEARTBEATS_TABLE, LIVE_TASKS};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainOutcome {
    Verified {
        pending_rows: i64,
    },
    Blocked {
        claimed_rows: i64,
        running_rows: i64,
        finalizing_rows: i64,
        recent_heartbeats: i64,
    },
}

#[derive(FromRow)]
struct DrainCounts {
    claimed_rows: i64,
    running_rows: i64,
    finalizing_rows: i64,
    pending_rows: i64,
}

pub async fn verify_drained(
    connection: &mut PgConnection,
    heartbeat_quiet_seconds: f64,
) -> Result<DrainOutcome, HistoryError> {
    if !heartbeat_quiet_seconds.is_finite() || heartbeat_quiet_seconds < 0.0 {
        return Err(HistoryError::contract(
            "heartbeat quiet interval must be finite and non-negative",
        ));
    }
    let counts: DrainCounts = sqlx::query_as(&format!(
        "SELECT
             count(*) FILTER (WHERE status = 'CLAIMED') AS claimed_rows,
             count(*) FILTER (WHERE status = 'RUNNING') AS running_rows,
             count(*) FILTER (
                 WHERE status IN ('CLAIMED', 'RUNNING')
                   AND finalizing_at IS NOT NULL
             ) AS finalizing_rows,
             count(*) FILTER (WHERE status = 'PENDING') AS pending_rows
         FROM {LIVE_TASKS}"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let recent_heartbeats: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {HEARTBEATS_TABLE}
         WHERE sent_at > statement_timestamp() - make_interval(secs => $1)"
    ))
    .bind(heartbeat_quiet_seconds)
    .fetch_one(connection)
    .await?;

    if counts.claimed_rows != 0
        || counts.running_rows != 0
        || counts.finalizing_rows != 0
        || recent_heartbeats != 0
    {
        Ok(DrainOutcome::Blocked {
            claimed_rows: counts.claimed_rows,
            running_rows: counts.running_rows,
            finalizing_rows: counts.finalizing_rows,
            recent_heartbeats,
        })
    } else {
        Ok(DrainOutcome::Verified {
            pending_rows: counts.pending_rows,
        })
    }
}
