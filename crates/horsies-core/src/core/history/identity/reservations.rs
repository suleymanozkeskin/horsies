//! Typed clients for the reservation program installed by migration 0034.

use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::broker::core_rows::{one_row, query_on, scalar, StatementLog};
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage::maintenance_connection;
use crate::db::{CorePool, ParameterType, TypedStatement};

pub const KEY_RESERVATION_OUTCOME_TYPE: &str = "horsies_key_reservation_outcome";
pub const KEY_RESERVATION_CLAIM_FUNCTION: &str = "horsies_key_reservation_claim";
pub const KEY_RESERVATION_TERMINALIZE_FUNCTION: &str = "horsies_key_reservation_terminalize";
pub const KEY_RESERVATION_TERMINALIZE_BATCH_FUNCTION: &str =
    "horsies_key_reservation_terminalize_batch";
pub const KEY_RESERVATION_CLEANUP_FUNCTION: &str = "horsies_key_reservation_cleanup";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationClaim {
    Applied {
        task_id: Uuid,
    },
    Replay {
        task_id: Uuid,
    },
    Conflict {
        task_id: Uuid,
        observed_fingerprint_version: i16,
    },
}

#[derive(FromRow)]
struct ReservationRow {
    outcome: Option<String>,
    task_id: Option<Uuid>,
    observed_fingerprint_version: Option<i16>,
}

/// The key reservation claim: `$1` key digest, `$2` key scope version, `$3`
/// reservation window in microseconds (made into the exact `interval`), `$4`
/// fingerprint version, `$5` fingerprint, `$6` task id. One row: `outcome`,
/// `task_id`, `observed_fingerprint_version`.
pub(crate) fn key_reservation_claim_sql() -> String {
    format!(
        "SELECT outcome, task_id, observed_fingerprint_version
         FROM {KEY_RESERVATION_CLAIM_FUNCTION}(
             $1, $2, $3::bigint * interval '1 microsecond', $4, $5, $6
         )"
    )
}

pub async fn claim_key_reservation(
    connection: &mut PgConnection,
    key_digest: &[u8],
    key_scope_version: i16,
    reservation_window_micros: i64,
    fingerprint_version: i16,
    fingerprint: &[u8],
    task_id: Uuid,
) -> Result<ReservationClaim, HistoryError> {
    let claim_sql = key_reservation_claim_sql();
    let row: ReservationRow = sqlx::query_as(&claim_sql)
        .bind(key_digest)
        .bind(key_scope_version)
        .bind(reservation_window_micros)
        .bind(fingerprint_version)
        .bind(fingerprint)
        .bind(task_id)
        .fetch_one(&mut *connection)
        .await?;
    decode_reservation_row(
        row.outcome.as_deref(),
        row.task_id,
        row.observed_fingerprint_version,
    )
}

pub async fn terminalize_key_reservation(
    connection: &mut PgConnection,
    key_digest: &[u8],
    task_id: Uuid,
) -> Result<bool, HistoryError> {
    let terminalize_sql = format!(
        "SELECT {KEY_RESERVATION_TERMINALIZE_FUNCTION}(
             $1, $2, statement_timestamp()
         )"
    );
    let updated: Option<bool> = sqlx::query_scalar(&terminalize_sql)
        .bind(key_digest)
        .bind(task_id)
        .fetch_one(&mut *connection)
        .await?;
    updated.ok_or_else(|| HistoryError::contract("reservation terminalize did not return boolean"))
}

pub async fn cleanup_expired_reservations(
    connection: &mut PgConnection,
    batch_size: i32,
) -> Result<i32, HistoryError> {
    if batch_size <= 0 {
        return Err(HistoryError::contract(
            "cleanup batch size must be positive",
        ));
    }
    let cleanup_sql = format!("SELECT {KEY_RESERVATION_CLEANUP_FUNCTION}($1)");
    let deleted: Option<i32> = sqlx::query_scalar(&cleanup_sql)
        .bind(batch_size)
        .fetch_one(&mut *connection)
        .await?;
    deleted.ok_or_else(|| HistoryError::contract("reservation cleanup did not return a count"))
}

/// Cleanup statements one maintenance pass runs at most. Each statement
/// deletes at most one batch and commits on its own.
pub const RESERVATION_CLEANUP_BATCHES_PER_PASS: u32 = 20;

/// What one bounded cleanup pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationCleanupPass {
    /// The last batch was not full: no expired `TERMINAL` row was left.
    Complete { deleted: u64 },
    /// Every batch of the pass was full: expired rows may remain for the next
    /// pass.
    Truncated { deleted: u64 },
}

/// `horsies_key_reservation_cleanup($1)`: delete at most `$1` expired
/// `TERMINAL` reservations. One row: the count deleted.
static KEY_RESERVATION_CLEANUP: TypedStatement = TypedStatement::new(
    "key_reservation_cleanup",
    "SELECT horsies_key_reservation_cleanup($1)",
    &[ParameterType::Int4],
);

/// Delete expired `TERMINAL` reservations in batches of `batch_size`, at most
/// `max_batches` statements, each in autocommit on one connection of `pool`.
/// The connection is checked out as reserved work (the reaper's
/// maintenance; on a pool with no reserve the class changes nothing).
///
/// A failed statement ends the pass with its error; the batches before it
/// stay committed. Live and unexpired rows are never touched.
///
/// Errors: [`HistoryError::Contract`] for a `batch_size` below 1 (nothing is
/// sent), or a count that is null or negative; [`HistoryError::Statement`]
/// for the checkout or a failed statement; [`HistoryError::RowDecode`] for a
/// reply that is not one integer.
pub async fn cleanup_expired_reservations_pass(
    pool: &CorePool,
    log: StatementLog,
    batch_size: i32,
    max_batches: u32,
) -> Result<ReservationCleanupPass, HistoryError> {
    if batch_size <= 0 {
        return Err(HistoryError::contract(
            "cleanup batch size must be positive",
        ));
    }
    let statement = &KEY_RESERVATION_CLEANUP;
    let mut connection = maintenance_connection(pool, statement).await?;
    let mut deleted = 0_u64;
    for _ in 0..max_batches {
        let rows = query_on(
            &mut connection,
            log,
            &statement.bind().value(&batch_size).finish(),
        )
        .await?;
        let batch: Option<i32> = scalar(&one_row(rows, statement)?, statement)?;
        let batch = batch
            .ok_or_else(|| HistoryError::contract("reservation cleanup did not return a count"))?;
        let batch_deleted = u64::try_from(batch)
            .map_err(|_| HistoryError::contract("reservation cleanup returned a negative count"))?;
        deleted += batch_deleted;
        if batch < batch_size {
            return Ok(ReservationCleanupPass::Complete { deleted });
        }
    }
    Ok(ReservationCleanupPass::Truncated { deleted })
}

/// The claim outcome of one reservation row. Pure.
pub(crate) fn decode_reservation_row(
    outcome: Option<&str>,
    task_id: Option<Uuid>,
    observed_fingerprint_version: Option<i16>,
) -> Result<ReservationClaim, HistoryError> {
    let (outcome, task_id) = match (outcome, task_id) {
        (Some(outcome), Some(task_id)) => (outcome, task_id),
        _ => {
            return Err(HistoryError::contract(
                "reservation outcome row did not decode",
            ));
        }
    };
    match (outcome, observed_fingerprint_version) {
        ("APPLIED", None) => Ok(ReservationClaim::Applied { task_id }),
        ("REPLAY", _) => Ok(ReservationClaim::Replay { task_id }),
        ("CONFLICT", Some(version)) => Ok(ReservationClaim::Conflict {
            task_id,
            observed_fingerprint_version: version,
        }),
        ("APPLIED", Some(_)) => Err(HistoryError::contract(
            "applied reservation carried an observed fingerprint",
        )),
        ("CONFLICT", None) => Err(HistoryError::contract(
            "conflict outcome lacked the observed fingerprint version",
        )),
        _ => Err(HistoryError::contract(format!(
            "unknown reservation outcome {outcome:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn reservation_row_decode_fails_closed() {
        let task_id = Uuid::nil();
        assert_eq!(
            decode_reservation_row(Some("APPLIED"), Some(task_id), None).unwrap(),
            ReservationClaim::Applied { task_id }
        );
        assert!(decode_reservation_row(Some("CONFLICT"), Some(task_id), None).is_err());
        assert!(decode_reservation_row(Some("FOREIGN"), Some(task_id), None).is_err());
        assert!(decode_reservation_row(Some("APPLIED"), None, None).is_err());
    }

    #[tokio::test]
    #[serial]
    async fn cleanup_wrapper_deletes_only_expired_terminal_rows_with_the_requested_bound() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let expired = [vec![31_u8; 32], vec![32_u8; 32]];
        let retained = vec![33_u8; 32];
        for (index, digest) in expired.iter().enumerate() {
            sqlx::query(
                "INSERT INTO horsies_key_reservations (
                    idempotency_key_digest, key_scope_version, fingerprint_version,
                    command_fingerprint, task_id, disposition, reservation_window, expires_at
                 ) VALUES ($1, 1, 1, $2, $3, 'TERMINAL', INTERVAL '1 hour',
                           NOW() - INTERVAL '1 hour')",
            )
            .bind(digest)
            .bind(vec![index as u8 + 1; 32])
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
            .expect("seed expired reservation");
        }
        sqlx::query(
            "INSERT INTO horsies_key_reservations (
                idempotency_key_digest, key_scope_version, fingerprint_version,
                command_fingerprint, task_id, disposition, reservation_window, expires_at
             ) VALUES ($1, 1, 1, $2, $3, 'TERMINAL', INTERVAL '1 hour',
                       NOW() + INTERVAL '1 hour')",
        )
        .bind(&retained)
        .bind(vec![9_u8; 32])
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .expect("seed retained reservation");

        let mut connection = pool.acquire().await.expect("reservation connection");
        assert_eq!(
            cleanup_expired_reservations(&mut connection, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            cleanup_expired_reservations(&mut connection, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            cleanup_expired_reservations(&mut connection, 1)
                .await
                .unwrap(),
            0
        );
        assert!(cleanup_expired_reservations(&mut connection, 0)
            .await
            .is_err());
        drop(connection);

        let remaining: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM horsies_key_reservations
             WHERE idempotency_key_digest = $1",
        )
        .bind(&retained)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, 1, "unexpired TERMINAL evidence is retained");
        sqlx::query("DELETE FROM horsies_key_reservations WHERE idempotency_key_digest = $1")
            .bind(&retained)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn cleanup_pass_stops_at_its_batch_bound_and_reports_truncation() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let core = crate::broker::pools::core_pool_for_test_pool(&pool);
        // Start from no expired row of other tests in the shared database.
        cleanup_expired_reservations_pass(&core, StatementLog::Off, 10_000, 100)
            .await
            .expect("clear expired reservations");
        let expired: Vec<Vec<u8>> = (0..3_u8).map(|index| vec![41 + index; 32]).collect();
        for digest in &expired {
            sqlx::query(
                "INSERT INTO horsies_key_reservations (
                    idempotency_key_digest, key_scope_version, fingerprint_version,
                    command_fingerprint, task_id, disposition, reservation_window, expires_at
                 ) VALUES ($1, 1, 1, $2, $3, 'TERMINAL', INTERVAL '1 hour',
                           NOW() - INTERVAL '1 hour')",
            )
            .bind(digest)
            .bind(vec![7_u8; 32])
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
            .expect("seed expired reservation");
        }
        assert_eq!(
            cleanup_expired_reservations_pass(&core, StatementLog::Off, 1, 2)
                .await
                .unwrap(),
            ReservationCleanupPass::Truncated { deleted: 2 }
        );
        assert_eq!(
            cleanup_expired_reservations_pass(&core, StatementLog::Off, 2, 2)
                .await
                .unwrap(),
            ReservationCleanupPass::Complete { deleted: 1 }
        );
        assert_eq!(
            cleanup_expired_reservations_pass(&core, StatementLog::Off, 2, 2)
                .await
                .unwrap(),
            ReservationCleanupPass::Complete { deleted: 0 }
        );
        assert!(
            cleanup_expired_reservations_pass(&core, StatementLog::Off, 0, 2)
                .await
                .is_err()
        );
    }
}
