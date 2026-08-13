//! Typed clients for the reservation program installed by migration 0034.

use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::core::history::errors::HistoryError;

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

pub async fn claim_key_reservation(
    connection: &mut PgConnection,
    key_digest: &[u8],
    key_scope_version: i16,
    reservation_window_seconds: i64,
    fingerprint_version: i16,
    fingerprint: &[u8],
    task_id: Uuid,
) -> Result<ReservationClaim, HistoryError> {
    let claim_sql = format!(
        "SELECT outcome, task_id, observed_fingerprint_version
         FROM {KEY_RESERVATION_CLAIM_FUNCTION}(
             $1, $2, make_interval(secs => $3::double precision), $4, $5, $6
         )"
    );
    let row: ReservationRow = sqlx::query_as(&claim_sql)
        .bind(key_digest)
        .bind(key_scope_version)
        .bind(reservation_window_seconds)
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

fn decode_reservation_row(
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
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
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
}
