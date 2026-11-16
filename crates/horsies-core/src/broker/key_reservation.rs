//! The idempotency key reservation claim of the enqueue, on the statement
//! layer.
//!
//! The SQL text and the outcome decoding are those of
//! `core::history::identity::reservations` (`key_reservation_claim_sql`,
//! `decode_reservation_row`); the sqlx form stays for the rerun in the
//! caller's transaction.

use std::sync::LazyLock;

use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, query_in, StatementLog};
use crate::broker::error::BrokerError;
use crate::core::history::identity::reservations::{
    decode_reservation_row, key_reservation_claim_sql, ReservationClaim,
};
use crate::db::{ParameterType, Transaction, TypedStatement};

static KEY_RESERVATION_CLAIM_SQL: LazyLock<String> = LazyLock::new(key_reservation_claim_sql);

static KEY_RESERVATION_CLAIM: LazyLock<TypedStatement> = LazyLock::new(|| {
    TypedStatement::new(
        "key_reservation_claim",
        KEY_RESERVATION_CLAIM_SQL.as_str(),
        &[
            ParameterType::Bytea,
            ParameterType::Int2,
            ParameterType::Int8,
            ParameterType::Int2,
            ParameterType::Bytea,
            ParameterType::Uuid,
        ],
    )
});

/// The arguments of one key reservation claim.
pub(crate) struct KeyClaim<'a> {
    pub(crate) key_digest: &'a [u8],
    pub(crate) key_scope_version: i16,
    pub(crate) reservation_window_seconds: i64,
    pub(crate) fingerprint_version: i16,
    pub(crate) fingerprint: &'a [u8],
    pub(crate) task_id: Uuid,
}

/// Claims the reservation of an idempotency key in the caller's
/// transaction. Writes the reservation row when the outcome is `Applied`;
/// the caller's commit or rollback decides whether it stays.
///
/// Errors: [`BrokerError::Statement`], [`BrokerError::RowDecode`], and
/// [`BrokerError::EnqueueContract`] for an outcome row that breaks the
/// claim contract (as the sqlx path maps it).
pub(crate) async fn claim_key_reservation(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    claim: &KeyClaim<'_>,
) -> Result<ReservationClaim, BrokerError> {
    let statement: &TypedStatement = &KEY_RESERVATION_CLAIM;
    let rows = query_in(
        transaction,
        log,
        &statement
            .bind()
            .value(&claim.key_digest)
            .value(&claim.key_scope_version)
            .value(&claim.reservation_window_seconds)
            .value(&claim.fingerprint_version)
            .value(&claim.fingerprint)
            .value(&claim.task_id)
            .finish(),
    )
    .await?;
    let row = one_row(rows, statement)?;
    let outcome: Option<String> = column(&row, statement, "outcome")?;
    let task_id: Option<Uuid> = column(&row, statement, "task_id")?;
    let observed_fingerprint_version: Option<i16> =
        column(&row, statement, "observed_fingerprint_version")?;
    decode_reservation_row(outcome.as_deref(), task_id, observed_fingerprint_version)
        .map_err(|error| BrokerError::EnqueueContract(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_claim_statement_carries_the_sqlx_claim_sql() {
        assert_eq!(KEY_RESERVATION_CLAIM.sql(), key_reservation_claim_sql());
        assert_eq!(KEY_RESERVATION_CLAIM.parameter_types().len(), 6);
    }
}
