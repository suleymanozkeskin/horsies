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
/// Errors: [`BrokerError::Statement`] and [`BrokerError::RowDecode`].
///
/// Panics when the outcome row breaks the claim contract (an unknown
/// outcome, no task id, or an observed fingerprint version that does not fit
/// the outcome): that is a defect of `horsies_key_reservation_claim`, not a
/// refusal of the caller's input. The panic names the statement and the row
/// (Python `read_enqueue_outcome`, `core/brokers/enqueue_task.py:255-282`).
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
    match decode_reservation_row(outcome.as_deref(), task_id, observed_fingerprint_version) {
        Ok(claim) => Ok(claim),
        Err(error) => panic!(
            "statement `{}` returned a row outside the claim contract \
             (outcome {outcome:?}, task_id {task_id:?}, \
             observed_fingerprint_version {observed_fingerprint_version:?}): {error}",
            statement.name(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::broker::pools::core_pool_for_test_pool;
    use crate::broker::terminalization_matrix::migrated_pool;
    use crate::db::{CheckoutClass, IsolationLevel};

    #[test]
    fn the_claim_statement_carries_the_sqlx_claim_sql() {
        assert_eq!(KEY_RESERVATION_CLAIM.sql(), key_reservation_claim_sql());
        assert_eq!(KEY_RESERVATION_CLAIM.parameter_types().len(), 6);
    }

    /// Python `test_broker_postgres.py::TestEnqueueCatch::test_an_outcome_outside_the_contract_crashes_loud`
    /// (`test_reservation_decode_failure_stays_enqueue_failed` in the test
    /// map): an outcome row outside the claim contract is a defect of the
    /// claim function, not a refusal of the caller's input. The claim panics
    /// with the statement and the row, and answers no catalog error. The
    /// claim function is replaced, in this test's database, by one that
    /// reports the outcome `SOMETHING_ELSE`.
    #[tokio::test]
    #[serial]
    async fn an_outcome_row_outside_the_claim_contract_panics_with_the_statement_and_the_row() {
        let pool = migrated_pool().await;
        sqlx::raw_sql(
            "CREATE OR REPLACE FUNCTION horsies_key_reservation_claim(
                 p_key_digest bytea, p_key_scope_version smallint,
                 p_reservation_window interval, p_fingerprint_version smallint,
                 p_fingerprint bytea, p_task_id uuid
             ) RETURNS horsies_key_reservation_outcome
             LANGUAGE sql AS $$
                 SELECT 'SOMETHING_ELSE'::text, p_task_id, NULL::smallint
             $$;",
        )
        .execute(&pool)
        .await
        .expect("replace the key reservation claim function");
        let task_id = Uuid::from_u128(0x1231);
        let core = core_pool_for_test_pool(&pool);

        let claimed = tokio::spawn(async move {
            let key_digest = [11_u8; 32];
            let fingerprint = [12_u8; 32];
            let mut connection = core
                .checkout(CheckoutClass::Batch)
                .await
                .expect("check out a connection");
            let mut transaction = connection
                .begin(IsolationLevel::ServerDefault)
                .await
                .expect("begin the transaction");
            let claim = KeyClaim {
                key_digest: &key_digest,
                key_scope_version: 1,
                reservation_window_seconds: 3_600,
                fingerprint_version: 1,
                fingerprint: &fingerprint,
                task_id,
            };
            claim_key_reservation(&mut transaction, StatementLog::Off, &claim).await
        })
        .await;

        let panic = match claimed {
            Ok(answer) => panic!("the claim answered {answer:?} instead of a panic"),
            Err(joined) => joined.into_panic(),
        };
        let message = panic
            .downcast_ref::<String>()
            .expect("the panic carries its formatted message");
        assert!(
            message.contains("statement `key_reservation_claim`"),
            "{message}"
        );
        assert!(message.contains("\"SOMETHING_ELSE\""), "{message}");
        assert!(message.contains(&task_id.to_string()), "{message}");
    }
}
