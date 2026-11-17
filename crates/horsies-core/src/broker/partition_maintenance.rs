//! Partition maintenance of a client handle on its session-work core pool
//! (`crates/horsies-core/baseline/0008_history_maintenance_functions.sql`):
//!
//! - [`PostgresBroker::ensure_partition_coverage`]: one coverage pass (Python
//!   `maintain_partition_coverage`): gated at worker startup, ungated in the
//!   reaper's periodic pass.
//!
//! The session-work pool is the session pool when the session URL differs
//! from the main URL, else the main pool: partition DDL and the coverage gate
//! need a session-capable connection. Its connections carry the TCP
//! keepalives and the TCP user timeout of the config. A pass checks the
//! client family once per handle before its first write, and holds a write
//! token, so [`PostgresBroker::close`] waits for it.

use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage::{
    run_coverage_pass, CoverageOutcome, CoverageRequest, CoverageSerialization,
};

/// Why a coverage pass has no outcome.
#[derive(Debug, thiserror::Error)]
pub enum CoveragePassError {
    /// The pools could not open ([`BrokerError::PoolSettings`]), or the
    /// client family check refused ([`BrokerError::ClientFamilyMismatch`])
    /// or could not read the family row ([`BrokerError::Statement`]).
    /// Nothing was sent to the history tables.
    #[error(transparent)]
    Broker(BrokerError),
    /// A probe, the coverage gate, the class registration or the final
    /// publication failed, or a row broke its function's contract. The leaf
    /// transactions that committed before the failure stay committed; the
    /// gate is released.
    #[error(transparent)]
    History(HistoryError),
}

impl CoveragePassError {
    /// Whether the cause may clear: the broker or history error's own rule.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(error) => error.is_retryable(),
            Self::History(error) => error.is_retryable(),
        }
    }
}

impl PostgresBroker {
    /// One coverage pass on the session-work core pool (Python
    /// `maintain_partition_coverage`, with `serialize_damaged_work` as
    /// `serialization`): a healthy set costs one probe statement; a damaged
    /// set is repaired with one transaction per leaf, and a leaf's error is
    /// contained in the outcome. Writes (DDL and catalog rows).
    ///
    /// Errors: [`CoveragePassError`].
    pub async fn ensure_partition_coverage(
        &self,
        request: &CoverageRequest,
        serialization: CoverageSerialization,
    ) -> Result<CoverageOutcome, CoveragePassError> {
        self.ensure_client_family_for_write()
            .await
            .map_err(CoveragePassError::Broker)?;
        let (open, _write) = self
            .core_pools_for_write()
            .map_err(CoveragePassError::Broker)?;
        run_coverage_pass(
            open.pools().session_work(),
            self.statement_log(),
            request,
            serialization,
        )
        .await
        .map_err(CoveragePassError::History)
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::broker::client_family::{ClientFamily, RecordedFamily};

    async fn class_rows(pool: &sqlx::PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM horsies_retention_classes")
            .fetch_one(pool)
            .await
            .expect("count the class rows")
    }

    /// A pass on a database of this handle's family ends with an outcome
    /// that covers the present heartbeat hour.
    #[tokio::test]
    #[serial]
    async fn a_pass_of_the_recorded_family_ends_covered() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = PostgresBroker::from_pool(pool);
        let request = CoverageRequest::new(2, 2, &[]).expect("a request in bounds");

        let outcome = broker
            .ensure_partition_coverage(&request, CoverageSerialization::Gated)
            .await
            .expect("a pass of the recorded family");

        assert!(outcome.heartbeat_covered_now(), "{outcome:?}");
    }

    /// A handle of the other family is refused before the pass writes: the
    /// class rows do not change, and the refusal is not retryable.
    #[tokio::test]
    #[serial]
    async fn a_handle_of_the_other_family_is_refused_before_the_pass() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let before = class_rows(&pool).await;
        let broker =
            PostgresBroker::from_pool(pool.clone()).with_client_family(ClientFamily::Python);
        let request = CoverageRequest::new(2, 2, &[]).expect("a request in bounds");

        let refused = broker
            .ensure_partition_coverage(&request, CoverageSerialization::PerLeafLocks)
            .await
            .expect_err("the other family is refused");

        assert!(
            matches!(
                refused,
                CoveragePassError::Broker(BrokerError::ClientFamilyMismatch {
                    stored: RecordedFamily::Recorded(ClientFamily::Rust),
                    own: ClientFamily::Python,
                })
            ),
            "{refused:?}"
        );
        assert!(!refused.is_retryable());
        assert_eq!(class_rows(&pool).await, before);
    }

    /// A history failure keeps its own retry rule.
    #[test]
    fn a_history_failure_keeps_its_retry_rule() {
        let broken = CoveragePassError::History(HistoryError::contract("probe returned no row"));
        assert!(!broken.is_retryable());
        let timed_out = CoveragePassError::History(HistoryError::Statement(
            crate::db::StatementFailure::NotSent {
                statement: "coverage_probe",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Session,
                    waited: std::time::Duration::from_secs(30),
                }),
            },
        ));
        assert!(timed_out.is_retryable());
    }
}
