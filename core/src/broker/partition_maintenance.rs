//! Partition maintenance of a client handle on its session-work core pool
//! (`core/baseline/0008_history_maintenance_functions.sql`):
//!
//! - [`PostgresBroker::ensure_partition_coverage`]: one coverage pass (Python
//!   `maintain_partition_coverage`): gated at worker startup and at the end
//!   of the schema install (`PostgresBroker::cover_after_install`), ungated
//!   in the reaper's periodic pass.
//! - [`PostgresBroker::prune_expired_partitions`]: one pruning pass (Python
//!   `prune_expired_partitions`), the reaper's detach and drop of the expired
//!   leaves (`0006_history_functions.sql`).
//!
//! The session-work pool is the session pool when the session URL differs
//! from the main URL, else the main pool: partition DDL, the coverage gate and
//! the session leaf lock of a detach need a session-capable connection. Its connections carry the TCP
//! keepalives and the TCP user timeout of the config. A pass checks the
//! client family once per handle before its first write, and holds a write
//! token, so [`PostgresBroker::close`] waits for it.

use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage::{
    run_coverage_pass, CoverageOutcome, CoverageRequest, CoverageSerialization,
};
use crate::core::history::maintenance::pruning::{self, PrunePass};

/// The coverage request of the schema install of a handle whose client set
/// none ([`PostgresBroker::with_install_coverage`]): the request of the
/// default retention config, which covers the forever class and the 30-day
/// default class. Pure.
///
/// # Panics
///
/// When the default retention config is outside the coverage bounds: a
/// defect of the defaults, which `RetentionConfig::validate` accepts.
pub(crate) fn default_install_coverage() -> CoverageRequest {
    match CoverageRequest::of_retention(&RetentionConfig::default()) {
        Ok(request) => request,
        Err(refused) => {
            panic!("the default retention config is outside the coverage bounds: {refused}")
        }
    }
}

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

/// Why a pruning pass has no report.
#[derive(Debug, thiserror::Error)]
pub enum PrunePassError {
    /// The pools could not open ([`BrokerError::PoolSettings`]), or the
    /// client family check refused ([`BrokerError::ClientFamilyMismatch`])
    /// or could not read the family row ([`BrokerError::Statement`]).
    /// Nothing was sent to the history tables.
    #[error(transparent)]
    Broker(BrokerError),
    /// The candidate read (`horsies_prune_candidates`) failed, or its rows
    /// broke the function's contract. Nothing was detached or dropped.
    #[error(transparent)]
    History(HistoryError),
}

impl PrunePassError {
    /// Whether the cause may clear: the broker or history error's own rule.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Broker(error) => error.is_retryable(),
            Self::History(error) => error.is_retryable(),
        }
    }
}

impl PostgresBroker {
    /// One pruning pass on the session-work core pool (Python
    /// `prune_expired_partitions` with `StagedLoaderPublisher`): one
    /// candidate read, the finalization of each interrupted detach, then the
    /// heartbeat and the period-class sweeps, at most
    /// `MAX_PRUNE_LEAVES_PER_PASS` acted leaves. Each leaf's detach and drop
    /// commit alone; a leaf's error or refusal is listed in the report and
    /// the pass goes on. Writes (DDL and catalog rows).
    ///
    /// A cancel of the caller's future stops the pass between two
    /// statements: the leaves before it stay committed, and a leaf whose
    /// session lock state is not known has its connection closed.
    ///
    /// Errors: [`PrunePassError`].
    pub async fn prune_expired_partitions(&self) -> Result<PrunePass, PrunePassError> {
        self.ensure_client_family_for_write()
            .await
            .map_err(PrunePassError::Broker)?;
        let (open, _write) = self
            .core_pools_for_write()
            .map_err(PrunePassError::Broker)?;
        pruning::prune_expired_partitions(open.pools().session_work(), self.statement_log())
            .await
            .map_err(PrunePassError::History)
    }

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

    /// The coverage pass at the end of the schema install
    /// ([`PostgresBroker::ensure_schema_initialized`]): the gated pass of the
    /// install's coverage request ([`PostgresBroker::install_coverage`]),
    /// the pass that the worker start runs. A producer that installs the
    /// schema so leaves a database where the terminal writes of its classes
    /// find their leaves for the current UTC day and the lookahead, with or
    /// without a worker. Writes (DDL and catalog rows).
    ///
    /// A contained refusal ([`CoverageOutcome::Failed`], for example the
    /// coverage gate held by a worker's start) is logged and ends the
    /// install: a terminal write whose leaf is still missing fails closed
    /// with SQLSTATE `55H01` ([`HISTORY_LEAF_MISSING_SQLSTATE`]), and the
    /// worker start and the reaper run the pass again.
    ///
    /// Errors: [`BrokerError::InstallCoverage`] when the pass had no outcome;
    /// the [`BrokerError`] of the pools or of the family check, with nothing
    /// sent to the history tables. The leaf transactions that committed
    /// before an error stay.
    ///
    /// [`HISTORY_LEAF_MISSING_SQLSTATE`]: crate::core::history::errors::HISTORY_LEAF_MISSING_SQLSTATE
    pub(crate) async fn cover_after_install(&self) -> Result<(), BrokerError> {
        let outcome = self
            .ensure_partition_coverage(self.install_coverage(), CoverageSerialization::Gated)
            .await
            .map_err(|error| match error {
                CoveragePassError::Broker(error) => error,
                CoveragePassError::History(error) => BrokerError::InstallCoverage(error),
            })?;
        match outcome {
            CoverageOutcome::Ensured(ensured) => {
                tracing::debug!(
                    created_history_leaves = ensured.created_history_leaves,
                    created_heartbeat_leaves = ensured.created_heartbeat_leaves,
                    "the schema install's coverage pass ensured the leaves",
                );
            }
            CoverageOutcome::Failed(failed) => {
                tracing::warn!(
                    stage = failed.stage.as_str(),
                    class_key = ?failed.class_key,
                    refusal = %failed.refusal,
                    absent_leaves = ?failed.absent_leaves,
                    "the schema install's coverage pass stopped at {}: {}; a terminal write \
                     whose leaf is missing fails with SQLSTATE 55H01 until a later pass \
                     creates it",
                    failed.stage.as_str(),
                    failed.refusal,
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serial_test::serial;
    use uuid::Uuid;

    use super::*;
    use crate::broker::client_family::{ClientFamily, RecordedFamily};
    use crate::broker::{CancelAnswer, SendKind};
    use crate::core::config::EnqueueRetention;
    use crate::core::history::identity::uuid7::mint_task_id;
    use crate::core::history::periods::RetentionDays;
    use crate::core::types::TaskStatus;
    use crate::test_support::sql::{self, TestPool};
    use crate::test_support::TestDatabase;

    async fn class_rows(pool: &TestPool) -> i64 {
        sql::query_scalar("SELECT count(*) FROM horsies_retention_classes")
            .fetch_one(pool)
            .await
            .expect("count the class rows")
    }

    /// A pass on a database of this handle's family ends with an outcome
    /// that covers the present heartbeat hour.
    #[tokio::test]
    #[serial]
    async fn a_pass_of_the_recorded_family_ends_covered() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), ClientFamily::Rust);
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
        let pool = crate::test_support::migrated_pool().await;
        let before = class_rows(&pool).await;
        let broker = PostgresBroker::from_test_url(pool.url(), ClientFamily::Python);
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

    async fn catalog_stamps(pool: &TestPool) -> (i64, i64) {
        sql::query_as(
            "SELECT count(detached_at), count(dropped_at) FROM horsies_task_history_leaf_catalog",
        )
        .fetch_one(pool)
        .await
        .expect("count the catalog stamps")
    }

    /// A pruning pass on a database of this handle's family passes the family
    /// check and ends with a report. The shared database may hold expired
    /// leaves of other tests, so the report itself is not checked here; the
    /// pass tests of `core::history::partitions::tests` check it.
    #[tokio::test]
    #[serial]
    async fn a_pruning_pass_of_the_recorded_family_ends_with_a_report() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = PostgresBroker::from_test_url(pool.url(), ClientFamily::Rust);

        let _pass = broker
            .prune_expired_partitions()
            .await
            .expect("a pass of the recorded family");
    }

    /// A handle of the other family is refused before the pruning pass
    /// writes: no catalog stamp changes, and the refusal is not retryable.
    #[tokio::test]
    #[serial]
    async fn a_handle_of_the_other_family_is_refused_before_the_pruning_pass() {
        let pool = crate::test_support::migrated_pool().await;
        let before = catalog_stamps(&pool).await;
        let broker = PostgresBroker::from_test_url(pool.url(), ClientFamily::Python);

        let refused = broker
            .prune_expired_partitions()
            .await
            .expect_err("the other family is refused");

        assert!(
            matches!(
                refused,
                PrunePassError::Broker(BrokerError::ClientFamilyMismatch {
                    stored: RecordedFamily::Recorded(ClientFamily::Rust),
                    own: ClientFamily::Python,
                })
            ),
            "{refused:?}"
        );
        assert!(!refused.is_retryable());
        assert_eq!(catalog_stamps(&pool).await, before);
    }

    /// A failed candidate read keeps the history error's retry rule.
    #[test]
    fn a_failed_candidate_read_keeps_its_retry_rule() {
        let broken = PrunePassError::History(HistoryError::contract(
            "prune candidate bound does not fit an integer limit",
        ));
        assert!(!broken.is_retryable());
        let timed_out = PrunePassError::History(HistoryError::Statement(
            crate::db::StatementFailure::NotSent {
                statement: "prune_candidates",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Session,
                    waited: std::time::Duration::from_secs(30),
                }),
            },
        ));
        assert!(timed_out.is_retryable());
    }

    /// Enqueue one PENDING task of `retention` with no deadline.
    async fn enqueue_pending(broker: &PostgresBroker, retention: EnqueueRetention) -> Uuid {
        broker
            .enqueue(
                "install_coverage.task",
                "{}",
                "default",
                100,
                Some(Utc::now()),
                None,
                None,
                None,
                Some(mint_task_id().expect("mint a task id")),
                None,
                None,
                retention,
                None,
                SendKind::First,
            )
            .await
            .expect("the send writes its row")
    }

    /// The status and the class key of each history row of `task_ids`, by
    /// task id.
    async fn history_rows(
        database: &TestDatabase,
        task_ids: &[Uuid],
    ) -> Vec<(Uuid, String, String)> {
        let rows = database
            .client()
            .query(
                "SELECT task_id, status::text, retention_class_key::text FROM horsies_task_history \
                 WHERE task_id = ANY($1) ORDER BY task_id",
                &[&task_ids],
            )
            .await
            .expect("read the history rows");
        rows.iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect()
    }

    /// A fresh install, and no worker ever started: the install's coverage
    /// pass creates the leaves of the request's classes, so a producer's
    /// administrative cancel (`CANCEL_ADMIN`, the single move) of a task of a
    /// declared class and its pending expiry (`EXPIRE_PENDING`, the batch
    /// move) of a task of the default class both move to history. Before the
    /// pass, the cancel failed with 23514 (no partition for the row).
    #[tokio::test]
    #[serial]
    async fn a_fresh_install_covers_the_terminal_writes_of_a_producer_with_no_worker() {
        let database = TestDatabase::create_empty().await;
        let seven_days =
            RetentionDays::parse_period(chrono::Duration::days(7)).expect("seven days");
        let request = CoverageRequest::new(2, 2, &[seven_days]).expect("a request in bounds");
        let broker = PostgresBroker::connect(database.url(), ClientFamily::Rust)
            .await
            .expect("connect the broker")
            .with_install_coverage(request);

        broker
            .ensure_schema_initialized()
            .await
            .expect("the install and its coverage pass");
        let workers: i64 = database
            .client()
            .query_one("SELECT count(*) FROM horsies_worker_states", &[])
            .await
            .expect("count the worker states")
            .get(0);
        assert_eq!(workers, 0, "no worker started");

        let cancelled = enqueue_pending(&broker, EnqueueRetention::Period(seven_days)).await;
        let answer = broker
            .cancel_task(cancelled, false)
            .await
            .expect("the cancel has an answer");
        assert_eq!(
            answer,
            CancelAnswer::Cancelled {
                was_status: TaskStatus::Pending
            }
        );

        let expired = enqueue_pending(&broker, EnqueueRetention::Default).await;
        let _ = database
            .client()
            .execute(
                "UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = $1",
                &[&expired],
            )
            .await
            .expect("pass the deadline");
        let pass = broker
            .expire_pending_tasks()
            .await
            .expect("the expiry pass");
        assert_eq!(pass.expired(), 1, "{pass:?}");

        let mut expected = vec![
            (cancelled, "CANCELLED".to_owned(), "standard_7d".to_owned()),
            (expired, "EXPIRED".to_owned(), "standard_30d".to_owned()),
        ];
        expected.sort();
        assert_eq!(
            history_rows(&database, &[cancelled, expired]).await,
            expected
        );
        broker.close().await;
        drop(broker);
        database.drop().await;
    }

    /// A handle whose client set no request covers the classes of the
    /// default retention config at its install: the cancel of a task of the
    /// default class moves to history with no worker.
    #[tokio::test]
    #[serial]
    async fn the_install_of_a_handle_with_no_request_covers_the_default_class() {
        let database = TestDatabase::create_empty().await;
        let broker = PostgresBroker::connect(database.url(), ClientFamily::Rust)
            .await
            .expect("connect the broker");
        assert_eq!(broker.install_coverage(), &default_install_coverage());

        broker
            .ensure_schema_initialized()
            .await
            .expect("the install and its coverage pass");
        let task_id = enqueue_pending(&broker, EnqueueRetention::Default).await;
        let answer = broker
            .cancel_task(task_id, false)
            .await
            .expect("the cancel has an answer");

        assert_eq!(
            answer,
            CancelAnswer::Cancelled {
                was_status: TaskStatus::Pending
            }
        );
        assert_eq!(
            history_rows(&database, &[task_id]).await,
            vec![(task_id, "CANCELLED".to_owned(), "standard_30d".to_owned())]
        );
        broker.close().await;
        drop(broker);
        database.drop().await;
    }

    /// The default request is the request of the default retention config:
    /// its horizons, and the 30-day default class, which every request
    /// declares.
    #[test]
    fn the_default_install_coverage_is_the_default_retention_request() {
        let defaults = RetentionConfig::default();
        let request = CoverageRequest::new(
            defaults.history_leaf_horizon_days,
            defaults.heartbeat_leaf_horizon_hours,
            &[],
        )
        .expect("the default horizons are in bounds");
        assert_eq!(default_install_coverage(), request);
    }

    /// A failed coverage pass of the install keeps the history error's retry
    /// rule and cause.
    #[test]
    fn an_install_coverage_failure_keeps_the_history_error_s_retry_rule() {
        let broken = BrokerError::InstallCoverage(HistoryError::contract("probe returned no row"));
        assert!(!broken.is_retryable());
        assert_eq!(
            broken.failure_cause(),
            crate::failure_cause::FailureCause::ContractViolation
        );
        let timed_out = BrokerError::InstallCoverage(HistoryError::Statement(
            crate::db::StatementFailure::NotSent {
                statement: "coverage_probe",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Session,
                    waited: std::time::Duration::from_secs(30),
                }),
            },
        ));
        assert!(timed_out.is_retryable());
        assert_eq!(
            timed_out.failure_cause(),
            crate::failure_cause::FailureCause::Connection
        );
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
