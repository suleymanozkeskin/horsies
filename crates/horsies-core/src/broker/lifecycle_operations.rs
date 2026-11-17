//! Lifecycle operations of a client handle on its main core pool, one 0004
//! function each (`crates/horsies-core/baseline/0004_lifecycle_functions.sql`):
//!
//! - [`PostgresBroker::requeue_stale_claimed`]: the stale CLAIMED requeue
//!   pass, `horsies_requeue_stale_claims` in bounded batches.
//! - [`PostgresBroker::release_claims`]: `horsies_release_claims`, the return
//!   of claimed rows that no child started.
//!
//! These are the entry points of the Python client while its own worker
//! still runs the parent loop: the client calls them through `horsies._core`
//! in place of its own statements. The core worker calls the same statement
//! functions on its worker pool.
//!
//! Each one checks the client family once per handle before its first write
//! ([`PostgresBroker::ensure_client_family_for_write`]) and holds a write
//! token, so [`PostgresBroker::close`] waits for it.

use uuid::Uuid;

use crate::broker::claims::{release_claims, ReleaseClaims, ReleaseOutcome};
use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::broker::stale_recovery::{
    requeue_stale_claimed_pass, StaleClaimsRequeueFailed, StaleClaimsRequeuePass, ThresholdMs,
};

impl PostgresBroker {
    /// One stale CLAIMED requeue pass (Python
    /// `PostgresBroker.requeue_stale_claimed`): a CLAIMED row with no
    /// claimer beat newer than `stale_after`, claimed longer ago than
    /// `stale_after`, goes back to PENDING (a workflow task with no runnable
    /// node stays), in batches until a batch is short or the pass reaches
    /// its bound. Each batch commits alone. Sends no queue wake.
    ///
    /// Errors: [`StaleClaimsRequeueFailed`]: the pools could not open, the
    /// client family check refused or could not read (`requeued_before` 0),
    /// or a batch failed after the batches before it committed.
    pub async fn requeue_stale_claimed(
        &self,
        stale_after: ThresholdMs,
    ) -> Result<StaleClaimsRequeuePass, StaleClaimsRequeueFailed> {
        let not_started = |error: BrokerError| StaleClaimsRequeueFailed {
            requeued_before: 0,
            error,
        };
        self.ensure_client_family_for_write()
            .await
            .map_err(not_started)?;
        let (open, _write) = self.core_pools_for_write().map_err(not_started)?;
        requeue_stale_claimed_pass(open.main(), self.statement_log(), stale_after).await
    }

    /// Return claimed rows that no child started to the queue (Python
    /// `horsies.core.worker.release.release_claims`, one call of at most
    /// [`crate::MAX_RELEASED_CLAIMS`] rows). A row that is
    /// CLAIMED by the command's worker at its claim generation goes back to
    /// PENDING with every claim and run field cleared; any other row is not
    /// touched. `ISOLATED` also writes the `ISOLATED` class and wakes each
    /// queue of a released row once. No attempt row is written.
    ///
    /// One statement in autocommit; an empty batch sends nothing and checks
    /// nothing. Returns one outcome per claim, in the batch order.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Statement`] (nothing written,
    /// or `OutcomeUnknown`: the release may have committed; the same
    /// release sent again answers `NotHeld` for each released row);
    /// [`BrokerError::RowDecode`] and [`BrokerError::ClaimContract`] for
    /// rows outside the function's contract.
    pub async fn release_claims(
        &self,
        command: &ReleaseClaims,
    ) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
        if command.batch.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        release_claims(open.main(), self.statement_log(), command).await
    }
}
