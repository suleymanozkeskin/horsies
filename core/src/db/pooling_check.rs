//! The statement-pooling check of every core pool, the session pool
//! included.
//!
//! On the first checkout of a pool in a process, that connection runs
//! `SELECT 1`, then a two-statement transaction sent as separate statements:
//! `BEGIN`, `SELECT 1`, `COMMIT` (4 round trips, once per pool per process).
//!
//! - `SELECT 1` passes and the server refuses the transaction: the URL does
//!   not keep a transaction (PgBouncer statement pooling). The checkout fails
//!   with [`CheckoutFailure::StatementPooledUrl`], which is not retryable.
//! - A connection error is the checkout's ordinary transient failure
//!   ([`CheckoutFailure::PoolingCheckFailed`]); it is never reported as
//!   statement pooling.
//! - A pass is kept for the life of the pool; a failure is not kept, so a
//!   corrected PgBouncer is seen at the next checkout without a restart.
//!
//! The single autocommit statements of the core pass under statement pooling,
//! so without this check a statement-pooled URL would pass the first send and
//! fail later at every `BEGIN`.

use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::error::DbError;
use tokio_postgres::Client;

use super::connect::ConnectPlan;
use super::failure::{CheckoutFailure, DbErrorText, PoolingCheckFailure};
use super::pool::{PoolRole, PooledConnection};

/// The bound of the whole check.
pub const STATEMENT_POOLING_CHECK_BOUND: Duration = Duration::from_secs(10);

const PROBE_SQL: &str = "SELECT 1";

/// Sent one by one: each statement is its own round trip, so a pooler that
/// ends the server assignment at each statement sees the open transaction.
const TRANSACTION_STEPS: [&str; 3] = ["BEGIN", "SELECT 1", "COMMIT"];

/// Proof that the check passed on a pool in this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PoolingCheckPassed;

/// What the check found.
enum PoolingVerdict {
    KeepsTransactions,
    StatementPooled(DbError),
    Failed(PoolingCheckFailure),
}

/// Runs the check on a checked-out connection. Marks the connection to be
/// discarded when the check does not pass.
pub(crate) async fn run_pooling_check(
    connection: &mut PooledConnection,
    role: PoolRole,
    connect: &Arc<ConnectPlan>,
) -> Result<PoolingCheckPassed, CheckoutFailure> {
    let verdict =
        match tokio::time::timeout(STATEMENT_POOLING_CHECK_BOUND, judge(connection.client())).await
        {
            Ok(verdict) => verdict,
            Err(_elapsed) => PoolingVerdict::Failed(PoolingCheckFailure::TimedOut {
                bound: STATEMENT_POOLING_CHECK_BOUND,
            }),
        };
    match verdict {
        PoolingVerdict::KeepsTransactions => Ok(PoolingCheckPassed),
        PoolingVerdict::StatementPooled(server_error) => {
            connection.discard();
            tracing::warn!(
                target: "horsies.db",
                url = %connect.masked_url(),
                server_error = %DbErrorText(&server_error),
                "the {role} pool's URL does not keep a transaction (statement pooling)",
            );
            Err(CheckoutFailure::StatementPooledUrl {
                masked_url: connect.masked_url().clone(),
            })
        }
        PoolingVerdict::Failed(cause) => {
            connection.discard();
            Err(CheckoutFailure::PoolingCheckFailed { role, cause })
        }
    }
}

async fn judge(client: &Client) -> PoolingVerdict {
    if let Err(error) = client.batch_execute(PROBE_SQL).await {
        return PoolingVerdict::Failed(PoolingCheckFailure::SelectFailed(error));
    }
    for step in TRANSACTION_STEPS {
        if let Err(error) = client.batch_execute(step).await {
            return match error.as_db_error().cloned() {
                Some(server_error) => PoolingVerdict::StatementPooled(server_error),
                None => PoolingVerdict::Failed(PoolingCheckFailure::ConnectionLost(error)),
            };
        }
    }
    PoolingVerdict::KeepsTransactions
}
