//! Typed lifecycle for one task-history leaf.

use std::ops::{Deref, DerefMut};

use chrono::{DateTime, Utc};
use tokio_postgres::error::SqlState;

use crate::broker::core_rows::{column, one_row, query_on, StatementLog};
use crate::core::history::commands::{
    DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf, LeafRef,
};
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage::maintenance_connection;
use crate::core::history::outcomes::{LeafDrop, LeafInspection};
use crate::core::history::reads::publisher::publish_readers_in;
use crate::db::{
    CorePool, IsolationLevel, ParameterType, PartitionDdl, PooledConnection, RollbackOutcome,
    SqlIdentifier, StatementBound, StatementFailure, Transaction, TypedStatement,
};

use super::locks::unlock_leaf_in_session;
use super::retirement::{
    begin_leaf_detach, drop_detached_leaf_in, inspect_retention_leaf, record_leaf_detached,
    recorded_detach, BeginDetach, DetachRecord, RecordedDetach, BEGIN_LEAF_DETACH,
    DROP_DETACHED_LEAF, INSPECT_LEAF,
};

const LEAF_DDL_LOCK_TIMEOUT_MS: u64 = 2_000;

/// A session-lock connection is reusable only after explicit cleanup.
/// Cancellation drops this guard before cleanup and discards the connection:
/// its socket closes, and PostgreSQL releases the session advisory lock,
/// instead of a connection that still holds it going back to the pool.
struct SessionConnection {
    inner: PooledConnection,
    reusable: bool,
}

impl SessionConnection {
    fn new(inner: PooledConnection) -> Self {
        Self {
            inner,
            reusable: false,
        }
    }

    fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Deref for SessionConnection {
    type Target = PooledConnection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for SessionConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for SessionConnection {
    fn drop(&mut self) {
        match self.reusable {
            true => {}
            false => self.inner.discard(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetachExpiredLeafOutcome {
    Busy {
        leaf_name: String,
    },
    /// This call detached the leaf, recorded the detach in the catalog and
    /// republished the readers.
    Detached {
        leaf_name: String,
    },
    /// This call did not detach the leaf; the inspection names the state that
    /// kept it. `LeafInspection::Detached` means an earlier call detached it.
    Inspection(LeafInspection),
}

impl DetachExpiredLeafOutcome {
    /// The name of the detach outcome in a pruning pass report, as
    /// [`LeafInspection::outcome_name`]. Python returns its `LeafDetached`
    /// for a leaf this call detached and for a leaf an earlier call detached,
    /// so both are `LeafDetached`.
    pub fn outcome_name(&self) -> &'static str {
        match self {
            Self::Busy { .. } => "LeafMaintenanceBusy",
            Self::Detached { .. } => "LeafDetached",
            Self::Inspection(inspection) => inspection.outcome_name(),
        }
    }

    /// The leaf has no parent after the call: this call detached it, or an
    /// earlier call did (Python `isinstance(detach, LeafDetached)`).
    pub fn left_detached(&self) -> bool {
        match self {
            Self::Detached { .. } | Self::Inspection(LeafInspection::Detached { .. }) => true,
            Self::Busy { .. } | Self::Inspection(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalizeInterruptedLeafOutcome {
    Busy { leaf_name: String },
    Inspection(LeafInspection),
}

impl FinalizeInterruptedLeafOutcome {
    /// The name of the finalize outcome in a pruning pass report, as
    /// [`LeafInspection::outcome_name`].
    pub fn outcome_name(&self) -> &'static str {
        match self {
            Self::Busy { .. } => "LeafMaintenanceBusy",
            Self::Inspection(inspection) => inspection.outcome_name(),
        }
    }
}

/// Detach one expired PERIOD or HEARTBEAT leaf without waiting for its
/// advisory lock.
///
/// `horsies_begin_leaf_detach` classifies the leaf and takes its session
/// lock; `DETACH PARTITION ... CONCURRENTLY` ([`PartitionDdl`]) runs under the
/// command's statement timeout and a 2 s lock timeout;
/// `horsies_record_leaf_detached` records it and, when the published manifest
/// names the leaf, the readers are published in the same transaction
/// (`horsies_publish_history_readers`). Any state other than `DETACHABLE` is
/// returned as its inspection and nothing is detached.
/// `LeafInspection::Detached` means the leaf has no parent (an earlier call,
/// or a detach out of band): `horsies_begin_leaf_detach` then records the
/// detach and, when the published manifest names the leaf, publishes the
/// readers in its own statement, so the drop that follows is not refused for
/// the manifest.
///
/// Runs on one connection of `pool`, checked out as reserved work. `pool`
/// must preserve PostgreSQL session affinity (a direct or session URL):
/// PgBouncer transaction pooling does not keep the session advisory lock.
///
/// Errors: `HistoryError::Statement`, `HistoryError::RowDecode`,
/// `HistoryError::Contract` and `HistoryError::LeafLockNotHeld`. After an
/// error, or a cancel, whose lock state is not known, the connection is
/// closed, so PostgreSQL releases the session lock.
pub(crate) async fn detach_expired_leaf(
    pool: &CorePool,
    log: StatementLog,
    command: &DetachExpiredHistoryLeaf,
) -> Result<DetachExpiredLeafOutcome, HistoryError> {
    let leaf = command.leaf();
    let mut connection =
        SessionConnection::new(maintenance_connection(pool, &BEGIN_LEAF_DETACH).await?);
    let detach = detach_retention_leaf(
        &mut connection,
        log,
        leaf,
        DetachStep::Concurrently,
        command.statement_timeout_ms(),
    )
    .await?;
    Ok(match detach {
        RetentionDetach::Busy => DetachExpiredLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        },
        RetentionDetach::Recorded { .. } => DetachExpiredLeafOutcome::Detached {
            leaf_name: leaf.leaf_name().to_owned(),
        },
        RetentionDetach::Inspection(inspection) => DetachExpiredLeafOutcome::Inspection(inspection),
    })
}

/// Finalize a concurrent detach that a crash or a timeout left pending,
/// without waiting for its advisory lock.
///
/// Acts on `DETACH_INTERRUPTED` only: `DETACH PARTITION ... FINALIZE`, then
/// the record (and the publication) as in [`detach_expired_leaf`]. A leaf
/// found already detached is returned as `LeafInspection::Detached`:
/// `horsies_begin_leaf_detach` records its detach, and this function sends no
/// record of its own. The result is `LeafInspection::Detached` once the
/// detach is recorded; every other state is returned unchanged.
///
/// Connection and errors as [`detach_expired_leaf`].
pub(crate) async fn finalize_interrupted_detach(
    pool: &CorePool,
    log: StatementLog,
    command: &FinalizeInterruptedLeafDetach,
) -> Result<FinalizeInterruptedLeafOutcome, HistoryError> {
    let leaf = command.leaf();
    let mut connection =
        SessionConnection::new(maintenance_connection(pool, &BEGIN_LEAF_DETACH).await?);
    let finalize = detach_retention_leaf(
        &mut connection,
        log,
        leaf,
        DetachStep::Finalize,
        command.statement_timeout_ms(),
    )
    .await?;
    Ok(match finalize {
        RetentionDetach::Busy => FinalizeInterruptedLeafOutcome::Busy {
            leaf_name: leaf.leaf_name().to_owned(),
        },
        RetentionDetach::Recorded { eligible_at } => {
            FinalizeInterruptedLeafOutcome::Inspection(LeafInspection::Detached {
                leaf_name: leaf.leaf_name().to_owned(),
                eligible_at,
            })
        }
        RetentionDetach::Inspection(inspection) => {
            FinalizeInterruptedLeafOutcome::Inspection(inspection)
        }
    })
}

/// The state of one PERIOD or HEARTBEAT leaf on its own connection of `pool`
/// (`horsies_inspect_history_leaf`; Python `inspect_leaf`). Reads only.
///
/// Errors: as `retirement::inspect_retention_leaf`; a failed checkout is the
/// statement's `NotSent`.
pub(crate) async fn inspect_leaf_in_pool(
    pool: &CorePool,
    log: StatementLog,
    command: &InspectHistoryLeaf,
) -> Result<LeafInspection, HistoryError> {
    let mut connection = maintenance_connection(pool, &INSPECT_LEAF).await?;
    inspect_retention_leaf(&mut connection, log, command.leaf()).await
}

/// Drop one already-detached PERIOD or HEARTBEAT leaf in its own transaction
/// on a connection of `pool`: one call of `horsies_drop_detached_leaf`, which
/// reads the published manifest itself, then `COMMIT`. Refused while the
/// published readers still probe the relation. The catalog keeps the dropped
/// leaf's row.
///
/// Errors: `HistoryError::Statement` (nothing committed, or the `COMMIT`
/// reply was lost: `OutcomeUnknown`), `HistoryError::RowDecode` and
/// `HistoryError::Contract` (rolled back, nothing committed).
pub(crate) async fn drop_detached_leaf_in_pool(
    pool: &CorePool,
    log: StatementLog,
    command: &DropDetachedHistoryLeaf,
) -> Result<LeafDrop, HistoryError> {
    let mut connection = maintenance_connection(pool, &DROP_DETACHED_LEAF).await?;
    let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
    let dropped = drop_detached_leaf_in(&mut transaction, log, command.leaf()).await;
    match dropped {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(error) => {
            // The first error is the one to report; a failed ROLLBACK
            // discards the connection, and nothing of the transaction
            // commits either way.
            let _rolled_back: RollbackOutcome = transaction.rollback().await;
            Err(error)
        }
    }
}

/// The detach statement one call runs, and the state it acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetachStep {
    /// `DETACH PARTITION ... CONCURRENTLY`; acts on `DETACHABLE`.
    Concurrently,
    /// `DETACH PARTITION ... FINALIZE`; acts on `DETACH_INTERRUPTED`.
    Finalize,
}

/// What one detach or finalize call ended with.
enum RetentionDetach {
    /// Another session holds the leaf lock, or the detach statement met a
    /// held relation lock.
    Busy,
    /// The detach is recorded in the catalog (by this call or before it).
    Recorded { eligible_at: DateTime<Utc> },
    /// The call did not detach the leaf; the inspection names its state.
    Inspection(LeafInspection),
}

/// One detach or finalize of a PERIOD or HEARTBEAT leaf on one session
/// connection.
///
/// The connection is marked reusable only when it holds no leaf lock and its
/// session timeouts are the ones it had before the call. A cancel, or a
/// failure after which that is not known, leaves it unmarked: it is
/// discarded, and PostgreSQL releases the session lock when the socket
/// closes.
async fn detach_retention_leaf(
    connection: &mut SessionConnection,
    log: StatementLog,
    leaf: &LeafRef,
    step: DetachStep,
    statement_timeout_ms: Option<u64>,
) -> Result<RetentionDetach, HistoryError> {
    let (parent_name, inspection) = match begin_leaf_detach(connection, log, leaf).await? {
        BeginDetach::Locked {
            parent_name,
            inspection,
        } => (parent_name, inspection),
        BeginDetach::Busy => {
            connection.mark_reusable();
            return Ok(RetentionDetach::Busy);
        }
        // `horsies_begin_leaf_detach` recorded the detach of a leaf it found
        // already detached; that state is returned as it is.
        BeginDetach::Unlocked(inspection) => {
            connection.mark_reusable();
            return Ok(RetentionDetach::Inspection(inspection));
        }
    };
    // The session holds the leaf lock from here on.
    let eligible_at = match (&inspection, step) {
        (LeafInspection::Detachable { eligible_at, .. }, DetachStep::Concurrently)
        | (LeafInspection::DetachInterrupted { eligible_at, .. }, DetachStep::Finalize) => {
            *eligible_at
        }
        _ => {
            let released = restore_timeouts_and_unlock(connection, log, leaf, None).await;
            return release_outcome(
                connection,
                Ok(RetentionDetach::Inspection(inspection)),
                released,
            );
        }
    };
    let prior = match read_prior_timeouts(connection, log, statement_timeout_ms).await {
        Ok(prior) => prior,
        Err(error) => {
            let released = restore_timeouts_and_unlock(connection, log, leaf, None).await;
            return release_outcome(connection, Err(error), released);
        }
    };
    let outcome = match run_detach_statement(
        connection,
        log,
        leaf,
        &parent_name,
        step,
        statement_timeout_ms,
    )
    .await
    {
        Ok(PhysicalDetach::Busy) => Ok(RetentionDetach::Busy),
        Ok(PhysicalDetach::Detached) => record_detach(connection, log, leaf, eligible_at).await,
        Err(error) => Err(error),
    };
    let released = restore_timeouts_and_unlock(connection, log, leaf, Some(&prior)).await;
    release_outcome(connection, outcome, released)
}

/// The outcome of a call that held the leaf lock, after the release. The
/// connection is marked reusable only when the release succeeded; the first
/// error wins.
fn release_outcome(
    connection: &mut SessionConnection,
    outcome: Result<RetentionDetach, HistoryError>,
    released: Result<(), HistoryError>,
) -> Result<RetentionDetach, HistoryError> {
    match released {
        Ok(()) => {
            connection.mark_reusable();
            outcome
        }
        Err(release_error) => match outcome {
            Err(error) => Err(error),
            Ok(_) => Err(release_error),
        },
    }
}

enum PhysicalDetach {
    Detached,
    Busy,
}

/// The detach statement of `step` for `leaf` under `parent_name`. Pure.
///
/// Errors: `HistoryError::Contract` when a name is not an unquoted SQL
/// identifier.
fn detach_ddl(
    leaf: &LeafRef,
    parent_name: &str,
    step: DetachStep,
) -> Result<PartitionDdl, HistoryError> {
    let parent = SqlIdentifier::parse(parent_name).map_err(|error| {
        HistoryError::contract(format!("leaf {}: parent name: {error}", leaf.leaf_name()))
    })?;
    let leaf_name = SqlIdentifier::parse(leaf.leaf_name())
        .map_err(|error| HistoryError::contract(format!("leaf name: {error}")))?;
    Ok(match step {
        DetachStep::Concurrently => PartitionDdl::DetachConcurrently {
            parent,
            leaf: leaf_name,
        },
        DetachStep::Finalize => PartitionDdl::DetachFinalize {
            parent,
            leaf: leaf_name,
        },
    })
}

/// Whether `failure` is the server's refusal with `code`.
fn refused_with(failure: &StatementFailure, code: &SqlState) -> bool {
    match failure {
        StatementFailure::StatementRefused { cause, .. } => cause.server_error().code() == code,
        StatementFailure::NotSent { .. } | StatementFailure::OutcomeUnknown { .. } => false,
    }
}

/// `ALTER TABLE <parent> DETACH PARTITION <leaf> CONCURRENTLY | FINALIZE`
/// under the statement timeout and the DDL lock timeout, in autocommit. The
/// caller holds the leaf's session lock and restores the timeouts. A held
/// relation lock (`55P03`) is `Busy`.
async fn run_detach_statement(
    connection: &mut PooledConnection,
    log: StatementLog,
    leaf: &LeafRef,
    parent_name: &str,
    step: DetachStep,
    statement_timeout_ms: Option<u64>,
) -> Result<PhysicalDetach, HistoryError> {
    let ddl = detach_ddl(leaf, parent_name, step)?;
    set_ddl_timeouts(connection, log, statement_timeout_ms).await?;
    log.log_ddl(&ddl);
    match connection
        .run_partition_ddl(&ddl, StatementBound::ConnectionTimeouts)
        .await
    {
        Ok(()) => Ok(PhysicalDetach::Detached),
        Err(failure) if refused_with(&failure, &SqlState::LOCK_NOT_AVAILABLE) => {
            Ok(PhysicalDetach::Busy)
        }
        Err(failure) => Err(HistoryError::Statement(failure)),
    }
}

/// Record the detach that this connection's detach statement completed, in
/// one READ COMMITTED transaction on the connection:
/// `horsies_record_leaf_detached` and, when the published manifest names the
/// leaf, the reader publication commit together. A failure rolls both back.
///
/// A relation that vanished after its detach is `LeafInspection::Missing`. A
/// leaf still attached, or a catalog row that vanished, contradicts the
/// detach and is `HistoryError::Contract`.
async fn record_detach(
    connection: &mut PooledConnection,
    log: StatementLog,
    leaf: &LeafRef,
    eligible_at: DateTime<Utc>,
) -> Result<RetentionDetach, HistoryError> {
    let mut transaction = connection.begin(IsolationLevel::ReadCommitted).await?;
    let recorded = match record_and_publish(&mut transaction, log, leaf.leaf_name()).await {
        Ok(recorded) => recorded,
        Err(error) => {
            // The first error is the one to report. A rollback that fails
            // discards the connection, so the release that follows is not
            // sent and the connection is closed.
            let _rolled_back: RollbackOutcome = transaction.rollback().await;
            return Err(error);
        }
    };
    transaction.commit().await?;
    match recorded_detach(leaf.leaf_name(), recorded)? {
        RecordedDetach::Detached => Ok(RetentionDetach::Recorded { eligible_at }),
        RecordedDetach::Missing => Ok(RetentionDetach::Inspection(LeafInspection::Missing {
            leaf_name: leaf.leaf_name().to_owned(),
            cataloged: true,
            eligible_at: Some(eligible_at),
        })),
    }
}

/// The record statement and, when it says so, the publication, inside the
/// caller's transaction. It does not commit.
async fn record_and_publish(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    leaf_name: &str,
) -> Result<DetachRecord, HistoryError> {
    let recorded = record_leaf_detached(transaction, log, leaf_name).await?;
    match recorded.manifest_names_leaf {
        true => {
            let _republished = publish_readers_in(transaction, log).await?;
        }
        false => {}
    }
    Ok(recorded.record)
}

/// `set_config('statement_timeout', $1, false)`: the session statement
/// timeout, as text (`5000ms`, or a value read back from the session).
static SET_STATEMENT_TIMEOUT: TypedStatement = TypedStatement::new(
    "set_session_statement_timeout",
    "SELECT set_config('statement_timeout', $1, false)",
    &[ParameterType::Text],
);

/// `set_config('lock_timeout', $1, false)`: the session lock timeout, as text.
static SET_LOCK_TIMEOUT: TypedStatement = TypedStatement::new(
    "set_session_lock_timeout",
    "SELECT set_config('lock_timeout', $1, false)",
    &[ParameterType::Text],
);

/// The two session timeouts, as `SHOW` renders them.
static READ_SESSION_TIMEOUTS: TypedStatement = TypedStatement::new(
    "read_session_timeouts",
    "SELECT current_setting('statement_timeout') AS statement_timeout, \
     current_setting('lock_timeout') AS lock_timeout",
    &[],
);

/// Sets one session setting through `statement` (one of the two `set_config`
/// statements above).
async fn set_session_setting(
    connection: &mut PooledConnection,
    log: StatementLog,
    statement: &TypedStatement,
    value: &str,
) -> Result<(), HistoryError> {
    let _set = query_on(connection, log, &statement.bind().value(&value).finish()).await?;
    Ok(())
}

async fn set_ddl_timeouts(
    connection: &mut PooledConnection,
    log: StatementLog,
    timeout_ms: Option<u64>,
) -> Result<(), HistoryError> {
    match timeout_ms {
        Some(timeout_ms) => {
            set_session_setting(
                connection,
                log,
                &SET_STATEMENT_TIMEOUT,
                &format!("{timeout_ms}ms"),
            )
            .await?;
        }
        None => {}
    }
    set_session_setting(
        connection,
        log,
        &SET_LOCK_TIMEOUT,
        &format!("{LEAF_DDL_LOCK_TIMEOUT_MS}ms"),
    )
    .await
}

struct PriorTimeouts {
    statement: Option<String>,
    lock: String,
}

/// The session timeouts before the detach statement: one statement. The
/// statement timeout is kept only when the call changes it (`timeout_ms`).
async fn read_prior_timeouts(
    connection: &mut PooledConnection,
    log: StatementLog,
    timeout_ms: Option<u64>,
) -> Result<PriorTimeouts, HistoryError> {
    let statement = &READ_SESSION_TIMEOUTS;
    let rows = query_on(connection, log, &statement.bind().finish()).await?;
    let row = one_row(rows, statement)?;
    let statement_timeout: String = column(&row, statement, "statement_timeout")?;
    let lock: String = column(&row, statement, "lock_timeout")?;
    Ok(PriorTimeouts {
        statement: timeout_ms.map(|_| statement_timeout),
        lock,
    })
}

/// Restore the session timeouts read before the detach statement (`None`
/// when they were not changed) and release the leaf's session lock. Every
/// step runs; the first error is returned.
async fn restore_timeouts_and_unlock(
    connection: &mut PooledConnection,
    log: StatementLog,
    leaf: &LeafRef,
    prior: Option<&PriorTimeouts>,
) -> Result<(), HistoryError> {
    let restore_statement = match prior.and_then(|prior| prior.statement.as_deref()) {
        Some(prior_timeout) => {
            set_session_setting(connection, log, &SET_STATEMENT_TIMEOUT, prior_timeout).await
        }
        None => Ok(()),
    };
    let restore_lock = match prior {
        Some(prior) => set_session_setting(connection, log, &SET_LOCK_TIMEOUT, &prior.lock).await,
        None => Ok(()),
    };
    let unlock =
        unlock_leaf_in_session(connection, log, leaf.class_key(), leaf.bounds().lower()).await;
    restore_statement.and(restore_lock).and(unlock)
}
