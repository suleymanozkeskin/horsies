//! Post-terminal rerun as a fresh, lineage-bearing enqueue.

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::broker::client_family::ClientFamily;
use crate::broker::core_rows::{RowDecodeError, StatementLog};
use crate::broker::pools::PoolSettingsError;
use crate::core::history::ddl::classes::DEFAULT_RETENTION_CLASS_KEY;
use crate::core::history::enqueue::EnqueuePreparationError;
use crate::core::history::identity::fingerprint::FingerprintError;
use crate::core::history::identity::keys::{validate_reservation_window, IdempotencyKeyError};
use crate::core::history::identity::uuid7::Uuid7Error;
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::db::{ParameterType, StatementFailure, Transaction, TypedStatement};

use super::caller::{CallerTransaction, OwnTransactionError};
use super::steps::{
    decode_rerun_write, prepare_rerun_write, rerun_read, RerunPreparation, RerunRowError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerunTask {
    source_task_id: Uuid,
    deadline: Option<DateTime<Utc>>,
    caller_key: Option<String>,
}

impl RerunTask {
    pub fn new(
        source_task_id: Uuid,
        deadline: Option<DateTime<Utc>>,
        caller_key: Option<String>,
    ) -> Self {
        Self {
            source_task_id,
            deadline,
            caller_key,
        }
    }

    pub fn source_task_id(&self) -> Uuid {
        self.source_task_id
    }

    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        self.deadline
    }

    pub fn caller_key(&self) -> Option<&str> {
        self.caller_key.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerunEnqueuePolicy {
    retention_class_key: String,
    retain_rerun_input: bool,
    reservation_window: Duration,
}

impl RerunEnqueuePolicy {
    pub fn new(
        retention_class_key: impl Into<String>,
        retain_rerun_input: bool,
        reservation_window: Duration,
    ) -> Result<Self, RerunError> {
        let retention_class_key = retention_class_key.into();
        if retention_class_key.is_empty() {
            return Err(RerunError::InvalidPolicy(
                "retention class key must be non-empty".to_owned(),
            ));
        }
        validate_reservation_window(reservation_window)?;
        Ok(Self {
            retention_class_key,
            retain_rerun_input,
            reservation_window,
        })
    }

    pub fn standard(retain_rerun_input: bool) -> Self {
        Self {
            retention_class_key: DEFAULT_RETENTION_CLASS_KEY.to_owned(),
            retain_rerun_input,
            reservation_window: Duration::hours(24),
        }
    }

    pub fn retention_class_key(&self) -> &str {
        &self.retention_class_key
    }

    pub fn retain_rerun_input(&self) -> bool {
        self.retain_rerun_input
    }

    pub fn reservation_window(&self) -> Duration {
        self.reservation_window
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotEligibleReason {
    CompletedSource,
    WorkflowTask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerunOutcome {
    Enqueued {
        new_task_id: Uuid,
        source_task_id: Uuid,
        rerun_root_task_id: Uuid,
    },
    SourceLive {
        task_id: Uuid,
    },
    SourceAbsent {
        task_id: Uuid,
        reason: TaskAbsenceReason,
    },
    /// The source history row does not decode: a stored status that is not a
    /// task status, an attempt snapshot that fails its check, or a location
    /// the staged read does not define. Nothing is written; a repeat gives the
    /// same outcome until the stored row or the server changes.
    SourceCorrupt {
        task_id: Uuid,
        detail: String,
    },
    NotEligible {
        task_id: Uuid,
        reason: NotEligibleReason,
    },
    InputUnavailable {
        task_id: Uuid,
        disposition: String,
    },
    InputCorrupt {
        task_id: Uuid,
        detail: String,
    },
    KeyConflict {
        task_id: Uuid,
        reserved_by_task_id: Uuid,
    },
    KeyReplay {
        existing_task_id: Uuid,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RerunError {
    /// A statement of the rerun failed with one of the statement layer's
    /// three outcomes. Nothing committed, except after `OutcomeUnknown` at
    /// `COMMIT`: the rerun may have committed, and the same keyed rerun sent
    /// again answers `KeyReplay` for it. Retryable when the cause may clear
    /// ([`StatementFailure::transience`]).
    #[error("database error: {0}")]
    Statement(#[from] StatementFailure),
    /// A row of the source read or the key claim does not have the shape its
    /// reader requires. Nothing committed. Not retryable.
    #[error("{0}")]
    RowDecode(#[from] RowDecodeError),
    /// A row of a rerun step does not fit its statement's columns, or holds
    /// `NULL` where its function sets a value. Nothing committed. Not
    /// retryable.
    #[error("{0}")]
    CallerRow(#[from] RerunRowError),
    /// A closed broker could not open its pools again. Nothing was read or
    /// written. Not retryable.
    #[error("connection settings refused: {0}")]
    PoolSettings(#[from] PoolSettingsError),
    #[error("unknown retention class {0:?}: register the class before rerunning into it")]
    UnknownRetentionClass(String),
    #[error("invalid rerun policy: {0}")]
    InvalidPolicy(String),
    #[error("rerun enqueue preparation failed: {0}")]
    EnqueuePreparation(#[from] EnqueuePreparationError),
    #[error("rerun fingerprint failed: {0}")]
    Fingerprint(#[from] FingerprintError),
    #[error("rerun idempotency key failed: {0}")]
    Idempotency(#[from] IdempotencyKeyError),
    #[error("rerun identity mint failed: {0}")]
    Identity(#[from] Uuid7Error),
    #[error("rerun JSON serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// The database records another client family, or none. Nothing was
    /// read or written. Not retryable: the database stays of its family.
    #[error(
        "rerun refused: this database was installed by {stored}; this is the {} client",
        own.as_str()
    )]
    ClientFamilyMismatch {
        stored: crate::broker::client_family::RecordedFamily,
        own: crate::broker::client_family::ClientFamily,
    },
    /// The family row holds a value that is not a client family: corrupt
    /// stored data. Nothing was written. Not retryable: the same row is
    /// refused again until it is repaired.
    #[error("rerun refused: {0}")]
    UnknownStoredFamily(crate::broker::client_family::UnknownStoredFamily),
    /// The client-family check before the rerun could not read the family
    /// row. Nothing was read or written. Retryable when
    /// [`FamilyCheckFailure::transience`] is `Transient`: the check keeps no
    /// answer after a failure, so a retry reads the row again.
    ///
    /// [`FamilyCheckFailure::transience`]: crate::broker::client_family::FamilyCheckFailure::transience
    #[error("rerun refused: {0}")]
    FamilyCheckFailed(crate::broker::client_family::FamilyCheckFailure),
}

/// `horsies_rerun_source_read` (`0006_history_functions.sql`): the class
/// check, the recorded client family, the detail read and the absence read
/// in one statement. `$1` is the source task id, `$2` the retention class
/// key of the new task. The detail reader keeps ACCESS SHARE on the source
/// leaf until the transaction ends. The columns are
/// `steps::RERUN_READ_COLUMNS`, in this order.
pub(super) static RERUN_SOURCE_READ: TypedStatement = TypedStatement::new(
    "rerun_source_read",
    "SELECT class_registered, client_family, location, absence_reason, task_id, task_name, \
            queue_name, priority, max_retries, status, is_workflow_task, rerun_root_task_id, \
            attempt_archive_version, attempt_snapshot, attempt_snapshot_digest, \
            rerun_input_disposition, rerun_input_version, rerun_input_digest, \
            rerun_input_inline \
     FROM horsies_rerun_source_read($1, $2)",
    &[ParameterType::Uuid, ParameterType::Text],
);

/// The first statement of a rerun, which names a checkout that failed
/// before it.
pub(crate) fn rerun_first_statement() -> &'static TypedStatement {
    &RERUN_SOURCE_READ
}

/// `horsies_enqueue_task` for a rerun, in the caller's transaction (Python
/// `rerun_task`): start `NOW`, lineage `RERUN`, send kind `FIRST`; a keyed
/// rerun claims its key inside the call. Parameters in order: task id, task
/// name, queue name, priority, input, task options, execution, retention
/// class key, retain rerun input, max retries, good until, sent at,
/// fingerprint version, fingerprint, source task id, root task id, key kind,
/// key digest, key scope version, key window in microseconds (made into the
/// exact `interval`; NULL for an unkeyed rerun). The row carries the
/// source's exact input and options texts and stores no envelope: the move
/// builds it from `input` and `task_options`. The trace is `NO_CARRIER`: the
/// rerun stores no carrier.
pub(super) static RERUN_ENQUEUE: TypedStatement = TypedStatement::new(
    "rerun_enqueue",
    "SELECT outcome, task_id FROM horsies_enqueue_task(\
     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'NOW', NULL, NULL, $13, $14, \
     'RERUN', $15, $16, $17, $18, $19, $20::bigint * interval '1 microsecond', 'FIRST', \
     'NO_CARRIER', NULL, NULL)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Bool,
        ParameterType::Int4,
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::Int2,
        ParameterType::Bytea,
        ParameterType::Uuid,
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Bytea,
        ParameterType::Int2,
        ParameterType::Int8,
    ],
);

/// Run one rerun inside the caller's transaction (Python `rerun_task`,
/// `clients/python/horsies/core/history/rerun/operations.py`): the three steps of
/// `steps` on `transaction`, so the core's own path and a client's
/// transaction run one rerun.
///
/// Statements: the source read (`horsies_rerun_source_read`); for a source
/// with an inline input, one `horsies_enqueue_task` with the lineage `RERUN`,
/// which claims a caller key first. Each outcome but `Enqueued` writes
/// nothing. The caller's commit or rollback decides whether the claim and
/// the row stay. `own` is the family of the calling handle: a database of
/// another family is refused before the write.
///
/// Errors: [`RerunError::UnknownRetentionClass`],
/// [`RerunError::ClientFamilyMismatch`], [`RerunError::UnknownStoredFamily`],
/// [`RerunError::Idempotency`] (the
/// caller key) and [`RerunError::Identity`] before any write;
/// [`RerunError::Statement`] (a missing history reader is refused with
/// `42883`); [`RerunError::RowDecode`] and [`RerunError::CallerRow`]. After
/// an error the caller's transaction may hold the claim; the caller does not
/// commit it.
pub(crate) async fn rerun_task_in_transaction(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
    own: ClientFamily,
) -> Result<RerunOutcome, RerunError> {
    let read = rerun_read(command, policy);
    log.log(read.typed_statement());
    let row = transaction
        .query_one(&read)
        .await
        .map_err(own_transaction_error)?;
    let write = match prepare_rerun_write(&read, &row, command, policy, own, Utc::now())? {
        RerunPreparation::Write(write) => write,
        RerunPreparation::Refused(refusal) => return Ok(refusal.into()),
        RerunPreparation::ClassNotRegistered { class_key } => {
            return Err(RerunError::UnknownRetentionClass(class_key));
        }
        RerunPreparation::ClientFamilyMismatch { stored, own } => {
            return Err(RerunError::ClientFamilyMismatch { stored, own });
        }
        RerunPreparation::UnknownStoredFamily(unknown) => {
            return Err(RerunError::UnknownStoredFamily(unknown));
        }
        RerunPreparation::CallerKeyRefused(refused) => {
            return Err(RerunError::Idempotency(refused));
        }
        RerunPreparation::IdMintRefused(refused) => return Err(RerunError::Identity(refused)),
    };
    log.log(write.statement().typed_statement());
    let row = transaction
        .query_one(write.statement())
        .await
        .map_err(own_transaction_error)?;
    Ok(decode_rerun_write(&write, &row)?.into())
}

/// The rerun error of a statement of the core's own transaction.
fn own_transaction_error(error: OwnTransactionError) -> RerunError {
    match error {
        OwnTransactionError::Statement(failure) => RerunError::Statement(failure),
        OwnTransactionError::RowDecode(error) => RerunError::RowDecode(error),
    }
}
