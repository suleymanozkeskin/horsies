//! Post-terminal rerun as a fresh, lineage-bearing enqueue.

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, query_in, RowDecodeError, StatementLog};
use crate::broker::enqueue_task::{decode_enqueue_outcome, EnqueueKey, EnqueueTaskOutcome};
use crate::broker::pools::PoolSettingsError;
use crate::core::history::archive::attempts::decode_attempt_snapshot;
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::ddl::classes::DEFAULT_RETENTION_CLASS_KEY;
use crate::core::history::enqueue::{prepare_enqueue_facts_with_lineage, EnqueuePreparationError};
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::fingerprint::FingerprintError;
use crate::core::history::identity::keys::{validate_reservation_window, IdempotencyKeyError};
use crate::core::history::identity::uuid7::{mint_task_id, Uuid7Error};
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::history::rerun::input_envelope::{
    decode_framed_rerun_input, InputEnvelopeDecodeError,
};
use crate::core::types::status::TaskStatus;
use crate::db::{ParameterType, Row, StatementFailure, Transaction, TypedStatement};

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
/// check, the detail read and the absence read in one statement. `$1` is
/// the source task id, `$2` the retention class key of the new task. The
/// detail reader keeps ACCESS SHARE on the source leaf until the
/// transaction ends.
static RERUN_SOURCE_READ: TypedStatement = TypedStatement::new(
    "rerun_source_read",
    "SELECT class_registered, location, absence_reason, task_id, task_name, queue_name, \
            priority, max_retries, status, is_workflow_task, rerun_root_task_id, \
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
/// builds it from `input` and `task_options`.
static RERUN_ENQUEUE: TypedStatement = TypedStatement::new(
    "rerun_enqueue",
    "SELECT outcome, task_id FROM horsies_enqueue_task(\
     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'NOW', NULL, NULL, $13, $14, \
     'RERUN', $15, $16, $17, $18, $19, $20::bigint * interval '1 microsecond', 'FIRST')",
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

/// The facts of a history source that the rerun decides on and copies
/// (Python `RerunSource`). The stored status is a [`TaskStatus`] and the
/// attempt snapshot decoded; a source that fails either is corrupt.
struct RerunSource {
    task_id: Uuid,
    task_name: String,
    queue_name: String,
    priority: i32,
    max_retries: i32,
    status: TaskStatus,
    is_workflow_task: bool,
    rerun_root_task_id: Option<Uuid>,
    rerun_input_disposition: String,
    rerun_input_version: Option<i16>,
    rerun_input_digest: Option<Vec<u8>>,
    rerun_input_inline: Option<Vec<u8>>,
}

/// The answer of the source read (Python `RerunSourceRead`).
enum SourceRead {
    /// The new task's retention class has no class row.
    ClassUnregistered,
    Live,
    Absent(TaskAbsenceReason),
    /// A location, absence reason or status outside its set, or an attempt
    /// snapshot that does not decode. The detail names it.
    Corrupt(String),
    History(RerunSource),
}

/// Run one rerun inside the caller's transaction (Python `rerun_task`,
/// `python/horsies/core/history/rerun/operations.py`).
///
/// Statements: the source read (`horsies_rerun_source_read`); for a source
/// with an inline input, one `horsies_enqueue_task` with the lineage `RERUN`,
/// which claims a caller key first. Each outcome but `Enqueued` writes
/// nothing. The caller's commit or rollback decides whether the claim and
/// the row stay.
///
/// Errors: [`RerunError::UnknownRetentionClass`] before any write;
/// [`RerunError::Statement`] (a missing history reader is refused with
/// `42883`); [`RerunError::RowDecode`]; [`RerunError::EnqueuePreparation`],
/// [`RerunError::Identity`]. After an error the caller's transaction may
/// hold the claim; the caller does not commit it.
pub(crate) async fn rerun_task_in_transaction(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
) -> Result<RerunOutcome, RerunError> {
    let source = match read_rerun_source(transaction, log, command.source_task_id, policy).await? {
        SourceRead::ClassUnregistered => {
            return Err(RerunError::UnknownRetentionClass(
                policy.retention_class_key.clone(),
            ));
        }
        SourceRead::Live => {
            return Ok(RerunOutcome::SourceLive {
                task_id: command.source_task_id,
            });
        }
        SourceRead::Absent(reason) => {
            return Ok(RerunOutcome::SourceAbsent {
                task_id: command.source_task_id,
                reason,
            });
        }
        SourceRead::Corrupt(detail) => {
            return Ok(RerunOutcome::SourceCorrupt {
                task_id: command.source_task_id,
                detail,
            });
        }
        SourceRead::History(source) => source,
    };

    if source.is_workflow_task {
        return Ok(RerunOutcome::NotEligible {
            task_id: command.source_task_id,
            reason: NotEligibleReason::WorkflowTask,
        });
    }
    if source.status == TaskStatus::Completed {
        return Ok(RerunOutcome::NotEligible {
            task_id: command.source_task_id,
            reason: NotEligibleReason::CompletedSource,
        });
    }
    if source.rerun_input_disposition != "INLINE" {
        return Ok(RerunOutcome::InputUnavailable {
            task_id: command.source_task_id,
            disposition: source.rerun_input_disposition,
        });
    }
    let (Some(version), Some(payload), Some(digest)) = (
        source.rerun_input_version,
        source.rerun_input_inline.as_deref(),
        source.rerun_input_digest.as_deref(),
    ) else {
        return Ok(RerunOutcome::InputCorrupt {
            task_id: command.source_task_id,
            detail: "inline disposition with an incomplete envelope".to_owned(),
        });
    };
    let framed = match decode_framed_rerun_input(version, payload, digest) {
        Ok(decoded) => decoded,
        Err(InputEnvelopeDecodeError::VersionUnknown(version)) => {
            return Ok(RerunOutcome::InputCorrupt {
                task_id: command.source_task_id,
                detail: format!("unknown input-envelope version {version}"),
            });
        }
        Err(InputEnvelopeDecodeError::Corrupt(detail)) => {
            return Ok(RerunOutcome::InputCorrupt {
                task_id: command.source_task_id,
                detail,
            });
        }
    };

    let new_task_id = mint_task_id()?;
    let root_task_id = source.rerun_root_task_id.unwrap_or(source.task_id);
    // The rerun runs with the execution class of the stored request; a stored
    // class that does not parse is corrupt input.
    let facts = match prepare_enqueue_facts_with_lineage(
        &source.task_name,
        &source.queue_name,
        source.priority,
        &framed.input,
        command.deadline,
        None,
        framed.options.as_deref(),
        Some(policy.retention_class_key()),
        policy.retain_rerun_input,
        command.caller_key(),
        Some(source.task_id),
        Some(root_task_id),
    ) {
        Ok(facts) => facts,
        Err(EnqueuePreparationError::InvalidExecution(invalid)) => {
            return Ok(RerunOutcome::InputCorrupt {
                task_id: command.source_task_id,
                detail: format!("stored task options: {}", invalid.detail),
            });
        }
        Err(other) => return Err(other.into()),
    };

    // The policy was checked at construction (`validate_reservation_window`):
    // at most 30 days, a whole number of microseconds.
    let window_micros = policy
        .reservation_window
        .num_microseconds()
        .expect("a checked window of at most 30 days fits an i64 count of microseconds");
    let key = EnqueueKey::of(facts.idempotency_key_digest, window_micros);
    let (key_kind, key_scope_version, key_window_micros) = key.columns();
    let key_digest = key.digest();
    let sent_at = Utc::now();
    let execution = facts.execution.as_str();
    let fingerprint = facts.command_fingerprint.as_slice();
    let statement = &RERUN_ENQUEUE;
    let rows = query_in(
        transaction,
        log,
        &statement
            .bind()
            .value(&new_task_id)
            .value(&source.task_name)
            .value(&source.queue_name)
            .value(&source.priority)
            .value(&framed.input)
            .value(&framed.options)
            .value(&execution)
            .value(&facts.retention_class_key)
            .value(&facts.retain_rerun_input)
            .value(&source.max_retries)
            .value(&command.deadline)
            .value(&sent_at)
            .value(&facts.command_fingerprint_version)
            .value(&fingerprint)
            .value(&source.task_id)
            .value(&root_task_id)
            .value(&key_kind)
            .value(&key_digest)
            .value(&key_scope_version)
            .value(&key_window_micros)
            .finish(),
    )
    .await?;
    let row = one_row(rows, statement)?;
    let outcome_text: String = column(&row, statement, "outcome")?;
    let answering_task_id: Uuid = column(&row, statement, "task_id")?;
    let written = match decode_enqueue_outcome(&outcome_text, answering_task_id) {
        Ok(written) => written,
        Err(defect) => panic!(
            "statement `{}` returned a row outside the enqueue contract: {defect}",
            statement.name(),
        ),
    };
    Ok(rerun_write_outcome(
        written,
        new_task_id,
        source.task_id,
        root_task_id,
    ))
}

/// The rerun outcome of the write of `new_task_id` (Python
/// `_rerun_write_outcome`). Pure.
///
/// `SAME_COMMAND` is the write of this id that already ran in the caller's
/// transaction: the rerun is enqueued.
///
/// Panics on `PAYLOAD_MISMATCH` and `CONFLICT_UNVERIFIABLE`: `new_task_id`
/// was minted for this write, so no other row can hold it; either answer is
/// a defect of the id mint or of the function.
fn rerun_write_outcome(
    written: EnqueueTaskOutcome,
    new_task_id: Uuid,
    source_task_id: Uuid,
    rerun_root_task_id: Uuid,
) -> RerunOutcome {
    match written {
        EnqueueTaskOutcome::Inserted | EnqueueTaskOutcome::SameCommand => RerunOutcome::Enqueued {
            new_task_id,
            source_task_id,
            rerun_root_task_id,
        },
        EnqueueTaskOutcome::KeyReplay { holder } => RerunOutcome::KeyReplay {
            existing_task_id: holder,
        },
        EnqueueTaskOutcome::KeyConflict { owner } => RerunOutcome::KeyConflict {
            task_id: source_task_id,
            reserved_by_task_id: owner,
        },
        EnqueueTaskOutcome::PayloadMismatch | EnqueueTaskOutcome::ConflictUnverifiable => panic!(
            "the rerun write of the minted task id {new_task_id} answered {written:?}: \
             no other row can hold a minted id"
        ),
    }
}

/// Read the source of a rerun and the class of the new task: one statement
/// in the caller's transaction (Python `read_rerun_source`). Writes nothing.
///
/// Errors: [`RerunError::Statement`]; [`RerunError::RowDecode`] for a row
/// that is not one row of the function's columns.
async fn read_rerun_source(
    transaction: &mut Transaction<'_>,
    log: StatementLog,
    source_task_id: Uuid,
    policy: &RerunEnqueuePolicy,
) -> Result<SourceRead, RerunError> {
    let statement = &RERUN_SOURCE_READ;
    let class_key = policy.retention_class_key();
    let rows = query_in(
        transaction,
        log,
        &statement
            .bind()
            .value(&source_task_id)
            .value(&class_key)
            .finish(),
    )
    .await?;
    let row = one_row(rows, statement)?;
    let class_registered: bool = column(&row, statement, "class_registered")?;
    if !class_registered {
        return Ok(SourceRead::ClassUnregistered);
    }
    let location: String = column(&row, statement, "location")?;
    match location.as_str() {
        "LIVE" => Ok(SourceRead::Live),
        "ABSENT" => {
            let reason: Option<String> = column(&row, statement, "absence_reason")?;
            Ok(absence_of(source_task_id, reason.as_deref()))
        }
        "HISTORY" => history_source(&row, statement),
        other => Ok(SourceRead::Corrupt(
            HistoryError::contract(format!("staged detail returned unknown location {other:?}"))
                .to_string(),
        )),
    }
}

/// The absence of a source, or the corruption of its reason. Pure.
fn absence_of(source_task_id: Uuid, reason: Option<&str>) -> SourceRead {
    let parsed = match reason {
        Some(reason) => TaskAbsenceReason::parse(reason),
        None => Err(HistoryError::contract(format!(
            "the absence read of {source_task_id} returned no reason"
        ))),
    };
    match parsed {
        Ok(reason) => SourceRead::Absent(reason),
        Err(error) => SourceRead::Corrupt(error.to_string()),
    }
}

/// The history source of one read row, or the corruption it carries: the
/// stored status is read as a [`TaskStatus`], then the attempt snapshot is
/// decoded (digest first), in Python's order.
///
/// Errors: [`RerunError::RowDecode`] for a column of a history row that is
/// `NULL` or of another type.
fn history_source(row: &Row, statement: &TypedStatement) -> Result<SourceRead, RerunError> {
    let task_id: Uuid = column(row, statement, "task_id")?;
    let status: String = column(row, statement, "status")?;
    let Ok(status) = status.parse::<TaskStatus>() else {
        return Ok(SourceRead::Corrupt(
            HistoryError::contract(format!(
                "history row {task_id} has status {status:?}, which is not a task status"
            ))
            .to_string(),
        ));
    };
    let attempt_archive_version: i16 = column(row, statement, "attempt_archive_version")?;
    let attempt_snapshot: Vec<u8> = column(row, statement, "attempt_snapshot")?;
    let attempt_snapshot_digest: Vec<u8> = column(row, statement, "attempt_snapshot_digest")?;
    if let Err(error) = decode_attempt_snapshot(
        attempt_archive_version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
        &attempt_snapshot,
        &attempt_snapshot_digest,
    ) {
        return Ok(SourceRead::Corrupt(HistoryError::from(error).to_string()));
    }
    Ok(SourceRead::History(RerunSource {
        task_id,
        task_name: column(row, statement, "task_name")?,
        queue_name: column(row, statement, "queue_name")?,
        priority: column(row, statement, "priority")?,
        max_retries: column(row, statement, "max_retries")?,
        status,
        is_workflow_task: column(row, statement, "is_workflow_task")?,
        rerun_root_task_id: column(row, statement, "rerun_root_task_id")?,
        rerun_input_disposition: column(row, statement, "rerun_input_disposition")?,
        rerun_input_version: column(row, statement, "rerun_input_version")?,
        rerun_input_digest: column(row, statement, "rerun_input_digest")?,
        rerun_input_inline: column(row, statement, "rerun_input_inline")?,
    }))
}

#[cfg(test)]
mod write_outcome_tests {
    use super::*;

    const NEW: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00e1);
    const SOURCE: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00e0);
    const ROOT: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00d0);
    const HOLDER: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00f0);

    /// Python `_rerun_write_outcome`: an insert and a repeat of this write
    /// are the enqueued rerun; the key answers name the task that holds the
    /// key.
    #[test]
    fn each_write_answer_maps_onto_its_rerun_outcome() {
        let enqueued = RerunOutcome::Enqueued {
            new_task_id: NEW,
            source_task_id: SOURCE,
            rerun_root_task_id: ROOT,
        };
        assert_eq!(
            rerun_write_outcome(EnqueueTaskOutcome::Inserted, NEW, SOURCE, ROOT),
            enqueued
        );
        assert_eq!(
            rerun_write_outcome(EnqueueTaskOutcome::SameCommand, NEW, SOURCE, ROOT),
            enqueued
        );
        assert_eq!(
            rerun_write_outcome(
                EnqueueTaskOutcome::KeyReplay { holder: HOLDER },
                NEW,
                SOURCE,
                ROOT
            ),
            RerunOutcome::KeyReplay {
                existing_task_id: HOLDER
            }
        );
        assert_eq!(
            rerun_write_outcome(
                EnqueueTaskOutcome::KeyConflict { owner: HOLDER },
                NEW,
                SOURCE,
                ROOT
            ),
            RerunOutcome::KeyConflict {
                task_id: SOURCE,
                reserved_by_task_id: HOLDER,
            }
        );
    }

    /// A minted id that another row holds is a defect, not an outcome.
    #[test]
    #[should_panic(expected = "no other row can hold a minted id")]
    fn a_payload_mismatch_on_a_minted_id_panics() {
        let _outcome = rerun_write_outcome(EnqueueTaskOutcome::PayloadMismatch, NEW, SOURCE, ROOT);
    }

    #[test]
    #[should_panic(expected = "no other row can hold a minted id")]
    fn a_conflict_that_cannot_be_verified_on_a_minted_id_panics() {
        let _outcome =
            rerun_write_outcome(EnqueueTaskOutcome::ConflictUnverifiable, NEW, SOURCE, ROOT);
    }
}
