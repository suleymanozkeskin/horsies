//! The rerun as three pure steps for a transaction that the caller owns
//! (Python `rerun_task(connection, command, policy)`). The core builds each
//! statement and decides the outcome; the caller's connection runs the
//! statements, so the rerun commits or rolls back with the caller's own
//! writes. No step does I/O or starts a runtime.
//!
//! 1. [`rerun_read`]: the source read (`horsies_rerun_source_read`), with
//!    the class check and the recorded client family.
//! 2. [`prepare_rerun_write`]: from the read's row, the write
//!    (`horsies_enqueue_task` with the lineage `RERUN`), or the answer that
//!    ends the rerun with nothing written.
//! 3. [`decode_rerun_write`]: the outcome of the write's row.
//!
//! The core's own path (`operations::rerun_task_in_transaction`) runs the
//! same three steps on its transaction, so every client has one rerun.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::client_family::{recorded_family_from, ClientFamily, RecordedFamily};
use crate::broker::enqueue_task::{decode_enqueue_outcome, EnqueueKey, EnqueueTaskOutcome};
use crate::core::history::archive::attempts::decode_attempt_snapshot;
use crate::core::history::archive::versions::{JSON_CONTENT_TYPE, JSON_UTF8_CODEC};
use crate::core::history::enqueue::{prepare_enqueue_facts_with_lineage, EnqueuePreparationError};
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::keys::IdempotencyKeyError;
use crate::core::history::identity::uuid7::{mint_task_id, Uuid7Error};
use crate::core::history::reads::detail::TaskAbsenceReason;
use crate::core::python_text::python_repr;
use crate::core::types::status::TaskStatus;

use super::caller::{
    CallerColumn, CallerRow, CallerRowRefused, CallerStatement, CallerType, StatementValue,
};
use super::input_envelope::{decode_framed_rerun_input, InputEnvelopeDecodeError};
use super::operations::{
    NotEligibleReason, RerunEnqueuePolicy, RerunOutcome, RerunTask, RERUN_ENQUEUE,
    RERUN_SOURCE_READ,
};

/// The result columns of the source read, in the order of its statement.
pub(super) static RERUN_READ_COLUMNS: [CallerColumn; 19] = [
    column("class_registered", CallerType::Bool),
    column("client_family", CallerType::Text),
    column("location", CallerType::Text),
    column("absence_reason", CallerType::Text),
    column("task_id", CallerType::Uuid),
    column("task_name", CallerType::Text),
    column("queue_name", CallerType::Text),
    column("priority", CallerType::Int4),
    column("max_retries", CallerType::Int4),
    column("status", CallerType::Text),
    column("is_workflow_task", CallerType::Bool),
    column("rerun_root_task_id", CallerType::Uuid),
    column("attempt_archive_version", CallerType::Int2),
    column("attempt_snapshot", CallerType::Bytea),
    column("attempt_snapshot_digest", CallerType::Bytea),
    column("rerun_input_disposition", CallerType::Text),
    column("rerun_input_version", CallerType::Int2),
    column("rerun_input_digest", CallerType::Bytea),
    column("rerun_input_inline", CallerType::Bytea),
];

/// The result columns of the write.
pub(super) static RERUN_WRITE_COLUMNS: [CallerColumn; 2] = [
    column("outcome", CallerType::Text),
    column("task_id", CallerType::Uuid),
];

const fn column(name: &'static str, value_type: CallerType) -> CallerColumn {
    CallerColumn { name, value_type }
}

/// The answer of the source read that ends a rerun with nothing written
/// (Python's outcome classes). `task_id` is the source task id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerunRefusal {
    /// The source is live; its own lifecycle owns its retry.
    SourceLive { task_id: Uuid },
    /// No live or history record has the source id.
    SourceAbsent {
        task_id: Uuid,
        reason: TaskAbsenceReason,
    },
    /// The source history row does not decode (a location, absence reason
    /// or status outside its set, or an attempt snapshot that fails its
    /// check). `detail` names it.
    SourceCorrupt { task_id: Uuid, detail: String },
    NotEligible {
        task_id: Uuid,
        reason: NotEligibleReason,
    },
    /// The source stores no input envelope; `disposition` is its stored
    /// reason.
    InputUnavailable { task_id: Uuid, disposition: String },
    /// The stored envelope, its input or its options fail their checks.
    InputCorrupt { task_id: Uuid, detail: String },
}

impl From<RerunRefusal> for RerunOutcome {
    fn from(refusal: RerunRefusal) -> Self {
        match refusal {
            RerunRefusal::SourceLive { task_id } => RerunOutcome::SourceLive { task_id },
            RerunRefusal::SourceAbsent { task_id, reason } => {
                RerunOutcome::SourceAbsent { task_id, reason }
            }
            RerunRefusal::SourceCorrupt { task_id, detail } => {
                RerunOutcome::SourceCorrupt { task_id, detail }
            }
            RerunRefusal::NotEligible { task_id, reason } => {
                RerunOutcome::NotEligible { task_id, reason }
            }
            RerunRefusal::InputUnavailable {
                task_id,
                disposition,
            } => RerunOutcome::InputUnavailable {
                task_id,
                disposition,
            },
            RerunRefusal::InputCorrupt { task_id, detail } => {
                RerunOutcome::InputCorrupt { task_id, detail }
            }
        }
    }
}

/// The write of one rerun: the statement for the caller's transaction and
/// the ids that its outcome names. Frozen once built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerunWrite {
    statement: CallerStatement,
    new_task_id: Uuid,
    source_task_id: Uuid,
    rerun_root_task_id: Uuid,
}

impl RerunWrite {
    /// `horsies_enqueue_task` with the lineage `RERUN`, start `NOW`, send
    /// kind `FIRST`.
    pub fn statement(&self) -> &CallerStatement {
        &self.statement
    }

    /// The task id minted for the new task.
    pub fn new_task_id(&self) -> Uuid {
        self.new_task_id
    }

    pub fn source_task_id(&self) -> Uuid {
        self.source_task_id
    }

    pub fn rerun_root_task_id(&self) -> Uuid {
        self.rerun_root_task_id
    }
}

/// What the read's row decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerunPreparation {
    /// Run this write, then decode its row with [`decode_rerun_write`].
    Write(RerunWrite),
    /// The rerun ends with this answer; nothing is written.
    Refused(RerunRefusal),
    /// The new task's retention class has no class row: a precondition of
    /// the call (Python raises `ValueError`). Nothing is written.
    ClassNotRegistered { class_key: String },
    /// The database records another client family, or none: a precondition
    /// of the call. Nothing is written.
    ClientFamilyMismatch {
        stored: RecordedFamily,
        own: ClientFamily,
    },
    /// The caller key is not a valid idempotency key (Python raises
    /// `ValueError`). Nothing is written.
    CallerKeyRefused(IdempotencyKeyError),
    /// The task id source refused to mint (the millisecond clock is outside
    /// the 48-bit range; Python raises `ValueError`). Nothing is written.
    IdMintRefused(Uuid7Error),
}

/// The outcome of a rerun write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerunWriteOutcome {
    /// The new task exists (this write, or this write already run in the
    /// caller's transaction).
    Enqueued {
        new_task_id: Uuid,
        source_task_id: Uuid,
        rerun_root_task_id: Uuid,
    },
    /// The caller key holds this rerun command already, as `existing_task_id`.
    KeyReplay { existing_task_id: Uuid },
    /// The caller key holds another command (the source's own included).
    KeyConflict {
        task_id: Uuid,
        reserved_by_task_id: Uuid,
    },
}

impl From<RerunWriteOutcome> for RerunOutcome {
    fn from(outcome: RerunWriteOutcome) -> Self {
        match outcome {
            RerunWriteOutcome::Enqueued {
                new_task_id,
                source_task_id,
                rerun_root_task_id,
            } => RerunOutcome::Enqueued {
                new_task_id,
                source_task_id,
                rerun_root_task_id,
            },
            RerunWriteOutcome::KeyReplay { existing_task_id } => {
                RerunOutcome::KeyReplay { existing_task_id }
            }
            RerunWriteOutcome::KeyConflict {
                task_id,
                reserved_by_task_id,
            } => RerunOutcome::KeyConflict {
                task_id,
                reserved_by_task_id,
            },
        }
    }
}

/// A row that does not fit its statement: a defect of the adapter that read
/// it, or a row outside the function's contract. The same row meets it
/// again, so it is not retryable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RerunRowError {
    #[error(transparent)]
    Row(#[from] CallerRowRefused),
    #[error("statement `{statement}` returned NULL in column `{column}`, which its function sets")]
    NullColumn {
        statement: &'static str,
        column: &'static str,
    },
}

/// The source read of a rerun as a caller statement: `$1` the source task
/// id, `$2` the retention class key of the new task (Python
/// `_RERUN_SOURCE_SQL`). Reads only; the detail reader keeps ACCESS SHARE
/// on the source leaf until the caller's transaction ends. Pure.
pub fn rerun_read(command: &RerunTask, policy: &RerunEnqueuePolicy) -> CallerStatement {
    CallerStatement::new(
        &RERUN_SOURCE_READ,
        vec![
            StatementValue::Uuid(command.source_task_id()),
            StatementValue::Text(policy.retention_class_key().to_owned()),
        ],
        &RERUN_READ_COLUMNS,
    )
    .expect("the source read declares a uuid and a text parameter")
}

/// The facts of a history source that the rerun decides on and copies
/// (Python `RerunSource`).
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

/// What the read's row says before the eligibility ladder.
enum SourceRead {
    Refused(RerunRefusal),
    History(RerunSource),
}

/// Decide the rerun from the row of `read` (Python `rerun_task` after its
/// read): the client family (refused unless it is `own`), the class check,
/// the location, the eligibility (workflow task, completed source), the
/// input envelope, the stored options; then the write with a minted task
/// id, sent at `sent_at`. Pure apart from the id mint.
///
/// Errors: [`RerunRowError`] when the row does not fit the read's columns,
/// or holds `NULL` where the function always sets a value.
pub fn prepare_rerun_write(
    read: &CallerStatement,
    row: &CallerRow,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
    own: ClientFamily,
    sent_at: DateTime<Utc>,
) -> Result<RerunPreparation, RerunRowError> {
    let stored = recorded_family_from(text(row, read, "client_family")?);
    match stored {
        RecordedFamily::Recorded(family) if family == own => {}
        RecordedFamily::Recorded(_) | RecordedFamily::NotRecorded => {
            return Ok(RerunPreparation::ClientFamilyMismatch { stored, own });
        }
    }
    match required(
        boolean(row, read, "class_registered")?,
        read,
        "class_registered",
    )? {
        true => {}
        false => {
            return Ok(RerunPreparation::ClassNotRegistered {
                class_key: policy.retention_class_key().to_owned(),
            });
        }
    }
    let source = match source_read(read, row, command.source_task_id())? {
        SourceRead::Refused(refusal) => return Ok(RerunPreparation::Refused(refusal)),
        SourceRead::History(source) => source,
    };
    Ok(prepare_write(source, command, policy, sent_at))
}

/// The location of the source and, for a history source, its facts.
fn source_read(
    read: &CallerStatement,
    row: &CallerRow,
    source_task_id: Uuid,
) -> Result<SourceRead, RerunRowError> {
    let location = required(text(row, read, "location")?, read, "location")?;
    match location.as_str() {
        "LIVE" => Ok(SourceRead::Refused(RerunRefusal::SourceLive {
            task_id: source_task_id,
        })),
        "ABSENT" => Ok(SourceRead::Refused(absence_of(
            source_task_id,
            text(row, read, "absence_reason")?.as_deref(),
        ))),
        "HISTORY" => history_source(read, row, source_task_id),
        other => Ok(SourceRead::Refused(RerunRefusal::SourceCorrupt {
            task_id: source_task_id,
            detail: format!(
                "staged detail returned unknown location {}",
                python_repr(other)
            ),
        })),
    }
}

/// The absence of a source, or the corruption of its reason. Pure.
fn absence_of(source_task_id: Uuid, reason: Option<&str>) -> RerunRefusal {
    match reason.map(|text| (text, TaskAbsenceReason::parse(text))) {
        Some((_, Ok(reason))) => RerunRefusal::SourceAbsent {
            task_id: source_task_id,
            reason,
        },
        Some((text, Err(_))) => RerunRefusal::SourceCorrupt {
            task_id: source_task_id,
            detail: absence_reason_refused(source_task_id, &python_repr(text)),
        },
        None => RerunRefusal::SourceCorrupt {
            task_id: source_task_id,
            detail: absence_reason_refused(source_task_id, "None"),
        },
    }
}

/// Python's text of a stored absence reason that is not a member
/// (`absence_reason_of`), with the stored value as Python shows it.
fn absence_reason_refused(source_task_id: Uuid, shown: &str) -> String {
    format!("absence reason of {source_task_id} is {shown}, which is not a task absence reason")
}

/// The history source of one read row, or the corruption it carries: the
/// stored status is read as a [`TaskStatus`], then the attempt snapshot is
/// decoded (digest first), in Python's order.
fn history_source(
    read: &CallerStatement,
    row: &CallerRow,
    source_task_id: Uuid,
) -> Result<SourceRead, RerunRowError> {
    let task_id = required(uuid(row, read, "task_id")?, read, "task_id")?;
    let status = required(text(row, read, "status")?, read, "status")?;
    let Ok(status) = status.parse::<TaskStatus>() else {
        return Ok(SourceRead::Refused(RerunRefusal::SourceCorrupt {
            task_id: source_task_id,
            detail: format!(
                "history row {task_id} has status {}, which is not a task status",
                python_repr(&status)
            ),
        }));
    };
    let attempt_archive_version = required(
        int2(row, read, "attempt_archive_version")?,
        read,
        "attempt_archive_version",
    )?;
    let attempt_snapshot = required(
        bytea(row, read, "attempt_snapshot")?,
        read,
        "attempt_snapshot",
    )?;
    let attempt_snapshot_digest = required(
        bytea(row, read, "attempt_snapshot_digest")?,
        read,
        "attempt_snapshot_digest",
    )?;
    match decode_attempt_snapshot(
        attempt_archive_version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
        &attempt_snapshot,
        &attempt_snapshot_digest,
    ) {
        Ok(_) => {}
        Err(error) => {
            return Ok(SourceRead::Refused(RerunRefusal::SourceCorrupt {
                task_id: source_task_id,
                detail: HistoryError::from(error).to_string(),
            }));
        }
    }
    Ok(SourceRead::History(RerunSource {
        task_id,
        task_name: required(text(row, read, "task_name")?, read, "task_name")?,
        queue_name: required(text(row, read, "queue_name")?, read, "queue_name")?,
        priority: required(int4(row, read, "priority")?, read, "priority")?,
        max_retries: required(int4(row, read, "max_retries")?, read, "max_retries")?,
        status,
        is_workflow_task: required(
            boolean(row, read, "is_workflow_task")?,
            read,
            "is_workflow_task",
        )?,
        rerun_root_task_id: uuid(row, read, "rerun_root_task_id")?,
        rerun_input_disposition: required(
            text(row, read, "rerun_input_disposition")?,
            read,
            "rerun_input_disposition",
        )?,
        rerun_input_version: int2(row, read, "rerun_input_version")?,
        rerun_input_digest: bytea(row, read, "rerun_input_digest")?,
        rerun_input_inline: bytea(row, read, "rerun_input_inline")?,
    }))
}

/// The eligibility ladder and the write of a history source (Python
/// `rerun_task` after `read_rerun_source`).
fn prepare_write(
    source: RerunSource,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
    sent_at: DateTime<Utc>,
) -> RerunPreparation {
    let source_task_id = command.source_task_id();
    if source.is_workflow_task {
        return RerunPreparation::Refused(RerunRefusal::NotEligible {
            task_id: source_task_id,
            reason: NotEligibleReason::WorkflowTask,
        });
    }
    if source.status == TaskStatus::Completed {
        return RerunPreparation::Refused(RerunRefusal::NotEligible {
            task_id: source_task_id,
            reason: NotEligibleReason::CompletedSource,
        });
    }
    if source.rerun_input_disposition != "INLINE" {
        return RerunPreparation::Refused(RerunRefusal::InputUnavailable {
            task_id: source_task_id,
            disposition: source.rerun_input_disposition,
        });
    }
    let (Some(version), Some(payload), Some(digest)) = (
        source.rerun_input_version,
        source.rerun_input_inline.as_deref(),
        source.rerun_input_digest.as_deref(),
    ) else {
        return input_corrupt(
            source_task_id,
            "inline disposition with an incomplete envelope",
        );
    };
    let framed = match decode_framed_rerun_input(version, payload, digest) {
        Ok(decoded) => decoded,
        Err(InputEnvelopeDecodeError::VersionUnknown(version)) => {
            return input_corrupt(
                source_task_id,
                &format!("unknown input-envelope version {version}"),
            );
        }
        Err(InputEnvelopeDecodeError::Corrupt(detail)) => {
            return input_corrupt(source_task_id, &detail);
        }
    };
    let root_task_id = source.rerun_root_task_id.unwrap_or(source.task_id);
    // The rerun runs with the execution class of the stored request; stored
    // options or input that the send rules refuse are corrupt input.
    let facts = match prepare_enqueue_facts_with_lineage(
        &source.task_name,
        &source.queue_name,
        source.priority,
        &framed.input,
        command.deadline(),
        None,
        framed.options.as_deref(),
        Some(policy.retention_class_key()),
        policy.retain_rerun_input(),
        command.caller_key(),
        Some(source.task_id),
        Some(root_task_id),
    ) {
        Ok(facts) => facts,
        Err(EnqueuePreparationError::Idempotency(refused)) => {
            return RerunPreparation::CallerKeyRefused(refused);
        }
        Err(EnqueuePreparationError::InvalidExecution(invalid)) => {
            return input_corrupt(
                source_task_id,
                &format!("stored task options: {}", invalid.detail),
            );
        }
        Err(
            other @ (EnqueuePreparationError::StrictJson { .. }
            | EnqueuePreparationError::Json { .. }
            | EnqueuePreparationError::NumberDomain { .. }
            | EnqueuePreparationError::InvalidTaskOptions
            | EnqueuePreparationError::Fingerprint(_)),
        ) => {
            return input_corrupt(source_task_id, &format!("stored input: {other}"));
        }
    };
    let new_task_id = match mint_task_id() {
        Ok(task_id) => task_id,
        Err(refused) => return RerunPreparation::IdMintRefused(refused),
    };
    // The policy was checked at construction (`validate_reservation_window`):
    // at most 30 days, a whole number of microseconds.
    let window_micros = policy
        .reservation_window()
        .num_microseconds()
        .expect("a checked window of at most 30 days fits an i64 count of microseconds");
    let key = EnqueueKey::of(facts.idempotency_key_digest, window_micros);
    let (key_kind, key_scope_version, key_window_micros) = key.columns();
    let values = vec![
        StatementValue::Uuid(new_task_id),
        StatementValue::Text(source.task_name),
        StatementValue::Text(source.queue_name),
        StatementValue::Int4(source.priority),
        StatementValue::Text(framed.input),
        optional_text(framed.options),
        StatementValue::Text(facts.execution.as_str().to_owned()),
        StatementValue::Text(facts.retention_class_key),
        StatementValue::Bool(facts.retain_rerun_input),
        StatementValue::Int4(source.max_retries),
        optional_instant(command.deadline()),
        StatementValue::Timestamptz(sent_at),
        StatementValue::Int2(facts.command_fingerprint_version),
        StatementValue::Bytea(facts.command_fingerprint.to_vec()),
        StatementValue::Uuid(source.task_id),
        StatementValue::Uuid(root_task_id),
        StatementValue::Text(key_kind.to_owned()),
        key.digest()
            .map_or(StatementValue::Null(CallerType::Bytea), |digest| {
                StatementValue::Bytea(digest.to_vec())
            }),
        key_scope_version.map_or(StatementValue::Null(CallerType::Int2), StatementValue::Int2),
        key_window_micros.map_or(StatementValue::Null(CallerType::Int8), StatementValue::Int8),
    ];
    let statement = CallerStatement::new(&RERUN_ENQUEUE, values, &RERUN_WRITE_COLUMNS)
        .expect("the rerun write values follow the declared parameters of its statement");
    RerunPreparation::Write(RerunWrite {
        statement,
        new_task_id,
        source_task_id: source.task_id,
        rerun_root_task_id: root_task_id,
    })
}

fn input_corrupt(task_id: Uuid, detail: &str) -> RerunPreparation {
    RerunPreparation::Refused(RerunRefusal::InputCorrupt {
        task_id,
        detail: detail.to_owned(),
    })
}

fn optional_text(value: Option<String>) -> StatementValue {
    match value {
        Some(text) => StatementValue::Text(text),
        None => StatementValue::Null(CallerType::Text),
    }
}

fn optional_instant(value: Option<DateTime<Utc>>) -> StatementValue {
    match value {
        Some(instant) => StatementValue::Timestamptz(instant),
        None => StatementValue::Null(CallerType::Timestamptz),
    }
}

/// The outcome of the row of `write` (Python `_rerun_write_outcome` after
/// `read_enqueue_outcome`). Pure.
///
/// Panics on an outcome outside the function's set, and on
/// `PAYLOAD_MISMATCH` or `CONFLICT_UNVERIFIABLE`: the task id was minted for
/// this write, so no other row can hold it; either answer is a defect of the
/// id mint or of the function.
///
/// Errors: [`RerunRowError`] when the row does not fit the write's columns.
pub fn decode_rerun_write(
    write: &RerunWrite,
    row: &CallerRow,
) -> Result<RerunWriteOutcome, RerunRowError> {
    let statement = write.statement();
    let outcome = required(text(row, statement, "outcome")?, statement, "outcome")?;
    let answering_task_id = required(uuid(row, statement, "task_id")?, statement, "task_id")?;
    let written = match decode_enqueue_outcome(&outcome, answering_task_id) {
        Ok(written) => written,
        Err(defect) => panic!(
            "statement `{}` returned a row outside the enqueue contract: {defect}",
            statement.name(),
        ),
    };
    Ok(rerun_write_outcome(
        written,
        write.new_task_id,
        write.source_task_id,
        write.rerun_root_task_id,
    ))
}

/// The rerun outcome of the write of `new_task_id`. Pure.
///
/// `SAME_COMMAND` is the write of this id that already ran in the caller's
/// transaction: the rerun is enqueued.
///
/// Panics on `PAYLOAD_MISMATCH` and `CONFLICT_UNVERIFIABLE`: `new_task_id`
/// was minted for this write, so no other row can hold it.
fn rerun_write_outcome(
    written: EnqueueTaskOutcome,
    new_task_id: Uuid,
    source_task_id: Uuid,
    rerun_root_task_id: Uuid,
) -> RerunWriteOutcome {
    match written {
        EnqueueTaskOutcome::Inserted | EnqueueTaskOutcome::SameCommand => {
            RerunWriteOutcome::Enqueued {
                new_task_id,
                source_task_id,
                rerun_root_task_id,
            }
        }
        EnqueueTaskOutcome::KeyReplay { holder } => RerunWriteOutcome::KeyReplay {
            existing_task_id: holder,
        },
        EnqueueTaskOutcome::KeyConflict { owner } => RerunWriteOutcome::KeyConflict {
            task_id: source_task_id,
            reserved_by_task_id: owner,
        },
        EnqueueTaskOutcome::PayloadMismatch | EnqueueTaskOutcome::ConflictUnverifiable => panic!(
            "the rerun write of the minted task id {new_task_id} answered {written:?}: \
             no other row can hold a minted id"
        ),
    }
}

/// `value` when the function set it, else [`RerunRowError::NullColumn`].
fn required<T>(
    value: Option<T>,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<T, RerunRowError> {
    value.ok_or(RerunRowError::NullColumn {
        statement: statement.name(),
        column,
    })
}

fn unreachable_type(column: &str, value: &StatementValue) -> ! {
    unreachable!("`CallerRow::value` checked the type of column `{column}`, and it holds {value:?}")
}

fn text(
    row: &CallerRow,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<Option<String>, CallerRowRefused> {
    match row.value(statement, column)? {
        StatementValue::Text(value) => Ok(Some(value.clone())),
        StatementValue::Null(_) => Ok(None),
        other => unreachable_type(column, other),
    }
}

fn boolean(
    row: &CallerRow,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<Option<bool>, CallerRowRefused> {
    match row.value(statement, column)? {
        StatementValue::Bool(value) => Ok(Some(*value)),
        StatementValue::Null(_) => Ok(None),
        other => unreachable_type(column, other),
    }
}

fn int2(
    row: &CallerRow,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<Option<i16>, CallerRowRefused> {
    match row.value(statement, column)? {
        StatementValue::Int2(value) => Ok(Some(*value)),
        StatementValue::Null(_) => Ok(None),
        other => unreachable_type(column, other),
    }
}

fn int4(
    row: &CallerRow,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<Option<i32>, CallerRowRefused> {
    match row.value(statement, column)? {
        StatementValue::Int4(value) => Ok(Some(*value)),
        StatementValue::Null(_) => Ok(None),
        other => unreachable_type(column, other),
    }
}

fn uuid(
    row: &CallerRow,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<Option<Uuid>, CallerRowRefused> {
    match row.value(statement, column)? {
        StatementValue::Uuid(value) => Ok(Some(*value)),
        StatementValue::Null(_) => Ok(None),
        other => unreachable_type(column, other),
    }
}

fn bytea(
    row: &CallerRow,
    statement: &CallerStatement,
    column: &'static str,
) -> Result<Option<Vec<u8>>, CallerRowRefused> {
    match row.value(statement, column)? {
        StatementValue::Bytea(value) => Ok(Some(value.clone())),
        StatementValue::Null(_) => Ok(None),
        other => unreachable_type(column, other),
    }
}

#[cfg(test)]
mod tests;
