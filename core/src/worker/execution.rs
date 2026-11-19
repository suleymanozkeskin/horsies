//! Task execution phase decomposition.
//!
//! Mirrors Python's `_finalize_after` architecture with named phase boundaries
//! and phase-aware error recovery:
//!
//! The start (CLAIMED → RUNNING) is the child's `horsies_start_attempt`
//! (`worker::executor::child_host`); this module holds what the child and
//! the worker's outcome writer share:
//!
//! - **Phase 1a** — `build_task_input`: the stored input, with workflow context
//! - **Phase 2a** — `schedule_retry_for_task`: atomic requeue + attempt record
//! - **Finalize Phase 1** — `persist_terminal_state`: terminal DB write
//! - **Finalize Phase 2** — `finalize_workflow_phase`: workflow callback + NOTIFY
//!
//! Error recovery uses per-phase retry budgets (Phase 1: 3, Phase 2: 5).
//! Phase 2 replays consume the durable v33 outbox row; terminal live rows are
//! no longer a recovery authority after the v35 move-to-history cutover.

use chrono::Utc;
use serde_json::value::RawValue;
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

use crate::broker::attempt_writes::{
    finish_failed_attempt, finish_unreported_attempt, finish_workflow_success, AttemptFailure,
    AttemptFailureKind, FinishFailedAttempt, FinishWorkflowSuccess, RetryPlacement, RetryRequest,
};
use crate::broker::child_writes::{query_within, WriteDeadline};
use crate::broker::core_rows::{column, one_row, scalar, StatementLog};
use crate::broker::listener::notify_task_queue;
use crate::broker::terminalization::terminalize;
use crate::broker::PostgresBroker;
use crate::core::codec::check_json_text;
use crate::core::codec::user_value::{
    check_input_members, check_input_text, input_text_kind, InputRefused, InputText,
};
use crate::core::config::payload::{enforce_payload_policy, PayloadKind, PayloadPolicy};
use crate::core::lifecycle::{
    AfterFailure, AttemptWriteRefusal, FailedAttemptOutcome, OwnedClaim, PhaseTwoStep,
    TerminalizationCommand, TerminalizationOutcome, UnreportedAttemptOutcome,
    WorkflowSuccessOutcome,
};
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::result::TaskResult;
use crate::core::workflow::context::{WORKFLOW_CTX_KWARG, WORKFLOW_META_KWARG};
use crate::db::{
    CheckoutClass, CorePool, ParameterType, StatementBound, StatementFailure, TypedStatement,
};
use crate::workflow_engine::args_merge::{
    args_from_inner_text, object_members, object_text, ARGS_FROM_ENVELOPE_KEY,
};
use crate::workflow_engine::child_spec::ChildSpecSource;
use crate::workflow_engine::phase2_recovery::Phase2Wake;
use crate::workflow_engine::steps::{
    advance_parents, node_finish_alone, node_finish_in_transaction, Dependents,
    NodeFinishDisposition, OwedParentAdvances,
};

use crate::worker::executor::slots::{ClaimedTask, RetryFacts};
use crate::worker::retry::{admit_retry, RetryAdmission, StoredOptions};

use self::parse::workflow_ctx_text;

// ---------------------------------------------------------------------------
// Phase types
// ---------------------------------------------------------------------------

/// The form of a workflow task's phase-2 statement
/// (`horsies_workflow_node_finish`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhaseTwoForm {
    /// The phase-1 outcome named the step from the node facts.
    Step(PhaseTwoStep),
    /// The phase-1 outcome is not at hand (a replay of a committed phase-1
    /// write, a pre-start expiry): the step reads the dependents and runs in
    /// a transaction. A plain task has no phase-2 statement and carries this
    /// form too.
    NotKnown,
}

/// Outcome of Phase 1 finalize: persisting the terminal task state.
pub(crate) enum FinalizeOutcome {
    /// Task completed or failed terminally. Needs Phase 2 (workflow callback).
    /// The node status comes from the kind of the phase-2 record, which the
    /// node finish reads; phase 2 does not take the task's status.
    Terminal {
        /// Phase 1 already fired the capacity wake (the fused success path
        /// notifies in-statement); Phase 2 must not wake capacity twice.
        capacity_notified: bool,
        /// The form of the phase-2 statement.
        phase_two: PhaseTwoForm,
    },
    /// Task was requeued for retry. No workflow callback needed.
    Retried,
    /// Plain (non-workflow) task fully finalized in one statement (fence,
    /// attempt, move and capacity wake), or a replay of a write that owes
    /// nothing more. No Phase 2 needed.
    Finalized,
    /// The fenced write found the row not held by this attempt (another
    /// claim generation, another state, or no row) and wrote nothing. Nothing
    /// is owed; the writer logged what it saw.
    Refused,
}

/// What phase 2 of one task needs, from its phase-1 write: the outcome
/// writer builds it after a terminal phase 1 and runs it with
/// [`run_phase2`].
pub(crate) struct Phase2Work {
    pub task_id: Uuid,
    pub queue_name: String,
    pub is_workflow_task: bool,
    /// Phase 1 already woke queue capacity; Phase 2 skips its notify.
    pub capacity_notified: bool,
    /// The form of the phase-2 statement of a workflow task.
    pub phase_two: PhaseTwoForm,
}

/// Which finalize phase failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalizeStage {
    /// Phase 1: persisting the terminal task state.
    Phase1Persist,
    /// Phase 2: workflow advancement + capacity notifications.
    Phase2Workflow,
}

/// Structured error for finalize phase failures.
///
/// Carries enough context for the phase-aware retry logic to decide
/// whether and how to replay.
#[derive(Debug)]
pub(crate) struct FinalizeError {
    pub stage: FinalizeStage,
    pub task_id: Uuid,
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for FinalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FinalizeError(stage={:?}, task={}, retryable={}, msg={})",
            self.stage, self.task_id, self.retryable, self.message
        )
    }
}

// ---------------------------------------------------------------------------
// Finalize retry bounds (Python `core/worker/runtime.py`)
// ---------------------------------------------------------------------------

/// Phase 1 finalize gets 3 retries (matches Python `_FINALIZE_PHASE1_MAX_RETRIES`).
pub(crate) const PHASE1_MAX_RETRIES: u32 = 3;
/// Phase 2 finalize gets 5 retries (matches Python `_FINALIZE_PHASE2_MAX_RETRIES`).
const PHASE2_MAX_RETRIES: u32 = 5;
/// Base delay for phase-level retry backoff.
const PHASE_RETRY_BASE_DELAY_S: f64 = 0.5;
/// Maximum delay for phase-level retry backoff.
const PHASE_RETRY_MAX_DELAY_S: f64 = 15.0;

// ---------------------------------------------------------------------------
// Phase 1a: Build the task input from the one stored input value
// ---------------------------------------------------------------------------

/// Build the task input bytes from one encoded input value.
///
/// Shared by the child's run (`worker::executor::child_host`) and
/// `app.check()`'s input validation (schedules and workflow nodes), so
/// check-time dry-runs read the input exactly as execution does.
///
/// The input must pass the core's strict JSON read rule; a refused text fails
/// the task before user code runs. An object input is Python's keyword input;
/// its engine members (Python `core/worker/child_runner.py:1362-1580`):
/// - `WORKFLOW_CTX_KWARG` becomes `workflow_ctx` when `accepts_workflow_ctx`,
///   and is removed otherwise (check-time callers pass `false`: no runtime
///   context);
/// - `WORKFLOW_META_KWARG` is removed;
/// - an `args_from` entry is replaced by its `inner` result envelope, which
///   the receiving `TaskResult` field decodes with its own type.
///
/// Every other member value, and any other input except `null` as one value,
/// follows the rules of a user value (no engine key, at most
/// `MAX_VALUE_JSON_DEPTH` levels), and no other member name is an engine key.
/// A refusal is `WORKER_SERIALIZATION_ERROR`, as Python's
/// `_serialization_error_response`.
///
/// An input with no engine member passes byte for byte. An input with one is
/// written from the texts of its members: each other member and each `inner`
/// envelope keeps its stored text, and so does each result of the context
/// ([`workflow_ctx_text`]). No value is parsed and written again, so an
/// integer outside the `i64` and `u64` range reaches the task as stored, as
/// Python's `loads_json` reads any integer (`core/codec/json_io.py:97`).
pub(crate) fn build_task_input(
    input: &str,
    accepts_workflow_ctx: bool,
) -> Result<Vec<u8>, TaskError> {
    check_json_text(input)
        .map_err(|e| serialization_error(format!("stored input refused: {e}")))?;
    let no_engine_members: HashSet<String> = HashSet::new();
    let object = match input_text_kind(input) {
        InputText::Object(members) => members,
        InputText::Null => return Ok(input.as_bytes().to_vec()),
        InputText::OneValue => {
            check_input_text(input, &no_engine_members)
                .map_err(|refusal| input_refused(&refusal))?;
            return Ok(input.as_bytes().to_vec());
        }
    };
    if !holds_engine_members(&object) {
        check_input_members(&object, &no_engine_members)
            .map_err(|refusal| input_refused(&refusal))?;
        return Ok(input.as_bytes().to_vec());
    }

    let members = task_members(input)?;
    let args_from_names: HashSet<String> = members
        .task_members
        .iter()
        .filter_map(|(name, member)| match member {
            TaskMember::ArgsFrom(_) => Some(name.clone()),
            TaskMember::Stored(_) => None,
        })
        .collect();
    let user_members: Vec<(String, &RawValue)> = object
        .into_iter()
        .filter(|(name, _value)| name != WORKFLOW_CTX_KWARG && name != WORKFLOW_META_KWARG)
        .collect();
    check_input_members(&user_members, &args_from_names)
        .map_err(|refusal| input_refused(&refusal))?;
    let workflow_ctx = match (members.workflow_ctx, accepts_workflow_ctx) {
        (Some(payload), true) => Some(workflow_ctx_text(payload.get())?),
        (Some(_), false) | (None, _) => None,
    };
    Ok(task_input_text(&members.task_members, workflow_ctx.as_deref()).into_bytes())
}

/// The input field that takes the workflow context (Python's `workflow_ctx`
/// parameter).
const WORKFLOW_CTX_FIELD: &str = "workflow_ctx";

/// Whether an object input holds a member that the engine writes: the
/// workflow context, the workflow meta, or an `args_from` entry (an object
/// whose `ARGS_FROM_ENVELOPE_KEY` member is `true`). The members are parts of
/// a text that passed the strict read rule; each is read as its text. Pure.
fn holds_engine_members(members: &[(String, &RawValue)]) -> bool {
    members.iter().any(|(name, value)| {
        name == WORKFLOW_CTX_KWARG || name == WORKFLOW_META_KWARG || is_args_from_entry(value)
    })
}

/// Whether a member text is an object whose `ARGS_FROM_ENVELOPE_KEY` member is
/// `true`. Pure.
fn is_args_from_entry(value: &RawValue) -> bool {
    match value.get().starts_with('{') {
        false => false,
        true => object_members(value.get()).is_ok_and(|entry| {
            entry
                .iter()
                .any(|(key, flag)| key == ARGS_FROM_ENVELOPE_KEY && flag.get() == "true")
        }),
    }
}

/// One member of the task input, as its text.
enum TaskMember<'a> {
    /// A member of the stored input, as stored.
    Stored(&'a RawValue),
    /// The `inner` result envelope of an `args_from` entry, as stored.
    ArgsFrom(&'a RawValue),
}

impl<'a> TaskMember<'a> {
    fn text(&self) -> &'a str {
        match *self {
            Self::Stored(value) | Self::ArgsFrom(value) => value.get(),
        }
    }
}

/// The members of an object input with engine members, read from its text.
struct TaskMembers<'a> {
    /// The members that the task gets, in text order, each with its name.
    task_members: Vec<(String, TaskMember<'a>)>,
    /// The context payload (`WORKFLOW_CTX_KWARG`), as its text.
    workflow_ctx: Option<&'a RawValue>,
}

/// Read the members of the object input `input` from its text: the context
/// payload, the workflow meta (left out), and the members that the task gets.
///
/// Errors: `WORKER_SERIALIZATION_ERROR` for a malformed `args_from` entry
/// ([`args_from_inner_text`]), and when serde_json does not read the members
/// of a text that the strict read took. Pure.
fn task_members(input: &str) -> Result<TaskMembers<'_>, TaskError> {
    let members = object_members(input)
        .map_err(|e| serialization_error(format!("stored input refused: {e}")))?;
    let mut read = TaskMembers {
        task_members: Vec::with_capacity(members.len()),
        workflow_ctx: None,
    };
    for (name, value) in members {
        match name.as_str() {
            WORKFLOW_CTX_KWARG => read.workflow_ctx = Some(value),
            WORKFLOW_META_KWARG => {}
            _ => {
                let member = match args_from_inner_text(&name, value)
                    .map_err(|refusal| serialization_error(refusal.to_string()))?
                {
                    Some(inner) => TaskMember::ArgsFrom(inner),
                    None => TaskMember::Stored(value),
                };
                read.task_members.push((name, member));
            }
        }
    }
    Ok(read)
}

/// The task input text: `members` in their order, with `workflow_ctx`, when
/// given, as the `workflow_ctx` field. A member of that name keeps its place
/// and takes the context, as Python's `kwargs['workflow_ctx'] = ...`; else
/// the field comes last. Pure.
fn task_input_text(members: &[(String, TaskMember<'_>)], workflow_ctx: Option<&str>) -> String {
    let mut written: Vec<(&str, &str)> = members
        .iter()
        .map(|(name, member)| (name.as_str(), member.text()))
        .collect();
    match workflow_ctx {
        None => {}
        Some(context) => match written
            .iter()
            .position(|(name, _)| *name == WORKFLOW_CTX_FIELD)
        {
            Some(place) => written[place].1 = context,
            None => written.push((WORKFLOW_CTX_FIELD, context)),
        },
    }
    object_text(&written)
}

fn serialization_error(message: String) -> TaskError {
    TaskError::builtin(OperationalErrorCode::WorkerSerializationError, message)
}

fn input_refused(refusal: &InputRefused) -> TaskError {
    serialization_error(format!("failed to decode task input: {refusal}"))
}

// ---------------------------------------------------------------------------
// Finalize Phase 1: Persist the outcome of the attempt
// ---------------------------------------------------------------------------

/// Phase 1 of finalization: write the outcome of the attempt in one statement.
///
/// - The success of a plain task: `horsies_complete_task_fused`.
/// - The success of a workflow task: `horsies_finish_workflow_success`.
/// - A failure, and a result that is not valid JSON:
///   `horsies_finish_failed_attempt`, with the retry that the policy admits.
///
/// Each statement runs alone on the pool and decides the whole outcome under
/// its own locks (the task advisory lock, then the row), and writes the
/// attempt row from the locked row on the database clock. A database error
/// never decides between a retry and a failure: it is a phase-1 error,
/// retryable when the connection failed, and the next try sends the same
/// statement.
/// A refusal is an outcome ([`FinalizeOutcome::Refused`]), not an error.
///
/// `task` is the claimed task as the claim returned it: its claim generation
/// fences every write, and its retry facts and stored options decide the
/// retry of a failure.
pub(crate) async fn persist_terminal_state(
    broker: &PostgresBroker,
    task: &ClaimedTask,
    result: TaskResult<Vec<u8>>,
    worker_id: &str,
    payload_policy: &PayloadPolicy,
) -> Result<FinalizeOutcome, FinalizeError> {
    let task_id = task.task_id;
    let is_workflow_task = task.kind.is_workflow_task();
    let fence = OwnedClaim {
        worker_id: worker_id.to_owned(),
        claimed_at: task.claimed_at,
    };
    match result {
        TaskResult::Ok(ref result_bytes) => {
            // Warn-only: results are never rejected over size (the work is
            // done; destroying it would convert a size concern into data
            // loss). Parity with horsies PR #208.
            enforce_payload_policy(
                payload_policy,
                &task.task_name,
                PayloadKind::Result,
                result_bytes.len(),
            );
            persist_ok_result(broker, task_id, result_bytes, fence, is_workflow_task).await
        }
        TaskResult::Err(ref task_error) => {
            // The stored error keeps its code when its `data` cannot be stored:
            // the serializer stores `{"data_refused": <cause>}` in its place
            // (Python `reportable_error`, `core/codec/typed.py:885-934`).
            if let Some(refusal) = task_error.storage_refusal() {
                tracing::error!(
                    task_id = %task_id,
                    error_code = %task_error.error_code,
                    cause = %refusal,
                    "data of a TaskError cannot be stored; it is stored as data_refused",
                );
            }
            let error_code = Some(task_error.error_code.to_string());
            let retry = retry_request_for(&task.retry, &task.options, error_code.as_deref());
            let result_json =
                serde_json::to_string(&TaskResult::<serde_json::Value>::Err(task_error.clone()))
                    .map_err(|error| envelope_error(task_id, error))?;
            // Warn-only: error envelopes can carry oversized data too.
            enforce_payload_policy(
                payload_policy,
                &task.task_name,
                PayloadKind::Result,
                result_json.len(),
            );
            let command = FinishFailedAttempt {
                task_id,
                fence,
                retry,
                result_json,
                error_code,
                failed_reason: None,
                attempt: AttemptFailure {
                    kind: AttemptFailureKind::Failed,
                    error_message: task_error.message.clone(),
                    failed_reason: None,
                },
            };
            persist_failed_attempt(broker, &command, is_workflow_task).await
        }
    }
}

/// The retry that the task's policy admits for this failure, with its delay
/// (Python `retry_request_for`). Pure. `retry_count` and `max_retries` come
/// from the claim of this claim generation; the write fences on that count.
/// The deadline is judged by the write, on the database clock.
fn retry_request_for(
    retry: &RetryFacts,
    stored_options: &StoredOptions,
    error_code: Option<&str>,
) -> RetryRequest {
    match admit_retry(
        retry.retry_count,
        retry.max_retries,
        stored_options.retry(),
        error_code,
    ) {
        RetryAdmission::Admitted(rule) => RetryRequest::RetryAfter {
            expected_retry_count: retry.retry_count,
            delay_seconds: rule.delay_seconds((retry.retry_count + 1).unsigned_abs()),
            placement: RetryPlacement::SameClass,
        },
        RetryAdmission::RetriesSpent | RetryAdmission::NoPolicy | RetryAdmission::CodeNotListed => {
            RetryRequest::NoRetry
        }
    }
}

/// Persist a success, or fail a result that is not valid JSON.
pub(crate) async fn persist_ok_result(
    broker: &PostgresBroker,
    task_id: Uuid,
    result_bytes: &[u8],
    fence: OwnedClaim,
    is_workflow_task: bool,
) -> Result<FinalizeOutcome, FinalizeError> {
    // Wrap the task's result bytes via `&RawValue`: parsing validates the JSON,
    // and the adjacently-tagged wrapper embeds the original bytes verbatim
    // instead of building and re-serializing a full `Value` tree (P6).
    let raw = match serde_json::from_slice::<&serde_json::value::RawValue>(result_bytes) {
        Ok(raw) => raw,
        Err(parse_error) => {
            return persist_undecodable_result(
                broker,
                task_id,
                fence,
                is_workflow_task,
                &parse_error.to_string(),
            )
            .await
        }
    };
    let result_json = serde_json::to_string(&TaskResult::Ok(raw))
        .map_err(|error| envelope_error(task_id, error))?;
    match is_workflow_task {
        true => persist_workflow_success(broker, task_id, fence, result_json).await,
        false => persist_plain_success(broker, task_id, fence, result_json).await,
    }
}

/// The success of a plain task: one statement on the main core pool locks the
/// RUNNING row under the full fence, writes the attempt, moves the row and
/// sends the capacity wake of the row's queue.
async fn persist_plain_success(
    broker: &PostgresBroker,
    task_id: Uuid,
    fence: OwnedClaim,
    result_json: String,
) -> Result<FinalizeOutcome, FinalizeError> {
    let command = TerminalizationCommand::CompleteTaskFused {
        task_id,
        fence,
        result_json,
    };
    let (open, _write) = broker
        .core_pools_for_write()
        .map_err(|error| phase1_error(task_id, "complete-fused", error))?;
    let outcomes = terminalize(open.main(), broker.statement_log(), &command)
        .await
        .map_err(|error| phase1_error(task_id, "complete-fused", error))?;
    match outcomes.into_iter().next() {
        // A replay found its own class committed: nothing is owed.
        Some(
            TerminalizationOutcome::Applied { .. } | TerminalizationOutcome::AlreadyApplied { .. },
        ) => Ok(FinalizeOutcome::Finalized),
        Some(
            TerminalizationOutcome::LostClaim { .. }
            | TerminalizationOutcome::SourceStateConflict { .. }
            | TerminalizationOutcome::TaskAbsent { .. },
        ) => Ok(FinalizeOutcome::Refused),
        None => Err(FinalizeError {
            stage: FinalizeStage::Phase1Persist,
            task_id,
            message: "complete-fused returned no outcome".to_owned(),
            retryable: false,
        }),
    }
}

/// The success of a workflow task, in one statement. Phase 2 is owed after
/// it applies.
async fn persist_workflow_success(
    broker: &PostgresBroker,
    task_id: Uuid,
    fence: OwnedClaim,
    result_json: String,
) -> Result<FinalizeOutcome, FinalizeError> {
    let command = FinishWorkflowSuccess {
        task_id,
        fence,
        result_json,
    };
    let (open, _write) = broker
        .core_pools_for_write()
        .map_err(|error| phase1_error(task_id, "workflow success", error))?;
    let outcome = finish_workflow_success(open.main(), broker.statement_log(), &command)
        .await
        .map_err(|error| phase1_error(task_id, "workflow success", error))?;
    match outcome {
        WorkflowSuccessOutcome::SuccessApplied { phase_two, .. } => Ok(FinalizeOutcome::Terminal {
            capacity_notified: false,
            phase_two: PhaseTwoForm::Step(phase_two.step),
        }),
        WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. }) => {
            replay_of_committed_write(broker, task_id, true).await
        }
        WorkflowSuccessOutcome::Refused(
            AttemptWriteRefusal::LostClaim { .. }
            | AttemptWriteRefusal::SourceStateConflict { .. }
            | AttemptWriteRefusal::TaskAbsent { .. },
        ) => Ok(FinalizeOutcome::Refused),
    }
}

/// A result that is not valid JSON fails the task with
/// `WORKER_SERIALIZATION_ERROR`, never retried, with the decode diagnosis as
/// the attempt message (Python `ResultUndecodable`).
pub(crate) async fn persist_undecodable_result(
    broker: &PostgresBroker,
    task_id: Uuid,
    fence: OwnedClaim,
    is_workflow_task: bool,
    diagnosis: &str,
) -> Result<FinalizeOutcome, FinalizeError> {
    let message = format!("failed to parse task result JSON: {diagnosis}");
    let error = TaskError::builtin(
        OperationalErrorCode::WorkerSerializationError,
        message.clone(),
    );
    let result_json = serde_json::to_string(&TaskResult::<serde_json::Value>::Err(error))
        .map_err(|error| envelope_error(task_id, error))?;
    let command = FinishFailedAttempt {
        task_id,
        fence,
        retry: RetryRequest::NoRetry,
        result_json,
        error_code: Some(OperationalErrorCode::WorkerSerializationError.to_string()),
        failed_reason: None,
        attempt: AttemptFailure {
            kind: AttemptFailureKind::Failed,
            error_message: Some(message),
            failed_reason: None,
        },
    };
    persist_failed_attempt(broker, &command, is_workflow_task).await
}

/// Write a failed attempt in one statement and map its outcome to what is
/// owed (Python `_after_failed_attempt`): a plain task's failure sent its
/// wake in the statement; a workflow task's failure owes phase 2; a retry
/// wakes the queue when it is due; a replay owes what the committed write
/// owed; a refusal owes nothing.
pub(crate) async fn persist_failed_attempt(
    broker: &PostgresBroker,
    command: &FinishFailedAttempt,
    is_workflow_task: bool,
) -> Result<FinalizeOutcome, FinalizeError> {
    let (open, _write) = broker
        .core_pools_for_write()
        .map_err(|error| phase1_error(command.task_id, "failed attempt", error))?;
    let outcome = finish_failed_attempt(open.main(), broker.statement_log(), command)
        .await
        .map_err(|error| phase1_error(command.task_id, "failed attempt", error))?;
    after_failed_attempt(broker, command, is_workflow_task, outcome).await
}

/// What the write of an attempt whose child ended without a report did.
pub(crate) enum UnreportedAttemptWrite {
    /// The row was still CLAIMED by this claim generation: no child started
    /// it, and the statement put it back in the queue with no attempt row.
    ClaimReleased,
    /// The row was not CLAIMED by this claim generation: the failed attempt
    /// was written or refused, as [`persist_failed_attempt`] maps it.
    Written(FinalizeOutcome),
}

/// Write the failed attempt of a child that ended without a report (a
/// timeout, a loop kill or a crash) in one statement, with the claimed-row
/// rule `RELEASE`.
///
/// Errors: a phase-1 [`FinalizeError`], retryable when the database error
/// is. After an unknown outcome a released row reads as not held when the
/// write is sent again.
pub(crate) async fn persist_unreported_attempt(
    broker: &PostgresBroker,
    command: &FinishFailedAttempt,
    is_workflow_task: bool,
) -> Result<UnreportedAttemptWrite, FinalizeError> {
    let (open, _write) = broker
        .core_pools_for_write()
        .map_err(|error| phase1_error(command.task_id, "unreported attempt", error))?;
    let outcome = finish_unreported_attempt(open.main(), broker.statement_log(), command)
        .await
        .map_err(|error| phase1_error(command.task_id, "unreported attempt", error))?;
    match outcome {
        UnreportedAttemptOutcome::Released(_) => Ok(UnreportedAttemptWrite::ClaimReleased),
        UnreportedAttemptOutcome::Failed(failed) => {
            after_failed_attempt(broker, command, is_workflow_task, failed)
                .await
                .map(UnreportedAttemptWrite::Written)
        }
    }
}

/// What a written or refused failed attempt owes.
async fn after_failed_attempt(
    broker: &PostgresBroker,
    command: &FinishFailedAttempt,
    is_workflow_task: bool,
    outcome: FailedAttemptOutcome,
) -> Result<FinalizeOutcome, FinalizeError> {
    match outcome {
        FailedAttemptOutcome::FailureApplied {
            after: AfterFailure::CapacityWoken,
            ..
        } => Ok(FinalizeOutcome::Finalized),
        FailedAttemptOutcome::FailureApplied {
            after: AfterFailure::PhaseTwoOwed(owed),
            ..
        } => Ok(FinalizeOutcome::Terminal {
            capacity_notified: false,
            phase_two: PhaseTwoForm::Step(owed.step),
        }),
        FailedAttemptOutcome::RetryScheduled {
            retry_count,
            next_retry_at,
            queue_name,
            ..
        } => {
            wake_when_retry_due(broker, command, queue_name);
            tracing::info!(
                task_id = %command.task_id,
                retry_count,
                %next_retry_at,
                "task scheduled for retry",
            );
            Ok(FinalizeOutcome::Retried)
        }
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. }) => {
            replay_of_committed_write(broker, command.task_id, is_workflow_task).await
        }
        FailedAttemptOutcome::Refused(
            AttemptWriteRefusal::LostClaim { .. }
            | AttemptWriteRefusal::SourceStateConflict { .. }
            | AttemptWriteRefusal::TaskAbsent { .. },
        ) => Ok(FinalizeOutcome::Refused),
    }
}

/// Wake the queue of a scheduled retry after the delay of its request
/// (Python `RetryMixin._wake_when_retry_due`): one `pg_notify` in autocommit
/// on the main core pool of `broker`, as open when the retry was written. A
/// failed wake is logged: the retried row is PENDING and the worker's poll
/// finds it. A close of the broker in the delay closes that pool, so the
/// wake fails and is logged.
fn wake_when_retry_due(broker: &PostgresBroker, command: &FinishFailedAttempt, queue_name: String) {
    let delay_seconds = match command.retry {
        RetryRequest::RetryAfter { delay_seconds, .. } => delay_seconds,
        RetryRequest::NoRetry => unreachable!(
            "task {} was scheduled for a retry that was not requested",
            command.task_id
        ),
    };
    let task_id = command.task_id;
    let open = match broker.core_pools_for_write() {
        Ok((open, _write)) => open,
        Err(error) => {
            tracing::warn!(
                task_id = %task_id,
                queue = %queue_name,
                error = %error,
                "delayed retry NOTIFY not scheduled: {error}; worker polling fallback will recover",
            );
            return;
        }
    };
    let statement_log = broker.statement_log();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs_f64(delay_seconds)).await;
        if let Err(error) =
            notify_task_queue(open.main(), statement_log, &queue_name, task_id).await
        {
            tracing::warn!(
                task_id = %task_id,
                queue = %queue_name,
                error = %error,
                "Error sending delayed notification for retry:{task_id}: {error}",
            );
        }
    });
}

/// A replay of a committed write (`AlreadyApplied`). A workflow task owes
/// phase 2 while its durable phase-2 record exists; a plain task owes
/// nothing.
async fn replay_of_committed_write(
    broker: &PostgresBroker,
    task_id: Uuid,
    is_workflow_task: bool,
) -> Result<FinalizeOutcome, FinalizeError> {
    match is_workflow_task {
        true => match load_pending_phase_two(broker, task_id).await? {
            PendingPhaseTwo::Owed => Ok(FinalizeOutcome::Terminal {
                capacity_notified: false,
                phase_two: PhaseTwoForm::NotKnown,
            }),
            PendingPhaseTwo::Consumed => Ok(FinalizeOutcome::Finalized),
        },
        false => Ok(FinalizeOutcome::Finalized),
    }
}

/// The phase-1 error of a failed write: retryable when the connection failed.
fn phase1_error(task_id: Uuid, write: &str, error: crate::broker::BrokerError) -> FinalizeError {
    FinalizeError {
        stage: FinalizeStage::Phase1Persist,
        task_id,
        message: format!("{write} write failed: {error}"),
        retryable: error.is_retryable(),
    }
}

/// A result envelope that does not encode is a defect of the value, not of
/// the database: not retryable.
fn envelope_error(task_id: Uuid, error: serde_json::Error) -> FinalizeError {
    FinalizeError {
        stage: FinalizeStage::Phase1Persist,
        task_id,
        message: format!("failed to encode the result envelope: {error}"),
        retryable: false,
    }
}

// ---------------------------------------------------------------------------
// Finalize Phase 2: Workflow advancement + capacity notifications
// ---------------------------------------------------------------------------

/// Phase 2 of finalization: the node finish of a workflow task, or the
/// capacity wake of a plain task.
///
/// Separate from phase 1. If this fails after phase 1 committed, the terminal
/// task result and its phase-2 record are durable and phase-2 recovery
/// applies them.
///
/// A workflow task runs `horsies_workflow_node_finish` on the worker reserve
/// ([`finish_workflow_node`]), which sends the capacity wake when phase 1 did
/// not. A plain task sends the wake as one statement on the main core pool,
/// on the reserve, when phase 1 did not; an error of the wake is a phase-2
/// error, never dropped, retryable when the database error is transient
/// (`BrokerError::is_retryable`). Returns the parent advances that the
/// step owes after its commit.
///
/// The node finish builds the child of each sub-workflow node that it
/// promotes with `build`.
///
/// Errors: [`FinalizeError`] at [`FinalizeStage::Phase2Workflow`], retryable
/// when the failure may clear.
pub(crate) async fn finalize_workflow_phase(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    task_id: Uuid,
    queue_name: &str,
    is_workflow_task: bool,
    capacity_notified: bool,
    phase_two: PhaseTwoForm,
) -> Result<OwedParentAdvances, FinalizeError> {
    // Workflow membership is carried on the task row (is_workflow_task), set at
    // insert time — no per-task JOIN to horsies_workflow_tasks needed here.
    match is_workflow_task {
        true => {
            Box::pin(finish_workflow_node(
                broker,
                build,
                task_id,
                capacity_notified,
                phase_two,
            ))
            .await
        }
        false => {
            // Skipped when phase 1's statement already sent the wake in the
            // same commit as the transition: no statement, no checkout.
            let wake = match capacity_notified {
                true => return Ok(OwedParentAdvances::default()),
                false => capacity_wake(queue_name),
            };
            let wake_error = |error: crate::broker::BrokerError| FinalizeError {
                stage: FinalizeStage::Phase2Workflow,
                task_id,
                message: format!("capacity wake of queue {queue_name:?} failed: {error}"),
                retryable: error.is_retryable(),
            };
            let (open, _write) = broker.core_pools_for_write().map_err(wake_error)?;
            send_phase2_wake(open.main(), broker.statement_log(), &wake)
                .await
                .map_err(|failure| wake_error(crate::broker::BrokerError::Statement(failure)))?;
            Ok(OwedParentAdvances::default())
        }
    }
}

/// The statement of a phase-2 wake: `$1` the channel, `$2` the payload.
static SEND_PHASE2_WAKE: TypedStatement = TypedStatement::new(
    "send_phase2_wake",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

/// Send `wake` as one statement in autocommit on `pool`, a per-task write
/// ([`CheckoutClass::Reserved`]). Writes nothing else. [`Phase2Wake::NoWake`]
/// sends nothing.
///
/// Errors: the failed statement, for example on a dead connection or a
/// channel name that the server refuses; nothing was sent, or the outcome is
/// unknown.
pub(crate) async fn send_phase2_wake(
    pool: &CorePool,
    statement_log: StatementLog,
    wake: &Phase2Wake,
) -> Result<(), StatementFailure> {
    match wake {
        Phase2Wake::Queue { channel, payload } => {
            let statement = &SEND_PHASE2_WAKE;
            let bound = statement.bind().value(channel).value(payload).finish();
            statement_log.log(statement);
            pool.execute(
                CheckoutClass::Reserved,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await
            .map(|_notified| ())
        }
        Phase2Wake::NoWake => Ok(()),
    }
}

/// The payload of a capacity wake. The baseline functions send the same
/// constant (`crates/horsies-core/baseline/0004_lifecycle_functions.sql:1064`,
/// `:1510`; `0005_workflow_functions.sql:1972`). No receiver reads the
/// payload on a queue channel: the claim loop subscribes by channel.
const CAPACITY_WAKE_PAYLOAD: &str = "capacity";

/// The capacity wake of `queue_name`: the workers of the queue check their
/// capacity and backlog. It goes only to the queue's channel
/// `task_queue_<queue>`; workers do not listen on `task_new`.
pub(crate) fn capacity_wake(queue_name: &str) -> Phase2Wake {
    Phase2Wake::Queue {
        channel: format!("task_queue_{queue_name}"),
        payload: CAPACITY_WAKE_PAYLOAD.to_owned(),
    }
}

/// The node finish of a workflow task, in the form that the phase-1 node
/// facts name (`PhaseTwoStep`):
///
/// | Form | Statements |
/// |---|---|
/// | `NoDependents` | `horsies_workflow_node_finish(task, wake, 'NONE')`, one statement in autocommit |
/// | `DependentsWithoutSubworkflow` | `horsies_workflow_node_finish(task, wake, 'READ')`, one statement in autocommit |
/// | `SubworkflowReachable`, `NotKnown` | `BEGIN`; `horsies_workflow_node_finish(task, wake, 'READ')`; `horsies_workflow_child_start` per owed child start; `COMMIT` |
///
/// `wake` is true when phase 1 did not send the capacity wake; the statement
/// sends it on the channel of the node row's queue, delivered at its commit.
/// A call that finds no phase-2 record (a replay) sends no wake
/// (`horsies_workflow_node_apply`, `0005_workflow_functions.sql:1914-1919`).
///
/// A retaining disposition (the stored result did not pass its check) writes
/// nothing and is a phase-2 error that is not retried; phase-2 recovery
/// counts the record.
async fn finish_workflow_node(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    task_id: Uuid,
    capacity_notified: bool,
    phase_two: PhaseTwoForm,
) -> Result<OwedParentAdvances, FinalizeError> {
    let wake = !capacity_notified;
    let finished = match phase_two {
        PhaseTwoForm::Step(PhaseTwoStep::NoDependents) => Box::pin(node_finish_alone(
            broker,
            task_id,
            wake,
            Dependents::NoneToRead,
        ))
        .await
        .map(|outcome| {
            (
                outcome.disposition,
                OwedParentAdvances::of(outcome.parent_advance),
            )
        }),
        PhaseTwoForm::Step(PhaseTwoStep::DependentsWithoutSubworkflow) => {
            Box::pin(node_finish_alone(broker, task_id, wake, Dependents::Read))
                .await
                .map(|outcome| {
                    (
                        outcome.disposition,
                        OwedParentAdvances::of(outcome.parent_advance),
                    )
                })
        }
        PhaseTwoForm::Step(PhaseTwoStep::SubworkflowReachable) | PhaseTwoForm::NotKnown => {
            Box::pin(node_finish_in_transaction(broker, task_id, wake, build))
                .await
                .map(|committed| (committed.disposition, committed.parent_advances))
        }
    };
    let (disposition, parent_advances) = finished.map_err(|error| FinalizeError {
        stage: FinalizeStage::Phase2Workflow,
        task_id,
        message: format!("workflow node finish failed: {error}"),
        retryable: error.is_retryable(),
    })?;
    node_finish_outcome(task_id, disposition, parent_advances)
}

/// What a committed node finish leaves owed. Pure, apart from the log.
fn node_finish_outcome(
    task_id: Uuid,
    disposition: NodeFinishDisposition,
    parent_advances: OwedParentAdvances,
) -> Result<OwedParentAdvances, FinalizeError> {
    match disposition {
        NodeFinishDisposition::Applied | NodeFinishDisposition::AppliedWorkflowEnded => {
            Ok(parent_advances)
        }
        NodeFinishDisposition::AlreadyApplied | NodeFinishDisposition::PendingAbsent => {
            tracing::debug!(
                task_id = %task_id,
                disposition = ?disposition,
                "phase 2 found its record applied or absent; nothing written",
            );
            Ok(parent_advances)
        }
        NodeFinishDisposition::Retained(failure) => Err(FinalizeError {
            stage: FinalizeStage::Phase2Workflow,
            task_id,
            message: format!(
                "phase-2 record kept: the stored result did not pass its check ({}); \
                 phase-2 recovery counts it",
                failure.as_str()
            ),
            retryable: false,
        }),
    }
}

/// Whether a task has a phase-2 record. Its status is not read: the node
/// finish takes the node status from the record's kind.
const PENDING_PHASE_TWO_EXISTS_SQL: &str =
    "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)";

/// What the phase-2 record of a committed workflow write owes, as a replay
/// reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingPhaseTwo {
    /// The record exists: phase 2 is owed.
    Owed,
    /// No record: phase 2 consumed it, so the replay owes nothing.
    Consumed,
}

/// [`PENDING_PHASE_TWO_EXISTS_SQL`] with its parameter type: task id.
static PENDING_PHASE_TWO_EXISTS: TypedStatement = TypedStatement::new(
    "pending_phase_two_exists",
    PENDING_PHASE_TWO_EXISTS_SQL,
    &[ParameterType::Uuid],
);

/// Read whether a workflow task's phase-2 record exists after a phase-1
/// replay. One statement in autocommit on the main core pool of `broker`,
/// in the per-task write that replays ([`CheckoutClass::Reserved`]); writes
/// nothing. The moved history row is not read: the record is the evidence
/// that phase 2 is still owed.
///
/// Errors: the read fails: [`FinalizeError`] at
/// [`FinalizeStage::Phase2Workflow`], retryable when the failure may clear
/// (Python `_load_persisted_terminal`: `is_retryable_connection_error`).
async fn load_pending_phase_two(
    broker: &PostgresBroker,
    task_id: Uuid,
) -> Result<PendingPhaseTwo, FinalizeError> {
    let read_error = |error: crate::broker::BrokerError| FinalizeError {
        stage: FinalizeStage::Phase2Workflow,
        task_id,
        message: format!("failed to load the phase-2 record: {error}"),
        retryable: error.is_retryable(),
    };
    let (open, _write) = broker.core_pools_for_write().map_err(read_error)?;
    let statement = &PENDING_PHASE_TWO_EXISTS;
    let bound = statement.bind().value(&task_id).finish();
    broker.statement_log().log(statement);
    let rows = open
        .main()
        .query(
            CheckoutClass::Reserved,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await
        .map_err(|failure| read_error(crate::broker::BrokerError::Statement(failure)))?;
    let row = one_row(rows, statement)
        .map_err(|decode| read_error(crate::broker::BrokerError::RowDecode(decode)))?;
    let owed: bool = scalar(&row, statement)
        .map_err(|decode| read_error(crate::broker::BrokerError::RowDecode(decode)))?;
    Ok(match owed {
        true => PendingPhaseTwo::Owed,
        false => PendingPhaseTwo::Consumed,
    })
}

#[cfg(test)]
mod replay_read_tests;

// ---------------------------------------------------------------------------
// The finalizing marker and phase 2
// ---------------------------------------------------------------------------

/// What the finalizing marker found on the task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizingMark {
    /// The row is RUNNING under this worker's claim generation; the marker is
    /// written.
    Marked,
    /// The row is absent, not RUNNING, or held by another worker or another
    /// claim generation; nothing is written.
    NotHeld,
}

impl FinalizingMark {
    /// The mark of an outcome text of `horsies_mark_finalizing`, or `None`
    /// for a text outside its contract. Pure.
    fn from_outcome(outcome: &str) -> Option<Self> {
        match outcome {
            "MARKED" => Some(Self::Marked),
            "NOT_HELD" => Some(Self::NotHeld),
            _ => None,
        }
    }
}

/// The finalizing marker: `horsies_mark_finalizing`, as Python's
/// `MARK_FINALIZING_SQL` (`core/worker/child_runner.py`). The function reads
/// the row by id alone and locks it, so only the primary key serves the read;
/// the fence (RUNNING, this worker, this claim generation) is checked on the
/// locked row, with no NULL arm (a NULL argument is refused with 22023).
const MARK_FINALIZING_SQL: &str = "SELECT outcome FROM horsies_mark_finalizing($1, $2, $3)";

/// [`MARK_FINALIZING_SQL`] with its parameter types: task id, worker id,
/// claim generation.
static MARK_FINALIZING: TypedStatement = TypedStatement::new(
    "mark_finalizing",
    MARK_FINALIZING_SQL,
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
    ],
);

/// Stamp the finalization handoff only for the current owner and claim
/// generation. One statement in autocommit on the main core pool of
/// `broker`, a per-task write ([`CheckoutClass::Reserved`]): its row lock
/// ends with it. A close of the broker waits for it.
///
/// The marker is a child write within
/// [`crate::broker::child_writes::FINALIZING_MARKER_BOUND`] (Python
/// `FINALIZING_MARKER_TIMEOUT_S`): the wait for the connection and the
/// statement end by it, and a statement past it gets a cancel request, so a
/// marker that waits on a row lock is not written after the bound.
///
/// Errors: [`crate::broker::BrokerError::PoolSettings`] when a closed
/// handle cannot open its pools again; [`crate::broker::BrokerError::Statement`]
/// for a failed statement (nothing was sent when the bound passed first;
/// nothing was written, a cancel at the bound included; or the outcome is
/// unknown); [`crate::broker::BrokerError::RowDecode`] and
/// [`crate::broker::BrokerError::ClaimContract`] for a row outside the
/// function's contract.
pub(crate) async fn mark_task_finalizing(
    broker: &PostgresBroker,
    task_id: Uuid,
    worker_id: &str,
    claimed_at: chrono::DateTime<Utc>,
) -> Result<FinalizingMark, crate::broker::BrokerError> {
    let (open, _write) = broker.core_pools_for_write()?;
    let statement = &MARK_FINALIZING;
    let bound = statement
        .bind()
        .value(&task_id)
        .value(&worker_id)
        .value(&claimed_at)
        .finish();
    broker.statement_log().log(statement);
    let rows = query_within(open.main(), &bound, WriteDeadline::finalizing_marker()).await?;
    let [row] = rows.as_slice() else {
        return Err(crate::broker::BrokerError::ClaimContract(format!(
            "horsies_mark_finalizing returned {} rows for task {task_id}; it returns one",
            rows.len()
        )));
    };
    let outcome: String = column(row, statement, "outcome")?;
    FinalizingMark::from_outcome(&outcome).ok_or_else(|| {
        crate::broker::BrokerError::ClaimContract(format!(
            "horsies_mark_finalizing returned the outcome {outcome:?} for task {task_id}"
        ))
    })
}

/// Run Phase 2 finalize: the node finish of a workflow task (or the capacity
/// wake of a plain task), then the parent advances it owes.
///
/// Should be called AFTER the semaphore permit is released. Phase 2 has up to
/// [`PHASE2_MAX_RETRIES`] retries; the parent advances that a committed phase 2
/// owes run after it, one transaction per level, with their own retries
/// ([`retry_parent_advances`]). Both build the child of each sub-workflow
/// node that they promote with `build`.
pub(crate) async fn run_phase2(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    work: Phase2Work,
) {
    let finished = Box::pin(retry_phase2(broker, build, &work)).await;
    match finished {
        Ok(owed) if !owed.is_empty() => {
            Box::pin(retry_parent_advances(broker, build, work.task_id, owed)).await;
        }
        // Each failure was logged where it happened; workflow recovery
        // applies the durable phase-2 record.
        Ok(_) | Err(_) => {}
    }
}

/// Run Phase 2 with bounded retries: the first try and up to
/// [`PHASE2_MAX_RETRIES`] retries, each after [`phase_retry_delay`]. Only a
/// retryable error is retried.
///
/// Phase 1 already committed the durable phase-2 record, so every retry sends
/// the same step. If an earlier try committed (its reply was lost), the step
/// finds the record gone and reports `ALREADY_APPLIED`; the parent advances
/// that try owed are then left to workflow recovery (case 1.6).
///
/// Returns the parent advances of the committed step, or the last error
/// (already logged).
async fn retry_phase2(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    work: &Phase2Work,
) -> Result<OwedParentAdvances, FinalizeError> {
    let mut attempt: u32 = 0;
    loop {
        if attempt > 0 {
            let delay = phase_retry_delay(attempt);
            tracing::warn!(
                task_id = %work.task_id,
                attempt,
                max = PHASE2_MAX_RETRIES,
                delay_s = format!("{:.1}", delay),
                "retrying Phase 2 finalize",
            );
            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
        }
        let finished = Box::pin(finalize_workflow_phase(
            broker,
            build,
            work.task_id,
            &work.queue_name,
            work.is_workflow_task,
            work.capacity_notified,
            work.phase_two,
        ))
        .await;
        match finished {
            Ok(owed) => return Ok(owed),
            Err(error) if !error.retryable => {
                tracing::error!(
                    task_id = %work.task_id,
                    error = %error,
                    attempt,
                    "Finalize stage will not be re-attempted ({}) stage=phase2_workflow: {error}",
                    work.task_id,
                );
                return Err(error);
            }
            Err(error) if attempt >= PHASE2_MAX_RETRIES => {
                tracing::error!(
                    task_id = %work.task_id,
                    error = %error,
                    attempts = PHASE2_MAX_RETRIES,
                    "Finalize retries exhausted for task {} stage=phase2_workflow after \
                     {PHASE2_MAX_RETRIES} attempts: {error}",
                    work.task_id,
                );
                return Err(error);
            }
            Err(error) => {
                tracing::warn!(
                    task_id = %work.task_id,
                    error = %error,
                    attempt,
                    "Finalize retry scheduled for task {} stage=phase2_workflow \
                     ({}/{PHASE2_MAX_RETRIES}) in {:.1}s: {error}",
                    work.task_id,
                    attempt + 1,
                    phase_retry_delay(attempt + 1),
                );
            }
        }
        attempt += 1;
    }
}

/// Advances the parents above the child workflows that phase 2 ended
/// ([`advance_parents`], one transaction per level, on the worker reserve),
/// with up to [`PHASE2_MAX_RETRIES`] retries that go on from the level that
/// failed. A non-retryable failure, or spent
/// retries, leave the rest to workflow recovery (case 1.6: a terminal child
/// under a live parent node). Logs every failure.
async fn retry_parent_advances(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    task_id: Uuid,
    first_owed: OwedParentAdvances,
) {
    let mut owed = first_owed;
    for attempt in 0..=PHASE2_MAX_RETRIES {
        if attempt > 0 {
            let delay = phase_retry_delay(attempt);
            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
        }
        let failure = match Box::pin(advance_parents(
            broker,
            CheckoutClass::Reserved,
            owed,
            build,
        ))
        .await
        {
            Ok(()) => return,
            Err(failure) => failure,
        };
        match failure.error.is_retryable() {
            true => tracing::warn!(
                task_id = %task_id,
                child_workflow_id = %failure.failed_child_workflow_id,
                attempt,
                error = %failure.error,
                "parent advance failed (retryable): {}",
                failure.error,
            ),
            false => {
                tracing::error!(
                    task_id = %task_id,
                    child_workflow_id = %failure.failed_child_workflow_id,
                    owed_after = failure.owed_after.child_workflow_ids().len(),
                    error = %failure.error,
                    "parent advance failed (non-retryable): {} — workflow recovery will handle",
                    failure.error,
                );
                return;
            }
        }
        owed = failure.still_owed();
    }
    tracing::error!(
        task_id = %task_id,
        child_workflow_ids = ?owed.child_workflow_ids(),
        attempts = PHASE2_MAX_RETRIES,
        "parent advance retries exhausted — workflow recovery will handle",
    );
}

/// Compute phase-level retry delay with exponential backoff.
pub(crate) fn phase_retry_delay(attempt: u32) -> f64 {
    (PHASE_RETRY_BASE_DELAY_S * 2.0f64.powi(attempt as i32 - 1)).min(PHASE_RETRY_MAX_DELAY_S)
}

// ---------------------------------------------------------------------------
// Workflow context parsing (private helper)
// ---------------------------------------------------------------------------

mod parse {
    use std::collections::{BTreeMap, HashMap};

    use serde::de::IgnoredAny;
    use serde::{Deserialize, Serialize};
    use serde_json::value::RawValue;
    use uuid::Uuid;

    use crate::core::task::error::{OperationalErrorCode, TaskError, TaskErrorKind};
    use crate::core::task::result::TaskResult;
    use crate::core::workflow::SubWorkflowSummary;

    /// The context payload in Python's shape (`core/workflows/engine.py:1700-1721`), each result
    /// envelope as its text. `definition_key_by_id` is not read: the receiving task decodes each
    /// result with its own type.
    #[derive(Debug, Deserialize)]
    struct WorkflowCtxPayload {
        workflow_id: Uuid,
        task_index: i32,
        task_name: String,
        #[serde(default)]
        results_by_id: BTreeMap<String, Box<RawValue>>,
        #[serde(default)]
        task_name_by_id: HashMap<String, serde_json::Value>,
        #[serde(default)]
        summaries_by_id: HashMap<String, serde_json::Value>,
    }

    /// The `workflow_ctx` field that the task reads, in the form of the core's `WorkflowContext`
    /// (`crate::core::workflow::context`).
    #[derive(Serialize)]
    struct WorkflowCtxField {
        workflow_id: Uuid,
        task_index: i32,
        task_name: String,
        results_by_id: BTreeMap<String, ContextEntry>,
        summaries_by_id: BTreeMap<String, SubWorkflowSummary>,
    }

    /// One result of the context.
    #[derive(Serialize)]
    #[serde(untagged)]
    enum ContextEntry {
        /// The stored result envelope, as its text: each value keeps the text it was stored
        /// with, an integer outside the `i64` and `u64` range included.
        Stored(Box<RawValue>),
        /// Python's error entry in place of an envelope that the task cannot read.
        Unreadable(TaskResult<serde_json::Value>),
    }

    /// The text of the `workflow_ctx` field from the text of the context payload (Python
    /// `core/worker/child_runner.py:1438-1580`).
    ///
    /// - A result entry that reads as a result envelope is kept as its text. An entry that is
    ///   `null`, that has no task name, or that is not a result envelope becomes a
    ///   `RESULT_DESERIALIZATION_ERROR` entry naming the node; the other entries stay readable.
    /// - A summary entry is the stored summary text; one that does not parse is
    ///   `WORKER_SERIALIZATION_ERROR` for the whole task. An entry that is not text is skipped.
    /// - A payload of another shape is `WORKER_SERIALIZATION_ERROR`.
    ///
    /// Pure.
    pub(crate) fn workflow_ctx_text(payload_text: &str) -> Result<String, TaskError> {
        let payload: WorkflowCtxPayload =
            crate::core::codec::from_json_str(payload_text).map_err(|e| {
                TaskError::builtin(
                    OperationalErrorCode::WorkerSerializationError,
                    format!("failed to parse workflow context payload: {}", e),
                )
            })?;
        let WorkflowCtxPayload {
            workflow_id,
            task_index,
            task_name,
            results_by_id,
            task_name_by_id,
            summaries_by_id,
        } = payload;
        let results_by_id: BTreeMap<String, ContextEntry> = results_by_id
            .into_iter()
            .map(|(node_id, envelope)| {
                let entry = context_entry(&node_id, envelope, task_name_by_id.get(&node_id));
                (node_id, entry)
            })
            .collect();
        let field = WorkflowCtxField {
            workflow_id,
            task_index,
            task_name,
            results_by_id,
            summaries_by_id: context_summaries(summaries_by_id)?,
        };
        serde_json::to_string(&field).map_err(|e| {
            TaskError::builtin(
                OperationalErrorCode::WorkerSerializationError,
                format!("failed to serialize workflow context: {}", e),
            )
        })
    }

    /// The summaries of the context, from their stored texts. Pure.
    fn context_summaries(
        summaries_by_id: HashMap<String, serde_json::Value>,
    ) -> Result<BTreeMap<String, SubWorkflowSummary>, TaskError> {
        let mut summaries: BTreeMap<String, SubWorkflowSummary> = BTreeMap::new();
        for (node_id, summary_json) in summaries_by_id {
            let serde_json::Value::String(summary_text) = summary_json else {
                continue;
            };
            let parsed: SubWorkflowSummary = serde_json::from_str(&summary_text).map_err(|e| {
                TaskError::builtin(
                    OperationalErrorCode::WorkerSerializationError,
                    format!(
                        "failed to parse workflow context summary for node_id '{}': {}",
                        node_id, e,
                    ),
                )
            })?;
            summaries.insert(node_id, parsed);
        }
        Ok(summaries)
    }

    /// One result entry of the context, with Python's sentinel errors (`core/worker/child_runner.py:1457-1537`).
    /// An envelope that reads under the reader of [`TaskResult`] is kept as its text; the read keeps no value.
    fn context_entry(
        node_id: &str,
        envelope: Box<RawValue>,
        source_task_name: Option<&serde_json::Value>,
    ) -> ContextEntry {
        let unreadable = |message: String, data: serde_json::Value| -> ContextEntry {
            ContextEntry::Unreadable(TaskResult::Err(TaskError {
                exception: None,
                error_code: OperationalErrorCode::ResultDeserializationError.into(),
                data: Some(crate::core::codec::JsonText::of_engine_value(&data)),
                message: Some(message),
                kind: TaskErrorKind::Plain,
            }))
        };
        if envelope.get() == "null" {
            return unreadable(
                format!(
                    "workflow_ctx result for node '{node_id}' unavailable (source task not registered at encode time)"
                ),
                serde_json::json!({ "node_id": node_id }),
            );
        }
        let Some(serde_json::Value::String(source_task_name)) = source_task_name else {
            return unreadable(
                format!("workflow_ctx missing task_name for node '{node_id}'; cannot decode"),
                serde_json::json!({ "node_id": node_id }),
            );
        };
        let read = serde_json::from_str::<TaskResult<IgnoredAny>>(envelope.get());
        match read {
            Ok(_reads) => ContextEntry::Stored(envelope),
            Err(e) => unreadable(
                format!("decode_task_result failed for workflow_ctx node '{node_id}': {e}"),
                serde_json::json!({ "node_id": node_id, "source_task_name": source_task_name }),
            ),
        }
    }
}

#[cfg(test)]
mod result_wrap_tests {
    //! P6: the ok-result wrap embeds the task's result bytes verbatim via
    //! `&RawValue` — no intermediate `Value` tree, and no reformatting.

    use crate::core::task::result::TaskResult;

    #[test]
    fn raw_value_wrap_embeds_result_bytes_verbatim() {
        // A number form the old parse-to-Value path would have reformatted
        // ("1.2300" -> 1.23) must survive byte-for-byte inside the wrapper.
        let result_bytes: &[u8] = br#"{"amount": 1.2300, "id": 7}"#;
        let raw = serde_json::from_slice::<&serde_json::value::RawValue>(result_bytes)
            .expect("valid JSON");
        let wrapped_json = serde_json::to_string(&TaskResult::Ok(raw)).expect("wrap serializes");

        assert_eq!(
            wrapped_json,
            r#"{"__h_task_result__":true,"ok":{"amount": 1.2300, "id": 7},"err":null}"#,
            "result bytes must be embedded verbatim",
        );

        // And the wrapper still parses back through the typed reader.
        let reparsed: TaskResult<serde_json::Value> =
            serde_json::from_str(&wrapped_json).expect("wrapper round-trips");
        assert!(reparsed.is_ok());
    }

    #[test]
    fn invalid_result_bytes_are_rejected_like_the_value_parse() {
        // `&RawValue` parsing validates full JSON syntax, matching the previous
        // `from_slice::<Value>` gate: trailing garbage and truncation both fail.
        assert!(serde_json::from_slice::<&serde_json::value::RawValue>(b"{\"a\": 1} x").is_err());
        assert!(serde_json::from_slice::<&serde_json::value::RawValue>(b"{\"a\": ").is_err());
    }
}

#[cfg(test)]
mod task_input_tests {
    use super::build_task_input;
    use crate::core::workflow::context::WORKFLOW_CTX_KWARG;

    fn task_input(input: &str) -> String {
        let bytes = build_task_input(input, false).expect("task input built");
        String::from_utf8(bytes).expect("task input is UTF-8")
    }

    #[test]
    fn null_input_passes_as_null() {
        assert_eq!(task_input("null"), "null");
    }

    #[test]
    fn array_and_object_inputs_pass_as_written() {
        assert_eq!(task_input("[1, 2]"), "[1, 2]");
        assert_eq!(task_input(r#"{"a": 1}"#), r#"{"a": 1}"#);
        assert_eq!(task_input("{}"), "{}");
    }

    #[test]
    fn the_context_member_is_removed_for_a_task_without_context() {
        let input = format!(r#"{{"a":1,"{WORKFLOW_CTX_KWARG}":{{"ignored":true}}}}"#);
        let value: serde_json::Value = serde_json::from_str(&task_input(&input)).unwrap();
        assert_eq!(value, serde_json::json!({"a": 1}));
    }

    #[test]
    fn input_text_that_breaks_the_strict_read_rule_is_refused() {
        for input in ["{", "NaN", r#"{"a":1,"a":2}"#] {
            let err = build_task_input(input, false).unwrap_err();
            assert!(
                err.message
                    .unwrap_or_default()
                    .contains("stored input refused"),
                "{input}"
            );
        }
    }

    #[test]
    fn a_long_integer_passes_byte_for_byte() {
        let input = r#"{"k":123456789012345678901234567890}"#;
        assert_eq!(task_input(input), input);
    }

    fn refusal(input: &str) -> String {
        let err = build_task_input(input, false).unwrap_err();
        assert_eq!(err.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
        err.message.unwrap_or_default()
    }

    fn task_input_value(input: &str) -> serde_json::Value {
        serde_json::from_str(&task_input(input)).unwrap()
    }

    /// Python `core/worker/child_runner.py:1566-1612`: the receiving task gets the `inner` envelope of an
    /// `args_from` entry.
    #[test]
    fn an_args_from_entry_is_replaced_by_its_inner_envelope() {
        let input = r#"{"x": 1, "dep": {"__h_taskresult_envelope__": true, "source_task_name": "produce",
            "source_definition_key": null, "inner": {"__h_task_result__": true, "ok": 7, "err": null}}}"#;
        assert_eq!(
            task_input_value(input),
            serde_json::json!({
                "x": 1,
                "dep": {"__h_task_result__": true, "ok": 7, "err": null},
            }),
        );
    }

    #[test]
    fn a_malformed_args_from_entry_is_refused() {
        for entry in [
            r#"{"__h_taskresult_envelope__": true, "inner": null}"#,
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "p", "source_definition_key": 3, "inner": null}"#,
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "p", "source_definition_key": null}"#,
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "p", "source_definition_key": null, "inner": null, "x": 1}"#,
        ] {
            let message = refusal(&format!(r#"{{"dep": {entry}}}"#));
            assert!(
                message.contains("args_from envelope for kwarg 'dep'"),
                "{message}"
            );
        }
    }

    /// Python's child removes the metadata kwarg before the call (`core/worker/child_runner.py:1641-1642`).
    #[test]
    fn the_workflow_meta_kwarg_is_removed() {
        let input = r#"{"x": 1, "__h_workflow_meta__": {"workflow_id": "w", "task_index": 0, "task_name": "t"}}"#;
        assert_eq!(task_input_value(input), serde_json::json!({"x": 1}));
    }

    /// Python `decode_kwargs` and `decode_value`: an engine key in a user value, or as a kwarg name, is
    /// refused; so is a value deeper than 200 levels. A non-object input is one value under the same rules.
    #[test]
    fn user_input_follows_the_user_value_rules() {
        for input in [
            r#"{"x": {"__h_task_result__": true}}"#,
            r#"{"x": [{"__builtin_task_code__": "X"}]}"#,
            r#"{"__horsies_workflow_ctx__": {}}"#,
            r#"{"__h_other__": 1}"#,
            r#"[{"__h_x": 1}]"#,
        ] {
            assert!(refusal(input).contains("reserved key"), "{input}");
        }
        let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        let deep = format!(
            r#"{{"x": {}}}"#,
            nested(crate::core::codec::MAX_VALUE_JSON_DEPTH + 1)
        );
        assert!(refusal(&deep).contains("nested deeper than 200"));
        let at_bound = format!(
            r#"{{"x": {}}}"#,
            nested(crate::core::codec::MAX_VALUE_JSON_DEPTH)
        );
        assert_eq!(task_input(&at_bound), at_bound);
    }

    /// The context kwarg is Python's key; at check time (no runtime context) it is dropped.
    #[test]
    fn the_context_kwarg_is_pythons_key() {
        assert_eq!(WORKFLOW_CTX_KWARG, "__h_workflow_ctx__");
        let input = r#"{"x": 1, "__h_workflow_ctx__": {"workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 0, "task_name": "t"}}"#;
        assert_eq!(task_input_value(input), serde_json::json!({"x": 1}));
    }

    /// The stored result of `u128::MAX`.
    const U128_MAX_ENVELOPE: &str =
        r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#;
    /// The stored result of `i64::MIN - 1`.
    const BELOW_I64_ENVELOPE: &str =
        r#"{"__h_task_result__":true,"ok":-9223372036854775809,"err":null}"#;

    /// The task input of a task that takes the workflow context.
    fn task_input_with_context(input: &str) -> String {
        let bytes = build_task_input(input, true).expect("task input built");
        String::from_utf8(bytes).expect("task input is UTF-8")
    }

    /// Each `args_from` member is the text of its `inner` envelope as stored, so an integer
    /// outside the `i64` and `u64` range reaches the task (Python reads any integer:
    /// `loads_json`, `python/horsies/core/codec/json_io.py:97`).
    #[test]
    fn an_args_from_member_keeps_the_stored_text_of_its_envelope() {
        let wrap = |inner: &str| {
            format!(
                r#"{{"__h_taskresult_envelope__":true,"source_task_name":"produce","source_definition_key":null,"inner":{inner}}}"#
            )
        };
        let input = format!(
            r#"{{"x":1,"max":{},"below":{}}}"#,
            wrap(U128_MAX_ENVELOPE),
            wrap(BELOW_I64_ENVELOPE),
        );

        assert_eq!(
            task_input(&input),
            format!(r#"{{"x":1,"max":{U128_MAX_ENVELOPE},"below":{BELOW_I64_ENVELOPE}}}"#)
        );
    }

    /// Each result of the context keeps its stored envelope text.
    #[test]
    fn a_context_result_keeps_its_stored_text() {
        let input = format!(
            r#"{{"x":1,"__h_workflow_ctx__":{{"workflow_id":"00000000-0000-0000-0000-000000000000","task_index":2,"task_name":"consume","results_by_id":{{"max":{U128_MAX_ENVELOPE},"below":{BELOW_I64_ENVELOPE}}},"task_name_by_id":{{"max":"produce","below":"produce"}},"definition_key_by_id":{{"max":null,"below":null}},"summaries_by_id":{{}}}}}}"#
        );

        assert_eq!(
            task_input_with_context(&input),
            format!(
                r#"{{"x":1,"workflow_ctx":{{"workflow_id":"00000000-0000-0000-0000-000000000000","task_index":2,"task_name":"consume","results_by_id":{{"below":{BELOW_I64_ENVELOPE},"max":{U128_MAX_ENVELOPE}}},"summaries_by_id":{{}}}}}}"#
            )
        );
    }

    /// Beside the workflow meta, each other member keeps its stored text: an integer outside
    /// the `i64` and `u64` range and the notation of a float.
    #[test]
    fn a_member_beside_the_workflow_meta_keeps_its_stored_text() {
        let input = r#"{"max":340282366920938463463374607431768211455,"below":-9223372036854775809,"ratio":1e-05,"__h_workflow_meta__":{"workflow_id":"w","task_index":0,"task_name":"t"}}"#;

        assert_eq!(
            task_input(input),
            r#"{"max":340282366920938463463374607431768211455,"below":-9223372036854775809,"ratio":1e-05}"#
        );
    }

    /// The checks of the input read each member from its text: an integer past the `f64` range, which a
    /// `serde_json::Value` refuses, passes with and without an engine member, and an engine key is still refused.
    #[test]
    fn an_integer_of_any_width_passes_the_input_checks() {
        let long = "9".repeat(400);
        let plain = format!(r#"{{"long":{long},"max":340282366920938463463374607431768211455}}"#);
        assert_eq!(task_input(&plain), plain);
        assert_eq!(task_input(&format!("[{long}]")), format!("[{long}]"));
        let with_meta = format!(r#"{{"long":{long},"__h_workflow_meta__":{{"workflow_id":"w"}}}}"#);
        assert_eq!(task_input(&with_meta), format!(r#"{{"long":{long}}}"#));
        for refused in [
            format!(r#"{{"long":{long},"x":{{"__h_x":1}}}}"#),
            format!(r#"{{"long":{long},"x":{{"__h_x":1}},"__h_workflow_meta__":{{}}}}"#),
            r#"{"__h_other__":1}"#.to_owned(),
        ] {
            let error = build_task_input(&refused, false).unwrap_err();
            assert_eq!(
                error.error_code.to_string(),
                "WORKER_SERIALIZATION_ERROR",
                "{refused}"
            );
        }
    }

    /// The context takes the place of a member named `workflow_ctx`, as Python's
    /// `kwargs['workflow_ctx'] = ...`; without one, it comes last.
    #[test]
    fn the_context_takes_the_place_of_a_member_of_its_name() {
        let payload = r#"{"workflow_id":"00000000-0000-0000-0000-000000000000","task_index":0,"task_name":"t"}"#;
        let context = r#"{"workflow_id":"00000000-0000-0000-0000-000000000000","task_index":0,"task_name":"t","results_by_id":{},"summaries_by_id":{}}"#;
        for (input, expected) in [
            (
                format!(r#"{{"workflow_ctx":5,"y":2,"__h_workflow_ctx__":{payload}}}"#),
                format!(r#"{{"workflow_ctx":{context},"y":2}}"#),
            ),
            (
                format!(r#"{{"__h_workflow_ctx__":{payload},"y":2}}"#),
                format!(r#"{{"y":2,"workflow_ctx":{context}}}"#),
            ),
        ] {
            assert_eq!(task_input_with_context(&input), expected, "{input}");
        }
    }
}

#[cfg(test)]
mod workflow_ctx_tests {
    use super::workflow_ctx_text;
    use crate::core::codec::JsonText;
    use crate::core::task::error::TaskError;
    use crate::core::task::result::TaskResult;
    use crate::core::workflow::context::WorkflowContext;
    use crate::core::workflow::node::NodeKey;

    /// The context that the task reads from the `workflow_ctx` field built from `payload`.
    fn built_context(payload: &serde_json::Value) -> WorkflowContext {
        let text = workflow_ctx_text(&payload.to_string()).unwrap();
        serde_json::from_str(&text).unwrap()
    }

    fn refusal(payload: &serde_json::Value) -> TaskError {
        workflow_ctx_text(&payload.to_string()).unwrap_err()
    }

    /// Python's payload (`core/workflows/engine.py:1700-1721`): envelope objects or null, the task name and
    /// the definition key of each node, and the stored summary text.
    #[test]
    fn pythons_context_payload_reads() {
        let summary = r#"{"status":"COMPLETED","output":5,"total_tasks":1,"completed_tasks":1,"failed_tasks":0,"skipped_tasks":0,"error_summary":null}"#;
        let payload = serde_json::json!({
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 2,
            "task_name": "consume",
            "results_by_id": {
                "a": {"__h_task_result__": true, "ok": 41, "err": null},
                "b": null,
                "c": {"__h_task_result__": true, "ok": 1, "err": null},
                "d": {"not": "an envelope"},
                "sub": {"__h_task_result__": true, "ok": 5, "err": null},
            },
            "task_name_by_id": {"a": "produce", "b": "produce", "d": "produce", "sub": "__sub_workflow:child"},
            "definition_key_by_id": {"a": null, "b": null, "c": null, "d": null, "sub": "app.child"},
            "summaries_by_id": {"sub": summary},
        });
        let ctx = built_context(&payload);
        let value: TaskResult<i32> = ctx.result_for(&NodeKey::new("a".to_owned())).unwrap();
        assert_eq!(value.unwrap(), 41);
        for (node, message) in [
            ("b", "unavailable"),
            ("c", "missing task_name"),
            ("d", "decode_task_result failed"),
        ] {
            let entry: TaskResult<i32> = ctx.result_for(&NodeKey::new(node.to_owned())).unwrap();
            let error = entry.unwrap_err();
            assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
            assert!(error.message.unwrap().contains(message), "{node}");
        }
        let summary = ctx
            .summary_for(&NodeKey::<i32>::new("sub".to_owned()))
            .unwrap();
        assert_eq!(summary.output.as_ref().map(JsonText::get), Some("5"));
    }

    /// A summary output outside the `i64` and `u64` range keeps its stored text in the `workflow_ctx` field,
    /// and the task's context reads it as `u128` or `i128`.
    #[test]
    fn a_summary_output_keeps_its_stored_text() {
        let summary = |output: &str| {
            format!(
                r#"{{"status":"COMPLETED","output":{output},"total_tasks":1,"completed_tasks":1,"failed_tasks":0,"skipped_tasks":0,"error_summary":null}}"#
            )
        };
        let wide = "340282366920938463463374607431768211455";
        let below = "-9223372036854775809";
        let payload = serde_json::json!({
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 2,
            "task_name": "consume",
            "summaries_by_id": {"wide": summary(wide), "below": summary(below)},
        });
        let text = workflow_ctx_text(&payload.to_string()).unwrap();
        for output in [wide, below] {
            assert!(text.contains(&summary(output)), "{text}");
        }
        let ctx: WorkflowContext = serde_json::from_str(&text).unwrap();
        assert_eq!(
            ctx.output_for(&NodeKey::<u128>::new("wide".to_owned()))
                .unwrap(),
            Some(u128::MAX)
        );
        assert_eq!(
            ctx.output_for(&NodeKey::<i128>::new("below".to_owned()))
                .unwrap(),
            Some(i128::from(i64::MIN) - 1)
        );
    }

    #[test]
    fn a_summary_text_that_does_not_parse_fails_the_task() {
        let payload = serde_json::json!({
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "task_index": 0,
            "task_name": "t",
            "summaries_by_id": {"sub": "{not json"},
        });
        let error = refusal(&payload);
        assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
    }

    /// A payload that is not Python's shape fails the task.
    #[test]
    fn a_payload_of_another_shape_fails_the_task() {
        for payload in [
            serde_json::json!([1]),
            serde_json::json!({"task_index": 0, "task_name": "t"}),
        ] {
            let error = refusal(&payload);
            assert_eq!(
                error.error_code.to_string(),
                "WORKER_SERIALIZATION_ERROR",
                "{payload}"
            );
            assert!(
                error
                    .message
                    .unwrap_or_default()
                    .contains("failed to parse workflow context payload"),
                "{payload}"
            );
        }
    }

    /// Each result keeps its stored envelope text, so an integer outside the `i64` and `u64`
    /// range reaches the task as stored (Python `loads_json`, `core/codec/json_io.py:97`).
    #[test]
    fn each_result_keeps_its_stored_envelope_text() {
        let max =
            r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#;
        let below = r#"{"__h_task_result__":true,"ok":-9223372036854775809,"err":null}"#;
        let payload = format!(
            r#"{{"workflow_id":"00000000-0000-0000-0000-000000000000","task_index":2,"task_name":"consume","results_by_id":{{"max":{max},"below":{below}}},"task_name_by_id":{{"max":"produce","below":"produce"}},"definition_key_by_id":{{"max":null,"below":null}},"summaries_by_id":{{}}}}"#
        );

        let text = workflow_ctx_text(&payload).unwrap();

        assert_eq!(
            text,
            format!(
                r#"{{"workflow_id":"00000000-0000-0000-0000-000000000000","task_index":2,"task_name":"consume","results_by_id":{{"below":{below},"max":{max}}},"summaries_by_id":{{}}}}"#
            )
        );
    }
}

#[cfg(test)]
#[path = "finalizing_tests.rs"]
mod finalizing_tests;

#[cfg(test)]
mod finalize_spec_tests;
