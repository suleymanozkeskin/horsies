//! The Python surface of the extension module `horsies._core`, as data.
//!
//! The classes and functions here are defined with PyO3 in
//! `crates/horsies-python`. PyO3 attribute values must be literals, so this
//! table repeats their names and signatures. A change to the extension
//! changes this table in the same commit; a Python test compares the
//! generated stub with the built module. Where a value is a catalog code,
//! the type comes from the core catalog itself.

use horsies_core::catalog::{CodeCatalog, CodeEnum};
use horsies_core::{BrokerErrorCode, TaskAttemptOutcome, TaskStatus};

/// The type text of an annotation.
#[derive(Clone, Copy, Debug)]
pub enum TypeText {
    /// Python type text, written as it is.
    Plain(&'static str),
    /// `Literal[...]` of every wire value of a catalog.
    WireValuesOf(CodeCatalog),
    /// `asyncio.Future[...]` of the type text.
    FutureOf(&'static str),
}

/// An exception class.
#[derive(Clone, Copy, Debug)]
pub struct StubException {
    pub name: &'static str,
    pub base: &'static str,
    pub doc: &'static str,
}

/// How a parameter is passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParameterKind {
    Positional,
    /// After `*`: by keyword only.
    KeywordOnly,
}

/// A parameter of a function or method, after `self`. Keyword-only
/// parameters come after every positional one.
#[derive(Clone, Copy, Debug)]
pub struct StubParameter {
    pub name: &'static str,
    pub annotation: TypeText,
    pub kind: ParameterKind,
}

/// A function, or a method that takes `self`.
#[derive(Clone, Copy, Debug)]
pub struct StubFunction {
    pub name: &'static str,
    pub parameters: &'static [StubParameter],
    pub returns: TypeText,
    pub doc: &'static str,
}

/// A read-only attribute of a class.
#[derive(Clone, Copy, Debug)]
pub struct StubProperty {
    pub name: &'static str,
    pub annotation: TypeText,
}

/// Whether Python can build a class.
#[derive(Clone, Copy, Debug)]
pub enum Constructor {
    Parameters(&'static [StubParameter]),
    NoConstructor,
}

/// A final class.
#[derive(Clone, Copy, Debug)]
pub struct StubClass {
    pub name: &'static str,
    pub doc: &'static str,
    pub constructor: Constructor,
    pub properties: &'static [StubProperty],
    pub methods: &'static [StubFunction],
}

/// The whole module.
#[derive(Clone, Copy, Debug)]
pub struct ModuleSurface {
    pub exceptions: &'static [StubException],
    pub classes: &'static [StubClass],
    pub functions: &'static [StubFunction],
}

const fn positional(name: &'static str, annotation: &'static str) -> StubParameter {
    StubParameter {
        name,
        annotation: TypeText::Plain(annotation),
        kind: ParameterKind::Positional,
    }
}

const fn keyword(name: &'static str, annotation: &'static str) -> StubParameter {
    StubParameter {
        name,
        annotation: TypeText::Plain(annotation),
        kind: ParameterKind::KeywordOnly,
    }
}

const fn property(name: &'static str, annotation: &'static str) -> StubProperty {
    StubProperty {
        name,
        annotation: TypeText::Plain(annotation),
    }
}

const START_AFTER_DELAY_PARAMETERS: &[StubParameter] = &[positional("seconds", "int")];

const START_AT_PARAMETERS: &[StubParameter] = &[positional("at", "datetime.datetime")];

const CORE_HANDLE_PARAMETERS: &[StubParameter] = &[
    positional("manifest_json", "str"),
    keyword("pool_profile", "Literal['PRODUCER', 'WORKER']"),
];

const SET_PANIC_RULE_PARAMETERS: &[StubParameter] =
    &[positional("rule", "Literal['STOP_CORE', 'ABORT_PROCESS']")];

const ENQUEUE_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("task_id", "uuid.UUID"),
    keyword("task_name", "str"),
    keyword("queue_name", "str"),
    keyword("priority", "int"),
    keyword("input_json", "str"),
    keyword("sent_at", "datetime.datetime"),
    keyword("start", "StartNow | StartAfterDelay | StartAt"),
    keyword("good_until", "datetime.datetime | None"),
    keyword("task_options_json", "str | None"),
    keyword("idempotency_key", "str | None"),
    keyword("retention_class_key", "str"),
    keyword("rerun_input", "Literal['APP_DEFAULT', 'KEEP', 'DROP']"),
    keyword("send_kind", "Literal['FIRST', 'REPEAT']"),
];

const ENQUEUE_PARAMETERS: &[StubParameter] = &[positional("request", "EnqueueRequest")];
const RELEASE_CLAIMS_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("worker_id", "str"),
    keyword("claims", "list[tuple[uuid.UUID, datetime.datetime]]"),
    keyword("placement", "Literal['SAME_CLASS', 'ISOLATED']"),
];
const RELEASE_CLAIMS_PARAMETERS: &[StubParameter] =
    &[positional("request", "ReleaseClaimsRequest")];
const REQUEUE_STALE_CLAIMED_PARAMETERS: &[StubParameter] = &[positional("stale_after_ms", "int")];
const NO_RETRY_PARAMETERS: &[StubParameter] = &[];
const RETRY_AFTER_PARAMETERS: &[StubParameter] = &[
    keyword("expected_retry_count", "int"),
    keyword("delay_s", "float"),
    keyword("placement", "Literal['SAME_CLASS', 'ISOLATED']"),
];
const FINISH_FAILED_ATTEMPT_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
    keyword("claimed_at", "datetime.datetime"),
    keyword("retry", "NoRetry | RetryAfter"),
    keyword("result_json", "str"),
    keyword("error_code", "str | None"),
    keyword("failed_reason", "str | None"),
    keyword("attempt_outcome", "Literal['FAILED', 'WORKER_FAILURE']"),
    keyword("attempt_error_message", "str | None"),
    keyword("attempt_failed_reason", "str | None"),
];
const FINISH_FAILED_ATTEMPT_PARAMETERS: &[StubParameter] =
    &[positional("request", "FinishFailedAttemptRequest")];
const SUCCESS_BATCH_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("worker_id", "str"),
    keyword("rows", "list[tuple[uuid.UUID, datetime.datetime, str]]"),
];
const COMPLETE_SUCCESSES_PARAMETERS: &[StubParameter] =
    &[positional("request", "SuccessBatchRequest")];
const FINISH_WORKFLOW_SUCCESS_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
    keyword("claimed_at", "datetime.datetime"),
    keyword("result_json", "str"),
];
const CLAIM_PASS_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("worker_id", "str"),
    keyword("queues", "list[str]"),
    keyword("queue_priority", "dict[str, int]"),
    keyword("queue_max_concurrency", "dict[str, int]"),
    keyword("hard_cap_mode", "bool"),
    keyword(
        "budgets",
        "dict[Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'], int]",
    ),
    keyword(
        "routes",
        "dict[Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'], \
         Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']]",
    ),
    keyword("prefetch_buffer", "int"),
    keyword("max_claim_per_worker", "int"),
    keyword("max_claim_batch", "int"),
    keyword("cluster_wide_cap", "int | None"),
    keyword("claim_lease_ms", "int"),
    keyword("lock_keys", "list[int]"),
];
const CLAIM_PASS_PARAMETERS: &[StubParameter] = &[positional("request", "ClaimPassRequest")];
const QUEUE_STARTS_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("worker_id", "str"),
    keyword("queues", "list[str]"),
    keyword("started", "list[tuple[uuid.UUID, datetime.datetime]]"),
];
const COUNT_QUEUE_STARTS_PARAMETERS: &[StubParameter] =
    &[positional("request", "QueueStartsRequest")];
const MARK_STALE_RUNNING_PARAMETERS: &[StubParameter] = &[
    positional("stale_after_ms", "int"),
    positional("finalizing_stale_after_ms", "int"),
];
const CANCEL_TASK_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("include_running", "bool"),
];
const WORKFLOW_ID_PARAMETERS: &[StubParameter] = &[positional("workflow_id", "uuid.UUID")];
const DELETE_EXPIRED_WORKFLOWS_PARAMETERS: &[StubParameter] = &[
    positional("older_than_hours", "int"),
    positional("batch_size", "int"),
    positional("budget_ms", "int"),
];
const RAW_RESULT_RECORD_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("timeout_ms", "int | None"),
];
const TASK_INFO_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("include_result", "bool"),
    keyword("include_failed_reason", "bool"),
    keyword("include_attempts", "bool"),
];

const CORE_CHILD_HANDLE_PARAMETERS: &[StubParameter] = &[
    positional("manifest_json", "str"),
    keyword("kept_open", "int"),
    keyword("check_at_checkout", "bool"),
];
const START_ATTEMPT_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
    keyword("claimed_at", "datetime.datetime"),
    keyword("worker_pid", "int"),
    keyword("worker_hostname", "str"),
    keyword("deadline_in_ms", "int | None"),
];
const MARK_FINALIZING_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
    keyword("claimed_at", "datetime.datetime"),
];
const EXPIRE_OWNED_CLAIM_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
];
const CANCEL_OWNED_ORPHAN_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
    keyword("claimed_at", "datetime.datetime"),
];

const RESOLVE_MONITORING_WINDOW_PARAMETERS: &[StubParameter] = &[
    keyword("since", "datetime.datetime | None"),
    keyword("until", "datetime.datetime | None"),
    keyword("now", "datetime.datetime | None"),
];

const CORE_SCHEDULER_PARAMETERS: &[StubParameter] = &[
    positional("handle", "CoreHandle"),
    positional("manifest_json", "str"),
];

const PING_OUTCOME: &str = "DatabasePingRecord | BrokerFailure";
const INSTALL_OUTCOME: &str = "SchemaInstalled | SchemaInstallNotRun | BrokerFailure";
const SEND_OUTCOME: &str = "Enqueued | SendSuppressed | SendValidationFailed | \
     SendEnqueueFailed | SendPayloadMismatch | SendPayloadTooLarge | SendKeyConflict";
const TASK_INFO_OUTCOME: &str = "TaskInfoRecord | TaskInfoNotFound | BrokerFailure";
const RAW_RESULT_OUTCOME: &str = "RawResultRecordRecord | RawResultAbsent | BrokerFailure";
const REQUEUE_STALE_CLAIMED_OUTCOME: &str =
    "StaleClaimsDrained | StaleClaimsBoundReached | StaleClaimsRequeueFailed";
const RELEASE_CLAIMS_OUTCOME: &str = "ClaimsReleased | ReleaseFailed";
const FINISH_FAILED_ATTEMPT_OUTCOME: &str = "AttemptFailureApplied | AttemptRetryScheduled | \
     AttemptAlreadyApplied | AttemptLostClaim | AttemptSourceStateConflict | AttemptTaskAbsent | \
     AttemptWriteFailed";
const FINISH_ENDED_ATTEMPT_OUTCOME: &str = "AttemptClaimReleased | AttemptFailureApplied | \
     AttemptRetryScheduled | AttemptAlreadyApplied | AttemptLostClaim | \
     AttemptSourceStateConflict | AttemptTaskAbsent | AttemptWriteFailed";
const COMPLETE_SUCCESSES_OUTCOME: &str =
    "SuccessesWritten | BatchNotSent | BatchRefused | BatchCommitUnknown";
const FINISH_WORKFLOW_SUCCESS_OUTCOME: &str = "WorkflowSuccessApplied | AttemptAlreadyApplied | \
     AttemptLostClaim | AttemptSourceStateConflict | AttemptTaskAbsent | AttemptWriteFailed";
const GUARD_TEXT: &str = "Literal['CLAIM', 'DEADLINE', 'STALENESS', 'RETRY_COUNT', \
     'WORKFLOW_STATUS', 'WORKFLOW_LINK_ABSENT', 'WORKFLOW_LINK_STATE', \
     'FOREIGN_TERMINALIZATION', 'WORKFLOW_TASK']";
const MARK_STALE_RUNNING_OUTCOME: &str =
    "StaleRunningRecovered | StaleRunningScanFull | StaleRunningScanFailed";
const CANCEL_TASK_OUTCOME: &str = "TaskCancelledRecord | CancelRefusedWorkflowTask | \
     CancelRefusedStatus | CancelTaskAbsent | CancelFailed | CancelStoredDataInvalid";
const EXPIRE_PENDING_TASKS_OUTCOME: &str =
    "PendingExpiryDrained | PendingExpiryBoundReached | PendingExpiryFailed";
const AUDIT_ORPHANED_WORKFLOW_TASKS_OUTCOME: &str =
    "OrphanPageAudited | OrphanAuditLockBusy | OrphanAuditFailed";
const TASK_ROW_END_OUTCOME: &str = "TaskRowEnded | AttemptAlreadyApplied | AttemptLostClaim | \
     AttemptSourceStateConflict | AttemptTaskAbsent | AttemptWriteFailed";

const PAUSE_WORKFLOW_OUTCOME: &str =
    "WorkflowPaused | WorkflowNotRunning | WorkflowAbsent | HandleFailed";
const CANCEL_WORKFLOW_OUTCOME: &str =
    "WorkflowCancelled | WorkflowNotCancellable | WorkflowAbsent | HandleFailed";

const START_ATTEMPT_OUTCOME: &str =
    "AttemptStarted | AttemptMissed | DeadlineBeforeStart | StartFailed";
const MARK_FINALIZING_OUTCOME: &str = "FinalizingMarked | FinalizingNotHeld | MarkFinalizingFailed";

const SCHEDULER_START_OUTCOME: &str =
    "SchedulerStarted | SchedulerStartRefused | SchedulerStartFailed";
const SCHEDULER_TICK_OUTCOME: &str = "SchedulerTicked | SchedulerTickFailed";
const SCHEDULER_RUN_OUTCOME: &str =
    "SchedulerStopped | SchedulerStartRefused | SchedulerStartFailed";

const CORE_SCHEDULER_METHODS: [StubFunction; 4] = [
    StubFunction {
        name: "start_async",
        parameters: &[],
        returns: TypeText::FutureOf(SCHEDULER_START_OUTCOME),
        doc: "Connect, install the schema once per handle, and start the scheduler: the start checks,\nthe client family check and the state start. Writes. A start that completed before\nanswers SchedulerStarted at once.",
    },
    StubFunction {
        name: "tick_async",
        parameters: &[],
        returns: TypeText::FutureOf(SCHEDULER_TICK_OUTCOME),
        doc: "One tick: the due read, the existence heal on its cadence, one fire per due schedule.\nWrites. Raises RuntimeError before a completed start and after the end of a run.",
    },
    StubFunction {
        name: "run_async",
        parameters: &[],
        returns: TypeText::FutureOf(SCHEDULER_RUN_OUTCOME),
        doc: "The start when none completed, then a tick at once and one per check interval until a\nstop request; with nothing to fire, the wait for the stop request. A cancel of the future\nasks for the stop.",
    },
    StubFunction {
        name: "request_stop",
        parameters: &[],
        returns: TypeText::Plain("None"),
        doc: "Ask the run to stop at its next tick boundary; a fire in flight runs to its end.\nIdempotent; returns at once.",
    },
];

const CORE_CHILD_HANDLE_METHODS: [StubFunction; 6] = [
    StubFunction {
        name: "start_attempt",
        parameters: START_ATTEMPT_PARAMETERS,
        returns: TypeText::Plain(START_ATTEMPT_OUTCOME),
        doc: "Start one claimed task. Writes, in one statement. deadline_in_ms is the time left before\nthe deadline of the run, or None: when it has passed by the time the pool gives the\nconnection, no statement runs. A naive claimed_at raises TypeError.",
    },
    StubFunction {
        name: "start_attempt_async",
        parameters: START_ATTEMPT_PARAMETERS,
        returns: TypeText::FutureOf(START_ATTEMPT_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "expire_owned_claim",
        parameters: EXPIRE_OWNED_CLAIM_PARAMETERS,
        returns: TypeText::Plain(TASK_ROW_END_OUTCOME),
        doc: "Expire one claimed row of this worker whose deadline passed before its start. Writes,\nin one statement.",
    },
    StubFunction {
        name: "expire_owned_claim_async",
        parameters: EXPIRE_OWNED_CLAIM_PARAMETERS,
        returns: TypeText::FutureOf(TASK_ROW_END_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "mark_finalizing",
        parameters: MARK_FINALIZING_PARAMETERS,
        returns: TypeText::Plain(MARK_FINALIZING_OUTCOME),
        doc: "Write the finalizing marker of a run whose body ended. Writes, in one statement, with\nthe pool's bounds. A naive claimed_at raises TypeError.",
    },
    StubFunction {
        name: "mark_finalizing_async",
        parameters: MARK_FINALIZING_PARAMETERS,
        returns: TypeText::FutureOf(MARK_FINALIZING_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
];

const CORE_HANDLE_METHODS: [StubFunction; 30] = [
    StubFunction {
        name: "close",
        parameters: &[],
        returns: TypeText::Plain("HandleClosed"),
        doc: "Close the handle's core pools. Writes in flight end within the close bound; the ones that\ndo not are logged with their count. A handle that never connected starts nothing.",
    },
    StubFunction {
        name: "close_async",
        parameters: &[],
        returns: TypeText::FutureOf("HandleClosed"),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "ping_database",
        parameters: &[],
        returns: TypeText::Plain(PING_OUTCOME),
        doc: "Ping the database.",
    },
    StubFunction {
        name: "ping_database_async",
        parameters: &[],
        returns: TypeText::FutureOf(PING_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "install_schema",
        parameters: &[],
        returns: TypeText::Plain(INSTALL_OUTCOME),
        doc: "Apply the embedded migrations and check the schema version, once per handle. Writes.",
    },
    StubFunction {
        name: "install_schema_async",
        parameters: &[],
        returns: TypeText::FutureOf(INSTALL_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "schema_status",
        parameters: &[],
        returns: TypeText::Plain("SchemaStatusRecord"),
        doc: "The schema state of the database. Reads only.",
    },
    StubFunction {
        name: "schema_status_async",
        parameters: &[],
        returns: TypeText::FutureOf("SchemaStatusRecord"),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "enqueue",
        parameters: ENQUEUE_PARAMETERS,
        returns: TypeText::Plain(SEND_OUTCOME),
        doc: "Enqueue one task, in one statement. Installs the schema once per handle first. Writes; a\nresend with send_kind REPEAT is safe.",
    },
    StubFunction {
        name: "enqueue_async",
        parameters: ENQUEUE_PARAMETERS,
        returns: TypeText::FutureOf(SEND_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "requeue_stale_claimed_async",
        parameters: REQUEUE_STALE_CLAIMED_PARAMETERS,
        returns: TypeText::FutureOf(REQUEUE_STALE_CLAIMED_OUTCOME),
        doc: "One stale CLAIMED requeue pass. Writes; each batch commits alone. A threshold that\ndoes not fit in 64 bits raises OverflowError.",
    },
    StubFunction {
        name: "mark_stale_running_async",
        parameters: MARK_STALE_RUNNING_PARAMETERS,
        returns: TypeText::FutureOf(MARK_STALE_RUNNING_OUTCOME),
        doc: "One stale RUNNING recovery pass. Writes; each candidate commits alone. A threshold\nthat does not fit in 64 bits raises OverflowError.",
    },
    StubFunction {
        name: "finish_failed_attempt_async",
        parameters: FINISH_FAILED_ATTEMPT_PARAMETERS,
        returns: TypeText::FutureOf(FINISH_FAILED_ATTEMPT_OUTCOME),
        doc: "Write the failed attempt of a child that reported a result (claimed-row rule REFUSE).\nWrites, in one statement.",
    },
    StubFunction {
        name: "finish_ended_attempt_async",
        parameters: FINISH_FAILED_ATTEMPT_PARAMETERS,
        returns: TypeText::FutureOf(FINISH_ENDED_ATTEMPT_OUTCOME),
        doc: "Write the end of an attempt whose child ended without a result (claimed-row rule\nRELEASE). Writes, in one statement.",
    },
    StubFunction {
        name: "complete_successes_async",
        parameters: COMPLETE_SUCCESSES_PARAMETERS,
        returns: TypeText::FutureOf(COMPLETE_SUCCESSES_OUTCOME),
        doc: "Write the plain successes of one request in one statement. Writes. Rows outside the\nfunction's contract raise PanicException.",
    },
    StubFunction {
        name: "finish_workflow_success_async",
        parameters: FINISH_WORKFLOW_SUCCESS_PARAMETERS,
        returns: TypeText::FutureOf(FINISH_WORKFLOW_SUCCESS_OUTCOME),
        doc: "Write phase 1 of a workflow task's success. Writes, in one statement. A naive\nclaimed_at raises TypeError.",
    },
    StubFunction {
        name: "claim_pass_async",
        parameters: CLAIM_PASS_PARAMETERS,
        returns: TypeText::FutureOf("ClaimPassRecord | ClaimPassFailed"),
        doc: "One claim pass. Writes, in one statement; the dispatch stays with the caller.",
    },
    StubFunction {
        name: "count_queue_starts_async",
        parameters: COUNT_QUEUE_STARTS_PARAMETERS,
        returns: TypeText::FutureOf("QueueStartsRecord | QueueStartsFailed"),
        doc: "Count the capped queues of one soft-cap start check. Reads only, in one statement; a\ncheck with no queue sends nothing.",
    },
    StubFunction {
        name: "release_claims_async",
        parameters: RELEASE_CLAIMS_PARAMETERS,
        returns: TypeText::FutureOf(RELEASE_CLAIMS_OUTCOME),
        doc: "Release the claims of one request. Writes, in one statement.",
    },
    StubFunction {
        name: "cancel_task_async",
        parameters: CANCEL_TASK_PARAMETERS,
        returns: TypeText::FutureOf(CANCEL_TASK_OUTCOME),
        doc: "Cancel one plain task; a RUNNING task only with include_running. Writes, in one\nstatement. An outcome that contradicts the function raises PanicException.",
    },
    StubFunction {
        name: "expire_pending_tasks_async",
        parameters: &[],
        returns: TypeText::FutureOf(EXPIRE_PENDING_TASKS_OUTCOME),
        doc: "One pending expiry pass. Writes; each batch commits alone.",
    },
    StubFunction {
        name: "audit_orphaned_workflow_tasks_async",
        parameters: &[],
        returns: TypeText::FutureOf(AUDIT_ORPHANED_WORKFLOW_TASKS_OUTCOME),
        doc: "One page of the orphan sweep and its cursor counts, in one transaction. Writes.",
    },
    StubFunction {
        name: "cancel_owned_orphan_async",
        parameters: CANCEL_OWNED_ORPHAN_PARAMETERS,
        returns: TypeText::FutureOf(TASK_ROW_END_OUTCOME),
        doc: "Cancel one claimed workflow task that this claim holds and that no runnable node\nlinks. Writes, in one statement. A naive claimed_at raises TypeError.",
    },
    StubFunction {
        name: "pause_workflow_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(PAUSE_WORKFLOW_OUTCOME),
        doc: "Pause one workflow tree. Writes, in one statement. A row outside the function's contract\nraises PanicException.",
    },
    StubFunction {
        name: "cancel_workflow_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(CANCEL_WORKFLOW_OUTCOME),
        doc: "Cancel one workflow tree. Writes, in one statement. A row outside the function's contract\nraises PanicException.",
    },
    StubFunction {
        name: "delete_expired_workflows_async",
        parameters: DELETE_EXPIRED_WORKFLOWS_PARAMETERS,
        returns: TypeText::FutureOf("WorkflowsDeleted | WorkflowDeleteFailed"),
        doc: "One workflow retention pass of the reaper, in batches until one deletes nothing or the\nbudget has passed. Writes; each batch commits alone. A negative value raises\nOverflowError.",
    },
    StubFunction {
        name: "task_info",
        parameters: TASK_INFO_PARAMETERS,
        returns: TypeText::Plain(TASK_INFO_OUTCOME),
        doc: "The info of one task. Installs the schema once per handle first; then reads only.",
    },
    StubFunction {
        name: "task_info_async",
        parameters: TASK_INFO_PARAMETERS,
        returns: TypeText::FutureOf(TASK_INFO_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "raw_result_record",
        parameters: RAW_RESULT_RECORD_PARAMETERS,
        returns: TypeText::Plain(RAW_RESULT_OUTCOME),
        doc: "The result of one task, waiting up to timeout_ms (no limit for None) while it has not\nfinished. Installs the schema once per handle first; then reads only. Bounded by the limit\nplus the bound of a blocking call; not bounded with no limit.",
    },
    StubFunction {
        name: "raw_result_record_async",
        parameters: RAW_RESULT_RECORD_PARAMETERS,
        returns: TypeText::FutureOf(RAW_RESULT_OUTCOME),
        doc: "The awaitable form of the method above. A cancelled await drops the wait.",
    },
];

const EXCEPTIONS: &[StubException] = &[
    StubException {
        name: "CoreError",
        base: "Exception",
        doc: "Base of the errors raised at the core boundary.",
    },
    StubException {
        name: "ForkedAfterCoreStart",
        base: "CoreError",
        doc: "This process was forked without exec after the core started in its parent.\n\nargs: (core_pid, this_pid, advice).",
    },
    StubException {
        name: "CoreStopped",
        base: "CoreError",
        doc: "A core background task panicked, so the core is stopped in this process.\n\nargs: (subject, cause) of the first panic.",
    },
    StubException {
        name: "CoreStartFailed",
        base: "CoreError",
        doc: "The core runtime or its log forwarder could not start. args: (cause,).",
    },
    StubException {
        name: "CoreCallTimedOut",
        base: "CoreError",
        doc: "A blocking call did not end within its bound.\n\nargs: (subject, bound_seconds, state_after); state_after is 'no_change'\nfor a read and 'outcome_unknown' for a write.",
    },
    StubException {
        name: "TooManyPendingCalls",
        base: "CoreError",
        doc: "The event loop already has the most core calls in flight. args: (subject, bound).",
    },
    StubException {
        name: "CoreDefect",
        base: "BaseException",
        doc: "The client sent a value that the core refuses: a defect of the client.",
    },
    StubException {
        name: "ManifestRefused",
        base: "CoreDefect",
        doc: "The core refused a manifest. args: (field_path, cause).",
    },
    StubException {
        name: "PanicRuleFixed",
        base: "CoreDefect",
        doc: "The panic rule of this process was set to another rule before, or fixed when the core\nstarted. args: (current_rule,).",
    },
    StubException {
        name: "NotMainThread",
        base: "CoreDefect",
        doc: "A run form was called off the main thread, where Python runs signal handlers.\n\nThe run did not start. args: (subject,).",
    },
    StubException {
        name: "PanicException",
        base: "BaseException",
        doc: "Rust code called from Python panicked.",
    },
];

const ARGUMENT_CLASSES: [StubClass; 11] = [
    StubClass {
        name: "StartNow",
        doc: "Start the task as soon as a worker can claim it.",
        constructor: Constructor::Parameters(&[]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "StartAfterDelay",
        doc: "Start the task `seconds` after the enqueue.",
        constructor: Constructor::Parameters(START_AFTER_DELAY_PARAMETERS),
        properties: &[property("seconds", "int")],
        methods: &[],
    },
    StubClass {
        name: "StartAt",
        doc: "Start the task at an instant. A naive datetime is refused.",
        constructor: Constructor::Parameters(START_AT_PARAMETERS),
        properties: &[property("at", "datetime.datetime")],
        methods: &[],
    },
    StubClass {
        name: "EnqueueRequest",
        doc: "One frozen send. None for good_until, task_options_json or idempotency_key\nmeans no deadline, no options, no key. retention_class_key is 'forever' or\n'standard_<days>d'; the core checks it at the send. send_kind is FIRST for the first send\nof the task id and REPEAT for a send retry: a task that finished then answers from history.",
        constructor: Constructor::Parameters(ENQUEUE_REQUEST_PARAMETERS),
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "NoRetry",
        doc: "The failed attempt writes the terminal failure.",
        constructor: Constructor::Parameters(NO_RETRY_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "RetryAfter",
        doc: "The failed attempt writes the retry, unless its next attempt would start at or after\ngood_until. expected_retry_count is part of the fence. Another placement raises ValueError.",
        constructor: Constructor::Parameters(RETRY_AFTER_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "FinishFailedAttemptRequest",
        doc: "One frozen failed attempt of a RUNNING task. A naive claimed_at raises TypeError; another\nattempt_outcome raises ValueError.",
        constructor: Constructor::Parameters(FINISH_FAILED_ATTEMPT_REQUEST_PARAMETERS),
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "SuccessBatchRequest",
        doc: "The plain successes of one worker for one statement: 1 to 64 rows, each task once. A\nnaive claimed_at raises TypeError; no row, more rows or a repeated task id raises\nValueError.",
        constructor: Constructor::Parameters(SUCCESS_BATCH_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ClaimPassRequest",
        doc: "One frozen claim pass. A negative count or one above 32 bits raises OverflowError; an\nunknown class name, a route to a budget the pass does not hold or a lease of 0 raises\nValueError.",
        constructor: Constructor::Parameters(CLAIM_PASS_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "QueueStartsRequest",
        doc: "One frozen soft-cap start check: the capped queues that have held rows, and the started\nclaims (task id, claim generation) whose outcome write has not ended. A naive datetime\nraises TypeError.",
        constructor: Constructor::Parameters(QUEUE_STARTS_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ReleaseClaimsRequest",
        doc: "One frozen release: the claims of one worker, at most 1000, each task once, each with\nthe claim generation of its dispatch. A naive datetime raises TypeError; more claims, a\nrepeated task id or another placement raises ValueError.",
        constructor: Constructor::Parameters(RELEASE_CLAIMS_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
];

const RECORD_CLASSES: [StubClass; 78] = [
    StubClass {
        name: "BrokerFailure",
        doc: "An expected failure of a broker operation.",
        constructor: Constructor::NoConstructor,
        properties: &[
            StubProperty {
                name: "code",
                annotation: TypeText::WireValuesOf(<BrokerErrorCode as CodeEnum>::CATALOG),
            },
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "DatabasePingRecord",
        doc: "The round trip of one database ping.",
        constructor: Constructor::NoConstructor,
        properties: &[property("latency_ms", "float")],
        methods: &[],
    },
    StubClass {
        name: "HandleClosed",
        doc: "The close of a handle ended: its core pools are closed, or it never connected. The next\noperation on the handle opens new core pools.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "IdMintRefused",
        doc: "The core's UUIDv7 source refused to mint a task id.",
        constructor: Constructor::NoConstructor,
        properties: &[property("cause", "str")],
        methods: &[],
    },
    StubClass {
        name: "SchemaInstalled",
        doc: "The migrations ran, and the database is at version.",
        constructor: Constructor::NoConstructor,
        properties: &[property("version", "int")],
        methods: &[],
    },
    StubClass {
        name: "SchemaInstallNotRun",
        doc: "The handle was built with the OBSERVE_ONLY or the ASSUME_INITIALIZED schema mode, so the install ran nothing.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "SchemaStatusRecord",
        doc: "The schema state of the database.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property(
                "state",
                "Literal['MATCH', 'MISMATCH', 'ABSENT', 'ZERO_X', 'UNKNOWN']",
            ),
            property("version", "int | None"),
            property("expected_version", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "Enqueued",
        doc: "The task is stored, or it already was under the same command.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "SendSuppressed",
        doc: "Sends are suppressed. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str")],
        methods: &[],
    },
    StubClass {
        name: "SendValidationFailed",
        doc: "The core refused the send before any statement. Nothing was written. cause_text is the\nJSON reader's error when the refused text does not read.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("code", "Literal['INVALID_INPUT', 'INVALID_TASK_OPTIONS']"),
            property("message", "str"),
            property("cause_text", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "SendEnqueueFailed",
        doc: "The enqueue failed; the row may or may not exist. A retention class key that no class\nderives is this record too, built before any statement: nothing was written, and\nretryable is False.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "SendPayloadMismatch",
        doc: "The task id exists with another command or idempotency key. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("message", "str")],
        methods: &[],
    },
    StubClass {
        name: "SendPayloadTooLarge",
        doc: "The input is larger than the configured limit. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("message", "str"),
            property("size_bytes", "int"),
            property("limit_bytes", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "SendKeyConflict",
        doc: "The idempotency key is reserved by owner_task_id with another command.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("owner_task_id", "uuid.UUID"),
            property("message", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "TaskAttemptRecord",
        doc: "One execution attempt of a task.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("attempt", "int"),
            StubProperty {
                name: "outcome",
                annotation: TypeText::WireValuesOf(<TaskAttemptOutcome as CodeEnum>::CATALOG),
            },
            property("will_retry", "bool"),
            property("started_at", "datetime.datetime"),
            property("finished_at", "datetime.datetime"),
            property("error_code", "str | None"),
            property("error_message", "str | None"),
            property("failed_reason", "str | None"),
            property("worker_id", "str | None"),
            property("worker_hostname", "str | None"),
            property("worker_pid", "int | None"),
            property("worker_process_name", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "TaskInfoRecord",
        doc: "The metadata of one task, with the fields of TaskInfo. result_json is the\nstored result envelope as JSON text; execution is None for a task read from history.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("task_name", "str"),
            StubProperty {
                name: "status",
                annotation: TypeText::WireValuesOf(<TaskStatus as CodeEnum>::CATALOG),
            },
            property("queue_name", "str"),
            property("priority", "int"),
            property("retry_count", "int"),
            property("max_retries", "int"),
            property("next_retry_at", "datetime.datetime | None"),
            property("sent_at", "datetime.datetime | None"),
            property("enqueued_at", "datetime.datetime"),
            property("claimed_at", "datetime.datetime | None"),
            property("started_at", "datetime.datetime | None"),
            property("completed_at", "datetime.datetime | None"),
            property("failed_at", "datetime.datetime | None"),
            property("worker_hostname", "str | None"),
            property("worker_pid", "int | None"),
            property("worker_process_name", "str | None"),
            property("error_code", "str | None"),
            property("failed_reason", "str | None"),
            property("result_json", "str | None"),
            property("attempts", "list[TaskAttemptRecord] | None"),
            property(
                "execution",
                "Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'] | None",
            ),
        ],
        methods: &[],
    },
    StubClass {
        name: "RawResultRecordRecord",
        doc: "The status of one task and its stored result envelope as JSON text; result_json is None\nfor a task that had not finished when the limit passed.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("task_name", "str"),
            StubProperty {
                name: "status",
                annotation: TypeText::WireValuesOf(<TaskStatus as CodeEnum>::CATALOG),
            },
            property("result_json", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "RawResultAbsent",
        doc: "Neither the live table nor the task history holds the task.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "TaskInfoNotFound",
        doc: "Neither the live table nor the task history holds the task.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "StaleClaimsDrained",
        doc: "The requeue pass ended at a short batch: no eligible row was left. Every batch committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("requeued", "int")],
        methods: &[],
    },
    StubClass {
        name: "StaleClaimsBoundReached",
        doc: "The requeue pass ran its bound of full batches and stopped; eligible rows can remain.\nEvery batch committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("requeued", "int")],
        methods: &[],
    },
    StubClass {
        name: "StaleClaimsRequeueFailed",
        doc: "The requeue pass stopped at a failure. The batches before it committed requeued_before\nrows; the failed batch wrote nothing, or its outcome is unknown.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("requeued_before", "int"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "StaleRunningRecovered",
        doc: "The stale RUNNING pass ended; its scan read fewer candidates than its batch. recovered\ncounts the candidates whose crash retry or failure committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("recovered", "int")],
        methods: &[],
    },
    StubClass {
        name: "StaleRunningScanFull",
        doc: "The stale RUNNING pass ended, and its scan read a full batch of scan_batch candidates:\nstale rows can remain for the next pass.",
        constructor: Constructor::NoConstructor,
        properties: &[property("recovered", "int"), property("scan_batch", "int")],
        methods: &[],
    },
    StubClass {
        name: "StaleRunningScanFailed",
        doc: "The stale RUNNING pass did not run: a threshold out of range, a failed connect or family\ncheck, or a failed scan. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "PendingExpiryDrained",
        doc: "The pending expiry pass ended at a short batch: no due row was left. Every batch\ncommitted.",
        constructor: Constructor::NoConstructor,
        properties: &[property("expired", "int")],
        methods: &[],
    },
    StubClass {
        name: "PendingExpiryBoundReached",
        doc: "The pending expiry pass ran its bound of full batches and stopped; due rows can remain.\nEvery batch committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("expired", "int")],
        methods: &[],
    },
    StubClass {
        name: "PendingExpiryFailed",
        doc: "The pending expiry pass stopped at a failure. The batches before it committed and\nexpired expired_before rows; the failed batch committed nothing, or its outcome is unknown.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("expired_before", "int"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "OrphanPageAudited",
        doc: "The orphan page committed: rows_selected workflow tasks scanned, and\ncandidates_returned of them, with no runnable node link, moved to history as CANCELLED.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("rows_selected", "int"),
            property("candidates_returned", "int"),
            property("cancelled", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "OrphanAuditLockBusy",
        doc: "Another sweep holds the orphan cursor: the page was refused at once (SQLSTATE 55P03)\nand wrote nothing.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "OrphanAuditFailed",
        doc: "The orphan page did not commit: a failed connect, family check or statement, or a page\noutside the function's contract. After a lost COMMIT reply its outcome is unknown.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "TaskRowEnded",
        doc: "The row moved to history: kind is EXPIRE_CLAIMED or CANCEL_ORPHAN. The observed fields\nare the row as the statement locked it.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("terminal_at", "datetime.datetime"),
            property("kind", "str"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptAlreadyApplied",
        doc: "The writer's own effect was already committed: a replay after a lost reply. Nothing was\nwritten.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("terminal_at", "datetime.datetime"),
            property("kind", "str"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptLostClaim",
        doc: "The row is live, and the fence does not match it. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptSourceStateConflict",
        doc: "The row exists and is not this writer's to end; guard names the guard that refused it.\nNothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("guard", GUARD_TEXT),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptTaskAbsent",
        doc: "No live or history row has this id. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "AttemptWriteFailed",
        doc: "The statement failed. It wrote nothing, or it committed and its reply was lost.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "PhaseTwoOwedRecord",
        doc: "Phase 2 of a workflow task is owed: it advances the node and wakes the queue.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_name", "str"),
            property("queue_name", "str"),
            property(
                "step",
                "Literal['NO_DEPENDENTS', 'DEPENDENTS_WITHOUT_SUBWORKFLOW', 'SUBWORKFLOW_REACHABLE']",
            ),
        ],
        methods: &[],
    },
    StubClass {
        name: "RetryNotRequested",
        doc: "The caller asked for no retry.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "RetryPastDeadline",
        doc: "The caller asked for a retry, and its next attempt would start at or after good_until.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("next_retry_at", "datetime.datetime"),
            property("good_until", "datetime.datetime"),
        ],
        methods: &[],
    },
    StubClass {
        name: "CapacityWoken",
        doc: "A plain task: the statement sent its capacity wake. Nothing is owed.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "AttemptFailureApplied",
        doc: "The terminal failure and its attempt row committed.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("terminal_at", "datetime.datetime"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
            property("retry", "RetryNotRequested | RetryPastDeadline"),
            property("after", "CapacityWoken | PhaseTwoOwedRecord"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptRetryScheduled",
        doc: "The retry and its attempt row committed; retry_count is the count the row now holds.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("retry_count", "int"),
            property("next_retry_at", "datetime.datetime"),
            property("queue_name", "str"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptClaimReleased",
        doc: "Rule RELEASE: the row was still CLAIMED by this claim; it is PENDING again with no\nattempt row.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("queue_name", "str"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "SuccessApplied",
        doc: "The success of this row committed with the statement.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("terminal_at", "datetime.datetime"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "SuccessesWritten",
        doc: "The batch statement ran: one row record per success, in the request order.",
        constructor: Constructor::NoConstructor,
        properties: &[property(
            "rows",
            "list[SuccessApplied | AttemptAlreadyApplied | AttemptLostClaim | \
             AttemptSourceStateConflict | AttemptTaskAbsent]",
        )],
        methods: &[],
    },
    StubClass {
        name: "BatchNotSent",
        doc: "No row of the batch changed, and the statement was not sent: the pools, the client\nfamily check or the checkout failed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "BatchRefused",
        doc: "No row of the batch changed: the server refused the statement. One row can be the cause.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "BatchCommitUnknown",
        doc: "The reply of the batch statement was lost: each row may have committed or not.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowSuccessApplied",
        doc: "Phase 1 of the workflow task's success committed; phase_two is the phase 2 owed.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("terminal_at", "datetime.datetime"),
            property("observed_status", "str | None"),
            property("observed_worker_id", "str | None"),
            property("observed_claimed_at", "datetime.datetime | None"),
            property("phase_two", "PhaseTwoOwedRecord"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClaimedRowRecord",
        doc: "One row the claim pass took; claimed_at is the claim generation of this claim.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("id", "uuid.UUID"),
            property("task_name", "str"),
            property("queue_name", "str"),
            property("input", "str"),
            property("task_options", "str | None"),
            property(
                "execution",
                "Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']",
            ),
            property("is_workflow_task", "bool"),
            property("claimed_at", "datetime.datetime"),
            property("retry_count", "int"),
            property("max_retries", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClaimPassRecord",
        doc: "The claim pass ran: the claimed rows in dispatch order, and the rows its plan chose and\nits take did not get.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("claimed", "list[ClaimedRowRecord]"),
            property("short_of_plan", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClaimPassFailed",
        doc: "The claim pass failed. It wrote nothing, or it committed and its reply was lost; claimed\nrows then return to the queue when their lease expires.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "QueueStartsRecord",
        doc: "The count of each queue of the check: its RUNNING rows plus the started claims still\nCLAIMED. A queue with no counted row is absent: its count is 0.",
        constructor: Constructor::NoConstructor,
        properties: &[property("starts", "dict[str, int]")],
        methods: &[],
    },
    StubClass {
        name: "QueueStartsFailed",
        doc: "The count failed. It wrote nothing.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "AttemptStarted",
        doc: "The row is RUNNING with its first runner beat, from started_at on the database clock.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("started_at", "datetime.datetime"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AttemptMissed",
        doc: "The start wrote nothing; reason is the first reason that holds. The path after the miss\nis the caller's.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property(
                "reason",
                "Literal['DEADLINE_PASSED', 'NOT_HELD', 'LEASE_EXPIRED', 'WORKFLOW_PAUSED', \
                 'WORKFLOW_CANCELLED', 'NODE_NOT_LINKED']",
            ),
        ],
        methods: &[],
    },
    StubClass {
        name: "DeadlineBeforeStart",
        doc: "The deadline of the run passed while the start waited for a pooled connection. No\nstatement ran: the row stays CLAIMED.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "StartFailed",
        doc: "The start failed. It wrote nothing, or it committed and its reply was lost: the row may\nbe RUNNING.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "FinalizingMarked",
        doc: "The finalizing marker is written: the row is RUNNING under this worker's claim generation.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "FinalizingNotHeld",
        doc: "The finalizing marker wrote nothing: the row is absent, not RUNNING, or held by another\nworker or another claim generation.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "MarkFinalizingFailed",
        doc: "The finalizing marker failed. It wrote nothing, or it committed and its reply was lost:\nthe marker may be written.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClaimsReleased",
        doc: "The release statement ran: one row outcome per claim, in the request order.",
        constructor: Constructor::NoConstructor,
        properties: &[property("rows", "list[ReleasedRow | NotHeldRow]")],
        methods: &[],
    },
    StubClass {
        name: "ReleasedRow",
        doc: "The row was CLAIMED by the worker at its claim generation and is PENDING again.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "NotHeldRow",
        doc: "The row is absent, or not CLAIMED by the worker at this claim generation. It was not\ntouched.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "ReleaseFailed",
        doc: "The release failed. The statement wrote nothing, or it committed and its reply was lost.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "TaskCancelledRecord",
        doc: "The plain task moved to history as CANCELLED; was_status is the status it left.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            StubProperty {
                name: "was_status",
                annotation: TypeText::WireValuesOf(<TaskStatus as CodeEnum>::CATALOG),
            },
        ],
        methods: &[],
    },
    StubClass {
        name: "CancelRefusedWorkflowTask",
        doc: "The task belongs to a workflow, live or finished. Nothing changed.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            StubProperty {
                name: "current_status",
                annotation: TypeText::WireValuesOf(<TaskStatus as CodeEnum>::CATALOG),
            },
        ],
        methods: &[],
    },
    StubClass {
        name: "CancelRefusedStatus",
        doc: "The task's status does not admit the cancel: a terminal status (an earlier cancel\nincluded), or RUNNING without the opt-in. Nothing changed.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            StubProperty {
                name: "current_status",
                annotation: TypeText::WireValuesOf(<TaskStatus as CodeEnum>::CATALOG),
            },
        ],
        methods: &[],
    },
    StubClass {
        name: "CancelTaskAbsent",
        doc: "Neither the live table nor history holds the task. Nothing changed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "CancelFailed",
        doc: "The connect, the client family check or the statement failed. The statement wrote\nnothing, or it committed and its reply was lost.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "CancelStoredDataInvalid",
        doc: "Stored task data does not parse. The cancel refused the task; nothing changed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("message", "str")],
        methods: &[],
    },
    StubClass {
        name: "SchedulerStarted",
        doc: "The start of the scheduler completed.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "SchedulerStartRefused",
        doc: "The start refused a schedule before any state write: a zone that the core does not\nknow, a send that the command rules refuse, or a pattern with no slot after the database\ntime.",
        constructor: Constructor::NoConstructor,
        properties: &[property("schedule", "str"), property("reason", "str")],
        methods: &[],
    },
    StubClass {
        name: "SchedulerStartFailed",
        doc: "The start failed before the state start: the connect, the schema install or the client\nfamily check. No schedule state was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "SchedulerTicked",
        doc: "The tick ran. The failure of one due schedule is logged and does not fail the tick.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "SchedulerTickFailed",
        doc: "The tick fired nothing: the core pools could not open, or the due read failed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool")],
        methods: &[],
    },
    StubClass {
        name: "SchedulerStopped",
        doc: "The run ended at a stop request.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
];

const HANDLE_CLASSES: [StubClass; 3] = [
    StubClass {
    name: "CoreHandle",
    doc: "The core handle of one producer manifest and the pool profile of its process: PRODUCER,\nor WORKER (the main core pool keeps 2 connections free from batch checkouts for the\nper-task writes). Building it starts nothing.",
    constructor: Constructor::Parameters(CORE_HANDLE_PARAMETERS),
    properties: &[property("manifest_version", "int")],
    methods: &CORE_HANDLE_METHODS,
},
    StubClass {
        name: "CoreChildHandle",
        doc: "The core handle of one worker's child process: the producer manifest of its child pool,\nthe connections the pool keeps open, and the ping at checkout. Built in the child after\nits start; building it starts nothing.",
        constructor: Constructor::Parameters(CORE_CHILD_HANDLE_PARAMETERS),
        properties: &[],
        methods: &CORE_CHILD_HANDLE_METHODS,
    },
    StubClass {
        name: "CoreScheduler",
        doc: "The scheduler of one scheduler manifest on the broker of a CoreHandle. Building it\nstarts nothing. A refused manifest raises ManifestRefused. One call runs at a time: a\ncall while another is in flight raises RuntimeError.",
        constructor: Constructor::Parameters(CORE_SCHEDULER_PARAMETERS),
        properties: &[],
        methods: &CORE_SCHEDULER_METHODS,
    },
];

/// The classes of the monitoring reads and the web handler.
const MONITORING_CLASSES: [StubClass; 2] = [
    StubClass {
        name: "MonitoringWindowRecord",
        doc: "The window of one monitoring request: UTC bounds, increasing, at most the core's maximum\napart.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("lower", "datetime.datetime"),
            property("upper", "datetime.datetime"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WindowRefusedRecord",
        doc: "The requested window is not servable; reason names why.",
        constructor: Constructor::NoConstructor,
        properties: &[property("reason", "str")],
        methods: &[],
    },
];

const WORKFLOW_CLASSES: [StubClass; 8] = [
    StubClass {
        name: "WorkflowPaused",
        doc: "The workflow was RUNNING; it and its RUNNING descendants are PAUSED, and the claimed\ntasks of the paused tree are abandoned.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowNotRunning",
        doc: "The workflow exists and is not RUNNING. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowCancelled",
        doc: "The workflow was PENDING, RUNNING or PAUSED; its tree is CANCELLED.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowNotCancellable",
        doc: "The workflow is COMPLETED, FAILED, CANCELLED or EXPIRED. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowAbsent",
        doc: "No workflow has this id. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "HandleFailed",
        doc: "The connect, the client family check or the statement failed (DB_OPERATION_FAILED), or a\nstored value of the workflow does not parse (STORED_DATA_INVALID, not retryable). After\nDB_OPERATION_FAILED the statement wrote nothing, or it committed and its reply was lost.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("code", "Literal['DB_OPERATION_FAILED', 'STORED_DATA_INVALID']"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowsDeleted",
        doc: "The workflow retention pass ended: a batch deleted no workflow, or the budget passed\nafter a batch.",
        constructor: Constructor::NoConstructor,
        properties: &[property("deleted", "int")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowDeleteFailed",
        doc: "The workflow retention pass failed. deleted_before counts the workflows of the batches\nthat committed first; a batch whose reply was lost may have committed.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("deleted_before", "int"),
            property("message", "str"),
            property("retryable", "bool"),
        ],
        methods: &[],
    },
];

const CLASS_COUNT: usize = HANDLE_CLASSES.len()
    + ARGUMENT_CLASSES.len()
    + RECORD_CLASSES.len()
    + MONITORING_CLASSES.len()
    + WORKFLOW_CLASSES.len();

const CLASSES: [StubClass; CLASS_COUNT] = {
    let mut classes = [HANDLE_CLASSES[0]; CLASS_COUNT];
    let mut handle_position = 0;
    while handle_position < HANDLE_CLASSES.len() {
        classes[handle_position] = HANDLE_CLASSES[handle_position];
        handle_position += 1;
    }
    let mut position = 0;
    while position < ARGUMENT_CLASSES.len() {
        classes[HANDLE_CLASSES.len() + position] = ARGUMENT_CLASSES[position];
        position += 1;
    }
    let mut record_position = 0;
    while record_position < RECORD_CLASSES.len() {
        classes[HANDLE_CLASSES.len() + ARGUMENT_CLASSES.len() + record_position] =
            RECORD_CLASSES[record_position];
        record_position += 1;
    }
    let monitoring_start = HANDLE_CLASSES.len() + ARGUMENT_CLASSES.len() + RECORD_CLASSES.len();
    let mut monitoring_position = 0;
    while monitoring_position < MONITORING_CLASSES.len() {
        classes[monitoring_start + monitoring_position] = MONITORING_CLASSES[monitoring_position];
        monitoring_position += 1;
    }
    let workflow_start = monitoring_start + MONITORING_CLASSES.len();
    let mut workflow_position = 0;
    while workflow_position < WORKFLOW_CLASSES.len() {
        classes[workflow_start + workflow_position] = WORKFLOW_CLASSES[workflow_position];
        workflow_position += 1;
    }
    classes
};

const FUNCTIONS: &[StubFunction] = &[
    StubFunction {
        name: "baseline_sql_files",
        parameters: &[],
        returns: TypeText::Plain("list[tuple[str, str]]"),
        doc: "The baseline SQL files as (file name, text) pairs, in run order. Starts nothing.",
    },
    StubFunction {
        name: "core_state",
        parameters: &[],
        returns: TypeText::Plain("Literal['not_started', 'running', 'stopped', 'inherited']"),
        doc: "The state of the core in this process. Starts nothing.",
    },
    StubFunction {
        name: "mint_task_id",
        parameters: &[],
        returns: TypeText::Plain("uuid.UUID | IdMintRefused"),
        doc: "Mint a new task id (UUIDv7). Starts no runtime.",
    },
    StubFunction {
        name: "refresh_log_levels",
        parameters: &[],
        returns: TypeText::Plain("None"),
        doc: "Read the effective level of every horsies logger again.",
    },
    StubFunction {
        name: "resolve_monitoring_window",
        parameters: RESOLVE_MONITORING_WINDOW_PARAMETERS,
        returns: TypeText::Plain("MonitoringWindowRecord | WindowRefusedRecord"),
        doc: "Resolve the terminal-history window of one monitoring request. Absent bounds take the\ndefault: the last 24 hours ending at until, or at now (the clock of this process when\nNone). A naive since or until is refused; a naive now raises TypeError. Starts no runtime.",
    },
    StubFunction {
        name: "set_panic_rule",
        parameters: SET_PANIC_RULE_PARAMETERS,
        returns: TypeText::Plain("None"),
        doc: "Set what a panic in a core background task does in this process, before the first\ncore call. Another rule than the one in force raises PanicRuleFixed.",
    },
];

/// The surface of `horsies._core`.
pub const CORE_MODULE: ModuleSurface = ModuleSurface {
    exceptions: EXCEPTIONS,
    classes: &CLASSES,
    functions: FUNCTIONS,
};
