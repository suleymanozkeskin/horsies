//! The Python surface of the extension module `horsies._core`, as data.
//!
//! The classes and functions here are defined with PyO3 in
//! `clients/python/bridge`. PyO3 attribute values must be literals, so this
//! table repeats their names and signatures. A change to the extension
//! changes this table in the same commit; a Python test compares the
//! generated stub with the built module. Where a value is a catalog code,
//! the type comes from the core catalog itself.

use horsies_core::catalog::{CodeCatalog, CodeEnum};
use horsies_core::{TaskAbsenceReason, TaskAttemptOutcome, TaskStatus};

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
    pub body: ExceptionBody,
}

/// What an exception class declares besides its base and its doc.
#[derive(Clone, Copy, Debug)]
pub enum ExceptionBody {
    /// A class whose facts are in `args` (PyO3 `create_exception!`).
    ArgsOnly,
    /// A class with named read-only attributes, and a constructor when
    /// Python can build it.
    Attributes {
        constructor: Constructor,
        properties: &'static [StubProperty],
    },
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

/// A static method of a class: a function with no `self`, called on the
/// class.
#[derive(Clone, Copy, Debug)]
pub struct StubStaticMethod {
    /// The name of the class that holds it, as in [`StubClass::name`].
    pub class_name: &'static str,
    pub function: StubFunction,
}

/// A `from <module> import <names>` line of the stub: classes of the
/// package that the annotations name, for example the generated error
/// classes that an operation returns.
#[derive(Clone, Copy, Debug)]
pub struct StubImport {
    pub module: &'static str,
    pub names: &'static [&'static str],
}

/// The whole module.
#[derive(Clone, Copy, Debug)]
pub struct ModuleSurface {
    pub imports: &'static [StubImport],
    pub exceptions: &'static [StubException],
    pub classes: &'static [StubClass],
    pub static_methods: &'static [StubStaticMethod],
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

const TRACE_CARRIER_PARAMETERS: &[StubParameter] = &[
    positional("carrier", "dict[str, str]"),
    positional("trace_id", "bytes"),
];

const CORE_HANDLE_PARAMETERS: &[StubParameter] = &[
    positional("manifest_json", "str"),
    keyword("pool_profile", "Literal['PRODUCER', 'WORKER']"),
];

const SET_PANIC_RULE_PARAMETERS: &[StubParameter] =
    &[positional("rule", "Literal['STOP_CORE', 'ABORT_PROCESS']")];

const TRACE_LINK_REFUSAL_PARAMETERS: &[StubParameter] = &[positional("template", "str")];

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
    keyword("trace", "NoTraceCarrier | TraceCarrier"),
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
const COVERAGE_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("history_horizon_days", "int"),
    keyword("heartbeat_horizon_hours", "int"),
    keyword("declared_period_days", "list[int]"),
    keyword("serialization", "Literal['GATED', 'PER_LEAF_LOCKS']"),
];
const INSTALL_COVERAGE_PARAMETERS: &[StubParameter] = &[
    keyword("history_horizon_days", "int"),
    keyword("heartbeat_horizon_hours", "int"),
    keyword("declared_period_days", "list[int]"),
];
const WITH_INSTALL_COVERAGE_PARAMETERS: &[StubParameter] =
    &[positional("install_coverage", "InstallCoverage")];
const ENSURE_PARTITION_COVERAGE_PARAMETERS: &[StubParameter] =
    &[positional("request", "CoverageRequest")];
const CANCEL_TASK_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("include_running", "bool"),
];
const WORKFLOW_ID_PARAMETERS: &[StubParameter] = &[positional("workflow_id", "uuid.UUID")];
const WAIT_WORKFLOW_RESULT_PARAMETERS: &[StubParameter] = &[
    positional("workflow_id", "uuid.UUID"),
    keyword("timeout_ms", "int | None"),
];
const WORKFLOW_NODE_RESULT_PARAMETERS: &[StubParameter] = &[
    positional("workflow_id", "uuid.UUID"),
    positional("node_id", "str"),
];
const DELETE_EXPIRED_WORKFLOWS_PARAMETERS: &[StubParameter] = &[
    positional("older_than_hours", "int"),
    positional("batch_size", "int"),
    positional("budget_ms", "int"),
];
const EXPIRE_PAUSED_WORKFLOWS_PARAMETERS: &[StubParameter] = &[
    positional("older_than_us", "int"),
    positional("batch_size", "int"),
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

const TASK_WORKFLOW_NODE_PARAMETERS: &[StubParameter] = &[positional("task_id", "uuid.UUID")];

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
    keyword("attempt_elapsed_ms", "int"),
];
const STOP_BEFORE_START_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("worker_id", "str"),
    keyword("claimed_at", "datetime.datetime"),
    keyword("orphan_self_heal", "bool"),
    keyword("attempt_elapsed_ms", "int"),
];

const TASK_LIST_QUERY_PARAMETERS: &[StubParameter] = &[
    keyword("window_lower", "datetime.datetime"),
    keyword("window_upper", "datetime.datetime"),
    keyword("statuses", "list[str]"),
    keyword("task_names", "list[str]"),
    keyword("queues", "list[str]"),
    keyword("workers", "list[str]"),
    keyword("error_codes", "list[str]"),
    keyword("error_categories", "list[str]"),
    keyword("retried_only", "bool"),
    keyword("sort_by", "str"),
    keyword("sort_dir", "str"),
    keyword("offset", "int"),
    keyword("limit", "int"),
];
const TASK_FACETS_QUERY_PARAMETERS: &[StubParameter] = &[
    keyword("window_lower", "datetime.datetime"),
    keyword("window_upper", "datetime.datetime"),
    keyword("statuses", "list[str]"),
    keyword("error_categories", "list[str]"),
    keyword("retried_only", "bool"),
];
const TASK_STATS_QUERY_PARAMETERS: &[StubParameter] = &[
    keyword("window_lower", "datetime.datetime"),
    keyword("window_upper", "datetime.datetime"),
    keyword("task_names", "list[str]"),
    keyword("queues", "list[str]"),
    keyword("workers", "list[str]"),
    keyword("error_codes", "list[str]"),
    keyword("error_categories", "list[str]"),
    keyword("retried_only", "bool"),
];
const TASK_BREAKDOWN_QUERY_PARAMETERS: &[StubParameter] = &[
    keyword("window_lower", "datetime.datetime"),
    keyword("window_upper", "datetime.datetime"),
    keyword("group_by", "str"),
    keyword("statuses", "list[str]"),
    keyword("task_names", "list[str]"),
    keyword("queues", "list[str]"),
    keyword("workers", "list[str]"),
    keyword("error_codes", "list[str]"),
    keyword("error_categories", "list[str]"),
    keyword("retried_only", "bool"),
    keyword("limit", "int"),
];
const WORKFLOW_RUNS_QUERY_PARAMETERS: &[StubParameter] = &[
    keyword("name", "str | None"),
    keyword("status", "str | None"),
    keyword("limit", "int"),
];
const MONITORING_TASK_STATS_PARAMETERS: &[StubParameter] = &[positional("query", "TaskStatsQuery")];
const MONITORING_TASK_BREAKDOWN_PARAMETERS: &[StubParameter] =
    &[positional("query", "TaskBreakdownQuery")];
const MONITORING_WORKFLOW_RUNS_PARAMETERS: &[StubParameter] =
    &[positional("query", "WorkflowRunsQuery")];
const MONITORING_WORKFLOW_RUN_PARAMETERS: &[StubParameter] =
    &[positional("workflow_id", "uuid.UUID")];
const MONITORING_WORKFLOW_NODE_PARAMETERS: &[StubParameter] = &[
    positional("workflow_id", "uuid.UUID"),
    keyword("task_index", "int"),
];
const MONITORING_LIST_TASKS_PARAMETERS: &[StubParameter] = &[positional("query", "TaskListQuery")];
const MONITORING_TASK_DETAIL_PARAMETERS: &[StubParameter] = &[positional("task_id", "uuid.UUID")];
const STALE_TASKS_PARAMETERS: &[StubParameter] = &[positional("threshold_minutes", "object")];
const PING_WORKERS_PARAMETERS: &[StubParameter] = &[
    keyword("target_worker_id", "str | None"),
    keyword("timeout_seconds", "float"),
    keyword("min_responses", "int | None"),
];
const WORKER_STATE_PARAMETERS: &[StubParameter] = &[positional("worker_id", "str")];
const WORKER_STATE_HISTORY_PARAMETERS: &[StubParameter] = &[
    positional("worker_id", "str"),
    keyword("limit", "int | None"),
];
const MONITORING_TASK_FACETS_PARAMETERS: &[StubParameter] =
    &[positional("query", "TaskFacetsQuery")];

const RESOLVE_MONITORING_WINDOW_PARAMETERS: &[StubParameter] = &[
    keyword("since", "datetime.datetime | None"),
    keyword("until", "datetime.datetime | None"),
    keyword("now", "datetime.datetime | None"),
];

const CORE_SCHEDULER_PARAMETERS: &[StubParameter] = &[
    positional("handle", "CoreHandle"),
    positional("manifest_json", "str"),
];

const PING_OUTCOME: &str = "DatabasePingRecord | BrokerOperationFailed";
const INSTALL_OUTCOME: &str = "SchemaInstalled | SchemaInstallNotRun | BrokerOperationFailed";
/// A refused or failed send is a generated class of
/// `horsies._catalog.broker_errors`, built in Rust.
const SEND_OUTCOME: &str = "Enqueued | BrokerCoreEnqueueError";
const TASK_INFO_OUTCOME: &str =
    "TaskInfoRecord | TaskInfoNotFound | BrokerOperationFailed | BrokerInvalidJsonPayload";
const TASK_WORKFLOW_NODE_OUTCOME: &str =
    "TaskWorkflowNodeLinked | TaskWorkflowNodeNotLinked | BrokerOperationFailed";
const RAW_RESULT_OUTCOME: &str = "RawResultRecordRecord | RawResultAbsent | BrokerOperationFailed | BrokerInvalidJsonPayload | BrokerInvalidInput";
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
const ENSURE_PARTITION_COVERAGE_OUTCOME: &str =
    "CoverageEnsuredRecord | CoverageEnsureFailedRecord | CoverageFailed";
const PRUNE_EXPIRED_PARTITIONS_OUTCOME: &str = "PrunePassRecord | PruneFailed";
/// A refused or failed cancel is a generated class of
/// `horsies._catalog.monitoring_errors`, built in Rust.
const CANCEL_TASK_OUTCOME: &str = "TaskCancelledRecord | TaskActionCoreError";
const EXPIRE_PENDING_TASKS_OUTCOME: &str =
    "PendingExpiryDrained | PendingExpiryBoundReached | PendingExpiryFailed";
const AUDIT_ORPHANED_WORKFLOW_TASKS_OUTCOME: &str =
    "OrphanPageAudited | OrphanAuditLockBusy | OrphanAuditFailed";
const TASK_ROW_END_OUTCOME: &str = "TaskRowEnded | AttemptAlreadyApplied | AttemptLostClaim | \
     AttemptSourceStateConflict | AttemptTaskAbsent | AttemptWriteFailed";

/// A refused or failed start is a generated class of
/// `horsies._catalog.workflow_start_errors`, built in Rust.
const START_WORKFLOW_OUTCOME: &str =
    "WorkflowStarted | WorkflowExists | WorkflowStartIdConflict | WorkflowStartEnqueueFailed";
const START_WORKFLOW_PARAMETERS: &[StubParameter] =
    &[positional("request", "WorkflowStartRequest")];
const REGISTER_CHILD_SPEC_SOURCE_PARAMETERS: &[StubParameter] = &[positional(
    "source",
    "Callable[[ChildSpecRequest], ChildSpecBuilt | ChildSpecRefused]",
)];
const WORKFLOW_START_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("spec_json", "str"),
    keyword("workflow_id", "uuid.UUID"),
    keyword("sent_at", "datetime.datetime"),
    keyword("resend_on_transient_err", "bool"),
    keyword("trace", "NoTraceCarrier | TraceCarrier"),
];
const CHILD_SPEC_BUILT_PARAMETERS: &[StubParameter] = &[keyword("spec_json", "str")];
const CHILD_SPEC_REFUSED_PARAMETERS: &[StubParameter] =
    &[keyword("message", "str"), keyword("failure_result", "str")];
/// A failed handle operation is a generated class of
/// `horsies._catalog.workflow_handle_errors`, built in Rust.
const RESUME_WORKFLOW_OUTCOME: &str = "WorkflowResumed | WorkflowNotPaused | WorkflowAbsent | \
     HandleDbOperationFailed | HandleStoredDataInvalid | HandleResumeRecoveryFailed";
const FINISH_WORKFLOW_NODE_PARAMETERS: &[StubParameter] =
    &[positional("request", "NodeFinishRequest")];
const ADVANCE_WORKFLOW_PARENTS_PARAMETERS: &[StubParameter] =
    &[positional("child_workflow_ids", "list[uuid.UUID]")];
const NODE_FINISH_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("task_id", "uuid.UUID"),
    keyword(
        "facts",
        "Literal['NO_DEPENDENTS', 'DEPENDENTS_WITHOUT_SUBWORKFLOW', 'SUBWORKFLOW_REACHABLE', \
         'UNKNOWN']",
    ),
    keyword("wake", "Literal['SEND', 'NONE']"),
];
const NODE_FINISH_DISPOSITION: &str = "Literal['APPLIED', 'APPLIED_WORKFLOW_ENDED', \
     'ALREADY_APPLIED', 'PENDING_ABSENT', 'SOURCE_ABSENT', 'SOURCE_STATE_CONFLICT', \
     'SOURCE_VERSION_CONFLICT', 'SOURCE_DIGEST_MISMATCH']";
const RECOVER_WORKFLOW_PHASE2_PARAMETERS: &[StubParameter] = &[
    keyword("grace_ms", "int"),
    keyword("max_rows", "int"),
    keyword("quarantine_after_attempts", "int"),
];
const PAUSE_WORKFLOW_OUTCOME: &str = "WorkflowPaused | WorkflowNotRunning | WorkflowAbsent | \
     HandleDbOperationFailed | HandleStoredDataInvalid";
const CANCEL_WORKFLOW_OUTCOME: &str = "WorkflowCancelled | WorkflowNotCancellable | \
     WorkflowAbsent | HandleDbOperationFailed | HandleStoredDataInvalid";
const WORKFLOW_STATUS_OUTCOME: &str =
    "WorkflowStatusRead | WorkflowAbsent | HandleDbOperationFailed | HandleStoredDataInvalid";
const WAIT_WORKFLOW_RESULT_OUTCOME: &str = "WorkflowCompletedRead | WorkflowEndedRead | \
     WorkflowPausedRead | WaitTimedOut | WorkflowAbsent | HandleDbOperationFailed | \
     HandleStoredDataInvalid";
const WORKFLOW_NODE_RESULTS_OUTCOME: &str = "WorkflowNodeResultsRead | WorkflowAbsent | \
     HandleDbOperationFailed | HandleStoredDataInvalid";
const WORKFLOW_NODE_RESULT_OUTCOME: &str = "StoredNodeResultRecord | NodeResultNotStored | \
     HandleDbOperationFailed | HandleStoredDataInvalid";
const WORKFLOW_NODES_OUTCOME: &str =
    "WorkflowNodesRead | WorkflowAbsent | HandleDbOperationFailed | HandleStoredDataInvalid";

const START_ATTEMPT_OUTCOME: &str =
    "AttemptStarted | AttemptMissed | DeadlineBeforeStart | StartFailed";
const MARK_FINALIZING_OUTCOME: &str = "FinalizingMarked | FinalizingNotHeld | MarkFinalizingFailed";
const STOP_BEFORE_START_OUTCOME: &str =
    "StoppedBeforeStart | ResumedBeforeStart | StopBeforeStartFailed";

const SCHEDULER_START_OUTCOME: &str =
    "SchedulerStarted | SchedulerStartRefused | SchedulerStartFailed";
const SCHEDULER_TICK_OUTCOME: &str = "SchedulerTicked | SchedulerTickFailed";
const SCHEDULER_RUN_OUTCOME: &str =
    "SchedulerStopped | SchedulerStartRefused | SchedulerStartFailed";

const CORE_SCHEDULER_METHODS: &[StubFunction] = &[
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

const CORE_CHILD_HANDLE_METHODS: &[StubFunction] = &[
    StubFunction {
        name: "start_attempt",
        parameters: START_ATTEMPT_PARAMETERS,
        returns: TypeText::Plain(START_ATTEMPT_OUTCOME),
        doc: "Start one claimed task. Writes, in one statement. deadline_in_ms is the time left before\nthe deadline of the run, or None: when it has passed by the time the pool gives the\nconnection, no statement runs. The call begins a start attempt of 40 s: a statement past\nits time gets a cancel request and does not commit later. A naive claimed_at raises\nTypeError.",
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
        doc: "Expire one claimed row of this worker whose deadline passed before its start. Writes,\nin one statement, by the end of the start attempt that began attempt_elapsed_ms ago.",
    },
    StubFunction {
        name: "expire_owned_claim_async",
        parameters: EXPIRE_OWNED_CLAIM_PARAMETERS,
        returns: TypeText::FutureOf(TASK_ROW_END_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "stop_workflow_task_before_start",
        parameters: STOP_BEFORE_START_PARAMETERS,
        returns: TypeText::Plain(STOP_BEFORE_START_OUTCOME),
        doc: "Stop one claimed workflow task whose start missed. Writes, in one statement, which judges\nthe task again under its locks; with orphan_self_heal it cancels a task that no node\nlinks live. It ends by the end of the start attempt that began attempt_elapsed_ms ago.\nA naive claimed_at raises TypeError.",
    },
    StubFunction {
        name: "stop_workflow_task_before_start_async",
        parameters: STOP_BEFORE_START_PARAMETERS,
        returns: TypeText::FutureOf(STOP_BEFORE_START_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "mark_finalizing",
        parameters: MARK_FINALIZING_PARAMETERS,
        returns: TypeText::Plain(MARK_FINALIZING_OUTCOME),
        doc: "Write the finalizing marker of a run whose body ended. Writes, in one statement, within\n10 s: a marker cancelled at the bound is not written later. A naive claimed_at raises\nTypeError.",
    },
    StubFunction {
        name: "mark_finalizing_async",
        parameters: MARK_FINALIZING_PARAMETERS,
        returns: TypeText::FutureOf(MARK_FINALIZING_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
];

const CORE_HANDLE_METHODS: &[StubFunction] = &[
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
        name: "check_connectivity",
        parameters: &[],
        returns: TypeText::Plain("ConnectivityPassed | ConnectivityRefused"),
        doc: "The connectivity step of the live check: SELECT 1 on the main URL, then on the session URL\nwhen it differs, then the LISTEN probe in PgBouncer transaction mode. Reads only.",
    },
    StubFunction {
        name: "check_worker_connections",
        parameters: &[positional("profiles_json", "str")],
        returns: TypeText::Plain("list[WorkerConnectionError]"),
        doc: "The worker connection step of the live check: each profile against the server connection\nlimit. No profile, or PgBouncer transaction mode, connects nothing. Reads only.",
    },
    StubFunction {
        name: "install_schema",
        parameters: &[],
        returns: TypeText::Plain(INSTALL_OUTCOME),
        doc: "Apply the embedded migrations, check the schema version, then run the startup coverage\npass that creates the history leaves of the current UTC day and the lookahead, once per\nhandle. Writes.",
    },
    StubFunction {
        name: "install_schema_async",
        parameters: &[],
        returns: TypeText::FutureOf(INSTALL_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "with_install_coverage",
        parameters: WITH_INSTALL_COVERAGE_PARAMETERS,
        returns: TypeText::Plain("CoreHandle"),
        doc: "A new handle of the same manifest and pool profile whose schema install ends with the\ncoverage pass of install_coverage. It shares no pool and no child spec source with this\none. Starts nothing.",
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
        name: "ensure_partition_coverage_async",
        parameters: ENSURE_PARTITION_COVERAGE_PARAMETERS,
        returns: TypeText::FutureOf(ENSURE_PARTITION_COVERAGE_OUTCOME),
        doc: "One partition coverage pass on the session-work pool. Writes (DDL and catalog rows);\neach leaf commits alone.",
    },
    StubFunction {
        name: "prune_expired_partitions_async",
        parameters: &[],
        returns: TypeText::FutureOf(PRUNE_EXPIRED_PARTITIONS_OUTCOME),
        doc: "One pruning pass on the session-work pool. Writes (DDL and catalog rows); each leaf's\ndetach and drop commit alone.",
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
        name: "register_child_spec_source",
        parameters: REGISTER_CHILD_SPEC_SOURCE_PARAMETERS,
        returns: TypeText::Plain("None"),
        doc: "Register the child spec source of the handle's workflow operations. The core calls it\non the callback thread for each sub-workflow child that an operation builds. A second\nregistration raises RuntimeError. Starts nothing.",
    },
    StubFunction {
        name: "start_workflow",
        parameters: START_WORKFLOW_PARAMETERS,
        returns: TypeText::Plain(START_WORKFLOW_OUTCOME),
        doc: "Start one workflow. Writes; a caller that leaves does not stop it. The children of its\nsub-workflow roots come from the child spec source. A build that raised (KeyboardInterrupt,\nSystemExit, or an Exception such as SerializationError) rolls the start back and raises it\nagain. With no child spec source it raises RuntimeError.",
    },
    StubFunction {
        name: "start_workflow_async",
        parameters: START_WORKFLOW_PARAMETERS,
        returns: TypeText::FutureOf(START_WORKFLOW_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "resume_workflow",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::Plain(RESUME_WORKFLOW_OUTCOME),
        doc: "Resume one workflow tree. Writes; a caller that leaves does not stop it. The child starts\nit owes build their children through the child spec source. A build that raised\nKeyboardInterrupt or SystemExit raises it again; one that raised an Exception raises it\nagain when the resume failed with it. With no child spec source it raises RuntimeError.",
    },
    StubFunction {
        name: "resume_workflow_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(RESUME_WORKFLOW_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "finish_workflow_node_async",
        parameters: FINISH_WORKFLOW_NODE_PARAMETERS,
        returns: TypeText::FutureOf("NodeFinished | NodeFinishFailed"),
        doc: "Phase 2 of one finished workflow task. Writes; the child starts it owes build their\nchildren through the child spec source. A build that raised KeyboardInterrupt or\nSystemExit raises it again. With no child spec source it raises RuntimeError.",
    },
    StubFunction {
        name: "advance_workflow_parents_async",
        parameters: ADVANCE_WORKFLOW_PARENTS_PARAMETERS,
        returns: TypeText::FutureOf("ParentsAdvanced | ParentAdvanceStopped"),
        doc: "Advance the parent nodes above child_workflow_ids, one transaction per level. Writes; the\nchild starts a level owes build their children through the child spec source. A build\nthat raised KeyboardInterrupt or SystemExit raises it again. With no child spec source\nit raises RuntimeError.",
    },
    StubFunction {
        name: "recover_workflow_phase2_async",
        parameters: RECOVER_WORKFLOW_PHASE2_PARAMETERS,
        returns: TypeText::FutureOf("Phase2RecoveryRecord | Phase2RecoveryFailed"),
        doc: "One phase-2 recovery pass of the reaper: the phase-2 records older than grace_ms and\nunder quarantine_after_attempts, oldest first, at most max_rows, each in its own\ntransaction. Writes; owed child starts build their children through the child spec\nsource. A build that raised KeyboardInterrupt or SystemExit raises it again after the\npass. A negative value raises OverflowError; with no child spec source it raises\nRuntimeError.",
    },
    StubFunction {
        name: "recover_workflows_async",
        parameters: &[keyword("failure_limit", "int")],
        returns: TypeText::FutureOf("RecoveryPassRecord | RecoveryPassFailed"),
        doc: "One global workflow recovery pass of the reaper, with failure_limit passes per workflow\nbefore it fails closed. Writes; each candidate commits alone, and owed child starts build\ntheir children through the child spec source. A build that raised KeyboardInterrupt or\nSystemExit raises it again after the pass. A negative value raises OverflowError; with\nno child spec source it raises RuntimeError.",
    },
    StubFunction {
        name: "settle_claimed_workflow_tasks_async",
        parameters: &[positional("request", "SettleRequest")],
        returns: TypeText::FutureOf("ClaimsSettled | SettleFailed"),
        doc: "Settle the claimed workflow tasks of one claim pass. Writes, one statement per 1000\nclaims. A request with no claim sends nothing.",
    },
    StubFunction {
        name: "pause_workflow",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::Plain(PAUSE_WORKFLOW_OUTCOME),
        doc: "Pause one workflow tree. Writes, in one statement; a caller that leaves does not stop\nit. A row outside the function's contract raises PanicException.",
    },
    StubFunction {
        name: "pause_workflow_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(PAUSE_WORKFLOW_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "cancel_workflow",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::Plain(CANCEL_WORKFLOW_OUTCOME),
        doc: "Cancel one workflow tree. Writes, in one statement; a caller that leaves does not stop\nit. A row outside the function's contract raises PanicException.",
    },
    StubFunction {
        name: "cancel_workflow_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(CANCEL_WORKFLOW_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "delete_expired_workflows_async",
        parameters: DELETE_EXPIRED_WORKFLOWS_PARAMETERS,
        returns: TypeText::FutureOf("WorkflowsDeleted | WorkflowDeleteFailed"),
        doc: "One workflow retention pass of the reaper, in batches until one deletes nothing or the\nbudget has passed. Writes; each batch commits alone. A negative value raises\nOverflowError.",
    },
    StubFunction {
        name: "expire_paused_workflows_async",
        parameters: EXPIRE_PAUSED_WORKFLOWS_PARAMETERS,
        returns: TypeText::FutureOf("PausedExpired | PausedExpiryFailed"),
        doc: "Expire the PAUSED workflows older than older_than_us microseconds, at most batch_size,\noldest first. Writes; each candidate commits alone.",
    },
    StubFunction {
        name: "workflow_status",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::Plain(WORKFLOW_STATUS_OUTCOME),
        doc: "The status of one workflow. Reads only.",
    },
    StubFunction {
        name: "workflow_status_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(WORKFLOW_STATUS_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "wait_workflow_result",
        parameters: WAIT_WORKFLOW_RESULT_PARAMETERS,
        returns: TypeText::Plain(WAIT_WORKFLOW_RESULT_OUTCOME),
        doc: "Wait for the end of one workflow, up to timeout_ms (no limit for None). Reads only; a\ncaller that leaves drops the wait and its subscription. Bounded by the limit plus the bound\nof a blocking call; not bounded with no limit. A negative timeout raises OverflowError.",
    },
    StubFunction {
        name: "wait_workflow_result_async",
        parameters: WAIT_WORKFLOW_RESULT_PARAMETERS,
        returns: TypeText::FutureOf(WAIT_WORKFLOW_RESULT_OUTCOME),
        doc: "The awaitable form of the method above. A cancelled await drops the wait and its\nsubscription.",
    },
    StubFunction {
        name: "workflow_node_results",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::Plain(WORKFLOW_NODE_RESULTS_OUTCOME),
        doc: "The nodes of one workflow that store a result. Reads only.",
    },
    StubFunction {
        name: "workflow_node_results_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(WORKFLOW_NODE_RESULTS_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "workflow_node_result",
        parameters: WORKFLOW_NODE_RESULT_PARAMETERS,
        returns: TypeText::Plain(WORKFLOW_NODE_RESULT_OUTCOME),
        doc: "The stored result of one node. Reads only.",
    },
    StubFunction {
        name: "workflow_node_result_async",
        parameters: WORKFLOW_NODE_RESULT_PARAMETERS,
        returns: TypeText::FutureOf(WORKFLOW_NODE_RESULT_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "workflow_nodes",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::Plain(WORKFLOW_NODES_OUTCOME),
        doc: "The nodes of one workflow in index order. Reads only.",
    },
    StubFunction {
        name: "workflow_nodes_async",
        parameters: WORKFLOW_ID_PARAMETERS,
        returns: TypeText::FutureOf(WORKFLOW_NODES_OUTCOME),
        doc: "The awaitable form of the method above.",
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
        name: "task_workflow_node",
        parameters: TASK_WORKFLOW_NODE_PARAMETERS,
        returns: TypeText::Plain(TASK_WORKFLOW_NODE_OUTCOME),
        doc: "The workflow node that links one task. One bounded statement; no schema step.",
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

/// A failed monitoring read is a generated class of
/// `horsies._catalog.monitoring_errors`, built in Rust.
const MONITORING_READ_OUTCOME: &str = "MonitoringJson | MonitoringQueryCoreError";
const PING_WORKERS_OUTCOME: &str =
    "WorkerPongsRecord | BrokerOperationFailed | BrokerInvalidInput | BrokerListenerDisabled";
const WORKER_STATES_OUTCOME: &str =
    "WorkerStatesRecord | BrokerOperationFailed | BrokerInvalidInput";
const WORKER_STATE_OUTCOME: &str = "WorkerStateRecord | WorkerStateAbsent | BrokerOperationFailed";
const CLASS_COVERAGE_OUTCOME: &str = "ClassCoverageRecord | BrokerOperationFailed";
const CLASS_REPORT_OUTCOME: &str = "ClassReportRecord | BrokerOperationFailed";
const TASK_LIST_OUTCOME: &str =
    "TaskListComplete | TaskListTruncated | BrokerOperationFailed | BrokerInvalidInput";
const MONITORING_DETAIL_OUTCOME: &str =
    "MonitoringJson | TaskDetailAbsent | MonitoringQueryCoreError";

/// The monitoring methods of `CoreHandle`.
const MONITORING_HANDLE_METHODS: &[StubFunction] = &[
    StubFunction {
        name: "monitoring_list_tasks_async",
        parameters: MONITORING_LIST_TASKS_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "One page of the task list: the JSON text of the core's TaskListPage. Reads only, in one\nstatement. A cancelled await drops the read.",
    },
    StubFunction {
        name: "monitoring_task_detail_async",
        parameters: MONITORING_TASK_DETAIL_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_DETAIL_OUTCOME),
        doc: "One task with its attempts: the JSON text of the core's TaskDetail, or the reason it is\nabsent. Reads only, in one statement. A cancelled await drops the read.",
    },
    StubFunction {
        name: "monitoring_task_facets_async",
        parameters: MONITORING_TASK_FACETS_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "The facets of one scope: the JSON text of the core's Facets. Reads only, in one\nstatement. A cancelled await drops the read.",
    },
    StubFunction {
        name: "monitoring_task_stats_async",
        parameters: MONITORING_TASK_STATS_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "The count of each task status: the JSON text of the core's list of StatusCount. Reads\nonly, in one statement.",
    },
    StubFunction {
        name: "monitoring_task_breakdown_async",
        parameters: MONITORING_TASK_BREAKDOWN_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "The task counts per group: the JSON text of the core's Breakdown. Reads only, in one\nstatement.",
    },
    StubFunction {
        name: "monitoring_workflow_names_async",
        parameters: &[],
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "The names of the root runs: the JSON text of a list of str. Reads only, in one statement.",
    },
    StubFunction {
        name: "monitoring_workflow_runs_async",
        parameters: MONITORING_WORKFLOW_RUNS_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "The newest root runs: the JSON text of the core's list of WorkflowRunSummary. Reads only,\nin one statement.",
    },
    StubFunction {
        name: "monitoring_workflow_run_async",
        parameters: MONITORING_WORKFLOW_RUN_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "One run with its nodes and edges: the JSON text of the core's WorkflowRunDetail, or null\nwhen no run has the id. Reads only, in one statement.",
    },
    StubFunction {
        name: "monitoring_workflow_node_async",
        parameters: MONITORING_WORKFLOW_NODE_PARAMETERS,
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "One node with its task: the JSON text of the core's WorkflowTaskDetail, or null when the\nrun has no node at the index. Reads only, in one statement. An index outside 32 bits\nraises OverflowError.",
    },
    StubFunction {
        name: "monitoring_schedules_async",
        parameters: &[],
        returns: TypeText::FutureOf(MONITORING_READ_OUTCOME),
        doc: "Every schedule state: the JSON text of the core's list of ScheduleStateInfo. Reads only,\nin one statement.",
    },
    StubFunction {
        name: "stale_tasks_async",
        parameters: STALE_TASKS_PARAMETERS,
        returns: TypeText::FutureOf(TASK_LIST_OUTCOME),
        doc: "The RUNNING tasks whose latest runner beat, or their start, is older than the\nthreshold. A bool, a value that is not an int, or one outside 0 to 525600 minutes is\nINVALID_INPUT before any core call. Reads only, in one statement.",
    },
    StubFunction {
        name: "expired_tasks_async",
        parameters: &[],
        returns: TypeText::FutureOf(TASK_LIST_OUTCOME),
        doc: "The PENDING tasks whose good_until passed before a worker took them. Reads only, in\none statement.",
    },
    StubFunction {
        name: "ping_workers",
        parameters: PING_WORKERS_PARAMETERS,
        returns: TypeText::Plain(PING_WORKERS_OUTCOME),
        doc: "Ping the workers and collect the distinct replies within the window, on the process\nlistener. A refused argument is INVALID_INPUT before any core call; a handle with no\nlistener is LISTENER_DISABLED. Sends one NOTIFY; writes no row. Bounded by the window plus\nthe bound of a blocking call; not bounded for an infinite window.",
    },
    StubFunction {
        name: "ping_workers_async",
        parameters: PING_WORKERS_PARAMETERS,
        returns: TypeText::FutureOf(PING_WORKERS_OUTCOME),
        doc: "The awaitable form of the method above. A cancelled await drops the wait and the reply\nsubscription.",
    },
    StubFunction {
        name: "worker_states",
        parameters: &[],
        returns: TypeText::Plain(WORKER_STATES_OUTCOME),
        doc: "The latest state snapshot per worker, idle workers included. Reads only, in one\nstatement.",
    },
    StubFunction {
        name: "worker_states_async",
        parameters: &[],
        returns: TypeText::FutureOf(WORKER_STATES_OUTCOME),
        doc: "The awaitable form of the method above. A cancelled await drops the read.",
    },
    StubFunction {
        name: "worker_state",
        parameters: WORKER_STATE_PARAMETERS,
        returns: TypeText::Plain(WORKER_STATE_OUTCOME),
        doc: "The latest state snapshot of one worker, or WorkerStateAbsent when it wrote none. Reads\nonly, in one statement.",
    },
    StubFunction {
        name: "worker_state_async",
        parameters: WORKER_STATE_PARAMETERS,
        returns: TypeText::FutureOf(WORKER_STATE_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "worker_state_history",
        parameters: WORKER_STATE_HISTORY_PARAMETERS,
        returns: TypeText::Plain(WORKER_STATES_OUTCOME),
        doc: "The state snapshots of one worker, newest first: at most limit, or every retained one for\nNone. A limit of 0 or less is INVALID_INPUT; a limit that PostgreSQL refuses as a bigint\nis MONITORING_QUERY_FAILED, not retryable; both before any core call. Reads only, in one\nstatement.",
    },
    StubFunction {
        name: "worker_state_history_async",
        parameters: WORKER_STATE_HISTORY_PARAMETERS,
        returns: TypeText::FutureOf(WORKER_STATES_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "class_coverage",
        parameters: &[],
        returns: TypeText::Plain(CLASS_COVERAGE_OUTCOME),
        doc: "The fleet check: for each (queue, class) with PENDING rows, whether a live worker serves\nit. Reads only, in one statement.",
    },
    StubFunction {
        name: "class_coverage_async",
        parameters: &[],
        returns: TypeText::FutureOf(CLASS_COVERAGE_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
    StubFunction {
        name: "class_report",
        parameters: &[],
        returns: TypeText::Plain(CLASS_REPORT_OUTCOME),
        doc: "The pending streams with their oldest wait and coverage, and the budget load of each\nlive worker. Reads only, in one statement.",
    },
    StubFunction {
        name: "class_report_async",
        parameters: &[],
        returns: TypeText::FutureOf(CLASS_REPORT_OUTCOME),
        doc: "The awaitable form of the method above.",
    },
];

const HANDLE_METHOD_COUNT: usize = CORE_HANDLE_METHODS.len() + MONITORING_HANDLE_METHODS.len();

/// Every method of `CoreHandle`: the client operations, then the monitoring
/// reads.
const HANDLE_METHODS: [StubFunction; HANDLE_METHOD_COUNT] = {
    let mut methods = [CORE_HANDLE_METHODS[0]; HANDLE_METHOD_COUNT];
    let mut position = 0;
    while position < CORE_HANDLE_METHODS.len() {
        methods[position] = CORE_HANDLE_METHODS[position];
        position += 1;
    }
    let mut monitoring_position = 0;
    while monitoring_position < MONITORING_HANDLE_METHODS.len() {
        methods[CORE_HANDLE_METHODS.len() + monitoring_position] =
            MONITORING_HANDLE_METHODS[monitoring_position];
        monitoring_position += 1;
    }
    methods
};

/// The parameters of `CoreServerError`.
const CORE_SERVER_ERROR_PARAMETERS: &[StubParameter] = &[
    positional("subject", "str"),
    positional("sqlstate", "str"),
    positional("message", "str"),
    positional("retryable", "bool"),
];

/// The parameters of `CoreContractViolation`.
const SUBJECT_MESSAGE_PARAMETERS: &[StubParameter] =
    &[positional("subject", "str"), positional("message", "str")];

/// The parameters of `CoreConnectionError` and `CorePreconditionFailed`.
const SUBJECT_MESSAGE_RETRYABLE_PARAMETERS: &[StubParameter] = &[
    positional("subject", "str"),
    positional("message", "str"),
    positional("retryable", "bool"),
];

/// The type of the `exception` of a failure record: the leaf class of the
/// cause of the failure.
const CORE_FAILURE_EXCEPTION: &str =
    "CoreServerError | CoreConnectionError | CoreContractViolation | CorePreconditionFailed";

const EXCEPTIONS: &[StubException] = &[
    StubException {
        name: "CoreError",
        base: "Exception",
        doc: "Base of the errors raised at the core boundary.",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "ForkedAfterCoreStart",
        base: "CoreError",
        doc: "This process was forked without exec after the core started in its parent.\n\nargs: (core_pid, this_pid, advice).",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "CoreStopped",
        base: "CoreError",
        doc: "A core background task panicked, so the core is stopped in this process.\n\nargs: (subject, cause) of the first panic.",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "CoreStartFailed",
        base: "CoreError",
        doc: "The core runtime or its log forwarder could not start. args: (cause,).",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "CoreCallTimedOut",
        base: "CoreError",
        doc: "A blocking call did not end within its bound.\n\nargs: (subject, bound_seconds, state_after); state_after is 'no_change'\nfor a read and 'outcome_unknown' for a write.",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "TooManyPendingCalls",
        base: "CoreError",
        doc: "The event loop already has the most core calls in flight. args: (subject, bound).",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "CoreDefect",
        base: "BaseException",
        doc: "The client sent a value that the core refuses: a defect of the client.",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "ManifestRefused",
        base: "CoreDefect",
        doc: "The core refused a manifest. args: (field_path, cause).",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "PanicRuleFixed",
        base: "CoreDefect",
        doc: "The panic rule of this process was set to another rule before, or fixed when the core\nstarted. args: (current_rule,).",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "NotMainThread",
        base: "CoreDefect",
        doc: "A run form was called off the main thread, where Python runs signal handlers.\n\nThe run did not start. args: (subject,).",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "PanicException",
        base: "BaseException",
        doc: "Rust code called from Python panicked.",
        body: ExceptionBody::ArgsOnly,
    },
    StubException {
        name: "CoreFailure",
        base: "Exception",
        doc: "Base of the causes of a failed core operation. subject is the operation, for example\n'release_claims'; message says what failed and why. No constructor.",
        body: ExceptionBody::Attributes {
            constructor: Constructor::NoConstructor,
            properties: &[property("subject", "str"), property("message", "str")],
        },
    },
    StubException {
        name: "CoreDatabaseError",
        base: "CoreFailure",
        doc: "Base of the database failures: the server answered with an error, or did not answer.\nretryable: the cause may clear, so a repeat of the operation can succeed. No constructor.",
        body: ExceptionBody::Attributes {
            constructor: Constructor::NoConstructor,
            properties: &[property("retryable", "bool")],
        },
    },
    StubException {
        name: "CoreServerError",
        base: "CoreDatabaseError",
        doc: "The server answered with an error: sqlstate is its five-character SQLSTATE, message\nthe server's primary message.",
        body: ExceptionBody::Attributes {
            constructor: Constructor::Parameters(CORE_SERVER_ERROR_PARAMETERS),
            properties: &[property("sqlstate", "str")],
        },
    },
    StubException {
        name: "CoreConnectionError",
        base: "CoreDatabaseError",
        doc: "The operation got no answer from the server: a connect or a checkout that failed or\ntimed out, a lost connection, or a reply that did not come within its bound.",
        body: ExceptionBody::Attributes {
            constructor: Constructor::Parameters(SUBJECT_MESSAGE_RETRYABLE_PARAMETERS),
            properties: &[],
        },
    },
    StubException {
        name: "CoreContractViolation",
        base: "CoreFailure",
        doc: "A row, a stored value or a result broke the contract that the core reads it with. Not\nretryable: the same call meets it again.",
        body: ExceptionBody::Attributes {
            constructor: Constructor::Parameters(SUBJECT_MESSAGE_PARAMETERS),
            properties: &[],
        },
    },
    StubException {
        name: "CorePreconditionFailed",
        base: "CoreFailure",
        doc: "A precondition of the operation does not hold: the settings, the database's client\nfamily or schema, an argument, a row that the call names, a key or a lock that another\ncommand holds, a usable transaction, or a client that can build a child spec now.\nretryable: the precondition may come to hold without a change by the caller.",
        body: ExceptionBody::Attributes {
            constructor: Constructor::Parameters(SUBJECT_MESSAGE_RETRYABLE_PARAMETERS),
            properties: &[property("retryable", "bool")],
        },
    },
];

const ARGUMENT_CLASSES: &[StubClass] = &[
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
        name: "NoTraceCarrier",
        doc: "The send carries no trace carrier: tracing is off, or the propagator wrote no key.",
        constructor: Constructor::Parameters(&[]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "TraceCarrier",
        doc: "The carrier that the client's propagator injected for the span of a send or a\nworkflow start, or for the current span of the caller of a rerun, and the 16-byte trace\nid of that span. The core checks its bounds at the send; a carrier that breaks a bound\nis not stored and never fails the send.",
        constructor: Constructor::Parameters(TRACE_CARRIER_PARAMETERS),
        properties: &[
            property("carrier", "dict[str, str]"),
            property("trace_id", "bytes"),
        ],
        methods: &[],
    },
    StubClass {
        name: "EnqueueRequest",
        doc: "One frozen send. None for good_until, task_options_json or idempotency_key\nmeans no deadline, no options, no key. retention_class_key is 'forever' or\n'standard_<days>d'; the core checks it at the send. send_kind is FIRST for the first send\nof the task id and REPEAT for a send retry: a task that finished then answers from history.\ntrace is NoTraceCarrier or the TraceCarrier of the send's span.",
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

const RECORD_CLASSES: &[StubClass] = &[
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
        name: "ConnectivityPassed",
        doc: "Every step of the connectivity check passed.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ConnectivityRefused",
        doc: "The first step of the connectivity check that refused, and its cause. No later step ran.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property(
                "step",
                "Literal['CONNECT', 'POOL_SETTINGS', 'MAIN_URL', 'SESSION_URL', 'LISTENER']",
            ),
            property("message", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerConnectionError",
        doc: "One refusal of the worker connection check: the core's error as data.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("message", "str"),
            property("code", "str | None"),
            property("notes", "list[str]"),
            property("help_text", "str | None"),
        ],
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
        name: "TaskWorkflowNodeLinked",
        doc: "The node at node_index of the workflow workflow_id links the task; root_workflow_id is the top-level workflow of its tree.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("node_index", "int"),
            property("root_workflow_id", "uuid.UUID"),
        ],
        methods: &[],
    },
    StubClass {
        name: "TaskWorkflowNodeNotLinked",
        doc: "No workflow node links the task now.",
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
            property("exception", CORE_FAILURE_EXCEPTION),
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
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
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
            property("exception", CORE_FAILURE_EXCEPTION),
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
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "OrphanAuditFailed",
        doc: "The orphan page did not commit: a failed connect, family check or statement, or a page\noutside the function's contract. After a lost COMMIT reply its outcome is unknown.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "TaskRowEnded",
        doc: "The row moved to history: kind is EXPIRE_CLAIMED. The observed fields are the row as the\nstatement locked it.",
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
            property("exception", CORE_FAILURE_EXCEPTION),
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
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "BatchRefused",
        doc: "No row of the batch changed: the server refused the statement. One row can be the cause.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "BatchCommitUnknown",
        doc: "The reply of the batch statement was lost: each row may have committed or not.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("exception", CORE_FAILURE_EXCEPTION)],
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
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
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
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "AttemptStarted",
        doc: "The row is RUNNING (and, for a workflow task, its node RUNNING), from started_at on the database clock. The start writes no runner beat. workflow is the node that links the task.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("started_at", "datetime.datetime"),
            property("workflow", "RunWorkflowNode | Literal['NOT_IN_WORKFLOW']"),
        ],
        methods: &[],
    },
    StubClass {
        name: "RunWorkflowNode",
        doc: "The workflow node that links a started task: the node at node_index of the workflow\nworkflow_id, in the tree whose top-level workflow is root_workflow_id.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("node_index", "int"),
            property("root_workflow_id", "uuid.UUID"),
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
        name: "StoppedBeforeStart",
        doc: "The stop ran a step that ends the start of this claim: outcome is the step, move_outcome\nthe outcome of its move, None for NODE_NOT_LINKED, which writes nothing.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property(
                "outcome",
                "Literal['EXPIRED', 'ABANDONED', 'CANCELLED', 'ORPHAN_CANCELLED', 'NODE_NOT_LINKED', \
                 'NOT_HELD']",
            ),
            property(
                "move_outcome",
                "Literal['APPLIED', 'ALREADY_APPLIED', 'LOST_CLAIM', 'SOURCE_STATE_CONFLICT', \
                 'TASK_ABSENT'] | None",
            ),
        ],
        methods: &[],
    },
    StubClass {
        name: "ResumedBeforeStart",
        doc: "A resume came first: the claim is still startable. Nothing is written; the caller runs\nthe start once more.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "StopBeforeStartFailed",
        doc: "The stop failed. It wrote nothing, or it committed and its reply was lost: the same stop\nsent again reports a committed move as ALREADY_APPLIED, or the row as not held.",
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
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
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

const HANDLE_CLASSES: &[StubClass] = &[
    StubClass {
    name: "CoreHandle",
    doc: "The core handle of one producer manifest and the pool profile of its process: PRODUCER,\nor WORKER (the main core pool keeps 2 connections free from batch checkouts for the\nper-task writes). Building it starts nothing.",
    constructor: Constructor::Parameters(CORE_HANDLE_PARAMETERS),
    properties: &[property("manifest_version", "int")],
    methods: &HANDLE_METHODS,
},
    StubClass {
        name: "CoreChildHandle",
        doc: "The core handle of one worker's child process, built from its init frame (from_init), or\nfrom the producer manifest of its child pool, the connections the pool keeps open, and the\nping at checkout. Built in the child after its start; building it starts nothing.",
        constructor: Constructor::Parameters(CORE_CHILD_HANDLE_PARAMETERS),
        properties: &[],
        methods: CORE_CHILD_HANDLE_METHODS,
    },
    StubClass {
        name: "CoreScheduler",
        doc: "The scheduler of one scheduler manifest on the broker of a CoreHandle. Building it\nstarts nothing. A refused manifest raises ManifestRefused. One call runs at a time: a\ncall while another is in flight raises RuntimeError.",
        constructor: Constructor::Parameters(CORE_SCHEDULER_PARAMETERS),
        properties: &[],
        methods: CORE_SCHEDULER_METHODS,
    },
];

const ACCESS_GRANT_PARAMETERS: &[StubParameter] = &[
    keyword("view", "Literal['ALLOWED', 'REFUSED', 'NOT_ASKED']"),
    keyword("act", "Literal['ALLOWED', 'REFUSED', 'NOT_ASKED']"),
];

const MONITORING_REQUEST_PARAMETERS: &[StubParameter] = &[
    keyword("method", "str"),
    keyword("path", "str"),
    keyword("raw_query", "str | None"),
    keyword("intent_header", "str | None"),
    keyword("content_type", "str | None"),
    keyword("body", "bytes"),
    keyword("base_path", "str"),
    keyword("grant", "AccessGrant"),
];

const MONITORING_HANDLER_PARAMETERS: &[StubParameter] = &[
    positional("handle", "CoreHandle"),
    keyword("actions_enabled", "bool"),
    keyword("custom_css_url", "str | None"),
    keyword("trace_link_template", "str | None"),
    keyword("client_version", "str"),
    keyword("heartbeat_ms", "int"),
    keyword("queue_names", "list[str]"),
    keyword("schedule_names", "list[str]"),
];

/// The classes of the monitoring reads and the web handler.
const MONITORING_CLASSES: &[StubClass] = &[
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
    StubClass {
        name: "TaskListQuery",
        doc: "One frozen task list read: the window, the filters, the sort and the page. A value\noutside its rule raises ValueError; a naive bound raises TypeError.",
        constructor: Constructor::Parameters(TASK_LIST_QUERY_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "TaskFacetsQuery",
        doc: "One frozen task facets read: the window, the statuses, the error categories that narrow\nthe code list, and the retried filter. A value outside its rule raises ValueError; a naive\nbound raises TypeError.",
        constructor: Constructor::Parameters(TASK_FACETS_QUERY_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "TaskStatsQuery",
        doc: "One frozen task stats read: the window and every filter but the status. A value outside\nits rule raises ValueError; a naive bound raises TypeError.",
        constructor: Constructor::Parameters(TASK_STATS_QUERY_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "TaskBreakdownQuery",
        doc: "One frozen task breakdown read: the window, the group column, the filters and the group\nlimit. A value outside its rule raises ValueError; a naive bound raises TypeError.",
        constructor: Constructor::Parameters(TASK_BREAKDOWN_QUERY_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "WorkflowRunsQuery",
        doc: "One frozen workflow runs read: the exact name and status filters (None for none) and the\nlimit. A limit outside 1 to 200 raises ValueError.",
        constructor: Constructor::Parameters(WORKFLOW_RUNS_QUERY_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "MonitoringJson",
        doc: "The JSON text of the core model of one monitoring read.",
        constructor: Constructor::NoConstructor,
        properties: &[property("json", "str")],
        methods: &[],
    },
    StubClass {
        name: "TaskDetailAbsent",
        doc: "Neither the live table nor the history holds the task.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            StubProperty {
                name: "reason",
                annotation: TypeText::WireValuesOf(<TaskAbsenceReason as CodeEnum>::CATALOG),
            },
        ],
        methods: &[],
    },
    StubClass {
        name: "TaskListComplete",
        doc: "Every matching row of a task list: one dict per row, with the task id as uuid.UUID.",
        constructor: Constructor::NoConstructor,
        properties: &[property("rows", "list[dict[str, object]]")],
        methods: &[],
    },
    StubClass {
        name: "TaskListTruncated",
        doc: "The first bound matching rows of a task list; more rows match.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("rows", "list[dict[str, object]]"),
            property("bound", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerPongRecord",
        doc: "One worker's reply to a ping, with the measured round trip.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("worker_id", "str"),
            property("hostname", "str"),
            property("pid", "int"),
            property("round_trip_ms", "float"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerPongsRecord",
        doc: "The distinct workers that replied within the window, in reply order.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("pongs", "list[WorkerPongRecord]"),
        ],
        methods: &[],
    },

    StubClass {
        name: "ResourceShortageRecord",
        doc: "The cause of a down slot that ran short of an OS resource.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("errno", "Literal['EMFILE', 'ENFILE', 'ENOMEM', 'EAGAIN']"),
        ],
        methods: &[],
    },
    StubClass {
        name: "DownSlotRecord",
        doc: "A worker slot whose last child boot failed for a cause that can clear.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("slot", "int"),
            property("cause", "ResourceShortageRecord | Literal['THREAD_START_REFUSED']"),
            property("detail", "str"),
            property("down_since", "datetime.datetime"),
            property("failed_boots", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AllSlotsServingRecord",
        doc: "No slot of the worker was down when it wrote the snapshot.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "SlotsDownRecord",
        doc: "The worker was degraded: these slots were down, in slot order. Never empty.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("slots", "list[DownSlotRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClassBudgetRecord",
        doc: "One claim budget of a worker, as its snapshot recorded it; serves is in Execution order.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("budget", "Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']"),
            property("capacity", "int"),
            property("serves", "list[Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerStateRecord",
        doc: "One horsies_worker_states row. recovery_config_json is the JSON text of the stored object.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("worker_id", "str"),
            property("snapshot_at", "datetime.datetime"),
            property("hostname", "str"),
            property("pid", "int"),
            property("processes", "int"),
            property("max_claim_batch", "int"),
            property("max_claim_per_worker", "int"),
            property("cluster_wide_cap", "int | None"),
            property("queues", "list[str]"),
            property("queue_priorities", "dict[str, int]"),
            property("queue_max_concurrency", "dict[str, int]"),
            property("recovery_config_json", "str"),
            property("tasks_running", "int"),
            property("tasks_claimed", "int"),
            property("memory_usage_mb", "float | None"),
            property("memory_percent", "float | None"),
            property("cpu_percent", "float | None"),
            property("children_memory_mb", "float | None"),
            property("worker_started_at", "datetime.datetime"),
            property("slot_health", "AllSlotsServingRecord | SlotsDownRecord"),
            property("class_capacity", "list[ClassBudgetRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerStatesRecord",
        doc: "The snapshots of a worker state read: the latest per worker, or the history of one worker,\nnewest first.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("states", "list[WorkerStateRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerStateAbsent",
        doc: "The worker has written no snapshot.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("worker_id", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "StreamServedRecord",
        doc: "Live workers serve the stream with capacity in service.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workers", "int"),
            property("capacity", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "StreamWithoutCapacityRecord",
        doc: "Live workers serve the stream, and none has capacity in service.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workers", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "StreamUnservedRecord",
        doc: "No live worker serves the stream.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "StreamCoverageRecord",
        doc: "The coverage of one pending stream.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("queue_name", "str"),
            property("execution", "Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']"),
            property("state", "StreamServedRecord | StreamWithoutCapacityRecord | StreamUnservedRecord"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClassCoverageRecord",
        doc: "The fleet check, read at checked_at (database time).",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("checked_at", "datetime.datetime"),
            property("live_workers", "int"),
            property("streams", "list[StreamCoverageRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "OldestWaitingRecord",
        doc: "At least one row of the stream is due; wait is how long the oldest due row waited.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("enqueued_at", "datetime.datetime"),
            property("wait", "datetime.timedelta"),
        ],
        methods: &[],
    },
    StubClass {
        name: "NoneDueRecord",
        doc: "Every row of the stream is due later.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("next_due_at", "datetime.datetime"),
        ],
        methods: &[],
    },
    StubClass {
        name: "PendingStreamRecord",
        doc: "One pending stream of the class report.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("queue_name", "str"),
            property("execution", "Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']"),
            property("wait", "OldestWaitingRecord | NoneDueRecord"),
            property("state", "StreamServedRecord | StreamWithoutCapacityRecord | StreamUnservedRecord"),
        ],
        methods: &[],
    },
    StubClass {
        name: "BudgetLoadRecord",
        doc: "One claim budget of a live worker, with the rows it holds.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("budget", "Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']"),
            property("capacity", "int"),
            property("serves", "list[Literal['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD']]"),
            property("in_flight", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerClassLoadRecord",
        doc: "The claim budgets of one live worker, in Execution order.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("worker_id", "str"),
            property("snapshot_at", "datetime.datetime"),
            property("budgets", "list[BudgetLoadRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ClassReportRecord",
        doc: "The class report, read at checked_at (database time); workers by worker id.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("checked_at", "datetime.datetime"),
            property("streams", "list[PendingStreamRecord]"),
            property("workers", "list[WorkerClassLoadRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "RouteAccess",
        doc: "The verdicts of the user's policy that a request needs, in order, and the form of its\nanswer.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("need", "Literal['PUBLIC', 'VIEW', 'VIEW_REPORT_ACT', 'VIEW_AND_ACT']"),
            property("form", "Literal['ONE_RESPONSE', 'EVENT_STREAM']"),
        ],
        methods: &[],
    },
    StubClass {
        name: "AccessGrant",
        doc: "The verdicts that the host's run of the user's policy gave for one request. Another text\nraises ValueError.",
        constructor: Constructor::Parameters(ACCESS_GRANT_PARAMETERS),
        properties: &[
            property("view", "Literal['ALLOWED', 'REFUSED', 'NOT_ASKED']"),
            property("act", "Literal['ALLOWED', 'REFUSED', 'NOT_ASKED']"),
        ],
        methods: &[],
    },
    StubClass {
        name: "MonitoringRequest",
        doc: "One request as the host read it: the path inside the mount as it came on the wire, the\nquery text or None, the mount path (the ASGI root_path), at most max_request_body_bytes + 1\nbody bytes, and the verdicts. A method that is not an HTTP token raises ValueError.",
        constructor: Constructor::Parameters(MONITORING_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "MonitoringResponse",
        doc: "One response for the host to send: the status, the headers in order with lowercase\nnames, and the body.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("status", "int"),
            property("headers", "list[tuple[str, str]]"),
            property("body", "bytes"),
        ],
        methods: &[],
    },
    StubClass {
        name: "EventFrame",
        doc: "One frame of an event stream, with the blank line that ends it.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("text", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "EventStreamEnded",
        doc: "The event stream ended: after the degraded frame, after the handler closed its events, or\nafter close.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "EventStreamRefused",
        doc: "No stream: the handler has no broker. The host writes frame, the degraded frame, and ends\nthe response.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("message", "str"),
            property("frame", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "EventStream",
        doc: "One web client's event stream, subscribed on the process listener.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[
            StubFunction {
                name: "next_async",
                parameters: &[],
                returns: TypeText::FutureOf("EventFrame | EventStreamEnded"),
                doc: "The next frame. Reads only; a cancelled await drops the wait and loses no event.",
            },
            StubFunction {
                name: "close",
                parameters: &[],
                returns: TypeText::Plain("None"),
                doc: "End the stream: a pending and every later next_async answers EventStreamEnded. Starts\nnothing and waits for nothing.",
            },
        ],
    },
    StubClass {
        name: "MonitoringHandler",
        doc: "The monitoring web app of the broker of a CoreHandle, for the FastAPI mount of\ncreate_monitoring_app. Starts nothing: the first call builds it. queue_names and\nschedule_names are the label values of /metrics; other names are (unknown). A name that\ncannot be a label value of its own (empty, (unknown), or past the bound of names) counts\nin the (unknown) series; the first call logs one WARNING line that names it. A handle\nwithout a child spec source raises RuntimeError; heartbeat_ms outside 1 to 3,600,000\nraises ValueError.",
        constructor: Constructor::Parameters(MONITORING_HANDLER_PARAMETERS),
        properties: &[
            property("max_request_body_bytes", "int"),
        ],
        methods: &[
            StubFunction {
                name: "route_access",
                parameters: &[positional("method", "str"), positional("path", "str")],
                returns: TypeText::Plain("RouteAccess"),
                doc: "The verdicts that a request needs and the form of its answer, from its method and its\npath inside the mount. Pure. A method that is not an HTTP token raises ValueError.",
            },
            StubFunction {
                name: "handle_async",
                parameters: &[positional("request", "MonitoringRequest")],
                returns: TypeText::FutureOf("MonitoringResponse"),
                doc: "Serve one request. A POST writes and runs to its end when the caller leaves; every other\nrequest reads, and a cancelled await drops it. A child build that raised KeyboardInterrupt\nor SystemExit raises it again.",
            },
            StubFunction {
                name: "subscribe_events_async",
                parameters: &[],
                returns: TypeText::FutureOf("EventStream | EventStreamRefused"),
                doc: "Subscribe one event stream, after an allowed view. A broker without a listener, or a\nstopped listener, gives a stream whose first frame is the degraded frame. Reads only.",
            },
            StubFunction {
                name: "close_events_async",
                parameters: &[],
                returns: TypeText::FutureOf("None"),
                doc: "End every event stream of the handler after the degraded frame. Runs to its end when the\ncaller leaves.",
            },
        ],
    },
];

const WORKFLOW_CLASSES: &[StubClass] = &[
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
            property("exception", CORE_FAILURE_EXCEPTION),
        ],
        methods: &[],
    },
    StubClass {
        name: "PausedExpired",
        doc: "The paused expiry call ended; expired counts the workflows it expired.",
        constructor: Constructor::NoConstructor,
        properties: &[property("expired", "int")],
        methods: &[],
    },
    StubClass {
        name: "PausedExpiryFailed",
        doc: "The paused expiry call failed. The candidates before a failed one are expired; a\nstatement whose reply was lost may have committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "WorkflowStatusRead",
        doc: "The stored status of a workflow.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property(
                "status",
                "Literal['PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PAUSED', 'CANCELLED', 'EXPIRED']",
            ),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowCompletedRead",
        doc: "The workflow is COMPLETED, with its stored result envelope (None only when a write\noutside the library stored it so).",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("result_json", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowEndedRead",
        doc: "The workflow is FAILED, CANCELLED or EXPIRED, with its stored error.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("status", "Literal['FAILED', 'CANCELLED', 'EXPIRED']"),
            property("error_json", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowPausedRead",
        doc: "The workflow is PAUSED.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WaitTimedOut",
        doc: "The workflow is PENDING or RUNNING after the timeout.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "StoredNodeResultRecord",
        doc: "One node that stores a result.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("node_id", "str | None"),
            property("task_name", "str"),
            property("result_json", "str"),
            property("is_subworkflow", "bool"),
            property("sub_definition_key", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowNodeResultsRead",
        doc: "The nodes of a workflow that store a result; empty when none does.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("nodes", "list[StoredNodeResultRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "NodeResultNotStored",
        doc: "No node of this id stores a result, or no workflow has the id; the read does not tell\nthe two apart.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("node_id", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "StoredNodeRecord",
        doc: "One node of a workflow, with its stored values as text.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("node_id", "str | None"),
            property("task_index", "int"),
            property("task_name", "str"),
            property("status", "str"),
            property("result_json", "str | None"),
            property("started_at", "datetime.datetime | None"),
            property("completed_at", "datetime.datetime | None"),
            property("sub_workflow_id", "uuid.UUID | None"),
            property("sub_workflow_summary_json", "str | None"),
            property("is_subworkflow", "bool"),
            property("sub_definition_key", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowNodesRead",
        doc: "The nodes of a workflow in index order; empty when it has none.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("workflow_id", "uuid.UUID"),
            property("nodes", "list[StoredNodeRecord]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ChildSpecSourceRow",
        doc: "One dependency of the node whose result an args_from member of the child's input takes.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_index", "int"),
            property("node_id", "str | None"),
            property("status", "str"),
            property("task_name", "str"),
            property("definition_key", "str | None"),
            property("result", "str | None"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ChildSpecRequest",
        doc: "The sub-workflow node whose child the Python client builds.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("parent_workflow_id", "uuid.UUID"),
            property("parent_workflow_name", "str"),
            property("task_index", "int"),
            property("child_depth", "int"),
            property("definition_key", "str | None"),
            property("params", "Literal['GIVEN', 'NOT_GIVEN']"),
            property("node_input", "str"),
            property("args_from", "str | None"),
            property("sources", "list[ChildSpecSourceRow]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "ChildSpecBuilt",
        doc: "The child built: its spec as workflow spec data.",
        constructor: Constructor::Parameters(CHILD_SPEC_BUILT_PARAMETERS),
        properties: &[property("spec_json", "str")],
        methods: &[],
    },
    StubClass {
        name: "ChildSpecRefused",
        doc: "The child cannot start: its node fails. message is the error text for the log;\nfailure_result is the result envelope that the node stores.",
        constructor: Constructor::Parameters(CHILD_SPEC_REFUSED_PARAMETERS),
        properties: &[
            property("message", "str"),
            property("failure_result", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkflowStartRequest",
        doc: "One frozen workflow start. spec_json is the workflow spec data of the spec; spec data\nthat the core refuses raises ValueError with the field path. A naive sent_at raises\nTypeError. trace is NoTraceCarrier or the TraceCarrier of the start's span; every row of\nthe tree stores it.",
        constructor: Constructor::Parameters(WORKFLOW_START_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "WorkflowStarted",
        doc: "The tree was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowExists",
        doc: "A workflow with this id and this name exists, or the row with the id was gone by the\nread of its name. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "NodeFinishRequest",
        doc: "One frozen phase 2. Another facts or wake value raises ValueError.",
        constructor: Constructor::Parameters(NODE_FINISH_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "NodeFinished",
        doc: "The phase 2 committed, or found nothing to write. A SOURCE_* disposition kept the\nphase-2 record and wrote nothing. parent_advances are the child workflows whose parent\nnodes advance after the commit.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            StubProperty {
                name: "disposition",
                annotation: TypeText::Plain(NODE_FINISH_DISPOSITION),
            },
            property("workflow_status", "str | None"),
            property("parent_advances", "list[uuid.UUID]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "NodeFinishFailed",
        doc: "The phase 2 failed: the client family check, a statement, a child build that did not\nrun, a child tree that the core refuses, or a row outside the function's contract (not\nretryable). Nothing committed, or the outcome is unknown.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("task_id", "uuid.UUID"),
            property("message", "str"),
            property("retryable", "bool"),
            property("exception", CORE_FAILURE_EXCEPTION),
        ],
        methods: &[],
    },
    StubClass {
        name: "ParentsAdvanced",
        doc: "Every parent advance committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("child_workflow_ids", "list[uuid.UUID]")],
        methods: &[],
    },
    StubClass {
        name: "ParentAdvanceStopped",
        doc: "The parent advance stopped at the level of failed_child_workflow_id; the levels before\nit committed. still_owed holds every advance a retry runs, the failed level first.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("failed_child_workflow_id", "uuid.UUID"),
            property("still_owed", "list[uuid.UUID]"),
            property("message", "str"),
            property("retryable", "bool"),
            property("exception", CORE_FAILURE_EXCEPTION),
        ],
        methods: &[],
    },
    StubClass {
        name: "Phase2RecoveryRecord",
        doc: "What one pass did. pending_absent counts the records that were gone while their node is\nnot terminal (nothing written).",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("considered", "int"),
            property("applied", "int"),
            property("already_applied", "int"),
            property("superseded", "int"),
            property("pending_absent", "int"),
            property("retained", "int"),
            property("failed", "int"),
            property("quarantined", "int"),
            property("raised_counted", "int"),
            property("over_attempt_bound", "int"),
            property("retained_details", "list[str]"),
            property("quarantine_refusals", "list[str]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "Phase2RecoveryFailed",
        doc: "The pass failed before it recovered a record: the client family check, the discovery or\nthe count. Nothing was recovered.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "RecoveryCaseRecord",
        doc: "Bounded work and outcome counts of one recovery case.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("rows_selected", "int"),
            property("candidates_returned", "int"),
            property("duration_ms", "int"),
            property("refusals", "int"),
            property("errors", "int"),
            property("failures_counted", "int"),
            property("limit_reached", "int"),
        ],
        methods: &[],
    },
    StubClass {
        name: "RecoveryPassRecord",
        doc: "What one pass did: the candidates recovered over every case, the failures that no case\ncounted, the workflows over the failure limit, and the counts of each case. The audit\npage of cases 2/3 and 4 scanned case_2_3.rows_selected workflows and returned\ncase_2_3.candidates_returned completion candidates.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("recovered", "int"),
            property("pass_errors", "int"),
            property("over_failure_limit", "int"),
            property("case_0", "RecoveryCaseRecord"),
            property("case_1", "RecoveryCaseRecord"),
            property("case_1_5", "RecoveryCaseRecord"),
            property("case_1_6", "RecoveryCaseRecord"),
            property("case_2_3", "RecoveryCaseRecord"),
            property("case_4", "RecoveryCaseRecord"),
        ],
        methods: &[],
    },
    StubClass {
        name: "RecoveryPassFailed",
        doc: "The pass failed: the client family check, a discovery statement, the audit page or the\ncount of workflows over the limit. report holds the counts of the cases before it.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("report", "RecoveryPassRecord"),
            property("message", "str"),
            property("retryable", "bool"),
            property("exception", CORE_FAILURE_EXCEPTION),
        ],
        methods: &[],
    },
    StubClass {
        name: "SettleRequest",
        doc: "The claims of one settle: the worker and each claimed workflow task with its claim\ngeneration. A naive datetime raises TypeError.",
        constructor: Constructor::Parameters(&[
            keyword("worker_id", "str"),
            keyword("claims", "list[tuple[uuid.UUID, datetime.datetime]]"),
        ]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ClaimsSettled",
        doc: "The settle committed: the tasks of PAUSED and CANCELLED workflows, never to dispatch, in\ninput order. Every other claim dispatches.",
        constructor: Constructor::NoConstructor,
        properties: &[property("blocked_task_ids", "list[uuid.UUID]")],
        methods: &[],
    },
    StubClass {
        name: "SettleFailed",
        doc: "The settle failed: the client family check, a statement, or rows outside the function's\ncontract (not retryable). The statements before the failed one stay committed.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "WorkflowResumed",
        doc: "The workflow was PAUSED; its tree is RUNNING again.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "WorkflowNotPaused",
        doc: "The workflow exists and is not PAUSED. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("workflow_id", "uuid.UUID")],
        methods: &[],
    },
];

/// The classes of the history switch: the coverage pass, the pruning pass and the rerun
/// in the caller's transaction.
const HISTORY_CLASSES: &[StubClass] = &[
    StubClass {
        name: "CoverageRequest",
        doc: "One frozen coverage pass: the two horizons, the declared retention periods in whole days,\nand how a damaged pass is serialized. A period outside 1 to 1825 days, a horizon below two\nleaves, or another serialization raises ValueError.",
        constructor: Constructor::Parameters(COVERAGE_REQUEST_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "InstallCoverage",
        doc: "The coverage request of a handle's schema install: the two horizons and the declared\nretention periods in whole days. The install runs it gated, as the worker start. A period\noutside 1 to 1825 days or a horizon below two leaves raises ValueError.",
        constructor: Constructor::Parameters(INSTALL_COVERAGE_PARAMETERS),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "CoveredForeverRecord",
        doc: "No history class has an end of coverage: only the forever class has leaves, and its leaf\nhas an open upper bound.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "CoveredUntilRecord",
        doc: "The first instant (UTC) at which a history class has no leaf.",
        constructor: Constructor::NoConstructor,
        properties: &[property("at", "datetime.datetime")],
        methods: &[],
    },
    StubClass {
        name: "CoverageEnsuredRecord",
        doc: "The pass ended with the expected leaves conformant: what it created, whether it published\nthe readers, and how far coverage reaches. absent_leaves names cataloged leaves whose\nrelation is gone.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("created_history_leaves", "int"),
            property("created_heartbeat_leaves", "int"),
            property("republished", "bool"),
            property("heartbeat_covered_now", "bool"),
            property(
                "history_covered_through",
                "CoveredForeverRecord | CoveredUntilRecord",
            ),
            property("heartbeats_covered_through", "datetime.datetime"),
            property("absent_leaves", "list[str]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "CoverageEnsureFailedRecord",
        doc: "The pass stopped at a refusal; stage, class_key and refusal name where and why.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("stage", "str"),
            property("class_key", "str | None"),
            property("refusal", "str"),
            property("heartbeat_covered_now", "bool"),
            property("absent_leaves", "list[str]"),
        ],
        methods: &[],
    },
    StubClass {
        name: "CoverageFailed",
        doc: "The pass has no outcome: the connect, the client family check, a probe, the coverage\ngate, the class registration or the final publication failed, or a row broke its\nfunction's contract.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "PrunePassRecord",
        doc: "One pruning pass: the finalized leaves, the counts of the swept leaves left detached and\ndropped, the refusals and errors (one entry per leaf), and whether a per-pass bound left\nexpired leaves for the next pass.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("finalized_leaves", "list[str]"),
            property("detached", "int"),
            property("dropped", "int"),
            property("refusals", "list[str]"),
            property("errors", "list[str]"),
            property("reach", "Literal['WITHIN_BOUNDS', 'TRUNCATED']"),
        ],
        methods: &[],
    },
    StubClass {
        name: "PruneFailed",
        doc: "The pass has no report: the connect, the client family check or the candidate read\nfailed, or a candidate row broke its function's contract. Nothing was detached or dropped.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str"), property("retryable", "bool"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "RerunCommandRecord",
        doc: "One rerun request: the source task id, the deadline of the new task (aware, or None\nfor no deadline), the caller key, and the trace that the new task stores (NoTraceCarrier,\nor the TraceCarrier of the caller's current context). A naive deadline raises TypeError.",
        constructor: Constructor::Parameters(&[keyword("source_task_id", "uuid.UUID"), keyword("deadline", "datetime.datetime | None"), keyword("caller_key", "str | None"), keyword("trace", "NoTraceCarrier | TraceCarrier")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "RerunPolicyRecord",
        doc: "The enqueue policy of the new task: its retention class key, whether it keeps its input\nfor a rerun, and the reservation window of a caller key in microseconds. A window not\nabove 0 or above 30 days, or an empty class key, raises ValueError.",
        constructor: Constructor::Parameters(&[keyword("retention_class_key", "str"), keyword("retain_rerun_input", "bool"), keyword("reservation_window_us", "int")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "PreparedStatement",
        doc: "One statement for the caller's connection: sql with a named bind and a cast for each\nparameter, parameters (one value per bind, p1 first), and the result columns as (name,\nSQL type) in row order.",
        constructor: Constructor::NoConstructor,
        properties: &[property("name", "str"), property("sql", "str"), property("parameters", "dict[str, object]"), property("columns", "list[tuple[str, str]]")],
        methods: &[],
    },
    StubClass {
        name: "RerunWriteRecord",
        doc: "The write of a rerun: run statement, then pass its row to decode_rerun_write.\nnew_task_id is the id minted for the new task.",
        constructor: Constructor::NoConstructor,
        properties: &[property("statement", "PreparedStatement"), property("new_task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "RerunSourceLiveRecord",
        doc: "The source is live; its own lifecycle owns its retry. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "RerunSourceAbsentRecord",
        doc: "No live or history record has the source id. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("reason", "Literal['PREDATES_RETAINED_HISTORY', 'WITHIN_RETAINED_HISTORY', 'NOT_CLASSIFIED']")],
        methods: &[],
    },
    StubClass {
        name: "RerunSourceCorruptRecord",
        doc: "The source history row does not decode; detail names why. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("detail", "str")],
        methods: &[],
    },
    StubClass {
        name: "RerunNotEligibleRecord",
        doc: "The source is not eligible for a rerun. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("reason", "Literal['COMPLETED_SOURCE', 'WORKFLOW_TASK']")],
        methods: &[],
    },
    StubClass {
        name: "RerunInputUnavailableRecord",
        doc: "The source stores no input envelope; disposition is its stored reason. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("disposition", "str")],
        methods: &[],
    },
    StubClass {
        name: "RerunInputCorruptRecord",
        doc: "The stored envelope, its input or its options fail their checks; detail names why.\nNothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("detail", "str")],
        methods: &[],
    },
    StubClass {
        name: "RerunClassNotRegistered",
        doc: "The new task's retention class has no class row. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("class_key", "str")],
        methods: &[],
    },
    StubClass {
        name: "RerunClientFamilyMismatch",
        doc: "The database records the other client family, or none (stored is None). Nothing\nwritten. message names both.",
        constructor: Constructor::NoConstructor,
        properties: &[property("stored", "Literal['PYTHON', 'RUST'] | None"), property("message", "str"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "RerunUnknownStoredFamily",
        doc: "The family row of the database holds a value that is not a client family (stored):\ncorrupt stored data. Nothing written. message names the table, the column and the\nvalue; exception is a CoreContractViolation with the subject rerun_task.",
        constructor: Constructor::NoConstructor,
        properties: &[property("stored", "str"), property("message", "str"), property("exception", CORE_FAILURE_EXCEPTION)],
        methods: &[],
    },
    StubClass {
        name: "RerunCallerKeyRefused",
        doc: "The caller key is not a valid idempotency key; cause names why. Nothing written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("cause", "str")],
        methods: &[],
    },
    StubClass {
        name: "RerunEnqueuedRecord",
        doc: "The new task exists: this write, or this write already run in the caller's transaction.",
        constructor: Constructor::NoConstructor,
        properties: &[property("new_task_id", "uuid.UUID"), property("source_task_id", "uuid.UUID"), property("rerun_root_task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "RerunKeyReplayRecord",
        doc: "The caller key already holds this rerun command, as existing_task_id.",
        constructor: Constructor::NoConstructor,
        properties: &[property("existing_task_id", "uuid.UUID")],
        methods: &[],
    },
    StubClass {
        name: "RerunKeyConflictRecord",
        doc: "The caller key holds another command (the source's own included).",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("reserved_by_task_id", "uuid.UUID")],
        methods: &[],
    },
];

/// The classes of the parent-child frame codec, for the Python child program
/// and the preload process.
const FRAME_CLASSES: &[StubClass] = &[
    StubClass {
        name: "PythonProgram",
        doc: "What a Python child imports before it reports ready.",
        constructor: Constructor::NoConstructor,
        properties: &[property("app_locator", "str"), property("imports", "list[str]"), property("sys_path_roots", "list[str]"), property("parent_sys_path", "list[str]"), property("parent_argv", "list[str]")],
        methods: &[],
    },
    StubClass {
        name: "ChildPool",
        doc: "The connection pool of a child: min_size connections kept open, at most max_size; check\npings each pooled connection at checkout.",
        constructor: Constructor::NoConstructor,
        properties: &[property("min_size", "int"), property("max_size", "int"), property("check", "bool")],
        methods: &[],
    },
    StubClass {
        name: "IsolatedChildKind",
        doc: "An isolated child: one task at a time.",
        constructor: Constructor::NoConstructor,
        properties: &[property("pool", "ChildPool"), property("task_send_pool", "int")],
        methods: &[],
    },
    StubClass {
        name: "SharedAsyncChildKind",
        doc: "A shared async child: up to seats async tasks on one event loop.",
        constructor: Constructor::NoConstructor,
        properties: &[property("seats", "int"), property("pool", "ChildPool"), property("executor_threads", "int"), property("task_send_pool", "int")],
        methods: &[],
    },
    StubClass {
        name: "SharedThreadChildKind",
        doc: "A shared thread child: up to seats blocking tasks, each on a thread.",
        constructor: Constructor::NoConstructor,
        properties: &[property("seats", "int"), property("pool", "ChildPool"), property("task_send_pool", "int")],
        methods: &[],
    },
    StubClass {
        name: "HandlerChildKind",
        doc: "A handler child: it runs the event handlers of the app and no task. A handler may\nsend tasks, through a pool of task_send_pool connections.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_send_pool", "int")],
        methods: &[],
    },
    StubClass {
        name: "InitFrame",
        doc: "The first frame to a child or to the preload process. The database URL and the\nconnection settings stay in the core.",
        constructor: Constructor::NoConstructor,
        properties: &[property("program", "PythonProgram | Literal['RUST']"), property("log_level", "Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']"), property("process_name", "str"), property("child_kind", "IsolatedChildKind | SharedAsyncChildKind | SharedThreadChildKind | HandlerChildKind")],
        methods: &[],
    },
    StubClass {
        name: "StoredOptionsRefused",
        doc: "Stored options of the row that the worker cannot use: the child sets the row\nRUNNING and fails the task before it resolves the task.",
        constructor: Constructor::NoConstructor,
        properties: &[property("kind", "Literal['OPTIONS_UNREADABLE', 'TIMEOUT_VALUE_INVALID', 'CANCEL_GRACE_INVALID']"), property("reason", "str")],
        methods: &[],
    },
    StubClass {
        name: "StoredTraceCarrier",
        doc: "The trace carrier that the send stored on the task row: the keys and the values that\nthe propagator of the sender wrote. The child extracts it with the app's propagator.",
        constructor: Constructor::NoConstructor,
        properties: &[property("carrier", "dict[str, str]")],
        methods: &[],
    },
    StubClass {
        name: "RunFrame",
        doc: "Run one task in an isolated child. The parent holds the deadline.",
        constructor: Constructor::NoConstructor,
        properties: &[property("task_id", "uuid.UUID"), property("task_name", "str"), property("queue_name", "str"), property("attempt", "int"), property("worker_id", "str"), property("claimed_at", "datetime.datetime"), property("orphan_self_heal", "bool"), property("work", "Literal['RUN'] | StoredOptionsRefused"), property("trace", "StoredTraceCarrier | Literal['NO_CARRIER']"), property("input", "str")],
        methods: &[],
    },
    StubClass {
        name: "AsyncTaskWork",
        doc: "Cancel the task timeout_ms after its start, then give it cancel_grace_ms to stop.",
        constructor: Constructor::NoConstructor,
        properties: &[property("timeout_ms", "int"), property("cancel_grace_ms", "int")],
        methods: &[],
    },
    StubClass {
        name: "ThreadTaskWork",
        doc: "Report the deadline timeout_ms after the child takes the run.",
        constructor: Constructor::NoConstructor,
        properties: &[property("timeout_ms", "int")],
        methods: &[],
    },
    StubClass {
        name: "SharedRunFrame",
        doc: "Run one task on a free seat of a shared child.",
        constructor: Constructor::NoConstructor,
        properties: &[property("dispatch", "int"), property("task_id", "uuid.UUID"), property("task_name", "str"), property("queue_name", "str"), property("attempt", "int"), property("worker_id", "str"), property("claimed_at", "datetime.datetime"), property("orphan_self_heal", "bool"), property("work", "AsyncTaskWork | ThreadTaskWork | StoredOptionsRefused"), property("trace", "StoredTraceCarrier | Literal['NO_CARRIER']"), property("input", "str")],
        methods: &[],
    },
    StubClass {
        name: "EventsFrame",
        doc: "Lifecycle events for a handler child, in the order that the core emitted them; events\nholds the JSON text of each event.",
        constructor: Constructor::NoConstructor,
        properties: &[property("events", "list[str]")],
        methods: &[],
    },
    StubClass {
        name: "StopFrame",
        doc: "Exit normally. Sent only to a child with no task.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "FrameRefused",
        doc: "A parent frame that the child refuses: the child exits with the bad-frame code.",
        constructor: Constructor::NoConstructor,
        properties: &[property("reason", "str")],
        methods: &[],
    },
    StubClass {
        name: "ParentProtocolMismatch",
        doc: "An init frame of another protocol version: the child logs both numbers and exits\nwith the protocol-mismatch code.",
        constructor: Constructor::NoConstructor,
        properties: &[property("received", "int | Literal['NOT_NAMED']"), property("expected", "int")],
        methods: &[],
    },
    StubClass {
        name: "FrameOverBound",
        doc: "A header over the bounds: nothing was allocated for its frame, and the channel\ncannot be read on.",
        constructor: Constructor::NoConstructor,
        properties: &[property("meta_bytes", "int"), property("body_bytes", "int")],
        methods: &[],
    },
    StubClass {
        name: "FrameTooLarge",
        doc: "A frame that cannot be sent: its metadata or its body is over a bound of the\nheader. Nothing was encoded.",
        constructor: Constructor::NoConstructor,
        properties: &[property("part", "Literal['METADATA', 'BODY']"), property("bytes", "int"), property("bound", "int")],
        methods: &[],
    },
    StubClass {
        name: "ProtocolNumbers",
        doc: "The protocol version, the exit codes of a child, the spawn request byte, and the bounds\nthat a child applies before it builds a frame: the names of a report of left work, the\nbytes of a body, the handlers of a handler child and the characters of a handler name.",
        constructor: Constructor::NoConstructor,
        properties: &[property("protocol_version", "int"), property("parent_gone_exit_code", "int"), property("bad_parent_frame_exit_code", "int"), property("bad_arguments_exit_code", "int"), property("stopped_with_executor_threads_exit_code", "int"), property("boot_failed_exit_code", "int"), property("reparent_failed_exit_code", "int"), property("protocol_mismatch_exit_code", "int"), property("core_stopped_exit_code", "int"), property("child_hook_failed_exit_code", "int"), property("spawn_request_byte", "int"), property("max_leftover_task_names", "int"), property("max_body_bytes", "int"), property("max_event_handlers", "int"), property("max_handler_name_chars", "int")],
        methods: &[],
    },
    StubClass {
        name: "FrameReader",
        doc: "The read buffer of the channel from the worker parent, with the bounds of the core\nreader.",
        constructor: Constructor::Parameters(&[]),
        properties: &[],
        methods: &[
            StubFunction {
                name: "feed",
                parameters: &[positional("data", "bytes")],
                returns: TypeText::Plain("list[InitFrame | RunFrame | SharedRunFrame | EventsFrame | StopFrame] | FrameOverBound | FrameRefused | ParentProtocolMismatch"),
                doc: "Take the bytes that arrived and give the whole parent frames they complete, in\norder. A refusal means the channel cannot be read on.",
            },
        ],
    },
    StubClass {
        name: "TaskResult",
        doc: "The task produced a result envelope: a value or a task error.",
        constructor: Constructor::Parameters(&[keyword("envelope", "str")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "StoppedBeforeRunning",
        doc: "The child stopped before it set the row RUNNING; no task code ran.",
        constructor: Constructor::Parameters(&[keyword("reason", "Literal['CLAIM_LOST', 'OWNERSHIP_UNCONFIRMED', 'WORKFLOW_STOPPED', 'TASK_EXPIRED', 'NODE_NOT_LINKED', 'ORPHAN_CANCELLED']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "WorkerFailure",
        doc: "The worker code of the child failed, not the task. The detail is cut to its bound.",
        constructor: Constructor::Parameters(&[keyword("detail", "str")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "LoopHoldsTasks",
        doc: "count asyncio tasks that are not done are on a task loop; names holds\nmin(count, 10) names, each cut to its bound.",
        constructor: Constructor::Parameters(&[keyword("count", "int"), keyword("names", "list[str]")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "StuckWork",
        doc: "The work of a run that did not stop after its cancel rounds: at least one task or\nexecutor item; names holds min(tasks, 10) names.",
        constructor: Constructor::Parameters(&[keyword("tasks", "int"), keyword("executor_items", "int"), keyword("names", "list[str]")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "BlockingRun",
        doc: "The run whose task held a blocked loop.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ResourceShortage",
        doc: "A boot step failed with an errno of a resource limit.",
        constructor: Constructor::Parameters(&[keyword("errno", "Literal['EMFILE', 'ENFILE', 'ENOMEM', 'EAGAIN']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ReadyFrame",
        doc: "Boot finished; the child can take a task.",
        constructor: Constructor::Parameters(&[keyword("pid", "int"), keyword("rss_mb", "float | Literal['UNREADABLE']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "BootFailedFrame",
        doc: "In place of the ready frame: the boot failed, and the child exits. Texts are cut\nto their bounds.",
        constructor: Constructor::Parameters(&[keyword("cause", "ResourceShortage | Literal['APP_LOAD_FAILED', 'CHILD_POOL_SETUP_FAILED', 'TASK_LOOP_CREATE_FAILED', 'THREAD_START_REFUSED']"), keyword("error_name", "str"), keyword("detail", "str")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ResultFrame",
        doc: "An isolated task ended and left no work on a task loop. started_at is the start time\nthat the start write returned, or NOT_REPORTED for a run that stopped before running.",
        constructor: Constructor::Parameters(&[keyword("task_id", "uuid.UUID"), keyword("end", "TaskResult | StoppedBeforeRunning | WorkerFailure"), keyword("started_at", "datetime.datetime | Literal['NOT_REPORTED']"), keyword("rss_mb", "float | Literal['UNREADABLE']"), keyword("task_loop", "Literal['NO_TASK_LOOP', 'LOOP_HOLDS_NO_TASK']"), keyword("child", "Literal['SERVES_ON', 'ENDS_ITSELF']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "BodyReturnedFrame",
        doc: "The body of an isolated task returned, and work it started is still on the task\nloop.",
        constructor: Constructor::Parameters(&[keyword("task_id", "uuid.UUID"), keyword("returned", "TaskResult | WorkerFailure"), keyword("started_at", "datetime.datetime"), keyword("left", "LoopHoldsTasks")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "LeftWorkEndedFrame",
        doc: "The work that an isolated task left has ended.",
        constructor: Constructor::Parameters(&[keyword("task_id", "uuid.UUID"), keyword("end", "Literal['IN_GRACE', 'CANCELLED']"), keyword("rss_mb", "float | Literal['UNREADABLE']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "LeftWorkStuckFrame",
        doc: "The work that an isolated task left did not stop after the cancel rounds.",
        constructor: Constructor::Parameters(&[keyword("task_id", "uuid.UUID"), keyword("still", "LoopHoldsTasks")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "SharedResultFrame",
        doc: "A run of a shared child ended; its seat is free. started_at is the start time that the\nstart write returned, or NOT_REPORTED for a run that stopped before running.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID"), keyword("end", "TaskResult | StoppedBeforeRunning | WorkerFailure | Literal['TIMED_OUT']"), keyword("started_at", "datetime.datetime | Literal['NOT_REPORTED']"), keyword("rss_mb", "float | Literal['UNREADABLE']"), keyword("child", "Literal['SERVES_ON', 'ENDS_ITSELF']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "DeadlineFiredFrame",
        doc: "The deadline of a run fired.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "SecondCancelFrame",
        doc: "The cancel grace of a run ran out while it was alive.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "RunStuckFrame",
        doc: "A run did not stop after its second cancel and its cancel rounds.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID"), keyword("work", "StuckWork")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "LeftoverCancelFrame",
        doc: "The body of a run returned before its deadline, and its marked work was alive one\ncancel grace later.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID"), keyword("returned", "TaskResult | WorkerFailure"), keyword("started_at", "datetime.datetime")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "LeftoverStuckFrame",
        doc: "The work a run left after its return did not stop after its cancel rounds.",
        constructor: Constructor::Parameters(&[keyword("dispatch", "int"), keyword("task_id", "uuid.UUID"), keyword("work", "StuckWork"), keyword("returned", "TaskResult | WorkerFailure"), keyword("started_at", "datetime.datetime")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "LoopBlockedFrame",
        doc: "The event loop of a shared async child has not turned for blocked_ms. The location\nis cut to its bound.",
        constructor: Constructor::Parameters(&[keyword("culprit", "BlockingRun | Literal['UNKNOWN']"), keyword("blocked_ms", "int"), keyword("location", "str")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "HandlerCounts",
        doc: "The cumulative counts of one event handler: errors for which it failed, dropped events\nthat its full queue dropped. The name is 1 to max_handler_name_chars characters.",
        constructor: Constructor::Parameters(&[keyword("handler", "str"), keyword("errors", "int"), keyword("dropped", "int")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "HandlerStatsFrame",
        doc: "The counts of each event handler of a handler child since it started, each name once,\nand its RSS.",
        constructor: Constructor::Parameters(&[keyword("handlers", "list[HandlerCounts]"), keyword("span_read_failed", "int"), keyword("span_not_read", "int"), keyword("rss_mb", "float | Literal['UNREADABLE']")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "PreloadReadyFrame",
        doc: "Imports are done, so forks can start.",
        constructor: Constructor::Parameters(&[keyword("pid", "int")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ChildForkedFrame",
        doc: "The child exists, and it has left the preload process.",
        constructor: Constructor::Parameters(&[keyword("pid", "int")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "ForkFailedFrame",
        doc: "This fork failed. The preload process stays up. The reason is cut to its bound.",
        constructor: Constructor::Parameters(&[keyword("reason", "str")]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "PreloadRefusedFrame",
        doc: "The preload process cannot fork safely, and it exits. The reason is cut to its\nbound.",
        constructor: Constructor::Parameters(&[keyword("reason", "str")]),
        properties: &[],
        methods: &[],
    },
];

const WORKER_BOOT_CAUSE: &str = "Literal['SCHEMA_REFUSED', 'CLIENT_FAMILY_REFUSED', \
     'STATEMENT_POOLED_URL', 'SESSION_NOT_CAPABLE', 'START_TIMED_OUT', 'DATABASE_UNAVAILABLE', \
     'DATABASE_REFUSED', 'BOOT_ATTEMPTS_EXHAUSTED', 'CHILD_HOOK_FAILED', 'PRELOAD_REFUSED', \
     'BASELINE_OVER_LIMIT', 'PLACEMENT_REFUSED', 'SPAWNER_START_FAILED', 'CONFIG_REFUSED', \
     'LISTENER_REFUSED']";
const WORKER_FAIL_CAUSE: &str = "Literal['SERVICE_LOOP_DIED', 'RUNNER_HEARTBEAT_LOST', \
     'POOL_FAILED', 'BASELINE_OVER_LIMIT', 'CLAIM_FAILED']";
const WORKER_RUN_OUTCOME: &str = "WorkerStopped | WorkerBootFailed | WorkerFailed";

const PREPARED_WORKER_METHODS: &[StubFunction] = &[
    StubFunction {
        name: "run",
        parameters: &[],
        returns: TypeText::Plain(WORKER_RUN_OUTCOME),
        doc: "Build the worker and run it until a stop request or a failure. Main thread only: the\nsignal handlers run between wait slices, and there is no total bound. Raises\nNotMainThread off the main thread, RuntimeError for a second run, and, after the run\nended, the exception of a signal handler or the interrupt that a child build raised.",
    },
    StubFunction {
        name: "request_stop",
        parameters: &[],
        returns: TypeText::Plain("StopRequested"),
        doc: "Ask the run to stop: the claim loop ends and the children drain within the profile's\ndrain timeout. Idempotent; any thread; returns at once.",
    },
];

/// The classes of the worker run.
const WORKER_CLASSES: &[StubClass] = &[
    StubClass {
        name: "PreparedWorker",
        doc: "The worker of one worker manifest, not run yet. host.on_started() runs once per run on\nthe callback thread, after the start and before the first claim; an exception from it is\nlogged at ERROR and the worker runs on. host.build_child_spec(request) builds each\nsub-workflow child that a workflow step of the worker starts, on the callback thread.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: PREPARED_WORKER_METHODS,
    },
    StubClass {
        name: "WorkerProfileRefused",
        doc: "The profile of the manifest is refused: the profile set fails its checks, or the start\nnames no profile or an unknown one. refusals are the lines of the report.",
        constructor: Constructor::NoConstructor,
        properties: &[property("refusals", "list[str]")],
        methods: &[],
    },
    StubClass {
        name: "WorkerStopped",
        doc: "The run ended at a stop request.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "WorkerBootFailed",
        doc: "The start of the worker failed; it claimed nothing. message names the subject and the\ncause.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("cause", WORKER_BOOT_CAUSE),
            property("message", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "WorkerFailed",
        doc: "The worker was ready, then failed; its running tasks drained first. message names the\nsubject and the cause.",
        constructor: Constructor::NoConstructor,
        properties: &[
            property("cause", WORKER_FAIL_CAUSE),
            property("message", "str"),
        ],
        methods: &[],
    },
    StubClass {
        name: "StopRequested",
        doc: "The stop request is recorded.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
];

const FLEET_METRICS_SERVER_METHODS: &[StubFunction] = &[
    StubFunction {
        name: "run",
        parameters: &[],
        returns: TypeText::Plain("FleetMetricsStopped | FleetMetricsFailed"),
        doc: "Log one WARNING line that names each queue or schedule name that is not declared, bind\nthe address, and serve the fleet metrics until a stop request. Main thread only: the\nsignal handlers run between wait slices, and there is no total bound. A failed read is a\nfailure gauge in the text. Raises NotMainThread off the main thread, RuntimeError for a\nsecond run, and, after the run ended, the exception of a signal handler.",
    },
    StubFunction {
        name: "request_stop",
        parameters: &[],
        returns: TypeText::Plain("StopRequested"),
        doc: "Ask the run to stop: the requests in flight end, then the run answers\nFleetMetricsStopped. Idempotent; any thread; returns at once.",
    },
];

/// The classes of the fleet metrics server (`horsies metrics`).
const FLEET_METRICS_CLASSES: &[StubClass] = &[
    StubClass {
        name: "ListenAddress",
        doc: "A HOST:PORT address of a metrics endpoint; an IPv6 host without its brackets.",
        constructor: Constructor::NoConstructor,
        properties: &[property("host", "str"), property("port", "int")],
        methods: &[],
    },
    StubClass {
        name: "ListenAddressRefused",
        doc: "A text that is not a HOST:PORT address. message names the text and the rule.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str")],
        methods: &[],
    },
    StubClass {
        name: "FleetMetricsServer",
        doc: "The fleet metrics server of one CoreHandle, not run yet. It reads the fleet state at\nmost once per collect interval and serves the last read at GET /metrics.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: FLEET_METRICS_SERVER_METHODS,
    },
    StubClass {
        name: "FleetMetricsStopped",
        doc: "The run ended at a stop request.",
        constructor: Constructor::NoConstructor,
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "FleetMetricsFailed",
        doc: "The address did not bind. message names the address and the cause.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str")],
        methods: &[],
    },
];

const CLASS_COUNT: usize = HANDLE_CLASSES.len()
    + ARGUMENT_CLASSES.len()
    + RECORD_CLASSES.len()
    + MONITORING_CLASSES.len()
    + WORKFLOW_CLASSES.len()
    + HISTORY_CLASSES.len()
    + FRAME_CLASSES.len()
    + WORKER_CLASSES.len()
    + FLEET_METRICS_CLASSES.len();

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
    let history_start = workflow_start + WORKFLOW_CLASSES.len();
    let mut history_position = 0;
    while history_position < HISTORY_CLASSES.len() {
        classes[history_start + history_position] = HISTORY_CLASSES[history_position];
        history_position += 1;
    }
    let frame_start = history_start + HISTORY_CLASSES.len();
    let mut frame_position = 0;
    while frame_position < FRAME_CLASSES.len() {
        classes[frame_start + frame_position] = FRAME_CLASSES[frame_position];
        frame_position += 1;
    }
    let worker_start = frame_start + FRAME_CLASSES.len();
    let mut worker_position = 0;
    while worker_position < WORKER_CLASSES.len() {
        classes[worker_start + worker_position] = WORKER_CLASSES[worker_position];
        worker_position += 1;
    }
    let fleet_metrics_start = worker_start + WORKER_CLASSES.len();
    let mut fleet_metrics_position = 0;
    while fleet_metrics_position < FLEET_METRICS_CLASSES.len() {
        classes[fleet_metrics_start + fleet_metrics_position] =
            FLEET_METRICS_CLASSES[fleet_metrics_position];
        fleet_metrics_position += 1;
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
        name: "decode_parent_frame",
        parameters: &[positional("meta", "bytes"), positional("body", "bytes")],
        returns: TypeText::Plain("InitFrame | RunFrame | SharedRunFrame | EventsFrame | StopFrame | FrameRefused | ParentProtocolMismatch"),
        doc: "Decode one parent frame from its metadata and its body. Pure.",
    },
    StubFunction {
        name: "decode_rerun_write",
        parameters: &[positional("write", "RerunWriteRecord"), positional("row", "tuple[object, ...]")],
        returns: TypeText::Plain("RerunEnqueuedRecord | RerunKeyReplayRecord | RerunKeyConflictRecord"),
        doc: "The outcome of the row of a rerun write (a tuple in column order). Pure. A row that does\nnot fit the columns raises CoreDefect; an answer that contradicts the minted id raises\nPanicException.",
    },
    StubFunction {
        name: "encode_child_frame",
        parameters: &[positional("frame", "ReadyFrame | BootFailedFrame | ResultFrame | BodyReturnedFrame | LeftWorkEndedFrame | LeftWorkStuckFrame | SharedResultFrame | DeadlineFiredFrame | SecondCancelFrame | RunStuckFrame | LeftoverCancelFrame | LeftoverStuckFrame | LoopBlockedFrame | HandlerStatsFrame")],
        returns: TypeText::Plain("bytes | FrameTooLarge"),
        doc: "Encode a child frame: its bytes, or FrameTooLarge when it is over a bound of the\nheader. Pure.",
    },
    StubFunction {
        name: "encode_preload_frame",
        parameters: &[positional("frame", "PreloadReadyFrame | ChildForkedFrame | ForkFailedFrame | PreloadRefusedFrame")],
        returns: TypeText::Plain("bytes"),
        doc: "Encode a preload frame: its bytes. Pure.",
    },
    StubFunction {
        name: "mint_task_id",
        parameters: &[],
        returns: TypeText::Plain("uuid.UUID | IdMintRefused"),
        doc: "Mint a new task id (UUIDv7). Starts no runtime.",
    },
    StubFunction {
        name: "parse_listen_address",
        parameters: &[positional("text", "str")],
        returns: TypeText::Plain("ListenAddress | ListenAddressRefused"),
        doc: "The address of text: HOST:PORT or [IPV6]:PORT, the port 1 to 65535. Pure: starts no\nruntime. The host is resolved at the bind.",
    },
    StubFunction {
        name: "prepare_fleet_metrics",
        parameters: &[positional("handle", "CoreHandle"), keyword("listen", "ListenAddress"), keyword("queue_names", "list[str]"), keyword("schedule_names", "list[str]")],
        returns: TypeText::Plain("FleetMetricsServer"),
        doc: "The fleet metrics server of the database of handle on listen, with the queue and\nschedule names of the app config as label values. A name that cannot be a label value of\nits own (empty, (unknown), or past the bound of names) is not declared: its samples count\nin the (unknown) series. Pure: starts no runtime.",
    },
    StubFunction {
        name: "prepare_rerun_write",
        parameters: &[positional("read", "PreparedStatement"), positional("row", "tuple[object, ...]"), positional("command", "RerunCommandRecord"), positional("policy", "RerunPolicyRecord"), keyword("sent_at", "datetime.datetime")],
        returns: TypeText::Plain("RerunWriteRecord | RerunSourceLiveRecord | RerunSourceAbsentRecord | RerunSourceCorruptRecord | RerunNotEligibleRecord | RerunInputUnavailableRecord | RerunInputCorruptRecord | RerunClassNotRegistered | RerunClientFamilyMismatch | RerunUnknownStoredFamily | RerunCallerKeyRefused | IdMintRefused"),
        doc: "Decide the rerun from the row of its read (a tuple in column order): the write, or the\nanswer that ends it with nothing written. Pure apart from the id mint. A row that does not\nfit the columns raises CoreDefect.",
    },
    StubFunction {
        name: "prepare_worker",
        parameters: &[positional("manifest_json", "str"), keyword("host", "object")],
        returns: TypeText::Plain("PreparedWorker | WorkerProfileRefused"),
        doc: "Parse a worker manifest and pick its profile. Pure: starts no runtime. host has the\nmethods on_started() and build_child_spec(request). A refused manifest raises\nManifestRefused.",
    },
    StubFunction {
        name: "protocol_numbers",
        parameters: &[],
        returns: TypeText::Plain("ProtocolNumbers"),
        doc: "The protocol version, the exit codes of a child and the spawn request byte. Pure.",
    },
    StubFunction {
        name: "refresh_log_levels",
        parameters: &[],
        returns: TypeText::Plain("None"),
        doc: "Read the effective level of every horsies logger again.",
    },
    StubFunction {
        name: "rerun_read",
        parameters: &[positional("command", "RerunCommandRecord"), positional("policy", "RerunPolicyRecord")],
        returns: TypeText::Plain("PreparedStatement"),
        doc: "The source read of a rerun as a PreparedStatement. Pure: starts no runtime.",
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
    StubFunction {
        name: "trace_link_refusal",
        parameters: TRACE_LINK_REFUSAL_PARAMETERS,
        returns: TypeText::Plain("str | None"),
        doc: "Why the core refuses template as the trace link template of the monitoring page, or\nNone when it accepts it. Pure.",
    },
];

/// The surface of `horsies._core`.
const STATIC_METHODS: &[StubStaticMethod] = &[StubStaticMethod {
    class_name: "CoreChildHandle",
    function: StubFunction {
        name: "from_init",
        parameters: &[positional("init", "InitFrame")],
        returns: TypeText::Plain("CoreChildHandle"),
        doc: "The child handle of the task child that init boots: the database URL, the connection\nsettings and the pool of the child kind come from the frame and stay in the core. Starts\nnothing. The init frame of a handler child raises ValueError: it runs no task.",
    },
}];

/// The generated classes of `horsies._catalog` that the annotations name.
const IMPORTS: &[StubImport] = &[
    StubImport {
        module: "horsies._catalog.broker_errors",
        names: &[
            "BrokerCoreEnqueueError",
            "BrokerInvalidInput",
            "BrokerInvalidJsonPayload",
            "BrokerListenerDisabled",
            "BrokerOperationFailed",
        ],
    },
    StubImport {
        module: "horsies._catalog.monitoring_errors",
        names: &["MonitoringQueryCoreError", "TaskActionCoreError"],
    },
    StubImport {
        module: "horsies._catalog.workflow_handle_errors",
        names: &[
            "HandleDbOperationFailed",
            "HandleResumeRecoveryFailed",
            "HandleStoredDataInvalid",
        ],
    },
    StubImport {
        module: "horsies._catalog.workflow_start_errors",
        names: &["WorkflowStartEnqueueFailed", "WorkflowStartIdConflict"],
    },
];

pub const CORE_MODULE: ModuleSurface = ModuleSurface {
    imports: IMPORTS,
    exceptions: EXCEPTIONS,
    classes: &CLASSES,
    static_methods: STATIC_METHODS,
    functions: FUNCTIONS,
};
