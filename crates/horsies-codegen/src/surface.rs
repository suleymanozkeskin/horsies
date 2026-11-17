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

const RETENTION_PERIOD_PARAMETERS: &[StubParameter] = &[positional("days", "int")];

const CORE_HANDLE_PARAMETERS: &[StubParameter] = &[positional("manifest_json", "str")];

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
    keyword(
        "retention",
        "RetentionDefault | RetentionForever | RetentionPeriod",
    ),
    keyword("rerun_input", "Literal['APP_DEFAULT', 'KEEP', 'DROP']"),
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
const TASK_INFO_PARAMETERS: &[StubParameter] = &[
    positional("task_id", "uuid.UUID"),
    keyword("include_result", "bool"),
    keyword("include_failed_reason", "bool"),
    keyword("include_attempts", "bool"),
];

const PING_OUTCOME: &str = "DatabasePingRecord | BrokerFailure";
const INSTALL_OUTCOME: &str = "SchemaInstalled | SchemaInstallNotRun | BrokerFailure";
const SEND_OUTCOME: &str = "Enqueued | SendSuppressed | SendValidationFailed | \
     SendEnqueueFailed | SendPayloadMismatch | SendPayloadTooLarge | SendKeyConflict";
const TASK_INFO_OUTCOME: &str = "TaskInfoRecord | TaskInfoNotFound | BrokerFailure";
const REQUEUE_STALE_CLAIMED_OUTCOME: &str =
    "StaleClaimsDrained | StaleClaimsBoundReached | StaleClaimsRequeueFailed";
const RELEASE_CLAIMS_OUTCOME: &str = "ClaimsReleased | ReleaseFailed";

const CORE_HANDLE_METHODS: [StubFunction; 12] = [
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
        doc: "Enqueue one task. Writes; a resend of the same request is safe.",
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
        name: "release_claims_async",
        parameters: RELEASE_CLAIMS_PARAMETERS,
        returns: TypeText::FutureOf(RELEASE_CLAIMS_OUTCOME),
        doc: "Release the claims of one request. Writes, in one statement.",
    },
    StubFunction {
        name: "task_info",
        parameters: TASK_INFO_PARAMETERS,
        returns: TypeText::Plain(TASK_INFO_OUTCOME),
        doc: "The info of one task. Reads only.",
    },
    StubFunction {
        name: "task_info_async",
        parameters: TASK_INFO_PARAMETERS,
        returns: TypeText::FutureOf(TASK_INFO_OUTCOME),
        doc: "The awaitable form of the method above.",
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
        name: "PanicException",
        base: "BaseException",
        doc: "Rust code called from Python panicked.",
    },
];

const ARGUMENT_CLASSES: [StubClass; 8] = [
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
        name: "RetentionDefault",
        doc: "Keep the history record for the default period.",
        constructor: Constructor::Parameters(&[]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "RetentionForever",
        doc: "Keep the history record forever.",
        constructor: Constructor::Parameters(&[]),
        properties: &[],
        methods: &[],
    },
    StubClass {
        name: "RetentionPeriod",
        doc: "Keep the history record for a period of whole days (1 to 1825).",
        constructor: Constructor::Parameters(RETENTION_PERIOD_PARAMETERS),
        properties: &[property("days", "int")],
        methods: &[],
    },
    StubClass {
        name: "EnqueueRequest",
        doc: "One frozen send. None for good_until, task_options_json or idempotency_key\nmeans no deadline, no options, no key.",
        constructor: Constructor::Parameters(ENQUEUE_REQUEST_PARAMETERS),
        properties: &[property("task_id", "uuid.UUID")],
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

const RECORD_CLASSES: [StubClass; 23] = [
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
        doc: "The input or the options were refused. Nothing was written.",
        constructor: Constructor::NoConstructor,
        properties: &[property("message", "str")],
        methods: &[],
    },
    StubClass {
        name: "SendEnqueueFailed",
        doc: "The database failed during the enqueue; the row may or may not exist.",
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
        doc: "The metadata of one task, with the fields of TaskInfo. result_json is the\nstored result envelope as JSON text.",
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
        ],
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
];

const HANDLE_CLASSES: [StubClass; 1] = [StubClass {
    name: "CoreHandle",
    doc: "The core handle of one producer manifest. Building it starts nothing.",
    constructor: Constructor::Parameters(CORE_HANDLE_PARAMETERS),
    properties: &[property("manifest_version", "int")],
    methods: &CORE_HANDLE_METHODS,
}];

const CLASSES: [StubClass; 32] = {
    let mut classes = [HANDLE_CLASSES[0]; 32];
    let mut position = 0;
    while position < ARGUMENT_CLASSES.len() {
        classes[1 + position] = ARGUMENT_CLASSES[position];
        position += 1;
    }
    let mut record_position = 0;
    while record_position < RECORD_CLASSES.len() {
        classes[1 + ARGUMENT_CLASSES.len() + record_position] = RECORD_CLASSES[record_position];
        record_position += 1;
    }
    classes
};

const FUNCTIONS: &[StubFunction] = &[
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
