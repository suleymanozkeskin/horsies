pub mod diagnostic;

use serde::{Deserialize, Serialize};

use crate::catalog::CodeEnum;

/// Error category corresponding to Python's exception subclass hierarchy.
///
/// Python has four exception subclasses of `HorsiesError`:
/// - `WorkflowValidationError` -- workflow spec validation failures
/// - `TaskDefinitionError` -- task definition problems
/// - `ConfigurationError` -- app/broker configuration issues
/// - `RegistryError` -- task/workflow registry operations
///
/// In Rust, rather than separate error types, we use this enum for
/// type-level discrimination via `match error.category()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// Workflow validation errors (HRS-001-HRS-099). Maps to Python's `WorkflowValidationError`.
    Workflow,
    /// Task definition errors (HRS-100-HRS-199). Maps to Python's `TaskDefinitionError`.
    Task,
    /// Configuration/broker errors (HRS-200-HRS-299). Maps to Python's `ConfigurationError`.
    Config,
    /// Registry errors (HRS-300-HRS-399). Maps to Python's `RegistryError`.
    Registry,
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Workflow => write!(f, "workflow validation"),
            Self::Task => write!(f, "task definition"),
            Self::Config => write!(f, "configuration"),
            Self::Registry => write!(f, "registry"),
        }
    }
}

/// Error codes for startup/validation errors.
///
/// Organized by category:
/// - HRS-001-HRS-099: Workflow validation
/// - HRS-100-HRS-199: Task definition
/// - HRS-200-HRS-299: Config/broker
/// - HRS-300-HRS-399: Registry
///
/// HRS-023 and HRS-106 are retired and are not used again.
///
/// This enum is the one list of the codes of both clients. `horsies-codegen`
/// renders it as `horsies._catalog.error_codes.ErrorCode`, the Python
/// `ErrorCode`: the member name is the variant name in SCREAMING_SNAKE_CASE,
/// the value is the `HRS-nnn` code, and the docstring is the variant doc.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    horsies_core_derive::CodeCatalog,
)]
#[code_catalog(module = "error_codes", base = "str_enum")]
pub enum ErrorCode {
    // Workflow validation (HRS-001-HRS-099)
    /// The workflow has no name.
    #[code_catalog(value = "HRS-001")]
    WorkflowNoName,
    /// The workflow has no nodes.
    #[code_catalog(value = "HRS-002")]
    WorkflowNoNodes,
    /// A node id reference is not valid, or `key()` is called on a node with
    /// no `node_id`.
    #[code_catalog(value = "HRS-003")]
    WorkflowInvalidNodeId,
    /// Two nodes have the same node id.
    #[code_catalog(value = "HRS-004")]
    WorkflowDuplicateNodeId,
    /// The workflow has no root node.
    #[code_catalog(value = "HRS-005")]
    WorkflowNoRootTasks,
    /// A dependency is not valid.
    #[code_catalog(value = "HRS-006")]
    WorkflowInvalidDependency,
    /// The dependencies of the workflow make a cycle.
    #[code_catalog(value = "HRS-007")]
    WorkflowCycleDetected,
    /// An `args_from` reference is not valid.
    #[code_catalog(value = "HRS-008")]
    WorkflowInvalidArgsFrom,
    /// A `workflow_context_from` reference is not valid.
    #[code_catalog(value = "HRS-009")]
    WorkflowInvalidCtxFrom,
    /// The workflow context parameter is missing.
    #[code_catalog(value = "HRS-010")]
    WorkflowCtxParamMissing,
    /// The output of the workflow is not valid.
    #[code_catalog(value = "HRS-011")]
    WorkflowInvalidOutput,
    /// The success policy is not valid.
    #[code_catalog(value = "HRS-012")]
    WorkflowInvalidSuccessPolicy,
    /// The join setting is not valid.
    #[code_catalog(value = "HRS-013")]
    WorkflowInvalidJoin,
    /// The queue of a node is not resolved.
    #[code_catalog(value = "HRS-014")]
    WorkflowUnresolvedQueue,
    /// The priority of a node is not resolved.
    #[code_catalog(value = "HRS-015")]
    WorkflowUnresolvedPriority,
    /// A workflow definition or spec has no `definition_key`.
    #[code_catalog(value = "HRS-016")]
    WorkflowNoDefinitionKey,
    /// Two workflow definitions have the same `definition_key`.
    #[code_catalog(value = "HRS-017")]
    WorkflowDuplicateDefinitionKey,
    /// A sub-workflow has no app reference.
    #[code_catalog(value = "HRS-018")]
    WorkflowSubworkflowAppMissing,
    /// A node input or an `args_from` names a key that the task or the child
    /// workflow does not take.
    #[code_catalog(value = "HRS-019")]
    WorkflowInvalidKwargKey,
    /// A node does not give a required parameter of its task or child
    /// workflow.
    #[code_catalog(value = "HRS-020")]
    WorkflowMissingRequiredParams,
    /// A node's input object and its `args_from` name the same member.
    #[code_catalog(value = "HRS-021")]
    WorkflowKwargsArgsFromOverlap,
    /// A sub-workflow node gives params to a child workflow whose definition
    /// takes none.
    #[code_catalog(value = "HRS-022")]
    WorkflowSubworkflowParamsRequireBuildWith,
    /// The result type of an `args_from` source does not match the type of
    /// the parameter that it fills.
    #[code_catalog(value = "HRS-024")]
    WorkflowArgsFromTypeMismatch,
    /// The type of the output node does not match the output type of the
    /// workflow spec.
    #[code_catalog(value = "HRS-025")]
    WorkflowOutputTypeMismatch,
    /// A node's input is positional (not a JSON object) while the node takes
    /// `args_from` or `workflow_context_from` members, which merge only into an
    /// object.
    #[code_catalog(value = "HRS-026")]
    WorkflowPositionalArgsNotSupported,
    /// A workflow builder with parameters has no check cases.
    #[code_catalog(value = "HRS-027")]
    WorkflowCheckCasesRequired,
    /// A check case of a workflow builder is not valid.
    #[code_catalog(value = "HRS-028")]
    WorkflowCheckCaseInvalid,
    /// A workflow builder raised an exception or returned no workflow spec,
    /// or `build_with()` returned a spec that the call did not build.
    #[code_catalog(value = "HRS-029")]
    WorkflowCheckBuilderException,
    /// A function returns a workflow spec and is not decorated as a workflow
    /// builder.
    #[code_catalog(value = "HRS-030")]
    WorkflowCheckUndecoratedBuilder,
    /// A node's static input does not encode, or does not read back with the
    /// input type of its task or child workflow.
    #[code_catalog(value = "HRS-031")]
    WorkflowKwargsNotSerializable,
    /// A given `workflow_id` is not a UUID.
    #[code_catalog(value = "HRS-032")]
    WorkflowInvalidId,
    /// A `WorkflowDefinition` class defines `Meta`, or sets `config` to a
    /// value that is not a `WorkflowDefinitionConfig`.
    #[code_catalog(value = "HRS-033")]
    WorkflowInvalidDefinitionConfig,
    /// A node's priority is outside 1 to 100.
    #[code_catalog(value = "HRS-034")]
    WorkflowInvalidPriority,
    /// A node sets a `good_until` with no time zone.
    #[code_catalog(value = "HRS-035")]
    WorkflowNaiveGoodUntil,
    /// A nesting of sub-workflows passes the depth limit for certain.
    #[code_catalog(value = "HRS-036")]
    WorkflowSubworkflowDepthLimit,
    /// A sub-workflow includes its own definition with the same params, and
    /// no node between the two waits for other nodes.
    #[code_catalog(value = "HRS-037")]
    WorkflowSubworkflowSelfNesting,
    /// A workflow spec has more than `MAX_WORKFLOW_NODES` (10,000) nodes.
    #[code_catalog(value = "HRS-038")]
    WorkflowTooManyNodes,

    // Task definition (HRS-100-HRS-199)
    /// The task function has no return type.
    #[code_catalog(value = "HRS-100")]
    TaskNoReturnType,
    /// The return type of the task function is not a task result.
    #[code_catalog(value = "HRS-101")]
    TaskInvalidReturnType,
    /// A task option is not valid.
    #[code_catalog(value = "HRS-102")]
    TaskInvalidOptions,
    /// The queue of the task is not valid.
    #[code_catalog(value = "HRS-103")]
    TaskInvalidQueue,
    /// The task function is pre-decorated: it has a `__wrapped__` chain.
    #[code_catalog(value = "HRS-104")]
    TaskPredecoratedNotSupported,
    /// The task signature fails the strict check: a parameter that is not
    /// keyword-only, a type that is not allowed, or a return type that is not
    /// `Result[OkT, TaskError]`.
    #[code_catalog(value = "HRS-105")]
    TaskStrictSignatureViolation,
    /// `execution=SHARED_ASYNC` on a function defined with `def`.
    #[code_catalog(value = "HRS-107")]
    TaskSharedAsyncRequiresAsyncDef,
    /// `execution=SHARED_THREAD` on a function defined with `async def`.
    #[code_catalog(value = "HRS-108")]
    TaskSharedThreadRequiresDef,
    /// `SHARED_ASYNC` or `SHARED_THREAD` without `timeout_ms`.
    #[code_catalog(value = "HRS-109")]
    TaskSharedExecutionRequiresTimeout,
    /// `cancel_grace_ms` with `execution=SHARED_THREAD`: only `SHARED_ASYNC`
    /// and `ISOLATED` take one, since a thread cannot be stopped.
    #[code_catalog(value = "HRS-110")]
    TaskCancelGraceRequiresSharedAsync,
    /// `cancel_grace_ms` outside 100 to 10,000 ms.
    #[code_catalog(value = "HRS-111")]
    TaskCancelGraceOutOfRange,
    /// A code that a task declares fails the code-text rule: it is empty,
    /// not `UPPER_SNAKE_CASE`, or the name of an exception class. A
    /// reserved name or the name of a built-in code is HRS-212.
    #[code_catalog(value = "HRS-112")]
    TaskInvalidErrorCode,
    /// A task declares one error code more than once.
    #[code_catalog(value = "HRS-113")]
    TaskDuplicateErrorCode,

    // Config/broker (HRS-200-HRS-299)
    /// The queue mode or the custom queue list is not valid.
    #[code_catalog(value = "HRS-200")]
    ConfigInvalidQueueMode,
    /// The cluster capacity is not valid.
    #[code_catalog(value = "HRS-201")]
    ConfigInvalidClusterCap,
    /// The prefetch setting is not valid.
    #[code_catalog(value = "HRS-202")]
    ConfigInvalidPrefetch,
    /// The broker URL is not valid, or the broker connectivity check failed.
    #[code_catalog(value = "HRS-203")]
    BrokerInvalidUrl,
    /// The recovery config is not valid.
    #[code_catalog(value = "HRS-204")]
    ConfigInvalidRecovery,
    /// The schedule config is not valid.
    #[code_catalog(value = "HRS-205")]
    ConfigInvalidSchedule,
    /// The CLI arguments are not valid.
    #[code_catalog(value = "HRS-206")]
    CliInvalidArgs,
    /// The worker locator is not valid.
    #[code_catalog(value = "HRS-207")]
    WorkerInvalidLocator,
    /// The resilience config is not valid.
    #[code_catalog(value = "HRS-208")]
    ConfigInvalidResilience,
    /// An exception mapper is not valid.
    #[code_catalog(value = "HRS-209")]
    ConfigInvalidExceptionMapper,
    /// A module raised an error while it was imported.
    #[code_catalog(value = "HRS-210")]
    ModuleExecError,
    /// The broker failed to start.
    #[code_catalog(value = "HRS-211")]
    BrokerInitFailed,
    /// A config value or a declared task code is a built-in code or a
    /// reserved name.
    #[code_catalog(value = "HRS-212")]
    CheckReservedCodeCollision,
    /// A pool value of a broker config is outside its range, or the config
    /// names a pool field that 0.7.0 moved to the worker profile.
    #[code_catalog(value = "HRS-213")]
    ConfigInvalidBrokerPool,
    /// A child start hook is not callable, or it is an async function.
    #[code_catalog(value = "HRS-214")]
    ConfigInvalidChildHook,
    /// A TCP keepalive value or the TCP user timeout is outside its range.
    #[code_catalog(value = "HRS-215")]
    ConfigInvalidKeepalive,
    /// The retention config is not valid.
    #[code_catalog(value = "HRS-216")]
    ConfigInvalidRetention,
    /// The live check was called from a blocking call on a thread that runs
    /// an event loop or a tokio runtime: the live phases did not run.
    #[code_catalog(value = "HRS-217")]
    CheckLiveAsyncContext,
    /// A worker profile that the app config validation refuses.
    #[code_catalog(value = "HRS-218")]
    ConfigInvalidWorkerProfile,
    /// The live check: one worker of a profile needs more Postgres
    /// connections than the server has usable slots, or the server limit
    /// could not be read.
    #[code_catalog(value = "HRS-219")]
    CheckWorkerConnections,
    /// The app config asks for OpenTelemetry tracing, and the OpenTelemetry
    /// API (`horsies[otel]`) is not installed.
    #[code_catalog(value = "HRS-220")]
    ConfigOtelApiMissing,
    /// A trace link template of the monitoring page that the core refuses.
    #[code_catalog(value = "HRS-221")]
    ConfigInvalidTraceLink,
    /// An event handler registration names a value that is not an event
    /// type. Only the Python client refuses with it: the Rust registration
    /// takes the event type as a type parameter.
    #[code_catalog(value = "HRS-222")]
    EventHandlerUnknownEventType,
    /// An event handler cannot take the events of its type: a value that is
    /// not a function or a bound method, a generator function, a function
    /// that does not take a call with one event, or an event parameter
    /// annotated with another event type. Only the Python client refuses
    /// with it: the Rust compiler checks the handler.
    #[code_catalog(value = "HRS-223")]
    EventHandlerInvalidSignature,
    /// An earlier event handler of the app has the same name.
    #[code_catalog(value = "HRS-224")]
    EventHandlerDuplicateName,
    /// The name of an event handler is empty, longer than 255 characters, or
    /// `(unknown)`; in Python, also a function with no module name.
    #[code_catalog(value = "HRS-225")]
    EventHandlerInvalidName,
    /// The app has 32 event handlers already, the most that one handler
    /// child runs.
    #[code_catalog(value = "HRS-226")]
    EventHandlerLimit,
    /// A payload size bound (`warn_bytes` or `reject_bytes`) is not valid.
    #[code_catalog(value = "HRS-227")]
    ConfigInvalidPayload,

    // Registry (HRS-300-HRS-399)
    /// The task is not registered.
    #[code_catalog(value = "HRS-300")]
    TaskNotRegistered,
    /// Two tasks have the same name.
    #[code_catalog(value = "HRS-301")]
    TaskDuplicateName,
    /// A workflow node names a task that is not registered in this process.
    /// The Rust client refuses `<task>::node()` before
    /// `<task>::register(&mut app)` with it.
    #[code_catalog(value = "HRS-302")]
    WorkflowUnregisteredTask,
}

impl ErrorCode {
    /// Numeric code string (e.g. "HRS-001"): the catalog value of the code.
    pub fn as_code_str(&self) -> &'static str {
        CodeEnum::wire_value(self)
    }
}

impl ErrorCode {
    /// Returns the error category for this code.
    ///
    /// Maps each error code to one of the four categories that correspond
    /// to Python's exception subclass hierarchy.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::WorkflowNoName
            | Self::WorkflowNoNodes
            | Self::WorkflowInvalidNodeId
            | Self::WorkflowDuplicateNodeId
            | Self::WorkflowNoRootTasks
            | Self::WorkflowInvalidDependency
            | Self::WorkflowCycleDetected
            | Self::WorkflowInvalidArgsFrom
            | Self::WorkflowInvalidCtxFrom
            | Self::WorkflowCtxParamMissing
            | Self::WorkflowInvalidOutput
            | Self::WorkflowInvalidSuccessPolicy
            | Self::WorkflowInvalidJoin
            | Self::WorkflowUnresolvedQueue
            | Self::WorkflowUnresolvedPriority
            | Self::WorkflowNoDefinitionKey
            | Self::WorkflowDuplicateDefinitionKey
            | Self::WorkflowSubworkflowAppMissing
            | Self::WorkflowInvalidKwargKey
            | Self::WorkflowMissingRequiredParams
            | Self::WorkflowKwargsArgsFromOverlap
            | Self::WorkflowSubworkflowParamsRequireBuildWith
            | Self::WorkflowArgsFromTypeMismatch
            | Self::WorkflowOutputTypeMismatch
            | Self::WorkflowPositionalArgsNotSupported
            | Self::WorkflowCheckCasesRequired
            | Self::WorkflowCheckCaseInvalid
            | Self::WorkflowCheckBuilderException
            | Self::WorkflowCheckUndecoratedBuilder
            | Self::WorkflowKwargsNotSerializable
            | Self::WorkflowInvalidId
            | Self::WorkflowInvalidDefinitionConfig
            | Self::WorkflowInvalidPriority
            | Self::WorkflowNaiveGoodUntil
            | Self::WorkflowSubworkflowDepthLimit
            | Self::WorkflowSubworkflowSelfNesting
            | Self::WorkflowTooManyNodes => ErrorCategory::Workflow,

            Self::TaskNoReturnType
            | Self::TaskInvalidReturnType
            | Self::TaskInvalidOptions
            | Self::TaskInvalidQueue
            | Self::TaskPredecoratedNotSupported
            | Self::TaskStrictSignatureViolation
            | Self::TaskSharedAsyncRequiresAsyncDef
            | Self::TaskSharedThreadRequiresDef
            | Self::TaskSharedExecutionRequiresTimeout
            | Self::TaskCancelGraceRequiresSharedAsync
            | Self::TaskCancelGraceOutOfRange
            | Self::TaskInvalidErrorCode
            | Self::TaskDuplicateErrorCode => ErrorCategory::Task,

            Self::ConfigInvalidQueueMode
            | Self::ConfigInvalidClusterCap
            | Self::ConfigInvalidPrefetch
            | Self::BrokerInvalidUrl
            | Self::ConfigInvalidRecovery
            | Self::ConfigInvalidSchedule
            | Self::CliInvalidArgs
            | Self::WorkerInvalidLocator
            | Self::ConfigInvalidResilience
            | Self::ConfigInvalidExceptionMapper
            | Self::ModuleExecError
            | Self::BrokerInitFailed
            | Self::CheckReservedCodeCollision
            | Self::ConfigInvalidBrokerPool
            | Self::ConfigInvalidChildHook
            | Self::ConfigInvalidKeepalive
            | Self::ConfigInvalidRetention
            | Self::CheckLiveAsyncContext
            | Self::ConfigInvalidWorkerProfile
            | Self::CheckWorkerConnections
            | Self::ConfigOtelApiMissing
            | Self::ConfigInvalidTraceLink
            | Self::EventHandlerUnknownEventType
            | Self::EventHandlerInvalidSignature
            | Self::EventHandlerDuplicateName
            | Self::EventHandlerInvalidName
            | Self::EventHandlerLimit
            | Self::ConfigInvalidPayload => ErrorCategory::Config,

            Self::TaskNotRegistered | Self::TaskDuplicateName | Self::WorkflowUnregisteredTask => {
                ErrorCategory::Registry
            }
        }
    }

    /// Returns true if this is a workflow validation error (HRS-001-HRS-099).
    pub fn is_workflow_error(&self) -> bool {
        self.category() == ErrorCategory::Workflow
    }

    /// Returns true if this is a task definition error (HRS-100-HRS-199).
    pub fn is_task_error(&self) -> bool {
        self.category() == ErrorCategory::Task
    }

    /// Returns true if this is a configuration error (HRS-200-HRS-299).
    pub fn is_config_error(&self) -> bool {
        self.category() == ErrorCategory::Config
    }

    /// Returns true if this is a registry error (HRS-300-HRS-399).
    pub fn is_registry_error(&self) -> bool {
        self.category() == ErrorCategory::Registry
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_code_str())
    }
}

/// Base error type for horsies startup/validation errors.
#[derive(Debug, Clone)]
pub struct HorsiesError {
    pub message: String,
    pub code: Option<ErrorCode>,
    pub notes: Vec<String>,
    pub help_text: Option<String>,
}

impl std::fmt::Display for HorsiesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(code) = &self.code {
            write!(f, "error[{}]: {}", code, self.message)?;
        } else {
            write!(f, "error: {}", self.message)?;
        }
        for note in &self.notes {
            write!(f, "\n  = note: {}", note)?;
        }
        if let Some(help) = &self.help_text {
            write!(f, "\n  = help: {}", help)?;
        }
        Ok(())
    }
}

impl std::error::Error for HorsiesError {}

impl HorsiesError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
            notes: Vec::new(),
            help_text: None,
        }
    }

    pub fn with_code(mut self, code: ErrorCode) -> Self {
        self.code = Some(code);
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    pub fn with_help(mut self, help_text: impl Into<String>) -> Self {
        self.help_text = Some(help_text.into());
        self
    }

    /// Returns the error category derived from the error code, if present.
    ///
    /// This provides type-level discrimination matching Python's exception
    /// subclass hierarchy (`WorkflowValidationError`, `TaskDefinitionError`,
    /// `ConfigurationError`, `RegistryError`).
    ///
    /// # Example
    /// ```
    /// # use horsies_core::core::error::{HorsiesError, ErrorCode, ErrorCategory};
    /// let err = HorsiesError::new("bad config")
    ///     .with_code(ErrorCode::ConfigInvalidQueueMode);
    /// assert_eq!(err.category(), Some(ErrorCategory::Config));
    /// ```
    pub fn category(&self) -> Option<ErrorCategory> {
        self.code.map(|c| c.category())
    }

    /// Returns true if this error belongs to the given category.
    pub fn is_category(&self, category: ErrorCategory) -> bool {
        self.category() == Some(category)
    }

    /// Returns true if this is a workflow validation error.
    pub fn is_workflow_error(&self) -> bool {
        self.is_category(ErrorCategory::Workflow)
    }

    /// Returns true if this is a task definition error.
    pub fn is_task_error(&self) -> bool {
        self.is_category(ErrorCategory::Task)
    }

    /// Returns true if this is a configuration error.
    pub fn is_config_error(&self) -> bool {
        self.is_category(ErrorCategory::Config)
    }

    /// Returns true if this is a registry error.
    pub fn is_registry_error(&self) -> bool {
        self.is_category(ErrorCategory::Registry)
    }
}

/// The refusals of one validation phase: the container that `check`,
/// `check_live` and `Horsies::new` return on their error side.
///
/// Formats all refusals together with a summary line, similar to rustc's
/// multi-error output.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    phase_name: String,
    refusals: Vec<HorsiesError>,
}

impl ValidationReport {
    pub fn new(phase_name: impl Into<String>) -> Self {
        Self {
            phase_name: phase_name.into(),
            refusals: Vec::new(),
        }
    }

    /// A report of the phase `phase_name` that holds `refusals`.
    pub fn of(phase_name: impl Into<String>, refusals: Vec<HorsiesError>) -> Self {
        Self {
            phase_name: phase_name.into(),
            refusals,
        }
    }

    pub fn add(&mut self, error: HorsiesError) {
        self.refusals.push(error);
    }

    pub fn has_errors(&self) -> bool {
        !self.refusals.is_empty()
    }

    /// The refusals, in the order the phase found them.
    pub fn refusals(&self) -> &[HorsiesError] {
        &self.refusals
    }

    /// The phase that refused, for example `check`, `config`,
    /// `worker-startup` or `scheduler-startup`.
    pub fn phase_name(&self) -> &str {
        &self.phase_name
    }

    /// `Ok` when the report holds no refusal, else the report itself.
    pub fn into_checked(self) -> Result<(), ValidationReport> {
        match self.refusals.is_empty() {
            true => Ok(()),
            false => Err(self),
        }
    }

    /// Raise collected errors following the backward-compat rule:
    /// - 0 errors: returns Ok(())
    /// - 1 error: returns the original error
    /// - 2+ errors: returns a MultipleValidationErrors wrapping all
    pub fn into_result(self) -> Result<(), HorsiesError> {
        match self.refusals.len() {
            0 => Ok(()),
            1 => Err(self
                .refusals
                .into_iter()
                .next()
                .expect("length checked as 1")),
            n => {
                let message = format!("aborting due to {} previous errors", n);
                let mut combined = HorsiesError::new(message);
                let common_code = self.refusals.first().and_then(|first| {
                    first
                        .code
                        .filter(|code| self.refusals.iter().all(|error| error.code == Some(*code)))
                });
                combined.code = common_code;
                for error in &self.refusals {
                    combined.notes.push(format!("{}", error));
                }
                Err(combined)
            }
        }
    }
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.refusals {
            writeln!(f, "{}", error)?;
        }
        write!(
            f,
            "error: aborting due to {} previous errors",
            self.refusals.len()
        )
    }
}

impl std::error::Error for ValidationReport {}

/// One error of a refused report, for a caller whose error type is
/// [`HorsiesError`]: one refusal as it is, two or more as one error with
/// each refusal as a note ([`ValidationReport::into_result`]).
///
/// # Panics
///
/// For a report with no refusal: a refused report always holds one.
impl From<ValidationReport> for HorsiesError {
    fn from(report: ValidationReport) -> Self {
        match report.into_result() {
            Err(error) => error,
            Ok(()) => panic!("a refused validation report holds at least one refusal"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(ErrorCode::WorkflowCycleDetected.to_string(), "HRS-007");
        assert_eq!(ErrorCode::TaskNotRegistered.to_string(), "HRS-300");
        assert_eq!(ErrorCode::ConfigInvalidRetention.to_string(), "HRS-216");
        assert_eq!(
            ErrorCode::ConfigInvalidRetention.category(),
            ErrorCategory::Config,
        );
        assert!(ErrorCode::ConfigInvalidRetention.is_config_error());
        assert_eq!(ErrorCode::ConfigInvalidResilience.to_string(), "HRS-208");
        assert!(ErrorCode::ConfigInvalidResilience.is_config_error());
        assert_eq!(ErrorCode::CheckWorkerConnections.to_string(), "HRS-219");
        assert_eq!(
            ErrorCode::CheckWorkerConnections.category(),
            ErrorCategory::Config,
        );
        assert!(ErrorCode::CheckWorkerConnections.is_config_error());
        assert_eq!(ErrorCode::CheckLiveAsyncContext.to_string(), "HRS-217");
        assert!(ErrorCode::CheckLiveAsyncContext.is_config_error());
    }

    #[test]
    fn horsies_error_builder() {
        let err = HorsiesError::new("cycle detected in workflow DAG")
            .with_code(ErrorCode::WorkflowCycleDetected)
            .with_note("workflows must be acyclic directed graphs")
            .with_help("remove circular dependencies between tasks");

        let display = format!("{}", err);
        assert!(display.contains("HRS-007"));
        assert!(display.contains("cycle detected"));
        assert!(display.contains("note:"));
        assert!(display.contains("help:"));
    }

    #[test]
    fn validation_report_empty() {
        let report = ValidationReport::new("test");
        assert!(!report.has_errors());
        assert!(report.into_result().is_ok());
    }

    #[test]
    fn validation_report_single_error() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("bad config").with_code(ErrorCode::ConfigInvalidQueueMode));
        assert!(report.has_errors());
        let err = report.into_result().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::ConfigInvalidQueueMode));
    }

    #[test]
    fn validation_report_multiple_errors() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("error 1"));
        report.add(HorsiesError::new("error 2"));
        report.add(HorsiesError::new("error 3"));
        let err = report.into_result().unwrap_err();
        assert!(err.message.contains("3 previous errors"));
        assert_eq!(err.notes.len(), 3);
    }

    // --- ErrorCode catalog tests ---

    /// Each code is `HRS-` and three digits, and its hundred is the range of
    /// its category: 0 workflow, 1 task, 2 config, 3 registry.
    #[test]
    fn every_code_is_three_digits_in_the_range_of_its_category() {
        for code in <ErrorCode as CodeEnum>::VARIANTS {
            let text = code.as_code_str();
            let digits = text
                .strip_prefix("HRS-")
                .unwrap_or_else(|| panic!("{code:?}: {text} does not start with HRS-"));
            assert!(
                digits.len() == 3 && digits.chars().all(|digit| digit.is_ascii_digit()),
                "{code:?}: {text}"
            );
            let hundred = match code.category() {
                ErrorCategory::Workflow => '0',
                ErrorCategory::Task => '1',
                ErrorCategory::Config => '2',
                ErrorCategory::Registry => '3',
            };
            assert!(
                digits.starts_with(hundred),
                "{code:?}: {text} is outside the range of {}",
                code.category()
            );
        }
    }

    /// Each code has its short text: the docstring of its Python member.
    #[test]
    fn every_code_has_its_short_text() {
        for member in <ErrorCode as CodeEnum>::CATALOG.members {
            assert!(
                !member.doc.trim().is_empty(),
                "{} ({}) has no doc",
                member.name,
                member.value
            );
        }
    }

    /// HRS-302 is a registry code for `category()` and for
    /// `is_registry_error()` alike.
    #[test]
    fn the_unregistered_workflow_task_code_is_a_registry_error() {
        let code = ErrorCode::WorkflowUnregisteredTask;
        assert_eq!(code.as_code_str(), "HRS-302");
        assert_eq!(code.category(), ErrorCategory::Registry);
        assert!(code.is_registry_error());
        assert!(!code.is_workflow_error());
    }

    // --- ErrorCode category helper tests ---

    #[test]
    fn workflow_code_is_workflow_error_only() {
        let code = ErrorCode::WorkflowCycleDetected;
        assert!(code.is_workflow_error());
        assert!(!code.is_task_error());
        assert!(!code.is_config_error());
        assert!(!code.is_registry_error());
    }

    #[test]
    fn task_code_is_task_error_only() {
        let code = ErrorCode::TaskInvalidOptions;
        assert!(!code.is_workflow_error());
        assert!(code.is_task_error());
        assert!(!code.is_config_error());
        assert!(!code.is_registry_error());
    }

    #[test]
    fn config_code_is_config_error_only() {
        let code = ErrorCode::ConfigInvalidQueueMode;
        assert!(!code.is_workflow_error());
        assert!(!code.is_task_error());
        assert!(code.is_config_error());
        assert!(!code.is_registry_error());
    }

    #[test]
    fn registry_code_is_registry_error_only() {
        let code = ErrorCode::TaskNotRegistered;
        assert!(!code.is_workflow_error());
        assert!(!code.is_task_error());
        assert!(!code.is_config_error());
        assert!(code.is_registry_error());
    }

    // --- ErrorCode::category() tests ---

    #[test]
    fn error_code_category_workflow() {
        assert_eq!(
            ErrorCode::WorkflowCycleDetected.category(),
            ErrorCategory::Workflow
        );
        assert_eq!(
            ErrorCode::WorkflowNoName.category(),
            ErrorCategory::Workflow
        );
        assert_eq!(
            ErrorCode::WorkflowSubworkflowAppMissing.category(),
            ErrorCategory::Workflow
        );
    }

    #[test]
    fn error_code_category_task() {
        assert_eq!(ErrorCode::TaskNoReturnType.category(), ErrorCategory::Task);
        assert_eq!(ErrorCode::TaskInvalidQueue.category(), ErrorCategory::Task);
    }

    /// HRS-112 and HRS-113, the refusals of a declared code, are task
    /// codes.
    #[test]
    fn the_declared_code_refusals_are_task_codes() {
        for (code, text) in [
            (ErrorCode::TaskInvalidErrorCode, "HRS-112"),
            (ErrorCode::TaskDuplicateErrorCode, "HRS-113"),
        ] {
            assert_eq!(code.as_code_str(), text);
            assert_eq!(code.category(), ErrorCategory::Task);
            assert!(code.is_task_error());
            assert!(!code.is_config_error());
        }
    }

    #[test]
    fn error_code_category_config() {
        assert_eq!(
            ErrorCode::ConfigInvalidQueueMode.category(),
            ErrorCategory::Config
        );
        assert_eq!(
            ErrorCode::BrokerInvalidUrl.category(),
            ErrorCategory::Config
        );
        assert_eq!(
            ErrorCode::WorkerInvalidLocator.category(),
            ErrorCategory::Config
        );
    }

    #[test]
    fn error_code_category_registry() {
        assert_eq!(
            ErrorCode::TaskNotRegistered.category(),
            ErrorCategory::Registry
        );
        assert_eq!(
            ErrorCode::TaskDuplicateName.category(),
            ErrorCategory::Registry
        );
    }

    // --- ErrorCategory Display tests ---

    #[test]
    fn error_category_display() {
        assert_eq!(ErrorCategory::Workflow.to_string(), "workflow validation");
        assert_eq!(ErrorCategory::Task.to_string(), "task definition");
        assert_eq!(ErrorCategory::Config.to_string(), "configuration");
        assert_eq!(ErrorCategory::Registry.to_string(), "registry");
    }

    // --- HorsiesError category methods ---

    #[test]
    fn horsies_error_category_from_code() {
        let err = HorsiesError::new("bad config").with_code(ErrorCode::ConfigInvalidQueueMode);
        assert_eq!(err.category(), Some(ErrorCategory::Config));
        assert!(err.is_config_error());
        assert!(!err.is_workflow_error());
        assert!(!err.is_task_error());
        assert!(!err.is_registry_error());
    }

    #[test]
    fn horsies_error_category_workflow() {
        let err = HorsiesError::new("cycle").with_code(ErrorCode::WorkflowCycleDetected);
        assert_eq!(err.category(), Some(ErrorCategory::Workflow));
        assert!(err.is_workflow_error());
    }

    #[test]
    fn horsies_error_category_none_without_code() {
        let err = HorsiesError::new("generic error");
        assert_eq!(err.category(), None);
        assert!(!err.is_workflow_error());
        assert!(!err.is_task_error());
        assert!(!err.is_config_error());
        assert!(!err.is_registry_error());
    }

    #[test]
    fn horsies_error_is_category() {
        let err = HorsiesError::new("not found").with_code(ErrorCode::TaskNotRegistered);
        assert!(err.is_category(ErrorCategory::Registry));
        assert!(!err.is_category(ErrorCategory::Workflow));
    }

    // -- HorsiesError creation and fluent API (ported from Python test_errors.py) --

    #[test]
    fn horsies_error_basic_creation_defaults() {
        let err = HorsiesError::new("something went wrong");
        assert_eq!(err.message, "something went wrong");
        assert!(err.code.is_none());
        assert!(err.notes.is_empty());
        assert!(err.help_text.is_none());
    }

    #[test]
    fn horsies_error_creation_with_code() {
        let err = HorsiesError::new("bad cycle").with_code(ErrorCode::WorkflowCycleDetected);
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
        assert!(err.notes.is_empty());
        assert!(err.help_text.is_none());
    }

    #[test]
    fn horsies_error_fluent_api_chained() {
        let err = HorsiesError::new("problem")
            .with_code(ErrorCode::WorkflowNoNodes)
            .with_note("first note")
            .with_note("second note")
            .with_help("try adding tasks");
        assert_eq!(err.notes.len(), 2);
        assert_eq!(err.notes[0], "first note");
        assert_eq!(err.notes[1], "second note");
        assert_eq!(err.help_text.as_deref(), Some("try adding tasks"));
    }

    #[test]
    fn horsies_error_display_without_code() {
        let err = HorsiesError::new("generic error");
        let s = err.to_string();
        assert!(s.contains("generic error"));
    }

    #[test]
    fn horsies_error_display_with_code() {
        let err = HorsiesError::new("cycle detected").with_code(ErrorCode::WorkflowCycleDetected);
        let s = err.to_string();
        assert!(s.contains("cycle detected"));
    }

    #[test]
    fn horsies_error_implements_std_error() {
        let err = HorsiesError::new("test");
        // Verify it implements std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    // -- Error code well-formedness (ported from Python TestErrorCode) --

    #[test]
    fn error_code_display_format() {
        // All error codes should display as "HRS-XXX" format.
        let code = ErrorCode::WorkflowNoName;
        let s = code.to_string();
        assert!(
            s.starts_with("HRS-"),
            "code should start with 'HRS-': {}",
            s
        );
        assert!(s.len() == 7, "code should be 7 chars (HRS-XXX): {}", s);
    }

    // -- ValidationReport additional tests (ported from Python test_errors.py) --

    #[test]
    fn validation_report_stores_phase_name() {
        let report = ValidationReport::new("dag_validation");
        assert_eq!(report.phase_name(), "dag_validation");
    }

    #[test]
    fn validation_report_add_multiple_errors() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("err1"));
        report.add(HorsiesError::new("err2"));
        report.add(HorsiesError::new("err3"));
        assert_eq!(report.refusals().len(), 3);
        assert!(report.has_errors());
    }

    #[test]
    fn validation_report_display_contains_errors() {
        let mut report = ValidationReport::new("test_phase");
        report.add(HorsiesError::new("first error"));
        report.add(HorsiesError::new("second error"));
        let s = report.to_string();
        assert!(s.contains("first error"));
        assert!(s.contains("second error"));
    }

    #[test]
    fn validation_report_display_empty() {
        let report = ValidationReport::new("empty_phase");
        let s = report.to_string();
        assert!(s.contains("0") || s.is_empty() || s.contains("no errors"));
    }

    #[test]
    fn into_checked_is_ok_with_no_refusal_and_the_report_with_one() {
        assert!(ValidationReport::new("config").into_checked().is_ok());
        let mut report = ValidationReport::new("config");
        report.add(HorsiesError::new("bad").with_code(ErrorCode::ConfigInvalidQueueMode));
        let refused = report.into_checked().unwrap_err();
        assert_eq!(refused.phase_name(), "config");
        assert_eq!(
            refused.refusals()[0].code,
            Some(ErrorCode::ConfigInvalidQueueMode)
        );
    }

    #[test]
    fn a_report_converts_into_one_error() {
        let single = ValidationReport::of("config", vec![HorsiesError::new("only")]);
        assert_eq!(HorsiesError::from(single).message, "only");
        let two = ValidationReport::of(
            "config",
            vec![HorsiesError::new("first"), HorsiesError::new("second")],
        );
        let combined = HorsiesError::from(two);
        assert!(combined.message.contains("2 previous errors"));
        assert_eq!(combined.notes.len(), 2);
    }

    #[test]
    fn into_result_two_errors_message_contains_count() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("err1").with_code(ErrorCode::WorkflowNoName));
        report.add(HorsiesError::new("err2").with_code(ErrorCode::WorkflowNoNodes));
        let result: Result<(), HorsiesError> = report.into_result();
        let err = result.unwrap_err();
        assert!(
            err.message.contains("2"),
            "multi-error message should contain count, got: {}",
            err.message,
        );
    }
}
