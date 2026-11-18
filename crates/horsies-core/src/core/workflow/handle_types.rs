/// Typed error types for WorkflowHandle operations.
use uuid::Uuid;

use crate::broker::BrokerError;
use crate::failure_cause::FailureCause;
use crate::workflow_engine::error::WorkflowError;
///
/// Mirrors Python's `python/horsies/core/models/workflow/handle_types.py`.
///
/// Two error strategies by return type:
///
/// 1. **Wrap strategy** — methods that return non-TaskResult types
///    (status, cancel, pause, resume, results, tasks) return
///    `HandleResult<T>`. Infrastructure errors are `Err(HandleOperationError)`.
///
/// 2. **Fold strategy** — methods that return `TaskResult`
///    (`WorkflowHandle::get`, `WorkflowHandle::result_for`) keep that
///    shape. Infrastructure/query failures fold into
///    `TaskResult::Err(TaskError(...))` instead of bubbling out as
///    `HandleResult::Err(...)`.
///
/// Categorized handle operation failure codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, horsies_core_derive::CodeCatalog)]
#[code_catalog(module = "workflow_handle", base = "str_enum")]
pub enum HandleErrorCode {
    /// Workflow ID does not exist in the database.
    WorkflowNotFound,
    /// Database query or commit failed.
    DbOperationFailed,
    /// A stored value of the workflow does not parse (an unknown status, a stored JSON value). Nothing
    /// changed, and the same call returns the same error. Not retryable.
    StoredDataInvalid,
    /// `resume` resumed the workflow, and the recovery pass for its tree, which runs after that, failed.
    /// The workflow is RUNNING. Not retryable.
    ResumeRecoveryFailed,
    /// Internal polling/event-loop runner failed.
    LoopRunnerFailed,
    /// The broker already holds resource sets for its limit of open event loops, and this call ran on a
    /// new one. Nothing ran. Python client only; removed before 0.8.0.
    EventLoopLimit,
    /// A sync or blocking handle method was called on a thread that runs an event loop or a tokio
    /// runtime. Its wait would stall that loop, so nothing ran.
    AsyncContext,
}

/// A workflow handle operation that the core runs: what the message of its
/// failures names (Python's handle texts) and the name of the core
/// operation, which is the subject of the exception of a failure (the
/// values of Python's `handle_records.WorkflowOperation`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOperation {
    Pause,
    Cancel,
    Status,
    /// The wait for the end of the workflow (`get`).
    Get,
    /// The read of the stored result of the node `node_id` (`result_for`).
    ResultFor {
        node_id: String,
    },
    Results,
    Tasks,
    Resume,
}

impl HandleOperation {
    /// The name of the core operation: the subject of the core's exception
    /// class of a failure. Pure.
    pub fn subject(&self) -> &'static str {
        match self {
            Self::Pause => "pause_workflow",
            Self::Cancel => "cancel_workflow",
            Self::Status => "workflow_status",
            Self::Get => "wait_workflow_result",
            Self::ResultFor { .. } => "workflow_node_result",
            Self::Results => "workflow_node_results",
            Self::Tasks => "workflow_nodes",
            Self::Resume => "resume_workflow",
        }
    }

    /// Python's message of a failure of this operation on `workflow_id`
    /// with the text `cause`. Pure.
    ///
    /// - `ResultFor`: `DB query failed for result_for node {node_id} in
    ///   workflow {workflow_id}: {cause}` (Python 0.7's text).
    /// - Every other operation: `{label} failed for workflow {workflow_id}:
    ///   {cause}`, with the label of Python's `handle_records._label`.
    pub fn failure_message(&self, workflow_id: Uuid, cause: &dyn std::fmt::Display) -> String {
        let label = match self {
            Self::ResultFor { node_id } => {
                return format!(
                    "DB query failed for result_for node {node_id} in workflow {workflow_id}: \
                     {cause}"
                );
            }
            Self::Pause => "Pause",
            Self::Cancel => "Cancel",
            Self::Status => "Status",
            Self::Get => "Get",
            Self::Results => "Results",
            Self::Tasks => "Tasks",
            Self::Resume => "Resume",
        };
        format!("{label} failed for workflow {workflow_id}: {cause}")
    }
}

impl std::fmt::Display for HandleErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkflowNotFound => write!(f, "WORKFLOW_NOT_FOUND"),
            Self::DbOperationFailed => write!(f, "DB_OPERATION_FAILED"),
            Self::StoredDataInvalid => write!(f, "STORED_DATA_INVALID"),
            Self::ResumeRecoveryFailed => write!(f, "RESUME_RECOVERY_FAILED"),
            Self::LoopRunnerFailed => write!(f, "LOOP_RUNNER_FAILED"),
            Self::EventLoopLimit => write!(f, "EVENT_LOOP_LIMIT"),
            Self::AsyncContext => write!(f, "ASYNC_CONTEXT"),
        }
    }
}

/// The failure of a workflow handle operation: one variant per Python class
/// of `python/horsies/core/models/workflow/handle_types.py` that the core or
/// a client of the core produces, each with the facts of its state only.
/// Carried inside `Err(...)` of a [`HandleResult`]. Each variant's doc is
/// its Python class doc (`ErrorCatalog`).
///
/// The classes that only the Python client produces are not variants:
/// `HandleLoopRunnerFailed` and `HandleEventLoopLimit`. A variant without
/// `cause` has the precondition cause of its state
/// ([`Self::failure_cause`]); a variant without `retryable` never clears.
/// A variant with an `exception` carries its typed cause, the
/// [`WorkflowError`], which the client builds that exception from
/// (`ErrorClassValues`).
#[derive(Debug, horsies_core_derive::ErrorCatalog, horsies_core_derive::ErrorClassValues)]
#[error_catalog(
    module = "workflow_handle_errors",
    code = "HandleErrorCode",
    union = "HandleCoreError",
    union_doc = "Every failure of a workflow handle operation that the core gives: one class per state."
)]
pub enum HandleOperationError {
    /// No workflow has this id. Nothing changed.
    ///
    /// The same call returns this error while no workflow has this id.
    #[error_class(
        class = "HandleWorkflowNotFound",
        code = "WorkflowNotFound",
        never_retryable
    )]
    WorkflowNotFound {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
    },
    /// The operation failed with a database or driver error.
    ///
    /// ``retryable`` is the driver classification of the cause: ``True`` when
    /// the condition may clear, for example a lost connection. A read changed
    /// nothing. A write (``cancel``, ``pause``, or ``resume`` before its commit)
    /// rolled back, or its commit outcome is unknown.
    #[error_class(
        class = "HandleDbOperationFailed",
        code = "DbOperationFailed",
        exception = "BaseException"
    )]
    DbOperationFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
        /// The failed connect, client family check, statement or child
        /// build.
        #[error_slot(cause)]
        cause: WorkflowError,
    },
    /// A stored value of the workflow does not parse. Nothing changed.
    ///
    /// The value is a workflow status that is not a ``WorkflowStatus``
    /// (``status``), or a subworkflow summary that is not valid (``tasks``). ``exception`` is the parse error. The same call returns this
    /// error until the stored value changes.
    #[error_class(
        class = "HandleStoredDataInvalid",
        code = "StoredDataInvalid",
        never_retryable,
        exception = "BaseException"
    )]
    StoredDataInvalid {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
        /// The stored value that does not parse.
        #[error_slot(cause)]
        cause: WorkflowError,
    },
    /// ``resume`` committed, and the recovery pass for the workflow tree failed.
    ///
    /// The workflow is ``RUNNING`` and the resume enqueued its ready nodes. The
    /// recovery pass runs after the commit, in a second transaction. It
    /// completes the propagation for a task that finished while the resume
    /// transaction ran, when the completion callback saw the workflow as
    /// ``PAUSED``. That pass rolled back, or its commit outcome is unknown.
    /// ``exception`` is the database or driver error. The worker reaper runs
    /// the same pass over all workflows. A repeated ``resume`` returns
    /// ``Ok(False)``.
    #[error_class(
        class = "HandleResumeRecoveryFailed",
        code = "ResumeRecoveryFailed",
        never_retryable,
        exception = "BaseException"
    )]
    ResumeRecoveryFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
        /// The failure of the work after the commit.
        #[error_slot(cause)]
        cause: WorkflowError,
    },
    /// A sync method was called on a thread that runs an event loop. Nothing ran.
    ///
    /// Its wait would stall that loop. Call the ``*_async`` form; a sync call on
    /// that thread returns this error again.
    #[error_class(class = "HandleAsyncContext", code = "AsyncContext", never_retryable)]
    AsyncContext {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
    },
}

impl HandleOperationError {
    /// No workflow has `workflow_id`, with Python's message. Nothing
    /// changed. Pure.
    pub fn not_found(workflow_id: Uuid) -> Self {
        Self::WorkflowNotFound {
            message: format!("Workflow {workflow_id} not found"),
            workflow_id,
        }
    }

    /// `operation` on `workflow_id` failed at the connect, before any
    /// statement: `DB_OPERATION_FAILED` with the broker error's retry rule.
    /// The message holds the broker error's own text after the operation's
    /// label; the cause is the broker error. Nothing ran. Pure.
    pub fn connect_failed(
        operation: HandleOperation,
        workflow_id: Uuid,
        error: BrokerError,
    ) -> Self {
        Self::DbOperationFailed {
            message: operation.failure_message(workflow_id, &error),
            retryable: error.is_retryable(),
            workflow_id,
            cause: WorkflowError::Broker(error),
        }
    }

    /// The catalog code of the variant: the code of its Python class
    /// (`ErrorCatalog`). Pure.
    pub fn code(&self) -> HandleErrorCode {
        crate::error_catalog::ErrorEnum::error_code(self)
    }

    /// What failed and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::WorkflowNotFound { message, .. }
            | Self::DbOperationFailed { message, .. }
            | Self::StoredDataInvalid { message, .. }
            | Self::ResumeRecoveryFailed { message, .. }
            | Self::AsyncContext { message, .. } => message,
        }
    }

    /// Whether a repeat of the call can succeed: the classification of the
    /// cause of `DbOperationFailed`, else `false`. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::DbOperationFailed { retryable, .. } => *retryable,
            Self::WorkflowNotFound { .. }
            | Self::StoredDataInvalid { .. }
            | Self::ResumeRecoveryFailed { .. }
            | Self::AsyncContext { .. } => false,
        }
    }

    /// The workflow of the handle. Pure.
    pub fn workflow_id(&self) -> Uuid {
        match self {
            Self::WorkflowNotFound { workflow_id, .. }
            | Self::DbOperationFailed { workflow_id, .. }
            | Self::StoredDataInvalid { workflow_id, .. }
            | Self::ResumeRecoveryFailed { workflow_id, .. }
            | Self::AsyncContext { workflow_id, .. } => *workflow_id,
        }
    }

    /// The cause of the failure: the cause of the variant's `cause`, else a
    /// failed precondition (an absent workflow, a refused thread). Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::DbOperationFailed { cause, .. }
            | Self::StoredDataInvalid { cause, .. }
            | Self::ResumeRecoveryFailed { cause, .. } => cause.failure_cause(),
            Self::WorkflowNotFound { .. } | Self::AsyncContext { .. } => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

impl std::fmt::Display for HandleOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (workflow_id={}, retryable={})",
            self.code(),
            self.message(),
            self.workflow_id(),
            self.retryable(),
        )
    }
}

impl std::error::Error for HandleOperationError {
    /// The variant's `cause`, when it has one.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DbOperationFailed { cause, .. }
            | Self::StoredDataInvalid { cause, .. }
            | Self::ResumeRecoveryFailed { cause, .. } => Some(cause),
            Self::WorkflowNotFound { .. } | Self::AsyncContext { .. } => None,
        }
    }
}

/// Result type for wrap-strategy workflow handle operations.
///
/// Used by methods like `status()`, `results()`, `tasks()`, `cancel()`,
/// `pause()`, and `resume()`.
///
/// `Ok(T)` on success, `Err(HandleOperationError)` on failure.
pub type HandleResult<T> = Result<T, HandleOperationError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(
            HandleErrorCode::WorkflowNotFound.to_string(),
            "WORKFLOW_NOT_FOUND"
        );
        assert_eq!(
            HandleErrorCode::DbOperationFailed.to_string(),
            "DB_OPERATION_FAILED"
        );
        assert_eq!(
            HandleErrorCode::StoredDataInvalid.to_string(),
            "STORED_DATA_INVALID"
        );
        assert_eq!(
            HandleErrorCode::ResumeRecoveryFailed.to_string(),
            "RESUME_RECOVERY_FAILED"
        );
    }

    /// A refused connect, as the broker reports it.
    fn refused() -> WorkflowError {
        WorkflowError::Broker(BrokerError::ConnectionFailed("refused".to_owned()))
    }

    #[test]
    fn handle_operation_error_display() {
        let err = HandleOperationError::DbOperationFailed {
            message: "connection refused".to_owned(),
            retryable: true,
            workflow_id: uuid::Uuid::nil(),
            cause: refused(),
        };
        let s = format!("{}", err);
        assert!(s.contains("DB_OPERATION_FAILED"));
        assert!(s.contains(&uuid::Uuid::nil().to_string()));
        assert!(s.contains("retryable=true"));
    }

    /// Each variant has the code of its Python class; only
    /// `DbOperationFailed` can be retryable, and a variant without a cause
    /// has a failed precondition.
    #[test]
    fn each_variant_has_the_code_of_its_python_class() {
        let workflow_id = uuid::Uuid::from_u128(9);
        let cases = [
            (
                HandleOperationError::WorkflowNotFound {
                    message: "m".to_owned(),
                    workflow_id,
                },
                HandleErrorCode::WorkflowNotFound,
                FailureCause::PreconditionFailed,
            ),
            (
                HandleOperationError::DbOperationFailed {
                    message: "m".to_owned(),
                    retryable: false,
                    workflow_id,
                    cause: refused(),
                },
                HandleErrorCode::DbOperationFailed,
                FailureCause::Connection,
            ),
            (
                HandleOperationError::StoredDataInvalid {
                    message: "m".to_owned(),
                    workflow_id,
                    cause: WorkflowError::InvalidStatus("BOGUS".to_owned()),
                },
                HandleErrorCode::StoredDataInvalid,
                FailureCause::ContractViolation,
            ),
            (
                HandleOperationError::ResumeRecoveryFailed {
                    message: "m".to_owned(),
                    workflow_id,
                    cause: refused(),
                },
                HandleErrorCode::ResumeRecoveryFailed,
                FailureCause::Connection,
            ),
            (
                HandleOperationError::AsyncContext {
                    message: "m".to_owned(),
                    workflow_id,
                },
                HandleErrorCode::AsyncContext,
                FailureCause::PreconditionFailed,
            ),
        ];
        for (error, code, cause) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), "m");
            assert_eq!(error.workflow_id(), workflow_id);
            assert_eq!(error.failure_cause(), cause);
            assert!(!error.retryable());
        }
    }

    /// Python's labels (`handle_records._label`) and operation names
    /// (`handle_records.WorkflowOperation`), one per operation.
    #[test]
    fn each_operation_has_python_s_label_and_subject() {
        let workflow_id = uuid::Uuid::from_u128(9);
        let operations = [
            (HandleOperation::Pause, "Pause", "pause_workflow"),
            (HandleOperation::Cancel, "Cancel", "cancel_workflow"),
            (HandleOperation::Status, "Status", "workflow_status"),
            (HandleOperation::Get, "Get", "wait_workflow_result"),
            (HandleOperation::Results, "Results", "workflow_node_results"),
            (HandleOperation::Tasks, "Tasks", "workflow_nodes"),
            (HandleOperation::Resume, "Resume", "resume_workflow"),
        ];
        for (operation, label, subject) in operations {
            assert_eq!(
                operation.failure_message(workflow_id, &"boom"),
                format!("{label} failed for workflow {workflow_id}: boom")
            );
            assert_eq!(operation.subject(), subject);
        }
    }

    /// A failed `result_for` read names the node, in Python 0.7's text.
    #[test]
    fn a_failed_node_result_read_names_the_node() {
        let workflow_id = uuid::Uuid::from_u128(9);
        let operation = HandleOperation::ResultFor {
            node_id: "fetch".to_owned(),
        };
        assert_eq!(operation.subject(), "workflow_node_result");
        assert_eq!(
            operation.failure_message(workflow_id, &"boom"),
            format!("DB query failed for result_for node fetch in workflow {workflow_id}: boom")
        );
    }

    /// Python's `HandleWorkflowNotFound` message.
    #[test]
    fn an_absent_workflow_has_python_s_message() {
        let workflow_id = uuid::Uuid::from_u128(9);
        let missing = HandleOperationError::not_found(workflow_id);
        assert_eq!(missing.code(), HandleErrorCode::WorkflowNotFound);
        assert_eq!(
            missing.message(),
            format!("Workflow {workflow_id} not found")
        );
        assert_eq!(missing.workflow_id(), workflow_id);
        assert!(std::error::Error::source(&missing).is_none());
    }

    /// A failed connect keeps the broker error as its cause, with its retry
    /// rule, and its message holds the broker error's own text after the
    /// label.
    #[test]
    fn a_failed_connect_names_the_operation_and_keeps_the_broker_error() {
        let workflow_id = uuid::Uuid::from_u128(9);
        let failed = HandleOperationError::connect_failed(
            HandleOperation::Status,
            workflow_id,
            BrokerError::ConnectionFailed("refused".to_owned()),
        );
        assert_eq!(failed.code(), HandleErrorCode::DbOperationFailed);
        assert_eq!(
            failed.message(),
            format!("Status failed for workflow {workflow_id}: connection failed: refused")
        );
        assert!(failed.retryable());
        assert_eq!(failed.failure_cause(), FailureCause::Connection);
        assert!(matches!(
            failed,
            HandleOperationError::DbOperationFailed {
                cause: WorkflowError::Broker(BrokerError::ConnectionFailed(ref text)),
                ..
            } if text == "refused"
        ));
        assert_eq!(
            std::error::Error::source(&failed).map(ToString::to_string),
            Some("broker error: connection failed: refused".to_owned())
        );
    }
}
