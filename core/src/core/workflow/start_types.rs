/// Typed error types for workflow start operations.
///
/// Mirrors Python's `clients/python/horsies/core/workflows/start_types.py`.
use uuid::Uuid;

use crate::broker::workflow_start::{ClientStartError, IdHolder};
use crate::error_catalog::ErrorEnum;
use crate::workflow_engine::error::WorkflowError;
use crate::FailureCause;

/// Categorized workflow start failure codes.
///
/// The members are Python's, in Python's order
/// (`clients/python/horsies/core/workflows/start_types.py`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, horsies_core_derive::CodeCatalog)]
#[code_catalog(module = "workflow_start", base = "str_enum")]
pub enum WorkflowStartErrorCode {
    /// Config error: no broker attached.
    BrokerNotConfigured,
    /// Programmer error: bad DAG, bad args, serialization failure.
    ValidationFailed,
    /// The workflow id exists for another workflow name; nothing was written. Not retryable.
    WorkflowIdConflict,
    /// The workflow id holds a run of this workflow name that another start command started, or a
    /// sub-workflow; nothing was written. Not retryable.
    WorkflowCommandConflict,
    /// Infrastructure error: schema init, DB transaction. Conditionally retryable.
    EnqueueFailed,
    /// A sync or blocking start was called on a thread that runs an event loop or a tokio
    /// runtime; its wait would stall that loop, so nothing was written. `resend_start` accepts it.
    #[code_catalog(blocking_form_only)]
    AsyncContext,
}

impl std::fmt::Display for WorkflowStartErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BrokerNotConfigured => write!(f, "BROKER_NOT_CONFIGURED"),
            Self::ValidationFailed => write!(f, "VALIDATION_FAILED"),
            Self::WorkflowIdConflict => write!(f, "WORKFLOW_ID_CONFLICT"),
            Self::WorkflowCommandConflict => write!(f, "WORKFLOW_COMMAND_CONFLICT"),
            Self::EnqueueFailed => write!(f, "ENQUEUE_FAILED"),
            Self::AsyncContext => write!(f, "ASYNC_CONTEXT"),
        }
    }
}

/// The failure of a workflow start: one variant per Python class of
/// `clients/python/horsies/core/workflows/start_types.py` that the core or a client
/// of the core produces, each with the facts of its state only. Carried
/// inside `Err(...)` of a [`WorkflowStartResult`]. Each variant's doc is
/// its Python class doc (`ErrorCatalog`, module
/// `horsies._catalog.workflow_start_errors`).
///
/// The class that only the Python client produces is not a variant:
/// `WorkflowStartBrokerNotConfigured`. A variant with an `exception` carries
/// its typed cause, which the client builds that exception from
/// (`ErrorClassValues`).
#[derive(Debug, horsies_core_derive::ErrorCatalog, horsies_core_derive::ErrorClassValues)]
#[error_catalog(
    module = "workflow_start_errors",
    code = "WorkflowStartErrorCode",
    union = "WorkflowStartCoreError",
    union_doc = "Every failure of a workflow start that the core gives: one class per state."
)]
pub enum WorkflowStartError {
    /// The spec, its args or the retry input was refused. Nothing was written.
    ///
    /// The same start is refused again. ``exception`` is ``NoCause()`` when a
    /// check refused the input without a Python exception. The resend methods
    /// refuse this error.
    // `workflow_id` is the id of the start: the caller's id, or the id
    // minted for it, as in Python.
    #[error_class(
        class = "WorkflowStartValidationFailed",
        code = "ValidationFailed",
        never_retryable,
        exception = "BaseException | NoCause"
    )]
    ValidationFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Str")]
        workflow_name: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
        /// The error that refused the start, or a check.
        #[error_slot(cause)]
        cause: StartRefusalCause,
    },
    /// The workflow id exists for another workflow name. Nothing was written.
    ///
    /// ``existing_workflow_name`` names the workflow that holds the id. A start
    /// with the same id is refused again; use another id. The resend methods
    /// refuse this error.
    #[error_class(
        class = "WorkflowStartIdConflict",
        code = "WorkflowIdConflict",
        never_retryable
    )]
    IdConflict {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Str")]
        workflow_name: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
        #[error_slot(python = "Str")]
        existing_workflow_name: String,
    },
    /// The workflow id holds a run that another start command started. Nothing was written.
    ///
    /// The run has the name of this start. Its stored start command (the
    /// nodes, their inputs and options, and the workflow settings) differs
    /// from the command of this start, or the run is a sub-workflow, which has
    /// no start command. ``workflow_id`` is the id of that run. A start with
    /// this id must send the same command; another command is refused again,
    /// so send the same command or use another id. The resend methods refuse
    /// this error.
    #[error_class(
        class = "WorkflowStartCommandConflict",
        code = "WorkflowCommandConflict",
        never_retryable
    )]
    CommandConflict {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Str")]
        workflow_name: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
    },
    /// Schema initialization or the start transaction failed with a database or driver error.
    ///
    /// ``retryable`` is the core's classification of the cause: ``True`` when
    /// the condition may clear, for example a lost connection. The transaction
    /// rolled back, or its commit outcome is unknown. ``resend_start(err)``
    /// starts again with the same id; a workflow that the failed attempt
    /// committed comes back as the existing handle.
    #[error_class(
        class = "WorkflowStartEnqueueFailed",
        code = "EnqueueFailed",
        exception = "BaseException"
    )]
    EnqueueFailed {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Bool")]
        retryable: bool,
        #[error_slot(python = "Str")]
        workflow_name: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
        /// The failed connect, schema install, client family check,
        /// statement or child build.
        #[error_slot(cause)]
        cause: Box<WorkflowError>,
    },
    /// Sync ``start()`` or ``resend_start()`` was called on a thread that runs an event loop.
    ///
    /// Nothing was written: the wait would stall that loop. ``resend_start_async(err)``
    /// starts the workflow with the same id; a sync call on that thread returns
    /// this error again.
    // The Rust client's blocking starts give it on a thread that runs a
    // tokio runtime; `resend_start` completes the start with `workflow_id`.
    #[error_class(
        class = "WorkflowStartAsyncContext",
        code = "AsyncContext",
        never_retryable
    )]
    AsyncContext {
        #[error_slot(python = "Str", hidden)]
        message: String,
        #[error_slot(python = "Str")]
        workflow_name: String,
        #[error_slot(python = "UuidText")]
        workflow_id: Uuid,
    },
}

/// The cause of a refused start (`exception` of
/// `WorkflowStartValidationFailed`): the workflow error that refused the
/// spec or its arguments, or a check of the client or the core that found
/// the refusal with no error. Python builds `NoCause()` for a check.
#[derive(Debug, thiserror::Error)]
pub enum StartRefusalCause {
    /// The error that refused the start.
    #[error(transparent)]
    Error(Box<WorkflowError>),
    /// A check refused the start; no error was raised.
    #[error("a check refused the start")]
    Check,
}

impl From<WorkflowError> for StartRefusalCause {
    fn from(error: WorkflowError) -> Self {
        Self::Error(Box::new(error))
    }
}

impl StartRefusalCause {
    /// The cause of the error; a check is a failed precondition. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::Error(error) => error.failure_cause(),
            Self::Check => FailureCause::PreconditionFailed,
        }
    }
}

impl WorkflowStartError {
    /// The id `workflow_id` of the start of `workflow_name` belongs to the
    /// workflow `existing_workflow_name`, with Python's message. Nothing was
    /// written. Pure.
    pub fn id_conflict(
        workflow_name: &str,
        workflow_id: Uuid,
        existing_workflow_name: String,
    ) -> Self {
        Self::IdConflict {
            message: format!(
                "workflow_id '{workflow_id}' already exists for workflow name \
                 '{existing_workflow_name}', cannot start workflow name '{workflow_name}' with \
                 the same id"
            ),
            workflow_name: workflow_name.to_owned(),
            workflow_id,
            existing_workflow_name,
        }
    }

    /// The start `workflow_id` of `workflow_name` met the run `holder`, which
    /// it cannot return. Nothing was written. Pure.
    ///
    /// | Holder | Error |
    /// |---|---|
    /// | [`IdHolder::OtherWorkflowName`] | `IdConflict` ([`Self::id_conflict`]) |
    /// | [`IdHolder::OtherCommand`] | `CommandConflict`: the run of this name has another start command |
    /// | [`IdHolder::SubWorkflow`] | `CommandConflict`: the run is a sub-workflow, with no start command |
    ///
    /// The messages hold no input value.
    pub fn id_held(workflow_name: &str, workflow_id: Uuid, holder: IdHolder) -> Self {
        let command_conflict = |message: String| Self::CommandConflict {
            message,
            workflow_name: workflow_name.to_owned(),
            workflow_id,
        };
        match holder {
            IdHolder::OtherWorkflowName { existing_name } => {
                Self::id_conflict(workflow_name, workflow_id, existing_name)
            }
            IdHolder::OtherCommand => command_conflict(format!(
                "workflow_id '{workflow_id}' already exists for workflow name '{workflow_name}' \
                 with another start command; a start with this id must send the same command"
            )),
            IdHolder::SubWorkflow => command_conflict(format!(
                "workflow_id '{workflow_id}' belongs to a sub-workflow that another workflow \
                 started; it has no start command"
            )),
        }
    }

    /// The start `workflow_id` of `workflow_name` failed with `error`: its
    /// text is the message, its rule the retry flag, and the error itself
    /// (the broker error of a failed install) the cause. Nothing committed,
    /// or the commit outcome is unknown. Pure.
    pub fn start_failed(workflow_name: &str, workflow_id: Uuid, error: ClientStartError) -> Self {
        Self::EnqueueFailed {
            message: error.to_string(),
            retryable: error.is_retryable(),
            workflow_name: workflow_name.to_owned(),
            workflow_id,
            cause: Box::new(error.into_workflow_error()),
        }
    }

    /// The catalog code of the variant (the derive's one mapping,
    /// [`ErrorEnum::error_code`]). Pure.
    pub fn code(&self) -> WorkflowStartErrorCode {
        ErrorEnum::error_code(self)
    }

    /// What failed and why. Pure.
    pub fn message(&self) -> &str {
        match self {
            Self::ValidationFailed { message, .. }
            | Self::IdConflict { message, .. }
            | Self::CommandConflict { message, .. }
            | Self::EnqueueFailed { message, .. }
            | Self::AsyncContext { message, .. } => message,
        }
    }

    /// Whether a start again can succeed: the classification of the cause
    /// of `EnqueueFailed`, else `false`. Pure.
    pub fn retryable(&self) -> bool {
        match self {
            Self::EnqueueFailed { retryable, .. } => *retryable,
            Self::ValidationFailed { .. }
            | Self::IdConflict { .. }
            | Self::CommandConflict { .. }
            | Self::AsyncContext { .. } => false,
        }
    }

    /// The name of the workflow spec of the start. Pure.
    pub fn workflow_name(&self) -> &str {
        match self {
            Self::ValidationFailed { workflow_name, .. }
            | Self::IdConflict { workflow_name, .. }
            | Self::CommandConflict { workflow_name, .. }
            | Self::EnqueueFailed { workflow_name, .. }
            | Self::AsyncContext { workflow_name, .. } => workflow_name,
        }
    }

    /// The id of the start: the caller's id, or the id minted for it. Pure.
    pub fn workflow_id(&self) -> Uuid {
        match self {
            Self::ValidationFailed { workflow_id, .. }
            | Self::IdConflict { workflow_id, .. }
            | Self::CommandConflict { workflow_id, .. }
            | Self::EnqueueFailed { workflow_id, .. }
            | Self::AsyncContext { workflow_id, .. } => *workflow_id,
        }
    }

    /// What the start met. A variant without `cause` has the cause of its
    /// state: an id that another workflow or another start command holds, or
    /// a blocking start on a runtime thread, is a failed precondition. Pure.
    pub fn failure_cause(&self) -> FailureCause {
        match self {
            Self::ValidationFailed { cause, .. } => cause.failure_cause(),
            Self::EnqueueFailed { cause, .. } => cause.failure_cause(),
            Self::IdConflict { .. } | Self::CommandConflict { .. } | Self::AsyncContext { .. } => {
                FailureCause::PreconditionFailed
            }
        }
    }
}

impl std::fmt::Display for WorkflowStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (workflow={}, id={}, retryable={})",
            self.code(),
            self.message(),
            self.workflow_name(),
            self.workflow_id(),
            self.retryable(),
        )
    }
}

impl std::error::Error for WorkflowStartError {}

/// Result type for workflow start operations.
///
/// `Ok(T)` on success (typically `WorkflowHandle<T>`),
/// `Err(WorkflowStartError)` on failure.
pub type WorkflowStartResult<T> = Result<T, WorkflowStartError>;

/// Validate whether a `WorkflowStartError` is eligible for resend_start.
///
/// `EnqueueFailed` and `AsyncContext` errors can be retried: each carries the
/// id of a start that wrote nothing or may have written. The error's
/// workflow_name must match the spec being retried (prevents cross-workflow
/// retry).
///
/// Returns `Ok(workflow_id)` if retry is valid, `Err(WorkflowStartError)`
/// with `ValidationFailed` code otherwise.
///
/// Mirrors Python's `WorkflowSpec._validate_start_retry(error)`.
pub fn validate_start_retry(
    error: &WorkflowStartError,
    spec_name: &str,
) -> Result<Uuid, WorkflowStartError> {
    let (workflow_name, workflow_id) = match error {
        WorkflowStartError::EnqueueFailed {
            workflow_name,
            workflow_id,
            ..
        }
        | WorkflowStartError::AsyncContext {
            workflow_name,
            workflow_id,
            ..
        } => (workflow_name, *workflow_id),
        WorkflowStartError::ValidationFailed { .. }
        | WorkflowStartError::IdConflict { .. }
        | WorkflowStartError::CommandConflict { .. } => {
            return Err(WorkflowStartError::ValidationFailed {
                message: format!(
                    "resend_start only accepts ENQUEUE_FAILED and ASYNC_CONTEXT errors, got {}",
                    error.code(),
                ),
                workflow_name: spec_name.to_owned(),
                workflow_id: error.workflow_id(),
                cause: StartRefusalCause::Check,
            });
        }
    };
    if workflow_name != spec_name {
        return Err(WorkflowStartError::ValidationFailed {
            message: format!(
                "cross-workflow retry rejected: error from '{workflow_name}', spec is \
                 '{spec_name}'",
            ),
            workflow_name: spec_name.to_owned(),
            workflow_id,
            cause: StartRefusalCause::Check,
        });
    }
    Ok(workflow_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::BrokerError;
    use crate::error_catalog::{
        ErrorClassValues, ErrorSlot, ExceptionType, ExceptionValue, SlotData, SlotRepr, SlotType,
    };

    /// A lost connection: the cause of a failed start that may clear.
    fn connection_lost() -> WorkflowError {
        WorkflowError::Broker(BrokerError::ConnectionFailed(
            "connection refused".to_owned(),
        ))
    }

    #[test]
    fn error_code_display() {
        assert_eq!(
            WorkflowStartErrorCode::BrokerNotConfigured.to_string(),
            "BROKER_NOT_CONFIGURED",
        );
        assert_eq!(
            WorkflowStartErrorCode::EnqueueFailed.to_string(),
            "ENQUEUE_FAILED",
        );
        assert_eq!(
            WorkflowStartErrorCode::WorkflowIdConflict.to_string(),
            "WORKFLOW_ID_CONFLICT",
        );
        assert_eq!(
            WorkflowStartErrorCode::WorkflowCommandConflict.to_string(),
            "WORKFLOW_COMMAND_CONFLICT",
        );
    }

    #[test]
    fn workflow_start_error_display() {
        let workflow_id = Uuid::new_v4();
        let err = WorkflowStartError::EnqueueFailed {
            message: "connection refused".to_owned(),
            retryable: true,
            workflow_name: "my_pipeline".to_owned(),
            workflow_id,
            cause: Box::new(connection_lost()),
        };
        let s = format!("{}", err);
        assert!(s.contains("ENQUEUE_FAILED"));
        assert!(s.contains("my_pipeline"));
        assert!(s.contains(&workflow_id.to_string()));
    }

    /// Each variant has the code of its Python class and the id of its
    /// start; only `EnqueueFailed` can be retryable. A variant without
    /// `cause` has a failed precondition as its cause.
    #[test]
    fn each_variant_has_the_code_of_its_python_class() {
        let workflow_id = Uuid::new_v4();
        let name = || "wf".to_owned();
        let cases = [
            (
                WorkflowStartError::ValidationFailed {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                    cause: StartRefusalCause::Error(Box::new(WorkflowError::InvalidStatus(
                        "BOGUS".to_owned(),
                    ))),
                },
                WorkflowStartErrorCode::ValidationFailed,
                FailureCause::ContractViolation,
            ),
            (
                WorkflowStartError::IdConflict {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                    existing_workflow_name: "other".to_owned(),
                },
                WorkflowStartErrorCode::WorkflowIdConflict,
                FailureCause::PreconditionFailed,
            ),
            (
                WorkflowStartError::CommandConflict {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                },
                WorkflowStartErrorCode::WorkflowCommandConflict,
                FailureCause::PreconditionFailed,
            ),
            (
                WorkflowStartError::EnqueueFailed {
                    message: "m".to_owned(),
                    retryable: false,
                    workflow_name: name(),
                    workflow_id,
                    cause: Box::new(connection_lost()),
                },
                WorkflowStartErrorCode::EnqueueFailed,
                FailureCause::Connection,
            ),
            (
                WorkflowStartError::AsyncContext {
                    message: "m".to_owned(),
                    workflow_name: name(),
                    workflow_id,
                },
                WorkflowStartErrorCode::AsyncContext,
                FailureCause::PreconditionFailed,
            ),
        ];
        for (error, code, cause) in cases {
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), "m");
            assert_eq!(error.workflow_name(), "wf");
            assert_eq!(error.workflow_id(), workflow_id);
            assert_eq!(error.failure_cause(), cause);
            assert!(!error.retryable());
        }
    }

    // -- validate_start_retry (ported from Python test_workflow_resend_start.py) --

    #[test]
    fn validate_retry_accepts_enqueue_failed() {
        let workflow_id = Uuid::new_v4();
        let error = WorkflowStartError::EnqueueFailed {
            message: "DB connection lost".to_owned(),
            retryable: true,
            workflow_name: "test-wf".to_owned(),
            workflow_id,
            cause: Box::new(connection_lost()),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), workflow_id);
    }

    #[test]
    fn validate_retry_accepts_async_context() {
        let workflow_id = Uuid::new_v4();
        let error = WorkflowStartError::AsyncContext {
            message: "a blocking start on a runtime thread".to_owned(),
            workflow_name: "test-wf".to_owned(),
            workflow_id,
        };
        let result = validate_start_retry(&error, "test-wf");
        assert_eq!(result.unwrap(), workflow_id);
    }

    #[test]
    fn validate_retry_rejects_async_context_of_another_workflow() {
        let error = WorkflowStartError::AsyncContext {
            message: "a blocking start on a runtime thread".to_owned(),
            workflow_name: "other-workflow".to_owned(),
            workflow_id: Uuid::new_v4(),
        };
        let result = validate_start_retry(&error, "my-workflow");
        let err = result.unwrap_err();
        assert_eq!(err.code(), WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message().contains("cross-workflow"));
    }

    #[test]
    fn validate_retry_rejects_validation_failed() {
        let error = WorkflowStartError::ValidationFailed {
            message: "Bad DAG".to_owned(),
            workflow_name: "test-wf".to_owned(),
            workflow_id: Uuid::new_v4(),
            cause: StartRefusalCause::Check,
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message().contains("ENQUEUE_FAILED"));
        assert_eq!(err.workflow_id(), error.workflow_id());
        assert_eq!(err.failure_cause(), FailureCause::PreconditionFailed);
    }

    #[test]
    fn validate_retry_rejects_workflow_id_conflict() {
        let error = WorkflowStartError::IdConflict {
            message: "id belongs to another workflow".to_owned(),
            workflow_name: "test-wf".to_owned(),
            workflow_id: Uuid::new_v4(),
            existing_workflow_name: "other-wf".to_owned(),
        };
        let result = validate_start_retry(&error, "test-wf");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            WorkflowStartErrorCode::ValidationFailed,
        );
    }

    /// A start that met another start command of the same id is refused
    /// again with the same id, so the retry refuses it.
    #[test]
    fn validate_retry_rejects_a_command_conflict() {
        let error = WorkflowStartError::id_held("test-wf", Uuid::new_v4(), IdHolder::OtherCommand);
        let refused = validate_start_retry(&error, "test-wf").unwrap_err();
        assert_eq!(refused.code(), WorkflowStartErrorCode::ValidationFailed);
        assert_eq!(
            refused.message(),
            "resend_start only accepts ENQUEUE_FAILED and ASYNC_CONTEXT errors, got \
             WORKFLOW_COMMAND_CONFLICT"
        );
        assert_eq!(refused.workflow_id(), error.workflow_id());
    }

    #[test]
    fn validate_retry_rejects_cross_workflow_name() {
        let error = WorkflowStartError::EnqueueFailed {
            message: "DB connection lost".to_owned(),
            retryable: true,
            workflow_name: "other-workflow".to_owned(),
            workflow_id: Uuid::new_v4(),
            cause: Box::new(connection_lost()),
        };
        let result = validate_start_retry(&error, "my-workflow");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), WorkflowStartErrorCode::ValidationFailed);
        assert!(err.message().contains("cross-workflow"));
    }

    /// The classes of the start catalog have Python's names, slots and
    /// order (`clients/python/horsies/core/workflows/start_types.py`).
    #[test]
    fn the_start_classes_have_python_s_slots_in_python_s_order() {
        let catalog = <WorkflowStartError as ErrorEnum>::ERROR_CATALOG;
        assert_eq!(catalog.python_module, "workflow_start_errors");
        assert_eq!(catalog.union, "WorkflowStartCoreError");
        let names: Vec<&str> = catalog
            .classes
            .iter()
            .map(|class| class.python_class)
            .collect();
        assert_eq!(
            names,
            [
                "WorkflowStartValidationFailed",
                "WorkflowStartIdConflict",
                "WorkflowStartCommandConflict",
                "WorkflowStartEnqueueFailed",
                "WorkflowStartAsyncContext",
            ]
        );
        let field = |name: &'static str, repr: SlotRepr| ErrorSlot::Field {
            name,
            python_type: SlotType::Str,
            repr,
        };
        let workflow_id = ErrorSlot::Field {
            name: "workflow_id",
            python_type: SlotType::UuidText,
            repr: SlotRepr::Shown,
        };
        assert_eq!(
            catalog.classes[0].slots,
            [
                ErrorSlot::CodeMember {
                    member: "VALIDATION_FAILED"
                },
                ErrorSlot::NeverRetryable,
                field("message", SlotRepr::Hidden),
                field("workflow_name", SlotRepr::Shown),
                workflow_id,
                ErrorSlot::Exception {
                    python_type: ExceptionType::ExceptionOrNoCause
                },
            ]
        );
        assert_eq!(
            catalog.classes[1].slots,
            [
                ErrorSlot::CodeMember {
                    member: "WORKFLOW_ID_CONFLICT"
                },
                ErrorSlot::NeverRetryable,
                field("message", SlotRepr::Hidden),
                field("workflow_name", SlotRepr::Shown),
                workflow_id,
                field("existing_workflow_name", SlotRepr::Shown),
            ]
        );
        assert_eq!(
            catalog.classes[2].slots,
            [
                ErrorSlot::CodeMember {
                    member: "WORKFLOW_COMMAND_CONFLICT"
                },
                ErrorSlot::NeverRetryable,
                field("message", SlotRepr::Hidden),
                field("workflow_name", SlotRepr::Shown),
                workflow_id,
            ]
        );
        assert_eq!(
            catalog.classes[3].slots,
            [
                ErrorSlot::CodeMember {
                    member: "ENQUEUE_FAILED"
                },
                field("message", SlotRepr::Hidden),
                ErrorSlot::Field {
                    name: "retryable",
                    python_type: SlotType::Bool,
                    repr: SlotRepr::Shown,
                },
                field("workflow_name", SlotRepr::Shown),
                workflow_id,
                ErrorSlot::Exception {
                    python_type: ExceptionType::Exception
                },
            ]
        );
        assert_eq!(
            catalog.classes[4].slots,
            [
                ErrorSlot::CodeMember {
                    member: "ASYNC_CONTEXT"
                },
                ErrorSlot::NeverRetryable,
                field("message", SlotRepr::Hidden),
                field("workflow_name", SlotRepr::Shown),
                workflow_id,
            ]
        );
        assert!(catalog.classes[3]
            .doc
            .starts_with("Schema initialization or the start transaction failed"));
    }

    /// The id of a conflict is the canonical text of the UUID in the
    /// message and in the class; the message is Python's.
    #[test]
    fn an_id_conflict_has_python_s_message_and_its_values() {
        let workflow_id = Uuid::from_u128(0x0193_0000_0000_7000_8000_0000_0000_0001);
        let error = WorkflowStartError::id_conflict("nightly", workflow_id, "weekly".to_owned());
        assert_eq!(error.code(), WorkflowStartErrorCode::WorkflowIdConflict);
        assert_eq!(
            error.message(),
            format!(
                "workflow_id '{workflow_id}' already exists for workflow name 'weekly', cannot \
                 start workflow name 'nightly' with the same id"
            )
        );
        let values = error.slot_values();
        let named: Vec<(&str, SlotData<'_>)> = values
            .into_iter()
            .map(|slot| (slot.name, slot.value))
            .collect();
        assert_eq!(
            named,
            [
                ("message", SlotData::Str(error.message())),
                ("workflow_name", SlotData::Str("nightly")),
                ("workflow_id", SlotData::UuidText(workflow_id)),
                ("existing_workflow_name", SlotData::Str("weekly")),
            ]
        );
    }

    /// Each holder of an id that a start cannot return has its class and its
    /// message: another name is `IdConflict` with Python's message; another
    /// start command and a sub-workflow are `CommandConflict`, whose
    /// `workflow_id` names the run that holds the id. No message holds an
    /// input value.
    #[test]
    fn each_holder_of_the_id_has_its_class_and_its_message() {
        let workflow_id = Uuid::from_u128(0x0193_0000_0000_7000_8000_0000_0000_0002);
        let cases = [
            (
                IdHolder::OtherWorkflowName {
                    existing_name: "weekly".to_owned(),
                },
                WorkflowStartErrorCode::WorkflowIdConflict,
                format!(
                    "workflow_id '{workflow_id}' already exists for workflow name 'weekly', \
                     cannot start workflow name 'nightly' with the same id"
                ),
            ),
            (
                IdHolder::OtherCommand,
                WorkflowStartErrorCode::WorkflowCommandConflict,
                format!(
                    "workflow_id '{workflow_id}' already exists for workflow name 'nightly' with \
                     another start command; a start with this id must send the same command"
                ),
            ),
            (
                IdHolder::SubWorkflow,
                WorkflowStartErrorCode::WorkflowCommandConflict,
                format!(
                    "workflow_id '{workflow_id}' belongs to a sub-workflow that another workflow \
                     started; it has no start command"
                ),
            ),
        ];
        for (holder, code, message) in cases {
            let error = WorkflowStartError::id_held("nightly", workflow_id, holder);
            assert_eq!(error.code(), code);
            assert_eq!(error.message(), message);
            assert_eq!(error.workflow_name(), "nightly");
            assert_eq!(error.workflow_id(), workflow_id);
            assert!(!error.retryable());
            assert_eq!(error.failure_cause(), FailureCause::PreconditionFailed);
        }
        let conflict = WorkflowStartError::id_held("nightly", workflow_id, IdHolder::OtherCommand);
        let named: Vec<(&str, SlotData<'_>)> = conflict
            .slot_values()
            .into_iter()
            .map(|slot| (slot.name, slot.value))
            .collect();
        assert_eq!(
            named,
            [
                ("message", SlotData::Str(conflict.message())),
                ("workflow_name", SlotData::Str("nightly")),
                ("workflow_id", SlotData::UuidText(workflow_id)),
            ]
        );
    }

    /// A failed install keeps Python's message (the install's prefix), and
    /// its exception is the broker error's own exception: its own text and
    /// its retry rule.
    #[test]
    fn a_failed_install_is_enqueue_failed_with_the_broker_error_as_its_exception() {
        let workflow_id = Uuid::from_u128(7);
        let broker_error = || BrokerError::ConnectionFailed("connection refused".to_owned());
        let error = WorkflowStartError::start_failed(
            "nightly",
            workflow_id,
            ClientStartError::SchemaInstall(broker_error()),
        );
        assert_eq!(error.code(), WorkflowStartErrorCode::EnqueueFailed);
        assert_eq!(
            error.message(),
            format!("Schema initialization failed: {}", broker_error())
        );
        assert!(error.retryable());
        assert_eq!(error.workflow_id(), workflow_id);
        assert_eq!(error.failure_cause(), FailureCause::Connection);
        let exception = error
            .slot_values()
            .into_iter()
            .find(|slot| slot.name == "exception")
            .map(|slot| slot.value);
        assert_eq!(
            exception,
            Some(SlotData::Exception(ExceptionValue::CoreFailure {
                text: broker_error().to_string(),
                retryable: true,
                cause: FailureCause::Connection,
            }))
        );
    }

    /// A refusal that a check found has `NoCause()` as its exception; a
    /// refusal by a workflow error has that error's exception.
    #[test]
    fn a_refusal_by_a_check_has_no_cause_and_one_by_an_error_has_its_exception() {
        let refused = |cause: StartRefusalCause| WorkflowStartError::ValidationFailed {
            message: "refused".to_owned(),
            workflow_name: "nightly".to_owned(),
            workflow_id: Uuid::from_u128(8),
            cause,
        };
        fn exception_of(error: &WorkflowStartError) -> Option<SlotData<'_>> {
            error
                .slot_values()
                .into_iter()
                .find(|slot| slot.name == "exception")
                .map(|slot| slot.value)
        }
        let by_check = refused(StartRefusalCause::Check);
        assert_eq!(
            exception_of(&by_check),
            Some(SlotData::Exception(ExceptionValue::NoCause))
        );
        assert_eq!(by_check.failure_cause(), FailureCause::PreconditionFailed);
        let by_error = refused(StartRefusalCause::Error(Box::new(
            WorkflowError::Validation("unresolved queue".to_owned()),
        )));
        assert_eq!(
            exception_of(&by_error),
            Some(SlotData::Exception(ExceptionValue::CoreFailure {
                text: "workflow validation error: unresolved queue".to_owned(),
                retryable: false,
                cause: FailureCause::PreconditionFailed,
            }))
        );
    }
}
