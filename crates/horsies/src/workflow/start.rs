//! Starting a workflow (Python `WorkflowSpec.start`, `start_workflow`).
//!
//! A start, in order: the definition key of the spec (`VALIDATION_FAILED`
//! without one), the output type against the handle's type, the spec
//! checked against the app's registered definitions and converted to the
//! core's spec data, the connect of the handle, then the core's start of
//! the tree, whose sub-workflow roots build from the app's registry. With
//! `AppConfig.resend_on_transient_err` the core runs a retryable failure of
//! the start again, with the same workflow id and start time, up to 3
//! times. In a worker child the start goes through the child's producer
//! handle, as a send does.

use std::sync::Arc;

use chrono::Utc;
use horsies_core::core::workflow::validate_start_retry;
use horsies_core::handle::CoreHandle;
use horsies_core::{
    child_producer, ChildProducer, ClientStartError, ClientStartOutcome, FailureCause,
    WorkflowError, WorkflowStartError,
};
use uuid::Uuid;

use super::handle::WorkflowHandle;
use super::registry::WorkflowRegistry;
use super::spec::WorkflowSpec;
use crate::process_state::refuse_in_a_fork;
use crate::{Horsies, HorsiesError};

/// What a start sends with: the app's handle, its workflow registry and
/// its resend setting. Clones share one app.
#[derive(Clone)]
pub(crate) struct WorkflowStarter {
    handle: Arc<CoreHandle>,
    workflows: Arc<WorkflowRegistry>,
}

impl std::fmt::Debug for WorkflowStarter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowStarter")
            .finish_non_exhaustive()
    }
}

impl WorkflowStarter {
    /// The starter of `app`. No I/O.
    pub(crate) fn of(app: &Horsies) -> Self {
        Self {
            handle: Arc::clone(app.handle()),
            workflows: Arc::clone(app.workflows()),
        }
    }

    /// The handle that a start and its reads go through: the child's
    /// producer handle in a worker child, else the app's.
    fn operation_handle(&self) -> Arc<CoreHandle> {
        match child_producer() {
            ChildProducer::InChild(handle) => handle,
            ChildProducer::NotInAChild => Arc::clone(&self.handle),
        }
    }

    /// The handle of `workflow_id` with the result type `T`. No I/O.
    pub(crate) fn handle_of<T>(&self, workflow_id: Uuid) -> WorkflowHandle<T> {
        WorkflowHandle::new(
            workflow_id,
            self.operation_handle(),
            Arc::clone(&self.workflows),
        )
    }

    /// Start `spec` as the workflow `workflow_id`, with the result type `T`.
    ///
    /// Outcomes: the handle of the new workflow, or of the workflow that
    /// already has this id and this name (nothing written).
    ///
    /// Errors: `VALIDATION_FAILED` for a spec without a definition key, an
    /// output type that is not `T`, and a spec that the registry or the
    /// conversion refuses, before any I/O; `WORKFLOW_ID_CONFLICT` when the
    /// id belongs to another workflow name (nothing written);
    /// `ENQUEUE_FAILED` with `retryable` for a connect, a schema install or
    /// a start that failed (nothing committed, or the commit reply was
    /// lost: a start again with the same id finds the workflow).
    ///
    /// # Panics
    ///
    /// After the checks before any I/O, in a process that was forked
    /// without `exec` after the core's process runtime started. Nothing
    /// was written.
    pub(crate) async fn start<T: 'static>(
        &self,
        spec: &WorkflowSpec,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let validation_failed = |message: String| WorkflowStartError::ValidationFailed {
            message,
            workflow_name: spec.name().to_owned(),
            workflow_id,
            cause: FailureCause::PreconditionFailed,
        };
        let validation = |error: HorsiesError| validation_failed(error.to_string());
        if spec
            .definition_key()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
        {
            return Err(validation_failed(format!(
                "WorkflowSpec is missing definition_key: workflow '{}' has no stable \
                 definition_key; set a definition key such as 'your.workflow.v1'",
                spec.name()
            )));
        }
        spec.check_output_type::<T>().map_err(validation)?;
        let client = self.workflows.prepare(spec).map_err(validation)?;
        refuse_in_a_fork(format_args!("start of workflow '{}'", spec.name()));
        let handle = self.operation_handle();
        let broker = handle.initialized_broker().await.map_err(|error| {
            WorkflowStartError::EnqueueFailed {
                message: format!("Schema initialization failed: {error}"),
                retryable: error.is_retryable(),
                workflow_name: spec.name().to_owned(),
                workflow_id,
                cause: error.failure_cause(),
            }
        })?;
        let started = broker
            .start_client_workflow(
                &client,
                workflow_id,
                Utc::now(),
                self.workflows.config().resend_on_transient_err,
                &*self.workflows,
            )
            .await;
        match started {
            Ok(ClientStartOutcome::Started | ClientStartOutcome::Exists) => Ok(
                WorkflowHandle::new(workflow_id, handle, Arc::clone(&self.workflows)),
            ),
            Ok(ClientStartOutcome::IdTaken { existing_name }) => {
                Err(WorkflowStartError::IdConflict {
                    message: format!(
                        "workflow_id '{workflow_id}' already exists for workflow name \
                         '{existing_name}', cannot start workflow name '{}' with the same id",
                        spec.name(),
                    ),
                    workflow_name: spec.name().to_owned(),
                    workflow_id,
                    existing_workflow_name: existing_name,
                })
            }
            Err(error) => Err(start_error(&error, spec.name(), workflow_id)),
        }
    }

    /// Start `spec` again after `error`, with its workflow id (Python
    /// `retry_start`).
    ///
    /// Errors: `VALIDATION_FAILED` for an error that is not
    /// `ENQUEUE_FAILED` or `ASYNC_CONTEXT`, an error of another workflow
    /// name, and an error without a workflow id; else the errors of
    /// [`Self::start`].
    pub(crate) async fn retry_start<T: 'static>(
        &self,
        spec: &WorkflowSpec,
        error: &WorkflowStartError,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let workflow_id = validate_start_retry(error, spec.name())?;
        self.start(spec, workflow_id).await
    }
}

/// A new workflow id.
pub(crate) fn new_workflow_id() -> Uuid {
    Uuid::new_v4()
}

/// The start error of the failed start `workflow_id` of `workflow_name`: a
/// spec or a child spec that the core refuses is `VALIDATION_FAILED`; the
/// schema install, the database, the client family check and a child build
/// that raised are `ENQUEUE_FAILED`, with the retry rule of the cause. Pure.
///
/// Panics: an error that a start never gives (a defect).
fn start_error(
    error: &ClientStartError,
    workflow_name: &str,
    workflow_id: Uuid,
) -> WorkflowStartError {
    match error {
        ClientStartError::Start(WorkflowError::Serialization(_) | WorkflowError::Validation(_)) => {
            WorkflowStartError::ValidationFailed {
                message: error.to_string(),
                workflow_name: workflow_name.to_owned(),
                workflow_id,
                cause: error.failure_cause(),
            }
        }
        ClientStartError::SchemaInstall(_)
        | ClientStartError::Start(
            WorkflowError::Broker(_)
            | WorkflowError::StepContract(_)
            | WorkflowError::ChildSpecSource(_),
        ) => WorkflowStartError::EnqueueFailed {
            message: error.to_string(),
            retryable: error.is_retryable(),
            workflow_name: workflow_name.to_owned(),
            workflow_id,
            cause: error.failure_cause(),
        },
        ClientStartError::Start(
            WorkflowError::WorkflowNotFound { .. } | WorkflowError::InvalidStatus(_),
        ) => panic!(
            "the start of workflow {workflow_id} returned an error outside the start catalog: \
             {error}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerError, ChildSpecUnavailable, WorkflowStartErrorCode};

    use super::*;
    use crate::workflow::node::tests::task_node;
    use crate::workflow::{
        SubWorkflowParams, WorkflowDefConfig, WorkflowDefinition, WorkflowSpecBuilder,
    };
    use crate::{AppConfig, HorsiesError};

    /// The child of the starter test, registered after the starter is made.
    struct LateChild;

    impl WorkflowDefinition for LateChild {
        type Output = u32;
        type Params = ();

        fn name() -> &'static str {
            "late_child"
        }

        fn definition_key() -> &'static str {
            "tests.start.late_child.v1"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let tick = builder.task(task_node::<u32, ()>("tick"));
            Ok(WorkflowDefConfig::new().output(tick))
        }
    }

    /// A starter, and each clone of it, reads the registry of its app as the
    /// registry grows: a spec whose child registers after the starter was
    /// made passes the check of the start. No I/O.
    #[test]
    fn a_starter_made_before_a_registration_checks_against_it() {
        let mut app = Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap();
        let starter = WorkflowStarter::of(&app);
        let clone = starter.clone();
        let mut builder = WorkflowSpecBuilder::new("late_parent");
        builder.definition_key("tests.start.late_parent.v1");
        builder.sub_workflow(LateChild::node(SubWorkflowParams::NotGiven).unwrap());
        let spec = builder.build().unwrap();
        assert!(
            starter.workflows.prepare(&spec).is_err(),
            "the child is not registered yet"
        );
        let _child = app.register_workflow_definition::<LateChild>().unwrap();
        assert!(starter.workflows.prepare(&spec).is_ok());
        assert!(clone.workflows.prepare(&spec).is_ok());
    }

    /// A failed start takes the code of its class: a spec that the core
    /// refuses, or a value of it that does not encode, is
    /// `VALIDATION_FAILED`; the schema install, a broker failure and a child
    /// build that raised are `ENQUEUE_FAILED`.
    #[test]
    fn a_failed_start_takes_the_code_of_its_class() {
        let not_json = serde_json::from_str::<u8>("not json").expect_err("the text is not JSON");
        let cases = [
            (
                ClientStartError::SchemaInstall(BrokerError::ListenerClosed),
                WorkflowStartErrorCode::EnqueueFailed,
            ),
            (
                ClientStartError::Start(WorkflowError::Validation("no queue".to_owned())),
                WorkflowStartErrorCode::ValidationFailed,
            ),
            (
                ClientStartError::Start(WorkflowError::Serialization(not_json)),
                WorkflowStartErrorCode::ValidationFailed,
            ),
            (
                ClientStartError::Start(WorkflowError::Broker(BrokerError::ListenerClosed)),
                WorkflowStartErrorCode::EnqueueFailed,
            ),
            (
                ClientStartError::Start(WorkflowError::ChildSpecSource(
                    ChildSpecUnavailable::Raised {
                        subject: "build of the child of parent:1".to_owned(),
                        cause: "refused".to_owned(),
                    },
                )),
                WorkflowStartErrorCode::EnqueueFailed,
            ),
        ];
        for (error, code) in cases {
            let failed = start_error(&error, "nightly", Uuid::nil());
            assert_eq!(failed.code(), code, "{error}");
            assert_eq!(failed.workflow_name(), "nightly", "{error}");
            assert_eq!(failed.workflow_id(), Uuid::nil(), "{error}");
            assert_eq!(failed.failure_cause(), error.failure_cause(), "{error}");
        }
    }
}
