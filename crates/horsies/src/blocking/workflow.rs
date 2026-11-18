//! The blocking forms of the workflow starts and of the workflow handle
//! (Python's sync `WorkflowSpec.start` and `retry_start`, and the sync
//! methods of `WorkflowHandle`).
//!
//! As for the other views, a view method has the name, the arguments and
//! the result type of the async method, and runs that method on the core's
//! process runtime.
//!
//! - On a thread that runs a tokio runtime the call runs nothing and
//!   answers the `ASYNC_CONTEXT` member of its catalog: a start answers
//!   `WorkflowStartError::AsyncContext` with the workflow id of the
//!   start, nothing written (Python `WorkflowStartAsyncContext`); `status`,
//!   `results`, `tasks`, `cancel`, `pause` and `resume` answer
//!   `HandleOperationError::AsyncContext` (Python `HandleAsyncContext`); `get`
//!   and `result_for` answer `WorkflowReadError::AsyncContext` (Python's
//!   `RetrievalCode.ASYNC_CONTEXT`).
//! - The thread check comes first, as in Python's `WorkflowSpec.start`: a
//!   template's build runs inside its start, so on a runtime thread the
//!   build does not run either. Two checks come before it: `retry_start`
//!   checks its error (`VALIDATION_FAILED` for an error that the start
//!   cannot retry), and `get` refuses a wait in a process that runs task
//!   bodies (`WaitInsideTask`).
//! - When the OS refused the threads of the process runtime, a start
//!   answers `ENQUEUE_FAILED` with the workflow id, a handle method
//!   `DB_OPERATION_FAILED`, and `get` and `result_for` `Database`, each
//!   retryable; nothing ran.
//! - In a process that was forked without `exec` after the process runtime
//!   started, every call panics before it runs anything.

use std::collections::BTreeMap;
use std::future::Future;

use horsies_core::core::workflow::validate_start_retry;
use horsies_core::process_runtime::ProcessRuntimeRefusal;
use horsies_core::{
    process_role, wait_inside_task_message, BrokerError, HandleErrorCode, HandleOperation,
    HandleOperationError, HandleResult, ProcessRole, UserCode, WorkflowError, WorkflowStartError,
    WorkflowStatus,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use super::{async_context_message, place, HorsiesView, Place};
use crate::workflow::new_workflow_id;
use crate::{
    NodeKey, NodeResult, TaskOutcome, UserErrorCode, WaitLimit, WorkflowDefinition,
    WorkflowFunction, WorkflowHandle, WorkflowNodeInfo, WorkflowReadError, WorkflowSpec,
    WorkflowTemplate,
};

/// The names of a blocking start: the workflow, and the call with its
/// async form for the `ASYNC_CONTEXT` message.
struct StartNames<'a> {
    workflow_name: &'a str,
    blocking_call: String,
    async_call: &'a str,
}

/// Run a start on the process runtime, or answer why it cannot run, with
/// `workflow_id`: the id that the start would have used, so
/// `start_with_id` or `retry_start` can run the same start later.
///
/// # Panics
///
/// In a fork ([`place`]).
fn block_start<H>(
    start: impl Future<Output = Result<H, WorkflowStartError>>,
    workflow_id: Uuid,
    names: &StartNames<'_>,
) -> Result<H, WorkflowStartError> {
    match place(format_args!("blocking {}", names.blocking_call)) {
        Place::Runs(runtime) => runtime.block_on(start),
        Place::AsyncContext => Err(WorkflowStartError::AsyncContext {
            message: async_context_message(&names.blocking_call, names.async_call),
            workflow_name: names.workflow_name.to_owned(),
            workflow_id,
        }),
        Place::RuntimeStartFailed(failed) => Err(WorkflowStartError::EnqueueFailed {
            message: format!("the start did not run: {failed}"),
            retryable: true,
            workflow_name: names.workflow_name.to_owned(),
            workflow_id,
            cause: WorkflowError::Broker(BrokerError::ProcessRuntime(
                ProcessRuntimeRefusal::StartFailed {
                    cause: failed.cause,
                    os_error: failed.os_error,
                },
            )),
        }),
    }
}

/// The blocking starts of a workflow function.
pub struct WorkflowFunctionView<'a, T> {
    function: &'a WorkflowFunction<T>,
}

impl<T> WorkflowFunction<T> {
    /// The blocking forms of this function's starts.
    pub fn blocking(&self) -> WorkflowFunctionView<'_, T> {
        WorkflowFunctionView { function: self }
    }
}

impl<T> WorkflowFunctionView<'_, T>
where
    T: DeserializeOwned + 'static,
{
    /// `WorkflowFunction::start`, blocking.
    pub fn start(&self) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let workflow_id = new_workflow_id();
        block_start(
            self.function.start_with_id(workflow_id),
            workflow_id,
            &self.names("start()", "function.start()"),
        )
    }

    /// `WorkflowFunction::start_with_id`, blocking.
    pub fn start_with_id(
        &self,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        block_start(
            self.function.start_with_id(workflow_id),
            workflow_id,
            &self.names("start_with_id()", "function.start_with_id(workflow_id)"),
        )
    }

    /// `WorkflowFunction::retry_start`, blocking. The error is checked
    /// first, on any thread, as in Python: `VALIDATION_FAILED` for an error
    /// that the start cannot retry. On a runtime thread the answer is
    /// `ASYNC_CONTEXT` with the workflow id of the error.
    pub fn retry_start(
        &self,
        error: &WorkflowStartError,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let workflow_id = validate_start_retry(error, self.function.name())?;
        block_start(
            self.function.retry_start(error),
            workflow_id,
            &self.names("retry_start()", "function.retry_start(&error)"),
        )
    }

    fn names<'s>(&'s self, method: &str, async_call: &'s str) -> StartNames<'s> {
        StartNames {
            workflow_name: self.function.name(),
            blocking_call: format!(
                "WorkflowFunction::{method} of workflow '{}'",
                self.function.name()
            ),
            async_call,
        }
    }
}

/// The blocking starts of a workflow template.
pub struct WorkflowTemplateView<'a, P, T> {
    template: &'a WorkflowTemplate<P, T>,
}

impl<P, T> WorkflowTemplate<P, T> {
    /// The blocking forms of this template's starts.
    pub fn blocking(&self) -> WorkflowTemplateView<'_, P, T> {
        WorkflowTemplateView { template: self }
    }
}

impl<P, T> WorkflowTemplateView<'_, P, T>
where
    P: Serialize + DeserializeOwned + 'static,
    T: DeserializeOwned + 'static,
{
    /// `WorkflowTemplate::start`, blocking. On a runtime thread the build
    /// does not run.
    pub fn start(&self, params: P) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let workflow_id = new_workflow_id();
        block_start(
            self.template.start_with_id(params, workflow_id),
            workflow_id,
            &self.names("start()", "template.start(params)"),
        )
    }

    /// `WorkflowTemplate::start_with_id`, blocking. On a runtime thread the
    /// build does not run.
    pub fn start_with_id(
        &self,
        params: P,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        block_start(
            self.template.start_with_id(params, workflow_id),
            workflow_id,
            &self.names(
                "start_with_id()",
                "template.start_with_id(params, workflow_id)",
            ),
        )
    }

    fn names<'s>(&'s self, method: &str, async_call: &'s str) -> StartNames<'s> {
        StartNames {
            workflow_name: self.template.name(),
            blocking_call: format!(
                "WorkflowTemplate::{method} of workflow '{}'",
                self.template.name()
            ),
            async_call,
        }
    }
}

impl HorsiesView<'_> {
    /// `Horsies::start`, blocking.
    pub fn start<T>(&self, spec: &WorkflowSpec) -> Result<WorkflowHandle<T>, WorkflowStartError>
    where
        T: DeserializeOwned + 'static,
    {
        let workflow_id = new_workflow_id();
        block_start(
            self.app.start_with_id::<T>(spec, workflow_id),
            workflow_id,
            &spec_names(spec, "start()", "app.start(&spec)"),
        )
    }

    /// `Horsies::start_with_id`, blocking.
    pub fn start_with_id<T>(
        &self,
        spec: &WorkflowSpec,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError>
    where
        T: DeserializeOwned + 'static,
    {
        block_start(
            self.app.start_with_id::<T>(spec, workflow_id),
            workflow_id,
            &spec_names(
                spec,
                "start_with_id()",
                "app.start_with_id(&spec, workflow_id)",
            ),
        )
    }
}

fn spec_names<'s>(spec: &'s WorkflowSpec, method: &str, async_call: &'s str) -> StartNames<'s> {
    StartNames {
        workflow_name: spec.name(),
        blocking_call: format!("Horsies::{method} of workflow '{}'", spec.name()),
        async_call,
    }
}

/// [`crate::start_workflow`], blocking: start the workflow definition `D`,
/// which takes no params, through its first registration in this process.
///
/// Errors: as the async function. On a runtime thread `ASYNC_CONTEXT`,
/// also before the registration of `D`.
pub fn start_workflow<D>() -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
where
    D: WorkflowDefinition<Params = ()>,
    D::Output: DeserializeOwned + Send + Sync,
{
    let workflow_id = new_workflow_id();
    block_start(
        crate::workflow::start_workflow_as::<D>(workflow_id),
        workflow_id,
        &definition_names::<D>("start_workflow()", "start_workflow::<D>()"),
    )
}

/// [`crate::start_workflow_with`], blocking: start the workflow definition
/// `D` with `params`, through its first registration in this process.
///
/// Errors: as the async function. On a runtime thread `ASYNC_CONTEXT`,
/// also before the registration of `D`; the build does not run.
pub fn start_workflow_with<D>(
    params: D::Params,
) -> Result<WorkflowHandle<D::Output>, WorkflowStartError>
where
    D: WorkflowDefinition,
    D::Params: Send + Sync,
    D::Output: DeserializeOwned + Send + Sync,
{
    let workflow_id = new_workflow_id();
    block_start(
        crate::workflow::start_workflow_with_as::<D>(params, workflow_id),
        workflow_id,
        &definition_names::<D>("start_workflow_with()", "start_workflow_with::<D>(params)"),
    )
}

fn definition_names<D: WorkflowDefinition>(
    function: &str,
    async_call: &'static str,
) -> StartNames<'static> {
    StartNames {
        workflow_name: D::name(),
        blocking_call: format!("{function} of workflow '{}'", D::name()),
        async_call,
    }
}

/// The blocking reads and changes of a workflow handle.
pub struct WorkflowHandleView<'a, T> {
    handle: &'a WorkflowHandle<T>,
}

impl<T> WorkflowHandle<T> {
    /// The blocking forms of this handle's reads and changes.
    pub fn blocking(&self) -> WorkflowHandleView<'_, T> {
        WorkflowHandleView { handle: self }
    }
}

impl<T> WorkflowHandleView<'_, T> {
    /// `WorkflowHandle::status`, blocking.
    pub fn status(&self) -> HandleResult<WorkflowStatus> {
        self.block_handle(self.handle.status(), "status()", HandleOperation::Status)
    }

    /// `WorkflowHandle::results`, blocking.
    pub fn results(&self) -> HandleResult<BTreeMap<String, NodeResult>> {
        self.block_handle(self.handle.results(), "results()", HandleOperation::Results)
    }

    /// `WorkflowHandle::tasks`, blocking.
    pub fn tasks(&self) -> HandleResult<Vec<WorkflowNodeInfo>> {
        self.block_handle(self.handle.tasks(), "tasks()", HandleOperation::Tasks)
    }

    /// `WorkflowHandle::cancel`, blocking.
    pub fn cancel(&self) -> HandleResult<()> {
        self.block_handle(self.handle.cancel(), "cancel()", HandleOperation::Cancel)
    }

    /// `WorkflowHandle::pause`, blocking.
    pub fn pause(&self) -> HandleResult<bool> {
        self.block_handle(self.handle.pause(), "pause()", HandleOperation::Pause)
    }

    /// `WorkflowHandle::resume`, blocking.
    pub fn resume(&self) -> HandleResult<bool> {
        self.block_handle(self.handle.resume(), "resume()", HandleOperation::Resume)
    }

    /// `WorkflowHandle::result_for`, blocking. Not a wait: it runs in a
    /// process that runs task bodies too.
    pub fn result_for<V, E>(&self, key: &NodeKey<V>) -> Result<TaskOutcome<V, E>, WorkflowReadError>
    where
        V: DeserializeOwned,
        E: UserErrorCode,
    {
        self.block_read(
            self.handle.result_for::<V, E>(key),
            "result_for()",
            "result_for(&key)",
        )
    }

    /// Run a handle method on the process runtime, or answer why it cannot
    /// run. A process runtime that did not start is `DB_OPERATION_FAILED`
    /// of `operation`, retryable, with the refusal as its cause.
    ///
    /// # Panics
    ///
    /// In a fork ([`place`]).
    fn block_handle<V>(
        &self,
        call: impl Future<Output = HandleResult<V>>,
        method: &str,
        operation: HandleOperation,
    ) -> HandleResult<V> {
        let workflow_id = self.handle.workflow_id();
        match place(format_args!(
            "blocking WorkflowHandle::{method} for workflow {workflow_id}"
        )) {
            Place::Runs(runtime) => runtime.block_on(call),
            Place::AsyncContext => Err(HandleOperationError::AsyncContext {
                message: async_context_message(
                    &format!("WorkflowHandle::{method} for workflow {workflow_id}"),
                    &format!("handle.{method}"),
                ),
                workflow_id,
            }),
            Place::RuntimeStartFailed(failed) => {
                let message = operation
                    .failure_message(workflow_id, &format!("the call did not run: {failed}"));
                Err(HandleOperationError::DbOperationFailed {
                    message,
                    retryable: true,
                    workflow_id,
                    cause: WorkflowError::Broker(BrokerError::ProcessRuntime(
                        ProcessRuntimeRefusal::StartFailed {
                            cause: failed.cause,
                            os_error: failed.os_error,
                        },
                    )),
                })
            }
        }
    }

    /// Run a read of the outcome on the process runtime, or answer why it
    /// cannot run.
    ///
    /// # Panics
    ///
    /// In a fork ([`place`]).
    fn block_read<V>(
        &self,
        read: impl Future<Output = Result<V, WorkflowReadError>>,
        method: &str,
        async_method: &str,
    ) -> Result<V, WorkflowReadError> {
        let workflow_id = self.handle.workflow_id();
        match place(format_args!(
            "blocking WorkflowHandle::{method} for workflow {workflow_id}"
        )) {
            Place::Runs(runtime) => runtime.block_on(read),
            Place::AsyncContext => Err(WorkflowReadError::AsyncContext {
                message: async_context_message(
                    &format!("WorkflowHandle::{method} for workflow {workflow_id}"),
                    &format!("handle.{async_method}"),
                ),
            }),
            Place::RuntimeStartFailed(failed) => Err(WorkflowReadError::Database {
                code: HandleErrorCode::DbOperationFailed,
                message: format!("the read did not run: {failed}"),
                retryable: true,
            }),
        }
    }
}

impl<T> WorkflowHandleView<'_, T>
where
    T: DeserializeOwned + 'static,
{
    /// `WorkflowHandle::get`, blocking. In Python's order: a process that
    /// runs task bodies refuses the wait (`WaitInsideTask`), then a runtime
    /// thread answers `AsyncContext`.
    pub fn get(&self, limit: WaitLimit) -> Result<TaskOutcome<T, UserCode>, WorkflowReadError> {
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(WorkflowReadError::WaitInsideTask {
                message: wait_inside_task_message(&format!(
                    "WorkflowHandle.get() for workflow {}",
                    self.handle.workflow_id()
                )),
            });
        }
        self.block_read(self.handle.get(limit), "get()", "get(limit)")
    }
}

#[cfg(test)]
mod tests;
