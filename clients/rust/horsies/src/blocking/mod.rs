//! Blocking forms of the async API, for a program that does not run a
//! tokio runtime (Python's sync calls: `send`, `resend`,
//! `TaskHandle.get` and `info`, the app's reads by task id and health
//! reads, `check(live=True)`, the workflow starts and the sync methods of
//! `WorkflowHandle`).
//!
//! Each async type gives a view with `blocking()`: [`TaskFunctionView`],
//! [`TaskHandleView`], [`HorsiesView`], [`WorkflowFunctionView`],
//! [`WorkflowTemplateView`] and [`WorkflowHandleView`]. The starts of a
//! definition by its type are [`HorsiesView::start_workflow`] and
//! [`HorsiesView::start_workflow_with`]. A view method has the name, the
//! arguments and the result type of the async method. It runs the async
//! method on the core's process runtime, which drives the app's
//! connections, and blocks the calling thread until the method ends.
//!
//! ```ignore
//! let send_email = send_email::register(&mut app)?;
//! let handle = send_email.blocking().send(input)?;
//! let outcome = handle.blocking().get(WaitLimit::NoLimit)?;
//! ```
//!
//! - On a thread that runs a tokio runtime, a blocking call would stall the
//!   runtime, so it runs nothing and answers the `ASYNC_CONTEXT` member of
//!   its catalog, as a Python sync call inside a running event loop does:
//!   `TaskSendError::AsyncContext` (with the prepared task id and payload,
//!   so `resend(&error).await` completes the same send),
//!   `ReadError::AsyncContext`, `BrokerOperationError::AsyncContext`, a
//!   `CheckReport` with `HRS-217`,
//!   `WorkflowStartErrorCode::AsyncContext` with the workflow id of the
//!   start, `HandleErrorCode::AsyncContext`, or
//!   `WorkflowReadError::AsyncContext`.
//! - A read that takes a `WaitLimit` checks it first, on any thread: a
//!   `Within` limit above `MAX_WAIT_LIMIT_MS` is `ReadError::InvalidInput`
//!   or `WorkflowReadError::InvalidInput`, and nothing runs.
//! - The thread of a blocking task body in a worker child (a `fn` task)
//!   drives no async task: a blocking call works there, also where the
//!   child runtime's handle is set.
//! - When the OS refused the threads of the process runtime, the call
//!   answers the retryable database failure of its catalog with the cause;
//!   nothing ran, and a later call tries the start again.
//! - In a process that was forked without `exec` after the process runtime
//!   started, every call panics before it runs anything: a fork is a state
//!   of the process, never a result, as every Python core call raises
//!   `ForkedAfterCoreStart`.

mod workflow;

pub use workflow::{WorkflowFunctionView, WorkflowHandleView, WorkflowTemplateView};

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use horsies_core::process_runtime::ProcessRuntimeRefusal;
use horsies_core::{
    process_role, thread_role, BrokerError, BrokerErrorCode, BrokerOperationError,
    BrokerOperationFailedCode, BrokerOperationSource, ClassCoverage, ClassReport, DatabasePing,
    FailureCause, ProcessRole, SendKind, TaskSendError, ThreadRole, WorkerPong,
    WorkerStateSnapshot,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::runtime::{Handle, Runtime};
use uuid::Uuid;

use crate::check::{connectivity_report, CHECK};
use crate::handle::checked_task_wait;
use crate::process_state::{runtime_for, RuntimeStartFailed};
use crate::{
    CheckPassed, CheckReport, CustomErrorCode, ErrorCode, Horsies, HorsiesError, PingTarget,
    ReadError, StoredResult, TaskFunction, TaskHandle, TaskInfo, TaskInfoParts, TaskOutcome,
    TaskSendOptions, WaitLimit, WorkerHistoryLimit,
};

/// Where a blocking call can run.
enum Place {
    /// On the process runtime, from this thread.
    Runs(&'static Runtime),
    /// This thread runs a tokio runtime: a wait would stall it.
    AsyncContext,
    /// The OS refused the threads of the process runtime. A later call
    /// tries the start again, so the call's failure is retryable.
    RuntimeStartFailed(RuntimeStartFailed),
}

/// Where the blocking call `call` from this thread runs. Starts the process
/// runtime at its first use.
///
/// # Panics
///
/// In a fork, without `exec`, of the process that started the process
/// runtime (`process_state::runtime_for`), on a thread that is not a
/// runtime thread. Nothing ran.
fn place(call: fmt::Arguments<'_>) -> Place {
    match (Handle::try_current().is_ok(), thread_role()) {
        (true, ThreadRole::Other) => Place::AsyncContext,
        (false, _) | (true, ThreadRole::BlockingTaskBody) => match runtime_for(call) {
            Ok(runtime) => Place::Runs(runtime.runtime()),
            Err(failed) => Place::RuntimeStartFailed(failed),
        },
    }
}

/// The `ASYNC_CONTEXT` message: what was refused, why, and what to call
/// (Python `async_context_message`). Pure.
fn async_context_message(blocking_call: &str, async_call: &str) -> String {
    format!(
        "blocking {blocking_call} was called on a thread that runs a tokio runtime; its wait \
         would stall the runtime, so nothing ran. Use `{async_call}.await` instead."
    )
}

/// The blocking forms of a task function's sends.
pub struct TaskFunctionView<'a, A, T, E> {
    function: &'a TaskFunction<A, T, E>,
}

impl<A, T, E> TaskFunction<A, T, E> {
    /// The blocking forms of this function's sends.
    pub fn blocking(&self) -> TaskFunctionView<'_, A, T, E> {
        TaskFunctionView { function: self }
    }
}

impl<'a, A, T, E> TaskFunctionView<'a, A, T, E>
where
    A: Serialize + DeserializeOwned + 'static,
    T: Serialize + DeserializeOwned + 'static,
    E: CustomErrorCode,
{
    /// `TaskFunction::send`, blocking.
    pub fn send(&self, input: A) -> Result<TaskHandle<T, E>, TaskSendError> {
        self.send_with_options(TaskSendOptions::new(), input)
    }

    /// `TaskFunction::send_with_options`, blocking.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the blocking form takes the arguments of \
                  `TaskFunction::send_with_options` by value; a reference would change \
                  every caller"
    )]
    pub fn send_with_options(
        &self,
        options: TaskSendOptions,
        input: A,
    ) -> Result<TaskHandle<T, E>, TaskSendError> {
        self.first_send(&options, &input)
    }

    /// `TaskFunction::with_options`, blocking.
    pub fn with_options(
        &self,
        options: TaskSendOptions,
    ) -> TaskFunctionWithOptionsView<'a, A, T, E> {
        TaskFunctionWithOptionsView {
            function: self.function,
            options,
        }
    }

    /// `TaskFunction::resend`, blocking. On a runtime thread it answers
    /// `AsyncContext` again, with the same task id and payload.
    pub fn resend(&self, error: &TaskSendError) -> Result<TaskHandle<T, E>, TaskSendError> {
        let (task_id, payload) = self.function.checked_resend_target(error)?;
        self.enqueue(task_id, payload, SendKind::Repeat, "resend")
    }

    /// The checks of a first send, then the enqueue. A refused input is
    /// refused before the thread is checked, as in Python.
    fn first_send(
        &self,
        options: &TaskSendOptions,
        input: &A,
    ) -> Result<TaskHandle<T, E>, TaskSendError> {
        let (task_id, payload) = self.function.prepare(options, input)?;
        self.enqueue(task_id, payload, SendKind::First, "send")
    }

    /// The enqueue with resends of a prepared send, on the process runtime.
    ///
    /// # Panics
    ///
    /// In a fork, without `exec`, of the process that started the process
    /// runtime ([`place`]). Nothing was sent.
    fn enqueue(
        &self,
        task_id: Uuid,
        payload: horsies_core::TaskSendPayload,
        kind: SendKind,
        call: &str,
    ) -> Result<TaskHandle<T, E>, TaskSendError> {
        match place(format_args!(
            "blocking {call} of task '{}'",
            self.function.task_name()
        )) {
            Place::Runs(runtime) => {
                runtime.block_on(self.function.enqueue_with_resends(task_id, payload, kind))
            }
            Place::AsyncContext => Err(TaskSendError::AsyncContext {
                task_id,
                payload: Box::new(payload),
                message: format!(
                    "blocking {call} of task '{}' was called on a thread that runs a tokio \
                     runtime; its wait would stall the runtime, so nothing was written. Use \
                     `{call}(...).await`; an async resend with this error sends the same task \
                     id and payload.",
                    self.function.task_name()
                ),
            }),
            Place::RuntimeStartFailed(failed) => Err(start_failed_send(task_id, payload, &failed)),
        }
    }
}

/// The answer of a blocking send whose process runtime the OS refused to
/// start: `EnqueueFailed` with the prepared task id and payload, retryable,
/// since the next use tries the start again. Nothing was sent. Pure.
fn start_failed_send(
    task_id: Uuid,
    payload: horsies_core::TaskSendPayload,
    failed: &RuntimeStartFailed,
) -> TaskSendError {
    TaskSendError::EnqueueFailed {
        task_id,
        payload: Box::new(payload),
        message: format!("the send did not start: {failed}"),
        retryable: true,
        cause: FailureCause::PreconditionFailed,
    }
}

/// The blocking sends of one task with per-send options.
pub struct TaskFunctionWithOptionsView<'a, A, T, E> {
    function: &'a TaskFunction<A, T, E>,
    options: TaskSendOptions,
}

impl<A, T, E> TaskFunctionWithOptionsView<'_, A, T, E>
where
    A: Serialize + DeserializeOwned + 'static,
    T: Serialize + DeserializeOwned + 'static,
    E: CustomErrorCode,
{
    /// Send the task with these options, now or after their delay,
    /// blocking.
    pub fn send(&self, input: A) -> Result<TaskHandle<T, E>, TaskSendError> {
        self.function
            .blocking()
            .send_with_options(self.options.clone(), input)
    }
}

/// The blocking reads of a task handle.
pub struct TaskHandleView<'a, T, E> {
    handle: &'a TaskHandle<T, E>,
}

impl<T, E> TaskHandle<T, E> {
    /// The blocking forms of this handle's reads.
    pub fn blocking(&self) -> TaskHandleView<'_, T, E> {
        TaskHandleView { handle: self }
    }
}

impl<T, E> TaskHandleView<'_, T, E>
where
    T: DeserializeOwned + Clone,
    E: CustomErrorCode,
{
    /// `TaskHandle::get`, blocking. The limit is the first check, as in
    /// the async form: a `Within` limit above `MAX_WAIT_LIMIT_MS` is
    /// `InvalidInput`. Then, in Python's order: a process that runs task
    /// bodies refuses the wait; a kept read is returned; a runtime thread
    /// answers `AsyncContext`, which is not kept.
    pub fn get(&self, limit: WaitLimit) -> Result<TaskOutcome<T, E>, ReadError> {
        let _wait = checked_task_wait(self.handle.task_id(), limit)?;
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(ReadError::wait_inside_task(&format!(
                "TaskHandle.get() for task {}",
                self.handle.task_id()
            )));
        }
        if let Some(kept) = self.handle.kept() {
            return kept;
        }
        block_read(
            self.handle.get(limit),
            &format!("TaskHandle::get() for task {}", self.handle.task_id()),
            "handle.get(limit)",
        )
    }

    /// `TaskHandle::info`, blocking.
    pub fn info(&self, parts: TaskInfoParts) -> Result<TaskInfo, ReadError> {
        block_read(
            self.handle.info(parts),
            &format!("TaskHandle::info() for task {}", self.handle.task_id()),
            "handle.info(parts)",
        )
    }
}

/// Run a read on the process runtime, or answer why it cannot run.
///
/// # Panics
///
/// In a fork ([`place`]).
fn block_read<V>(
    read: impl Future<Output = Result<V, ReadError>>,
    blocking_call: &str,
    async_call: &str,
) -> Result<V, ReadError> {
    match place(format_args!("blocking {blocking_call}")) {
        Place::Runs(runtime) => runtime.block_on(read),
        Place::AsyncContext => Err(ReadError::AsyncContext {
            message: async_context_message(blocking_call, async_call),
        }),
        Place::RuntimeStartFailed(failed) => Err(ReadError::Database {
            code: BrokerErrorCode::TaskInfoQueryFailed,
            message: format!("the read did not start: {failed}"),
            retryable: true,
        }),
    }
}

/// Run a health read on the process runtime, or answer why it cannot run.
/// A runtime that did not start has the broker code `code` of the read,
/// retryable, with the refusal as its cause.
///
/// # Panics
///
/// In a fork ([`place`]).
fn block_health<V>(
    read: impl Future<Output = Result<V, BrokerOperationError>>,
    code: BrokerOperationFailedCode,
    blocking_call: &str,
    async_call: &str,
) -> Result<V, BrokerOperationError> {
    match place(format_args!("blocking {blocking_call}")) {
        Place::Runs(runtime) => runtime.block_on(read),
        Place::AsyncContext => Err(BrokerOperationError::AsyncContext {
            message: async_context_message(blocking_call, async_call),
        }),
        Place::RuntimeStartFailed(failed) => Err(BrokerOperationError::operation_failed(
            code,
            format!("the read did not start: {failed}"),
            BrokerOperationSource::Broker(BrokerError::ProcessRuntime(
                ProcessRuntimeRefusal::StartFailed {
                    cause: failed.cause,
                    os_error: failed.os_error,
                },
            )),
        )),
    }
}

/// The blocking reads of the app.
pub struct HorsiesView<'a> {
    app: &'a Horsies,
}

impl Horsies {
    /// The blocking forms of the app's reads and of `check_live`.
    pub fn blocking(&self) -> HorsiesView<'_> {
        HorsiesView { app: self }
    }
}

impl HorsiesView<'_> {
    /// `Horsies::get_result`, blocking.
    pub fn get_result<T, E>(
        &self,
        task_id: Uuid,
        limit: WaitLimit,
    ) -> Result<TaskOutcome<T, E>, ReadError>
    where
        T: DeserializeOwned + Clone,
        E: CustomErrorCode,
    {
        self.app
            .task_handle_of::<T, E>(task_id)
            .blocking()
            .get(limit)
    }

    /// `Horsies::get_raw_result`, blocking. The limit is the first check,
    /// before the process and the thread checks.
    pub fn get_raw_result(
        &self,
        task_id: Uuid,
        limit: WaitLimit,
    ) -> Result<StoredResult, ReadError> {
        let _wait = checked_task_wait(task_id, limit)?;
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(ReadError::wait_inside_task(&format!(
                "Horsies.get_raw_result() for task {task_id}"
            )));
        }
        block_read(
            self.app.get_raw_result(task_id, limit),
            &format!("Horsies::get_raw_result() for task {task_id}"),
            "app.get_raw_result(task_id, limit)",
        )
    }

    /// `Horsies::get_task_info`, blocking.
    pub fn get_task_info(
        &self,
        task_id: Uuid,
        parts: TaskInfoParts,
    ) -> Result<TaskInfo, ReadError> {
        block_read(
            self.app.get_task_info(task_id, parts),
            &format!("Horsies::get_task_info() for task {task_id}"),
            "app.get_task_info(task_id, parts)",
        )
    }

    /// `Horsies::ping_database`, blocking.
    pub fn ping_database(&self) -> Result<DatabasePing, BrokerOperationError> {
        block_health(
            self.app.ping_database(),
            BrokerOperationFailedCode::DbPingFailed,
            "Horsies::ping_database()",
            "app.ping_database()",
        )
    }

    /// `Horsies::ping_workers`, blocking.
    pub fn ping_workers(
        &self,
        target: PingTarget,
        timeout: Duration,
        min_responses: Option<NonZeroUsize>,
    ) -> Result<Vec<WorkerPong>, BrokerOperationError> {
        block_health(
            self.app.ping_workers(target, timeout, min_responses),
            BrokerOperationFailedCode::WorkerPingFailed,
            "Horsies::ping_workers()",
            "app.ping_workers(target, timeout, min_responses)",
        )
    }

    /// `Horsies::list_worker_states`, blocking.
    pub fn list_worker_states(&self) -> Result<Vec<WorkerStateSnapshot>, BrokerOperationError> {
        block_health(
            self.app.list_worker_states(),
            BrokerOperationFailedCode::MonitoringQueryFailed,
            "Horsies::list_worker_states()",
            "app.list_worker_states()",
        )
    }

    /// `Horsies::get_worker_state`, blocking.
    pub fn get_worker_state(
        &self,
        worker_id: &str,
    ) -> Result<Option<WorkerStateSnapshot>, BrokerOperationError> {
        block_health(
            self.app.get_worker_state(worker_id),
            BrokerOperationFailedCode::MonitoringQueryFailed,
            "Horsies::get_worker_state()",
            "app.get_worker_state(worker_id)",
        )
    }

    /// `Horsies::get_worker_state_history`, blocking.
    pub fn get_worker_state_history(
        &self,
        worker_id: &str,
        limit: WorkerHistoryLimit,
    ) -> Result<Vec<WorkerStateSnapshot>, BrokerOperationError> {
        block_health(
            self.app.get_worker_state_history(worker_id, limit),
            BrokerOperationFailedCode::MonitoringQueryFailed,
            "Horsies::get_worker_state_history()",
            "app.get_worker_state_history(worker_id, limit)",
        )
    }

    /// `Horsies::get_class_coverage`, blocking.
    pub fn get_class_coverage(&self) -> Result<ClassCoverage, BrokerOperationError> {
        block_health(
            self.app.get_class_coverage(),
            BrokerOperationFailedCode::MonitoringQueryFailed,
            "Horsies::get_class_coverage()",
            "app.get_class_coverage()",
        )
    }

    /// `Horsies::get_class_report`, blocking.
    pub fn get_class_report(&self) -> Result<ClassReport, BrokerOperationError> {
        block_health(
            self.app.get_class_report(),
            BrokerOperationFailedCode::MonitoringQueryFailed,
            "Horsies::get_class_report()",
            "app.get_class_report()",
        )
    }

    /// `Horsies::check_live`, blocking. The static check runs anywhere; on
    /// a runtime thread the live phases do not run, and the report holds
    /// `HRS-217` (Python `CHECK_LIVE_ASYNC_CONTEXT`).
    pub fn check_live(&self) -> Result<CheckPassed, CheckReport> {
        let _static_passed = self.app.check()?;
        match place(format_args!("blocking Horsies::check_live()")) {
            Place::Runs(runtime) => runtime.block_on(self.app.check_live()),
            Place::AsyncContext => Err(live_check_in_async_context()),
            Place::RuntimeStartFailed(failed) => Err(connectivity_report(&format!(
                "the live check did not start: {failed}"
            ))),
        }
    }
}

/// The report of a blocking live check on a runtime thread (Python
/// `_check_broker_connectivity` in a running event loop). Pure.
fn live_check_in_async_context() -> CheckReport {
    CheckReport::of(
        CHECK,
        vec![HorsiesError::new(
            "live broker check cannot run on a thread that runs a tokio runtime",
        )
        .with_code(ErrorCode::CheckLiveAsyncContext)
        .with_note(
            "check_live was called on a thread that runs a tokio runtime; the connectivity \
             check did not run",
        )
        .with_help(
            "call app.blocking().check_live() from code without a runtime, or \
             app.check_live().await from async code",
        )],
    )
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{AppConfig, TaskError};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Invoice {
        id: u32,
    }

    #[allow(clippy::needless_pass_by_value)]
    fn bill(input: Invoice) -> Result<u32, TaskError<Infallible>> {
        Ok(input.id)
    }

    fn app() -> Horsies {
        let mut app = Horsies::new(AppConfig::for_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap();
        app.register("bill", crate::blocking_task_fn!(bill, Invoice))
            .unwrap();
        app
    }

    fn function(app: &Horsies) -> TaskFunction<Invoice, u32, Infallible> {
        TaskFunction::__registered(app, "bill").unwrap()
    }

    /// Python `TaskSendAsyncContext`: a blocking send on a runtime thread
    /// writes nothing and keeps the prepared task id and payload, which an
    /// async resend accepts.
    #[tokio::test]
    async fn a_blocking_send_on_a_runtime_thread_keeps_its_prepared_send() {
        let app = app();
        let function = function(&app);
        let refused = function.blocking().send(Invoice { id: 7 }).unwrap_err();
        let TaskSendError::AsyncContext {
            task_id, payload, ..
        } = &refused
        else {
            panic!("expected ASYNC_CONTEXT, got {refused}");
        };
        assert_eq!(payload.input_json, r#"{"id":7}"#);
        assert!(refused.message().contains("blocking send of task 'bill'"));
        let (resent_id, _payload) = function.checked_resend_target(&refused).unwrap();
        assert_eq!(resent_id, *task_id);
        let again = function.blocking().resend(&refused).unwrap_err();
        assert_eq!(again.task_id(), Some(*task_id));
        assert_eq!(again.code(), horsies_core::TaskSendErrorCode::AsyncContext);
    }

    /// The input checks run before the thread check: a suppressed send is
    /// `SEND_SUPPRESSED` on any thread, and runs nothing.
    #[test]
    fn a_suppressed_blocking_send_is_refused_before_any_wait() {
        let app = app();
        app.suppress_sends(true);
        let refused = function(&app)
            .blocking()
            .with_options(TaskSendOptions::new())
            .send(Invoice { id: 1 })
            .unwrap_err();
        assert_eq!(
            refused.code(),
            horsies_core::TaskSendErrorCode::SendSuppressed
        );
    }

    #[tokio::test]
    async fn a_blocking_read_on_a_runtime_thread_is_async_context_and_not_kept() {
        let app = app();
        let handle = app.task_handle_of::<u32, Infallible>(Uuid::from_u128(5));
        let refused = handle.blocking().get(WaitLimit::NoLimit).unwrap_err();
        assert!(matches!(refused, ReadError::AsyncContext { .. }));
        assert_eq!(refused.code().as_str(), "ASYNC_CONTEXT");
        assert!(handle.kept().is_none());
        let info = app
            .blocking()
            .get_task_info(Uuid::from_u128(5), TaskInfoParts::default())
            .unwrap_err();
        assert!(info
            .to_string()
            .contains("`app.get_task_info(task_id, parts).await`"));
    }

    /// The limit is the first check of a blocking read: on a runtime
    /// thread, where the thread check answers `AsyncContext`, a limit above
    /// the bound is `InvalidInput`. Nothing is kept.
    #[tokio::test]
    async fn a_blocking_read_above_the_bound_is_invalid_input_on_any_thread() {
        let app = app();
        let task_id = Uuid::from_u128(6);
        let above = WaitLimit::Within(Duration::from_millis(crate::MAX_WAIT_LIMIT_MS + 1));
        let handle = app.task_handle_of::<u32, Infallible>(task_id);
        let refused = handle.blocking().get(above).unwrap_err();
        assert_eq!(
            refused,
            ReadError::InvalidInput {
                task_id,
                cause: crate::WaitLimitRefused,
            },
        );
        assert!(handle.kept().is_none());
        let typed = app
            .blocking()
            .get_result::<u32, Infallible>(task_id, above)
            .unwrap_err();
        assert!(matches!(typed, ReadError::InvalidInput { .. }), "{typed}");
        let raw = app.blocking().get_raw_result(task_id, above).unwrap_err();
        assert!(matches!(raw, ReadError::InvalidInput { .. }), "{raw}");
    }

    #[tokio::test]
    async fn a_blocking_health_read_on_a_runtime_thread_has_the_async_context_code() {
        let refused = app().blocking().ping_database().unwrap_err();
        assert_eq!(refused.code(), BrokerErrorCode::AsyncContext);
        assert!(!refused.retryable());
    }

    #[tokio::test]
    async fn a_blocking_live_check_on_a_runtime_thread_runs_the_static_check_only() {
        let report = app().blocking().check_live().unwrap_err();
        assert_eq!(
            report.errors()[0].code,
            Some(ErrorCode::CheckLiveAsyncContext)
        );
    }

    /// A runtime start that the OS refused is a retryable `ENQUEUE_FAILED`
    /// with the prepared task id and payload: the next use tries again.
    #[test]
    fn a_send_whose_runtime_did_not_start_is_a_retryable_enqueue_failed() {
        let app = app();
        let (task_id, payload) = function(&app)
            .prepare(&TaskSendOptions::new(), &Invoice { id: 3 })
            .unwrap();
        let refused = start_failed_send(
            task_id,
            payload,
            &RuntimeStartFailed {
                cause: "EAGAIN".to_owned(),
                os_error: horsies_core::process_runtime::OsErrorCode::Code(11),
            },
        );
        let TaskSendError::EnqueueFailed {
            task_id: failed_id,
            payload,
            message,
            retryable,
            cause,
        } = &refused
        else {
            panic!("expected ENQUEUE_FAILED, got {refused}");
        };
        assert_eq!(*failed_id, task_id);
        assert_eq!(payload.input_json, r#"{"id":3}"#);
        assert!(
            message.contains("the send did not start: the core runtime could not start: EAGAIN"),
            "{message}"
        );
        assert!(*retryable);
        assert_eq!(*cause, FailureCause::PreconditionFailed);
    }

    #[test]
    fn the_message_names_the_call_and_the_async_form() {
        assert_eq!(
            async_context_message("TaskHandle::get()", "handle.get(limit)"),
            "blocking TaskHandle::get() was called on a thread that runs a tokio runtime; its \
             wait would stall the runtime, so nothing ran. Use `handle.get(limit).await` \
             instead."
        );
    }
}
