//! `TaskRuntime`: what a task body reads while it runs.
//!
//! A body that takes `TaskRuntime` as its first parameter gets the app's
//! state, the typed functions and the workflow builders of the app, the
//! cancel signal and the cancel state of its run, the fencing token and the
//! attempt facts of its run (task id, attempt number, retry budget), and a
//! way to start work that belongs to its run. The app builds the
//! runtime when the task registers, so every worker child, which runs
//! `main` again, has its own.

use std::future::Future;
use std::sync::Arc;

use horsies_core::events::{AttemptNumber, MaxRetries, RetriesLeft};
use horsies_core::{
    run_cancel_signal, run_cancel_state, run_fencing_token, run_task_attempt, spawn_in_run,
    with_test_blocking_run_scope, with_test_run_scope, FencingToken, FencingTokenRefused,
    RunCancelState, RunSignalRefused, RunSpawnRefused, TaskAttemptRefused, TestRunCancel,
    TestRunFacts,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::app::{AppState, StateNotProvided};
use crate::send::TaskFunctions;
use crate::workflow::WorkflowStarter;
use crate::{
    CustomErrorCode, TaskFunction, TaskFunctionNotFound, WorkflowHandle, WorkflowSpec,
    WorkflowSpecBuilder, WorkflowStartError,
};

/// What a task body reads while it runs.
///
/// ```ignore
/// #[horsies::task("sync_feed", execution = shared_async, timeout_ms = 30_000)]
/// async fn sync_feed(rt: horsies::TaskRuntime, feed: Feed) -> Result<u32, TaskError<FeedCode>> {
///     let client = rt
///         .state::<HttpClient>()
///         .map_err(|missing| TaskError::custom(FeedCode::NotConfigured, missing.to_string()))?;
///     tokio::select! {
///         count = client.sync(&feed) => Ok(count),
///         () = rt.cancelled() => Err(TaskError::custom(FeedCode::Interrupted, "cancelled")),
///     }
/// }
/// ```
#[derive(Clone)]
pub struct TaskRuntime {
    state: Arc<AppState>,
    functions: Arc<TaskFunctions>,
    workflows: WorkflowStarter,
}

impl TaskRuntime {
    pub(crate) fn new(
        state: Arc<AppState>,
        functions: Arc<TaskFunctions>,
        workflows: WorkflowStarter,
    ) -> Self {
        Self {
            state,
            functions,
            workflows,
        }
    }

    /// A workflow builder of the workflow `name` on the app of this runtime,
    /// as `Horsies::workflow_builder` gives it: each task node resolves
    /// against the tasks of that app when it is added.
    pub fn workflow_builder(&self, name: impl Into<String>) -> WorkflowSpecBuilder {
        WorkflowSpecBuilder::new(name, Arc::clone(&self.functions))
    }

    /// Start `spec`, a workflow that the task built, with a new id and the
    /// result type `T`: the start of `Horsies::start`, through the child's
    /// producer handle in a worker child. The task does not wait for the
    /// workflow: a wait in a task body is refused (`WAIT_INSIDE_TASK`).
    ///
    /// Errors: the start errors of `WorkflowStartErrorCode`.
    pub async fn start<T>(
        &self,
        spec: &WorkflowSpec,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError>
    where
        T: DeserializeOwned + 'static,
    {
        self.workflows
            .start(spec, crate::workflow::new_workflow_id())
            .await
    }

    /// Start `spec` with the id `workflow_id`, as [`Self::start`]. A start
    /// again with the same id and the same start command, after a retry of
    /// the task, finds the workflow and starts nothing.
    ///
    /// Errors: as [`Self::start`]; `WORKFLOW_ID_CONFLICT` for an id of
    /// another workflow name; `WORKFLOW_COMMAND_CONFLICT` for an id of a run
    /// of this name with another start command, or of a sub-workflow.
    pub async fn start_with_id<T>(
        &self,
        spec: &WorkflowSpec,
        workflow_id: uuid::Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError>
    where
        T: DeserializeOwned + 'static,
    {
        self.workflows.start(spec, workflow_id).await
    }

    /// The typed function of task `task_name` in this app, as its
    /// `register` returned it. The generated `name::function(&runtime)` calls
    /// it.
    ///
    /// Errors: [`TaskFunctionNotFound`] for a name that no task attribute
    /// registered on this app, or one registered with other types.
    pub fn task_function<A, T, E>(
        &self,
        task_name: &str,
    ) -> Result<TaskFunction<A, T, E>, TaskFunctionNotFound>
    where
        A: Serialize + DeserializeOwned + 'static,
        T: Serialize + DeserializeOwned + 'static,
        E: CustomErrorCode,
    {
        self.functions.get::<A, T, E>(task_name)
    }

    /// The value of type `T` that the app provides
    /// (`Horsies::provide`). The first read freezes the app's state.
    ///
    /// Errors: [`StateNotProvided`] when the app provides no `T`.
    pub fn state<T>(&self) -> Result<Arc<T>, StateNotProvided>
    where
        T: Send + Sync + 'static,
    {
        self.state.get::<T>()
    }

    /// Complete when a cancel of the run is requested.
    ///
    /// The cancel signal of a run fires once, with the first cause
    /// ([`Self::cancel_state`]):
    ///
    /// - An operator cancel, in every execution mode: `cancel_task` with
    ///   `CancelPolicy::IncludeRunning` on a `RUNNING` task. The task is `CANCELLED`
    ///   already, with its attempt recorded as `CANCELLED`; what the body
    ///   returns is dropped. In a `SHARED_ASYNC` run the body and the work it
    ///   started with [`Self::spawn`] have the task's `cancel_grace_ms` to
    ///   end before the child drops them. In an `ISOLATED` run with a
    ///   `cancel_grace_ms`, the worker kills the child that much later when
    ///   the run has not ended; with none, nothing is killed.
    /// - The deadline of a `SHARED_ASYNC` run, at its `timeout_ms`. The run
    ///   then ends as `TASK_TIMEOUT` whatever the body returns, with the same
    ///   grace. The deadline of a `SHARED_THREAD` run fires it too; its
    ///   thread runs on.
    ///
    /// In an `ISOLATED` run the parent kills the child at the deadline, so
    /// the deadline never fires the signal there. A blocking body cannot
    /// await this future: it reads [`Self::is_cancelled`] or
    /// [`Self::cancel_state`]. Outside a run (a direct call of the function
    /// in a test, plain `tokio::spawn`) the future never completes.
    pub async fn cancelled(&self) {
        match run_cancel_signal() {
            Ok(signal) => signal.cancelled().await,
            Err(RunSignalRefused::NotInARun) => std::future::pending::<()>().await,
        }
    }

    /// Whether a cancel of the run is requested, for any cause. See
    /// [`Self::cancelled`]. It works in the async body, in work started with
    /// [`Self::spawn`], and on the thread of a blocking body; `false` outside
    /// a run, which [`Self::cancel_state`] tells apart.
    pub fn is_cancelled(&self) -> bool {
        match run_cancel_signal() {
            Ok(signal) => signal.is_cancelled(),
            Err(RunSignalRefused::NotInARun) => false,
        }
    }

    /// The cancel state of the run: `NotRequested`, `Requested` with the
    /// first cause (`Operator` or `Deadline`), or `NotInARun` outside a run
    /// (a direct call of the function in a test, work started with plain
    /// `tokio::spawn` or `spawn_blocking`, or a thread that a blocking body
    /// starts). It works in the async body, in work started with
    /// [`Self::spawn`], and on the thread of a blocking body.
    ///
    /// ```ignore
    /// #[horsies::blocking_task("import_rows", timeout_ms = 600_000)]
    /// fn import_rows(rt: TaskRuntime, file: RowFile) -> Result<u32, TaskError<ImportCode>> {
    ///     let mut imported = 0;
    ///     for row in file.rows() {
    ///         match rt.cancel_state() {
    ///             RunCancelState::Requested { .. } => {
    ///                 return Err(TaskError::custom(ImportCode::Stopped, "cancelled"))
    ///             }
    ///             RunCancelState::NotRequested | RunCancelState::NotInARun => {}
    ///         }
    ///         store(row)?;
    ///         imported += 1;
    ///     }
    ///     Ok(imported)
    /// }
    /// ```
    pub fn cancel_state(&self) -> RunCancelState {
        run_cancel_state()
    }

    /// The fencing token of the current run: the task id and a value. Write
    /// it with each write to another system, and make the system refuse a
    /// write whose token is lower than the one it holds for the task id.
    ///
    /// - Two reads in one attempt give the same token.
    /// - An attempt that runs after another attempt of the same task gives a
    ///   greater value.
    /// - The value depends on no clock.
    /// - Today the value is the attempt number, from 1
    ///   (`token.value().get()`). Compare the values of one task id; do not
    ///   depend on the number itself.
    ///
    /// A rerun, and a workflow node that a resume starts again, get a new
    /// task id, and their value starts again at 1. A stale attempt with no
    /// later attempt (the task failed, was cancelled or expired) is not
    /// refused.
    ///
    /// ```ignore
    /// #[horsies::task("charge", timeout_ms = 30_000)]
    /// async fn charge(rt: TaskRuntime, order: Order) -> Result<(), TaskError<ChargeCode>> {
    ///     let token = rt
    ///         .fencing_token()
    ///         .map_err(|refused| TaskError::custom(ChargeCode::NoRun, refused.to_string()))?;
    ///     // UPDATE payments SET state = 'charged', fence = $2
    ///     //  WHERE task_id = $1 AND fence <= $2
    ///     store.charge(token.task_id(), i64::from(token.value().get()), &order).await
    /// }
    /// ```
    ///
    /// Errors: [`FencingTokenRefused::NotInARun`] outside a run: a direct
    /// call of the function in a test, work started with plain
    /// `tokio::spawn` or `spawn_blocking`, or a thread that a blocking body
    /// starts. Work started with [`Self::spawn`] reads the token of its run.
    pub fn fencing_token(&self) -> Result<FencingToken, FencingTokenRefused> {
        run_fencing_token()
    }

    /// The id of the task of the current run. It is the same on every
    /// attempt, and the same as `TaskHandle::task_id`: a stable key for an
    /// idempotent write to another system.
    ///
    /// Errors: [`TaskAttemptRefused::NotInARun`] outside a run, as
    /// [`Self::fencing_token`].
    pub fn task_id(&self) -> Result<uuid::Uuid, TaskAttemptRefused> {
        run_task_attempt().map(|facts| facts.task_id())
    }

    /// The attempt number of the current run: 1 is the first run, 2 the
    /// first retry. The same numbering as the attempt rows of the task.
    ///
    /// Errors: [`TaskAttemptRefused::NotInARun`] outside a run.
    pub fn attempt(&self) -> Result<AttemptNumber, TaskAttemptRefused> {
        run_task_attempt().map(|facts| facts.attempt())
    }

    /// The retry budget of the task: the retries that follow its first
    /// attempt, from 0.
    ///
    /// Errors: [`TaskAttemptRefused::NotInARun`] outside a run.
    pub fn max_retries(&self) -> Result<MaxRetries, TaskAttemptRefused> {
        run_task_attempt().map(|facts| facts.max_retries())
    }

    /// The retries left after the current run: `max_retries - (attempt - 1)`,
    /// and 0 when the attempt is past the budget. A non-retryable error code
    /// or `good_until` can end the retries before it reaches 0, so the last
    /// attempt is not known in advance.
    ///
    /// ```ignore
    /// #[horsies::task("charge", timeout_ms = 30_000)]
    /// async fn charge(rt: TaskRuntime, order: Order) -> Result<(), TaskError<ChargeCode>> {
    ///     let key = rt
    ///         .task_id()
    ///         .map_err(|refused| TaskError::custom(ChargeCode::NoRun, refused.to_string()))?;
    ///     payments.charge(&order, &format!("charge-{key}")).await
    /// }
    /// ```
    ///
    /// Errors: [`TaskAttemptRefused::NotInARun`] outside a run.
    pub fn retries_left(&self) -> Result<RetriesLeft, TaskAttemptRefused> {
        run_task_attempt().map(|facts| facts.retries_left())
    }

    /// Start `future` as work of the current run, named `name` in the
    /// reports of the run. The child waits for such work when the body
    /// returns, cancels it after the grace, and reports work that does not
    /// stop. Work started with plain `tokio::spawn` is not tracked.
    ///
    /// Errors: [`RunSpawnRefused`]: outside the async body of a run in a
    /// worker child, after the run ended, or past `MAX_RUN_WORK` pieces of
    /// work alive. Nothing is started then.
    pub fn spawn<F>(&self, name: &str, future: F) -> Result<JoinHandle<F::Output>, RunSpawnRefused>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_in_run(name, future)
    }
}

/// The facts of a run that a unit test of a task body installs: the task id,
/// the attempt number, the retry budget and the cancel state. While the test
/// runs the body through [`TestRun::run`] or [`TestRun::run_blocking`], the
/// reads of [`TaskRuntime`] give these facts, as in a worker child; outside
/// that call they give what they give outside a run.
///
/// ```ignore
/// let facts = TestRun::new(task_id, AttemptNumber::try_from(2)?, MaxRetries::try_from(3)?);
/// let outcome = facts.run(charge(app.task_runtime(), order)).await;
/// ```
///
/// No database, worker or child takes part. Work that the body starts with
/// [`TaskRuntime::spawn`] is aborted when the future ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestRun {
    facts: TestRunFacts,
}

impl TestRun {
    /// The facts of attempt `attempt` of task `task_id`, whose retry budget
    /// is `max_retries`. No cancel is requested.
    pub fn new(task_id: uuid::Uuid, attempt: AttemptNumber, max_retries: MaxRetries) -> Self {
        Self {
            facts: TestRunFacts::new(task_id, attempt, max_retries),
        }
    }

    /// These facts with the cancel state `cancel`: `NotRequested`, or
    /// `Requested` with a cause. A requested cancel makes `cancelled()`
    /// complete at once, `is_cancelled()` `true` and `cancel_state()`
    /// `RunCancelState::Requested`.
    pub fn with_cancel_state(self, cancel: TestRunCancel) -> Self {
        Self {
            facts: self.facts.with_cancel(cancel),
        }
    }

    /// Drive `future`, the async body under test, as the body of a run with
    /// these facts, and give its output. The facts are gone when it ends.
    pub async fn run<F: Future>(self, future: F) -> F::Output {
        with_test_run_scope(self.facts, future).await
    }

    /// Call `body`, the blocking body under test, on this thread as the body
    /// of a run with these facts, and give its output. The facts are gone when
    /// it ends.
    pub fn run_blocking<R>(self, body: impl FnOnce() -> R) -> R {
        with_test_blocking_run_scope(self.facts, body)
    }
}

impl std::fmt::Debug for TaskRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskRuntime")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppConfig, CancelCause, Horsies};

    #[derive(Debug, PartialEq)]
    struct Settings(u32);

    fn app() -> Horsies {
        Horsies::new(AppConfig::from_database_url(
            "postgresql://localhost/horsies",
        ))
        .unwrap()
    }

    #[test]
    fn the_runtime_reads_the_state_that_the_app_provides() {
        let mut app = app();
        let runtime = app.task_runtime();
        app.provide(Settings(4)).unwrap();
        assert_eq!(*runtime.state::<Settings>().unwrap(), Settings(4));
    }

    #[test]
    fn a_missing_state_names_its_type() {
        let runtime = app().task_runtime();
        let missing = runtime.state::<Settings>().unwrap_err();
        assert!(missing.type_name.contains("Settings"));
    }

    #[tokio::test]
    async fn outside_a_run_the_signal_never_fires() {
        let runtime = app().task_runtime();
        assert!(!runtime.is_cancelled());
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), runtime.cancelled()).await;
        assert!(waited.is_err(), "cancelled() completed outside a run");
    }

    #[test]
    fn outside_a_run_the_cancel_state_is_not_in_a_run() {
        let runtime = app().task_runtime();
        assert_eq!(runtime.cancel_state(), RunCancelState::NotInARun);
    }

    #[tokio::test]
    async fn outside_a_run_the_fencing_token_is_refused() {
        let runtime = app().task_runtime();
        assert_eq!(runtime.fencing_token(), Err(FencingTokenRefused::NotInARun));
    }

    #[tokio::test]
    async fn outside_a_run_the_attempt_facts_are_refused() {
        let runtime = app().task_runtime();
        assert_eq!(runtime.task_id(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.attempt(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.max_retries(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.retries_left(), Err(TaskAttemptRefused::NotInARun));
    }

    fn second_attempt_of_three() -> (uuid::Uuid, TestRun) {
        let task_id = uuid::Uuid::from_u128(7);
        let facts = TestRun::new(
            task_id,
            AttemptNumber::try_from(2).unwrap(),
            MaxRetries::try_from(3).unwrap(),
        );
        (task_id, facts)
    }

    #[tokio::test]
    async fn a_test_run_gives_the_task_id() {
        let runtime = app().task_runtime();
        let (task_id, facts) = second_attempt_of_three();
        let read = facts.run(async { runtime.task_id() }).await;
        assert_eq!(read, Ok(task_id));
    }

    #[tokio::test]
    async fn a_test_run_gives_the_attempt_number() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        let read = facts.run(async { runtime.attempt() }).await;
        assert_eq!(read, Ok(AttemptNumber::try_from(2).unwrap()));
    }

    #[tokio::test]
    async fn a_test_run_gives_the_retry_budget() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        let read = facts.run(async { runtime.max_retries() }).await;
        assert_eq!(read, Ok(MaxRetries::try_from(3).unwrap()));
    }

    #[tokio::test]
    async fn a_test_run_gives_the_retries_left() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        let read = facts.run(async { runtime.retries_left() }).await;
        assert_eq!(read, Ok(RetriesLeft::try_from(2).unwrap()));
    }

    #[tokio::test]
    async fn a_test_run_gives_the_fencing_token_of_its_facts() {
        let runtime = app().task_runtime();
        let (task_id, facts) = second_attempt_of_three();
        let token = facts.run(async { runtime.fencing_token() }).await.unwrap();
        assert_eq!(token.task_id(), task_id);
        assert_eq!(token.value().get(), 2);
    }

    #[tokio::test]
    async fn a_test_run_has_no_cancel_requested_by_default() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        let (state, cancelled) = facts
            .run(async { (runtime.cancel_state(), runtime.is_cancelled()) })
            .await;
        assert_eq!(state, RunCancelState::NotRequested);
        assert!(!cancelled);
    }

    #[tokio::test]
    async fn a_test_run_gives_the_requested_cancel_state() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        let facts = facts.with_cancel_state(TestRunCancel::Requested {
            cause: CancelCause::Operator,
        });
        let (state, cancelled) = facts
            .run(async {
                runtime.cancelled().await;
                (runtime.cancel_state(), runtime.is_cancelled())
            })
            .await;
        assert_eq!(
            state,
            RunCancelState::Requested {
                cause: CancelCause::Operator
            }
        );
        assert!(cancelled);
    }

    #[test]
    fn a_blocking_test_run_gives_the_attempt_facts_and_the_cancel_state() {
        let runtime = app().task_runtime();
        let (task_id, facts) = second_attempt_of_three();
        let facts = facts.with_cancel_state(TestRunCancel::Requested {
            cause: CancelCause::Deadline,
        });
        let read = facts.run_blocking(|| {
            (
                runtime.task_id(),
                runtime.attempt(),
                runtime.max_retries(),
                runtime.retries_left(),
                runtime.cancel_state(),
            )
        });
        assert_eq!(
            read,
            (
                Ok(task_id),
                Ok(AttemptNumber::try_from(2).unwrap()),
                Ok(MaxRetries::try_from(3).unwrap()),
                Ok(RetriesLeft::try_from(2).unwrap()),
                RunCancelState::Requested {
                    cause: CancelCause::Deadline
                },
            )
        );
    }

    #[tokio::test]
    async fn the_facts_of_a_test_run_are_gone_when_its_future_ends() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        let facts = facts.with_cancel_state(TestRunCancel::Requested {
            cause: CancelCause::Operator,
        });
        facts.run(async {}).await;
        assert_eq!(runtime.task_id(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.attempt(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.max_retries(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.retries_left(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.cancel_state(), RunCancelState::NotInARun);
    }

    #[test]
    fn the_facts_of_a_blocking_test_run_are_gone_when_its_body_ends() {
        let runtime = app().task_runtime();
        let (_, facts) = second_attempt_of_three();
        facts.run_blocking(|| {});
        assert_eq!(runtime.task_id(), Err(TaskAttemptRefused::NotInARun));
        assert_eq!(runtime.cancel_state(), RunCancelState::NotInARun);
    }

    #[tokio::test]
    async fn spawn_outside_a_run_is_refused() {
        let runtime = app().task_runtime();
        let refused = runtime.spawn("work", async {}).unwrap_err();
        assert_eq!(refused, RunSpawnRefused::NotInARun);
    }

    /// A start from a task body takes the start path of the app: a spec
    /// without a definition key is refused before any I/O.
    #[tokio::test]
    async fn a_start_from_the_runtime_checks_the_spec_first() {
        let runtime = app().task_runtime();
        let mut builder = crate::workflow::tests_support::test_builder("keyless");
        let tick = crate::workflow::tests_support::add(
            &mut builder,
            crate::workflow::tests_support::unit_task_node("tick"),
        );
        builder.output(tick);
        let spec = builder.build().unwrap();
        let refused = runtime.start::<u32>(&spec).await.unwrap_err();
        assert_eq!(
            refused.code(),
            crate::WorkflowStartErrorCode::ValidationFailed
        );
        let refused = runtime
            .start_with_id::<u32>(&spec, uuid::Uuid::nil())
            .await
            .unwrap_err();
        assert_eq!(refused.workflow_id(), uuid::Uuid::nil());
    }
}
