//! `TaskRuntime`: what a task body reads while it runs.
//!
//! A body that takes `TaskRuntime` as its first parameter gets the app's
//! state, the cancel signal of its run, and a way to start work that belongs
//! to its run. The app builds the runtime when the task registers, so every
//! worker child, which runs `main` again, has its own.

use std::future::Future;
use std::sync::Arc;

use horsies_core::{run_cancel_signal, spawn_in_run, RunSignalRefused, RunSpawnRefused};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::app::{AppState, StateNotProvided};
use crate::send::TaskFunctions;
use crate::workflow::WorkflowStarter;
use crate::{
    TaskFunction, TaskFunctionNotFound, UserErrorCode, WorkflowHandle, WorkflowSpec,
    WorkflowStartError,
};

/// What a task body reads while it runs.
///
/// ```ignore
/// #[horsies::task("sync_feed", execution = shared_async, timeout_ms = 30_000)]
/// async fn sync_feed(rt: horsies::TaskRuntime, feed: Feed) -> Result<u32, TaskError<FeedCode>> {
///     let client = rt
///         .state::<HttpClient>()
///         .map_err(|missing| TaskError::user(FeedCode::NotConfigured, missing.to_string()))?;
///     tokio::select! {
///         count = client.sync(&feed) => Ok(count),
///         () = rt.cancelled() => Err(TaskError::user(FeedCode::Interrupted, "cancelled")),
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
    /// again with the same id, after a retry of the task, finds the
    /// workflow and starts nothing.
    ///
    /// Errors: as [`Self::start`]; `WORKFLOW_ID_CONFLICT` for an id of
    /// another workflow name.
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
    /// `register` returned it. The generated `name::handle(&runtime)` calls
    /// it.
    ///
    /// Errors: [`TaskFunctionNotFound`] for a name that no task attribute
    /// registered on this app, or one registered with other types.
    pub fn task_handle<A, T, E>(
        &self,
        task_name: &str,
    ) -> Result<TaskFunction<A, T, E>, TaskFunctionNotFound>
    where
        A: Serialize + DeserializeOwned + 'static,
        T: Serialize + DeserializeOwned + 'static,
        E: UserErrorCode,
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

    /// Complete when the run is cancelled at its deadline.
    ///
    /// The cancel signal fires only for a `SHARED_ASYNC` run, at its
    /// `timeout_ms`. The run then ends as `TASK_TIMEOUT` whatever the body
    /// returns, and the body and the work it started with [`Self::spawn`]
    /// have the task's `cancel_grace_ms` to end before the child drops them.
    ///
    /// In an `ISOLATED` run the parent kills the child at the deadline, so
    /// the signal never fires. Outside the async body of a run (a blocking
    /// body, or a direct call of the function in a test) there is no run
    /// deadline: the future never completes.
    pub async fn cancelled(&self) {
        match run_cancel_signal() {
            Ok(signal) => signal.cancelled().await,
            Err(RunSignalRefused::NotInARun) => std::future::pending::<()>().await,
        }
    }

    /// Whether the run is cancelled at its deadline. See [`Self::cancelled`];
    /// `false` outside the async body of a run.
    pub fn is_cancelled(&self) -> bool {
        match run_cancel_signal() {
            Ok(signal) => signal.is_cancelled(),
            Err(RunSignalRefused::NotInARun) => false,
        }
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
    use crate::{AppConfig, Horsies};

    #[derive(Debug, PartialEq)]
    struct Settings(u32);

    fn app() -> Horsies {
        Horsies::new(AppConfig::for_database_url(
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
        let mut builder = crate::WorkflowSpecBuilder::new("keyless");
        let tick = builder.task(crate::workflow::tests_support::unit_task_node("tick"));
        builder.output(tick);
        let spec = builder.build().unwrap();
        let refused = runtime.start::<u32>(&spec).await.unwrap_err();
        assert_eq!(
            refused.code,
            crate::WorkflowStartErrorCode::ValidationFailed
        );
        let refused = runtime
            .start_with_id::<u32>(&spec, uuid::Uuid::nil())
            .await
            .unwrap_err();
        assert_eq!(refused.workflow_id, Some(uuid::Uuid::nil()));
    }
}
