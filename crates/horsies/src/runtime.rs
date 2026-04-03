use serde::de::DeserializeOwned;

use crate::workflow::WorkflowStarter;
use crate::workflow_engine::WorkflowHandle;
use crate::{WorkflowSpec, WorkflowStartResult};

/// Runtime capabilities made available to tasks by the horsies worker.
///
/// `TaskRuntime` is intentionally narrow for now: it exists to make
/// dynamic workflow starts inside running tasks ergonomic without pushing
/// users toward globals or re-attaching runtime state to `WorkflowSpec`.
#[derive(Clone)]
pub struct TaskRuntime {
    workflow_starter: WorkflowStarter,
}

impl TaskRuntime {
    pub(crate) fn new(workflow_starter: WorkflowStarter) -> Self {
        Self { workflow_starter }
    }

    /// Start a dynamically-built workflow spec from inside a running task.
    pub async fn start<T: DeserializeOwned + Clone>(
        &self,
        spec: WorkflowSpec,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        self.workflow_starter.start(spec).await
    }

    /// Start a dynamically-built workflow with an explicit idempotent ID.
    pub async fn start_with_id<T: DeserializeOwned + Clone>(
        &self,
        spec: WorkflowSpec,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        self.workflow_starter.start_with_id(spec, workflow_id).await
    }

    /// Access the underlying starter for advanced workflow-start operations.
    pub fn workflow_starter(&self) -> &WorkflowStarter {
        &self.workflow_starter
    }
}

impl From<TaskRuntime> for WorkflowStarter {
    fn from(value: TaskRuntime) -> Self {
        value.workflow_starter
    }
}

impl std::fmt::Debug for TaskRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRuntime").finish()
    }
}
