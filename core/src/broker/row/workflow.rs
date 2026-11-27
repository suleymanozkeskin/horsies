use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Row from the `horsies_workflows` table.
#[derive(Debug, Clone)]
pub struct WorkflowRow {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub on_error: String,
    pub output_task_index: Option<i32>,
    pub success_policy: Option<serde_json::Value>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub definition_key: Option<String>,
    pub parent_workflow_id: Option<Uuid>,
    pub parent_task_index: Option<i32>,
    pub depth: i32,
    pub root_workflow_id: Option<Uuid>,
    pub sent_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// Row from the `horsies_workflow_tasks` table.
#[derive(Debug, Clone)]
pub struct WorkflowTaskRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    /// The node's input as one JSON value, as the client encoded it.
    pub input: String,
    pub queue_name: String,
    pub priority: i32,
    pub dependencies: Vec<i32>,
    pub args_from: Option<serde_json::Value>,
    // The columns are `workflow_ctx_from` and `allow_failed_deps`. The stored
    // names differ from the public names on purpose: a rename of a column
    // needs a migration, and the row keeps the stored form.
    pub workflow_context_from: Option<Vec<String>>,
    pub allow_failed_dependencies: bool,
    pub join_type: String,
    pub min_success: Option<i32>,
    pub task_options: Option<String>,
    /// Typed facts of a task node, parsed at workflow start; `None` on a
    /// sub-workflow node.
    pub max_retries: Option<i32>,
    pub execution: Option<String>,
    pub good_until: Option<DateTime<Utc>>,
    pub retention_class_key: Option<String>,
    pub status: String,
    pub task_id: Option<Uuid>,
    pub is_subworkflow: bool,
    pub sub_workflow_id: Option<Uuid>,
    pub sub_workflow_name: Option<String>,
    pub sub_definition_key: Option<String>,
    pub sub_workflow_summary: Option<String>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}
