use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Workflow summary statistics for dashboard
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkflowSummary {
    pub pending: Option<i64>,
    pub running: Option<i64>,
    pub completed: Option<i64>,
    pub failed: Option<i64>,
    pub paused: Option<i64>,
    pub cancelled: Option<i64>,
}

impl WorkflowSummary {
    /// Get total number of workflows
    pub fn total(&self) -> i64 {
        self.pending.unwrap_or(0)
            + self.running.unwrap_or(0)
            + self.completed.unwrap_or(0)
            + self.failed.unwrap_or(0)
            + self.paused.unwrap_or(0)
            + self.cancelled.unwrap_or(0)
    }

    /// Get number of active workflows (pending + running + paused)
    pub fn active(&self) -> i64 {
        self.pending.unwrap_or(0) + self.running.unwrap_or(0) + self.paused.unwrap_or(0)
    }
}

/// Workflow row for list view with task progress
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkflowRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub on_error: String,
    pub output_task_index: Option<i32>,
    pub success_policy: Option<JsonValue>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub total_tasks: Option<i64>,
    pub completed_tasks: Option<i64>,
    pub failed_tasks: Option<i64>,
    pub running_tasks: Option<i64>,
    pub pending_tasks: Option<i64>,
}

impl WorkflowRow {
    /// Get terminal (done) tasks count: completed + failed + skipped
    fn terminal_tasks(&self) -> i64 {
        // terminal = total - pending - running
        let total = self.total_tasks.unwrap_or(0);
        let pending = self.pending_tasks.unwrap_or(0);
        let running = self.running_tasks.unwrap_or(0);
        total - pending - running
    }

    /// Get execution progress as a string like "97/97" (terminal / total)
    pub fn progress_str(&self) -> String {
        let terminal = self.terminal_tasks();
        let total = self.total_tasks.unwrap_or(0);
        format!("{}/{}", terminal, total)
    }

    /// Get execution progress percentage (0.0 to 1.0) - how many tasks reached terminal state
    pub fn progress_pct(&self) -> f64 {
        let total = self.total_tasks.unwrap_or(0);
        if total == 0 {
            return 0.0;
        }
        let terminal = self.terminal_tasks();
        terminal as f64 / total as f64
    }

    /// Get success rate as a string like "29/97"
    pub fn success_str(&self) -> String {
        let completed = self.completed_tasks.unwrap_or(0);
        let total = self.total_tasks.unwrap_or(0);
        format!("{}/{}", completed, total)
    }

    /// Get success rate percentage (0.0 to 1.0) - how many tasks completed successfully
    pub fn success_pct(&self) -> f64 {
        let total = self.total_tasks.unwrap_or(0);
        if total == 0 {
            return 0.0;
        }
        let completed = self.completed_tasks.unwrap_or(0);
        completed as f64 / total as f64
    }

    /// Calculate duration if available
    pub fn duration_str(&self) -> String {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => {
                let duration = end - start;
                let secs = duration.num_seconds();
                if secs < 60 {
                    format!("{}s", secs)
                } else if secs < 3600 {
                    format!("{}m {}s", secs / 60, secs % 60)
                } else {
                    format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
                }
            }
            (Some(start), None) => {
                // Still running
                let duration = Utc::now() - start;
                let secs = duration.num_seconds();
                if secs < 60 {
                    format!("{}s", secs)
                } else if secs < 3600 {
                    format!("{}m {}s", secs / 60, secs % 60)
                } else {
                    format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
                }
            }
            _ => "-".to_string(),
        }
    }

    /// Truncate ID for display (first 8 chars)
    pub fn short_id(&self) -> &str {
        if self.id.len() > 8 {
            &self.id[..8]
        } else {
            &self.id
        }
    }

    /// Convert workflow to JSON string for clipboard
    pub fn to_clipboard_json(&self, tasks: &[WorkflowTaskRow]) -> String {
        #[derive(Serialize)]
        struct ClipboardWorkflow<'a> {
            workflow: &'a WorkflowRow,
            tasks: &'a [WorkflowTaskRow],
        }

        let data = ClipboardWorkflow {
            workflow: self,
            tasks,
        };

        serde_json::to_string_pretty(&data).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Workflow task row for detail view
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkflowTaskRow {
    pub task_index: i32,
    pub node_id: Option<String>,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub status: String,
    pub dependencies: Option<Vec<i32>>,
    pub args_from: Option<JsonValue>,
    pub workflow_ctx_from: Option<Vec<String>>,
    pub allow_failed_deps: bool,
    pub join_type: String,
    pub min_success: Option<i32>,
    pub task_options: Option<String>,
    pub task_id: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<String>,
    pub error: Option<String>,
}

impl WorkflowTaskRow {
    /// Get short status label
    pub fn status_label(&self) -> &str {
        match self.status.as_str() {
            "PENDING" => "PND",
            "READY" => "RDY",
            "ENQUEUED" => "ENQ",
            "RUNNING" => "RUN",
            "COMPLETED" => "OK",
            "FAILED" => "ERR",
            "SKIPPED" => "SKP",
            _ => &self.status,
        }
    }

    /// Get dependencies as string
    pub fn deps_str(&self) -> String {
        match &self.dependencies {
            Some(deps) if !deps.is_empty() => {
                deps.iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            }
            _ => "-".to_string(),
        }
    }

    /// Get workflow context sources as string (node_ids)
    pub fn ctx_from_str(&self) -> String {
        match &self.workflow_ctx_from {
            Some(ids) if !ids.is_empty() => ids.join(","),
            _ => "-".to_string(),
        }
    }

    /// Get join type display string
    pub fn join_str(&self) -> String {
        match self.join_type.as_str() {
            "all" => "ALL".to_string(),
            "any" => "ANY".to_string(),
            "quorum" => {
                if let Some(min) = self.min_success {
                    format!("Q({})", min)
                } else {
                    "Q(?)".to_string()
                }
            }
            _ => self.join_type.clone(),
        }
    }
}

/// Workflow status aggregation row
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkflowStatusAggRow {
    pub status: String,
    pub count: Option<i64>,
    pub avg_duration_secs: Option<f64>,
}
