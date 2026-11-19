//! The recovery point of a worker: a reaper pass of the worker changed a
//! task row.
//!
//! | Point | Event | Metrics |
//! |---|---|---|
//! | [`WorkerTelemetry::task_recovered`], [`WorkerTelemetry::tasks_recovered`] | `TaskRecovered` per row | `horsies.recovery.tasks{action}` |

use chrono::Utc;
use uuid::Uuid;

use super::{TelemetryOn, WorkerTelemetry};
use crate::broker::stale_recovery::RecoveredTask;
use crate::events::{self, TaskRecovered};
use crate::telemetry::labels::RecoveryAction;

impl WorkerTelemetry {
    /// A reaper pass changed task `task_id` (`task_name` on `queue_name`)
    /// with `action`: one `TaskRecovered` and one count of `action`.
    pub(crate) fn task_recovered(
        &self,
        task_id: Uuid,
        task_name: &str,
        queue_name: &str,
        action: RecoveryAction,
    ) {
        match self {
            Self::Off => {}
            Self::On(on) => on.task_recovered(task_id, task_name, queue_name, action),
        }
    }

    /// A reaper pass changed each row of `tasks` with `action`: one
    /// `TaskRecovered` and one count of `action` per row, in the order of
    /// `tasks`.
    pub(crate) fn tasks_recovered(&self, tasks: &[RecoveredTask], action: RecoveryAction) {
        match self {
            Self::Off => {}
            Self::On(on) => tasks.iter().for_each(|task| {
                on.task_recovered(task.task_id, &task.task_name, &task.queue_name, action);
            }),
        }
    }
}

impl TelemetryOn {
    fn task_recovered(
        &self,
        task_id: Uuid,
        task_name: &str,
        queue_name: &str,
        action: RecoveryAction,
    ) {
        let _counted = self.metrics.recovery_tasks.add((action,), 1);
        let _emitted = self.emit(|| TaskRecovered {
            at: Utc::now(),
            task_id,
            task_name: events::TaskName::cut(task_name),
            queue: events::QueueName::cut(queue_name),
            action,
        });
    }
}
