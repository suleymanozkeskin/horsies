//! The lifecycle points of a worker parent. Each point is one method of
//! [`WorkerTelemetry`]: with [`WorkerTelemetry::Off`] it is one branch; with
//! [`WorkerTelemetry::On`] it records the metrics of its step and emits the
//! event of its step ([`TelemetryOn::emit`]).
//!
//! | Point | Event | Metrics |
//! |---|---|---|
//! | [`WorkerTelemetry::claimed`] | `TaskClaimed` per row | `horsies.task.queue.duration` |
//! | [`WorkerTelemetry::released`] | `ClaimReleased` | `horsies.task.claims.released` |
//! | [`WorkerTelemetry::claim_pass`] | none | `horsies.worker.claim.passes` |

use std::time::Duration;

use uuid::Uuid;

use super::{TelemetryOn, WorkerTelemetry};
use crate::broker::claims::ClaimedRow;
use crate::events::{self, ClaimReleased, Event, TaskClaimed};
use crate::telemetry::declared::{self, DeclaredValue};
use crate::telemetry::labels::{ClaimPassResult, ReleaseReason};
use crate::worker::executor::slots::dispatch::attempt_of;

/// The value of the `queue` label.
type QueueLabel = DeclaredValue<declared::QueueName>;

impl WorkerTelemetry {
    /// The claim statement returned `rows`, read at `rows[..].read`: one
    /// `TaskClaimed` and one queue wait per row.
    pub(crate) fn claimed(&self, rows: &[ClaimedRow]) {
        match self {
            Self::Off => {}
            Self::On(on) => rows.iter().for_each(|row| on.claimed(row)),
        }
    }

    /// The worker released the claim of task `task_id` before the task
    /// reached a child: one `ClaimReleased`.
    pub(crate) fn released(
        &self,
        task_id: Uuid,
        task_name: &str,
        queue_name: &str,
        reason: ReleaseReason,
    ) {
        match self {
            Self::Off => {}
            Self::On(on) => on.released(task_id, task_name, queue_name, reason),
        }
    }

    /// One pass of the claim loop ended with `result`.
    pub(crate) fn claim_pass(&self, result: ClaimPassResult) {
        match self {
            Self::Off => {}
            Self::On(on) => {
                let _recorded = on.metrics.claim_passes.add((result,), 1);
            }
        }
    }
}

impl TelemetryOn {
    /// The value of the `queue` label of `queue_name`.
    fn queue_label(&self, queue_name: &str) -> QueueLabel {
        self.registry.labels().queues.value_of(queue_name)
    }

    fn claimed(&self, row: &ClaimedRow) {
        // The claim takes due rows only, so `enqueued_at <= claimed_at` on
        // the one database clock; a due time after the claim counts as no
        // wait.
        let queue_wait = (row.claimed_at - row.enqueued_at)
            .to_std()
            .unwrap_or(Duration::ZERO);
        let _recorded = self.metrics.queue_duration.observe(
            (self.queue_label(&row.queue_name), row.execution),
            queue_wait,
        );
        let _emitted = self.emit(|| {
            Event::TaskClaimed(TaskClaimed {
                at: row.read.wall,
                task_id: row.id,
                task_name: events::TaskName::cut(&row.task_name),
                queue: events::QueueName::cut(&row.queue_name),
                execution: row.execution,
                attempt: attempt_of(row.retry_count),
                priority: row.priority,
                queue_wait,
            })
        });
    }

    fn released(&self, task_id: Uuid, task_name: &str, queue_name: &str, reason: ReleaseReason) {
        let _recorded = self.metrics.claims_released.add((reason,), 1);
        let _emitted = self.emit(|| {
            Event::ClaimReleased(ClaimReleased {
                at: chrono::Utc::now(),
                task_id,
                task_name: events::TaskName::cut(task_name),
                queue: events::QueueName::cut(queue_name),
                reason,
            })
        });
    }
}
