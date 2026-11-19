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
//! | [`WorkerTelemetry::attempt_placed`] | `AttemptPlaced` | `horsies.task.slot_wait.duration` |

use std::time::Duration;

use uuid::Uuid;

use super::{QueueLabel, TaskLabel, TelemetryOn, WorkerTelemetry};
use crate::broker::claims::{ClaimRead, ClaimedRow};
use crate::events::{self, AttemptPlaced, ClaimReleased, TaskClaimed};
use crate::telemetry::labels::{ClaimPassResult, ReleaseReason};
use crate::worker::executor::slots::dispatch::attempt_of;
use crate::worker::executor::slots::{
    watched_placement, ChildPlacement, ClaimedTask, Placed, PlacementReceipt, PlacementWatch,
};

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

    /// The watch of the placement of one task: watched when telemetry is
    /// on, so that [`WorkerTelemetry::attempt_placed`] gets the child.
    pub(crate) fn placement_watch(&self) -> (PlacementWatch, PlacementReceipt) {
        match self {
            Self::Off => (PlacementWatch::Unwatched, PlacementReceipt::Unwatched),
            Self::On(_) => watched_placement(),
        }
    }

    /// A child took `task`, whose claim result the worker read at `read`:
    /// one `AttemptPlaced` and one slot wait. A task that no child took
    /// records nothing here.
    pub(crate) fn attempt_placed(
        &self,
        task: &ClaimedTask,
        read: ClaimRead,
        placement: &ChildPlacement,
    ) {
        match (self, placement) {
            (Self::Off, _) | (_, ChildPlacement::NotWatched | ChildPlacement::NotPlaced) => {}
            (Self::On(on), ChildPlacement::Placed(placed)) => on.placed(task, read, placed),
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
    pub(super) fn queue_label(&self, queue_name: &str) -> QueueLabel {
        self.registry.labels().queues.value_of(queue_name)
    }

    /// The value of the `task` label of `task_name`.
    pub(super) fn task_label(&self, task_name: &str) -> TaskLabel {
        self.registry.labels().tasks.value_of(task_name)
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
        let _emitted = self.emit(|| TaskClaimed {
            at: row.read.wall,
            task_id: row.id,
            task_name: events::TaskName::cut(&row.task_name),
            queue: events::QueueName::cut(&row.queue_name),
            execution: row.execution,
            attempt: attempt_of(row.retry_count),
            priority: row.priority,
            queue_wait,
        });
    }

    fn placed(&self, task: &ClaimedTask, read: ClaimRead, placed: &Placed) {
        let slot_wait = placed.at.saturating_duration_since(read.at);
        let _recorded = self.metrics.slot_wait.observe((task.execution,), slot_wait);
        let _emitted = self.emit(|| AttemptPlaced {
            at: placed.wall,
            task_id: task.task_id,
            task_name: events::TaskName::cut(&task.task_name),
            queue: events::QueueName::cut(&task.queue_name),
            execution: task.execution,
            attempt: task.retry.attempt(),
            child_kind: placed.child_kind,
            child_pid: placed.child_pid,
            slot_wait,
        });
    }

    pub(super) fn released(
        &self,
        task_id: Uuid,
        task_name: &str,
        queue_name: &str,
        reason: ReleaseReason,
    ) {
        let _recorded = self.metrics.claims_released.add((reason,), 1);
        let _emitted = self.emit(|| ClaimReleased {
            at: chrono::Utc::now(),
            task_id,
            task_name: events::TaskName::cut(task_name),
            queue: events::QueueName::cut(queue_name),
            reason,
        });
    }
}
