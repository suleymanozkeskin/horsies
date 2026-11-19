//! The points of the child slots of a worker: a child sent its ready frame,
//! a slot went down after a boot failure that can clear, and a down slot
//! serves again.
//!
//! | Point | Event | Metrics |
//! |---|---|---|
//! | [`WorkerTelemetry::child_started`] | `ChildStarted` | `horsies.worker.children.started`, `horsies.worker.child.boot.duration` |
//! | [`WorkerTelemetry::slot_down`] | `SlotDown` | none (the slot board gives `horsies.worker.slots.down` at a scrape) |
//! | [`WorkerTelemetry::slot_recovered`] | `SlotRecovered` | none |

use std::time::Duration;

use chrono::Utc;

use super::{TelemetryOn, WorkerTelemetry};
use crate::core::task::Execution;
use crate::events::{ChildStarted, Event, SlotDown, SlotDownCause, SlotIndex, SlotRecovered};
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::supervisor::BootFailureCause;

/// The event index of the slot at board index `index`. The board holds at
/// most `MAX_SLOT_INDEX + 1` slots (the profile bounds the children of each
/// kind), so every board index has one.
fn slot_index(index: usize) -> Option<SlotIndex> {
    u32::try_from(index)
        .ok()
        .and_then(|index| SlotIndex::try_from(index).ok())
}

/// The cause of a slot that is down. Only a cause that can clear puts a
/// slot down; a cause that repeats counts toward the boot attempts and gives
/// no `SlotDown`.
fn down_cause(cause: BootFailureCause) -> Option<SlotDownCause> {
    match cause {
        BootFailureCause::ResourceShortage(errno) => {
            Some(SlotDownCause::ResourceShortage { errno })
        }
        BootFailureCause::ThreadStartRefused => Some(SlotDownCause::ThreadStartRefused),
        BootFailureCause::AppLoadFailed
        | BootFailureCause::ChildPoolSetupFailed
        | BootFailureCause::TaskLoopCreateFailed
        | BootFailureCause::SpawnFailed
        | BootFailureCause::LostBeforeReady { .. }
        | BootFailureCause::NotReadyInTime
        | BootFailureCause::ProtocolMismatch(_) => None,
    }
}

impl WorkerTelemetry {
    /// The child `child_pid` of slot `index`, of kind `kind`, sent its ready
    /// frame `boot_duration` after its boot began.
    pub(crate) fn child_started(
        &self,
        kind: Execution,
        index: usize,
        child_pid: ProcessId,
        boot_duration: Duration,
    ) {
        match self {
            Self::Off => {}
            Self::On(on) => on.child_started(kind, index, child_pid, boot_duration),
        }
    }

    /// Slot `index`, of kind `kind`, went down after a failed boot of
    /// `cause`.
    pub(crate) fn slot_down(&self, kind: Execution, index: usize, cause: BootFailureCause) {
        match self {
            Self::Off => {}
            Self::On(on) => on.slot_down(kind, index, cause),
        }
    }

    /// Slot `index`, of kind `kind`, that was down serves again.
    pub(crate) fn slot_recovered(&self, kind: Execution, index: usize) {
        match self {
            Self::Off => {}
            Self::On(on) => on.slot_recovered(kind, index),
        }
    }
}

impl TelemetryOn {
    fn child_started(
        &self,
        kind: Execution,
        index: usize,
        child_pid: ProcessId,
        boot_duration: Duration,
    ) {
        let _counted = self.metrics.children_started.add((kind,), 1);
        let _observed = self.metrics.child_boot.observe((kind,), boot_duration);
        let Some(slot) = slot_index(index) else {
            return;
        };
        let _emitted = self.emit(|| {
            Event::ChildStarted(ChildStarted {
                at: Utc::now(),
                child_kind: kind,
                slot,
                child_pid,
                boot_duration,
            })
        });
    }

    fn slot_down(&self, kind: Execution, index: usize, cause: BootFailureCause) {
        let (Some(slot), Some(cause)) = (slot_index(index), down_cause(cause)) else {
            return;
        };
        let _emitted = self.emit(|| {
            Event::SlotDown(SlotDown {
                at: Utc::now(),
                child_kind: kind,
                slot,
                cause,
            })
        });
    }

    fn slot_recovered(&self, kind: Execution, index: usize) {
        let Some(slot) = slot_index(index) else {
            return;
        };
        let _emitted = self.emit(|| {
            Event::SlotRecovered(SlotRecovered {
                at: Utc::now(),
                child_kind: kind,
                slot,
            })
        });
    }
}
