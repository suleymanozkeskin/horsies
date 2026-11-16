//! The board of every slot of a worker, of every kind: the one view the
//! restart rules read, the pool failure, and the stop (Python
//! `core/worker/slots.py`, `SlotPool._all_slots`, `_record_failure`,
//! `_check_first_generation`).
//!
//! Slots are numbered across kinds, as in Python: the isolated slots first,
//! then the shared async slots, then the shared thread slots. "Another
//! child serves" and "every other slot is down" look at all of them.

use std::ops::Range;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use std::num::NonZeroU32;
use tokio_util::sync::CancellationToken;

use super::budgets::SlotService;
use super::dispatch::ExecutionClass;
use crate::worker::executor::protocol::ProcessId;
use crate::worker::executor::supervisor::boot::BootFailureDetail;
use crate::worker::executor::supervisor::{BootFailure, BootFailureCause, PoolAtFailure};

/// A slot that is down after a boot failure that can clear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownState {
    pub cause: BootFailureCause,
    pub detail: String,
    pub down_since: DateTime<Utc>,
    pub failed_boots: NonZeroU32,
}

/// The state of one slot, as the board keeps it (Python executor design,
/// sections 8.2 and 8.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotPhase {
    Booting,
    /// A child serves: an isolated child idle or busy, a shared child open.
    Serving {
        pid: ProcessId,
    },
    /// A shared child takes no new task; `runs` tasks are still on it.
    Closing {
        pid: ProcessId,
        runs: u32,
    },
    Retiring,
    Down(DownState),
}

impl SlotPhase {
    /// The service of the slot, for the claim budget.
    pub fn service(&self) -> SlotService {
        match self {
            Self::Booting => SlotService::Booting,
            Self::Serving { .. } => SlotService::InService,
            Self::Closing { runs, .. } => SlotService::Closing { runs: *runs },
            Self::Retiring => SlotService::Retiring,
            Self::Down(_) => SlotService::Down,
        }
    }
}

/// Why no replacement child can start. The worker stops with a non-zero
/// exit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolFailure {
    /// A start hook of a child failed or hung.
    ChildHookFailed,
    /// The last counted boot attempt of a slot failed.
    BootAttemptsExhausted { attempts: u32, last: BootFailure },
    /// Slot `slot` (its board index) hit [`SLOT_DEFECT_LIMIT`] defects with
    /// no finished task between them; `detail` names the last one.
    ///
    /// [`SLOT_DEFECT_LIMIT`]: super::defects::SLOT_DEFECT_LIMIT
    SlotDefectsRepeated {
        slot: usize,
        detail: BootFailureDetail,
    },
}

/// The health of the pools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolHealth {
    Serving,
    Failed(PoolFailure),
    Stopping,
}

/// Whether a slot booted a child at least once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootedOnce {
    No,
    Yes,
}

/// Whether the pools failed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Condition {
    Serving,
    Failed(PoolFailure),
}

struct BoardSlot {
    class: ExecutionClass,
    phase: SlotPhase,
    booted: BootedOnce,
}

struct Slots {
    slots: Vec<BoardSlot>,
    condition: Condition,
}

/// The children of each kind that a worker runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildLayout {
    pub isolated: usize,
    pub shared_async: usize,
    pub shared_thread: usize,
}

/// Every slot of a worker, the pool failure and the stop. Shared by the
/// isolated pool and the shared pools.
pub struct PoolBoard {
    slots: Mutex<Slots>,
    layout: ChildLayout,
    /// Cancelled at the stop: no new task, booting children are ended.
    pub(super) stop: CancellationToken,
    /// Cancelled when the pools fail: every slot stops taking tasks.
    pub(super) failed: CancellationToken,
    /// Cancelled at the drain deadline: children with tasks are killed.
    pub(super) drain_deadline: CancellationToken,
}

impl PoolBoard {
    /// A board for these children; every slot boots.
    pub fn new(layout: ChildLayout) -> Self {
        let classes = std::iter::repeat_n(ExecutionClass::Isolated, layout.isolated)
            .chain(std::iter::repeat_n(
                ExecutionClass::SharedAsync,
                layout.shared_async,
            ))
            .chain(std::iter::repeat_n(
                ExecutionClass::SharedThread,
                layout.shared_thread,
            ));
        let slots = classes
            .map(|class| BoardSlot {
                class,
                phase: SlotPhase::Booting,
                booted: BootedOnce::No,
            })
            .collect();
        Self {
            slots: Mutex::new(Slots {
                slots,
                condition: Condition::Serving,
            }),
            layout,
            stop: CancellationToken::new(),
            failed: CancellationToken::new(),
            drain_deadline: CancellationToken::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Slots> {
        // A poisoned lock means a panic while the board was held; its slots
        // are still valid values.
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The board indices of the slots of `class`.
    pub fn slots_of(&self, class: ExecutionClass) -> Range<usize> {
        let ChildLayout {
            isolated,
            shared_async,
            shared_thread,
        } = self.layout;
        match class {
            ExecutionClass::Isolated => 0..isolated,
            ExecutionClass::SharedAsync => isolated..isolated + shared_async,
            ExecutionClass::SharedThread => {
                isolated + shared_async..isolated + shared_async + shared_thread
            }
        }
    }

    /// Set the phase of slot `index`. A serving child marks the slot as
    /// booted once.
    pub fn set(&self, index: usize, phase: SlotPhase) {
        let mut slots = self.lock();
        let slot = &mut slots.slots[index];
        if let SlotPhase::Serving { .. } = phase {
            slot.booted = BootedOnce::Yes;
        }
        slot.phase = phase;
    }

    pub fn phase(&self, index: usize) -> SlotPhase {
        self.lock().slots[index].phase.clone()
    }

    /// The phases of the slots of `class`, in slot order.
    pub fn phases_of(&self, class: ExecutionClass) -> Vec<SlotPhase> {
        self.lock()
            .slots
            .iter()
            .filter(|slot| slot.class == class)
            .map(|slot| slot.phase.clone())
            .collect()
    }

    /// Every slot that is down, with its board index, for the worker
    /// snapshot.
    pub fn down_slots(&self) -> Vec<(usize, DownState)> {
        self.lock()
            .slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| match &slot.phase {
                SlotPhase::Down(down) => Some((index, down.clone())),
                SlotPhase::Booting
                | SlotPhase::Serving { .. }
                | SlotPhase::Closing { .. }
                | SlotPhase::Retiring => None,
            })
            .collect()
    }

    /// What every other slot looks like at a failed boot of `index`. The
    /// first generation is in when every slot booted once or is down, and
    /// at least one slot booted.
    pub fn pool_at_failure(&self, index: usize) -> PoolAtFailure {
        let slots = self.lock();
        let others = || {
            slots
                .slots
                .iter()
                .enumerate()
                .filter(move |(other, _)| *other != index)
                .map(|(_, slot)| &slot.phase)
        };
        let any_booted = slots
            .slots
            .iter()
            .any(|slot| slot.booted == BootedOnce::Yes);
        let every_slot_booted_or_down = slots
            .slots
            .iter()
            .all(|slot| matches!(slot.phase, SlotPhase::Down(_)) || slot.booted == BootedOnce::Yes);
        PoolAtFailure {
            first_generation_in: any_booted && every_slot_booted_or_down,
            another_child_serves: others().any(|phase| {
                matches!(phase, SlotPhase::Serving { .. } | SlotPhase::Closing { .. })
            }),
            every_other_slot_down: others().all(|phase| matches!(phase, SlotPhase::Down(_))),
        }
    }

    /// Fail the pools: the first failure wins. Every pool answers its
    /// waiting tasks when `failed` fires.
    pub fn fail(&self, failure: PoolFailure) {
        {
            let mut slots = self.lock();
            match slots.condition {
                Condition::Serving => {
                    tracing::error!(
                        failure = ?failure,
                        "the worker's child pool failed: no replacement child can start",
                    );
                    slots.condition = Condition::Failed(failure);
                }
                Condition::Failed(_) => {}
            }
        }
        self.failed.cancel();
    }

    pub fn health(&self) -> PoolHealth {
        let condition = self.lock().condition.clone();
        match (condition, self.stop.is_cancelled()) {
            (Condition::Failed(failure), _) => PoolHealth::Failed(failure),
            (Condition::Serving, true) => PoolHealth::Stopping,
            (Condition::Serving, false) => PoolHealth::Serving,
        }
    }

    /// Stop every pool: no new task, booting children are ended.
    pub fn begin_stop(&self) {
        self.stop.cancel();
    }

    /// Kill the children that still run tasks at the drain deadline.
    pub fn pass_drain_deadline(&self) {
        self.drain_deadline.cancel();
    }

    /// Whether a slot may not take a task: the pools stop or failed.
    pub fn not_serving(&self) -> bool {
        self.stop.is_cancelled() || self.failed.is_cancelled()
    }
}
