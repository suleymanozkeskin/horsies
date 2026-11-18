//! The board of every slot of a worker, of every kind: the one view the
//! restart rules read, the first generation and the baselines of its
//! children, the pool failure, and the stop (Python `core/worker/slots.py`,
//! `SlotPool._all_slots`, `_record_failure`, `_check_first_generation`,
//! `_note_ready`, `start`, `wait_new_kind_baseline`).
//!
//! Slots are numbered across kinds, as in Python: the isolated slots first,
//! then the shared async slots, then the shared thread slots. "Another
//! child serves" and "every other slot is down" look at all of them.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use std::num::NonZeroU32;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::budgets::SlotService;
use super::dispatch::ExecutionClass;
use crate::worker::executor::protocol::{ProcessId, RssReading};
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

impl std::fmt::Display for PoolFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChildHookFailed => f.write_str("an on_child_process_start hook failed"),
            Self::BootAttemptsExhausted { attempts, last } => {
                write!(
                    f,
                    "{attempts} child boot attempts failed; the last one: {last}"
                )
            }
            Self::SlotDefectsRepeated { slot, detail } => write!(
                f,
                "slot {slot} hit {} defects in a row: {detail}",
                super::defects::SLOT_DEFECT_LIMIT
            ),
        }
    }
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

/// A child that sent its ready frame: its pid and the RSS of that frame.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReadyChild {
    pub pid: ProcessId,
    pub rss: RssReading,
}

/// The settled RSS of a ready child, for the memory baseline guard of its
/// kind (Python `ChildBaseline`). `slot` is its board index; `class` names
/// the kind of the child.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChildBaseline {
    pub slot: usize,
    pub class: ExecutionClass,
    pub pid: ProcessId,
    pub rss: RssReading,
}

/// Whether the first generation is in: every slot booted once or is down,
/// and at least one slot booted. It stays in once it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirstGeneration {
    Pending,
    In,
}

/// Why the start of the pools ended with no first generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartEnd {
    /// No replacement child can start. The worker stops with a non-zero
    /// exit.
    Failed(PoolFailure),
    /// The stop came during the start. This is no failure.
    Stopped,
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
    first_generation: FirstGeneration,
    /// One baseline per slot that booted in the first generation; a
    /// replacement child of a slot replaces the baseline of its slot.
    first_baselines: BTreeMap<usize, ChildBaseline>,
    /// The kinds whose baseline went to the guard: through the start for
    /// the first generation, through the new-kind queue after it.
    kinds_with_baseline: BTreeSet<ExecutionClass>,
    /// The baseline of the first ready child of each kind that had no
    /// ready child in the first generation; each kind at most once.
    new_kind_baselines: VecDeque<ChildBaseline>,
}

impl Slots {
    /// Mark the first generation in when every slot booted once or is
    /// down, and at least one slot booted. Returns whether this call did.
    fn check_first_generation(&mut self) -> bool {
        if self.first_generation == FirstGeneration::In {
            return false;
        }
        let any_booted = self.slots.iter().any(|slot| slot.booted == BootedOnce::Yes);
        let every_slot_booted_or_down = self
            .slots
            .iter()
            .all(|slot| matches!(slot.phase, SlotPhase::Down(_)) || slot.booted == BootedOnce::Yes);
        match any_booted && every_slot_booted_or_down {
            true => {
                self.first_generation = FirstGeneration::In;
                true
            }
            false => false,
        }
    }
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
    /// Cancelled when the first generation is in.
    first_generation_in: CancellationToken,
    /// Woken when a new-kind baseline is queued.
    new_kind: Notify,
    /// Woken when a child becomes ready to take a task after a boot. Holds
    /// at most one wake while no one waits.
    child_ready: Notify,
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
                first_generation: FirstGeneration::Pending,
                first_baselines: BTreeMap::new(),
                kinds_with_baseline: BTreeSet::new(),
                new_kind_baselines: VecDeque::new(),
            }),
            layout,
            stop: CancellationToken::new(),
            failed: CancellationToken::new(),
            drain_deadline: CancellationToken::new(),
            first_generation_in: CancellationToken::new(),
            new_kind: Notify::new(),
            child_ready: Notify::new(),
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
        let first_generation_now = {
            let mut slots = self.lock();
            let slot = &mut slots.slots[index];
            if let SlotPhase::Serving { .. } = phase {
                slot.booted = BootedOnce::Yes;
            }
            slot.phase = phase;
            slots.check_first_generation()
        };
        if first_generation_now {
            self.first_generation_in.cancel();
        }
    }

    /// The child of slot `index` sent its ready frame: the slot serves it,
    /// and its baseline is noted for the memory guard (Python
    /// `_note_ready`). In the first generation the baseline goes to
    /// [`Self::first_generation`]; after it, the first ready child of a kind
    /// with no baseline yet goes to [`Self::wait_new_kind_baseline`], once
    /// per kind. Any other child adds nothing.
    pub fn ready(&self, index: usize, child: ReadyChild) {
        let (first_generation_now, new_kind) = {
            let mut slots = self.lock();
            let class = slots.slots[index].class;
            let slot = &mut slots.slots[index];
            slot.booted = BootedOnce::Yes;
            slot.phase = SlotPhase::Serving { pid: child.pid };
            let baseline = ChildBaseline {
                slot: index,
                class,
                pid: child.pid,
                rss: child.rss,
            };
            let new_kind = match slots.first_generation {
                FirstGeneration::Pending => {
                    let _replaced = slots.first_baselines.insert(index, baseline);
                    let _first = slots.kinds_with_baseline.insert(class);
                    false
                }
                FirstGeneration::In => match slots.kinds_with_baseline.insert(class) {
                    true => {
                        slots.new_kind_baselines.push_back(baseline);
                        true
                    }
                    false => false,
                },
            };
            (slots.check_first_generation(), new_kind)
        };
        if first_generation_now {
            self.first_generation_in.cancel();
        }
        if new_kind {
            self.new_kind.notify_one();
        }
    }

    /// Wait until the first generation is in, the pools fail, or the stop
    /// (Python `SlotPool.start`). `Ok` holds one baseline per slot that
    /// booted, in slot order; a pool with a slot down starts degraded and
    /// logs it, and that slot keeps booting. When every slot is down, failed
    /// boots count, so the pools fail after the boot attempts.
    pub async fn first_generation(&self) -> Result<Vec<ChildBaseline>, StartEnd> {
        tokio::select! {
            () = self.first_generation_in.cancelled() => {}
            () = self.failed.cancelled() => {}
            () = self.stop.cancelled() => {}
        }
        match self.health() {
            PoolHealth::Failed(failure) => return Err(StartEnd::Failed(failure)),
            PoolHealth::Stopping => return Err(StartEnd::Stopped),
            PoolHealth::Serving => {}
        }
        let baselines: Vec<ChildBaseline> = self.lock().first_baselines.values().copied().collect();
        self.log_degraded_start();
        Ok(baselines)
    }

    /// Wait for the baseline of the first ready child of a kind that had no
    /// ready child in the first generation. Each kind comes at most once; a
    /// kind of the first generation never comes.
    pub async fn wait_new_kind_baseline(&self) -> ChildBaseline {
        loop {
            let notified = self.new_kind.notified();
            let queued = self.lock().new_kind_baselines.pop_front();
            if let Some(baseline) = queued {
                return baseline;
            }
            notified.await;
        }
    }

    /// A child became ready to take a task after a boot: an isolated slot
    /// waits for its first task of a generation, or a shared child is open
    /// with a free seat for the first time in its generation. Wakes the
    /// claim loop once; wakes noted while no one waits are kept as one.
    pub(crate) fn note_child_ready(&self) {
        self.child_ready.notify_one();
    }

    /// Wait until a child becomes ready to take a task after a boot
    /// ([`Self::note_child_ready`]). A wake noted before the wait returns at
    /// once. Cancel-safe: a wait dropped before it returns loses no wake.
    pub async fn child_ready(&self) {
        self.child_ready.notified().await;
    }

    /// Log a start with slots down: how many slots serve, and why each down
    /// slot is down (Python `_log_degraded_start`).
    fn log_degraded_start(&self) {
        let down = self.down_slots();
        if down.is_empty() {
            return;
        }
        let total = self.lock().slots.len();
        let causes: Vec<String> = down
            .iter()
            .map(|(slot, state)| format!("slot {slot}: {}", state.cause))
            .collect();
        tracing::warn!(
            "the worker starts degraded: {} of {total} slots serve; down: {}",
            total - down.len(),
            causes.join("; "),
        );
    }

    pub fn phase(&self, index: usize) -> SlotPhase {
        self.lock().slots[index].phase.clone()
    }

    /// The number of slots of every kind.
    pub fn slot_count(&self) -> usize {
        self.lock().slots.len()
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
    /// at least one slot booted; it stays in once it is.
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
        PoolAtFailure {
            first_generation_in: slots.first_generation == FirstGeneration::In,
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
                        "Worker cannot start a child: {failure}",
                    );
                    slots.condition = Condition::Failed(failure);
                }
                Condition::Failed(_) => {}
            }
        }
        self.failed.cancel();
    }

    /// Wait until the pools fail, and return the failure that won (Python
    /// `SlotPool.wait_failed`). Waits forever on pools that never fail.
    pub async fn wait_failed(&self) -> PoolFailure {
        self.failed.cancelled().await;
        match &self.lock().condition {
            Condition::Failed(failure) => failure.clone(),
            // `fail` records the failure before it fires the token.
            Condition::Serving => unreachable!("the pools were failed with no failure recorded"),
        }
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
