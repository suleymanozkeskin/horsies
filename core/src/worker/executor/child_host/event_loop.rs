//! The watch of a host program's own event loop (Python
//! `core/worker/loop_watchdog.py`).
//!
//! A host program that runs task code on an event loop of its own (for
//! example a JavaScript event loop) gives the child a [`HostLoop`]. A timer
//! of that loop calls [`HostLoop::turned`] every [`HOST_LOOP_TURN_TICK`],
//! and the host marks the run whose code the loop runs now
//! ([`HostLoop::hold`], [`HostLoop::release`]). The core knows no host
//! language: a host loop is a loop with turns and a holder mark.
//!
//! The watch thread of the child reads the loop at each pass. When the loop
//! has not turned for `LOOP_BLOCK_LIMIT`, the child sends one `loop_blocked`
//! frame, and the parent kills the child at once; the watch sends no later
//! frame for the loop. The culprit is the held run only when every sample of
//! the block saw it: a sample counts once the loop has not turned for
//! [`CULPRIT_SAMPLE_AFTER`], and the read at the report is a sample too.
//! Samples that saw two runs, or no held run, give an unknown culprit.
//!
//! The watch runs in a shared async child, next to the poll watch of its
//! Rust futures, and in a shared thread child, for its main thread. An
//! isolated child has no watch: the parent holds the deadline.
//!
//! Differences from Python's watchdog, both inside the 5.5 s rule of a
//! blocked loop:
//! - The watch reads the loop at each pass of the child's watch thread,
//!   every 250 ms; Python reads every 100 ms. A block of 5 s gives about 19
//!   samples (Python: about 48), and the report comes at most 5.25 s after
//!   the last turn.
//! - In a shared async child the poll watch of the Rust futures and this
//!   watch can each send a `loop_blocked` frame in the same pass. The parent
//!   kills the child at the first frame and takes no action at the second
//!   (`AlreadyKilled`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::run_work::millis_since;
use crate::worker::executor::protocol::LOOP_BLOCK_LIMIT;

/// How often the host's timer records a turn of its loop (Python
/// `LOOP_TURN_TICK_S`). The timer also wakes an idle loop.
pub const HOST_LOOP_TURN_TICK: Duration = Duration::from_millis(100);

/// A sample of the run that a host loop runs counts only when the loop has
/// not turned for this long: two ticks (Python `CULPRIT_SAMPLE_AFTER_MS`).
/// An idle loop turns at each tick, so past two ticks the loop is busy in
/// one turn, and a sample sees code of the block.
const CULPRIT_SAMPLE_AFTER: Duration = Duration::from_millis(200);

const _: () = assert!(
    CULPRIT_SAMPLE_AFTER.as_millis() == 2 * HOST_LOOP_TURN_TICK.as_millis(),
    "a sample counts after two ticks",
);
const _: () = assert!(
    LOOP_BLOCK_LIMIT.as_millis() > CULPRIT_SAMPLE_AFTER.as_millis(),
    "the block limit must leave room for samples of the culprit",
);

/// The loops whose block a task child reports.
pub enum EventLoopWatch {
    /// The child's Rust runtime only: the poll watch times the polls of the
    /// Rust futures of each run (a Rust child).
    RuntimeOnly,
    /// Also the host program's own event loop.
    Host(Arc<HostLoop>),
}

/// The run whose code a host loop runs now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoopHolder {
    NoRun,
    Run(Uuid),
}

/// The event loop of a host program, as the host reports it: its last turn,
/// and the run whose code it runs now. Every method is a short write or
/// read, safe from any thread.
pub struct HostLoop {
    epoch: Instant,
    /// Milliseconds from `epoch` to the last turn. The creation of the loop
    /// counts as its first turn.
    last_turn_ms: AtomicU64,
    holder: Mutex<LoopHolder>,
}

impl Default for HostLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl HostLoop {
    /// A loop that turned now and runs code of no run.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            last_turn_ms: AtomicU64::new(0),
            holder: Mutex::new(LoopHolder::NoRun),
        }
    }

    fn lock_holder(&self) -> MutexGuard<'_, LoopHolder> {
        // A poisoned lock means a panic while it was held; the mark is a
        // valid value.
        self.holder.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The loop turned now. The host's timer calls it every
    /// [`HOST_LOOP_TURN_TICK`].
    pub fn turned(&self) {
        self.last_turn_ms
            .store(millis_since(self.epoch), Ordering::Release);
    }

    /// The loop runs code of the run of task `task_id` now: for example the
    /// first synchronous part of its body, or of work that it started.
    pub fn hold(&self, task_id: Uuid) {
        *self.lock_holder() = LoopHolder::Run(task_id);
    }

    /// The loop runs code of no run now.
    pub fn release(&self) {
        *self.lock_holder() = LoopHolder::NoRun;
    }

    /// Milliseconds from the loop's epoch to now. Pure read of the clock.
    fn now_ms(&self) -> u64 {
        millis_since(self.epoch)
    }

    /// One read of the loop by the watch: its last turn, the time of the
    /// read, and the holder mark, which belongs to the block only when no
    /// turn came during the read. Pure read.
    fn read(&self) -> LoopRead {
        let last_turn_ms = self.last_turn_ms.load(Ordering::Acquire);
        let now_ms = self.now_ms();
        let holder = *self.lock_holder();
        let holder = match self.last_turn_ms.load(Ordering::Acquire) == last_turn_ms {
            true => HolderRead::InBlock(holder),
            false => HolderRead::TurnedDuringRead,
        };
        LoopRead {
            last_turn_ms,
            now_ms,
            holder,
        }
    }
}

/// The holder mark of one read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HolderRead {
    /// No turn came during the read: the mark belongs to the block after
    /// the last turn.
    InBlock(LoopHolder),
    /// The loop turned during the read: it is not blocked, and the mark may
    /// belong to the next turn.
    TurnedDuringRead,
}

/// One read of a host loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LoopRead {
    last_turn_ms: u64,
    now_ms: u64,
    holder: HolderRead,
}

/// What the samples of the current block saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockSamples {
    NoSample,
    /// Every sample saw the run of this task.
    OneRun(Uuid),
    /// Two samples saw different runs, or one saw no run.
    NotOneRun,
}

/// `samples` with one more sample, `holder`. Pure.
fn with_sample(samples: BlockSamples, holder: LoopHolder) -> BlockSamples {
    match (samples, holder) {
        (_, LoopHolder::NoRun) => BlockSamples::NotOneRun,
        (BlockSamples::NoSample, LoopHolder::Run(task_id)) => BlockSamples::OneRun(task_id),
        (BlockSamples::OneRun(seen), LoopHolder::Run(task_id)) if seen == task_id => samples,
        (BlockSamples::OneRun(_) | BlockSamples::NotOneRun, LoopHolder::Run(_)) => {
            BlockSamples::NotOneRun
        }
    }
}

/// The run that held a blocked host loop, as its samples name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostCulprit {
    /// Every sample saw the run of this task.
    Run(Uuid),
    Unknown,
}

fn culprit_of(samples: BlockSamples) -> HostCulprit {
    match samples {
        BlockSamples::OneRun(task_id) => HostCulprit::Run(task_id),
        BlockSamples::NoSample | BlockSamples::NotOneRun => HostCulprit::Unknown,
    }
}

/// What one pass of the watch found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostPass {
    NoReport,
    /// The loop has not turned for `LOOP_BLOCK_LIMIT`: the one report.
    Blocked {
        blocked_ms: u64,
        culprit: HostCulprit,
    },
}

/// Whether the watch sent its report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Report {
    NotSent,
    /// The parent kills the child at the report; the watch reports no more.
    Sent,
}

/// How long a loop has not turned, against the two bounds of the watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockStage {
    /// Shorter than [`CULPRIT_SAMPLE_AFTER`]: the loop turns.
    Turning,
    /// From [`CULPRIT_SAMPLE_AFTER`] to the block limit: a sample counts.
    Sampling,
    PastLimit,
}

fn stage_of(blocked_ms: u64) -> BlockStage {
    let sample_after = u64::try_from(CULPRIT_SAMPLE_AFTER.as_millis()).unwrap_or(u64::MAX);
    let limit = u64::try_from(LOOP_BLOCK_LIMIT.as_millis()).unwrap_or(u64::MAX);
    match (blocked_ms >= limit, blocked_ms >= sample_after) {
        (true, _) => BlockStage::PastLimit,
        (false, true) => BlockStage::Sampling,
        (false, false) => BlockStage::Turning,
    }
}

/// The state of the watch of one host loop between its passes. Pure: each
/// pass takes one read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LoopWatchState {
    /// The time the watch started: a turn before it counts as a turn then,
    /// so a loop that the boot held (a start hook) is not blocked at once.
    started_ms: u64,
    /// The turn that the current block follows.
    block_after_ms: u64,
    samples: BlockSamples,
    report: Report,
}

impl LoopWatchState {
    fn new(started_ms: u64) -> Self {
        Self {
            started_ms,
            block_after_ms: started_ms,
            samples: BlockSamples::NoSample,
            report: Report::NotSent,
        }
    }

    /// One pass with `read`. A turn since the last sample starts a new
    /// block. Past the limit, the read is the last sample and the report
    /// names the culprit of the block's samples.
    fn pass(&mut self, read: LoopRead) -> HostPass {
        match self.report {
            Report::Sent => return HostPass::NoReport,
            Report::NotSent => {}
        }
        let holder = match read.holder {
            HolderRead::TurnedDuringRead => return HostPass::NoReport,
            HolderRead::InBlock(holder) => holder,
        };
        let turn = read.last_turn_ms.max(self.started_ms);
        let blocked_ms = read.now_ms.saturating_sub(turn);
        let stage = stage_of(blocked_ms);
        match stage {
            BlockStage::Turning => return HostPass::NoReport,
            BlockStage::Sampling | BlockStage::PastLimit => {}
        }
        if turn != self.block_after_ms {
            self.block_after_ms = turn;
            self.samples = BlockSamples::NoSample;
        }
        self.samples = with_sample(self.samples, holder);
        match stage {
            BlockStage::Turning | BlockStage::Sampling => HostPass::NoReport,
            BlockStage::PastLimit => {
                self.report = Report::Sent;
                HostPass::Blocked {
                    blocked_ms,
                    culprit: culprit_of(self.samples),
                }
            }
        }
    }
}

/// Whether the watch thread of a child reads a host loop.
pub(super) enum HostLoopWatched {
    NotWatched,
    Watched(Arc<HostLoop>),
}

impl HostLoopWatched {
    /// The watch of the loop, started now.
    pub(super) fn start(self) -> HostWatch {
        match self {
            Self::NotWatched => HostWatch::NotWatched,
            Self::Watched(host) => {
                let state = LoopWatchState::new(host.now_ms());
                HostWatch::Watching { host, state }
            }
        }
    }
}

/// The watch of a host loop on the watch thread.
pub(super) enum HostWatch {
    NotWatched,
    Watching {
        host: Arc<HostLoop>,
        state: LoopWatchState,
    },
}

impl HostWatch {
    /// Read the loop once and give what this pass found.
    pub(super) fn pass(&mut self) -> HostPass {
        match self {
            Self::NotWatched => HostPass::NoReport,
            Self::Watching { host, state } => state.pass(host.read()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time between two passes of the child's watch thread.
    const PASS_MS: u64 = 250;
    const LIMIT_MS: u64 = 5_000;

    fn run(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn read(last_turn_ms: u64, now_ms: u64, holder: LoopHolder) -> LoopRead {
        LoopRead {
            last_turn_ms,
            now_ms,
            holder: HolderRead::InBlock(holder),
        }
    }

    /// The passes of a watch that started at 0 over a loop that last turned
    /// at `last_turn_ms`, every [`PASS_MS`] up to `until_ms`, with the
    /// holder that `holder_at` gives for each pass time. The reports, with
    /// the time of each.
    fn passes(
        last_turn_ms: u64,
        until_ms: u64,
        holder_at: impl Fn(u64) -> LoopHolder,
    ) -> Vec<(u64, HostPass)> {
        let mut state = LoopWatchState::new(0);
        (1..=until_ms / PASS_MS)
            .map(|pass| pass * PASS_MS)
            .map(|now_ms| {
                let found = state.pass(read(last_turn_ms, now_ms, holder_at(now_ms)));
                (now_ms, found)
            })
            .filter(|(_, found)| *found != HostPass::NoReport)
            .collect()
    }

    /// A loop that turns every 100 ms gives no report: a thread turns a
    /// real loop while the watch reads it at its own pace.
    #[test]
    fn a_loop_that_turns_every_tick_gives_no_report() {
        let host = Arc::new(HostLoop::new());
        let turning = Arc::clone(&host);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_turning = Arc::clone(&stop);
        let timer = std::thread::spawn(move || {
            while !stop_turning.load(Ordering::Acquire) {
                std::thread::sleep(HOST_LOOP_TURN_TICK);
                turning.turned();
            }
        });
        let mut watch = HostLoopWatched::Watched(Arc::clone(&host)).start();
        let reports: Vec<HostPass> = (0..8)
            .map(|_| {
                std::thread::sleep(Duration::from_millis(PASS_MS));
                watch.pass()
            })
            .filter(|found| *found != HostPass::NoReport)
            .collect();
        stop.store(true, Ordering::Release);
        timer.join().expect("the timer thread ends");
        assert!(reports.is_empty(), "{reports:?}");
    }

    /// No turn for 5 s with one held run in every sample: one report, at
    /// the limit, that names the run.
    #[test]
    fn a_block_held_by_one_run_names_it() {
        let reports = passes(0, LIMIT_MS, |_| LoopHolder::Run(run(1)));
        assert_eq!(
            reports,
            vec![(
                LIMIT_MS,
                HostPass::Blocked {
                    blocked_ms: LIMIT_MS,
                    culprit: HostCulprit::Run(run(1)),
                }
            )]
        );
    }

    /// Samples that saw two different runs give an unknown culprit.
    #[test]
    fn a_block_seen_with_two_runs_has_no_named_culprit() {
        let reports = passes(0, LIMIT_MS, |now_ms| match now_ms < 2_500 {
            true => LoopHolder::Run(run(1)),
            false => LoopHolder::Run(run(2)),
        });
        assert_eq!(
            reports,
            vec![(
                LIMIT_MS,
                HostPass::Blocked {
                    blocked_ms: LIMIT_MS,
                    culprit: HostCulprit::Unknown,
                }
            )]
        );
    }

    /// Samples with no held run (code that belongs to no run, such as the
    /// main thread of a shared thread child) give an unknown culprit.
    #[test]
    fn a_block_with_no_held_run_has_no_named_culprit() {
        let reports = passes(0, LIMIT_MS, |_| LoopHolder::NoRun);
        assert_eq!(
            reports,
            vec![(
                LIMIT_MS,
                HostPass::Blocked {
                    blocked_ms: LIMIT_MS,
                    culprit: HostCulprit::Unknown,
                }
            )]
        );
    }

    /// A sample before two ticks without a turn does not count: a run that
    /// the loop held only in the first 200 ms of the block is not seen.
    #[test]
    fn a_read_before_two_ticks_is_not_a_sample() {
        let mut state = LoopWatchState::new(0);
        assert_eq!(
            state.pass(read(0, 150, LoopHolder::Run(run(9)))),
            HostPass::NoReport
        );
        let reports: Vec<HostPass> = (1..=20)
            .map(|pass| state.pass(read(0, pass * PASS_MS, LoopHolder::Run(run(1)))))
            .filter(|found| *found != HostPass::NoReport)
            .collect();
        assert_eq!(
            reports,
            vec![HostPass::Blocked {
                blocked_ms: LIMIT_MS,
                culprit: HostCulprit::Run(run(1)),
            }]
        );
    }

    /// A block of 20 s gives one report.
    #[test]
    fn a_block_of_twenty_seconds_gives_one_report() {
        let reports = passes(0, 20_000, |_| LoopHolder::Run(run(1)));
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].0, LIMIT_MS);
    }

    /// A turn starts a new block: the samples of the block before it do not
    /// count against the run that holds the next block.
    #[test]
    fn a_turn_starts_a_new_block_with_new_samples() {
        let mut state = LoopWatchState::new(0);
        for now_ms in [250, 500, 750] {
            assert_eq!(
                state.pass(read(0, now_ms, LoopHolder::Run(run(1)))),
                HostPass::NoReport
            );
        }
        let reports: Vec<HostPass> = (4..=24)
            .map(|pass| state.pass(read(800, pass * PASS_MS, LoopHolder::Run(run(2)))))
            .filter(|found| *found != HostPass::NoReport)
            .collect();
        assert_eq!(
            reports,
            vec![HostPass::Blocked {
                blocked_ms: 5_200,
                culprit: HostCulprit::Run(run(2)),
            }]
        );
    }

    /// A turn during a read: the loop is not blocked, and the read is no
    /// sample.
    #[test]
    fn a_turn_during_a_read_gives_no_sample_and_no_report() {
        let mut state = LoopWatchState::new(0);
        let turned = LoopRead {
            last_turn_ms: 0,
            now_ms: 6_000,
            holder: HolderRead::TurnedDuringRead,
        };
        assert_eq!(state.pass(turned), HostPass::NoReport);
        assert_eq!(state.samples, BlockSamples::NoSample);
        assert_eq!(state.report, Report::NotSent);
    }

    /// A loop that the boot held before the watch started is not blocked at
    /// the start: the start of the watch counts as a turn.
    #[test]
    fn a_turn_before_the_watch_started_counts_as_the_start() {
        let mut state = LoopWatchState::new(30_000);
        assert_eq!(
            state.pass(read(0, 30_250, LoopHolder::NoRun)),
            HostPass::NoReport
        );
        assert_eq!(state.report, Report::NotSent);
    }

    /// A loop that is not watched reports nothing.
    #[test]
    fn a_loop_that_is_not_watched_gives_no_report() {
        let mut watch = HostLoopWatched::NotWatched.start();
        assert_eq!(watch.pass(), HostPass::NoReport);
    }

    /// The holder mark of a real loop: a held run, then no run.
    #[test]
    fn the_holder_mark_names_the_held_run_until_its_release() {
        let host = HostLoop::new();
        assert_eq!(host.read().holder, HolderRead::InBlock(LoopHolder::NoRun));
        host.hold(run(3));
        assert_eq!(
            host.read().holder,
            HolderRead::InBlock(LoopHolder::Run(run(3)))
        );
        host.release();
        assert_eq!(host.read().holder, HolderRead::InBlock(LoopHolder::NoRun));
    }
}
