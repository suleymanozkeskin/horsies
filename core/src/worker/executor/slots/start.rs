//! The start of the children of a worker: the wait for the first
//! generation, the placement of the worker's threads, and the memory
//! baseline guard of each kind of child.
//!
//! The caller starts the pools on one [`PoolBoard`], then calls
//! [`start_children`]. After a start, [`guard_new_kind_baselines`] runs as a
//! service of the worker: a kind whose every slot was down at the first
//! start gets the guard at its first ready child.

use std::fmt;
use std::num::NonZeroU64;

use super::board::{ChildBaseline, PoolBoard, PoolFailure, StartEnd};
use super::dispatch::ExecutionClass;
use super::pool::MemoryLimit;
use crate::worker::executor::placement::{
    place_worker_threads, CpuPlacement, WorkerNotPlaced, WorkerThreads,
};
use crate::worker::executor::protocol::RssReading;

/// A baseline at or above this share of its limit leaves little room for
/// the working set of a task: the start logs a warning.
const BASELINE_WARN_SHARE: f64 = 0.8;

/// The memory limit of each kind of child, for the baseline guard: the
/// `memory_per_child` of the isolated pool and of each shared pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaselineLimits {
    pub isolated: MemoryLimit,
    pub shared_async: MemoryLimit,
    pub shared_thread: MemoryLimit,
}

impl BaselineLimits {
    fn of(&self, class: ExecutionClass) -> MemoryLimit {
        match class {
            ExecutionClass::Isolated => self.isolated,
            ExecutionClass::SharedAsync => self.shared_async,
            ExecutionClass::SharedThread => self.shared_thread,
        }
    }
}

/// The name of the memory limit of a kind, as the messages give it.
fn limit_name(class: ExecutionClass) -> &'static str {
    match class {
        ExecutionClass::Isolated => "max_memory_per_child_mb",
        ExecutionClass::SharedAsync => "the shared async memory limit",
        ExecutionClass::SharedThread => "the shared thread memory limit",
    }
}

/// The settled RSS of a ready child is at or above the memory limit of its
/// kind: the app does not fit this per-child budget, so a recycle would
/// follow every task.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BaselineOverLimit {
    pub class: ExecutionClass,
    /// The largest baseline of the kind, in MiB.
    pub baseline_mb: f64,
    pub limit_mb: NonZeroU64,
}

impl fmt::Display for BaselineOverLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "child baseline {:.0}MB >= {} {}MB; the app does not fit this per-child budget: \
             raise the budget or the limit",
            self.baseline_mb,
            limit_name(self.class),
            self.limit_mb
        )
    }
}

impl std::error::Error for BaselineOverLimit {}

/// Check the baselines of every kind against the limit of that kind (Python
/// `_check_memory_baseline`). A kind with no limit or no baseline checks
/// nothing. A kind whose every baseline is unreadable is skipped with a
/// warning: recycle by memory cannot fire for it either.
pub fn check_memory_baselines(
    baselines: &[ChildBaseline],
    limits: &BaselineLimits,
) -> Result<(), BaselineOverLimit> {
    for class in ExecutionClass::ALL {
        check_kind(class, baselines, limits.of(class))?;
    }
    Ok(())
}

/// Check the baselines of `class` against `limit` (Python
/// `_check_baseline_against`).
fn check_kind(
    class: ExecutionClass,
    baselines: &[ChildBaseline],
    limit: MemoryLimit,
) -> Result<(), BaselineOverLimit> {
    let limit_mb = match limit {
        MemoryLimit::Off => return Ok(()),
        MemoryLimit::AtMb(limit_mb) => limit_mb,
    };
    let of_kind: Vec<&ChildBaseline> = baselines
        .iter()
        .filter(|baseline| baseline.class == class)
        .collect();
    if of_kind.is_empty() {
        return Ok(());
    }
    let largest = of_kind
        .iter()
        .filter_map(|baseline| match baseline.rss {
            RssReading::Mb(rss) => Some(rss.get()),
            RssReading::Unreadable => None,
        })
        .reduce(f64::max);
    let Some(largest) = largest else {
        tracing::warn!(
            "{}={limit_mb}MB is set, but the baseline RSS of the children was unreadable; the \
             baseline guard is skipped, and recycle by memory does not fire while the RSS stays \
             unreadable",
            limit_name(class),
        );
        return Ok(());
    };
    // The limit is a whole number of MiB; the RSS a finite number.
    let limit = limit_mb.get() as f64;
    if largest >= limit {
        return Err(BaselineOverLimit {
            class,
            baseline_mb: largest,
            limit_mb,
        });
    }
    match largest >= BASELINE_WARN_SHARE * limit {
        true => tracing::warn!(
            "the child baseline {largest:.0}MB is within 80% of {} {limit_mb}MB; little room \
             is left for the working set of a task, so children may recycle often",
            limit_name(class),
        ),
        false => tracing::info!(
            "the child baseline {largest:.0}MB against {} {limit_mb}MB",
            limit_name(class),
        ),
    }
    Ok(())
}

/// How the start of the children ended, when it did not fail.
#[derive(Debug, Clone, PartialEq)]
pub enum ChildrenStart {
    /// The first generation is in, and every baseline is under the limit
    /// of its kind. One baseline per slot that booted.
    Started { baselines: Vec<ChildBaseline> },
    /// The stop came during the start. This is no failure.
    Stopped,
}

/// Why the start of the children failed. The worker stops with a non-zero
/// exit; after [`PoolFailure::ChildHookFailed`] it stops with no restart
/// loop.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ChildrenStartFailed {
    #[error("worker child start failed: {0}")]
    Pool(PoolFailure),
    #[error(transparent)]
    WorkerNotPlaced(WorkerNotPlaced),
    #[error(transparent)]
    BaselineOverLimit(BaselineOverLimit),
}

/// Put every thread of the worker process on its core when the placement
/// is [`CpuPlacement::OwnCore`] (Python `_place_worker_process`). Runs after
/// the children started: each child got its own CPUs before its init
/// frame.
fn place_worker(placement: &CpuPlacement, threads: &WorkerThreads) -> Result<(), WorkerNotPlaced> {
    let own = match placement {
        CpuPlacement::SharedCpus => return Ok(()),
        CpuPlacement::OwnCore(own) => own,
    };
    let placed = place_worker_threads(own.parent(), threads)?;
    tracing::info!(
        "the worker process runs on CPUs {} ({placed} threads); its children on CPUs {}",
        own.parent(),
        own.children(),
    );
    Ok(())
}

/// Wait for the first generation of the pools on `board`, place the
/// threads of the worker process by `placement`, then run the memory
/// baseline guard of each kind on its baselines (Python `_start_slots`).
/// Starts nothing itself: the pools on `board` boot their children, on the
/// CPUs of `placement` ([`CpuPlacement::child_cpus`] in their configs).
/// `threads` lists the threads to place:
/// [`WorkerThreads::of_this_process`] in a worker.
pub async fn start_children(
    board: &PoolBoard,
    placement: &CpuPlacement,
    threads: &WorkerThreads,
    limits: &BaselineLimits,
) -> Result<ChildrenStart, ChildrenStartFailed> {
    let baselines = match board.first_generation().await {
        Ok(baselines) => baselines,
        Err(StartEnd::Stopped) => return Ok(ChildrenStart::Stopped),
        Err(StartEnd::Failed(failure)) => return Err(ChildrenStartFailed::Pool(failure)),
    };
    tracing::info!(
        "worker child processes ready: {}/{}",
        baselines.len(),
        board.slot_count()
    );
    place_worker(placement, threads).map_err(ChildrenStartFailed::WorkerNotPlaced)?;
    check_memory_baselines(&baselines, limits).map_err(ChildrenStartFailed::BaselineOverLimit)?;
    Ok(ChildrenStart::Started { baselines })
}

/// Run the memory baseline guard on the first ready child of each kind
/// that had no ready child at the first start (Python
/// `_new_kind_baseline_loop`). Returns the first baseline at or above its
/// limit: the caller stops the worker, which drains the tasks of that child
/// and exits non-zero. Each kind comes at most once, so the guard checks at
/// most one baseline per kind and then waits until it is dropped.
pub async fn guard_new_kind_baselines(
    board: &PoolBoard,
    limits: &BaselineLimits,
) -> BaselineOverLimit {
    loop {
        let baseline = board.wait_new_kind_baseline().await;
        if let Err(over) = check_memory_baselines(std::slice::from_ref(&baseline), limits) {
            tracing::error!(
                slot = baseline.slot,
                pid = baseline.pid.get(),
                "stopping the worker: {over}",
            );
            return over;
        }
    }
}
