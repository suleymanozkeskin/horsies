//! The work of one run in a Rust child: its async body and the work the
//! body starts with [`spawn_in_run`] (Python: the asyncio tasks that carry
//! the run's mark, `core/worker/run_mark.py`).
//!
//! Work that a body starts with plain `tokio::spawn` is not tracked. Work
//! started with [`spawn_in_run`] belongs to the run: the child counts it as
//! work the run left when its body returns, waits for it, cancels it in
//! rounds, and reports what did not stop, as a Python child does with the
//! tasks of its loop. Work that it starts in turn belongs to the same run.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;

use crate::worker::executor::protocol::{
    LeftoverName, LoopHoldsTasks, StuckWork, MAX_LEFTOVER_TASK_NAMES,
};

/// The most pieces of work one run holds alive at once.
pub const MAX_RUN_WORK: usize = 10_000;

/// The rounds of aborts after the first one, for work that must stop
/// (Python `EXTRA_CANCEL_ROUNDS`).
pub(super) const EXTRA_CANCEL_ROUNDS: u32 = 2;
/// The time between two rounds, and from the last round to the stuck check
/// (Python `CANCEL_ROUND_INTERVAL_S`). Three intervals make the
/// protocol's `STUCK_REPORT_DELAY`.
pub(super) const CANCEL_ROUND_INTERVAL: Duration = Duration::from_millis(100);
/// How long an isolated child waits for the work a body left before it
/// cancels it (Python `ISOLATED_LEFTOVER_GRACE_S`).
pub(super) const ISOLATED_LEFTOVER_GRACE: Duration = Duration::from_secs(1);

tokio::task_local! {
    static RUN_SCOPE: RunScope;
}

/// Why [`spawn_in_run`] started no work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RunSpawnRefused {
    /// The caller is not the async body of a run in a Rust child, nor work
    /// that such a body started with `spawn_in_run`.
    #[error("spawn_in_run was called outside the async body of a task run in a Rust child")]
    NotInARun,
    /// The run has ended; its left work is being stopped.
    #[error("the run has ended; it takes no new work")]
    RunEnded,
    /// The run holds [`MAX_RUN_WORK`] pieces of work alive already.
    #[error("the run holds {bound} pieces of work alive already")]
    TooMuchWork { bound: usize },
}

/// Start `future` as work of the current run, named `name` in reports. It
/// is counted as work the run left when its body returns, and it is
/// aborted with the run. The returned handle gives its output.
///
/// Errors: [`RunSpawnRefused`]; nothing was started then.
pub fn spawn_in_run<F>(name: &str, future: F) -> Result<JoinHandle<F::Output>, RunSpawnRefused>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let scope = RUN_SCOPE
        .try_with(RunScope::clone)
        .map_err(|_| RunSpawnRefused::NotInARun)?;
    let id = scope.register(name)?;
    Ok(scope.spawn_registered(id, future))
}

/// Whether the run takes new work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeState {
    Open,
    Closed,
}

/// How a piece of work can be aborted. The handle comes right after the
/// spawn; an abort before it is kept and done when the handle comes.
enum AbortSlot {
    Pending,
    AbortWhenAttached,
    Attached(AbortHandle),
}

struct AliveWork {
    name: LeftoverName,
    abort: AbortSlot,
}

struct ScopeWork {
    state: ScopeState,
    next_id: u64,
    alive: BTreeMap<u64, AliveWork>,
}

struct ScopeShared {
    work: Mutex<ScopeWork>,
    /// Woken each time a piece of work ends.
    ended: Notify,
}

impl ScopeShared {
    fn lock(&self) -> MutexGuard<'_, ScopeWork> {
        // A poisoned lock means a panic while it was held; the map is a
        // valid value.
        self.work.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Removes its work from the run when the work's future ends or is
/// dropped: at its end, at an abort, or at a panic.
struct WorkEnded {
    shared: Arc<ScopeShared>,
    id: u64,
}

impl Drop for WorkEnded {
    fn drop(&mut self) {
        let _removed = self.shared.lock().alive.remove(&self.id);
        self.shared.ended.notify_waiters();
    }
}

/// What of a run is alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Alive {
    Nothing,
    Work {
        count: NonZeroU64,
        names: Vec<LeftoverName>,
    },
}

/// The report of the work a run left after its body returned.
pub(super) fn left_report(count: NonZeroU64, names: Vec<LeftoverName>) -> LoopHoldsTasks {
    LoopHoldsTasks::new(count, names).expect("the names are the first min(count, 10) alive")
}

/// The report of work that did not stop after its cancel rounds.
pub(super) fn stuck_report(count: NonZeroU64, names: Vec<LeftoverName>) -> StuckWork {
    StuckWork::new(count.get(), 0, names).expect("the names are the first min(count, 10) alive")
}

/// Whether a wait saw every piece of work end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WorkWait {
    Done,
    StillAlive,
}

/// How the cancel rounds ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RoundsEnd {
    Ended,
    Stuck,
}

/// The work of one run.
#[derive(Clone)]
pub(super) struct RunScope {
    shared: Arc<ScopeShared>,
}

impl RunScope {
    pub(super) fn new() -> Self {
        Self {
            shared: Arc::new(ScopeShared {
                work: Mutex::new(ScopeWork {
                    state: ScopeState::Open,
                    next_id: 0,
                    alive: BTreeMap::new(),
                }),
                ended: Notify::new(),
            }),
        }
    }

    fn register(&self, name: &str) -> Result<u64, RunSpawnRefused> {
        let mut work = self.shared.lock();
        match (work.state, work.alive.len() >= MAX_RUN_WORK) {
            (ScopeState::Closed, _) => return Err(RunSpawnRefused::RunEnded),
            (ScopeState::Open, true) => {
                return Err(RunSpawnRefused::TooMuchWork {
                    bound: MAX_RUN_WORK,
                })
            }
            (ScopeState::Open, false) => {}
        }
        let id = work.next_id;
        work.next_id = work.next_id.saturating_add(1);
        work.alive.insert(
            id,
            AliveWork {
                name: LeftoverName::cut(name),
                abort: AbortSlot::Pending,
            },
        );
        Ok(id)
    }

    fn attach(&self, id: u64, handle: AbortHandle) {
        let mut work = self.shared.lock();
        let Some(alive) = work.alive.get_mut(&id) else {
            // The work ended before its handle came.
            return;
        };
        match alive.abort {
            AbortSlot::Pending => alive.abort = AbortSlot::Attached(handle),
            AbortSlot::AbortWhenAttached => {
                handle.abort();
                alive.abort = AbortSlot::Attached(handle);
            }
            AbortSlot::Attached(_) => unreachable!("work {id} got its abort handle twice"),
        }
    }

    /// Spawn registered work inside this run, so the work it starts in turn
    /// belongs to the run too.
    fn spawn_registered<F>(&self, id: u64, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let ended = WorkEnded {
            shared: Arc::clone(&self.shared),
            id,
        };
        let handle = tokio::spawn(RUN_SCOPE.scope(self.clone(), async move {
            let _ended = ended;
            future.await
        }));
        self.attach(id, handle.abort_handle());
        handle
    }

    /// Spawn the body of a run inside it. A new run takes its body.
    pub(super) fn spawn_body<F>(&self, task_name: &str, body: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let id = self
            .register(task_name)
            .expect("a new run is open and holds no work");
        self.spawn_registered(id, body)
    }

    /// Refuse every later spawn of the run.
    pub(super) fn close(&self) {
        self.shared.lock().state = ScopeState::Closed;
    }

    /// The work of the run that is alive, and the names of the first ones.
    pub(super) fn alive(&self) -> Alive {
        let work = self.shared.lock();
        let Some(count) = u64::try_from(work.alive.len())
            .ok()
            .and_then(NonZeroU64::new)
        else {
            return Alive::Nothing;
        };
        let names = work
            .alive
            .values()
            .take(MAX_LEFTOVER_TASK_NAMES)
            .map(|alive| alive.name.clone())
            .collect();
        Alive::Work { count, names }
    }

    /// Abort every piece of work alive.
    fn abort_alive(&self) {
        let mut work = self.shared.lock();
        for alive in work.alive.values_mut() {
            match alive.abort {
                AbortSlot::Pending => alive.abort = AbortSlot::AbortWhenAttached,
                AbortSlot::AbortWhenAttached => {}
                AbortSlot::Attached(ref handle) => handle.abort(),
            }
        }
    }

    /// Wait until no work of the run is alive, at most `bound`.
    pub(super) async fn wait_done(&self, bound: Duration) -> WorkWait {
        let until = Instant::now() + bound;
        loop {
            let ended = self.shared.ended.notified();
            tokio::pin!(ended);
            // Registered before the check, so an end between the check and
            // the wait still wakes it.
            ended.as_mut().enable();
            if self.shared.lock().alive.is_empty() {
                return WorkWait::Done;
            }
            if tokio::time::timeout_at(until, ended).await.is_err() {
                return WorkWait::StillAlive;
            }
        }
    }

    /// Wait until no work of the run is alive, with no bound: the parent
    /// kills a child whose runs are all stuck.
    pub(super) async fn wait_all_ended(&self) {
        loop {
            let ended = self.shared.ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if self.shared.lock().alive.is_empty() {
                return;
            }
            ended.await;
        }
    }

    /// Abort the work alive, and again in [`EXTRA_CANCEL_ROUNDS`] more
    /// rounds [`CANCEL_ROUND_INTERVAL`] apart; work alive one interval
    /// after the last round is stuck.
    pub(super) async fn end_in_rounds(&self) -> RoundsEnd {
        self.abort_alive();
        for _ in 0..EXTRA_CANCEL_ROUNDS {
            if self.wait_done(CANCEL_ROUND_INTERVAL).await == WorkWait::Done {
                return RoundsEnd::Ended;
            }
            self.abort_alive();
        }
        match self.wait_done(CANCEL_ROUND_INTERVAL).await {
            WorkWait::Done => RoundsEnd::Ended,
            WorkWait::StillAlive => RoundsEnd::Stuck,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_spawn_outside_a_run_is_refused() {
        assert!(matches!(
            spawn_in_run("orphan", async {}),
            Err(RunSpawnRefused::NotInARun)
        ));
    }

    /// Python `test_run_mark.py`: work the body starts, and the work that
    /// work starts, carries the run; it is left work when the body returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_a_body_starts_belongs_to_its_run_and_ends_in_rounds() {
        let scope = RunScope::new();
        let body = scope.spawn_body("app.tasks.body", async {
            spawn_in_run("outer", async {
                spawn_in_run("inner", tokio::time::sleep(Duration::from_secs(60)))
                    .expect("work of a run may start work");
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .expect("a body may start work")
        });
        let _outer = body.await.expect("the body ends");
        // The inner spawn runs when the outer work is first polled.
        let _ = scope.wait_done(Duration::from_millis(200)).await;
        match scope.alive() {
            Alive::Work { count, names } => {
                assert_eq!(count.get(), 2);
                let names: Vec<&str> = names.iter().map(LeftoverName::as_str).collect();
                assert_eq!(names, ["outer", "inner"]);
            }
            Alive::Nothing => panic!("the work the body started is alive"),
        }
        scope.close();
        assert_eq!(scope.end_in_rounds().await, RoundsEnd::Ended);
        assert_eq!(scope.alive(), Alive::Nothing);
        assert_eq!(
            scope.wait_done(Duration::from_millis(10)).await,
            WorkWait::Done
        );
    }

    #[tokio::test]
    async fn a_closed_run_takes_no_new_work() {
        let scope = RunScope::new();
        scope.close();
        assert_eq!(scope.register("late"), Err(RunSpawnRefused::RunEnded));
    }

    #[tokio::test]
    async fn a_run_holds_a_bounded_amount_of_work() {
        let scope = RunScope::new();
        for _ in 0..MAX_RUN_WORK {
            scope.register("work").expect("under the bound");
        }
        assert_eq!(
            scope.register("one more"),
            Err(RunSpawnRefused::TooMuchWork {
                bound: MAX_RUN_WORK
            })
        );
    }

    /// Work that holds a runtime thread in one poll cannot be aborted: it
    /// is stuck after the rounds, and the run waits for its end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_that_holds_its_thread_is_stuck_after_the_rounds() {
        let scope = RunScope::new();
        let started = Instant::now();
        let body = scope.spawn_body("app.tasks.body", async {
            spawn_in_run("holds", async {
                std::thread::sleep(Duration::from_secs(2));
            })
            .expect("a body may start work")
        });
        let _held = body.await.expect("the body ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(scope.end_in_rounds().await, RoundsEnd::Stuck);
        match scope.alive() {
            Alive::Work { count, names } => {
                let stuck = stuck_report(count, names);
                assert_eq!(stuck.tasks(), 1);
                assert_eq!(stuck.executor_items(), 0);
            }
            Alive::Nothing => panic!("the held work is alive"),
        }
        scope.wait_all_ended().await;
        assert!(started.elapsed() >= Duration::from_secs(2));
    }

    #[test]
    fn a_report_names_the_first_ten_pieces_of_work() {
        let names: Vec<LeftoverName> = (0..10)
            .map(|n| LeftoverName::cut(&format!("w{n}")))
            .collect();
        let left = left_report(NonZeroU64::new(25).unwrap(), names.clone());
        assert_eq!(left.count().get(), 25);
        assert_eq!(left.names().len(), 10);
        assert_eq!(
            stuck_report(NonZeroU64::new(25).unwrap(), names).tasks(),
            25
        );
    }
}
