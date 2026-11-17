//! Slots: the children that run tasks, the budget of each class, and the
//! end of each attempt.
//!
//! - [`dispatch`]: the claimed task the executor takes, and [`AttemptEnd`],
//!   the one closed type for how an attempt ended.
//! - [`route`]: the claim plan and the route of each claimed row.
//! - [`budgets`]: the capacity in service of each budget.
//! - [`board`]: every slot of the worker, of every kind: the one view of
//!   the restart rules, the pool failure and the stop.
//! - [`pool`] and [`isolated`]: the isolated children and their slots.
//! - [`shared`] and the shared slot task: the shared children of one kind.
//! - [`defects`]: slot defects, and the pool failure after repeated ones.
//! - [`start`]: the wait for the first generation, and the memory baseline
//!   guard of each kind of child.
//! - [`outcome`]: the hand-off of each end to the existing outcome writers.
//! - [`shared_stop`] and [`seat_runs`]: the rules of shared children: the
//!   stop report of each run, the verdict of each run at a kill, the
//!   backstops, placement and recycle.

pub mod board;
mod booting;
pub mod budgets;
pub mod defects;
pub mod dispatch;
pub mod isolated;
pub mod outcome;
pub mod pool;
pub mod route;
pub mod seat_runs;
pub mod shared;
mod shared_slot;
pub mod shared_stop;
pub mod start;

#[cfg(test)]
mod boot_tests;
#[cfg(test)]
mod claim_plan_tests;
#[cfg(test)]
mod exit_not_seen_tests;
#[cfg(test)]
mod held_exits;
#[cfg(test)]
mod isolated_run_tests;
#[cfg(test)]
mod isolated_stop_tests;
#[cfg(test)]
mod isolated_verdict_tests;
#[cfg(test)]
mod outcome_tests;
#[cfg(all(test, target_os = "linux"))]
mod placement_tests;
#[cfg(test)]
mod plain_success_tests;
#[cfg(test)]
mod report_tests;
#[cfg(test)]
pub(crate) mod scripted;
#[cfg(test)]
mod settle_tests;
#[cfg(test)]
mod shared_async_end_tests;
#[cfg(test)]
mod shared_async_tests;
#[cfg(test)]
mod shared_pool_tests;
#[cfg(test)]
mod shared_rules_tests;
#[cfg(test)]
mod shared_thread_tests;
#[cfg(test)]
mod slot_defect_tests;
#[cfg(test)]
mod start_tests;
#[cfg(test)]
mod stop_and_loss_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod timeout_order_tests;
#[cfg(test)]
mod worker_children_tests;

pub use board::{
    ChildBaseline, ChildLayout, DownState, PoolBoard, PoolFailure, PoolHealth, ReadyChild,
    SlotPhase, StartEnd,
};
pub use budgets::{
    isolated_capacity, shared_capacity, BudgetCapacity, FreeCapacity, PoolService,
    SharedSlotService, SlotService,
};
pub use defects::{SlotDefect, SLOT_DEFECT_LIMIT};
pub use dispatch::{
    AttemptEnd, CancelEnd, Claimed, ClaimedTask, ExecutionClass, LoopKillCode, NotRunCause,
    RetryFacts, Settlement, TaskKind, TimeoutCancel,
};
#[cfg(test)]
pub(crate) use outcome::test_successes;
pub use outcome::{OrphanSelfHeal, OutcomeWriter};
pub use pool::{
    ExitNotSeen, IsolatedPool, IsolatedPoolConfig, MemoryLimit, PoolStopReport, Submitted,
    TaskLimit,
};
pub use route::{
    describe_claim_plan, dispatch_route, unserved_classes_line, ClaimPlan, ClaimRoute,
    DispatchRoute, IsolatedDeadline, UnservedClass,
};
pub use shared::{SharedPool, SharedPoolConfig};
pub use start::{
    check_memory_baselines, guard_new_kind_baselines, start_children, BaselineLimits,
    BaselineOverLimit, ChildrenStart, ChildrenStartFailed,
};
