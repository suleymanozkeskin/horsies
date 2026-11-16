//! Slots: the children that run tasks, the budget of each class, and the
//! end of each attempt.
//!
//! - [`dispatch`]: the claimed task the executor takes, and [`AttemptEnd`],
//!   the one closed type for how an attempt ended.
//! - [`route`]: the claim plan and the route of each claimed row.
//! - [`budgets`]: the capacity in service of each budget.
//! - [`pool`] and [`isolated`]: the isolated children and their slots.
//! - [`outcome`]: the hand-off of each end to the existing outcome writers.
//! - [`shared_stop`] and [`seat_runs`]: the rules of shared children: the
//!   stop report of each run, the verdict of each run at a kill, the
//!   backstops, placement and recycle.

pub mod budgets;
pub mod dispatch;
pub mod isolated;
pub mod outcome;
pub mod pool;
pub mod route;
pub mod seat_runs;
pub mod shared_stop;

#[cfg(test)]
mod shared_rules_tests;
#[cfg(test)]
mod tests;

pub use budgets::{
    isolated_capacity, shared_capacity, BudgetCapacity, PoolService, SharedSlotService, SlotService,
};
pub use dispatch::{
    AttemptEnd, Claimed, ClaimedTask, ExecutionClass, LoopKillCode, NotRunCause, RetryFacts,
    Settlement, TaskKind,
};
pub use outcome::OutcomeWriter;
pub use pool::{
    DownState, ExitNotSeen, IsolatedPool, IsolatedPoolConfig, IsolatedSlotState, MemoryLimit,
    PoolFailure, PoolHealth, PoolStopReport, Submitted, TaskLimit,
};
pub use route::{
    dispatch_route, ClaimPlan, ClaimRoute, DispatchRoute, IsolatedDeadline, UnservedClass,
};
