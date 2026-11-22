//! The claim plan of a worker and the route of each claimed row. Pure.
//!
//! - A class that the worker runs children for is claimed into its own
//!   budget.
//! - A shared class that the worker runs no children for is claimed into the
//!   `ISOLATED` budget when the worker runs isolated children: a task may get
//!   more isolation than its row asks, never less.
//! - Any other class is not served.
//!
//! The budget that claimed a row decides the child kind. A shared row needs
//! its stored timeout, and a `SHARED_ASYNC` row also its cancel grace. A row
//! whose stored options give no usable value is refused on every worker: the
//! child sets it RUNNING and fails it with `WORKER_SERIALIZATION_ERROR`
//! before it resolves the task. A shared row on the `ISOLATED` budget gets
//! the refusal or the timeout of its seat work, so its outcome does not
//! depend on the child kind.

use std::time::Duration;

use super::dispatch::ExecutionClass;
use crate::worker::executor::protocol::{
    CancelGraceMs, IsolatedWork, RefusalReason, SharedWork, StoredOptionsRefusal, TaskTimeoutMs,
};
use crate::worker::retry::{
    grace_refusal_message, timeout_refusal_message, StoredCancelGrace, StoredOptions, StoredRetry,
    StoredTimeout,
};

/// Whether the worker runs children of one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildrenOfClass {
    Runs,
    None,
}

impl ChildrenOfClass {
    fn of_count(children: usize) -> Self {
        match children {
            0 => Self::None,
            _ => Self::Runs,
        }
    }
}

/// The budget that claims rows of a class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimRoute {
    ServedBy(ExecutionClass),
    NotServed,
}

/// The routes of a worker, from the classes it runs children for. Every
/// route follows from these three facts, so a plan cannot hold a route the
/// rules refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimPlan {
    isolated: ChildrenOfClass,
    shared_async: ChildrenOfClass,
    shared_thread: ChildrenOfClass,
}

impl ClaimPlan {
    /// The plan of a worker that runs these children.
    pub fn for_children(isolated: usize, shared_async: usize, shared_thread: usize) -> Self {
        Self {
            isolated: ChildrenOfClass::of_count(isolated),
            shared_async: ChildrenOfClass::of_count(shared_async),
            shared_thread: ChildrenOfClass::of_count(shared_thread),
        }
    }

    fn children_of(self, class: ExecutionClass) -> ChildrenOfClass {
        match class {
            ExecutionClass::Isolated => self.isolated,
            ExecutionClass::SharedAsync => self.shared_async,
            ExecutionClass::SharedThread => self.shared_thread,
        }
    }

    /// The budget that claims rows of `class`.
    pub fn route(self, class: ExecutionClass) -> ClaimRoute {
        match (self.children_of(class), class, self.isolated) {
            (ChildrenOfClass::Runs, _, _) => ClaimRoute::ServedBy(class),
            (ChildrenOfClass::None, ExecutionClass::Isolated, _) => ClaimRoute::NotServed,
            (
                ChildrenOfClass::None,
                ExecutionClass::SharedAsync | ExecutionClass::SharedThread,
                ChildrenOfClass::Runs,
            ) => ClaimRoute::ServedBy(ExecutionClass::Isolated),
            (
                ChildrenOfClass::None,
                ExecutionClass::SharedAsync | ExecutionClass::SharedThread,
                ChildrenOfClass::None,
            ) => ClaimRoute::NotServed,
        }
    }

    /// The budget classes, in the order of [`ExecutionClass::ALL`].
    pub fn budgets(self) -> Vec<ExecutionClass> {
        ExecutionClass::ALL
            .into_iter()
            .filter(|class| self.children_of(*class) == ChildrenOfClass::Runs)
            .collect()
    }

    /// The classes whose rows the budget of `budget` claims.
    pub fn served_by(self, budget: ExecutionClass) -> Vec<ExecutionClass> {
        ExecutionClass::ALL
            .into_iter()
            .filter(|class| self.route(*class) == ClaimRoute::ServedBy(budget))
            .collect()
    }
}

/// One line that names the route of each class, for the start log (Python
/// `describe_claim_plan`): `ISOLATED -> ISOLATED budget, SHARED_ASYNC -> not
/// served, ...`. Pure.
pub fn describe_claim_plan(plan: ClaimPlan) -> String {
    ExecutionClass::ALL
        .into_iter()
        .map(|class| match plan.route(class) {
            ClaimRoute::ServedBy(budget) => format!("{class} -> {budget} budget"),
            ClaimRoute::NotServed => format!("{class} -> not served"),
        })
        .collect::<Vec<String>>()
        .join(", ")
}

/// The start log line of the classes of which the worker claims no row, or
/// none when it serves every class (Python `_log_claim_plan`). A worker is
/// not refused for them. Pure.
pub fn unserved_classes_line(plan: ClaimPlan) -> Option<String> {
    let unserved: Vec<&'static str> = ExecutionClass::ALL
        .into_iter()
        .filter(|class| plan.route(*class) == ClaimRoute::NotServed)
        .map(ExecutionClass::as_str)
        .collect();
    match unserved.is_empty() {
        true => None,
        false => Some(format!(
            "This worker claims no rows of class {}: it runs no children that can run them",
            unserved.join(", ")
        )),
    }
}

/// The deadline an isolated parent holds for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolatedDeadline {
    NoDeadline,
    After(TaskTimeoutMs),
}

/// The kind of child that runs a claimed row, and what it does with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchRoute {
    /// An isolated child runs the task, or its refusal; the parent holds
    /// the deadline.
    Isolated {
        work: IsolatedWork,
        deadline: IsolatedDeadline,
    },
    /// A seat of a shared async child: `AsyncTask` or `Refusal`.
    SharedAsync(SharedWork),
    /// A seat of a shared thread child: `ThreadTask` or `Refusal`.
    SharedThread(SharedWork),
}

/// The claim took a row of a class this worker does not serve. The claim
/// reads only served classes, so this names a claim that disagrees with
/// the plan; the row goes back to the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a {class} row was claimed by a worker that does not serve that class")]
pub struct UnservedClass {
    pub class: ExecutionClass,
}

/// The route of a claimed row of class `class`.
pub fn dispatch_route(
    class: ExecutionClass,
    plan: ClaimPlan,
    options: &StoredOptions,
) -> Result<DispatchRoute, UnservedClass> {
    match plan.route(class) {
        ClaimRoute::NotServed => Err(UnservedClass { class }),
        ClaimRoute::ServedBy(ExecutionClass::SharedAsync) => Ok(DispatchRoute::SharedAsync(
            match shared_async_work(options) {
                Ok((timeout_ms, cancel_grace_ms)) => SharedWork::AsyncTask {
                    timeout_ms,
                    cancel_grace_ms,
                },
                Err(refusal) => SharedWork::Refusal(refusal),
            },
        )),
        ClaimRoute::ServedBy(ExecutionClass::SharedThread) => Ok(DispatchRoute::SharedThread(
            match shared_thread_work(options) {
                Ok(timeout_ms) => SharedWork::ThreadTask { timeout_ms },
                Err(refusal) => SharedWork::Refusal(refusal),
            },
        )),
        ClaimRoute::ServedBy(ExecutionClass::Isolated) => Ok(isolated_route(class, options)),
    }
}

fn refusal_reason(text: &str) -> RefusalReason {
    RefusalReason::cut(text)
}

/// The timeout of usable options, or the refusal of options the worker
/// cannot use. Unreadable options and a retry policy that does not fit its
/// rule refuse the run as unreadable options; a timeout that does not fit
/// its rule refuses it as an invalid timeout.
fn usable_timeout(options: &StoredOptions) -> Result<Option<TaskTimeoutMs>, StoredOptionsRefusal> {
    match options {
        StoredOptions::Unreadable { reason } => Err(StoredOptionsRefusal::OptionsUnreadable(
            refusal_reason(reason),
        )),
        StoredOptions::Readable {
            retry: StoredRetry::Invalid { reason },
            ..
        } => Err(StoredOptionsRefusal::OptionsUnreadable(refusal_reason(
            reason,
        ))),
        StoredOptions::Readable {
            timeout: StoredTimeout::Invalid(refusal),
            ..
        } => Err(StoredOptionsRefusal::TimeoutValueInvalid(refusal_reason(
            &timeout_refusal_message(*refusal),
        ))),
        StoredOptions::Readable {
            timeout: StoredTimeout::NoTimeout,
            ..
        } => Ok(None),
        StoredOptions::Readable {
            timeout: StoredTimeout::Limit(limit),
            ..
        } => Ok(Some(timeout_ms(*limit))),
    }
}

/// A stored limit as the bounded type. The stored options reader keeps a
/// limit inside the bounds, so another value is a defect.
fn timeout_ms(limit: Duration) -> TaskTimeoutMs {
    let millis = u64::try_from(limit.as_millis()).unwrap_or(u64::MAX);
    TaskTimeoutMs::new(millis)
        .expect("the stored options reader keeps a timeout inside the task timeout bounds")
}

/// A stored grace as the bounded type, under the same rule.
fn grace_ms(grace: Duration) -> CancelGraceMs {
    let millis = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
    CancelGraceMs::new(millis)
        .expect("the stored options reader keeps a cancel grace inside its bounds")
}

fn absent_timeout(class: &str) -> StoredOptionsRefusal {
    StoredOptionsRefusal::TimeoutValueInvalid(refusal_reason(&format!(
        "stored timeout_ms is absent for execution {class}"
    )))
}

/// The work of a `SHARED_ASYNC` row: the task with its stored timeout and
/// grace, or the refusal of the first one the worker cannot use.
fn shared_async_work(
    options: &StoredOptions,
) -> Result<(TaskTimeoutMs, CancelGraceMs), StoredOptionsRefusal> {
    let Some(timeout) = usable_timeout(options)? else {
        return Err(absent_timeout("SHARED_ASYNC"));
    };
    match options.cancel_grace() {
        StoredCancelGrace::Grace(grace) => Ok((timeout, grace_ms(grace))),
        StoredCancelGrace::NotStored => Err(StoredOptionsRefusal::CancelGraceInvalid(
            refusal_reason("stored cancel_grace_ms is absent for execution SHARED_ASYNC"),
        )),
        StoredCancelGrace::Invalid(refusal) => Err(StoredOptionsRefusal::CancelGraceInvalid(
            refusal_reason(&grace_refusal_message(refusal)),
        )),
    }
}

/// The work of a `SHARED_THREAD` row: the task with its stored timeout, or
/// the refusal of a timeout the worker cannot use.
fn shared_thread_work(options: &StoredOptions) -> Result<TaskTimeoutMs, StoredOptionsRefusal> {
    match usable_timeout(options)? {
        Some(timeout) => Ok(timeout),
        None => Err(absent_timeout("SHARED_THREAD")),
    }
}

/// The route of a row on the `ISOLATED` budget. A shared row gets the
/// timeout or the refusal of its seat work.
fn isolated_route(class: ExecutionClass, options: &StoredOptions) -> DispatchRoute {
    let deadline = match class {
        ExecutionClass::Isolated => usable_timeout(options).map(|timeout| match timeout {
            Some(timeout) => IsolatedDeadline::After(timeout),
            None => IsolatedDeadline::NoDeadline,
        }),
        ExecutionClass::SharedAsync => {
            shared_async_work(options).map(|(timeout, _)| IsolatedDeadline::After(timeout))
        }
        ExecutionClass::SharedThread => shared_thread_work(options).map(IsolatedDeadline::After),
    };
    match deadline {
        Ok(deadline) => DispatchRoute::Isolated {
            work: IsolatedWork::Run,
            deadline,
        },
        Err(refusal) => DispatchRoute::Isolated {
            work: IsolatedWork::Refusal(refusal),
            deadline: IsolatedDeadline::NoDeadline,
        },
    }
}
