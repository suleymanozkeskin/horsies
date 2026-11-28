//! The start hooks of a JavaScript child: the app's providers, then its
//! `onChildStart` hooks, as the core's named start hooks.
//!
//! - The core runs the hooks one after the other, in the order of the plan,
//!   each with the child's start facts and for at most
//!   `CHILD_START_HOOK_BOUND` (10 s). The bound is kept by the core's thread,
//!   so it holds for a hook that blocks the JavaScript thread too.
//! - Each hook calls the package's hook dispatcher through the callback
//!   primitive with the hook's index and the start facts, and waits for its
//!   settle call. A throw or a rejection of the hook, a call that does not
//!   run, and an answer that does not read are each a hook failure: the core
//!   ends the child with exit code 173, runs no later hook, and the worker
//!   stops with `CHILD_HOOK_FAILED`. The logged cause names the hook.

use std::sync::Arc;

use std::num::NonZeroU64;

use horsies_core::frame_protocol::MemoryLimit;
use horsies_core::{
    ChildHookFailure, ChildStartFacts, ChildStartFuture, ChildStartHook, NamedChildStartHook,
};
use napi_derive::napi;

use crate::call::callback::{CallbackFailure, CallbackWait, JsCallback, NotRunCause};

/// The calls of the hook dispatcher that wait for the JavaScript thread at
/// once: the core runs one hook at a time.
pub const START_HOOK_QUEUE_BOUND: usize = 1;

/// The wait of a hook call. A start hook is user code of any duration; the
/// call ends at its answer or at the end of the environment.
const START_HOOK_WAIT: CallbackWait = CallbackWait::NoBound;

/// The facts of the child that a start hook runs in.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartFactsRecord {
    /// An isolated child: one run at a time.
    Isolated,
    /// A shared async child with `seats` runs at once.
    SharedAsync { seats: u32 },
    /// A shared thread child with `seats` runs at once. Its task sends
    /// share `task_send_pool` connections (1 to 1,000); `recycle_above` is
    /// its resident-memory limit.
    SharedThread {
        seats: u32,
        task_send_pool: u32,
        recycle_above: MemoryLimitRecord,
    },
    /// The handler child.
    Handler,
}

/// The resident-memory limit of a child (`recycle.above` of the profile).
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryLimitRecord {
    /// No limit.
    Off,
    /// The limit is `mb` MiB, 1 to `u32::MAX`.
    AtMb { mb: u32 },
}

impl MemoryLimitRecord {
    /// The record of the core's limit. Pure.
    fn of(limit: MemoryLimit) -> Self {
        match limit {
            MemoryLimit::Off => Self::Off,
            MemoryLimit::AtMb(mb) => Self::AtMb {
                mb: limit_megabytes(mb),
            },
        }
    }
}

impl StartFactsRecord {
    /// The record of the core's facts. Pure.
    fn of(facts: ChildStartFacts) -> Self {
        match facts {
            ChildStartFacts::Isolated => Self::Isolated,
            ChildStartFacts::SharedAsync { seats } => Self::SharedAsync {
                seats: seat_count(seats.get()),
            },
            ChildStartFacts::SharedThread {
                seats,
                task_send_pool,
                recycle_above,
            } => Self::SharedThread {
                seats: seat_count(seats.get()),
                task_send_pool: pool_connections(task_send_pool.get()),
                recycle_above: MemoryLimitRecord::of(recycle_above),
            },
            ChildStartFacts::Handler => Self::Handler,
        }
    }
}

/// A seat count of the core: 1 to 1,000, so it fits a `u32`.
fn seat_count(seats: u64) -> u32 {
    u32::try_from(seats).unwrap_or_else(|_| panic!("a seat count is 1 to 1,000, not {seats}"))
}

/// A task-send pool of the core: 1 to 1,000 connections, so it fits a
/// `u32`.
fn pool_connections(connections: u64) -> u32 {
    u32::try_from(connections)
        .unwrap_or_else(|_| panic!("a task-send pool is 1 to 1,000 connections, not {connections}"))
}

/// A memory limit of the core's start facts: the init frame decoder takes
/// 1 to `u32::MAX` MiB, so it fits a `u32`.
fn limit_megabytes(mb: NonZeroU64) -> u32 {
    u32::try_from(mb.get())
        .unwrap_or_else(|_| panic!("a memory limit is 1 to u32::MAX MiB, not {mb}"))
}

/// One call of the hook dispatcher: the index of the hook in the plan, and
/// the facts of the child.
#[napi(object, object_from_js = false)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartHookCall {
    pub index: u32,
    pub facts: StartFactsRecord,
}

/// The answer of a hook that ended: its function returned, or its Promise
/// resolved.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartHookEnd {
    Ended,
}

/// The hook dispatcher of the package.
pub type HookDispatcher = JsCallback<StartHookCall, StartHookEnd, START_HOOK_QUEUE_BOUND>;

/// The core's start hooks of `names`, in order: hook `index` calls the
/// dispatcher with its index. No name is `NoHook`. Pure.
pub fn start_hooks(names: Vec<String>, dispatcher: &Arc<HookDispatcher>) -> ChildStartHook {
    match names.is_empty() {
        true => ChildStartHook::NoHook,
        false => ChildStartHook::Run(
            names
                .into_iter()
                .enumerate()
                .map(|(index, name)| named_hook(index, name, Arc::clone(dispatcher)))
                .collect(),
        ),
    }
}

/// The core's hook of the hook `index`, named `name`.
fn named_hook(index: usize, name: String, dispatcher: Arc<HookDispatcher>) -> NamedChildStartHook {
    let index = u32::try_from(index)
        .unwrap_or_else(|_| panic!("a plan holds fewer than 2^32 start hooks, not {index}"));
    NamedChildStartHook {
        name,
        start: Box::new(move |facts: ChildStartFacts| -> ChildStartFuture {
            Box::pin(async move {
                let call = StartHookCall {
                    index,
                    facts: StartFactsRecord::of(facts),
                };
                dispatcher
                    .call(call, START_HOOK_WAIT)
                    .await
                    .map(|_ended: StartHookEnd| ())
                    .map_err(hook_failure)
            })
        }),
    }
}

/// The core's failure of a hook call that did not end. Pure.
fn hook_failure(failure: CallbackFailure) -> ChildHookFailure {
    let cause = match failure {
        CallbackFailure::NoAnswer { bound } => {
            format!("the hook gave no answer within {bound:?}")
        }
        CallbackFailure::Threw { name, message, .. } => format!("{name}: {message}"),
        CallbackFailure::NotRun {
            cause: NotRunCause::QueueFull,
        } => "the hook did not run: the queue of the JavaScript thread is full".to_owned(),
        CallbackFailure::NotRun {
            cause: NotRunCause::EnvClosing,
        } => "the hook did not run: the JavaScript environment is closing".to_owned(),
        CallbackFailure::Panicked { cause } => {
            format!("the read of the hook's answer panicked: {cause}")
        }
        CallbackFailure::AnswerRefused { cause } => {
            format!("the hook's answer does not read: {cause}")
        }
        CallbackFailure::NodeApiRefused { status } => {
            format!("Node-API refused the hook call: {status}")
        }
        CallbackFailure::OnJavaScriptThread => {
            "the hook was called on the JavaScript thread, which must run it".to_owned()
        }
    };
    ChildHookFailure { cause }
}

#[cfg(test)]
mod tests {
    use horsies_core::frame_protocol::{AsyncSeats, TaskSendPool, ThreadSeats};

    use super::*;

    #[test]
    fn a_throw_keeps_the_name_and_the_message() {
        assert_eq!(
            hook_failure(CallbackFailure::Threw {
                call_id: 1,
                name: "TypeError".to_owned(),
                message: "the pool did not open".to_owned(),
            })
            .cause,
            "TypeError: the pool did not open"
        );
    }

    #[test]
    fn each_other_failure_names_its_cause() {
        for (failure, part) in [
            (
                CallbackFailure::NotRun {
                    cause: NotRunCause::QueueFull,
                },
                "queue",
            ),
            (
                CallbackFailure::NotRun {
                    cause: NotRunCause::EnvClosing,
                },
                "closing",
            ),
            (
                CallbackFailure::NoAnswer {
                    bound: std::time::Duration::from_secs(3),
                },
                "no answer within 3s",
            ),
            (
                CallbackFailure::Panicked {
                    cause: "boom".to_owned(),
                },
                "panicked",
            ),
            (
                CallbackFailure::AnswerRefused {
                    cause: "not a string".to_owned(),
                },
                "does not read",
            ),
            (
                CallbackFailure::NodeApiRefused {
                    status: napi::Status::GenericFailure,
                },
                "Node-API",
            ),
            (CallbackFailure::OnJavaScriptThread, "JavaScript thread"),
        ] {
            let cause = hook_failure(failure).cause;
            assert!(cause.contains(part), "{cause}");
        }
    }

    #[test]
    fn each_start_fact_has_its_record() {
        assert_eq!(
            StartFactsRecord::of(ChildStartFacts::Isolated),
            StartFactsRecord::Isolated
        );
        assert_eq!(
            StartFactsRecord::of(ChildStartFacts::Handler),
            StartFactsRecord::Handler
        );
        assert_eq!(
            StartFactsRecord::of(ChildStartFacts::SharedAsync {
                seats: AsyncSeats::new(50).expect("50 async seats"),
            }),
            StartFactsRecord::SharedAsync { seats: 50 }
        );
    }

    fn shared_thread_facts(recycle_above: MemoryLimit) -> ChildStartFacts {
        ChildStartFacts::SharedThread {
            seats: ThreadSeats::new(4).expect("4 thread seats"),
            task_send_pool: TaskSendPool::new(3).expect("3 is a task-send pool"),
            recycle_above,
        }
    }

    #[test]
    fn a_shared_thread_record_has_the_seats_and_the_task_send_pool() {
        let StartFactsRecord::SharedThread {
            seats,
            task_send_pool,
            ..
        } = StartFactsRecord::of(shared_thread_facts(MemoryLimit::Off))
        else {
            panic!("a shared thread child has the shared thread record");
        };
        assert_eq!((seats, task_send_pool), (4, 3));
    }

    #[test]
    fn no_memory_limit_is_its_own_variant() {
        assert_eq!(
            StartFactsRecord::of(shared_thread_facts(MemoryLimit::Off)),
            StartFactsRecord::SharedThread {
                seats: 4,
                task_send_pool: 3,
                recycle_above: MemoryLimitRecord::Off,
            }
        );
    }

    #[test]
    fn a_memory_limit_keeps_its_megabytes() {
        let limit = NonZeroU64::new(1536).expect("1536 is above 0");
        assert_eq!(
            StartFactsRecord::of(shared_thread_facts(MemoryLimit::AtMb(limit))),
            StartFactsRecord::SharedThread {
                seats: 4,
                task_send_pool: 3,
                recycle_above: MemoryLimitRecord::AtMb { mb: 1536 },
            }
        );
        let largest = NonZeroU64::new(u64::from(u32::MAX)).expect("u32::MAX is above 0");
        assert_eq!(
            MemoryLimitRecord::of(MemoryLimit::AtMb(largest)),
            MemoryLimitRecord::AtMb { mb: u32::MAX }
        );
    }
}
