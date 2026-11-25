//! Calls from the core into JavaScript: the callback primitive.
//!
//! - One threadsafe function per registration, built on the JavaScript
//!   thread from the package's wrapper of a JavaScript function
//!   ([`JsCallback::register`]). It is weak: a registration that lives as
//!   long as a handle does not keep the event loop alive; an operation keeps
//!   the loop alive through its own Promise.
//! - A call uses the mode `NonBlocking` with a queue bound per callback
//!   kind. A full queue, or an environment that is closing, is the transient
//!   failure `NotRun`: the caller's step rolls back and runs again.
//! - The settle form: the call passes a call id and the argument. The
//!   package's wrapper runs the user function, sync or async, and calls
//!   `settleCallbackReturned(callId, value)` or
//!   `settleCallbackThrew(callId, name, message)`. A table of pending call
//!   ids, bounded by [`MAX_PENDING_CALLBACKS`], holds the senders; the end of
//!   a call (an answer, a drop of its future) removes its entry.
//! - The wrapper never throws into Node-API: with `CalleeHandled = false` a
//!   throw there ends the process. A thrown value stays in the package's
//!   table under its call id; Rust gets `Threw { call_id, name, message }`.
//! - The answer is read with its `FromNapiValue` on the JavaScript thread,
//!   in the settle call. An answer that does not read is `AnswerRefused`, a
//!   defect of the package.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};

use napi::bindgen_prelude::{sys, BigInt, FnArgs, FromNapiValue, Function, Object, ToNapiValue};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, JsValue, Status, Unknown};
use napi_derive::napi;
use tokio::sync::oneshot;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::spawn_operation;
use crate::call::thrown::Envelope;
use crate::process_core::environment::environment_closing;
use crate::process_core::panic_rule::take_panic_cause;
use crate::values::boundary::{bigint_of_u64, u64_of_bigint};

/// Most calls into JavaScript that wait for their settle call, over every
/// registration of the process.
pub const MAX_PENDING_CALLBACKS: usize = 65_536;

/// The queue bound of the registration of `probeCallback`: one call.
const PROBE_QUEUE_BOUND: usize = 1;

/// Why a call into JavaScript was not run. Both clear: the caller may run
/// its step again.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotRunCause {
    /// The queue of the registration, or the table of pending calls, is
    /// full.
    QueueFull,
    /// The JavaScript environment is closing.
    EnvClosing,
}

/// Why a call into JavaScript gave no answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallbackFailure {
    /// The call did not reach JavaScript, or its answer cannot come. It
    /// clears.
    NotRun { cause: NotRunCause },
    /// The JavaScript function threw. The thrown value stays in the
    /// package's table under `call_id`.
    Threw {
        call_id: u64,
        name: String,
        message: String,
    },
    /// The Rust read of the answer panicked.
    Panicked { cause: String },
    /// The answer does not read as the answer type: a defect of the package.
    AnswerRefused { cause: String },
}

/// The threadsafe function of one registration: weak, never throws into
/// Node-API, at most `QUEUE` calls waiting for the JavaScript thread.
pub type CallbackFunction<Argument, const QUEUE: usize> = ThreadsafeFunction<
    FnArgs<(BigInt, Argument)>,
    (),
    FnArgs<(BigInt, Argument)>,
    Status,
    false,
    true,
    QUEUE,
>;

/// One registration of a JavaScript function that the core calls with an
/// `Argument` and that answers an `Answer`.
pub struct JsCallback<Argument: ToNapiValue + 'static, Answer, const QUEUE: usize> {
    function: CallbackFunction<Argument, QUEUE>,
    answer: PhantomData<fn() -> Answer>,
}

impl<Argument, Answer, const QUEUE: usize> JsCallback<Argument, Answer, QUEUE>
where
    Argument: ToNapiValue + 'static,
    Answer: FromNapiValue + Send + 'static,
{
    /// Register the package's wrapper `function`. On the JavaScript thread.
    ///
    /// Failure exits: the `napi::Error` of a function that Node-API cannot
    /// turn into a threadsafe function.
    pub fn register(function: Function<FnArgs<(BigInt, Argument)>, ()>) -> napi::Result<Self> {
        let function = function
            .build_threadsafe_function::<FnArgs<(BigInt, Argument)>>()
            .callee_handled::<false>()
            .weak::<true>()
            .max_queue_size::<QUEUE>()
            .build()?;
        Ok(Self {
            function,
            answer: PhantomData,
        })
    }

    /// Call the function with `argument` and wait for its settle call. On a
    /// core thread. The wait has no bound of its own: a caller that needs one
    /// drops the future at its bound, and a late settle call then finds no
    /// pending call.
    pub async fn call(&self, argument: Argument) -> Result<Answer, CallbackFailure> {
        if environment_closing() {
            return Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing,
            });
        }
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        let _pending = PendingCall::insert(call_id, settler::<Answer>(call_id, sender))?;
        let status = self.function.call(
            FnArgs::from((bigint_of_u64(call_id), argument)),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
        if let Some(cause) = not_run_cause(status) {
            return Err(CallbackFailure::NotRun { cause });
        }
        match receiver.await {
            Ok(answer) => answer,
            // The settler was dropped without a settle call: the environment
            // dropped the call at its end.
            Err(_dropped) => Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing,
            }),
        }
    }
}

/// The cause of a call that Node-API did not queue, or `None` for a queued
/// call. A status other than a full queue or a closing environment means the
/// threadsafe function is no longer usable: it is closing. Pure.
fn not_run_cause(status: Status) -> Option<NotRunCause> {
    match status {
        Status::Ok => None,
        Status::QueueFull => Some(NotRunCause::QueueFull),
        _closing_or_refused => Some(NotRunCause::EnvClosing),
    }
}

/// What the settle call of the package brings.
enum Settlement {
    /// The function returned, or its Promise resolved, with `value`, read on
    /// the JavaScript thread of `env`.
    Returned {
        env: sys::napi_env,
        value: sys::napi_value,
    },
    Threw {
        name: String,
        message: String,
    },
}

/// Hands the settlement of one call to its waiting core task.
type Settler = Box<dyn FnOnce(Settlement) + Send>;

static PENDING: LazyLock<Mutex<HashMap<u64, Settler>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static NEXT_CALL_ID: AtomicU64 = AtomicU64::new(1);

/// The entry of one call in the table of pending calls; the drop removes it.
struct PendingCall {
    call_id: u64,
}

impl PendingCall {
    /// Add the entry, or refuse with `NotRun { QueueFull }` at the bound.
    fn insert(call_id: u64, settler: Settler) -> Result<Self, CallbackFailure> {
        let mut table = PENDING.lock().unwrap_or_else(PoisonError::into_inner);
        if table.len() >= MAX_PENDING_CALLBACKS {
            return Err(CallbackFailure::NotRun {
                cause: NotRunCause::QueueFull,
            });
        }
        let _no_earlier_entry = table.insert(call_id, settler);
        Ok(Self { call_id })
    }
}

impl Drop for PendingCall {
    fn drop(&mut self) {
        let _settled_or_left = PENDING
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.call_id);
    }
}

/// The settler of a call that answers an `Answer`.
fn settler<Answer: FromNapiValue + Send + 'static>(
    call_id: u64,
    sender: oneshot::Sender<Result<Answer, CallbackFailure>>,
) -> Settler {
    Box::new(move |settlement| {
        let answer = match settlement {
            Settlement::Returned { env, value } => read_answer::<Answer>(env, value),
            Settlement::Threw { name, message } => Err(CallbackFailure::Threw {
                call_id,
                name,
                message,
            }),
        };
        // The caller may have dropped its future; the answer then has no
        // reader.
        let _no_reader = sender.send(answer);
    })
}

/// Read the answer on the JavaScript thread. A panic of the read is caught:
/// it must not unwind through Node-API.
fn read_answer<Answer: FromNapiValue>(
    env: sys::napi_env,
    value: sys::napi_value,
) -> Result<Answer, CallbackFailure> {
    // SAFETY: `env` and `value` come from the settle call that runs now, on
    // the JavaScript thread of `env`.
    let read = std::panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        Answer::from_napi_value(env, value)
    }));
    match read {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(error)) => Err(CallbackFailure::AnswerRefused {
            cause: error.to_string(),
        }),
        Err(payload) => Err(CallbackFailure::Panicked {
            cause: take_panic_cause(payload.as_ref()),
        }),
    }
}

/// How a settle call ended.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettleOutcome {
    /// The waiting core task got the answer.
    Settled,
    /// No call waits under the id: it ended (its caller dropped it at its
    /// bound), or the id is not one of this process.
    UnknownCall,
}

/// Hand `settlement` to the call `call_id`. The lock is released before the
/// settler runs.
fn settle(call_id: &BigInt, settlement: Settlement) -> Envelope<SettleOutcome> {
    let call_id = match u64_of_bigint("callId", call_id) {
        Ok(call_id) => call_id,
        Err(refusal) => return Envelope::Thrown(refusal.into()),
    };
    let settler = PENDING
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&call_id);
    match settler {
        Some(settler) => {
            settler(settlement);
            Envelope::Value(SettleOutcome::Settled)
        }
        None => Envelope::Value(SettleOutcome::UnknownCall),
    }
}

/// The function of the call `callId` returned `value`, or its Promise
/// resolved with it.
#[napi(
    ts_args_type = "callId: bigint, value: unknown",
    ts_return_type = "NativeEnvelope<SettleOutcome>"
)]
pub fn settle_callback_returned(call_id: BigInt, value: Unknown) -> Envelope<SettleOutcome> {
    sync_call("settle_callback_returned", || {
        let raw = value.value();
        settle(
            &call_id,
            Settlement::Returned {
                env: raw.env,
                value: raw.value,
            },
        )
    })
}

/// The function of the call `callId` threw, or its Promise rejected. The
/// thrown value stays in the package.
#[napi(ts_return_type = "NativeEnvelope<SettleOutcome>")]
pub fn settle_callback_threw(
    call_id: BigInt,
    name: String,
    message: String,
) -> Envelope<SettleOutcome> {
    sync_call("settle_callback_threw", || {
        settle(&call_id, Settlement::Threw { name, message })
    })
}

/// How one call of `probeCallback` ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallbackProbe {
    Answered {
        answer: String,
    },
    NotRun {
        cause: NotRunCause,
    },
    Threw {
        call_id: BigInt,
        name: String,
        message: String,
    },
    Panicked {
        cause: String,
    },
    AnswerRefused {
        cause: String,
    },
}

impl CallbackProbe {
    /// The record of one call's end. Pure.
    fn of(ended: Result<String, CallbackFailure>) -> Self {
        match ended {
            Ok(answer) => Self::Answered { answer },
            Err(CallbackFailure::NotRun { cause }) => Self::NotRun { cause },
            Err(CallbackFailure::Threw {
                call_id,
                name,
                message,
            }) => Self::Threw {
                call_id: bigint_of_u64(call_id),
                name,
                message,
            },
            Err(CallbackFailure::Panicked { cause }) => Self::Panicked { cause },
            Err(CallbackFailure::AnswerRefused { cause }) => Self::AnswerRefused { cause },
        }
    }
}

/// Register `callback` and call it once from a core thread with `argument`;
/// resolve with how the call ended. It checks the callback primitive from
/// JavaScript; the package does not call it. Starts the core.
#[napi(ts_return_type = "Promise<NativeEnvelope<CallbackProbe>>")]
pub fn probe_callback<'env>(
    env: &'env Env,
    callback: Function<FnArgs<(BigInt, String)>, ()>,
    argument: String,
) -> napi::Result<Object<'env>> {
    let registered = JsCallback::<String, String, PROBE_QUEUE_BOUND>::register(callback)?;
    spawn_operation(
        env,
        "probe_callback",
        CallKind::Read,
        move |_core| async move { CallbackProbe::of(registered.call(argument).await) },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_queued_call_has_no_not_run_cause() {
        assert_eq!(not_run_cause(Status::Ok), None);
    }

    #[test]
    fn a_full_queue_is_not_run_queue_full() {
        assert_eq!(
            not_run_cause(Status::QueueFull),
            Some(NotRunCause::QueueFull)
        );
    }

    #[test]
    fn a_closing_function_is_not_run_env_closing() {
        assert_eq!(
            not_run_cause(Status::Closing),
            Some(NotRunCause::EnvClosing)
        );
        assert_eq!(
            not_run_cause(Status::GenericFailure),
            Some(NotRunCause::EnvClosing)
        );
    }

    #[test]
    fn a_throw_reaches_the_waiting_task_with_its_call_id() {
        let (sender, mut receiver) = oneshot::channel::<Result<String, CallbackFailure>>();
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let pending = PendingCall::insert(call_id, settler::<String>(call_id, sender))
            .expect("the table has room");
        assert_eq!(
            settle(
                &bigint_of_u64(call_id),
                Settlement::Threw {
                    name: "TypeError".to_owned(),
                    message: "x is not a function".to_owned(),
                },
            ),
            Envelope::Value(SettleOutcome::Settled)
        );
        assert_eq!(
            receiver.try_recv().expect("the answer arrived"),
            Err(CallbackFailure::Threw {
                call_id,
                name: "TypeError".to_owned(),
                message: "x is not a function".to_owned(),
            })
        );
        drop(pending);
    }

    #[test]
    fn a_settle_call_of_an_ended_call_finds_no_pending_call() {
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, _receiver) = oneshot::channel::<Result<String, CallbackFailure>>();
        let pending = PendingCall::insert(call_id, settler::<String>(call_id, sender))
            .expect("the table has room");
        drop(pending);
        assert_eq!(
            settle(
                &bigint_of_u64(call_id),
                Settlement::Threw {
                    name: "Error".to_owned(),
                    message: "late".to_owned(),
                },
            ),
            Envelope::Value(SettleOutcome::UnknownCall)
        );
    }

    #[test]
    fn a_negative_call_id_is_a_client_defect() {
        let envelope = settle(
            &BigInt::from(-1_i128),
            Settlement::Threw {
                name: "Error".to_owned(),
                message: "x".to_owned(),
            },
        );
        assert!(
            matches!(
                envelope,
                Envelope::Thrown(crate::call::thrown::NativeThrown::ClientDefect { ref subject, .. })
                    if subject == "callId"
            ),
            "{envelope:?}"
        );
    }

    #[test]
    fn each_end_of_a_call_has_its_probe_record() {
        assert_eq!(
            CallbackProbe::of(Ok("pong".to_owned())),
            CallbackProbe::Answered {
                answer: "pong".to_owned()
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing
            })),
            CallbackProbe::NotRun {
                cause: NotRunCause::EnvClosing
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::Threw {
                call_id: u64::MAX,
                name: "Error".to_owned(),
                message: "boom".to_owned(),
            })),
            CallbackProbe::Threw {
                call_id: BigInt::from(u64::MAX),
                name: "Error".to_owned(),
                message: "boom".to_owned(),
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::Panicked {
                cause: "boom".to_owned()
            })),
            CallbackProbe::Panicked {
                cause: "boom".to_owned()
            }
        );
        assert_eq!(
            CallbackProbe::of(Err(CallbackFailure::AnswerRefused {
                cause: "not a string".to_owned()
            })),
            CallbackProbe::AnswerRefused {
                cause: "not a string".to_owned()
            }
        );
    }
}
