//! Calls from the core into JavaScript: the callback primitive.
//!
//! - One threadsafe function per registration, built on the JavaScript
//!   thread from the package's wrapper of a JavaScript function
//!   ([`JsCallback::register`]). It is weak: a registration that lives as
//!   long as a handle does not keep the event loop alive; an operation keeps
//!   the loop alive through its own Promise.
//! - A call uses the mode `NonBlocking` with a queue bound per callback
//!   kind. A full queue, or an environment that is closing, is the transient
//!   failure `NotRun`: the caller's step rolls back and runs again. Any other
//!   status of Node-API is the defect `NodeApiRefused` (`call::node_api`).
//! - A call on the JavaScript thread of its registration is refused
//!   (`OnJavaScriptThread`): that thread must run the function, so a
//!   blocking wait there could never end.
//! - At the end of the environment, the cleanup hook ends every pending
//!   call ([`end_pending_calls`]): each waiting call answers
//!   `NotRun { envClosing }`.
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
//! - The take form ([`JsCallback::start_call`]): the package's wrapper calls
//!   `takeCallback(callId)` when the JavaScript thread takes the call, before
//!   it runs the function. A caller can bound the wait for the JavaScript
//!   thread with [`TakenSignal`] and wait for the answer with no bound. A
//!   settle call, a drop of the call and the end of the environment end the
//!   taken signal too, so it never waits past the end of the call.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::thread::ThreadId;

use napi::bindgen_prelude::{sys, BigInt, FnArgs, FromNapiValue, Function, ToNapiValue};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{JsValue, Status, Unknown};
use napi_derive::napi;
use tokio::sync::oneshot;

use crate::call::guard::sync_call;
use crate::call::node_api::{call_status, CallStatus};
use crate::call::thrown::Envelope;
use crate::process_core::environment::environment_closing;
use crate::process_core::panic_rule::take_panic_cause;
use crate::values::boundary::{bigint_of_u64, u64_of_bigint};

/// Most calls into JavaScript that wait for their settle call, over every
/// registration of the process.
pub const MAX_PENDING_CALLBACKS: usize = 65_536;

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
    /// Node-API refused the call with `status`: a defect, not a transient
    /// state.
    NodeApiRefused { status: Status },
    /// The call ran on the JavaScript thread of its registration, which
    /// must run the function: a defect of the caller.
    OnJavaScriptThread,
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
    /// The JavaScript thread of the registration.
    javascript_thread: ThreadId,
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
    pub fn register(function: &Function<FnArgs<(BigInt, Argument)>, ()>) -> napi::Result<Self> {
        let function = function
            .build_threadsafe_function::<FnArgs<(BigInt, Argument)>>()
            .callee_handled::<false>()
            .weak::<true>()
            .max_queue_size::<QUEUE>()
            .build()?;
        Ok(Self {
            function,
            javascript_thread: std::thread::current().id(),
            answer: PhantomData,
        })
    }

    /// Queue the call of the function with `argument`, in the take form: the
    /// signal that the JavaScript thread took the call, and the wait for its
    /// settle call. On a core thread. The drop of the answer wait ends the
    /// call: a late take or settle call finds no call.
    ///
    /// Failure exits: the `CallbackFailure` of a call that is not admitted
    /// or not queued (`NotRun`, `OnJavaScriptThread`, `NodeApiRefused`).
    pub fn start_call(
        &self,
        argument: Argument,
    ) -> Result<(TakenSignal, AnswerWait<Answer>), CallbackFailure> {
        admit(self.javascript_thread, std::thread::current().id())?;
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        let pending = PendingCall::insert(call_id, settler::<Answer>(call_id, sender))?;
        let (taken_sender, taken_receiver) = oneshot::channel();
        let watch = TakeWatch::insert(call_id, taken_sender);
        queued(self.function.call(
            FnArgs::from((bigint_of_u64(call_id), argument)),
            ThreadsafeFunctionCallMode::NonBlocking,
        ))?;
        Ok((
            TakenSignal {
                receiver: taken_receiver,
            },
            AnswerWait {
                receiver,
                pending,
                watch,
            },
        ))
    }

    /// Call the function with `argument` and wait for its settle call. On a
    /// core thread. The wait has no bound of its own: a caller that needs one
    /// drops the future at its bound, and a late settle call then finds no
    /// pending call.
    pub async fn call(&self, argument: Argument) -> Result<Answer, CallbackFailure> {
        admit(self.javascript_thread, std::thread::current().id())?;
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        let _pending = PendingCall::insert(call_id, settler::<Answer>(call_id, sender))?;
        queued(self.function.call(
            FnArgs::from((bigint_of_u64(call_id), argument)),
            ThreadsafeFunctionCallMode::NonBlocking,
        ))?;
        answer_of(receiver.await)
    }
}

/// The signal that the JavaScript thread took a call of the take form.
pub struct TakenSignal {
    receiver: oneshot::Receiver<()>,
}

impl TakenSignal {
    /// Wait until the JavaScript thread takes the call, or the call ends
    /// with no take call (a settle call, the end of the environment): the
    /// answer wait then has the answer or the failure.
    pub async fn wait(self) {
        let _taken_or_ended = self.receiver.await;
    }
}

/// The wait for the settle call of a call of the take form. Its drop ends
/// the call.
pub struct AnswerWait<Answer> {
    receiver: oneshot::Receiver<Result<Answer, CallbackFailure>>,
    /// The entries of the call; dropped after the answer, or with the wait.
    pending: PendingCall,
    watch: TakeWatch,
}

impl<Answer> AnswerWait<Answer> {
    /// The answer of the call, or its failure. No bound of its own.
    pub async fn wait(self) -> Result<Answer, CallbackFailure> {
        let answer = answer_of(self.receiver.await);
        drop(self.pending);
        drop(self.watch);
        answer
    }
}

/// Whether a call on `this_thread` may run: not after the end of the
/// environment, and not on the JavaScript thread of its registration.
fn admit(javascript_thread: ThreadId, this_thread: ThreadId) -> Result<(), CallbackFailure> {
    match (environment_closing(), this_thread == javascript_thread) {
        (true, _) => Err(CallbackFailure::NotRun {
            cause: NotRunCause::EnvClosing,
        }),
        (false, true) => Err(CallbackFailure::OnJavaScriptThread),
        (false, false) => Ok(()),
    }
}

/// Whether Node-API queued the call, by its status. Pure.
fn queued(status: Status) -> Result<(), CallbackFailure> {
    match call_status(status) {
        CallStatus::Queued => Ok(()),
        CallStatus::QueueFull => Err(CallbackFailure::NotRun {
            cause: NotRunCause::QueueFull,
        }),
        CallStatus::Closing => Err(CallbackFailure::NotRun {
            cause: NotRunCause::EnvClosing,
        }),
        CallStatus::Refused { status } => Err(CallbackFailure::NodeApiRefused { status }),
    }
}

/// The answer of a call from what its channel received. A settler that was
/// dropped with no settle call means the end of the environment
/// ([`end_pending_calls`]). Pure.
fn answer_of<Answer>(
    received: Result<Result<Answer, CallbackFailure>, oneshot::error::RecvError>,
) -> Result<Answer, CallbackFailure> {
    match received {
        Ok(answer) => answer,
        Err(_settler_dropped) => Err(CallbackFailure::NotRun {
            cause: NotRunCause::EnvClosing,
        }),
    }
}

/// End every pending call: drop its settler, so its caller gets
/// `NotRun { envClosing }`. The cleanup hook of the environment calls it:
/// Node-API drops a queued call at the end of the environment with no answer.
/// The settlers are dropped after the lock is released.
pub fn end_pending_calls() {
    let ended: Vec<Settler> = PENDING
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .drain()
        .map(|(_call_id, settler)| settler)
        .collect();
    drop(ended);
    let watches: Vec<oneshot::Sender<()>> = TAKE_WATCHES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .drain()
        .map(|(_call_id, sender)| sender)
        .collect();
    drop(watches);
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

/// The taken senders of the calls of the take form, by call id. An entry
/// exists only next to an entry of [`PENDING`], so the bound of that table
/// bounds this one.
static TAKE_WATCHES: LazyLock<Mutex<HashMap<u64, oneshot::Sender<()>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The entry of one call of the take form in [`TAKE_WATCHES`]; the drop
/// removes it.
struct TakeWatch {
    call_id: u64,
}

impl TakeWatch {
    /// Add the entry. Its call holds an entry of [`PENDING`].
    fn insert(call_id: u64, sender: oneshot::Sender<()>) -> Self {
        let _no_earlier_entry = TAKE_WATCHES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(call_id, sender);
        Self { call_id }
    }
}

impl Drop for TakeWatch {
    fn drop(&mut self) {
        let _taken_or_left = TAKE_WATCHES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.call_id);
    }
}

/// The taken sender of the call `call_id`, out of the table. Pure apart
/// from the table.
fn take_watch(call_id: u64) -> Option<oneshot::Sender<()>> {
    TAKE_WATCHES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&call_id)
}

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
    let read = std::panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: `env` and `value` come from the settle call that runs now,
        // on the JavaScript thread of `env`.
        unsafe { Answer::from_napi_value(env, value) }
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
    // A settle call ends the taken signal of a call of the take form too.
    drop(take_watch(call_id));
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
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
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

/// The JavaScript thread took the call `callId` of the take form, before it
/// runs the function: `settled` when the core still waits for the call,
/// `unknownCall` when it ended (its caller passed its bound), so the
/// function need not run.
#[napi(ts_return_type = "NativeEnvelope<SettleOutcome>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn take_callback(call_id: BigInt) -> Envelope<SettleOutcome> {
    sync_call("take_callback", || {
        match u64_of_bigint("callId", &call_id) {
            Ok(call_id) => Envelope::Value(take(call_id)),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// Signal the take of the call `call_id`, if a call of the take form waits
/// under the id.
fn take(call_id: u64) -> SettleOutcome {
    match take_watch(call_id) {
        Some(sender) => {
            let _no_reader = sender.send(());
            SettleOutcome::Settled
        }
        None => SettleOutcome::UnknownCall,
    }
}

/// The function of the call `callId` threw, or its Promise rejected. The
/// thrown value stays in the package.
#[napi(ts_return_type = "NativeEnvelope<SettleOutcome>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn settle_callback_threw(
    call_id: BigInt,
    name: String,
    message: String,
) -> Envelope<SettleOutcome> {
    sync_call("settle_callback_threw", || {
        settle(&call_id, Settlement::Threw { name, message })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The child test to run, set in the environment of a child process. The
    /// table of pending calls and the end of the environment are per
    /// process, so the tests that fill or end them run in a child process.
    const CHILD_TEST_ENV: &str = "HORSIES_TYPESCRIPT_CALLBACK_CHILD_TEST";

    fn in_child(test: &str) -> bool {
        std::env::var(CHILD_TEST_ENV).is_ok_and(|name| name == test)
    }

    /// Run one test of this binary in a child process and check that it
    /// passed.
    fn assert_child_passed(test: &str) {
        let binary = std::env::current_exe().expect("the test binary has a path");
        let output = std::process::Command::new(binary)
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST_ENV, test)
            .output()
            .expect("the child test process runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains(&format!("test {test} ... ok")), "{stdout}");
    }

    /// A pending call of a `String` answer, with its receiver.
    fn pending_call() -> (
        PendingCall,
        oneshot::Receiver<Result<String, CallbackFailure>>,
    ) {
        let (sender, receiver) = oneshot::channel();
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let pending = PendingCall::insert(call_id, settler::<String>(call_id, sender))
            .expect("the table has room");
        (pending, receiver)
    }

    #[test]
    fn a_take_call_ends_the_taken_signal_and_a_second_take_finds_no_call() {
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, mut receiver) = oneshot::channel();
        let _watch = TakeWatch::insert(call_id, sender);
        assert_eq!(take(call_id), SettleOutcome::Settled);
        assert_eq!(receiver.try_recv(), Ok(()));
        assert_eq!(take(call_id), SettleOutcome::UnknownCall);
    }

    #[test]
    fn a_settle_call_ends_the_taken_signal_of_a_call_with_no_take() {
        let (pending, _answer) = pending_call();
        let call_id = pending.call_id;
        let (sender, mut receiver) = oneshot::channel();
        let _watch = TakeWatch::insert(call_id, sender);
        assert!(matches!(
            settle(
                &bigint_of_u64(call_id),
                Settlement::Threw {
                    name: "Error".to_owned(),
                    message: "boom".to_owned(),
                }
            ),
            Envelope::Value(SettleOutcome::Settled)
        ));
        assert_eq!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
    }

    #[test]
    fn the_drop_of_a_watch_ends_its_taken_signal() {
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, mut receiver) = oneshot::channel();
        drop(TakeWatch::insert(call_id, sender));
        assert_eq!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
        assert_eq!(take(call_id), SettleOutcome::UnknownCall);
    }

    #[test]
    fn a_queued_call_passes() {
        assert_eq!(queued(Status::Ok), Ok(()));
    }

    #[test]
    fn a_full_queue_is_not_run_queue_full() {
        assert_eq!(
            queued(Status::QueueFull),
            Err(CallbackFailure::NotRun {
                cause: NotRunCause::QueueFull
            })
        );
    }

    #[test]
    fn a_closing_function_is_not_run_env_closing() {
        assert_eq!(
            queued(Status::Closing),
            Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing
            })
        );
    }

    #[test]
    fn another_status_is_a_node_api_refusal_not_a_retry() {
        assert_eq!(
            queued(Status::GenericFailure),
            Err(CallbackFailure::NodeApiRefused {
                status: Status::GenericFailure
            })
        );
    }

    #[test]
    fn a_call_on_the_javascript_thread_is_refused() {
        let this_thread = std::thread::current().id();
        assert_eq!(
            admit(this_thread, this_thread),
            Err(CallbackFailure::OnJavaScriptThread)
        );
    }

    #[test]
    fn a_call_on_another_thread_is_admitted() {
        let javascript_thread = std::thread::spawn(|| std::thread::current().id())
            .join()
            .expect("the thread ends");
        assert_eq!(
            admit(javascript_thread, std::thread::current().id()),
            Ok(())
        );
    }

    #[test]
    fn closed_environment_child() {
        if !in_child("call::callback::tests::closed_environment_child") {
            return;
        }
        let (pending, receiver) = pending_call();
        crate::process_core::environment::environment_closed();
        let javascript_thread = std::thread::spawn(|| std::thread::current().id())
            .join()
            .expect("the thread ends");
        assert_eq!(
            admit(javascript_thread, std::thread::current().id()),
            Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing
            })
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a test runtime starts");
        assert_eq!(
            answer_of(runtime.block_on(receiver)),
            Err(CallbackFailure::NotRun {
                cause: NotRunCause::EnvClosing
            })
        );
        assert!(PENDING
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty());
        drop(pending);
    }

    #[test]
    fn the_end_of_the_environment_ends_every_pending_call_with_env_closing() {
        assert_child_passed("call::callback::tests::closed_environment_child");
    }

    #[test]
    fn table_bound_child() {
        if !in_child("call::callback::tests::table_bound_child") {
            return;
        }
        let pending: Vec<PendingCall> = (0..MAX_PENDING_CALLBACKS)
            .map(|_| pending_call().0)
            .collect();
        let call_id = NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let (sender, _receiver) = oneshot::channel::<Result<String, CallbackFailure>>();
        assert_eq!(
            PendingCall::insert(call_id, settler::<String>(call_id, sender)).err(),
            Some(CallbackFailure::NotRun {
                cause: NotRunCause::QueueFull
            })
        );
        drop(pending);
        assert!(PENDING
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty());
    }

    #[test]
    fn the_table_past_its_bound_refuses_with_queue_full() {
        assert_child_passed("call::callback::tests::table_bound_child");
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
}
