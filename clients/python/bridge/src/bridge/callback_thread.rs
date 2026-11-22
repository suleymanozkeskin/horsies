//! The callback thread: the one thread of a process that runs Python
//! callbacks for the core, such as the `build_with` of a workflow
//! definition.
//!
//! A core task may never wait for the GIL. So a task hands a Python call to
//! this thread through a bounded queue and awaits the outcome; the thread
//! attaches to Python for the call only.
//!
//! Rules:
//! - The thread starts at the first callback of the process and runs for the
//!   life of the process. A failed start is returned and not kept: the next
//!   callback tries the start again.
//! - A submit never waits. A full queue refuses the callback at once with
//!   [`CallbackFailure::QueueFull`]: a transient failure, never a refusal of
//!   user code. The step that asked rolls back and runs again later.
//! - No core lock is held while the thread attaches.
//! - A callback whose caller left (its [`CallbackReply`] was dropped) before
//!   the thread took it does not run. A callback that runs is not
//!   interrupted, because Python code cannot be stopped from outside; its
//!   outcome is discarded with a DEBUG record.
//! - Every failure has a name and carries the subject of the callback:
//!   - [`CallbackFailure::RaisedException`]: the call raised an `Exception`;
//!     the raised object comes with it, for a caller that raises it again;
//!   - [`CallbackFailure::RaisedBaseException`]: the call raised a
//!     `BaseException` that is not an `Exception`, such as
//!     `KeyboardInterrupt` or `SystemExit`. It is not a refusal of user
//!     input; the caller raises it again where it can;
//!   - [`CallbackFailure::Panicked`]: the call panicked, in its Rust part or
//!     in a Rust function that the Python code called (`PanicException`).
//!     The panic is caught at the call boundary and the thread runs on;
//!   - [`CallbackFailure::InterpreterShutDown`]: the interpreter is shutting
//!     down, so the callback did not run.
//! - The thread attaches with `Python::try_attach`, because `attach` panics
//!   during interpreter shutdown.
//! - A fork child never submits a callback: every core call in a fork child
//!   is refused before it can reach this thread.

use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Mutex, OnceLock, PoisonError};

use pyo3::exceptions::PyException;
use pyo3::panic::PanicException;
use pyo3::prelude::*;
use tokio::sync::oneshot;

use crate::logging::make_thread_default;
use crate::process_core::take_panic_cause;

/// Callbacks that may wait in the queue of the callback thread. A callback
/// past it is refused with [`CallbackFailure::QueueFull`].
pub const CALLBACK_QUEUE_CAPACITY: usize = 1_024;

/// Name of the callback thread.
pub const CALLBACK_THREAD_NAME: &str = "horsies-callback";

/// One Python call. It runs attached on the callback thread and returns its
/// value or the exception it raised.
pub type PythonCall<T> = Box<dyn for<'py> FnOnce(Python<'py>) -> PyResult<T> + Send + 'static>;

/// Box `call` as a [`PythonCall`]. The bound lets the compiler infer the
/// closure's signature.
pub fn python_call<T, F>(call: F) -> PythonCall<T>
where
    F: for<'py> FnOnce(Python<'py>) -> PyResult<T> + Send + 'static,
{
    Box::new(call)
}

/// The type name and the text of a raised Python exception, read on the
/// callback thread so that a reader without the GIL can log them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaisedCause {
    pub exception_type: String,
    pub message: String,
}

impl fmt::Display for RaisedCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.exception_type, self.message)
    }
}

/// Why a callback gave no value. `subject` names the callback, as its
/// submitter named it.
#[derive(Debug)]
pub enum CallbackFailure {
    /// The queue held `capacity` callbacks; this one did not run. Transient.
    QueueFull { subject: String, capacity: usize },
    /// The callback thread could not start; this callback did not run. The
    /// next callback tries the start again. Transient.
    ThreadStartFailed { subject: String, cause: String },
    /// The call raised an `Exception`. `exception` is the raised object, for
    /// a caller that raises it again.
    RaisedException {
        subject: String,
        cause: RaisedCause,
        exception: Box<PyErr>,
    },
    /// The call raised a `BaseException` that is not an `Exception`.
    /// `exception` is the raised object, for the caller to raise again.
    RaisedBaseException {
        subject: String,
        cause: RaisedCause,
        exception: Box<PyErr>,
    },
    /// The call panicked. The thread runs on.
    Panicked { subject: String, cause: String },
    /// The interpreter is shutting down; the callback did not run.
    InterpreterShutDown { subject: String },
}

impl fmt::Display for CallbackFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull { subject, capacity } => write!(
                formatter,
                "{subject} did not run: the callback queue holds {capacity} callbacks"
            ),
            Self::ThreadStartFailed { subject, cause } => write!(
                formatter,
                "{subject} did not run: the callback thread could not start: {cause}"
            ),
            Self::RaisedException { subject, cause, .. }
            | Self::RaisedBaseException { subject, cause, .. } => {
                write!(formatter, "{subject} raised {cause}")
            }
            Self::Panicked { subject, cause } => write!(formatter, "{subject} panicked: {cause}"),
            Self::InterpreterShutDown { subject } => write!(
                formatter,
                "{subject} did not run: the Python interpreter is shutting down"
            ),
        }
    }
}

/// The awaiting side of one submitted callback. Dropping it before the
/// thread takes the callback means the callback does not run.
pub struct CallbackReply<T> {
    subject: String,
    outcome: oneshot::Receiver<Result<T, CallbackFailure>>,
}

impl<T> CallbackReply<T> {
    /// The outcome of the callback.
    ///
    /// The callback thread sends an outcome for every callback it takes,
    /// unless its caller left. A reply without an outcome is a defect of the
    /// callback thread: it panics with the subject.
    pub async fn outcome(self) -> Result<T, CallbackFailure> {
        match self.outcome.await {
            Ok(outcome) => outcome,
            Err(_dropped) => panic!(
                "the callback thread dropped {} without an outcome",
                self.subject
            ),
        }
    }
}

/// A queued callback with its reply. Type-erased, so that one queue carries
/// callbacks of every outcome type.
trait QueuedCallback: Send {
    fn subject(&self) -> &str;
    /// True when the caller left: nobody waits for the outcome.
    fn caller_left(&self) -> bool;
    /// Run the call attached and send its outcome.
    fn run(self: Box<Self>, py: Python<'_>);
    /// Send the outcome of a callback that cannot run because the
    /// interpreter is shutting down.
    fn refuse_at_shutdown(self: Box<Self>);
}

struct Queued<T> {
    subject: String,
    call: PythonCall<T>,
    reply: oneshot::Sender<Result<T, CallbackFailure>>,
}

impl<T: Send + 'static> QueuedCallback for Queued<T> {
    fn subject(&self) -> &str {
        &self.subject
    }

    fn caller_left(&self) -> bool {
        self.reply.is_closed()
    }

    fn run(self: Box<Self>, py: Python<'_>) {
        let Queued {
            subject,
            call,
            reply,
        } = *self;
        let outcome = match std::panic::catch_unwind(AssertUnwindSafe(move || call(py))) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(raised)) => Err(classify(py, subject.clone(), raised)),
            Err(payload) => Err(CallbackFailure::Panicked {
                subject: subject.clone(),
                cause: take_panic_cause(payload.as_ref()),
            }),
        };
        send_outcome(reply, &subject, outcome);
    }

    fn refuse_at_shutdown(self: Box<Self>) {
        let Queued { subject, reply, .. } = *self;
        let failure = CallbackFailure::InterpreterShutDown {
            subject: subject.clone(),
        };
        send_outcome(reply, &subject, Err(failure));
    }
}

/// Send `outcome` to the caller, or record that the caller left.
fn send_outcome<T>(
    reply: oneshot::Sender<Result<T, CallbackFailure>>,
    subject: &str,
    outcome: Result<T, CallbackFailure>,
) {
    if reply.send(outcome).is_err() {
        tracing::debug!("{subject} ended after its caller left; the outcome was discarded");
    }
}

/// Name a raised Python exception. Runs attached.
fn classify(py: Python<'_>, subject: String, raised: PyErr) -> CallbackFailure {
    let cause = raised_cause(py, &raised);
    match (
        raised.is_instance_of::<PanicException>(py),
        raised.is_instance_of::<PyException>(py),
    ) {
        (true, _) => CallbackFailure::Panicked {
            subject,
            cause: cause.to_string(),
        },
        (false, true) => CallbackFailure::RaisedException {
            subject,
            cause,
            exception: Box::new(raised),
        },
        (false, false) => CallbackFailure::RaisedBaseException {
            subject,
            cause,
            exception: Box::new(raised),
        },
    }
}

/// The type name and text of `raised`. A part that cannot be read is
/// replaced by a text that says so. Runs attached.
fn raised_cause(py: Python<'_>, raised: &PyErr) -> RaisedCause {
    let exception_type = match raised.get_type(py).name() {
        Ok(name) => name.to_string_lossy().into_owned(),
        Err(_unreadable) => "<exception type without a readable name>".to_owned(),
    };
    let message = match raised.value(py).str() {
        Ok(text) => text.to_string_lossy().into_owned(),
        Err(_unreadable) => "<exception text that could not be read>".to_owned(),
    };
    RaisedCause {
        exception_type,
        message,
    }
}

/// A started callback thread: the sending side of its queue.
struct CallbackThread {
    queue: SyncSender<Box<dyn QueuedCallback>>,
}

/// The callback thread of one process, started at the first callback.
pub struct CallbackThreadSlot {
    started: OnceLock<CallbackThread>,
    start_lock: Mutex<()>,
    capacity: usize,
    dispatch: tracing::Dispatch,
}

impl CallbackThreadSlot {
    /// A slot whose thread will hold at most `capacity` waiting callbacks
    /// and log through `dispatch`. Starts nothing.
    pub fn new(capacity: usize, dispatch: tracing::Dispatch) -> Self {
        Self {
            started: OnceLock::new(),
            start_lock: Mutex::new(()),
            capacity,
            dispatch,
        }
    }

    /// Queue `call` for the callback thread, starting the thread at the
    /// first callback. Never waits for the GIL or for queue space.
    ///
    /// Failure exits: [`CallbackFailure::QueueFull`],
    /// [`CallbackFailure::ThreadStartFailed`]. In both, the callback did not
    /// run and nothing changed.
    pub fn submit<T: Send + 'static>(
        &self,
        subject: String,
        call: PythonCall<T>,
    ) -> Result<CallbackReply<T>, CallbackFailure> {
        let thread = match self.thread() {
            Ok(thread) => thread,
            Err(cause) => return Err(CallbackFailure::ThreadStartFailed { subject, cause }),
        };
        let (reply, outcome) = oneshot::channel();
        let queued: Box<dyn QueuedCallback> = Box::new(Queued {
            subject: subject.clone(),
            call,
            reply,
        });
        match thread.queue.try_send(queued) {
            Ok(()) => Ok(CallbackReply { subject, outcome }),
            Err(TrySendError::Full(_refused)) => Err(CallbackFailure::QueueFull {
                subject,
                capacity: self.capacity,
            }),
            // The receiver lives on the callback thread, which never ends:
            // each callback runs inside a panic boundary.
            Err(TrySendError::Disconnected(_refused)) => {
                panic!("the callback thread ended; {subject} cannot run")
            }
        }
    }

    /// Submit `call` and wait for its outcome. Failure exits: those of
    /// [`Self::submit`] and of [`CallbackReply::outcome`].
    pub fn call<T: Send + 'static>(
        &self,
        subject: String,
        call: PythonCall<T>,
    ) -> impl Future<Output = Result<T, CallbackFailure>> + Send + '_ {
        let submitted = self.submit(subject, call);
        async move {
            match submitted {
                Ok(reply) => reply.outcome().await,
                Err(refused) => Err(refused),
            }
        }
    }

    /// The started thread, or the cause of a failed start. A failed start
    /// is not kept. The start lock is held only to spawn the thread.
    fn thread(&self) -> Result<&CallbackThread, String> {
        if let Some(thread) = self.started.get() {
            return Ok(thread);
        }
        let _start_guard = self
            .start_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(thread) = self.started.get() {
            return Ok(thread);
        }
        let thread = start_thread(self.capacity, self.dispatch.clone())
            .map_err(|error| error.to_string())?;
        Ok(self.started.get_or_init(|| thread))
    }
}

/// Spawn the callback thread. It is detached: it runs for the life of the
/// process.
fn start_thread(capacity: usize, dispatch: tracing::Dispatch) -> std::io::Result<CallbackThread> {
    let (queue, callbacks) = std::sync::mpsc::sync_channel(capacity);
    let _detached = std::thread::Builder::new()
        .name(CALLBACK_THREAD_NAME.to_owned())
        .spawn(move || {
            make_thread_default(&dispatch);
            serve(&callbacks);
        })?;
    Ok(CallbackThread { queue })
}

/// The service loop. It ends only when every sender is gone, which does not
/// happen while the core of the process lives.
fn serve(callbacks: &Receiver<Box<dyn QueuedCallback>>) {
    while let Ok(queued) = callbacks.recv() {
        if queued.caller_left() {
            tracing::debug!(
                "{} did not run: its caller left before the callback thread took it",
                queued.subject()
            );
            continue;
        }
        let subject = queued.subject().to_owned();
        let mut waiting = Some(queued);
        let attached = std::panic::catch_unwind(AssertUnwindSafe(|| {
            Python::try_attach(|py| {
                if let Some(queued) = waiting.take() {
                    queued.run(py);
                }
            })
        }));
        match (attached, waiting) {
            (Ok(Some(())), _) => {}
            (Ok(None), Some(queued)) => queued.refuse_at_shutdown(),
            (Ok(None), None) => {}
            // The call's own boundary catches its panics, so this panic came
            // from the outcome handling. The reply went with the unwind, and
            // the caller's `outcome` panics with the subject.
            (Err(payload), _) => tracing::error!(
                "panic in the callback thread while it handled {subject}: {}",
                take_panic_cause(payload.as_ref())
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::sync::Arc;

    use pyo3::exceptions::PyRuntimeError;

    use super::*;

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("the test runtime builds")
    }

    fn test_slot(capacity: usize) -> CallbackThreadSlot {
        Python::initialize();
        CallbackThreadSlot::new(capacity, tracing::Dispatch::none())
    }

    /// A callback that tells `started` that it runs, then holds the callback
    /// thread until `release` receives a message.
    fn blocker(
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> PythonCall<i64> {
        python_call(move |py| {
            let _receiver_waits = started.send(());
            py.detach(move || release.recv())
                .map_err(|_closed| PyRuntimeError::new_err("the test never released the call"))?;
            Ok(1)
        })
    }

    #[test]
    fn a_callback_returns_its_value() {
        let slot = test_slot(4);
        let outcome = test_runtime().block_on(slot.call(
            "test callback".to_owned(),
            python_call(|py| py.eval(c"6 * 7", None, None)?.extract::<i64>()),
        ));
        assert!(matches!(outcome, Ok(42)), "{outcome:?}");
    }

    #[test]
    fn an_exception_is_named_with_its_type_and_text() {
        let slot = test_slot(4);
        let outcome = test_runtime().block_on(slot.call(
            "test callback".to_owned(),
            python_call(|py| py.run(c"raise ValueError('bad input')", None, None)),
        ));
        match outcome {
            Err(CallbackFailure::RaisedException {
                subject,
                cause,
                exception,
            }) => {
                assert_eq!(subject, "test callback");
                assert_eq!(
                    cause,
                    RaisedCause {
                        exception_type: "ValueError".to_owned(),
                        message: "bad input".to_owned(),
                    }
                );
                Python::attach(|py| {
                    assert!(exception.is_instance_of::<pyo3::exceptions::PyValueError>(py));
                });
            }
            other => panic!("an Exception is RaisedException, got {other:?}"),
        }
    }

    #[test]
    fn a_base_exception_keeps_the_raised_object() {
        let slot = test_slot(4);
        let outcome = test_runtime().block_on(slot.call(
            "test callback".to_owned(),
            python_call(|py| py.run(c"raise SystemExit(3)", None, None)),
        ));
        match outcome {
            Err(CallbackFailure::RaisedBaseException {
                cause, exception, ..
            }) => {
                assert_eq!(cause.exception_type, "SystemExit");
                assert_eq!(cause.message, "3");
                Python::attach(|py| {
                    assert!(exception.is_instance_of::<pyo3::exceptions::PySystemExit>(py));
                });
            }
            other => panic!("SystemExit is RaisedBaseException, got {other:?}"),
        }
    }

    #[test]
    fn a_panic_is_caught_at_the_call_and_the_thread_runs_on() {
        let slot = test_slot(4);
        let runtime = test_runtime();
        let outcome = runtime.block_on(slot.call(
            "test callback".to_owned(),
            python_call(|_py| -> PyResult<()> { panic!("planned callback panic") }),
        ));
        match outcome {
            Err(CallbackFailure::Panicked { subject, cause }) => {
                assert_eq!(subject, "test callback");
                assert!(cause.starts_with("planned callback panic"), "{cause}");
            }
            other => panic!("a panic is Panicked, got {other:?}"),
        }
        let next =
            runtime.block_on(slot.call("next callback".to_owned(), python_call(|_py| Ok(5_i64))));
        assert!(matches!(next, Ok(5)), "{next:?}");
    }

    #[test]
    fn a_panic_exception_raised_through_python_is_a_panic() {
        let slot = test_slot(4);
        let outcome = test_runtime().block_on(slot.call(
            "test callback".to_owned(),
            python_call(|_py| -> PyResult<()> {
                Err(PanicException::new_err("panic in a Rust function"))
            }),
        ));
        match outcome {
            Err(CallbackFailure::Panicked { cause, .. }) => {
                assert!(cause.contains("panic in a Rust function"), "{cause}");
            }
            other => panic!("PanicException is Panicked, got {other:?}"),
        }
    }

    #[test]
    fn a_full_queue_refuses_the_callback_at_once() {
        let slot = test_slot(1);
        let (started, started_seen) = channel();
        let (release, released) = channel();
        let held = slot
            .submit("held".to_owned(), blocker(started, released))
            .expect("the thread starts and the queue has room");
        started_seen.recv().expect("the held callback runs");
        let waiting = slot
            .submit("waiting".to_owned(), python_call(|_py| Ok(2_i64)))
            .expect("the queue has room for one callback");
        let refused = slot.submit("refused".to_owned(), python_call(|_py| Ok(3_i64)));
        match refused {
            Err(CallbackFailure::QueueFull { subject, capacity }) => {
                assert_eq!(subject, "refused");
                assert_eq!(capacity, 1);
            }
            other => panic!("a full queue refuses at once, got {:?}", other.err()),
        }
        release.send(()).expect("the held callback waits");
        let runtime = test_runtime();
        let held_outcome = runtime.block_on(held.outcome());
        assert!(matches!(held_outcome, Ok(1)), "{held_outcome:?}");
        let waiting_outcome = runtime.block_on(waiting.outcome());
        assert!(matches!(waiting_outcome, Ok(2)), "{waiting_outcome:?}");
    }

    #[test]
    fn a_callback_whose_caller_left_before_it_ran_does_not_run() {
        let slot = test_slot(4);
        let (started, started_seen) = channel();
        let (release, released) = channel();
        let held = slot
            .submit("held".to_owned(), blocker(started, released))
            .expect("the thread starts and the queue has room");
        started_seen.recv().expect("the held callback runs");
        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_call = Arc::clone(&ran);
        let left = slot
            .submit(
                "left".to_owned(),
                python_call(move |_py| {
                    ran_in_call.store(true, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .expect("the queue has room");
        drop(left);
        let after = slot
            .submit("after".to_owned(), python_call(|_py| Ok(())))
            .expect("the queue has room");
        release.send(()).expect("the held callback waits");
        let runtime = test_runtime();
        let held_outcome = runtime.block_on(held.outcome());
        assert!(matches!(held_outcome, Ok(1)), "{held_outcome:?}");
        // The queue is first in, first out: `left` was taken before `after`.
        let after_outcome = runtime.block_on(after.outcome());
        assert!(matches!(after_outcome, Ok(())), "{after_outcome:?}");
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[test]
    fn the_failure_text_names_the_subject_and_the_cause() {
        let failure = CallbackFailure::RaisedException {
            subject: "build_with of definition 'report'".to_owned(),
            cause: RaisedCause {
                exception_type: "KeyError".to_owned(),
                message: "'day'".to_owned(),
            },
            exception: Box::new(pyo3::exceptions::PyKeyError::new_err("'day'")),
        };
        assert_eq!(
            failure.to_string(),
            "build_with of definition 'report' raised KeyError: 'day'"
        );
    }
}
