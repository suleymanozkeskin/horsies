//! The async form of a core call: one completion queue per event loop, woken
//! through one file descriptor.
//!
//! - A call creates an asyncio future on the running loop and spawns the
//!   operation on the core runtime.
//! - The finished operation pushes its outcome into the loop's queue and
//!   writes one wake byte, only when no wake is pending. Core threads never
//!   take the GIL.
//! - The loop watches the read end with `loop.add_reader`. Its callback runs
//!   on the loop thread, which holds the GIL, and sets every finished future
//!   in one pass.
//! - A cancelled future is removed at once by its done callback. A cancelled
//!   read is dropped on the runtime; a cancelled write runs on to its end and
//!   its outcome is discarded with a DEBUG record. A cancelled service run
//!   ([`call_service_async`]) is asked to stop and runs on to its boundary;
//!   its outcome is discarded the same way.
//!
//! The queues live in a `weakref.WeakKeyDictionary` keyed by the loop. A
//! queue, its wake socket pair and its pending futures go when the loop is
//! collected.

use std::collections::HashMap;
use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, PoisonError};

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::IntoPyObjectExt;
use tokio::sync::oneshot;

use super::service_run::StopSignal;
use super::{guard, guarded_into_result, CallKind};
use crate::errors::TooManyPendingCalls;
use crate::logging::RecordLevel;
use crate::process_core::{running_core, Core};

/// Most core calls that one event loop may have in flight, counted from the
/// submit to the drain of the outcome. A call past it is refused with
/// `TooManyPendingCalls`. It also bounds the outcomes waiting in the queue.
pub const MAX_CALLS_IN_FLIGHT_PER_LOOP: usize = 65_536;

/// Most outcomes one wake hands to their futures. When a wake takes this
/// many, it wakes the loop again for the rest.
pub const MAX_DRAIN_BATCH: usize = 4_096;

/// Bytes read from the wake socket per wake. One byte is written per pending
/// wake, so a few bytes clear it.
const WAKE_READ_BYTES: usize = 64;

type CallId = u64;

/// Turns an outcome into its Python value on the loop thread.
type Completion = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

/// One finished operation.
struct Finished {
    id: CallId,
    subject: &'static str,
    completion: Completion,
}

/// The side of a queue that core tasks hold.
struct WakeChannel {
    sender: Sender<Finished>,
    wake_pending: AtomicBool,
    wake_writer: UnixStream,
    in_flight: AtomicUsize,
}

impl WakeChannel {
    /// Queue an outcome and wake the loop. After the loop is gone nobody
    /// waits, so a send to a closed queue discards the outcome.
    fn push(&self, finished: Finished) {
        let _receiver_gone = self.sender.send(finished);
        self.wake();
    }

    /// Write one wake byte unless a wake is pending.
    fn wake(&self) {
        if self.wake_pending.swap(true, Ordering::AcqRel) {
            return;
        }
        match (&self.wake_writer).write(&[1]) {
            Ok(_written) => {}
            // A full socket already holds unread wake bytes.
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            // The read end is closed: the loop is gone.
            Err(_closed) => {}
        }
    }

    /// Count one more call in flight, or refuse it at the bound.
    fn reserve(&self) -> Result<(), usize> {
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                match count < MAX_CALLS_IN_FLIGHT_PER_LOOP {
                    true => Some(count + 1),
                    false => None,
                }
            })
            .map(|_previous| ())
            .map_err(|_at_bound| MAX_CALLS_IN_FLIGHT_PER_LOOP)
    }

    /// Count one call less in flight.
    fn release(&self) {
        let _previous = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            });
    }
}

/// How a cancel of the awaiting future reaches the operation.
enum CancelRoute {
    /// A read: the signal drops the operation on the runtime.
    DropOperation(oneshot::Sender<()>),
    /// A write: the operation runs on; only the future goes.
    Detach,
    /// A service run: the signal asks the run to stop; the run goes on to
    /// its boundary, and only the future goes.
    RequestStop(StopSignal),
}

/// What a cancel of the awaiting future does, chosen by the call form.
enum CancelPlan {
    DropOperation,
    Detach,
    RequestStop(StopSignal),
}

/// A future that waits for an operation of this loop.
struct PendingCall {
    future: Py<PyAny>,
    cancel: CancelRoute,
}

/// The completion queue of one event loop. Only the loop thread and
/// submitting threads, both attached, touch the pending map.
struct LoopQueue {
    channel: Arc<WakeChannel>,
    finished: Mutex<Receiver<Finished>>,
    wake_reader: UnixStream,
    pending: Mutex<HashMap<CallId, PendingCall>>,
    next_id: AtomicU64,
}

impl LoopQueue {
    /// Create the queue of `event_loop` and register its wake descriptor.
    fn open(py: Python<'_>, event_loop: &Bound<'_, PyAny>) -> PyResult<Arc<Self>> {
        let (wake_reader, wake_writer) = UnixStream::pair()?;
        wake_reader.set_nonblocking(true)?;
        wake_writer.set_nonblocking(true)?;
        let (sender, receiver) = std::sync::mpsc::channel();
        let queue = Arc::new(Self {
            channel: Arc::new(WakeChannel {
                sender,
                wake_pending: AtomicBool::new(false),
                wake_writer,
                in_flight: AtomicUsize::new(0),
            }),
            finished: Mutex::new(receiver),
            wake_reader,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        });
        let wake = Py::new(
            py,
            LoopWake {
                queue: Arc::clone(&queue),
            },
        )?;
        let _registered =
            event_loop.call_method1("add_reader", (queue.wake_reader.as_raw_fd(), wake))?;
        Ok(queue)
    }

    fn insert_pending(&self, id: CallId, call: PendingCall) {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let _no_previous_call = pending.insert(id, call);
    }

    fn take_pending(&self, id: CallId) -> Option<PendingCall> {
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        pending.remove(&id)
    }

    /// Clear the wake, then take up to [`MAX_DRAIN_BATCH`] outcomes.
    fn take_finished(&self) -> Vec<Finished> {
        self.take_finished_with(|| {})
    }

    /// [`Self::take_finished`], with `between_steps` called after step 1 and
    /// after step 2, where a push of a core thread can land:
    ///
    /// 1. Read the wake bytes.
    /// 2. Clear the wake flag. The swap acquires, so the outcome of every
    ///    push that set the flag before the clear is visible to step 3.
    /// 3. Take the outcomes.
    ///
    /// A push between 1 and 2 finds the flag set and writes no byte; step 3
    /// takes its outcome. A push after 2 finds the flag clear and writes a
    /// byte that step 1 did not read, so the loop wakes again. With the clear
    /// before the read, a push between them writes a byte that the read
    /// takes: the flag stays set with no byte in the socket, no later push
    /// writes one, and the loop never drains again.
    fn take_finished_with(&self, mut between_steps: impl FnMut()) -> Vec<Finished> {
        let mut bytes = [0_u8; WAKE_READ_BYTES];
        // Nothing to read is not a failure: no wake byte is left.
        let _wake_bytes_cleared = (&self.wake_reader).read(&mut bytes);
        between_steps();
        let _wake_was_pending = self.channel.wake_pending.swap(false, Ordering::AcqRel);
        between_steps();
        let receiver = self.finished.lock().unwrap_or_else(PoisonError::into_inner);
        let mut batch = Vec::new();
        while batch.len() < MAX_DRAIN_BATCH {
            match receiver.try_recv() {
                Ok(finished) => batch.push(finished),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        batch
    }

    /// Set every finished future of this loop. Runs on the loop thread. A
    /// failure to set one future does not stop the others; the first one is
    /// raised into the loop's exception handler.
    fn drain(&self, py: Python<'_>) -> PyResult<()> {
        let batch = self.take_finished();
        if batch.len() == MAX_DRAIN_BATCH {
            self.channel.wake();
        }
        let mut first_failure: Option<PyErr> = None;
        for finished in batch {
            self.channel.release();
            let delivered = match self.take_pending(finished.id) {
                Some(call) => deliver(py, &call.future, finished.completion),
                None => {
                    log_discarded(finished.subject);
                    Ok(())
                }
            };
            first_failure = first_failure.or(delivered.err());
        }
        match first_failure {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }
}

/// Set the result or the exception of a future that is not done.
fn deliver(py: Python<'_>, future: &Py<PyAny>, completion: Completion) -> PyResult<()> {
    let future = future.bind(py);
    match future.call_method0("done")?.is_truthy()? {
        true => Ok(()),
        false => {
            let _set = match completion(py) {
                Ok(value) => future.call_method1("set_result", (value,))?,
                Err(error) => future.call_method1("set_exception", (error.into_value(py),))?,
            };
            Ok(())
        }
    }
}

/// Record the end of an operation whose awaiting future was cancelled.
fn log_discarded(subject: &'static str) {
    if let Some(core) = running_core() {
        core.log().push_bridge_record(
            RecordLevel::Debug,
            format!(
                "{subject} ended after its awaiting future was cancelled; \
                 the outcome was discarded"
            ),
        );
    }
}

/// The value kept for a loop in the queue dictionary.
#[pyclass(module = "horsies._core", name = "_LoopQueue", frozen)]
struct LoopQueueHandle {
    queue: Arc<LoopQueue>,
}

/// The reader callback of a loop's wake descriptor.
#[pyclass(module = "horsies._core", name = "_LoopWake", frozen)]
struct LoopWake {
    queue: Arc<LoopQueue>,
}

#[pymethods]
impl LoopWake {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        self.queue.drain(py)
    }
}

/// The done callback of one call's future: on a cancel, it removes the
/// future and, for a read, drops the operation.
#[pyclass(module = "horsies._core", name = "_CallDone", frozen)]
struct CallDone {
    queue: Arc<LoopQueue>,
    id: CallId,
}

#[pymethods]
impl CallDone {
    fn __call__(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
        match future.call_method0("cancelled")?.is_truthy()? {
            false => Ok(()),
            true => {
                match self.queue.take_pending(self.id) {
                    Some(PendingCall {
                        cancel: CancelRoute::DropOperation(signal),
                        ..
                    }) => {
                        let _operation_already_ended = signal.send(());
                    }
                    Some(PendingCall {
                        cancel: CancelRoute::RequestStop(stop),
                        ..
                    }) => stop.request(),
                    // A write runs on; only its future goes. No entry: the
                    // outcome was already handed over.
                    Some(PendingCall {
                        cancel: CancelRoute::Detach,
                        ..
                    })
                    | None => {}
                }
                Ok(())
            }
        }
    }
}

static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static LOOP_QUEUES: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// The running loop of this thread and its queue. Raises `RuntimeError`
/// when no loop runs, as `asyncio.get_running_loop` does.
fn running_loop_queue(py: Python<'_>) -> PyResult<(Bound<'_, PyAny>, Arc<LoopQueue>)> {
    let event_loop = GET_RUNNING_LOOP
        .import(py, "asyncio", "get_running_loop")?
        .call0()?;
    let queues = LOOP_QUEUES
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(py
                .import("weakref")?
                .getattr("WeakKeyDictionary")?
                .call0()?
                .unbind())
        })?
        .bind(py);
    let found = queues.call_method1("get", (&event_loop,))?;
    match found.is_none() {
        false => {
            let handle = found.cast::<LoopQueueHandle>()?;
            let queue = Arc::clone(&handle.get().queue);
            Ok((event_loop, queue))
        }
        true => {
            let queue = LoopQueue::open(py, &event_loop)?;
            queues.set_item(
                &event_loop,
                LoopQueueHandle {
                    queue: Arc::clone(&queue),
                },
            )?;
            Ok((event_loop, queue))
        }
    }
}

/// An asyncio future of the running loop of this thread that is already
/// done with `value`: the awaitable form of an operation that has nothing
/// to run, such as the close of a handle that never connected. Starts
/// nothing on the core runtime.
///
/// Failure exits: `RuntimeError` (no running loop), the error of the value
/// conversion, and errors of the loop methods.
pub fn done_future<'py, T>(py: Python<'py>, value: T) -> PyResult<Bound<'py, PyAny>>
where
    T: IntoPyObject<'py>,
{
    let event_loop = GET_RUNNING_LOOP
        .import(py, "asyncio", "get_running_loop")?
        .call0()?;
    let future = event_loop.call_method0("create_future")?;
    let value = value.into_bound_py_any(py)?;
    let _done = future.call_method1("set_result", (value,))?;
    Ok(future)
}

/// Start `op` on the core runtime and return an asyncio future of its
/// outcome, bound to the running loop of this thread.
///
/// Failure exits at the call: `RuntimeError` (no running loop),
/// `TooManyPendingCalls`, and errors of the loop methods. Failure exits of
/// the future: `PanicException`, `CoreStopped`, and the error of the outcome
/// conversion.
pub fn call_async<'py, T, F>(
    py: Python<'py>,
    core: &'static Core,
    subject: &'static str,
    kind: CallKind,
    op: F,
) -> PyResult<Bound<'py, PyAny>>
where
    T: for<'a> IntoPyObject<'a> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let plan = match kind {
        CallKind::Read => CancelPlan::DropOperation,
        CallKind::Write => CancelPlan::Detach,
    };
    submit(py, core, subject, plan, op)
}

/// Start the service run `run` on the core runtime and return an asyncio
/// future of its end, bound to the running loop of this thread. A cancel of
/// the future calls `stop`; the run goes on to its boundary. There is no
/// total bound.
///
/// Failure exits: those of [`call_async`].
pub fn call_service_async<'py, T, F>(
    py: Python<'py>,
    core: &'static Core,
    subject: &'static str,
    stop: StopSignal,
    run: F,
) -> PyResult<Bound<'py, PyAny>>
where
    T: for<'a> IntoPyObject<'a> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    submit(py, core, subject, CancelPlan::RequestStop(stop), run)
}

/// The body of [`call_async`] and [`call_service_async`].
fn submit<'py, T, F>(
    py: Python<'py>,
    core: &'static Core,
    subject: &'static str,
    plan: CancelPlan,
    op: F,
) -> PyResult<Bound<'py, PyAny>>
where
    T: for<'a> IntoPyObject<'a> + Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    let (event_loop, queue) = running_loop_queue(py)?;
    let future = event_loop.call_method0("create_future")?;
    let id = queue.next_id.fetch_add(1, Ordering::Relaxed);
    let _registered = future.call_method1(
        "add_done_callback",
        (CallDone {
            queue: Arc::clone(&queue),
            id,
        },),
    )?;
    if let Err(bound) = queue.channel.reserve() {
        return Err(TooManyPendingCalls::new_err((subject, bound)));
    }
    let (cancel, cancel_signal) = match plan {
        CancelPlan::DropOperation => {
            let (signal, signalled) = oneshot::channel();
            (CancelRoute::DropOperation(signal), Some(signalled))
        }
        CancelPlan::Detach => (CancelRoute::Detach, None),
        CancelPlan::RequestStop(stop) => (CancelRoute::RequestStop(stop), None),
    };
    queue.insert_pending(
        id,
        PendingCall {
            future: future.clone().unbind(),
            cancel,
        },
    );
    let channel = Arc::clone(&queue.channel);
    let _detached = core.runtime_handle().spawn(async move {
        let guarded = guard(core, subject, op);
        let outcome = match cancel_signal {
            None => Some(guarded.await),
            Some(signalled) => tokio::select! {
                biased;
                outcome = guarded => Some(outcome),
                _cancelled_or_queue_gone = signalled => None,
            },
        };
        match outcome {
            Some(outcome) => channel.push(Finished {
                id,
                subject,
                completion: Box::new(move |py: Python<'_>| -> PyResult<Py<PyAny>> {
                    guarded_into_result(subject, outcome)?.into_py_any(py)
                }),
            }),
            None => channel.release(),
        }
    });
    Ok(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A queue with its wake socket pair and its channel, and no event loop:
    /// the tests drain it by hand.
    fn queue_without_loop() -> LoopQueue {
        let (wake_reader, wake_writer) = UnixStream::pair().expect("a socket pair opens");
        wake_reader
            .set_nonblocking(true)
            .expect("the read end turns nonblocking");
        wake_writer
            .set_nonblocking(true)
            .expect("the write end turns nonblocking");
        let (sender, receiver) = std::sync::mpsc::channel();
        LoopQueue {
            channel: Arc::new(WakeChannel {
                sender,
                wake_pending: AtomicBool::new(false),
                wake_writer,
                in_flight: AtomicUsize::new(0),
            }),
            finished: Mutex::new(receiver),
            wake_reader,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// An outcome of call `id`. The tests never hand it to a future.
    fn finished(id: CallId) -> Finished {
        Finished {
            id,
            subject: "completion queue test",
            completion: Box::new(|py: Python<'_>| -> PyResult<Py<PyAny>> { Ok(py.None()) }),
        }
    }

    fn ids(batch: &[Finished]) -> Vec<CallId> {
        batch.iter().map(|finished| finished.id).collect()
    }

    /// Whether the wake socket holds a byte, as the loop's reader sees it.
    /// Reads the byte.
    fn loop_is_woken(queue: &LoopQueue) -> bool {
        let mut byte = [0_u8; 1];
        matches!((&queue.wake_reader).read(&mut byte), Ok(1))
    }

    /// Push call 1, then drain while call 2 is pushed after drain step
    /// `gap` (0: after the first step, 1: after the second). The ids that
    /// the drain took.
    fn drain_with_a_push_after_step(queue: &LoopQueue, gap: usize) -> Vec<CallId> {
        queue.channel.push(finished(1));
        let mut step = 0;
        let batch = queue.take_finished_with(|| {
            if step == gap {
                queue.channel.push(finished(2));
            }
            step += 1;
        });
        ids(&batch)
    }

    #[test]
    fn a_push_that_lands_inside_a_drain_leaves_the_next_push_a_wake() {
        for gap in 0..2 {
            let queue = queue_without_loop();
            assert_eq!(
                drain_with_a_push_after_step(&queue, gap),
                vec![1, 2],
                "after step {gap}: the drain takes both outcomes"
            );
            queue.channel.push(finished(3));
            assert!(
                loop_is_woken(&queue),
                "after step {gap}: the push after the drain left no wake byte"
            );
            assert_eq!(ids(&queue.take_finished()), vec![3], "after step {gap}");
        }
    }

    #[test]
    fn pushes_before_a_drain_write_one_wake_byte() {
        let queue = queue_without_loop();
        queue.channel.push(finished(1));
        queue.channel.push(finished(2));
        assert!(loop_is_woken(&queue));
        assert!(
            !loop_is_woken(&queue),
            "the second push found the wake pending"
        );
        assert_eq!(ids(&queue.take_finished()), vec![1, 2]);
        queue.channel.push(finished(3));
        assert!(loop_is_woken(&queue), "the drain cleared the wake");
    }
}
