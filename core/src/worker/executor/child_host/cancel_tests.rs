//! The operator cancel of a run in a Rust child on the test database: each
//! serve loop takes `cancel_run`, gives the cancel to the body (the async
//! signal, or the cancel state on the thread of a blocking body), and
//! reports the end of the run as cancelled. The cancel statement is not run
//! here: the parent sends `cancel_run` only after it committed, and these
//! tests check the child's side alone.
//!
//! The stop of host-held work: a body registers work through the handle of
//! its run, as a host registers the promise of a body, and keeps it outside
//! its future. The cancel rounds ask that work to stop and keep it; the run
//! reports it as left or stuck, and keeps its seat until the host ends it.

use std::future::Future;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::host::{ChildApp, ChildLogSetup, ChildStartHook};
use super::link::{read_frame, FrameWriter};
use super::run::HostContext;
use super::run_work::{
    current_run, run_cancel_signal, run_cancel_state, CancelCause, HostStopState, HostWork,
    RunCancelState, WorkStopCause,
};
use super::seats::{serve_isolated, serve_shared_async, serve_shared_thread, PollWatch, ServeEnd};
use super::start_tests::{
    lock_the_row, remove_task, seed_claimed, until_a_backend_waits_in, WORKER_ID,
};
use crate::broker::{OrphanSelfHeal, PostgresBroker};
use crate::core::registry::task::TaskRegistry;
use crate::core::task::fn_trait::{
    AsyncTaskFn, BlockingTaskFn, RawTaskResult, RegisteredTask, TaskMeta,
};
use crate::core::task::result::TaskResult;
use crate::events::{AttemptNumber, AttemptStart, QueueName, RunTrace};
use crate::test_support::migrated_pool;
use crate::test_support::sql::{self, TestPool};
use crate::worker::executor::protocol::{
    decode_child_frame, CancelGraceMs, CancelTarget, CancelledBody, CancelledLoop, ChildFrame,
    DispatchNumber, FrameReader, IsolatedWork, NoLeftWork, ParentFrame, RunEnd, RunFrame, RunRef,
    SeatEnd, SharedRunFrame, SharedWork, TaskInputText, TaskTimeoutMs,
};

/// The task of the row that `seed_claimed` writes.
const TASK_NAME: &str = "app.tasks.start";
/// The longest wait for one child frame.
const FRAME_WAIT: Duration = Duration::from_secs(20);
/// The polls of a wait for a body to start, and the time between two.
const START_POLLS: u32 = 400;
const START_POLL_INTERVAL: Duration = Duration::from_millis(25);
/// The polls of a blocking body that reads its cancel state, and the time
/// between two: about 10 s.
const STATE_POLLS: u32 = 400;
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(25);
/// The deadline of a run that must not reach it.
const LONG_TIMEOUT_MS: u64 = 600_000;
/// The deadline of a run that reaches it: the smallest stored timeout.
const SHORT_TIMEOUT_MS: u64 = 1_000;
/// The smallest cancel grace.
const SHORT_GRACE_MS: u64 = 100;
/// The time a test waits for a frame that must not come.
const FRAME_READ_PAUSE: Duration = Duration::from_millis(300);
/// The polls of a wait for the serve loop to read every frame sent to it,
/// and the time between two: about 10 s.
const READ_POLLS: u32 = 400;
const READ_POLL_INTERVAL: Duration = Duration::from_millis(25);
/// The dispatch of a cancel that names no run of these tests.
const PROBE_DISPATCH: u64 = 99;
/// The name of the host-held work that the test bodies register.
const HOST_WORK_NAME: &str = "host.promise";
/// The time after which a host ends its work in the tests where it ends.
const HOST_WORK_END_DELAY: Duration = Duration::from_millis(200);
/// A cancel grace far longer than [`HOST_WORK_END_DELAY`].
const LONG_GRACE_MS: u64 = 2_000;

/// What the bodies of one test saw: how many started, and the cancel state
/// each read when it ended.
#[derive(Clone, Default)]
struct BodyLog {
    started: Arc<AtomicU32>,
    states: Arc<Mutex<Vec<RunCancelState>>>,
}

impl BodyLog {
    fn note_start(&self) {
        let _earlier = self.started.fetch_add(1, Ordering::AcqRel);
    }

    fn note_state(&self, state: RunCancelState) {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(state);
    }

    fn starts(&self) -> u32 {
        self.started.load(Ordering::Acquire)
    }

    fn states(&self) -> Vec<RunCancelState> {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Return once a body started.
    async fn until_started(&self) {
        for _ in 0..START_POLLS {
            match self.starts() {
                0 => tokio::time::sleep(START_POLL_INTERVAL).await,
                _ => return,
            }
        }
        panic!("no body started");
    }
}

fn returns_one() -> RawTaskResult {
    TaskResult::Ok(b"1".to_vec())
}

/// An async body that waits for the cancel signal of its run, notes its
/// cancel state, and returns a value: the value is dropped.
struct WaitsForCancel {
    log: BodyLog,
}

impl AsyncTaskFn for WaitsForCancel {
    fn execute(&self, _args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        Box::pin(async move {
            self.log.note_start();
            let signal = run_cancel_signal().expect("a body reads the signal of its run");
            signal.cancelled().await;
            self.log.note_state(run_cancel_state());
            returns_one()
        })
    }
}

/// An async body that ignores the cancel and sleeps far past every bound of
/// these tests.
struct IgnoresCancel {
    log: BodyLog,
}

impl AsyncTaskFn for IgnoresCancel {
    fn execute(&self, _args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        Box::pin(async move {
            self.log.note_start();
            tokio::time::sleep(Duration::from_secs(600)).await;
            returns_one()
        })
    }
}

/// A blocking body that reads its cancel state until a cancel is requested,
/// at most [`STATE_POLLS`] times, notes the state, and returns a value.
struct PollsCancelState {
    log: BodyLog,
}

impl BlockingTaskFn for PollsCancelState {
    fn execute(&self, _args: &[u8]) -> RawTaskResult {
        self.log.note_start();
        let mut state = run_cancel_state();
        for _ in 0..STATE_POLLS {
            match state {
                RunCancelState::Requested { .. } => break,
                RunCancelState::NotRequested | RunCancelState::NotInARun => {
                    std::thread::sleep(STATE_POLL_INTERVAL);
                    state = run_cancel_state();
                }
            }
        }
        self.log.note_state(state);
        returns_one()
    }
}

/// The host-held work of a test body, kept outside the body's future, as a
/// host keeps it in its run table.
#[derive(Clone, Default)]
struct HostSlot {
    work: Arc<Mutex<Option<HostWork>>>,
}

impl HostSlot {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<HostWork>> {
        self.work.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn put(&self, work: HostWork) {
        let _earlier = self.lock().replace(work);
    }

    fn stop_state(&self) -> HostStopState {
        match &*self.lock() {
            Some(work) => work.stop_state(),
            None => panic!("the body registered no host-held work"),
        }
    }

    fn stop_rounds(&self) -> u32 {
        match &*self.lock() {
            Some(work) => work.stop_rounds(),
            None => panic!("the body registered no host-held work"),
        }
    }

    /// End the host-held work, as a host does when its work settles.
    fn end(&self) {
        let work = self.lock().take();
        match work {
            Some(work) => work.end(),
            None => panic!("the body registered no host-held work"),
        }
    }
}

/// Host-held work of the run of the calling async body.
fn register_host_work() -> HostWork {
    current_run()
        .expect("an async body takes the handle of its run")
        .register_host_work(HOST_WORK_NAME)
        .expect("an open run takes host-held work")
}

/// An async body that registers host-held work, keeps it in `slot`, and
/// ignores the cancel past every bound of these tests.
struct HoldsHostWork {
    log: BodyLog,
    slot: HostSlot,
}

impl AsyncTaskFn for HoldsHostWork {
    fn execute(&self, _args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        Box::pin(async move {
            self.slot.put(register_host_work());
            self.log.note_start();
            tokio::time::sleep(Duration::from_secs(600)).await;
            returns_one()
        })
    }
}

/// An async body that registers host-held work, keeps it in `slot`, and
/// returns: the work is left.
struct LeavesHostWork {
    log: BodyLog,
    slot: HostSlot,
}

impl AsyncTaskFn for LeavesHostWork {
    fn execute(&self, _args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        Box::pin(async move {
            self.slot.put(register_host_work());
            self.log.note_start();
            returns_one()
        })
    }
}

/// When the body of [`HostEndsItsWork`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyReturn {
    /// At once: the work is left, and the host ends it later.
    BeforeTheWorkEnds,
    /// Once the host ended the work.
    AfterTheWorkEnded,
}

/// An async body that registers host-held work and hands it to a plain OS
/// thread, which ends it after [`HOST_WORK_END_DELAY`].
struct HostEndsItsWork {
    log: BodyLog,
    returns: BodyReturn,
}

impl AsyncTaskFn for HostEndsItsWork {
    fn execute(&self, _args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        Box::pin(async move {
            let work = register_host_work();
            let (ended, ended_seen) = tokio::sync::oneshot::channel();
            let _host = std::thread::spawn(move || {
                std::thread::sleep(HOST_WORK_END_DELAY);
                work.end();
                let _receiver_gone = ended.send(());
            });
            self.log.note_start();
            match self.returns {
                BodyReturn::BeforeTheWorkEnds => {}
                BodyReturn::AfterTheWorkEnded => ended_seen
                    .await
                    .expect("the host thread ends the work and reports it"),
            }
            returns_one()
        })
    }
}

/// The names of the work that a report names.
fn names_of(names: &[crate::worker::executor::protocol::LeftoverName]) -> Vec<&str> {
    names.iter().map(|name| name.as_str()).collect()
}

fn async_task(task: impl AsyncTaskFn) -> RegisteredTask {
    RegisteredTask::Async {
        task: Arc::new(task),
        meta: TaskMeta::default(),
    }
}

fn blocking_task(task: impl BlockingTaskFn) -> RegisteredTask {
    RegisteredTask::Blocking {
        task: Arc::new(task),
        meta: TaskMeta::default(),
    }
}

/// The child frames that the serve loop writes, read on a thread of their
/// own.
struct ChildFrames {
    frames: mpsc::Receiver<ChildFrame>,
}

impl ChildFrames {
    fn start(socket: UnixStream) -> Self {
        let (sender, frames) = mpsc::channel(64);
        std::thread::spawn(move || {
            let mut reader = FrameReader::new();
            let mut stream = &socket;
            while let Ok(raw) = read_frame(&mut stream, &mut reader) {
                let frame = decode_child_frame(&raw.meta, raw.body)
                    .expect("the child writes frames that decode");
                if sender.blocking_send(frame).is_err() {
                    return;
                }
            }
        });
        Self { frames }
    }

    async fn next(&mut self) -> ChildFrame {
        tokio::time::timeout(FRAME_WAIT, self.frames.recv())
            .await
            .expect("a child frame within the wait")
            .expect("the frame reader runs")
    }

    /// No frame comes within `wait`.
    async fn none_within(&mut self, wait: Duration) {
        if let Ok(Some(frame)) = tokio::time::timeout(wait, self.frames.recv()).await {
            panic!("no frame was due, got {frame:?}");
        }
    }
}

/// A child context on `pool` with the task `task` of [`TASK_NAME`], and the
/// frames that it writes.
fn context_with(pool: &TestPool, task: RegisteredTask) -> (HostContext, ChildFrames) {
    let mut registry = TaskRegistry::new();
    registry
        .register(TASK_NAME, task)
        .expect("one task registers");
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    let writer = FrameWriter::start(ours, 16).expect("the frame writer starts");
    let context = HostContext {
        broker: Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        )),
        app: Arc::new(ChildApp {
            registry: Arc::new(registry),
            hostname: "cancel-host".to_owned(),
            start_hook: ChildStartHook::NoHook,
            producer_manifest: super::producer::test_producer_manifest(),
            log_setup: ChildLogSetup::NoSetup,
            family: crate::ClientFamily::Rust,
            body_scope: super::BodyScope::NoScope,
            handler_child: super::HandlerChildApp::without_handlers(),
        }),
        pid: 4242,
        writer,
    };
    (context, ChildFrames::start(theirs))
}

fn run_frame(task_id: Uuid, claimed_at: DateTime<Utc>) -> RunFrame {
    RunFrame {
        task_id,
        task_name: TASK_NAME.to_owned(),
        queue_name: QueueName::cut("default"),
        attempt: AttemptNumber::SMALLEST,
        worker_id: WORKER_ID.to_owned(),
        claimed_at,
        orphan_self_heal: OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        trace: RunTrace::NoCarrier,
        input: TaskInputText::new("{}".to_owned()),
    }
}

fn dispatch(number: u64) -> DispatchNumber {
    DispatchNumber::new(number).expect("a dispatch number")
}

fn shared_run_frame(run: RunRef, claimed_at: DateTime<Utc>, work: SharedWork) -> SharedRunFrame {
    SharedRunFrame {
        dispatch: run.dispatch,
        task_id: run.task_id,
        task_name: TASK_NAME.to_owned(),
        queue_name: QueueName::cut("default"),
        attempt: AttemptNumber::SMALLEST,
        worker_id: WORKER_ID.to_owned(),
        claimed_at,
        orphan_self_heal: OrphanSelfHeal::Off,
        work,
        trace: RunTrace::NoCarrier,
        input: TaskInputText::new("{}".to_owned()),
    }
}

fn async_work(timeout_ms: u64) -> SharedWork {
    async_work_with_grace(timeout_ms, SHORT_GRACE_MS)
}

fn async_work_with_grace(timeout_ms: u64, grace_ms: u64) -> SharedWork {
    SharedWork::AsyncTask {
        timeout_ms: TaskTimeoutMs::new(timeout_ms).expect("a task timeout"),
        cancel_grace_ms: CancelGraceMs::new(grace_ms).expect("a cancel grace"),
    }
}

fn thread_work(timeout_ms: u64) -> SharedWork {
    SharedWork::ThreadTask {
        timeout_ms: TaskTimeoutMs::new(timeout_ms).expect("a task timeout"),
    }
}

/// The finalizing marker of the live row of `task_id`.
async fn finalizing_at(pool: &TestPool, task_id: Uuid) -> Option<DateTime<Utc>> {
    sql::query_scalar("SELECT finalizing_at FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the finalizing marker")
}

fn operator() -> RunCancelState {
    RunCancelState::Requested {
        cause: CancelCause::Operator,
    }
}

/// A cancel of an isolated task that no test runs.
fn isolated_probe() -> CancelTarget {
    CancelTarget::Isolated {
        task_id: Uuid::new_v4(),
    }
}

/// A cancel of a shared run that no test runs.
fn shared_probe() -> CancelTarget {
    CancelTarget::Shared(RunRef {
        dispatch: dispatch(PROBE_DISPATCH),
        task_id: Uuid::new_v4(),
    })
}

/// Return once the serve loop handled every frame sent before this call.
/// A serve loop handles its frames one at a time, in order, and reads a
/// frame only after it handled the one before. `probe` is a cancel that
/// names no run, which the serve loop ignores; it is sent last, so the
/// channel is empty again only after every earlier frame was handled.
async fn until_the_frames_are_handled(parent: &mpsc::Sender<ParentFrame>, probe: CancelTarget) {
    parent
        .send(ParentFrame::CancelRun(probe))
        .await
        .expect("the serve loop reads");
    for _ in 0..READ_POLLS {
        match parent.capacity() == parent.max_capacity() {
            true => return,
            false => tokio::time::sleep(READ_POLL_INTERVAL).await,
        }
    }
    panic!("the serve loop did not read the frames sent to it");
}

/// An isolated async body gets the operator cancel as its signal; what it
/// returns is dropped, `run_cancelled` replaces the result, and no marker
/// is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_async_body_gets_the_cancel_and_the_run_ends_cancelled() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(WaitsForCancel { log: log.clone() }));
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::Run(run_frame(task_id, generation)))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Isolated { task_id }))
            .await
            .expect("the serve loop reads");
        let frame = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        frame
    };
    let (end, frame) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    match frame {
        ChildFrame::RunCancelled(cancelled) => {
            assert_eq!(cancelled.task_id, task_id);
            assert_eq!(cancelled.body, CancelledBody::Ran);
            assert_eq!(
                cancelled.task_loop,
                CancelledLoop::NoLeftWork(NoLeftWork::NoTaskLoop)
            );
        }
        other => panic!("expected run_cancelled, got {other:?}"),
    }
    assert_eq!(log.states(), vec![operator()]);
    assert_eq!(finalizing_at(&pool, task_id).await, None);
    remove_task(&pool, task_id).await;
}

/// An isolated blocking body reads the operator cancel in its cancel state
/// on its thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_blocking_body_reads_the_operator_cancel_on_its_thread() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) =
        context_with(&pool, blocking_task(PollsCancelState { log: log.clone() }));
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::Run(run_frame(task_id, generation)))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Isolated { task_id }))
            .await
            .expect("the serve loop reads");
        let frame = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        frame
    };
    let (end, frame) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert!(
        matches!(
            &frame,
            ChildFrame::RunCancelled(cancelled) if cancelled.body == CancelledBody::Ran
        ),
        "{frame:?}"
    );
    assert_eq!(log.states(), vec![operator()]);
    remove_task(&pool, task_id).await;
}

/// A cancel that comes while the start write waits on the row lock keeps
/// the body from starting: `run_cancelled` with body `not_started`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_cancel_before_the_body_keeps_the_body_from_starting() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(WaitsForCancel { log: log.clone() }));
    let mut lock_holder = pool.begin().await.expect("begin the lock holder");
    lock_the_row(&mut lock_holder, task_id).await;
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::Run(run_frame(task_id, generation)))
            .await
            .expect("the serve loop reads");
        until_a_backend_waits_in(&pool, "horsies_start_attempt").await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Isolated { task_id }))
            .await
            .expect("the serve loop reads");
        until_the_frames_are_handled(&parent, isolated_probe()).await;
        lock_holder.rollback().await.expect("end the row lock");
        let frame = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        frame
    };
    let (end, frame) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    match frame {
        ChildFrame::RunCancelled(cancelled) => {
            assert_eq!(cancelled.body, CancelledBody::NotStarted);
        }
        other => panic!("expected run_cancelled, got {other:?}"),
    }
    assert_eq!(log.starts(), 0);
    remove_task(&pool, task_id).await;
}

/// A cancel that names another task is ignored: the run ends with its
/// result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_cancel_of_another_task_is_ignored() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) =
        context_with(&pool, blocking_task(PollsCancelState { log: log.clone() }));
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::Run(run_frame(task_id, generation)))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Isolated {
                task_id: Uuid::new_v4(),
            }))
            .await
            .expect("the serve loop reads");
        let frame = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        frame
    };
    let (end, frame) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    match frame {
        ChildFrame::Result(result) => {
            assert!(matches!(result.end, RunEnd::Result(_)), "{result:?}");
        }
        other => panic!("expected the result of the run, got {other:?}"),
    }
    assert_eq!(log.states(), vec![RunCancelState::NotRequested]);
    remove_task(&pool, task_id).await;
}

/// An isolated child with no run in flight ignores a cancel, and a cancel
/// for a shared run is a bad parent frame for it.
#[tokio::test]
async fn an_isolated_child_ignores_a_late_cancel_and_refuses_a_shared_target() {
    let pool = migrated_pool().await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(WaitsForCancel { log }));
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Isolated {
                task_id: Uuid::new_v4(),
            }))
            .await
            .expect("the serve loop reads");
        child.none_within(FRAME_READ_PAUSE).await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(RunRef {
                dispatch: dispatch(1),
                task_id: Uuid::new_v4(),
            })))
            .await
            .expect("the serve loop reads");
    };
    let (end, ()) = tokio::join!(serve, script);
    assert_eq!(end, ServeEnd::BadParentFrame);
}

/// A shared async body gets the operator cancel: `cancel_fired`, then the
/// end cancelled with its start, and no marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_body_gets_the_cancel_and_reports_cancel_fired_first() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(WaitsForCancel { log: log.clone() }));
    let run = RunRef {
        dispatch: dispatch(1),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        let work = async_work(LONG_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(run)))
            .await
            .expect("the serve loop reads");
        let fired = child.next().await;
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (fired, ended)
    };
    let (end, (fired, ended)) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert_eq!(fired, ChildFrame::CancelFired(run));
    match ended {
        ChildFrame::SharedResult(result) => {
            assert_eq!(result.run, run);
            assert_eq!(result.end, SeatEnd::Cancelled(CancelledBody::Ran));
            assert!(matches!(result.started_at, AttemptStart::Reported { .. }));
        }
        other => panic!("expected the end of the run, got {other:?}"),
    }
    assert_eq!(log.states(), vec![operator()]);
    assert_eq!(finalizing_at(&pool, task_id).await, None);
    remove_task(&pool, task_id).await;
}

/// A shared async body that ignores the cancel is dropped after the cancel
/// grace: `cancel_fired`, `second_cancel`, then the end cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_body_that_ignores_the_cancel_is_dropped_after_the_grace() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(IgnoresCancel { log: log.clone() }));
    let run = RunRef {
        dispatch: dispatch(2),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        let work = async_work(LONG_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(run)))
            .await
            .expect("the serve loop reads");
        let reported = vec![child.next().await, child.next().await, child.next().await];
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        reported
    };
    let (end, reported) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert_eq!(reported[0], ChildFrame::CancelFired(run));
    assert_eq!(reported[1], ChildFrame::SecondCancel(run));
    match &reported[2] {
        ChildFrame::SharedResult(result) => {
            assert_eq!(result.end, SeatEnd::Cancelled(CancelledBody::Ran));
        }
        other => panic!("expected the end of the run, got {other:?}"),
    }
    remove_task(&pool, task_id).await;
}

/// The deadline of a shared async run fires first: a later cancel is
/// ignored, and the run ends timed out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_cancel_after_the_deadline_is_ignored() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(IgnoresCancel { log: log.clone() }));
    let run = RunRef {
        dispatch: dispatch(3),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        let work = async_work(SHORT_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        let deadline = child.next().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(run)))
            .await
            .expect("the serve loop reads");
        let reported = vec![deadline, child.next().await, child.next().await];
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        reported
    };
    let (end, reported) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert_eq!(reported[0], ChildFrame::DeadlineFired(run));
    assert_eq!(reported[1], ChildFrame::SecondCancel(run));
    match &reported[2] {
        ChildFrame::SharedResult(result) => assert_eq!(result.end, SeatEnd::TimedOut),
        other => panic!("expected the end of the run, got {other:?}"),
    }
    remove_task(&pool, task_id).await;
}

/// A cancel for a dispatch that the shared child does not run is ignored.
#[tokio::test]
async fn a_shared_cancel_of_an_unknown_run_is_ignored() {
    let pool = migrated_pool().await;
    let log = BodyLog::default();
    let (context, mut child) = context_with(&pool, async_task(WaitsForCancel { log }));
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(RunRef {
                dispatch: dispatch(9),
                task_id: Uuid::new_v4(),
            })))
            .await
            .expect("the serve loop reads");
        child.none_within(FRAME_READ_PAUSE).await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
    };
    let (end, ()) = tokio::join!(serve, script);
    assert_eq!(end, ServeEnd::Stopped);
}

/// A shared thread body reads the operator cancel in its cancel state; the
/// thread runs on, and its return ends the run cancelled after
/// `cancel_fired`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_thread_body_reads_the_cancel_and_ends_cancelled() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) =
        context_with(&pool, blocking_task(PollsCancelState { log: log.clone() }));
    let run = RunRef {
        dispatch: dispatch(4),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_thread(context, &mut frames, 2);
    let script = async {
        let work = thread_work(LONG_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(run)))
            .await
            .expect("the serve loop reads");
        let fired = child.next().await;
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (fired, ended)
    };
    let (end, (fired, ended)) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert_eq!(fired, ChildFrame::CancelFired(run));
    match ended {
        ChildFrame::SharedResult(result) => {
            assert_eq!(result.end, SeatEnd::Cancelled(CancelledBody::Ran));
            assert!(matches!(result.started_at, AttemptStart::Reported { .. }));
        }
        other => panic!("expected the end of the run, got {other:?}"),
    }
    assert_eq!(log.states(), vec![operator()]);
    assert_eq!(finalizing_at(&pool, task_id).await, None);
    remove_task(&pool, task_id).await;
}

/// The deadline of a shared thread run sets the deadline cause in the cancel
/// state of its body; the run ends timed out, and a later cancel is ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_thread_deadline_sets_the_deadline_cause_and_a_later_cancel_is_ignored() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (context, mut child) =
        context_with(&pool, blocking_task(PollsCancelState { log: log.clone() }));
    let run = RunRef {
        dispatch: dispatch(5),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_thread(context, &mut frames, 2);
    let script = async {
        let work = thread_work(SHORT_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        let deadline = child.next().await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(run)))
            .await
            .expect("the serve loop reads");
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (deadline, ended)
    };
    let (end, (deadline, ended)) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert_eq!(deadline, ChildFrame::DeadlineFired(run));
    match ended {
        ChildFrame::SharedResult(result) => assert_eq!(result.end, SeatEnd::TimedOut),
        other => panic!("expected the end of the run, got {other:?}"),
    }
    assert_eq!(
        log.states(),
        vec![RunCancelState::Requested {
            cause: CancelCause::Deadline
        }]
    );
    remove_task(&pool, task_id).await;
}

/// Which shared serve loop a test drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedKind {
    Async,
    Thread,
}

/// A cancel that comes while the start write of a shared run waits on the
/// row lock keeps the body from starting: `cancel_fired`, then the end
/// cancelled with body `not_started` and its start.
async fn a_shared_cancel_before_the_body_keeps_the_body_from_starting(kind: SharedKind) {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let (task, work) = match kind {
        SharedKind::Async => (
            async_task(WaitsForCancel { log: log.clone() }),
            async_work(LONG_TIMEOUT_MS),
        ),
        SharedKind::Thread => (
            blocking_task(PollsCancelState { log: log.clone() }),
            thread_work(LONG_TIMEOUT_MS),
        ),
    };
    let (context, mut child) = context_with(&pool, task);
    let run = RunRef {
        dispatch: dispatch(6),
        task_id,
    };
    let mut lock_holder = pool.begin().await.expect("begin the lock holder");
    lock_the_row(&mut lock_holder, task_id).await;
    let (parent, mut frames) = mpsc::channel(16);
    let serve = async {
        match kind {
            SharedKind::Async => {
                let watch = PollWatch::without_thread(Instant::now());
                serve_shared_async(context, watch, &mut frames, 2).await
            }
            SharedKind::Thread => serve_shared_thread(context, &mut frames, 2).await,
        }
    };
    let script = async {
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        until_a_backend_waits_in(&pool, "horsies_start_attempt").await;
        parent
            .send(ParentFrame::CancelRun(CancelTarget::Shared(run)))
            .await
            .expect("the serve loop reads");
        until_the_frames_are_handled(&parent, shared_probe()).await;
        lock_holder.rollback().await.expect("end the row lock");
        let fired = child.next().await;
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (fired, ended)
    };
    let (end, (fired, ended)) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped, "{kind:?}");
    assert_eq!(fired, ChildFrame::CancelFired(run), "{kind:?}");
    match ended {
        ChildFrame::SharedResult(result) => {
            assert_eq!(result.end, SeatEnd::Cancelled(CancelledBody::NotStarted));
            assert!(matches!(result.started_at, AttemptStart::Reported { .. }));
        }
        other => panic!("{kind:?}: expected the end of the run, got {other:?}"),
    }
    assert_eq!(log.starts(), 0, "{kind:?}");
    remove_task(&pool, task_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_cancel_before_the_body_keeps_the_body_from_starting() {
    a_shared_cancel_before_the_body_keeps_the_body_from_starting(SharedKind::Async).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_thread_cancel_before_the_body_keeps_the_body_from_starting() {
    a_shared_cancel_before_the_body_keeps_the_body_from_starting(SharedKind::Thread).await;
}

/// Host-held work past the grace after the deadline: `second_cancel`, each
/// round asks the work to stop with the deadline cause, `run_stuck` names
/// the work, and the run keeps its seat until the host ends the work; the
/// run then ends timed out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_run_keeps_its_seat_until_the_host_ends_its_work() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let slot = HostSlot::default();
    let task = async_task(HoldsHostWork {
        log: log.clone(),
        slot: slot.clone(),
    });
    let (context, mut child) = context_with(&pool, task);
    let run = RunRef {
        dispatch: dispatch(7),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        let work = async_work(SHORT_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        let reported = vec![child.next().await, child.next().await, child.next().await];
        child.none_within(FRAME_READ_PAUSE).await;
        let stop = (slot.stop_state(), slot.stop_rounds());
        slot.end();
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (reported, stop, ended)
    };
    let (end, (reported, stop, ended)) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert_eq!(reported[0], ChildFrame::DeadlineFired(run));
    assert_eq!(reported[1], ChildFrame::SecondCancel(run));
    match &reported[2] {
        ChildFrame::RunStuck(stuck) => {
            assert_eq!(stuck.run, run);
            assert_eq!(stuck.work.tasks(), 1);
            assert_eq!(names_of(stuck.work.names()), [HOST_WORK_NAME]);
        }
        other => panic!("expected run_stuck, got {other:?}"),
    }
    let deadline = WorkStopCause::RunCancelled {
        cause: CancelCause::Deadline,
    };
    assert_eq!(
        stop,
        (HostStopState::Requested { cause: deadline }, 3),
        "each of the three rounds asks the work to stop"
    );
    match ended {
        ChildFrame::SharedResult(result) => assert_eq!(result.end, SeatEnd::TimedOut),
        other => panic!("expected the end of the run, got {other:?}"),
    }
    remove_task(&pool, task_id).await;
}

/// Host-held work that a shared async body left and that does not end:
/// `leftover_cancel`, the rounds ask it to stop with the left-work cause,
/// `leftover_stuck` names it, and the run keeps its seat until the host
/// ends it; the run then ends with what the body returned, and no marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_run_stops_left_host_work_with_the_left_work_cause() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let slot = HostSlot::default();
    let task = async_task(LeavesHostWork {
        log: log.clone(),
        slot: slot.clone(),
    });
    let (context, mut child) = context_with(&pool, task);
    let run = RunRef {
        dispatch: dispatch(8),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        let work = async_work(LONG_TIMEOUT_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        log.until_started().await;
        let reported = vec![child.next().await, child.next().await];
        child.none_within(FRAME_READ_PAUSE).await;
        let stop = slot.stop_state();
        slot.end();
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (reported, stop, ended)
    };
    let (end, (reported, stop, ended)) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    assert!(
        matches!(&reported[0], ChildFrame::LeftoverCancel(cancel) if cancel.run == run),
        "{:?}",
        reported[0]
    );
    match &reported[1] {
        ChildFrame::LeftoverStuck(stuck) => {
            assert_eq!(stuck.run, run);
            assert_eq!(names_of(stuck.work.names()), [HOST_WORK_NAME]);
        }
        other => panic!("expected leftover_stuck, got {other:?}"),
    }
    assert_eq!(
        stop,
        HostStopState::Requested {
            cause: WorkStopCause::LeftWork
        }
    );
    match ended {
        ChildFrame::SharedResult(result) => {
            assert!(
                matches!(result.end, SeatEnd::Finished(RunEnd::Result(_))),
                "{result:?}"
            );
        }
        other => panic!("expected the end of the run, got {other:?}"),
    }
    assert_eq!(finalizing_at(&pool, task_id).await, None);
    remove_task(&pool, task_id).await;
}

/// Host-held work that a shared async body left and that the host ends
/// within the cancel grace: no left-work report, the end that the body
/// returned, and the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_async_run_whose_host_work_ends_in_the_grace_reports_no_left_work() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let task = async_task(HostEndsItsWork {
        log: log.clone(),
        returns: BodyReturn::BeforeTheWorkEnds,
    });
    let (context, mut child) = context_with(&pool, task);
    let run = RunRef {
        dispatch: dispatch(9),
        task_id,
    };
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_shared_async(
        context,
        PollWatch::without_thread(Instant::now()),
        &mut frames,
        2,
    );
    let script = async {
        let work = async_work_with_grace(LONG_TIMEOUT_MS, LONG_GRACE_MS);
        parent
            .send(ParentFrame::SharedRun(shared_run_frame(
                run, generation, work,
            )))
            .await
            .expect("the serve loop reads");
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        ended
    };
    let (end, ended) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    match ended {
        ChildFrame::SharedResult(result) => {
            assert_eq!(result.run, run);
            assert!(
                matches!(result.end, SeatEnd::Finished(RunEnd::Result(_))),
                "{result:?}"
            );
        }
        other => panic!("expected the end of the run and no left-work report, got {other:?}"),
    }
    assert_eq!(log.starts(), 1);
    assert!(finalizing_at(&pool, task_id).await.is_some());
    remove_task(&pool, task_id).await;
}

/// An isolated body whose host-held work ends before the body returns: no
/// left-work report, the result, and the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_run_whose_host_work_ended_reports_its_result() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let task = async_task(HostEndsItsWork {
        log: log.clone(),
        returns: BodyReturn::AfterTheWorkEnded,
    });
    let (context, mut child) = context_with(&pool, task);
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::Run(run_frame(task_id, generation)))
            .await
            .expect("the serve loop reads");
        let ended = child.next().await;
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        ended
    };
    let (end, ended) = tokio::join!(serve, script);

    assert_eq!(end, ServeEnd::Stopped);
    match ended {
        ChildFrame::Result(result) => {
            assert_eq!(result.task_id, task_id);
            assert!(matches!(result.end, RunEnd::Result(_)), "{result:?}");
        }
        other => panic!("expected the result and no left-work report, got {other:?}"),
    }
    assert_eq!(log.starts(), 1);
    assert!(finalizing_at(&pool, task_id).await.is_some());
    remove_task(&pool, task_id).await;
}

/// Host-held work that an isolated body left and that does not end:
/// `body_returned` names it, the rounds ask it to stop with the left-work
/// cause, and `left_work_stuck` names it; no marker is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_run_reports_left_host_work_that_does_not_end_as_stuck() {
    let pool = migrated_pool().await;
    let (task_id, generation) = seed_claimed(&pool, false).await;
    let log = BodyLog::default();
    let slot = HostSlot::default();
    let task = async_task(LeavesHostWork {
        log: log.clone(),
        slot: slot.clone(),
    });
    let (context, mut child) = context_with(&pool, task);
    let (parent, mut frames) = mpsc::channel(16);
    let serve = serve_isolated(context, &mut frames);
    let script = async {
        parent
            .send(ParentFrame::Run(run_frame(task_id, generation)))
            .await
            .expect("the serve loop reads");
        let reported = vec![child.next().await, child.next().await];
        let stop = slot.stop_state();
        parent
            .send(ParentFrame::Stop)
            .await
            .expect("the serve loop reads");
        (reported, stop)
    };
    let (end, (reported, stop)) = tokio::join!(serve, script);
    slot.end();

    assert_eq!(end, ServeEnd::Stopped);
    match &reported[0] {
        ChildFrame::BodyReturned(returned) => {
            assert_eq!(returned.task_id, task_id);
            assert_eq!(names_of(returned.left.names()), [HOST_WORK_NAME]);
        }
        other => panic!("expected body_returned, got {other:?}"),
    }
    match &reported[1] {
        ChildFrame::LeftWorkStuck(stuck) => {
            assert_eq!(stuck.task_id, task_id);
            assert_eq!(names_of(stuck.still.names()), [HOST_WORK_NAME]);
        }
        other => panic!("expected left_work_stuck, got {other:?}"),
    }
    assert_eq!(
        stop,
        HostStopState::Requested {
            cause: WorkStopCause::LeftWork
        }
    );
    assert_eq!(finalizing_at(&pool, task_id).await, None);
    remove_task(&pool, task_id).await;
}
