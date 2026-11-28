//! The task bodies of a JavaScript child: the core's task function forms,
//! the run table, the cancel watch, and the host-held work of a run.
//!
//! The core calls a body through `AsyncTaskFn::execute` (an `ISOLATED` or a
//! `SHARED_ASYNC` task) or `BlockingTaskFn::execute` (a `SHARED_THREAD`
//! task). The JavaScript main thread runs the body of a post, or, in a
//! `SHARED_THREAD` child, gives it to a free seat isolate and settles the run
//! with the isolate's result; a seat isolate never loads this addon. Each
//! call:
//! 1. reads the run's attempt facts (fencing token and retry budget) and cancel signal; an async run also
//!    takes its run handle and registers the body as host-held work, named
//!    by the task name;
//! 2. enters the run in the run table under a new run key;
//! 3. posts `runBody` to the JavaScript main thread and does not wait for
//!    that thread;
//! 4. waits for the settle call of the run (`settleBodyValue`,
//!    `settleBodyError`). While it waits it watches the cancel signal: at the
//!    fire it keeps the cause in the run table (`runCancelState` reads it)
//!    and posts `cancelRun`.
//!
//! - A settle call ends the body's host-held work first, then gives the
//!   result to the waiting call. So the work of a body that ignores its
//!   signal stays in the run until the body settles or the child is
//!   replaced: a cancel round asks the work to stop and does not remove it.
//! - A body future that the core drops (the rounds of a shared async child)
//!   leaves the run in the table: the body runs on in JavaScript, and its
//!   settle call ends its work.
//! - `registerSpawn` adds a piece of host-held work to an async run
//!   (`runtime.spawn`) and `endSpawn` ends it. A round's stop request of a
//!   piece is posted as `stopWork`. The run's work place (its handle) stays
//!   until the body settled and its last piece ended, so work that spawned
//!   work starts after the body returned belongs to the run, as in Rust and
//!   Python; the core answers `RunEnded` once the run is over.
//! - A body call whose `runBody` post Node-API does not queue is a defect:
//!   the queue bound holds every post of a full child. The call panics with
//!   the cause, and the core stores the panic record as
//!   `UNHANDLED_EXCEPTION`.
//! - The tables are per process: one child host runs in a process.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use horsies_core::core::task::fn_trait::{AsyncTaskFn, BlockingTaskFn, RawTaskResult};
use horsies_core::frame_protocol::MAX_SEATS_PER_SHARED_ASYNC_CHILD;
use horsies_core::{
    current_run, run_cancel_signal, run_cancel_state, run_task_attempt, CancelCause, HostWork,
    HostWorkRefused, RunCancelSignal, RunCancelState, RunHandle, TaskAttempt, TaskResult,
    WorkStopCause, HOST_LOOP_TURN_TICK, MAX_RUN_WORK,
};
use napi::bindgen_prelude::{BigInt, Function};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Status;
use napi_derive::napi;
use tokio::sync::oneshot;

use crate::call::guard::sync_call;
use crate::call::node_api::{call_status, CallStatus};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::child::body_facts::{BodyEndRecord, BodyFacts, BodyFactsRecord};
use crate::sends::task_error::TaskErrorRecord;
use crate::values::boundary::{bigint_of_u64, u64_of_bigint};

/// Posts of one seat that can wait for the JavaScript thread at once: the
/// `runBody` and the `cancelRun` of its run, and the `cancelRun` and the
/// `bodyEnded` of the run before it, when that run settled before
/// JavaScript read them. The `bodyEnded` of a run comes after its settle
/// call, so JavaScript read the `runBody` of that run before it.
const RUN_POSTS_PER_SEAT: usize = 4;

/// The queue bound of the run posts: every post of a shared async child
/// with the most seats.
pub const RUN_POST_QUEUE_BOUND: usize = RUN_POSTS_PER_SEAT * seats_bound();

/// The queue bound of the stop posts of host-held work: the work of one run
/// at its bound. A post past it waits and tries again.
pub const WORK_STOP_QUEUE_BOUND: usize = MAX_RUN_WORK;

/// The tries of one stop post when its queue is full.
const WORK_STOP_POST_TRIES: u32 = 100;

/// The wait between two tries of a stop post.
const WORK_STOP_POST_PAUSE: Duration = Duration::from_millis(10);

/// How often the wait of a blocking body reads its cancel signal: the turn
/// tick of the host loop. The cancel then reaches the body through the
/// `cancelRun` post: on the JavaScript main thread, or in the cancel cell of
/// the body's seat isolate.
const BLOCKING_CANCEL_READ: Duration = HOST_LOOP_TURN_TICK;

/// The most seats of a shared async child, as a `usize`.
const fn seats_bound() -> usize {
    // The core's bound is 1,000: it fits every `usize`.
    MAX_SEATS_PER_SHARED_ASYNC_CHILD as usize
}

/// Why a run's cancel signal fired, as JavaScript reads it.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelCauseName {
    Deadline,
    Operator,
}

impl CancelCauseName {
    /// The name of the core's cause. Pure.
    fn of(cause: CancelCause) -> Self {
        match cause {
            CancelCause::Deadline => Self::Deadline,
            CancelCause::Operator => Self::Operator,
        }
    }
}

/// Why a cancel round asks a piece of host-held work to stop.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkStopCauseName {
    /// The run's signal fired at its deadline, and the work was alive after
    /// the grace.
    Deadline,
    /// An operator cancelled the run, and the work was alive after the
    /// grace.
    Operator,
    /// The body returned, and the work was alive after the grace for left
    /// work.
    LeftWork,
}

impl WorkStopCauseName {
    /// The name of the core's cause. Pure.
    fn of(cause: WorkStopCause) -> Self {
        match cause {
            WorkStopCause::RunCancelled {
                cause: CancelCause::Deadline,
            } => Self::Deadline,
            WorkStopCause::RunCancelled {
                cause: CancelCause::Operator,
            } => Self::Operator,
            WorkStopCause::LeftWork => Self::LeftWork,
        }
    }
}

/// What the core posts to the JavaScript main thread of a child.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "the value becomes a JavaScript object at the post, and napi-rs has no conversion of a Box"
)]
pub enum ChildPost {
    /// Run the body of `task_name` for the run `run_key`, with the stored
    /// input text. `attempt` is the fencing token's value, from 1;
    /// `max_retries` is the retry budget of the task row, from 0 to 20.
    RunBody {
        run_key: BigInt,
        task_name: String,
        input_json: String,
        task_id: String,
        attempt: u32,
        max_retries: u32,
        facts: BodyFactsRecord,
    },
    /// The cancel signal of the run fired.
    CancelRun {
        run_key: BigInt,
        cause: CancelCauseName,
    },
    /// A cancel round asks the host-held work `work_key` to stop.
    StopWork {
        work_key: BigInt,
        cause: WorkStopCauseName,
    },
    /// The core ended the body of the run of task `task_id`, attempt
    /// `attempt` (from 1), as `end`. Only a child that records its body
    /// facts gets it, once for each body that the core opened.
    BodyEnded {
        task_id: String,
        attempt: u32,
        end: BodyEndRecord,
    },
}

/// The threadsafe function of the posts: weak, never throws into Node-API,
/// at most `QUEUE` posts waiting for the JavaScript thread.
type PostFunction<const QUEUE: usize> =
    ThreadsafeFunction<ChildPost, (), ChildPost, Status, false, true, QUEUE>;

/// Why a post did not reach the queue of the JavaScript thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostRefused {
    QueueFull,
    Closing,
    NodeApi { status: Status },
}

impl fmt::Display for PostRefused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => formatter.write_str("the queue of the JavaScript thread is full"),
            Self::Closing => formatter.write_str("the JavaScript environment is closing"),
            Self::NodeApi { status } => write!(formatter, "Node-API refused the call: {status}"),
        }
    }
}

/// Whether Node-API queued a post, by its status. Pure.
fn post_queued(status: Status) -> Result<(), PostRefused> {
    match call_status(status) {
        CallStatus::Queued => Ok(()),
        CallStatus::QueueFull => Err(PostRefused::QueueFull),
        CallStatus::Closing => Err(PostRefused::Closing),
        CallStatus::Refused { status } => Err(PostRefused::NodeApi { status }),
    }
}

/// The posts of a child to its JavaScript main thread: one queue for the
/// posts of runs, one for the stop posts of host-held work, so a burst of
/// stop posts never fills the queue of a body call.
pub struct ChildPoster {
    runs: PostFunction<RUN_POST_QUEUE_BOUND>,
    work_stops: PostFunction<WORK_STOP_QUEUE_BOUND>,
}

impl ChildPoster {
    /// The poster of the package's dispatcher `function`. On the JavaScript
    /// thread.
    ///
    /// Failure exits: the `napi::Error` of a function that Node-API cannot
    /// turn into a threadsafe function.
    pub fn register(function: &Function<ChildPost, ()>) -> napi::Result<Self> {
        Ok(Self {
            runs: function
                .build_threadsafe_function::<ChildPost>()
                .callee_handled::<false>()
                .weak::<true>()
                .max_queue_size::<RUN_POST_QUEUE_BOUND>()
                .build()?,
            work_stops: function
                .build_threadsafe_function::<ChildPost>()
                .callee_handled::<false>()
                .weak::<true>()
                .max_queue_size::<WORK_STOP_QUEUE_BOUND>()
                .build()?,
        })
    }

    /// Post `post` of a run. Any thread.
    pub fn post_run(&self, post: ChildPost) -> Result<(), PostRefused> {
        post_queued(
            self.runs
                .call(post, ThreadsafeFunctionCallMode::NonBlocking),
        )
    }

    /// Post the stop request of a piece of host-held work. Any thread.
    fn post_work_stop(&self, post: ChildPost) -> Result<(), PostRefused> {
        post_queued(
            self.work_stops
                .call(post, ThreadsafeFunctionCallMode::NonBlocking),
        )
    }
}

/// Where the posts of runs go: the JavaScript main thread of the child, or
/// the recorder of a test.
pub trait RunPosts: Send + Sync {
    /// Post `post` of a run. Any thread.
    ///
    /// Failure exits: [`PostRefused`], when the post did not reach the
    /// queue; nothing was posted.
    fn post_run(&self, post: ChildPost) -> Result<(), PostRefused>;
}

impl RunPosts for ChildPoster {
    fn post_run(&self, post: ChildPost) -> Result<(), PostRefused> {
        ChildPoster::post_run(self, post)
    }
}

/// What every body of a child shares: the poster and the body facts.
pub struct ChildBodies {
    pub poster: Arc<ChildPoster>,
    pub facts: BodyFacts,
}

/// The cancel cause of a run as its watch saw it. The first cause stays.
struct CancelSeen(AtomicU8);

const SEEN_NOT_REQUESTED: u8 = 0;
const SEEN_DEADLINE: u8 = 1;
const SEEN_OPERATOR: u8 = 2;

impl CancelSeen {
    fn new() -> Self {
        Self(AtomicU8::new(SEEN_NOT_REQUESTED))
    }

    /// Keep `cause`, unless a cause is kept already.
    fn record(&self, cause: CancelCauseName) {
        let stored = match cause {
            CancelCauseName::Deadline => SEEN_DEADLINE,
            CancelCauseName::Operator => SEEN_OPERATOR,
        };
        let _first_cause_stays = self.0.compare_exchange(
            SEEN_NOT_REQUESTED,
            stored,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// The kept state. Pure read.
    fn state(&self) -> RunCancelStateRecord {
        match self.0.load(Ordering::Acquire) {
            SEEN_NOT_REQUESTED => RunCancelStateRecord::NotRequested,
            SEEN_DEADLINE => RunCancelStateRecord::Requested {
                cause: CancelCauseName::Deadline,
            },
            SEEN_OPERATOR => RunCancelStateRecord::Requested {
                cause: CancelCauseName::Operator,
            },
            other => unreachable!("a kept cancel cause holds the unknown state {other}"),
        }
    }
}

/// How the run waits for its settle call.
enum RunForm {
    /// An async run: the body's host-held work and the waiting call. Its
    /// work place is in [`PLACES`].
    Async {
        body: HostWork,
        done: oneshot::Sender<RawTaskResult>,
    },
    /// A blocking run: the waiting thread.
    Blocking { done: SyncSender<RawTaskResult> },
}

impl RunForm {
    /// End the body's work, then give `result` to the waiting call. A call
    /// that the core dropped has no reader; the result is left.
    fn finish(self, result: RawTaskResult) {
        match self {
            Self::Async { body, done, .. } => {
                body.end();
                let _no_reader_after_a_drop = done.send(result);
            }
            Self::Blocking { done } => {
                // The channel holds one result and this is its only send.
                let _no_reader_after_a_drop = done.send(result);
            }
        }
    }
}

/// One run in the table.
struct RunEntry {
    cancel: Arc<CancelSeen>,
    form: RunForm,
}

/// One piece of host-held work that JavaScript started with
/// `runtime.spawn`.
struct WorkEntry {
    run_key: u64,
    work: Arc<HostWork>,
    /// Ends the stop watch of the piece.
    ended: oneshot::Sender<()>,
}

static RUNS: LazyLock<Mutex<HashMap<u64, RunEntry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

static WORKS: LazyLock<Mutex<HashMap<u64, WorkEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Whether the body of an async run settled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyLife {
    Running,
    Settled,
}

/// The life of an async run's work place: its body, and its spawned
/// pieces that have not ended. The core bounds the pieces (`MAX_RUN_WORK`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RunLife {
    body: BodyLife,
    pieces: usize,
}

/// Whether a work place stays after a change of its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlaceAfter {
    Kept,
    Removed,
}

impl RunLife {
    /// A run whose body runs, with no piece.
    const RUNNING: Self = Self {
        body: BodyLife::Running,
        pieces: 0,
    };

    fn with_piece(self) -> Self {
        Self {
            pieces: self.pieces + 1,
            ..self
        }
    }

    /// One piece ended. A piece ends once, so a life with no piece is a
    /// defect.
    fn without_piece(self) -> Self {
        Self {
            pieces: self
                .pieces
                .checked_sub(1)
                .unwrap_or_else(|| panic!("a spawned piece of a run ended twice")),
            ..self
        }
    }

    fn settled(self) -> Self {
        Self {
            body: BodyLife::Settled,
            ..self
        }
    }

    /// The place stays while the body runs or a piece is alive. Pure.
    fn place(self) -> PlaceAfter {
        match (self.body, self.pieces) {
            (BodyLife::Settled, 0) => PlaceAfter::Removed,
            (BodyLife::Running, _) | (BodyLife::Settled, _) => PlaceAfter::Kept,
        }
    }
}

/// Where the spawned work of an async run goes: its handle, the runtime
/// that runs it, the poster of the stop requests, and its life.
struct WorkPlaceEntry {
    handle: RunHandle,
    runtime: tokio::runtime::Handle,
    poster: Arc<ChildPoster>,
    life: RunLife,
}

static PLACES: LazyLock<Mutex<HashMap<u64, WorkPlaceEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Change the life of the work place of `run_key` with `change`, and remove
/// the place when it ends. A run with no place is left.
fn change_place(run_key: u64, change: impl FnOnce(RunLife) -> RunLife) {
    let mut places = lock(&PLACES);
    let after = places.get_mut(&run_key).map(|entry| {
        entry.life = change(entry.life);
        entry.life.place()
    });
    if after == Some(PlaceAfter::Removed) {
        let _place_ended = places.remove(&run_key);
    }
}

/// The next run key and work key. Keys are never reused in a process.
static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

fn lock<T>(table: &Mutex<T>) -> MutexGuard<'_, T> {
    // A poisoned lock means a panic while it was held; each entry is a
    // valid value.
    table.lock().unwrap_or_else(PoisonError::into_inner)
}

fn next_key() -> u64 {
    NEXT_KEY.fetch_add(1, Ordering::Relaxed)
}

/// The async body of one task: `ISOLATED` and `SHARED_ASYNC`.
pub struct JsAsyncBody {
    pub task_name: String,
    pub bodies: Arc<ChildBodies>,
}

impl AsyncTaskFn for JsAsyncBody {
    fn execute(&self, args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
        let input = args.to_vec();
        Box::pin(async move { self.run(input).await })
    }
}

impl JsAsyncBody {
    /// The run of the body: enter it, post it, wait for its settle call.
    async fn run(&self, input: Vec<u8>) -> RawTaskResult {
        let attempt = run_attempt(&self.task_name);
        let signal = run_signal(&self.task_name);
        let handle = current_run().unwrap_or_else(|refused| {
            panic!(
                "the async body of task {} has no run handle: {refused}",
                self.task_name
            )
        });
        let body = handle
            .register_host_work(&self.task_name)
            .unwrap_or_else(|refused| {
                panic!(
                    "the body of task {} did not register as work of its run: {refused}",
                    self.task_name
                )
            });
        let (done, mut settled) = oneshot::channel();
        let cancel = Arc::new(CancelSeen::new());
        let run_key = next_key();
        let _no_earlier_entry = lock(&RUNS).insert(
            run_key,
            RunEntry {
                cancel: Arc::clone(&cancel),
                form: RunForm::Async { body, done },
            },
        );
        let _no_earlier_place = lock(&PLACES).insert(
            run_key,
            WorkPlaceEntry {
                handle,
                runtime: tokio::runtime::Handle::current(),
                poster: Arc::clone(&self.bodies.poster),
                life: RunLife::RUNNING,
            },
        );
        post_body(&self.task_name, &self.bodies, run_key, attempt, input);
        tokio::select! {
            result = &mut settled => return settled_value(result, &self.task_name),
            () = signal.cancelled() => {}
        }
        CancelWatch {
            run_key,
            cancel: &cancel,
            poster: &self.bodies.poster,
            task_name: &self.task_name,
        }
        .fired(run_cancel_state());
        settled_value(settled.await, &self.task_name)
    }
}

/// The result of a run's settle call. A sender dropped with no result is a
/// defect: only the settle call removes an entry, and it sends first.
fn settled_value(
    received: Result<RawTaskResult, oneshot::error::RecvError>,
    task_name: &str,
) -> RawTaskResult {
    match received {
        Ok(result) => result,
        Err(_entry_dropped) => {
            panic!("the run of task {task_name} left the run table with no settle call")
        }
    }
}

/// The blocking body of one task: a `SHARED_THREAD` task, on a seat thread
/// of the core. Its body runs in a seat isolate of a `SHARED_THREAD` child,
/// or on the JavaScript main thread of an isolated child of a worker that
/// runs no thread child.
pub struct JsBlockingBody {
    pub task_name: String,
    pub bodies: Arc<ChildBodies>,
}

impl BlockingTaskFn for JsBlockingBody {
    fn execute(&self, args: &[u8]) -> RawTaskResult {
        let attempt = run_attempt(&self.task_name);
        let signal = run_signal(&self.task_name);
        let (done, settled) = std::sync::mpsc::sync_channel(1);
        let cancel = Arc::new(CancelSeen::new());
        let run_key = next_key();
        let _no_earlier_entry = lock(&RUNS).insert(
            run_key,
            RunEntry {
                cancel: Arc::clone(&cancel),
                form: RunForm::Blocking { done },
            },
        );
        post_body(
            &self.task_name,
            &self.bodies,
            run_key,
            attempt,
            args.to_vec(),
        );
        let watched = CancelWatch {
            run_key,
            cancel: &cancel,
            poster: &self.bodies.poster,
            task_name: &self.task_name,
        };
        blocking_settled(&settled, &signal, &watched)
    }
}

/// Wait for the settle call of a blocking run, and read its cancel signal
/// every [`BLOCKING_CANCEL_READ`] until it fires. The wait ends with the
/// run: its settle call, or the end of the child.
fn blocking_settled(
    settled: &std::sync::mpsc::Receiver<RawTaskResult>,
    signal: &RunCancelSignal,
    watched: &CancelWatch<'_>,
) -> RawTaskResult {
    let mut watch = BlockingWatch::Watching;
    loop {
        match settled.recv_timeout(BLOCKING_CANCEL_READ) {
            Ok(result) => return result,
            Err(RecvTimeoutError::Timeout) => {
                watch = match (watch, signal.is_cancelled()) {
                    (BlockingWatch::Watching, true) => {
                        watched.fired(run_cancel_state());
                        BlockingWatch::Fired
                    }
                    (BlockingWatch::Watching, false) => BlockingWatch::Watching,
                    (BlockingWatch::Fired, _) => BlockingWatch::Fired,
                };
            }
            Err(RecvTimeoutError::Disconnected) => panic!(
                "the run of task {} left the run table with no settle call",
                watched.task_name
            ),
        }
    }
}

/// Whether the wait of a blocking run still reads its signal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockingWatch {
    Watching,
    Fired,
}

/// What the cancel watch of a run needs at the fire.
struct CancelWatch<'run> {
    run_key: u64,
    cancel: &'run CancelSeen,
    poster: &'run ChildPoster,
    task_name: &'run str,
}

impl CancelWatch<'_> {
    /// The signal fired: keep the cause for `runCancelState`, then post
    /// `cancelRun`. `state` is the run's cancel state, read in the run.
    /// A post that Node-API does not queue is logged: the kept cause still
    /// answers a read, and the core ends the run as the signal says.
    fn fired(&self, state: RunCancelState) {
        let cause = match state {
            RunCancelState::Requested { cause } => CancelCauseName::of(cause),
            RunCancelState::NotRequested | RunCancelState::NotInARun => panic!(
                "the cancel signal of the run of task {} fired, and its state reads {state:?}",
                self.task_name
            ),
        };
        self.cancel.record(cause);
        let posted = self.poster.post_run(ChildPost::CancelRun {
            run_key: bigint_of_u64(self.run_key),
            cause,
        });
        if let Err(refused) = posted {
            tracing::error!(
                task = self.task_name,
                "the cancel of the run of task {} did not reach the JavaScript thread: {refused}",
                self.task_name
            );
        }
    }
}

/// The attempt facts of the current run. Outside a run is a defect: the
/// core calls a body only in its run.
fn run_attempt(task_name: &str) -> TaskAttempt {
    run_task_attempt().unwrap_or_else(|refused| {
        panic!("the body of task {task_name} has no attempt facts: {refused}")
    })
}

/// The cancel signal of the current run; outside a run is a defect.
fn run_signal(task_name: &str) -> RunCancelSignal {
    run_cancel_signal().unwrap_or_else(|refused| {
        panic!("the body of task {task_name} has no cancel signal: {refused}")
    })
}

/// Post `runBody` of the run `run_key`. A post that Node-API does not queue
/// takes the run out of the table and panics: the body did not start.
fn post_body(
    task_name: &str,
    bodies: &ChildBodies,
    run_key: u64,
    attempt: TaskAttempt,
    input: Vec<u8>,
) {
    let input_json = String::from_utf8(input)
        .unwrap_or_else(|error| panic!("the input of task {task_name} is not UTF-8 text: {error}"));
    let posted = bodies.poster.post_run(ChildPost::RunBody {
        run_key: bigint_of_u64(run_key),
        task_name: task_name.to_owned(),
        input_json,
        task_id: attempt.task_id().to_string(),
        attempt: attempt.attempt().get(),
        max_retries: attempt.max_retries().get(),
        facts: bodies.facts.take_for_run(attempt.fencing_token()),
    });
    if let Err(refused) = posted {
        let _work_ends_with_the_entry = lock(&RUNS).remove(&run_key);
        let _no_work_place = lock(&PLACES).remove(&run_key);
        panic!("the body of task {task_name} did not reach the JavaScript thread: {refused}");
    }
}

/// How a settle call ended.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodySettle {
    /// The run got its result.
    Settled,
    /// No run waits under the key: it was settled before.
    UnknownRun,
}

/// End the run `run_key` with `result`. The lock is released before the
/// work ends and the result goes. The work place of an async run stays
/// while a spawned piece is alive.
fn settle(run_key: u64, result: RawTaskResult) -> BodySettle {
    let entry = lock(&RUNS).remove(&run_key);
    match entry {
        Some(entry) => {
            entry.form.finish(result);
            change_place(run_key, RunLife::settled);
            BodySettle::Settled
        }
        None => BodySettle::UnknownRun,
    }
}

/// The body of the run `runKey` returned, and its value reads back: `json`
/// is the stored text of the value. The body's host-held work ends.
#[napi(ts_return_type = "NativeEnvelope<BodySettle>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn settle_body_value(run_key: BigInt, json: String) -> Envelope<BodySettle> {
    sync_call("settle_body_value", || {
        match u64_of_bigint("runKey", &run_key) {
            Ok(run_key) => Envelope::Value(settle(run_key, TaskResult::Ok(json.into_bytes()))),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// The body of the run `runKey` ended with the task error `error`. The
/// body's host-held work ends.
#[napi(ts_return_type = "NativeEnvelope<BodySettle>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn settle_body_error(run_key: BigInt, error: &TaskErrorRecord) -> Envelope<BodySettle> {
    sync_call("settle_body_error", || {
        match u64_of_bigint("runKey", &run_key) {
            Ok(run_key) => Envelope::Value(settle(run_key, TaskResult::Err(error.error().clone()))),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// The cancel state of a run as its watch saw it.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunCancelStateRecord {
    NotRequested,
    /// `cause` is the first cause.
    Requested {
        cause: CancelCauseName,
    },
    /// The run settled: it is no longer in the table.
    RunEnded,
}

/// The cancel state of the run `runKey`: a sync read, so a sync body that
/// holds the JavaScript thread reads it. The watch of the run writes the
/// cause when the signal fires; a read in the same instant as the fire may
/// still see `notRequested`.
#[napi(ts_return_type = "NativeEnvelope<RunCancelStateRecord>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn run_cancel_state_of(run_key: BigInt) -> Envelope<RunCancelStateRecord> {
    sync_call("run_cancel_state_of", || {
        match u64_of_bigint("runKey", &run_key) {
            Ok(run_key) => Envelope::Value(
                lock(&RUNS)
                    .get(&run_key)
                    .map_or(RunCancelStateRecord::RunEnded, |entry| entry.cancel.state()),
            ),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// How a registration of spawned work ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpawnRegistration {
    /// The piece is work of the run under `work_key`.
    Registered { work_key: BigInt },
    /// The run has ended: it takes no new work. Nothing was registered.
    RunEnded,
    /// The run holds `bound` pieces of work already. Nothing was
    /// registered.
    TooMuchWork { bound: u32 },
}

/// Register spawned work of the async run `runKey`, named `name` in the
/// reports, and watch its stop request. Starts nothing in JavaScript.
/// A blocking run has no work set: `CLIENT_DEFECT` (the runtime of a
/// `SHARED_THREAD` body has no `spawn`).
#[napi(ts_return_type = "NativeEnvelope<SpawnRegistration>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn register_spawn(run_key: BigInt, name: String) -> Envelope<SpawnRegistration> {
    sync_call("register_spawn", || {
        let run_key = match u64_of_bigint("runKey", &run_key) {
            Ok(run_key) => run_key,
            Err(refusal) => return Envelope::Thrown(refusal.into()),
        };
        match registration(run_key, &name) {
            Registration::Started(started) => Envelope::Value(started.watched()),
            Registration::Refused(refused) => Envelope::Value(refused),
            Registration::Blocking => Envelope::Thrown(NativeThrown::ClientDefect {
                subject: "registerSpawn".to_owned(),
                rule: "the runtime of a SHARED_THREAD body has no spawn".to_owned(),
            }),
        }
    })
}

/// How a registration of spawned work ended, before its stop watch.
enum Registration {
    Started(StartedPiece),
    Refused(SpawnRegistration),
    /// A blocking run has no work set.
    Blocking,
}

/// A registered piece whose stop watch starts next.
struct StartedPiece {
    work_key: u64,
    work: Arc<HostWork>,
    ended: oneshot::Receiver<()>,
    runtime: tokio::runtime::Handle,
    poster: Arc<ChildPoster>,
}

impl StartedPiece {
    /// Start the stop watch of the piece on its run's runtime.
    fn watched(self) -> SpawnRegistration {
        let work_key = self.work_key;
        let _watch_runs_alone = self.runtime.spawn(watch_work_stop(
            work_key,
            self.work,
            self.ended,
            self.poster,
        ));
        SpawnRegistration::Registered {
            work_key: bigint_of_u64(work_key),
        }
    }
}

/// Register one piece of the run `run_key` on its work place. The place and
/// its count of pieces change under one lock, so the place cannot end
/// between the read and the count. The core refuses a piece of a run that is
/// over (`RunEnded`) or full (`TooMuchWork`).
fn registration(run_key: u64, name: &str) -> Registration {
    let mut places = lock(&PLACES);
    let Some(place) = places.get_mut(&run_key) else {
        drop(places);
        return match lock(&RUNS).get(&run_key).map(|entry| &entry.form) {
            Some(RunForm::Blocking { .. }) => Registration::Blocking,
            Some(RunForm::Async { .. }) | None => {
                Registration::Refused(SpawnRegistration::RunEnded)
            }
        };
    };
    let work = match place.handle.register_host_work(name) {
        Ok(work) => Arc::new(work),
        Err(HostWorkRefused::RunEnded) => {
            return Registration::Refused(SpawnRegistration::RunEnded)
        }
        Err(HostWorkRefused::TooMuchWork { bound }) => {
            return Registration::Refused(SpawnRegistration::TooMuchWork {
                bound: u32::try_from(bound).unwrap_or(u32::MAX),
            })
        }
    };
    place.life = place.life.with_piece();
    let work_key = next_key();
    let (ended, ended_watch) = oneshot::channel();
    let started = StartedPiece {
        work_key,
        work: Arc::clone(&work),
        ended: ended_watch,
        runtime: place.runtime.clone(),
        poster: Arc::clone(&place.poster),
    };
    drop(places);
    let _no_earlier_entry = lock(&WORKS).insert(
        work_key,
        WorkEntry {
            run_key,
            work,
            ended,
        },
    );
    Registration::Started(started)
}

/// Wait for a round's stop request of the piece `work_key`, and post it;
/// end at the end of the piece. The watch holds the piece until it ends, so
/// the piece leaves the run when the table's hold and this one are gone.
async fn watch_work_stop(
    work_key: u64,
    work: Arc<HostWork>,
    ended: oneshot::Receiver<()>,
    poster: Arc<ChildPoster>,
) {
    let cause = tokio::select! {
        cause = work.stop_requested() => cause,
        _ended_or_dropped = ended => return,
    };
    drop(work);
    let post = ChildPost::StopWork {
        work_key: bigint_of_u64(work_key),
        cause: WorkStopCauseName::of(cause),
    };
    for _try in 0..WORK_STOP_POST_TRIES {
        match poster.post_work_stop(post.clone()) {
            Ok(()) => return,
            Err(PostRefused::QueueFull) => tokio::time::sleep(WORK_STOP_POST_PAUSE).await,
            Err(refused @ (PostRefused::Closing | PostRefused::NodeApi { .. })) => {
                tracing::error!(
                    "the stop request of spawned work did not reach the JavaScript thread: \
                     {refused}"
                );
                return;
            }
        }
    }
    tracing::error!(
        "the stop request of spawned work did not reach the JavaScript thread after \
         {WORK_STOP_POST_TRIES} tries: the queue stayed full; the run reports the work as stuck"
    );
}

/// How the end of spawned work ended.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpawnEnd {
    /// The piece left its run.
    Ended,
    /// No piece is kept under the key: it ended before.
    UnknownWork,
}

/// The spawned work `workKey` settled: it leaves its run. The lock is
/// released before the piece ends.
#[napi(ts_return_type = "NativeEnvelope<SpawnEnd>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn end_spawn(work_key: BigInt) -> Envelope<SpawnEnd> {
    sync_call("end_spawn", || match u64_of_bigint("workKey", &work_key) {
        Ok(work_key) => {
            let entry = lock(&WORKS).remove(&work_key);
            Envelope::Value(match entry {
                Some(WorkEntry {
                    run_key,
                    work,
                    ended,
                }) => {
                    let _watch_ended_before = ended.send(());
                    drop(work);
                    change_place(run_key, RunLife::without_piece);
                    SpawnEnd::Ended
                }
                None => SpawnEnd::UnknownWork,
            })
        }
        Err(refusal) => Envelope::Thrown(refusal.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_kept_cause_stays() {
        let seen = CancelSeen::new();
        assert_eq!(seen.state(), RunCancelStateRecord::NotRequested);
        seen.record(CancelCauseName::Deadline);
        seen.record(CancelCauseName::Operator);
        assert_eq!(
            seen.state(),
            RunCancelStateRecord::Requested {
                cause: CancelCauseName::Deadline
            }
        );
    }

    #[test]
    fn each_stop_cause_has_its_name() {
        assert_eq!(
            WorkStopCauseName::of(WorkStopCause::LeftWork),
            WorkStopCauseName::LeftWork
        );
        assert_eq!(
            WorkStopCauseName::of(WorkStopCause::RunCancelled {
                cause: CancelCause::Deadline
            }),
            WorkStopCauseName::Deadline
        );
        assert_eq!(
            WorkStopCauseName::of(WorkStopCause::RunCancelled {
                cause: CancelCause::Operator
            }),
            WorkStopCauseName::Operator
        );
    }

    #[test]
    fn each_post_status_has_its_meaning() {
        assert_eq!(post_queued(Status::Ok), Ok(()));
        assert_eq!(post_queued(Status::QueueFull), Err(PostRefused::QueueFull));
        assert_eq!(post_queued(Status::Closing), Err(PostRefused::Closing));
        assert_eq!(
            post_queued(Status::GenericFailure),
            Err(PostRefused::NodeApi {
                status: Status::GenericFailure
            })
        );
    }

    #[test]
    fn a_settle_of_a_key_with_no_run_is_unknown() {
        assert_eq!(
            settle(next_key(), TaskResult::Ok(b"null".to_vec())),
            BodySettle::UnknownRun
        );
    }

    #[test]
    fn a_blocking_run_ends_with_its_settle_and_leaves_the_table() {
        let (done, settled) = std::sync::mpsc::sync_channel(1);
        let run_key = next_key();
        let _no_earlier_entry = lock(&RUNS).insert(
            run_key,
            RunEntry {
                cancel: Arc::new(CancelSeen::new()),
                form: RunForm::Blocking { done },
            },
        );
        assert_eq!(
            settle(run_key, TaskResult::Ok(b"7".to_vec())),
            BodySettle::Settled
        );
        assert!(matches!(settled.recv(), Ok(TaskResult::Ok(bytes)) if bytes == b"7"));
        assert!(!lock(&RUNS).contains_key(&run_key));
        assert!(matches!(
            registration(run_key, "late"),
            Registration::Refused(SpawnRegistration::RunEnded)
        ));
    }

    #[test]
    fn a_work_place_stays_while_the_body_runs_or_a_piece_is_alive() {
        let one_piece = RunLife::RUNNING.with_piece();
        assert_eq!(one_piece.place(), PlaceAfter::Kept);
        let settled_with_a_piece = one_piece.settled();
        assert_eq!(settled_with_a_piece.place(), PlaceAfter::Kept);
        assert_eq!(
            settled_with_a_piece.with_piece().without_piece().place(),
            PlaceAfter::Kept
        );
        assert_eq!(
            settled_with_a_piece.without_piece().place(),
            PlaceAfter::Removed
        );
        assert_eq!(RunLife::RUNNING.settled().place(), PlaceAfter::Removed);
    }

    #[test]
    fn a_change_of_a_run_with_no_place_is_left() {
        change_place(next_key(), RunLife::settled);
    }

    #[test]
    fn the_run_post_bound_holds_four_posts_per_seat_of_the_largest_child() {
        assert_eq!(RUN_POST_QUEUE_BOUND, 4_000);
        assert_eq!(WORK_STOP_QUEUE_BOUND, 10_000);
    }
}
