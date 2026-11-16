//! Scripted children for the tests of the slots: `/bin/sh` programs that
//! read each parent frame by its length and write child frames with
//! `printf`. The spawner appends the socket descriptor and the worker pid,
//! so a script reads the descriptor as `$0`.
//!
//! A script reads a frame only after it answered the frame before, so the
//! parent never has two frames in flight to one child and `head -c` reads
//! exactly one frame.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::board::{ChildLayout, PoolBoard, SlotPhase};
use super::dispatch::{AttemptEnd, ClaimedTask, ExecutionClass, RetryFacts, TaskKind};
use super::pool::{
    ExitNotSeen, IsolatedPool, IsolatedPoolConfig, MemoryLimit, Submitted, TaskLimit,
};
use super::route::IsolatedDeadline;
use super::shared::{SharedPool, SharedPoolConfig};
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, AsyncSeats, BootDetail, BootErrorName,
    BootFailedFrame, ChildFrame, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl,
    DispatchNumber, ExecutorThreads, InitFrame, IsolatedChild, IsolatedWork, NoLeftWork,
    ParentFrame, PoolMaxSize, PoolMinSize, ProcessId, ReadyFrame, ReportedBootCause,
    ResultEnvelopeText, ResultFrame, RssReading, RunEnd, RunFrame, RunRef, SeatEnd,
    SharedAsyncChild, SharedResultFrame, SharedRunFrame, SharedThreadChild, SharedWork,
    ShortageErrno, TaskInputText, TaskSendPool, TaskTimeoutMs, ThreadSeats, HEADER_BYTES,
};
use crate::worker::executor::supervisor::{ChildCommand, Spawner};
use crate::worker::retry::read_stored_options;
use crate::worker::runner_beats::LiveDispatches;

/// The worker id of every scripted pool and run frame.
pub(super) const WORKER_ID: &str = "worker-1";
/// The longest wait of a test for the answer of one task.
pub(super) const ANSWER_WAIT: Duration = Duration::from_secs(60);
/// The longest wait of a test for a state of the board.
pub(super) const STATE_WAIT: Duration = Duration::from_secs(30);
/// The bound of the spawner stop at the end of a test.
pub(super) const SPAWNER_STOP_WAIT: Duration = Duration::from_secs(10);

/// The claim generation of every scripted task.
pub(super) fn claimed_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap()
}

/// A claimed task with id `number`. Every id has the same length, so the
/// run frames of two scripted tasks have the same length.
pub(super) fn task(number: u128, execution: ExecutionClass) -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::from_u128(number),
        task_name: "app.tasks.scripted".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: claimed_at(),
        execution,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 0,
        },
        options: read_stored_options(Some("{}")),
    }
}

/// The input of every scripted task.
pub(super) fn input() -> TaskInputText {
    TaskInputText::new("{}".to_owned())
}

pub(super) fn timeout_ms(milliseconds: u64) -> TaskTimeoutMs {
    TaskTimeoutMs::new(milliseconds).unwrap()
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// `printf` text that writes `bytes`: one octal escape per byte.
pub(super) fn printf_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:03o}")).collect()
}

fn child_frames_text(frames: &[ChildFrame]) -> String {
    frames
        .iter()
        .map(|frame| printf_bytes(&encode_child_frame(frame).unwrap().to_bytes()))
        .collect()
}

/// The bytes of `frame` on the socket.
pub(super) fn parent_frame_len(frame: &ParentFrame) -> usize {
    encode_parent_frame(frame).unwrap().to_bytes().len()
}

fn child_pool() -> ChildPool {
    ChildPool {
        min_size: PoolMinSize::new(0).unwrap(),
        max_size: PoolMaxSize::new(2).unwrap(),
        check: true,
    }
}

fn init_of(child_kind: ChildKind) -> InitFrame {
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        process_name: "horsies-scripted-child".to_owned(),
        child_kind,
    }
}

pub(super) fn isolated_init() -> InitFrame {
    init_of(ChildKind::Isolated(IsolatedChild {
        pool: child_pool(),
        task_send_pool: TaskSendPool::new(1).unwrap(),
    }))
}

pub(super) fn shared_async_init(seats: u64) -> InitFrame {
    init_of(ChildKind::SharedAsync(SharedAsyncChild {
        seats: AsyncSeats::new(seats).unwrap(),
        pool: child_pool(),
        executor_threads: ExecutorThreads::new(2).unwrap(),
        task_send_pool: TaskSendPool::new(1).unwrap(),
    }))
}

pub(super) fn shared_thread_init(seats: u64) -> InitFrame {
    init_of(ChildKind::SharedThread(SharedThreadChild {
        seats: ThreadSeats::new(seats).unwrap(),
        pool: child_pool(),
        task_send_pool: TaskSendPool::new(1).unwrap(),
    }))
}

/// The run frame that an isolated pool sends for `task`.
pub(super) fn run_frame(task: &ClaimedTask) -> ParentFrame {
    ParentFrame::Run(RunFrame {
        task_id: task.task_id,
        task_name: task.task_name.clone(),
        worker_id: WORKER_ID.to_owned(),
        task_kind: task.kind,
        claimed_at: task.claimed_at,
        work: IsolatedWork::Run,
        input: input(),
    })
}

/// The run frame that a shared pool sends for `task` as its run `dispatch`.
pub(super) fn shared_run_frame(task: &ClaimedTask, dispatch: u64, work: SharedWork) -> ParentFrame {
    ParentFrame::SharedRun(SharedRunFrame {
        dispatch: DispatchNumber::new(dispatch).unwrap(),
        task_id: task.task_id,
        task_name: task.task_name.clone(),
        worker_id: WORKER_ID.to_owned(),
        task_kind: task.kind,
        claimed_at: task.claimed_at,
        work,
        input: input(),
    })
}

/// The run of `task` as a shared pool numbers it.
pub(super) fn run_ref(task: &ClaimedTask, dispatch: u64) -> RunRef {
    RunRef {
        dispatch: DispatchNumber::new(dispatch).unwrap(),
        task_id: task.task_id,
    }
}

pub(super) fn ok_envelope(value: u32) -> ResultEnvelopeText {
    ResultEnvelopeText::new(format!(r#"{{"__type":"ok","value":{value}}}"#))
}

/// The result frame of an isolated `task` that left no work.
pub(super) fn result_frame(task: &ClaimedTask, value: u32) -> ChildFrame {
    ChildFrame::Result(ResultFrame {
        task_id: task.task_id,
        end: RunEnd::Result(ok_envelope(value)),
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
    })
}

/// The end that the pool answers for [`result_frame`].
pub(super) fn reported(value: u32) -> AttemptEnd {
    AttemptEnd::Reported(RunEnd::Result(ok_envelope(value)))
}

/// The result frame of a shared run that finished.
pub(super) fn shared_result(run: RunRef, value: u32) -> ChildFrame {
    ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::Finished(RunEnd::Result(ok_envelope(value))),
        rss: RssReading::Unreadable,
    })
}

// ---------------------------------------------------------------------------
// Script steps
// ---------------------------------------------------------------------------

/// Read one parent frame and drop it.
pub(super) fn take(frame: &ParentFrame) -> String {
    format!(r#"head -c {} <&"$0" >/dev/null; "#, parent_frame_len(frame))
}

/// Write `frames` to the parent.
pub(super) fn send(frames: &[ChildFrame]) -> String {
    format!(r#"printf '{}' >&"$0"; "#, child_frames_text(frames))
}

/// Read the init frame and answer ready.
pub(super) fn boot(init: &InitFrame) -> String {
    let ready = ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    });
    format!(
        "{}{}",
        take(&ParentFrame::Init(init.clone())),
        send(&[ready])
    )
}

/// Read the stop frame and exit 0.
pub(super) fn exit_at_stop() -> String {
    format!("{}exit 0", take(&ParentFrame::Stop))
}

/// Read the next parent frame of any kind: its header, then its metadata
/// into `$meta`, then its body. Exit 0 when it is the stop frame.
pub(super) fn read_frame() -> String {
    let stop_meta = encode_parent_frame(&ParentFrame::Stop)
        .unwrap()
        .head()
        .len()
        - HEADER_BYTES;
    format!(
        r#"set -- $(head -c 12 <&"$0" | od -An -tu1); m=$(( $1 * 16777216 + $2 * 65536 + $3 * 256 + $4 )); b=$(( $9 * 16777216 + ${{10}} * 65536 + ${{11}} * 256 + ${{12}} )); meta=$(head -c "$m" <&"$0"); if [ "$b" -gt 0 ]; then head -c "$b" <&"$0" >/dev/null; fi; if [ "$m" -eq {stop_meta} ] && [ "$b" -eq 0 ]; then exit 0; fi; "#
    )
}

/// Answer `runs` run frames, each with the frames given for the task that
/// it names, then exit at the stop frame. A frame of another task, or a run
/// frame after the last, ends the child with exit code 3.
pub(super) fn answer_runs(answers: &[(&ClaimedTask, Vec<ChildFrame>)], runs: usize) -> String {
    let actions: Vec<(&ClaimedTask, String)> = answers
        .iter()
        .map(|(task, frames)| (*task, send(frames)))
        .collect();
    act_on_runs(&actions, runs)
}

/// As [`answer_runs`], with a shell action for each task in place of its
/// frames.
pub(super) fn act_on_runs(actions: &[(&ClaimedTask, String)], runs: usize) -> String {
    let cases: String = actions
        .iter()
        .map(|(task, action)| format!("*{}*) {action};; ", task.task_id))
        .collect();
    let one_run = format!("{}case $meta in {cases}*) exit 3;; esac; ", read_frame());
    format!("{}{}exit 3", one_run.repeat(runs), read_frame())
}

/// Boot, then serve nothing: exit at the stop frame.
pub(super) fn idle_child(init: &InitFrame) -> String {
    format!("{}{}exit 3", boot(init), read_frame())
}

/// Take a unique number from 1 up in `dir` as `$n`: the boot count of the
/// children of a test.
pub(super) fn ticket(dir: &TestDir) -> String {
    format!(
        r#"n=1; while ! mkdir "{}/ticket-$n" 2>/dev/null; do n=$((n + 1)); done; "#,
        dir.path().display()
    )
}

/// The children that took a ticket.
pub(super) fn tickets(dir: &TestDir) -> usize {
    std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("ticket-")
        })
        .count()
}

/// The boot failure frame of a child short of descriptors.
pub(super) fn short_of_descriptors() -> ChildFrame {
    boot_failed(
        ReportedBootCause::ResourceShortage(ShortageErrno::Emfile),
        "OSError",
        "[Errno 24] Too many open files",
    )
}

pub(super) fn boot_failed(cause: ReportedBootCause, error_name: &str, detail: &str) -> ChildFrame {
    ChildFrame::BootFailed(BootFailedFrame {
        cause,
        error_name: BootErrorName::cut(error_name),
        detail: BootDetail::cut(detail),
    })
}

/// Read the init frame, report `failure` in place of the ready frame, and
/// exit with the boot-failed code.
pub(super) fn fail_boot(init: &InitFrame, failure: &ChildFrame) -> String {
    format!(
        "{}{}exit 75",
        take(&ParentFrame::Init(init.clone())),
        send(std::slice::from_ref(failure))
    )
}

pub(super) fn shell(script: &str) -> ChildCommand {
    ChildCommand {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), script.into()],
    }
}

// ---------------------------------------------------------------------------
// Pools
// ---------------------------------------------------------------------------

pub(super) fn layout(isolated: usize, shared_async: usize, shared_thread: usize) -> ChildLayout {
    ChildLayout {
        isolated,
        shared_async,
        shared_thread,
    }
}

pub(super) fn isolated_config(command: ChildCommand) -> IsolatedPoolConfig {
    IsolatedPoolConfig {
        command,
        init: isolated_init(),
        worker_id: WORKER_ID.to_owned(),
        pending_limit: NonZeroUsize::new(16).unwrap(),
        tasks_per_child: TaskLimit::Off,
        memory_per_child: MemoryLimit::Off,
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
    }
}

pub(super) fn shared_config(command: ChildCommand, init: InitFrame) -> SharedPoolConfig {
    SharedPoolConfig {
        command,
        init,
        worker_id: WORKER_ID.to_owned(),
        pending_limit: NonZeroUsize::new(16).unwrap(),
        tasks_per_seat: TaskLimit::Off,
        memory_per_child: MemoryLimit::Off,
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
    }
}

/// An isolated pool of `slots` slots, alone on its board.
pub(super) fn start_isolated(
    spawner: &Spawner,
    slots: usize,
    config: IsolatedPoolConfig,
) -> (IsolatedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(slots, 0, 0)));
    let pool = IsolatedPool::start(
        config,
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );
    (pool, board)
}

/// A shared pool on `board`, whose layout has the pool's slots.
pub(super) fn start_shared(
    spawner: &Spawner,
    board: &Arc<PoolBoard>,
    config: SharedPoolConfig,
) -> SharedPool {
    SharedPool::start(
        config,
        Arc::clone(board),
        spawner.handle(),
        LiveDispatches::default(),
    )
}

/// The receiver of a task that the pool queued.
pub(super) fn queued(submitted: Submitted) -> oneshot::Receiver<AttemptEnd> {
    match submitted {
        Submitted::Queued(receiver) => receiver,
        Submitted::NotDispatched(cause) => panic!("the task was not dispatched: {cause}"),
    }
}

/// Queue `task` on an isolated pool.
pub(super) fn submit_isolated(
    pool: &IsolatedPool,
    task: &ClaimedTask,
    deadline: IsolatedDeadline,
) -> oneshot::Receiver<AttemptEnd> {
    queued(pool.submit(task, input(), IsolatedWork::Run, deadline))
}

/// The one answer of a task, within [`ANSWER_WAIT`].
pub(super) async fn answer_of(receiver: oneshot::Receiver<AttemptEnd>) -> AttemptEnd {
    tokio::time::timeout(ANSWER_WAIT, receiver)
        .await
        .expect("the task is answered within the test wait")
        .expect("every task gets an answer")
}

/// Poll `reached` every 10 ms until it holds; fail the test after `bound`.
pub(super) async fn until(what: &str, bound: Duration, mut reached: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + bound;
    while !reached() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "not reached within {bound:?}: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The pid of the child that serves slot `index`, if one serves.
pub(super) fn serving_pid(board: &PoolBoard, index: usize) -> Option<ProcessId> {
    match board.phase(index) {
        SlotPhase::Serving { pid } | SlotPhase::Closing { pid, .. } => Some(pid),
        SlotPhase::Booting | SlotPhase::Retiring | SlotPhase::Down(_) => None,
    }
}

/// Wait until slot `index` serves a child, and give its pid.
pub(super) async fn serving(board: &PoolBoard, index: usize) -> ProcessId {
    until("the slot serves a child", STATE_WAIT, || {
        serving_pid(board, index).is_some()
    })
    .await;
    serving_pid(board, index).expect("the slot serves a child")
}

/// Whether `pid` names no process, or a zombie that no one reaped yet.
pub(super) fn process_ended(pid: libc::pid_t) -> bool {
    // SAFETY: signal 0 checks the process and sends nothing.
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    match alive {
        false => true,
        true => std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|stat| {
                stat.rsplit(')')
                    .next()
                    .is_some_and(|rest| rest.trim_start().starts_with('Z'))
            })
            .unwrap_or(false),
    }
}

/// Wait until `pid` has ended.
pub(super) async fn ended(pid: libc::pid_t) {
    until("the process ended", STATE_WAIT, || process_ended(pid)).await;
}

/// The execution class of each kind of scripted shared child.
pub(super) fn shared_class(init: &InitFrame) -> ExecutionClass {
    match init.child_kind {
        ChildKind::SharedAsync(_) => ExecutionClass::SharedAsync,
        ChildKind::SharedThread(_) => ExecutionClass::SharedThread,
        ChildKind::Isolated(_) => ExecutionClass::Isolated,
    }
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

/// A directory of one test, removed at the end of the test.
pub(super) struct TestDir(PathBuf);

impl TestDir {
    pub(super) fn new() -> Self {
        let path = std::env::temp_dir().join(format!("horsies-slots-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(super) fn path(&self) -> &Path {
        &self.0
    }

    /// The path of `name` in the directory, as script text.
    pub(super) fn file(&self, name: &str) -> String {
        self.0.join(name).display().to_string()
    }

    /// The lines of `name`, or none when the file does not exist yet.
    pub(super) fn lines(&self, name: &str) -> Vec<String> {
        match std::fs::read_to_string(self.0.join(name)) {
            Ok(text) => text.lines().map(str::to_owned).collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("the test file {name} could not be read: {error}"),
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        // A file that a killed child still writes may refuse the removal;
        // the temporary directory of the OS then keeps it.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
