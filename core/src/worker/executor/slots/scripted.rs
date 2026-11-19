//! Scripted children for the tests of the slots: bash programs that
//! read each parent frame by its length and write child frames with
//! `printf`. The spawner appends the socket descriptor and the worker pid,
//! so a script reads the descriptor as `$0`.
//!
//! Every read takes exactly the bytes it names and no byte after them, on
//! every platform: [`read_bytes`] reads one byte per `read`, and
//! [`drop_bytes`] asks for at most the bytes still owed in each `read`.
//! `head -c` is not used: BSD `head` (macOS) reads a full stdio buffer from
//! the socket and drops what it does not write, so the rest of the frame,
//! and any frame after it, is lost to the next read.

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
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, AsyncSeats, BlockedWhere, BodyEnd, BootDetail,
    BootErrorName, BootFailedFrame, CancelGraceMs, ChildAfterFrame, ChildConnection, ChildFrame,
    ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl, DispatchNumber,
    ExecutorThreads, InitFrame, IsolatedChild, IsolatedWork, LeftoverCancelFrame, LeftoverName,
    LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit, NoLeftWork, ParentFrame, PoolMaxSize,
    PoolMinSize, ProcessId, ReadyFrame, ReportedBootCause, ResultEnvelopeText, ResultFrame,
    RssReading, RunEnd, RunFrame, RunRef, RunStuckFrame, SeatEnd, SharedAsyncChild,
    SharedResultFrame, SharedRunFrame, SharedThreadChild, SharedWork, ShortageErrno, StuckWork,
    TaskInputText, TaskSendPool, TaskTimeoutMs, ThreadSeats, HEADER_BYTES,
};
use crate::worker::executor::supervisor::{ChildCommand, Spawner, CHILD_KILL_WAIT};
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
        connection: ChildConnection::default(),
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
        claimed_at: task.claimed_at,
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
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
        claimed_at: task.claimed_at,
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
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

/// The result envelope of a task that returned `value`, in the stored form
/// of a task result (`core/task/result.rs`).
pub(super) fn ok_envelope(value: u32) -> ResultEnvelopeText {
    ResultEnvelopeText::new(format!(
        r#"{{"__h_task_result__":true,"ok":{value},"err":null}}"#
    ))
}

/// The result frame of an isolated `task` that left no work.
pub(super) fn result_frame(task: &ClaimedTask, value: u32) -> ChildFrame {
    ChildFrame::Result(ResultFrame {
        task_id: task.task_id,
        end: RunEnd::Result(ok_envelope(value)),
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    })
}

/// The end that the pool answers for [`result_frame`].
pub(super) fn reported(value: u32) -> AttemptEnd {
    AttemptEnd::Reported(RunEnd::Result(ok_envelope(value)))
}

/// The result frame of a shared run that finished.
pub(super) fn shared_result(run: RunRef, value: u32) -> ChildFrame {
    shared_result_with_rss(run, value, RssReading::Unreadable)
}

pub(super) fn shared_result_with_rss(run: RunRef, value: u32, rss: RssReading) -> ChildFrame {
    ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::Finished(RunEnd::Result(ok_envelope(value))),
        rss,
        child: ChildAfterFrame::ServesOn,
    })
}

/// The run ended at its deadline in its child.
pub(super) fn shared_timed_out(run: RunRef) -> ChildFrame {
    ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::TimedOut,
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    })
}

fn stuck_work() -> StuckWork {
    StuckWork::new(1, 0, vec![LeftoverName::cut("app.tasks.scripted")]).unwrap()
}

pub(super) fn run_stuck(run: RunRef) -> ChildFrame {
    ChildFrame::RunStuck(RunStuckFrame {
        run,
        work: stuck_work(),
    })
}

/// The body of the run returned `value`; work it left is cancelled in
/// rounds.
pub(super) fn leftover_cancel(run: RunRef, value: u32) -> ChildFrame {
    ChildFrame::LeftoverCancel(LeftoverCancelFrame {
        run,
        returned: BodyEnd::Result(ok_envelope(value)),
    })
}

pub(super) fn leftover_stuck(run: RunRef, value: u32) -> ChildFrame {
    ChildFrame::LeftoverStuck(LeftoverStuckFrame {
        run,
        work: stuck_work(),
        returned: BodyEnd::Result(ok_envelope(value)),
    })
}

/// The loop of the child did not turn; `culprit` held it.
pub(super) fn loop_blocked(culprit: LoopCulprit) -> ChildFrame {
    ChildFrame::LoopBlocked(LoopBlockedFrame {
        culprit,
        blocked_ms: 5_100,
        location: BlockedWhere::cut("app/tasks.py:12"),
    })
}

pub(super) fn async_work(timeout: u64, grace: u64) -> SharedWork {
    SharedWork::AsyncTask {
        timeout_ms: timeout_ms(timeout),
        cancel_grace_ms: CancelGraceMs::new(grace).unwrap(),
    }
}

pub(super) fn thread_work(timeout: u64) -> SharedWork {
    SharedWork::ThreadTask {
        timeout_ms: timeout_ms(timeout),
    }
}

/// Queue `task` on a shared pool.
pub(super) fn submit_shared(
    pool: &SharedPool,
    task: &ClaimedTask,
    work: SharedWork,
) -> oneshot::Receiver<AttemptEnd> {
    queued(pool.submit(task, input(), work))
}

/// Write `frames` to the parent from a background shell after `seconds`,
/// so the child reads its next frame meanwhile.
pub(super) fn send_later(seconds: &str, frames: &[ChildFrame]) -> String {
    format!("( sleep {seconds}; {}) & ", send(frames))
}

// ---------------------------------------------------------------------------
// Script steps
// ---------------------------------------------------------------------------

/// The byte count of one read of a script.
#[derive(Debug, Clone, Copy)]
pub(super) enum ScriptCount<'a> {
    /// A count known when the script is built.
    Known(usize),
    /// The shell variable of this name holds the count when the step runs.
    Variable(&'a str),
}

impl ScriptCount<'_> {
    fn shell_word(self) -> String {
        match self {
            Self::Known(count) => count.to_string(),
            Self::Variable(name) => format!(r#""${name}""#),
        }
    }
}

/// Shell text that writes the next `count` bytes of the socket to its
/// standard output, for a command substitution. `dd` with one-byte blocks
/// reads one byte per `read`, so it never takes a byte past `count`. One
/// `read` per byte: for headers and metadata, not for large bodies.
pub(super) fn read_bytes(count: ScriptCount<'_>) -> String {
    format!(r#"dd bs=1 count={} <&"$0" 2>/dev/null"#, count.shell_word())
}

/// Shell step that drops the next `count` bytes of the socket. Each `dd`
/// makes one `read` of at most the bytes still owed (`$left`) and the loop
/// subtracts what it got, so a short read is read again and no byte past
/// `count` is taken. The end of the stream ends the loop early, as `head -c`
/// ends at the end of its input. Sets `$left` and `$got`.
pub(super) fn drop_bytes(count: ScriptCount<'_>) -> String {
    format!(
        r#"left={}; while [ "$left" -gt 0 ]; do got=$(( $(dd bs="$left" count=1 <&"$0" 2>/dev/null | wc -c) )); [ "$got" -gt 0 ] || break; left=$(( left - got )); done; "#,
        count.shell_word()
    )
}

/// Read one parent frame and drop it.
pub(super) fn take(frame: &ParentFrame) -> String {
    drop_bytes(ScriptCount::Known(parent_frame_len(frame)))
}

/// Write `frames` to the parent.
pub(super) fn send(frames: &[ChildFrame]) -> String {
    format!(r#"printf '{}' >&"$0"; "#, child_frames_text(frames))
}

/// Read the init frame by its header and drop it. A slot sends the init
/// frame of its pool with the process name of the slot
/// (`booting::init_for_slot`), so the length of the frame depends on the
/// slot.
pub(crate) fn take_init() -> String {
    read_frame()
}

/// Read the init frame that a slot sends for `_template`, by its header, and
/// answer ready.
pub(super) fn boot(_template: &InitFrame) -> String {
    let ready = ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    });
    format!("{}{}", take_init(), send(&[ready]))
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
        r#"set -- $({header} | od -An -tu1); m=$(( $1 * 16777216 + $2 * 65536 + $3 * 256 + $4 )); b=$(( $9 * 16777216 + ${{10}} * 65536 + ${{11}} * 256 + ${{12}} )); meta=$({meta}); {body}if [ "$m" -eq {stop_meta} ] && [ "$b" -eq 0 ]; then exit 0; fi; "#,
        header = read_bytes(ScriptCount::Known(HEADER_BYTES)),
        meta = read_bytes(ScriptCount::Variable("m")),
        body = drop_bytes(ScriptCount::Variable("b")),
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
    act_on_runs_then(actions, runs, &format!("{}exit 3", read_frame()))
}

/// As [`act_on_runs`], then `tail` in place of the wait for the stop frame.
pub(super) fn act_on_runs_then(
    actions: &[(&ClaimedTask, String)],
    runs: usize,
    tail: &str,
) -> String {
    let cases: String = actions
        .iter()
        .map(|(task, action)| format!("*{}*) {action};; ", task.task_id))
        .collect();
    let one_run = format!("{}case $meta in {cases}*) exit 3;; esac; ", read_frame());
    format!("{}{tail}", one_run.repeat(runs))
}

/// Read the init frame of any kind of child, then answer ready: one script
/// for every child of a worker, whose pools send init frames of different
/// lengths.
pub(crate) fn boot_any_kind() -> String {
    let ready = ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    });
    format!("{}{}", read_frame(), send(&[ready]))
}

/// A child of any kind that boots, then serves nothing: it exits at the
/// stop frame.
pub(crate) fn idle_child_of_any_kind() -> ChildCommand {
    shell(&format!("{}{}exit 3", boot_any_kind(), read_frame()))
}

/// A child of any kind that writes its pid to `pid_file`, boots, then
/// serves nothing: it exits at the stop frame.
pub(crate) fn idle_child_recording_pid(pid_file: &Path) -> ChildCommand {
    shell(&format!(
        "echo $$ > '{}'; {}{}exit 3",
        pid_file.display(),
        boot_any_kind(),
        read_frame()
    ))
}

/// A child of any kind that writes its pid to `pid_file`, boots, then holds
/// its first run: it reads the run frame and answers nothing. It exits 0 at
/// the stop frame, 3 at any other frame after the run, or when it is killed.
pub(crate) fn child_holding_its_first_run(pid_file: &Path) -> ChildCommand {
    shell(&format!(
        "echo $$ > '{}'; {}{}{}exit 3",
        pid_file.display(),
        boot_any_kind(),
        read_frame(),
        read_frame()
    ))
}

/// Children of any kind whose ready frame reports an RSS by boot order: the
/// n-th child to boot reports `readings[n - 1]`, a later child the last
/// reading. Each then serves nothing: it exits at the stop frame. The boot
/// order is kept in the folder `dir`, as [`ticket`] keeps it.
///
/// Panics: `readings` is empty (a defect of the test).
pub(crate) fn idle_children_reporting_rss(dir: &Path, readings: &[RssReading]) -> ChildCommand {
    let ready = |rss: RssReading| {
        send(&[ChildFrame::Ready(ReadyFrame {
            pid: ProcessId::new(1).unwrap(),
            rss,
        })])
    };
    let last = *readings.last().expect("at least one RSS reading");
    let cases: String = readings
        .iter()
        .zip(1_usize..)
        .map(|(rss, boot)| format!("{boot}) {};; ", ready(*rss)))
        .collect();
    shell(&format!(
        r#"n=1; while ! mkdir "{}/ticket-$n" 2>/dev/null; do n=$((n + 1)); done; {}case $n in {cases}*) {};; esac; {}exit 3"#,
        dir.display(),
        read_frame(),
        ready(last),
        read_frame(),
    ))
}

/// A child of any kind whose start hook fails: it reads its init frame and
/// exits with the hook failure code (173).
pub(crate) fn hook_failing_child_of_any_kind() -> ChildCommand {
    shell(&format!("{}exit 173", read_frame()))
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

/// Read the init frame that a slot sends for `_template`, by its header,
/// report `failure` in place of the ready frame, and exit with the
/// boot-failed code.
pub(super) fn fail_boot(_template: &InitFrame, failure: &ChildFrame) -> String {
    format!(
        "{}{}exit 75",
        take_init(),
        send(std::slice::from_ref(failure))
    )
}

pub(super) fn shell(script: &str) -> ChildCommand {
    ChildCommand {
        program: crate::test_support::SCRIPT_SHELL.into(),
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
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        pending_limit: NonZeroUsize::new(16).unwrap(),
        tasks_per_child: TaskLimit::Off,
        memory_per_child: MemoryLimit::Off,
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
        child_cpus: ChildCpus::Inherited,
        kill_wait: CHILD_KILL_WAIT,
    }
}

pub(super) fn shared_config(command: ChildCommand, init: InitFrame) -> SharedPoolConfig {
    SharedPoolConfig {
        command,
        init,
        worker_id: WORKER_ID.to_owned(),
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        pending_limit: NonZeroUsize::new(16).unwrap(),
        tasks_per_seat: TaskLimit::Off,
        memory_per_child: MemoryLimit::Off,
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
        child_cpus: ChildCpus::Inherited,
        kill_wait: CHILD_KILL_WAIT,
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

#[test]
fn the_scripted_envelope_reads_as_a_task_result() {
    use crate::core::task::result::{read_task_result_text, TaskResultText};

    let envelope = ok_envelope(3);

    match read_task_result_text(envelope.as_str()) {
        Ok(TaskResultText::Ok(value)) => assert_eq!(value.get(), "3"),
        other => panic!("expected the value 3, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Pools over a source that holds exits back
// ---------------------------------------------------------------------------

/// An isolated pool of `slots` slots, alone on its board, whose children
/// come from `source`; its runner beats go to `live`.
pub(super) fn start_isolated_from(
    spawner: &Spawner,
    source: &Arc<super::held_exits::HeldExits>,
    slots: usize,
    config: IsolatedPoolConfig,
    live: LiveDispatches,
) -> (IsolatedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(slots, 0, 0)));
    let source: Arc<dyn crate::worker::executor::supervisor::ChildSource> =
        Arc::<super::held_exits::HeldExits>::clone(source);
    let pool =
        IsolatedPool::start_from_source(config, Arc::clone(&board), spawner.handle(), source, live);
    (pool, board)
}

/// A shared pool on `board` whose children come from `source`; its runner
/// beats go to `live`.
pub(super) fn start_shared_from(
    spawner: &Spawner,
    source: &Arc<super::held_exits::HeldExits>,
    board: &Arc<PoolBoard>,
    config: SharedPoolConfig,
    live: LiveDispatches,
) -> SharedPool {
    let source: Arc<dyn crate::worker::executor::supervisor::ChildSource> =
        Arc::<super::held_exits::HeldExits>::clone(source);
    SharedPool::start_from_source(config, Arc::clone(board), spawner.handle(), source, live)
}
