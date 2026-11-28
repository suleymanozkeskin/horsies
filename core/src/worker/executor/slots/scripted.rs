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
use super::route::{IsolatedCancelStop, IsolatedDeadline};
use super::shared::{SharedPool, SharedPoolConfig};
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, AsyncSeats, BlockedWhere, BodyEnd, BootDetail,
    BootErrorName, BootFailedFrame, CancelGraceMs, CancelledBody, CancelledLoop, ChildAfterFrame,
    ChildConnection, ChildFrame, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl,
    DispatchNumber, ExecutorThreads, InitFrame, IsolatedChild, IsolatedWork, LeftoverCancelFrame,
    LeftoverName, LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit, NoLeftWork, ParentFrame,
    PoolMaxSize, PoolMinSize, ProcessId, ReadyFrame, ReportedBootCause, ResultEnvelopeText,
    ResultFrame, RssReading, RunCancelledFrame, RunEnd, RunFrame, RunRef, RunStuckFrame, SeatEnd,
    SharedAsyncChild, SharedResultFrame, SharedThreadChild, SharedWork, ShortageErrno, StuckWork,
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
pub(crate) const STATE_WAIT: Duration = Duration::from_secs(30);
/// The bound of the spawner stop at the end of a test.
pub(crate) const SPAWNER_STOP_WAIT: Duration = Duration::from_secs(10);
/// The most creates that one child tries for its ticket (see [`ticket`]).
/// Every try counts: a ticket that exists and a failed create. No test
/// boots this many children, so only a child whose create keeps failing
/// reaches it.
const TICKET_ATTEMPTS: u32 = 64;
/// The exit code of a child that took no ticket in [`TICKET_ATTEMPTS`]
/// tries. No other scripted child and no exit code of the child protocol
/// uses it.
const NO_TICKET_EXIT_CODE: i32 = 97;

/// The claim generation of every scripted task.
pub(super) fn claimed_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap()
}

/// The start that every scripted frame that ends a run reports: the time
/// that the start write returned to the child.
pub(super) fn started_at() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
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
        trace: crate::events::RunTrace::NoCarrier,
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
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
        queue_name: crate::events::QueueName::cut(&task.queue_name),
        attempt: task.retry.attempt(),
        max_retries: task.retry.max_retries_allowed(),
        worker_id: WORKER_ID.to_owned(),
        claimed_at: task.claimed_at,
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        input: input(),
        trace: crate::events::RunTrace::NoCarrier,
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
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    })
}

/// The end that the pool answers for [`result_frame`] and
/// [`shared_result`]: the result, with the start that the frame reports.
pub(super) fn reported(value: u32) -> AttemptEnd {
    AttemptEnd::Result {
        envelope: ok_envelope(value),
        start: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
    }
}

/// The result frame of a shared run that finished.
pub(super) fn shared_result(run: RunRef, value: u32) -> ChildFrame {
    shared_result_with_rss(run, value, RssReading::Unreadable)
}

pub(super) fn shared_result_with_rss(run: RunRef, value: u32, rss: RssReading) -> ChildFrame {
    ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::Finished(RunEnd::Result(ok_envelope(value))),
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
        rss,
        child: ChildAfterFrame::ServesOn,
    })
}

/// The child applied the operator cancel to the shared run.
pub(super) fn cancel_fired(run: RunRef) -> ChildFrame {
    ChildFrame::CancelFired(run)
}

/// A shared run that an operator cancelled ended; its body ran or not.
pub(super) fn shared_cancelled(run: RunRef, body: CancelledBody) -> ChildFrame {
    ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::Cancelled(body),
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    })
}

/// An isolated `task` that an operator cancelled ended, with `task_loop`.
pub(super) fn run_cancelled(
    task: &ClaimedTask,
    body: CancelledBody,
    task_loop: CancelledLoop,
) -> ChildFrame {
    ChildFrame::RunCancelled(RunCancelledFrame {
        task_id: task.task_id,
        body,
        started_at: started_at(),
        rss: RssReading::Unreadable,
        task_loop,
        child: ChildAfterFrame::ServesOn,
    })
}

/// The run ended at its deadline in its child.
pub(super) fn shared_timed_out(run: RunRef) -> ChildFrame {
    ChildFrame::SharedResult(SharedResultFrame {
        run,
        end: SeatEnd::TimedOut,
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
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
        started_at: started_at(),
    })
}

pub(super) fn leftover_stuck(run: RunRef, value: u32) -> ChildFrame {
    ChildFrame::LeftoverStuck(LeftoverStuckFrame {
        run,
        work: stuck_work(),
        returned: BodyEnd::Result(ok_envelope(value)),
        started_at: started_at(),
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
pub(crate) enum ScriptCount<'a> {
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
pub(crate) fn read_bytes(count: ScriptCount<'_>) -> String {
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
pub(crate) fn send(frames: &[ChildFrame]) -> String {
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
pub(crate) fn read_frame() -> String {
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

/// A child of any kind that boots, reads its first run frame, then writes
/// its pid to `pid_file` and holds the run: it answers nothing. The pid
/// file appears only once the child has the run frame, through a rename,
/// so a reader never sees a part of it. It exits 0 at a stop frame, 3 at any
/// other frame after the run, or when it is killed.
pub(crate) fn child_holding_its_first_run(pid_file: &Path) -> ChildCommand {
    let partial = pid_file.with_extension("part");
    shell(&format!(
        "{}{}echo $$ > '{}'; mv '{}' '{}'; {}exit 3",
        boot_any_kind(),
        read_frame(),
        partial.display(),
        partial.display(),
        pid_file.display(),
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
        "{}{}case $n in {cases}*) {};; esac; {}exit 3",
        ticket_step(dir),
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
/// children of a test. The ticket is a file that holds the pid of the
/// child, for the report of a wait that times out ([`Shown::Tickets`]).
/// The steps are those of [`ticket_step`].
pub(crate) fn ticket(dir: &TestDir) -> String {
    ticket_step(dir.path())
}

/// Shell step that takes the lowest free ticket number from 1 up in
/// `folder` as `$n`: it creates the file `ticket-$n`, which holds `$$`.
///
/// - The create is exclusive. With `set -C` (noclobber), bash opens a file
///   that does not exist with `O_CREAT | O_EXCL`, so the kernel gives each
///   name to one child only. No external program takes part: the uutils
///   `mkdir` 0.8.0 and 0.10.0 exits 0 when a concurrent call made the
///   folder first.
/// - The create runs in a command substitution: `set -C` stays in its
///   subshell, and `$$` there is the pid of the child.
/// - A create that fails while the ticket exists moves to the next number.
/// - A create that fails for any other cause writes its error to standard
///   error (the test log), and the next try takes the same number.
/// - After [`TICKET_ATTEMPTS`] tries with no ticket, the child writes why to
///   standard error and exits with [`NO_TICKET_EXIT_CODE`]. The slot then
///   sees a child that ended before its first frame.
///
/// Sets `$n`, `$ticket_attempt` and `$ticket_error`.
fn ticket_step(folder: &Path) -> String {
    let ticket = format!("{}/ticket-$n", folder.display());
    format!(
        r#"n=1; ticket_attempt=1; while ! ticket_error=$( {{ set -C; echo $$ > "{ticket}"; }} 2>&1 ); do if [ -e "{ticket}" ]; then n=$((n + 1)); else echo "scripted child $$: create of {ticket} failed: $ticket_error" >&2; fi; if [ "$ticket_attempt" -ge {TICKET_ATTEMPTS} ]; then echo "scripted child $$: no ticket in {folder} after {TICKET_ATTEMPTS} creates, exit {NO_TICKET_EXIT_CODE}" >&2; exit {NO_TICKET_EXIT_CODE}; fi; ticket_attempt=$((ticket_attempt + 1)); done; "#,
        folder = folder.display(),
    )
}

/// The children that took a ticket.
pub(crate) fn tickets(dir: &TestDir) -> usize {
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

pub(crate) fn shell(script: &str) -> ChildCommand {
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
    start_shared_with_live(spawner, board, config, LiveDispatches::default())
}

/// As [`start_shared`], on the registry of held dispatches `live`, through
/// which a test routes an operator cancel.
pub(super) fn start_shared_with_live(
    spawner: &Spawner,
    board: &Arc<PoolBoard>,
    config: SharedPoolConfig,
    live: LiveDispatches,
) -> SharedPool {
    SharedPool::start(config, Arc::clone(board), spawner.handle(), live)
}

/// An isolated pool of `slots` slots, alone on its board, on the registry
/// of held dispatches `live`, through which a test routes an operator
/// cancel.
pub(super) fn start_isolated_with_live(
    spawner: &Spawner,
    slots: usize,
    config: IsolatedPoolConfig,
    live: LiveDispatches,
) -> (IsolatedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(slots, 0, 0)));
    let pool = IsolatedPool::start(config, Arc::clone(&board), spawner.handle(), live);
    (pool, board)
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

/// Queue `task` on an isolated pool, with the stop of an operator cancel.
pub(super) fn submit_isolated_with_cancel_stop(
    pool: &IsolatedPool,
    task: &ClaimedTask,
    deadline: IsolatedDeadline,
    cancel_stop: IsolatedCancelStop,
) -> oneshot::Receiver<AttemptEnd> {
    queued(pool.submit_with_cancel_stop(task, input(), IsolatedWork::Run, deadline, cancel_stop))
}

/// The one answer of a task, within [`ANSWER_WAIT`].
pub(super) async fn answer_of(receiver: oneshot::Receiver<AttemptEnd>) -> AttemptEnd {
    tokio::time::timeout(ANSWER_WAIT, receiver)
        .await
        .expect("the task is answered within the test wait")
        .expect("every task gets an answer")
}

/// Poll `reached` every 10 ms until it holds; fail the test after `bound`.
/// The first line of the panic message names the wait. The next lines show
/// each item of `shown`, read at the timeout.
pub(crate) async fn until(
    what: &str,
    bound: Duration,
    shown: &[Shown<'_>],
    mut reached: impl FnMut() -> bool,
) {
    let deadline = tokio::time::Instant::now() + bound;
    while !reached() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "not reached within {bound:?}: {what}{}",
            shown_text(shown)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// One item that [`until`] shows in its panic message when its wait times
/// out.
pub(crate) enum Shown<'a> {
    /// The health of the board, the phase of each slot, and the process
    /// state of each child that a phase names.
    Board(&'a PoolBoard),
    /// The tickets in the folder (see [`ticket`]), and the process state of
    /// the child that took each ticket.
    Tickets(&'a TestDir),
    /// The process state of one child.
    Child(libc::pid_t),
}

/// The lines that [`until`] adds to its panic message: one block per item
/// of `shown`, each line after a line break. Empty when `shown` is empty.
/// It does not panic: a read that fails shows its error.
fn shown_text(shown: &[Shown<'_>]) -> String {
    shown
        .iter()
        .flat_map(|item| match item {
            Shown::Board(board) => board_lines(board),
            Shown::Tickets(dir) => ticket_lines(dir),
            Shown::Child(pid) => vec![process_line(*pid)],
        })
        .map(|line| format!("\n{line}"))
        .collect()
}

/// The health of `board`, the phase of each slot, and the process state of
/// the child that a serving or closing slot names.
fn board_lines(board: &PoolBoard) -> Vec<String> {
    let slots = board.slot_count();
    let mut lines = vec![format!("board: {slots} slots, health {:?}", board.health())];
    for index in 0..slots {
        let phase = board.phase(index);
        lines.push(format!("  slot {index}: {phase:?}"));
        match phase {
            SlotPhase::Serving { pid } | SlotPhase::Closing { pid, .. } => {
                lines.push(format!("    {}", process_line(pid.as_pid_t())));
            }
            SlotPhase::Booting | SlotPhase::Retiring | SlotPhase::Down(_) => {}
        }
    }
    lines
}

/// The ticket count of `dir`, and for each ticket the pid that its child
/// wrote and the process state of that child.
fn ticket_lines(dir: &TestDir) -> Vec<String> {
    let folder = dir.path();
    match ticket_names(folder) {
        Ok(names) => std::iter::once(format!("tickets in {}: {}", folder.display(), names.len()))
            .chain(
                names
                    .iter()
                    .map(|name| format!("  {name}: {}", ticket_child_line(&folder.join(name)))),
            )
            .collect(),
        Err(error) => vec![format!(
            "tickets in {}: the folder was not read: {error}",
            folder.display()
        )],
    }
}

/// The names of the tickets in `folder`, in ticket order.
fn ticket_names(folder: &Path) -> std::io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(folder)? {
        names.push(entry?.file_name().to_string_lossy().into_owned());
    }
    names.retain(|name| name.starts_with("ticket-"));
    // Every name is `ticket-<n>`: a shorter name has a smaller number.
    names.sort_by(|left, right| left.len().cmp(&right.len()).then_with(|| left.cmp(right)));
    Ok(names)
}

/// The pid in the file `ticket` and the process state of that child, or why
/// there is no pid. A ticket is empty from its create until its child
/// writes the pid.
fn ticket_child_line(ticket: &Path) -> String {
    match std::fs::read_to_string(ticket) {
        Ok(text) => match text.trim() {
            "" => "the ticket is empty: the child did not write its pid yet".to_owned(),
            pid_text => match pid_text.parse::<libc::pid_t>() {
                Ok(pid) => process_line(pid),
                Err(error) => format!("the ticket holds {text:?}: {error}"),
            },
        },
        Err(error) => format!("the ticket was not read: {error}"),
    }
}

/// The command, the state and the kernel wait of process `pid`, from
/// `/proc/<pid>/stat` and `/proc/<pid>/wchan`.
#[cfg(target_os = "linux")]
fn process_line(pid: libc::pid_t) -> String {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat_fields(&stat),
        Err(error) => format!("stat not read: {error}"),
    };
    let wchan = match std::fs::read_to_string(format!("/proc/{pid}/wchan")) {
        Ok(wchan) => wchan.trim().to_owned(),
        Err(error) => format!("not read: {error}"),
    };
    format!("process {pid}: {stat}, wchan {wchan}")
}

/// The process state is in `/proc`, which this platform does not have.
#[cfg(not(target_os = "linux"))]
fn process_line(pid: libc::pid_t) -> String {
    format!("process {pid}: state not read, this platform has no /proc")
}

/// The command and the state letter of one `/proc/<pid>/stat` text:
/// `<pid> (<command>) <state> ...`. The command can hold `) `, so the last
/// `) ` ends it.
#[cfg(target_os = "linux")]
fn stat_fields(stat: &str) -> String {
    let fields = stat
        .split_once(" (")
        .and_then(|(_, rest)| rest.rsplit_once(") "))
        .and_then(|(command, rest)| rest.split_whitespace().next().map(|state| (command, state)));
    match fields {
        Some((command, state)) => format!("command {command}, state {state}"),
        None => format!("stat not parsed: {stat:?}"),
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
    until(
        "the slot serves a child",
        STATE_WAIT,
        &[Shown::Board(board)],
        || serving_pid(board, index).is_some(),
    )
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
    until(
        "the process ended",
        STATE_WAIT,
        &[Shown::Child(pid)],
        || process_ended(pid),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

/// A directory of one test, removed at the end of the test.
pub(crate) struct TestDir(PathBuf);

impl TestDir {
    pub(crate) fn new() -> Self {
        let path = std::env::temp_dir().join(format!("horsies-slots-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    /// The path of `name` in the directory, as script text.
    pub(crate) fn file(&self, name: &str) -> String {
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

/// The children that [`children_released_at_once_take_each_ticket_once`]
/// releases at once.
const CONCURRENT_TAKERS: usize = 32;
// A child tries at most one number for each child before it, so no child
// of that test reaches the bound of the ticket step.
const _: () = assert!(CONCURRENT_TAKERS <= TICKET_ATTEMPTS as usize);
/// The longest wait of that test until each child is ready, and again
/// until each child has ended.
const TAKERS_WAIT: Duration = Duration::from_secs(30);

/// [`CONCURRENT_TAKERS`] bash children run only the ticket step on one
/// folder. Each child marks that it is ready, then waits on one shared
/// pipe. The test writes the release only when every child is ready, so
/// all children race for the same numbers. Each number from 1 to
/// [`CONCURRENT_TAKERS`] goes to exactly one child, and the folder holds
/// exactly those tickets.
#[tokio::test]
async fn children_released_at_once_take_each_ticket_once() {
    use std::collections::BTreeSet;
    use std::io::Write;
    use std::process::{Child, Command, Output, Stdio};

    let dir = TestDir::new();
    let tickets = dir.path().join("tickets");
    let ready = dir.path().join("ready");
    std::fs::create_dir(&tickets).unwrap();
    std::fs::create_dir(&ready).unwrap();
    let command = shell(&format!(
        r#": > "{}/$$"; read -r release; {}echo "$n""#,
        ready.display(),
        ticket_step(&tickets),
    ));
    let (release_reader, mut release_writer) = std::io::pipe().unwrap();
    let mut children: Vec<Child> = (0..CONCURRENT_TAKERS)
        .map(|_| {
            Command::new(&command.program)
                .args(&command.args)
                .stdin(release_reader.try_clone().unwrap())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    drop(release_reader);
    until("each child is ready", TAKERS_WAIT, &[], || {
        std::fs::read_dir(&ready).unwrap().count() == CONCURRENT_TAKERS
    })
    .await;

    // One line for each child, in one write. bash reads a pipe one byte per
    // `read`, so each child takes one line and all children go on at once.
    release_writer
        .write_all(&[b'\n'; CONCURRENT_TAKERS])
        .unwrap();
    drop(release_writer);
    until("each child ended", TAKERS_WAIT, &[], || {
        children
            .iter_mut()
            .all(|child| child.try_wait().unwrap().is_some())
    })
    .await;
    let ends: Vec<Output> = children
        .into_iter()
        .map(|child| child.wait_with_output().unwrap())
        .collect();
    let mut numbers: Vec<usize> = ends
        .iter()
        .map(|end| {
            assert!(end.status.success(), "a child failed: {end:?}");
            String::from_utf8_lossy(&end.stdout)
                .trim()
                .parse()
                .unwrap_or_else(|error| {
                    panic!("a child printed no ticket number: {end:?}: {error}")
                })
        })
        .collect();
    numbers.sort_unstable();
    let names: BTreeSet<String> = std::fs::read_dir(&tickets)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();

    assert_eq!(
        numbers,
        (1..=CONCURRENT_TAKERS).collect::<Vec<_>>(),
        "the ticket number of each child, sorted"
    );
    assert_eq!(
        names,
        (1..=CONCURRENT_TAKERS)
            .map(|number| format!("ticket-{number}"))
            .collect::<BTreeSet<_>>()
    );
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
