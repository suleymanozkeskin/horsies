//! The slot paths of a killed or lost child whose exit is not seen within
//! the kill wait, a spawn refused in the parent, and the runner beat
//! registry of shared runs. The children are real; [`HeldExits`] holds back
//! the exit that the spawner published (Python tests use a process double
//! that never ends).
//!
//! - Stale RUNNING recovery on: the attempt ends `EndUnknown` and nothing
//!   is written.
//! - Stale RUNNING recovery off: the slot waits for the exit as long as the
//!   pool lives, then answers as at a seen exit; a stop first ends the
//!   attempt `EndUnknown`.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use super::held_exits::{ExitRelease, HeldExits};
use super::scripted::{
    async_work, boot, ended, idle_child, input, isolated_config, isolated_init, layout,
    leftover_cancel, ok_envelope, printf_bytes, read_frame, reported, result_frame, run_ref, send,
    serving, serving_pid, shared_async_init, shared_config, shared_result, shared_thread_init,
    shell, start_isolated_from, start_shared_from, submit_shared, task, thread_work, timeout_ms,
    until, TestDir, ANSWER_WAIT, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    encode_child_frame, BodyEnd, BodyReturnedFrame, ChildFrame, InitFrame, IsolatedWork,
    LeftoverName, LoopHoldsTasks, ShortageErrno,
};
use crate::worker::executor::supervisor::{BootFailureCause, Spawner};
use crate::worker::retry::MIN_TASK_TIMEOUT_MS;
use crate::worker::runner_beats::LiveDispatches;

/// The kill wait of the pools of these tests, in place of 30 s.
const SHORT_KILL_WAIT: Duration = Duration::from_millis(500);
/// Longer than the kill wait and the grace of a lost channel: the slot has
/// passed the kill wait by then.
const PAST_THE_KILL_WAIT: Duration = Duration::from_millis(1_500);
/// The deadline of the tasks that a deadline ends: the shortest timeout a
/// task can have (Python `MIN_TASK_TIMEOUT_MS`).
const SHORT_DEADLINE_MS: u64 = MIN_TASK_TIMEOUT_MS;
/// Past the deadline, then past the kill wait: the slot whose child the
/// deadline killed has passed the kill wait by then.
const PAST_THE_DEADLINE_KILL_WAIT: Duration =
    PAST_THE_KILL_WAIT.saturating_add(Duration::from_millis(SHORT_DEADLINE_MS));
/// A kill wait that the tests of an exit seen within it never pass: the
/// test releases the held exit long before.
const LONG_KILL_WAIT: Duration = Duration::from_secs(20);
/// How long the test watches a slot that waits for the exit of its killed
/// child, within [`LONG_KILL_WAIT`].
const WATCHED_IN_THE_KILL_WAIT: Duration = Duration::from_millis(500);

fn isolated(script: &str, exit_not_seen: ExitNotSeen, kill_wait: Duration) -> IsolatedPoolConfig {
    IsolatedPoolConfig {
        kill_wait,
        exit_not_seen,
        ..isolated_config(shell(script))
    }
}

fn shared(script: &str, init: InitFrame, exit_not_seen: ExitNotSeen) -> SharedPoolConfig {
    SharedPoolConfig {
        kill_wait: SHORT_KILL_WAIT,
        exit_not_seen,
        ..shared_config(shell(script), init)
    }
}

/// One isolated slot whose child exits are held from the start.
struct HeldPool {
    spawner: Spawner,
    source: Arc<HeldExits>,
    pool: IsolatedPool,
    board: Arc<PoolBoard>,
}

impl HeldPool {
    fn start(script: &str, exit_not_seen: ExitNotSeen) -> Self {
        Self::start_with_kill_wait(script, exit_not_seen, SHORT_KILL_WAIT)
    }

    fn start_with_kill_wait(script: &str, exit_not_seen: ExitNotSeen, kill_wait: Duration) -> Self {
        let spawner = Spawner::start().unwrap();
        let source = HeldExits::new(spawner.handle(), ExitRelease::Held);
        let (pool, board) = start_isolated_from(
            &spawner,
            &source,
            1,
            isolated(script, exit_not_seen, kill_wait),
            LiveDispatches::default(),
        );
        Self {
            spawner,
            source,
            pool,
            board,
        }
    }

    fn submit(
        &self,
        task: &ClaimedTask,
        deadline: IsolatedDeadline,
    ) -> oneshot::Receiver<AttemptEnd> {
        super::scripted::queued(self.pool.submit(task, input(), IsolatedWork::Run, deadline))
    }

    async fn stop(self) {
        self.source.release();
        let _ = self.pool.stop(Duration::from_secs(5)).await;
        let _ = self.spawner.stop(SPAWNER_STOP_WAIT);
    }
}

async fn answer_within(receiver: oneshot::Receiver<AttemptEnd>) -> AttemptEnd {
    tokio::time::timeout(ANSWER_WAIT, receiver)
        .await
        .expect("the task is answered within the test wait")
        .expect("every task gets an answer")
}

/// The answer has not come yet.
fn still_waiting(receiver: &mut oneshot::Receiver<AttemptEnd>) {
    assert_eq!(
        receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty),
        "the task was answered before its child's exit was seen"
    );
}

fn crash_reason(end: AttemptEnd) -> String {
    match end {
        AttemptEnd::Crashed { reason } => reason.into_string(),
        other => panic!("expected the crash path, got {other:?}"),
    }
}

/// Boot, read one run frame, then run `after`.
fn after_one_run(after: &str) -> String {
    format!("{}{}{after}", boot(&isolated_init()), read_frame())
}

fn body_returned(task: &ClaimedTask, value: u32) -> ChildFrame {
    ChildFrame::BodyReturned(BodyReturnedFrame {
        task_id: task.task_id,
        returned: BodyEnd::Result(ok_envelope(value)),
        left: LoopHoldsTasks::new(NonZeroU64::MIN, vec![LeftoverName::cut("_write_after")])
            .unwrap(),
        started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    })
}

fn deadline(milliseconds: u64) -> IsolatedDeadline {
    IsolatedDeadline::After(timeout_ms(milliseconds))
}

// ---------------------------------------------------------------------------
// Isolated, stale RUNNING recovery on
// ---------------------------------------------------------------------------

/// Python `TestLossWithNoExitSeen::test_a_lost_channel` (the slot half): the
/// child dies with no frame and its exit is not seen; the end is not known.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_channel_with_no_exit_seen_ends_unknown() {
    let held = HeldPool::start(&after_one_run("exit 0"), ExitNotSeen::LeaveToRecovery);
    let child = serving(&held.board, 0).await;

    let end = answer_within(held.submit(
        &task(1, ExecutionClass::Isolated),
        IsolatedDeadline::NoDeadline,
    ))
    .await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

/// Python `TestLossWithNoExitSeen::test_a_child_that_sent_a_bad_frame` (the
/// slot half).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_frame_with_no_exit_seen_ends_unknown() {
    let script = after_one_run(&format!(
        r#"printf '{}' >&"$0"; exec sleep 60"#,
        printf_bytes(&[0; 12])
    ));
    let held = HeldPool::start(&script, ExitNotSeen::LeaveToRecovery);
    let child = serving(&held.board, 0).await;

    let end = answer_within(held.submit(
        &task(1, ExecutionClass::Isolated),
        IsolatedDeadline::NoDeadline,
    ))
    .await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

/// Python `TestLossWithNoExitSeen::test_a_cleared_slot` (the slot half): a
/// slot defect kills the child; its exit is not seen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_defect_with_no_exit_seen_ends_unknown() {
    let held = HeldPool::start(&idle_child(&isolated_init()), ExitNotSeen::LeaveToRecovery);
    let child = serving(&held.board, 0).await;
    let unsendable = ClaimedTask {
        task_name: "x".repeat(2 << 20),
        ..task(1, ExecutionClass::Isolated)
    };

    let end = answer_within(held.submit(&unsendable, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

/// Python `TestNoExitSeen::test_no_retry_and_no_phase_2_without_the_exit`,
/// `::test_the_finalizer_leaves_the_row_and_does_not_wait_for_the_slot`,
/// `TestSlotAnsweredAfterTheDeadline::test_a_late_loss_with_no_exit_seen_writes_nothing`
/// and `TestKillAndWait::test_a_child_that_does_not_end_in_the_wait_is_reported`
/// (the slot half): the deadline kills the child, its exit is not seen
/// within the kill wait, and the end is not known. `EndUnknown` writes
/// nothing (`stop_and_loss_tests.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_kill_with_no_exit_seen_ends_unknown() {
    let held = HeldPool::start(
        &after_one_run("exec sleep 60"),
        ExitNotSeen::LeaveToRecovery,
    );
    let child = serving(&held.board, 0).await;

    let end = answer_within(held.submit(
        &task(1, ExecutionClass::Isolated),
        deadline(SHORT_DEADLINE_MS),
    ))
    .await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

/// With stale RUNNING recovery on: at the deadline the slot kills the child (the spawner reaps
/// it), and while its exit is not seen, within the kill wait, the task has no
/// end. The worker writes an end only after the answer
/// (`worker::worker`, `Worker::dispatch`), so nothing is written while the
/// child may run. Once the exit is seen, the end is the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_kill_gives_no_end_before_the_exit_is_seen() {
    let held = HeldPool::start_with_kill_wait(
        &after_one_run("exec sleep 60"),
        ExitNotSeen::LeaveToRecovery,
        LONG_KILL_WAIT,
    );
    let child = serving(&held.board, 0).await;
    let mut answer = held.submit(
        &task(1, ExecutionClass::Isolated),
        deadline(SHORT_DEADLINE_MS),
    );

    ended(child.as_pid_t()).await;
    tokio::time::sleep(WATCHED_IN_THE_KILL_WAIT).await;
    still_waiting(&mut answer);
    held.source.release();
    let end = answer_within(answer).await;

    assert_eq!(
        end,
        AttemptEnd::TimedOut {
            timeout: timeout_ms(SHORT_DEADLINE_MS),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    held.stop().await;
}

/// How long after the run frame the late writer of
/// [`a_result_that_reaches_the_socket_after_the_deadline_is_not_read`]
/// sends the result: past the deadline by two seconds.
const LATE_RESULT_AFTER_S: u64 = 3;

/// A complete result frame that reaches the socket after
/// the deadline is not read; the end is the timeout. The late writer is a
/// process of the child in a session of its own (`perl`, `POSIX::setsid`),
/// so the kill of the child's session does not end it, and it holds the
/// child's socket. The exit of the killed child is held until the frame was
/// written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_that_reaches_the_socket_after_the_deadline_is_not_read() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let frame = encode_child_frame(&result_frame(&first, 1))
        .unwrap()
        .to_bytes();
    let late_writer = dir.file("late_writer.sh");
    std::fs::write(
        &late_writer,
        format!(
            r#"sleep {LATE_RESULT_AFTER_S}; printf '{}' >&"$1"; touch "{}""#,
            printf_bytes(&frame),
            dir.file("sent"),
        ),
    )
    .unwrap();
    let script = after_one_run(&format!(
        r#"( exec perl -MPOSIX -e 'defined POSIX::setsid() or die "setsid: $!"; exec @ARGV or die "exec: $!"' {shell} "{late_writer}" "$0" ) & exec sleep 60"#,
        shell = crate::test_support::SCRIPT_SHELL,
    ));
    let held =
        HeldPool::start_with_kill_wait(&script, ExitNotSeen::LeaveToRecovery, LONG_KILL_WAIT);
    let child = serving(&held.board, 0).await;
    let mut answer = held.submit(&first, deadline(SHORT_DEADLINE_MS));

    ended(child.as_pid_t()).await;
    until("the late writer sent the result", STATE_WAIT, || {
        dir.path().join("sent").exists()
    })
    .await;
    still_waiting(&mut answer);
    held.source.release();
    let end = answer_within(answer).await;

    assert_eq!(
        end,
        AttemptEnd::TimedOut {
            timeout: timeout_ms(SHORT_DEADLINE_MS),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    held.stop().await;
}

/// The body returned with work left, then the child was
/// lost and its exit not seen. The left work may still run, so the end is
/// not known.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loss_after_a_return_with_no_exit_seen_ends_unknown() {
    let first = task(1, ExecutionClass::Isolated);
    let script = after_one_run(&format!("{}exit 0", send(&[body_returned(&first, 4)])));
    let held = HeldPool::start(&script, ExitNotSeen::LeaveToRecovery);
    let child = serving(&held.board, 0).await;

    let end = answer_within(held.submit(&first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

// ---------------------------------------------------------------------------
// Isolated, stale RUNNING recovery off
// ---------------------------------------------------------------------------

/// Python `TestLossWithStaleRecoveryOff::test_the_row_is_recovered_once_the_exit_is_seen`:
/// the slot waits for the exit past the kill wait; once it is seen, the
/// task takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_channel_waits_for_the_exit_with_recovery_off() {
    let held = HeldPool::start(&after_one_run("exit 0"), ExitNotSeen::WaitForExit);
    let _ = serving(&held.board, 0).await;
    let mut answer = held.submit(
        &task(1, ExecutionClass::Isolated),
        IsolatedDeadline::NoDeadline,
    );

    tokio::time::sleep(PAST_THE_KILL_WAIT).await;
    still_waiting(&mut answer);
    held.source.release();
    let reason = crash_reason(answer_within(answer).await);

    assert!(reason.contains("channel to the child was lost"), "{reason}");
    held.stop().await;
}

/// Python `TestNoExitSeen::test_with_stale_recovery_off_the_timeout_waits_for_the_late_exit`
/// and `TestKillAndWait::test_an_exit_after_the_bounded_wait_is_still_seen`:
/// an exit seen after the kill wait still gives the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_kill_waits_for_the_late_exit_with_recovery_off() {
    let held = HeldPool::start(&after_one_run("exec sleep 60"), ExitNotSeen::WaitForExit);
    let _ = serving(&held.board, 0).await;
    let mut answer = held.submit(
        &task(1, ExecutionClass::Isolated),
        deadline(SHORT_DEADLINE_MS),
    );

    tokio::time::sleep(PAST_THE_DEADLINE_KILL_WAIT).await;
    still_waiting(&mut answer);
    held.source.release();
    let end = answer_within(answer).await;

    assert_eq!(
        end,
        AttemptEnd::TimedOut {
            timeout: timeout_ms(SHORT_DEADLINE_MS),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    held.stop().await;
}

/// After a return with work left and a loss whose exit came late, the task
/// ends with what its body returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loss_after_a_return_ends_with_the_return_once_the_late_exit_is_seen() {
    let first = task(1, ExecutionClass::Isolated);
    let script = after_one_run(&format!("{}exit 0", send(&[body_returned(&first, 4)])));
    let held = HeldPool::start(&script, ExitNotSeen::WaitForExit);
    let _ = serving(&held.board, 0).await;
    let mut answer = held.submit(&first, IsolatedDeadline::NoDeadline);

    tokio::time::sleep(PAST_THE_KILL_WAIT).await;
    still_waiting(&mut answer);
    held.source.release();

    assert_eq!(answer_within(answer).await, reported(4));
    held.stop().await;
}

/// A stop while the slot waits for the exit ends the attempt unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_before_the_late_exit_ends_the_attempt_unknown() {
    let held = HeldPool::start(&after_one_run("exec sleep 60"), ExitNotSeen::WaitForExit);
    let child = serving(&held.board, 0).await;
    let mut answer = held.submit(
        &task(1, ExecutionClass::Isolated),
        deadline(SHORT_DEADLINE_MS),
    );
    tokio::time::sleep(PAST_THE_DEADLINE_KILL_WAIT).await;
    still_waiting(&mut answer);

    let HeldPool {
        spawner,
        source,
        pool,
        board: _,
    } = held;
    let stopping = tokio::spawn(async move { pool.stop(Duration::from_secs(5)).await });
    let end = answer_within(answer).await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    source.release();
    let _ = stopping.await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// A spawn refused in the parent
// ---------------------------------------------------------------------------

/// Python `TestDownSlots::test_a_parent_out_of_descriptors_takes_only_the_replacing_slot_down`
/// (the spawn half): the spawn of a replacement fails short of descriptors
/// in the parent; only that slot goes down, the other keeps serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parent_short_of_descriptors_takes_only_the_replacing_slot_down() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        super::scripted::answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let source = HeldExits::new(spawner.handle(), ExitRelease::Released);
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated(&script, ExitNotSeen::LeaveToRecovery, SHORT_KILL_WAIT)
    };
    let (pool, board) =
        start_isolated_from(&spawner, &source, 2, config, LiveDispatches::default());
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;
    source.refuse_next_spawn(ShortageErrno::Emfile);

    let answer = answer_within(super::scripted::queued(pool.submit(
        &first,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    )))
    .await;
    // The down slot boots again after its wait, so the view is taken in the
    // poll that sees it down.
    let mut seen = Vec::new();
    until("the replacing slot is down", STATE_WAIT, || {
        let down = board.down_slots();
        let other_serves = down
            .first()
            .is_some_and(|(slot, _)| matches!(board.phase(1 - *slot), SlotPhase::Serving { .. }));
        match (down.len(), other_serves) {
            (1, true) => {
                seen = down;
                true
            }
            _ => false,
        }
    })
    .await;

    assert_eq!(answer, reported(1));
    assert_eq!(
        seen[0].1.cause,
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
    );
    assert_eq!(board.health(), PoolHealth::Serving);
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Shared children
// ---------------------------------------------------------------------------

/// A shared pool of one slot whose child exits are held from the start.
struct HeldSharedPool {
    spawner: Spawner,
    source: Arc<HeldExits>,
    pool: SharedPool,
    board: Arc<PoolBoard>,
}

impl HeldSharedPool {
    fn start(
        config: SharedPoolConfig,
        class: ExecutionClass,
        release: ExitRelease,
        live: LiveDispatches,
    ) -> Self {
        let spawner = Spawner::start().unwrap();
        let source = HeldExits::new(spawner.handle(), release);
        let board = Arc::new(match class {
            ExecutionClass::SharedThread => PoolBoard::new(layout(0, 0, 1)),
            ExecutionClass::SharedAsync | ExecutionClass::Isolated => {
                PoolBoard::new(layout(0, 1, 0))
            }
        });
        let pool = start_shared_from(&spawner, &source, &board, config, live);
        Self {
            spawner,
            source,
            pool,
            board,
        }
    }

    async fn stop(self) {
        self.source.release();
        let _ = self.pool.stop(Duration::from_secs(5)).await;
        let _ = self.spawner.stop(SPAWNER_STOP_WAIT);
    }
}

/// A thread child closed at a deadline is killed when no
/// run has time left; its exit is not seen, so the end is not known.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_child_killed_past_its_deadlines_with_no_exit_seen_ends_unknown() {
    let late = task(1, ExecutionClass::SharedThread);
    let init = shared_thread_init(1);
    let script = format!(
        "{}{}{}exec sleep 60",
        boot(&init),
        read_frame(),
        send(&[ChildFrame::DeadlineFired(run_ref(&late, 1))]),
    );
    let held = HeldSharedPool::start(
        shared(&script, init, ExitNotSeen::LeaveToRecovery),
        ExecutionClass::SharedThread,
        ExitRelease::Held,
        LiveDispatches::default(),
    );
    let child = serving(&held.board, 0).await;

    let end = answer_within(submit_shared(
        &held.pool,
        &late,
        thread_work(SHORT_DEADLINE_MS),
    ))
    .await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

/// A run that reported its return, on a child lost with no exit seen, ends
/// unknown: the work it left may still run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_returned_run_on_a_child_lost_with_no_exit_seen_ends_unknown() {
    let returned = task(1, ExecutionClass::SharedAsync);
    let init = shared_async_init(1);
    let script = format!(
        "{}{}{}exit 0",
        boot(&init),
        read_frame(),
        send(&[leftover_cancel(run_ref(&returned, 1), 4)]),
    );
    let held = HeldSharedPool::start(
        shared(&script, init, ExitNotSeen::LeaveToRecovery),
        ExecutionClass::SharedAsync,
        ExitRelease::Held,
        LiveDispatches::default(),
    );
    let child = serving(&held.board, 0).await;

    let end = answer_within(submit_shared(
        &held.pool,
        &returned,
        async_work(60_000, 1_000),
    ))
    .await;

    assert_eq!(end, AttemptEnd::EndUnknown { pid: child });
    held.stop().await;
}

/// The backstop kills a frozen thread child; its exit is
/// seen only after the kill wait, and the run then ends at its timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_exit_after_the_backstop_kill_gives_the_timeout_with_recovery_off() {
    let frozen = task(1, ExecutionClass::SharedThread);
    let init = shared_thread_init(1);
    let script = format!("{}{}exec sleep 60", boot(&init), read_frame());
    let held = HeldSharedPool::start(
        shared(&script, init, ExitNotSeen::WaitForExit),
        ExecutionClass::SharedThread,
        ExitRelease::Held,
        LiveDispatches::default(),
    );
    let first_child = serving(&held.board, 0).await;
    let mut answer = submit_shared(&held.pool, &frozen, thread_work(SHORT_DEADLINE_MS));

    // The backstop comes the deadline plus its margin after the send; the
    // slot then boots a new child while a task waits for the late exit.
    until(
        "the backstop killed the child",
        Duration::from_secs(40),
        || serving_pid(&held.board, 0).is_some_and(|child| child != first_child),
    )
    .await;
    still_waiting(&mut answer);
    held.source.release();
    let end = answer_within(answer).await;

    assert_eq!(
        end,
        AttemptEnd::TimedOut {
            timeout: timeout_ms(SHORT_DEADLINE_MS),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    held.stop().await;
}

/// Every run of a shared child is held for the runner beats while it runs,
/// and released at its end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_run_of_a_shared_child_is_held_while_it_runs() {
    for (init, class, work) in [
        (
            shared_async_init(2),
            ExecutionClass::SharedAsync,
            async_work(60_000, 1_000),
        ),
        (
            shared_thread_init(2),
            ExecutionClass::SharedThread,
            thread_work(60_000),
        ),
    ] {
        let dir = TestDir::new();
        let go = dir.file("go");
        let first = task(1, class);
        let second = task(2, class);
        let script = format!(
            "{}{}{}n=0; while [ ! -f {go} ] && [ $n -lt 600 ]; do sleep 0.05; n=$((n + 1)); done; {}{}exit 3",
            boot(&init),
            read_frame(),
            read_frame(),
            send(&[
                shared_result(run_ref(&first, 1), 1),
                shared_result(run_ref(&second, 2), 2),
            ]),
            read_frame(),
        );
        let live = LiveDispatches::default();
        let held = HeldSharedPool::start(
            shared(&script, init, ExitNotSeen::LeaveToRecovery),
            class,
            ExitRelease::Released,
            live.clone(),
        );
        let _ = serving(&held.board, 0).await;
        let first_answer = submit_shared(&held.pool, &first, work.clone());
        let second_answer = submit_shared(&held.pool, &second, work);

        until("both runs are held", STATE_WAIT, || {
            live.held_task_ids() == vec![first.task_id, second.task_id]
        })
        .await;
        std::fs::write(&go, b"").unwrap();
        let answers = (
            answer_within(first_answer).await,
            answer_within(second_answer).await,
        );

        assert_eq!(answers, (reported(1), reported(2)), "{class}");
        until("both runs are released", STATE_WAIT, || {
            live.held_task_ids().is_empty()
        })
        .await;
        held.stop().await;
    }
}
