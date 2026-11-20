//! A shared async pool driven by scripted children that
//! report the stop of each run with frames.
//!
//! Python's child cancels a run at its deadline and reports each step; the
//! scripts here send those reports, so each test checks what the parent
//! does with them.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot::error::TryRecvError;
use uuid::Uuid;

use super::scripted::{
    act_on_runs, answer_of, boot, ended, layout, leftover_cancel, leftover_stuck, loop_blocked,
    process_ended, read_frame, reported, run_ref, run_stuck, send, send_later, serving,
    shared_async_init, shared_config, shared_result, shared_result_with_rss, shared_timed_out,
    shell, start_shared, submit_shared, task, ticket, tickets, until, TestDir, SPAWNER_STOP_WAIT,
    STATE_WAIT,
};
use super::seat_runs::{
    backstop_step, end_at_loss, place_on_shared_child, shared_recycle, Backstop, BackstopStep,
    CloseCulprit, LossSight, OpenChildLoad, Placement, Recycle, RunStopReport, RunWork, SeatRun,
};
use super::shared_stop::ChildKill;
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    ChildAfterFrame, ChildFrame, LoopCulprit, ProcessId, RefusalReason, RssMb, RssReading, RunEnd,
    SeatEnd, SharedResultFrame, SharedWork, StopReason, StoredOptionsRefusal,
    CORE_STOPPED_EXIT_CODE,
};
use crate::worker::executor::supervisor::{
    ChildExit, ChildLost, LossCause, PartialFrame, ReapedExit, Spawner,
};

/// A long deadline: no backstop of such a run comes due in a test.
const LONG_MS: u64 = 60_000;
/// The shortest timeout and grace the tests give a run that times out.
const SHORT_MS: u64 = 1_000;
const GRACE_MS: u64 = 100;

fn long_work() -> SharedWork {
    super::scripted::async_work(LONG_MS, 1_000)
}

fn short_work() -> SharedWork {
    super::scripted::async_work(SHORT_MS, GRACE_MS)
}

fn shared_task(number: u128) -> ClaimedTask {
    task(number, ExecutionClass::SharedAsync)
}

/// A shared async pool of `children` children with `seats` seats each.
fn async_pool(
    spawner: &Spawner,
    children: usize,
    config: SharedPoolConfig,
) -> (SharedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(0, children, 0)));
    let pool = start_shared(spawner, &board, config);
    (pool, board)
}

/// The children of a test: each takes a boot number, boots, and acts on
/// `runs` run frames.
fn children(dir: &TestDir, seats: u64, actions: &[(&ClaimedTask, String)], runs: usize) -> String {
    format!(
        "{}{}{}",
        ticket(dir),
        boot(&shared_async_init(seats)),
        act_on_runs(actions, runs)
    )
}

/// Log `<boot number> <task id>`, then do `action`.
fn logged(dir: &TestDir, task: &ClaimedTask, action: String) -> String {
    format!(
        r#"echo "$n {}" >> "{}"; {action}"#,
        task.task_id,
        dir.file("runs")
    )
}

/// The boot number of the child that ran each task of the log.
fn child_by_task(dir: &TestDir) -> HashMap<Uuid, String> {
    dir.lines("runs")
        .iter()
        .map(|line| {
            let (child, task_id) = line.split_once(' ').unwrap();
            (Uuid::parse_str(task_id).unwrap(), child.to_owned())
        })
        .collect()
}

async fn stop_shared(pool: SharedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

fn crash_reason(end: AttemptEnd) -> String {
    match end {
        AttemptEnd::Crashed { reason } => reason.into_string(),
        other => panic!("expected the crash path, got {other:?}"),
    }
}

fn timed_out(retry: RetryPlacement, cancel: TimeoutCancel) -> AttemptEnd {
    AttemptEnd::TimedOut {
        timeout: super::scripted::timeout_ms(SHORT_MS),
        retry,
        cancel,
    }
}

fn not_answered(receiver: &mut tokio::sync::oneshot::Receiver<AttemptEnd>) -> bool {
    matches!(receiver.try_recv(), Err(TryRecvError::Empty))
}

// ---------------------------------------------------------------------------
// Pure rules
// ---------------------------------------------------------------------------

/// Python `TestPlacement::test_the_child_with_the_fewest_tasks_takes_the_next`,
/// `::test_the_lowest_index_wins_among_equals`, `::test_a_full_child_takes_no_task`,
/// `::test_no_free_seat`.
#[test]
fn placement_picks_the_fewest_runs_skips_a_full_child_and_the_lowest_slot_wins() {
    let load = |slot, runs, seats| OpenChildLoad {
        slot,
        runs,
        seats: NonZeroUsize::new(seats).unwrap(),
    };
    assert_eq!(
        place_on_shared_child(&[load(2, 3, 5), load(3, 1, 5), load(4, 2, 5)]),
        Placement::OnSlot(3)
    );
    assert_eq!(
        place_on_shared_child(&[load(5, 1, 2), load(4, 1, 2)]),
        Placement::OnSlot(4)
    );
    assert_eq!(
        place_on_shared_child(&[load(0, 2, 2), load(1, 3, 4)]),
        Placement::OnSlot(1)
    );
    assert_eq!(
        place_on_shared_child(&[load(0, 1, 1)]),
        Placement::NoSeatFree
    );
    assert_eq!(place_on_shared_child(&[]), Placement::NoSeatFree);
}

/// Python `TestRecycleReason::test_no_limit_never_closes`.
#[test]
fn a_shared_child_with_no_limit_never_closes() {
    assert_eq!(
        shared_recycle(
            TaskLimit::Off,
            MemoryLimit::Off,
            NonZeroUsize::new(4).unwrap(),
            1_000_000_000,
            RssReading::Mb(RssMb::new(1_000_000.0).unwrap()),
        ),
        Recycle::ServeOn
    );
}

/// Python `TestBlockedLoop::test_a_frozen_child_that_holds_only_a_refusal_is_ended`:
/// a refusal has no stop to report, so its backstop kills a child that
/// gave no end; at the kill the refusal takes the crash path.
#[test]
fn a_refusal_on_a_frozen_child_is_killed_by_its_backstop_and_takes_the_crash_path() {
    let now = tokio::time::Instant::now();
    assert_eq!(
        backstop_step(
            Backstop::after_send(RunWork::Refusal, now),
            RunWork::Refusal,
            &RunStopReport::NotReported
        ),
        BackstopStep::KillChild
    );
    let seat = SeatRun {
        run: run_ref(&shared_task(1), 1),
        work: RunWork::Refusal,
        sent_at: now,
        report: RunStopReport::NotReported,
        attached: (),
    };
    let killed = match LossSight::of(ChildLost {
        cause: LossCause::KilledByParent,
        exit: ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
        partial: PartialFrame::NoneAtHand,
        pid: ProcessId::new(7).unwrap(),
    }) {
        LossSight::Seen(seen) => seen,
        LossSight::NotSeen(lost) => panic!("a reaped exit is seen: {lost}"),
    };
    assert!(matches!(
        end_at_loss(
            &seat,
            &ChildKill::NoNamedKill,
            CloseCulprit::NotNamed,
            killed,
            now + Duration::from_secs(200)
        ),
        AttemptEnd::Crashed { .. }
    ));
}

// ---------------------------------------------------------------------------
// Seats and placement with real children
// ---------------------------------------------------------------------------

/// Python `TestSeats::test_one_child_runs_its_seats_at_once`: the child
/// reads four run frames before it answers any.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_child_runs_its_seats_at_once() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(shared_task).collect();
    let all_results: Vec<ChildFrame> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| shared_result(run_ref(task, u64::from(value)), value))
        .collect();
    let mut actions: Vec<(&ClaimedTask, String)> = tasks[..3]
        .iter()
        .map(|task| (task, ":".to_owned()))
        .collect();
    actions.push((&tasks[3], send(&all_results)));
    let script = children(&dir, 4, &actions, 4);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(4)),
    );

    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_shared(&pool, task, long_work()))
        .collect();

    for (receiver, value) in receivers.into_iter().zip(1_u32..) {
        assert_eq!(answer_of(receiver).await, reported(value));
    }
    assert_eq!(tickets(&dir), 1);
    stop_shared(pool, spawner).await;
}

/// Python `TestSeats::test_a_free_seat_takes_the_next_task_at_once`: the
/// third task reaches the child as soon as the first run ends, while the
/// second still runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_free_seat_takes_the_next_task_at_once() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=3).map(shared_task).collect();
    let run = |index: usize| run_ref(&tasks[index], u64::try_from(index + 1).unwrap());
    let actions = vec![
        (&tasks[0], ":".to_owned()),
        (
            &tasks[1],
            format!("sleep 0.3; {}", send(&[shared_result(run(0), 1)])),
        ),
        (
            &tasks[2],
            send(&[shared_result(run(2), 3), shared_result(run(1), 2)]),
        ),
    ];
    let script = children(&dir, 2, &actions, 3);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );

    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_shared(&pool, task, long_work()))
        .collect();

    for (receiver, value) in receivers.into_iter().zip(1_u32..) {
        assert_eq!(answer_of(receiver).await, reported(value));
    }
    assert_eq!(tickets(&dir), 1);
    stop_shared(pool, spawner).await;
}

/// Python `TestSeats::test_a_task_goes_to_the_child_with_the_fewest_tasks`:
/// four tasks on two open children of three seats go two to each, in turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tasks_spread_over_the_open_children_by_their_runs() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(shared_task).collect();
    let actions: Vec<(&ClaimedTask, String)> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| {
            let answer = format!(
                "sleep 0.3; {}",
                send(&[shared_result(run_ref(task, u64::from(value)), value)])
            );
            (task, logged(&dir, task, answer))
        })
        .collect();
    let script = children(&dir, 3, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        2,
        shared_config(shell(&script), shared_async_init(3)),
    );
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_shared(&pool, task, long_work()))
        .collect();

    for (receiver, value) in receivers.into_iter().zip(1_u32..) {
        assert_eq!(answer_of(receiver).await, reported(value));
    }
    let by_task = child_by_task(&dir);
    let child_of = |index: usize| by_task[&tasks[index].task_id].clone();
    assert_eq!(child_of(0), child_of(2));
    assert_eq!(child_of(1), child_of(3));
    assert_ne!(child_of(0), child_of(1));
    stop_shared(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Recycle
// ---------------------------------------------------------------------------

/// Python `TestRecycle::test_a_child_closes_after_its_tasks_per_seat`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_child_closes_after_its_tasks_per_seat() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(shared_task).collect();
    let actions: Vec<(&ClaimedTask, String)> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| {
            let answer = send(&[shared_result(run_ref(task, u64::from(value)), value)]);
            (task, logged(&dir, task, answer))
        })
        .collect();
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let config = SharedPoolConfig {
        tasks_per_seat: TaskLimit::After(NonZeroU64::MIN),
        ..shared_config(shell(&script), shared_async_init(2))
    };
    let (pool, board) = async_pool(&spawner, 1, config);
    let first_child = serving(&board, 0).await;

    for (task, value) in tasks.iter().zip(1_u32..) {
        assert_eq!(
            answer_of(submit_shared(&pool, task, long_work())).await,
            reported(value)
        );
    }

    let by_task = child_by_task(&dir);
    let child_of = |index: usize| by_task[&tasks[index].task_id].clone();
    assert_eq!(child_of(0), child_of(1));
    assert_eq!(child_of(2), child_of(3));
    assert_ne!(child_of(0), child_of(2));
    ended(first_child.as_pid_t()).await;
    stop_shared(pool, spawner).await;
}

/// Python `TestRecycle::test_a_closing_child_finishes_its_tasks_before_it_retires`:
/// a closing child takes no new task and counts only its runs; it retires
/// after its last run, and the waiting task goes to its replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closing_child_finishes_its_runs_before_it_retires() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(shared_task).collect();
    let result = |index: usize| {
        let value = u32::try_from(index + 1).unwrap();
        shared_result(run_ref(&tasks[index], u64::from(value)), value)
    };
    let actions = vec![
        (
            &tasks[0],
            logged(&dir, &tasks[0], send_later("1.5", &[result(0)])),
        ),
        (&tasks[1], logged(&dir, &tasks[1], send(&[result(1)]))),
        (&tasks[2], logged(&dir, &tasks[2], send(&[result(2)]))),
        (&tasks[3], logged(&dir, &tasks[3], send(&[result(3)]))),
    ];
    let script = children(&dir, 2, &actions, 3);
    let spawner = Spawner::start().unwrap();
    let config = SharedPoolConfig {
        tasks_per_seat: TaskLimit::After(NonZeroU64::MIN),
        ..shared_config(shell(&script), shared_async_init(2))
    };
    let (pool, board) = async_pool(&spawner, 1, config);
    let _ = serving(&board, 0).await;

    let mut receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_shared(&pool, task, long_work()))
        .collect();
    until("the child closes", STATE_WAIT, || {
        matches!(board.phase(0), SlotPhase::Closing { .. })
    })
    .await;

    assert_eq!(shared_capacity(&pool.service()), 1);
    assert!(not_answered(&mut receivers[3]));
    for (receiver, value) in receivers.into_iter().zip(1_u32..) {
        assert_eq!(answer_of(receiver).await, reported(value));
    }
    let by_task = child_by_task(&dir);
    let child_of = |index: usize| by_task[&tasks[index].task_id].clone();
    assert_eq!(child_of(0), child_of(2));
    assert_ne!(child_of(0), child_of(3));
    stop_shared(pool, spawner).await;
}

/// A shared result frame that says the child ends itself (its core
/// stopped) answers its run and closes the child to new runs: the second
/// task, queued at once, runs on a new child. The first child waits 0.5 s
/// after the frame, notes its end in a file and exits with the core-stopped
/// code. A run frame sent to it in that time is never read, and its task
/// would take the crash path; a kill in that time would leave no note.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_child_that_ends_itself_gets_no_next_run() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let second = shared_task(2);
    let init = shared_async_init(1);
    let released = ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref(&first, 1),
        end: SeatEnd::Finished(RunEnd::StoppedBeforeRunning(
            StopReason::OwnershipUnconfirmed,
        )),
        started_at: crate::events::AttemptStart::NotReported,
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::EndsItself,
    });
    let second_result = send(&[shared_result(run_ref(&second, 2), 2)]);
    let script = format!(
        r#"{}case $n in 1) {}{}{}sleep 0.5; echo ended >> "{}"; exit {CORE_STOPPED_EXIT_CODE};; *) {}{};; esac"#,
        ticket(&dir),
        boot(&init),
        read_frame(),
        send(&[released]),
        dir.file("first-child"),
        boot(&init),
        act_on_runs(&[(&second, second_result)], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(&spawner, 1, shared_config(shell(&script), init));

    let first_answer = submit_shared(&pool, &first, long_work());
    let second_answer = submit_shared(&pool, &second, long_work());

    assert_eq!(
        answer_of(first_answer).await,
        AttemptEnd::StoppedBeforeRunning(StopReason::OwnershipUnconfirmed)
    );
    assert_eq!(answer_of(second_answer).await, reported(2));
    assert_eq!(dir.lines("first-child"), vec!["ended".to_owned()]);
    assert_eq!(tickets(&dir), 2);
    stop_shared(pool, spawner).await;
}

/// Python `TestRecycle::test_a_child_above_its_memory_limit_is_replaced`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_child_above_its_memory_limit_is_replaced() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=2).map(shared_task).collect();
    let rss = RssReading::Mb(RssMb::new(5.0).unwrap());
    let actions: Vec<(&ClaimedTask, String)> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| {
            let answer = send(&[shared_result_with_rss(
                run_ref(task, u64::from(value)),
                value,
                rss,
            )]);
            (task, logged(&dir, task, answer))
        })
        .collect();
    let script = children(&dir, 2, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let config = SharedPoolConfig {
        memory_per_child: MemoryLimit::AtMb(NonZeroU64::MIN),
        ..shared_config(shell(&script), shared_async_init(2))
    };
    let (pool, _board) = async_pool(&spawner, 1, config);

    for (task, value) in tasks.iter().zip(1_u32..) {
        assert_eq!(
            answer_of(submit_shared(&pool, task, long_work())).await,
            reported(value)
        );
    }

    let by_task = child_by_task(&dir);
    assert_ne!(by_task[&tasks[0].task_id], by_task[&tasks[1].task_id]);
    stop_shared(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Deadlines and backstops
// ---------------------------------------------------------------------------

/// Python `TestDeadline::test_the_child_cancels_a_task_at_its_deadline` and
/// `::test_a_value_after_the_deadline_is_the_timeout` (the parent half):
/// a run the child ended at its deadline times out in its class; its
/// neighbour ends with its result, and the child serves on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_timed_out_in_its_child_ends_at_its_timeout_beside_a_neighbour() {
    let dir = TestDir::new();
    let late = shared_task(1);
    let neighbour = shared_task(2);
    let late_run = run_ref(&late, 1);
    let actions = vec![
        (
            &late,
            send(&[
                ChildFrame::DeadlineFired(late_run),
                shared_timed_out(late_run),
            ]),
        ),
        (
            &neighbour,
            send(&[shared_result(run_ref(&neighbour, 2), 2)]),
        ),
    ];
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );

    let late_answer = submit_shared(&pool, &late, short_work());
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    assert_eq!(
        answer_of(late_answer).await,
        timed_out(
            RetryPlacement::SameClass,
            TimeoutCancel::Ended(CancelEnd::ReturnedAfterSignal)
        )
    );
    assert_eq!(answer_of(neighbour_answer).await, reported(2));
    assert_eq!(tickets(&dir), 1);
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_a_refusal_has_no_deadline_in_the_child` (the
/// parent half): a refusal on a seat ends with what the child reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_on_a_shared_seat_ends_with_its_result() {
    let dir = TestDir::new();
    let refused = shared_task(1);
    let actions = vec![(&refused, send(&[shared_result(run_ref(&refused, 1), 1)]))];
    let script = children(&dir, 1, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );
    let refusal = SharedWork::Refusal(StoredOptionsRefusal::TimeoutValueInvalid(
        RefusalReason::cut("y"),
    ));

    let answer = answer_of(submit_shared(&pool, &refused, refusal)).await;

    assert_eq!(answer, reported(1));
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_the_backstop_kills_a_frozen_child` and
/// `TestBlockedLoop::test_a_block_that_holds_the_gil_names_no_culprit`: a
/// child that reports nothing is killed by the backstop of the run past
/// its deadline; that run times out in its class, and its neighbour, with
/// time left and no named cause, takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_backstop_kills_a_frozen_async_child_and_its_neighbour_takes_the_crash_path() {
    let frozen = shared_task(1);
    let neighbour = shared_task(2);
    let script = format!(
        "{}{}{}exec sleep 120",
        boot(&shared_async_init(2)),
        super::scripted::read_frame(),
        super::scripted::read_frame(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let started = Instant::now();

    let frozen_answer = submit_shared(&pool, &frozen, short_work());
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    assert_eq!(
        answer_of(frozen_answer).await,
        timed_out(
            RetryPlacement::SameClass,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    assert!(started.elapsed() >= Duration::from_secs(11));
    let _ = crash_reason(answer_of(neighbour_answer).await);
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_the_backstop_leaves_a_live_child_whose_task_is_stuck`
/// and `TestNamedCulpritAtAKill::test_a_stuck_task_killed_at_the_drain_deadline_retries_isolated`:
/// a child that reported a run stuck is alive, so the backstops pass it;
/// the drain deadline of the stop kills it, the stuck run keeps its
/// isolated retry, and the neighbour takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_reported_a_run_stuck_survives_the_backstops_and_the_drain_kill_isolates_it() {
    let stuck = shared_task(1);
    let neighbour = shared_task(2);
    let stuck_run = run_ref(&stuck, 1);
    let script = format!(
        "{}{}{}{}exec sleep 120",
        boot(&shared_async_init(2)),
        super::scripted::read_frame(),
        super::scripted::read_frame(),
        send(&[
            ChildFrame::DeadlineFired(stuck_run),
            ChildFrame::SecondCancel(stuck_run),
            run_stuck(stuck_run),
        ]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();
    let mut stuck_answer = submit_shared(&pool, &stuck, short_work());
    let mut neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    until("the child closes", STATE_WAIT, || {
        matches!(board.phase(0), SlotPhase::Closing { .. })
    })
    .await;
    tokio::time::sleep_until((started + Duration::from_secs(12)).into()).await;
    assert!(!process_ended(pid.as_pid_t()));
    assert!(not_answered(&mut stuck_answer));
    assert!(not_answered(&mut neighbour_answer));
    let report = pool.stop(Duration::from_millis(500)).await;

    assert_eq!(
        report,
        PoolStopReport::KilledAtDrainDeadline {
            killed: vec![stuck.task_id, neighbour.task_id]
        }
    );
    assert_eq!(
        answer_of(stuck_answer).await,
        timed_out(
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    let _ = crash_reason(answer_of(neighbour_answer).await);
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestDeadline::test_the_backstop_kills_a_child_that_froze_after_the_deadline`:
/// the child reported the deadline and froze before the second cancel;
/// the backstop kills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_backstop_kills_a_child_that_reported_only_the_deadline() {
    let frozen = shared_task(1);
    let neighbour = shared_task(2);
    let script = format!(
        "{}{}{}{}exec sleep 120",
        boot(&shared_async_init(2)),
        super::scripted::read_frame(),
        super::scripted::read_frame(),
        send(&[ChildFrame::DeadlineFired(run_ref(&frozen, 1))]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );

    let frozen_answer = submit_shared(&pool, &frozen, short_work());
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    assert_eq!(
        answer_of(frozen_answer).await,
        timed_out(
            RetryPlacement::SameClass,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    let _ = crash_reason(answer_of(neighbour_answer).await);
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_the_backstop_after_the_end_kills_nothing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_backstop_of_an_ended_run_kills_nothing() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let second = shared_task(2);
    let actions = vec![
        (&first, send(&[shared_result(run_ref(&first, 1), 1)])),
        (&second, send(&[shared_result(run_ref(&second, 2), 2)])),
    ];
    let script = children(&dir, 1, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();

    assert_eq!(
        answer_of(submit_shared(&pool, &first, short_work())).await,
        reported(1)
    );
    tokio::time::sleep_until((started + Duration::from_secs(12)).into()).await;

    assert!(!process_ended(pid.as_pid_t()));
    assert_eq!(
        answer_of(submit_shared(&pool, &second, long_work())).await,
        reported(2)
    );
    assert_eq!(tickets(&dir), 1);
    stop_shared(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// The stop of a run
// ---------------------------------------------------------------------------

/// Python `TestTheStopOfARun::test_a_stuck_task_closes_the_child_and_is_killed_when_only_stuck_tasks_are_left`:
/// the stuck run closes its child, which counts its two runs; once the
/// neighbour ends, only the stuck run is left, the child is killed, and the
/// stuck run times out with an isolated retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_run_waits_for_its_neighbour_then_the_child_is_killed() {
    let dir = TestDir::new();
    let neighbour = shared_task(1);
    let stuck = shared_task(2);
    let next = shared_task(3);
    let stuck_run = run_ref(&stuck, 2);
    let actions = vec![
        (
            &neighbour,
            send_later("2.5", &[shared_result(run_ref(&neighbour, 1), 1)]),
        ),
        (
            &stuck,
            send(&[
                ChildFrame::DeadlineFired(stuck_run),
                ChildFrame::SecondCancel(stuck_run),
                run_stuck(stuck_run),
            ]),
        ),
        (&next, send(&[shared_result(run_ref(&next, 3), 3)])),
    ];
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let first_child = serving(&board, 0).await;
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());
    let mut stuck_answer = submit_shared(&pool, &stuck, short_work());

    until("the child closes at the stuck run", STATE_WAIT, || {
        matches!(board.phase(0), SlotPhase::Closing { .. })
    })
    .await;

    assert_eq!(shared_capacity(&pool.service()), 2);
    assert!(not_answered(&mut stuck_answer));
    assert_eq!(answer_of(neighbour_answer).await, reported(1));
    assert_eq!(
        answer_of(stuck_answer).await,
        timed_out(
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    ended(first_child.as_pid_t()).await;
    assert_eq!(
        answer_of(submit_shared(&pool, &next, long_work())).await,
        reported(3)
    );
    assert_eq!(tickets(&dir), 2);
    stop_shared(pool, spawner).await;
}

/// Python `TestTheStopOfARun::test_a_task_that_needed_the_second_cancel_closes_its_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_needed_the_second_cancel_closes_its_child() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let next = shared_task(2);
    let run = run_ref(&first, 1);
    let actions = vec![
        (
            &first,
            send(&[
                ChildFrame::DeadlineFired(run),
                ChildFrame::SecondCancel(run),
                shared_timed_out(run),
            ]),
        ),
        (&next, send(&[shared_result(run_ref(&next, 2), 2)])),
    ];
    let script = children(&dir, 1, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );
    let first_child = serving(&board, 0).await;

    let answer = answer_of(submit_shared(&pool, &first, short_work())).await;

    assert_eq!(
        answer,
        timed_out(
            RetryPlacement::SameClass,
            TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace)
        )
    );
    assert_eq!(
        answer_of(submit_shared(&pool, &next, long_work())).await,
        reported(2)
    );
    assert_eq!(tickets(&dir), 2);
    ended(first_child.as_pid_t()).await;
    stop_shared(pool, spawner).await;
}

/// Python `TestTheStopOfARun::test_an_executor_thread_that_runs_on_is_stuck`
/// (the parent half): the only run of the child is stuck, so the child is
/// killed at once, and the run times out with an isolated retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_stuck_alone_is_killed_at_once_and_retries_isolated() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let run = run_ref(&first, 1);
    let actions = vec![(
        &first,
        send(&[
            ChildFrame::DeadlineFired(run),
            ChildFrame::SecondCancel(run),
            run_stuck(run),
        ]),
    )];
    let script = children(&dir, 1, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );
    let pid = serving(&board, 0).await;

    let answer = answer_of(submit_shared(&pool, &first, short_work())).await;

    assert_eq!(
        answer,
        timed_out(
            RetryPlacement::Isolated,
            TimeoutCancel::Ended(CancelEnd::KilledWithChild)
        )
    );
    ended(pid.as_pid_t()).await;
    stop_shared(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Work left after a return
// ---------------------------------------------------------------------------

/// Python `TestWorkLeftAfterAReturn::test_work_that_ends_within_the_grace_keeps_the_child_open`
/// (the parent half): a run that ends after its deadline time with no stop
/// report ends with its result, and its child serves on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_past_the_deadline_time_with_no_report_keeps_the_child_open() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let next = shared_task(2);
    let actions = vec![
        (
            &first,
            format!(
                "sleep 1.5; {}",
                send(&[shared_result(run_ref(&first, 1), 1)])
            ),
        ),
        (&next, send(&[shared_result(run_ref(&next, 2), 2)])),
    ];
    let script = children(&dir, 1, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );

    let answer = answer_of(submit_shared(
        &pool,
        &first,
        super::scripted::async_work(SHORT_MS, 3_000),
    ))
    .await;

    assert_eq!(answer, reported(1));
    assert_eq!(
        answer_of(submit_shared(&pool, &next, long_work())).await,
        reported(2)
    );
    assert_eq!(tickets(&dir), 1);
    stop_shared(pool, spawner).await;
}

/// Python `TestWorkLeftAfterAReturn::test_work_cancelled_after_the_grace_keeps_the_result_and_closes_the_child`
/// and `::test_a_client_opened_on_first_use_costs_a_restart_not_a_timeout`:
/// a leftover cancel keeps what the run returned and replaces the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leftover_cancel_keeps_the_result_and_replaces_the_child() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let next = shared_task(2);
    let run = run_ref(&first, 1);
    let actions = vec![
        (
            &first,
            send(&[leftover_cancel(run, 1), shared_result(run, 1)]),
        ),
        (&next, send(&[shared_result(run_ref(&next, 2), 2)])),
    ];
    let script = children(&dir, 1, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );
    let first_child = serving(&board, 0).await;

    let answer = answer_of(submit_shared(&pool, &first, long_work())).await;

    assert_eq!(answer, reported(1));
    assert_eq!(
        answer_of(submit_shared(&pool, &next, long_work())).await,
        reported(2)
    );
    assert_eq!(tickets(&dir), 2);
    ended(first_child.as_pid_t()).await;
    stop_shared(pool, spawner).await;
}

/// Python `TestWorkLeftAfterAReturn::test_stuck_work_alone_is_killed_and_the_result_kept`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stuck_left_work_alone_is_killed_and_the_result_kept() {
    let dir = TestDir::new();
    let first = shared_task(1);
    let next = shared_task(2);
    let run = run_ref(&first, 1);
    let actions = vec![
        (
            &first,
            send(&[leftover_cancel(run, 1), leftover_stuck(run, 1)]),
        ),
        (&next, send(&[shared_result(run_ref(&next, 2), 2)])),
    ];
    let script = children(&dir, 1, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(1)),
    );
    let first_child = serving(&board, 0).await;

    let answer = answer_of(submit_shared(&pool, &first, long_work())).await;

    assert_eq!(answer, reported(1));
    ended(first_child.as_pid_t()).await;
    assert_eq!(
        answer_of(submit_shared(&pool, &next, long_work())).await,
        reported(2)
    );
    stop_shared(pool, spawner).await;
}

/// Python `TestWorkLeftAfterAReturn::test_stuck_work_waits_for_a_neighbour_with_time`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stuck_left_work_waits_for_a_neighbour_with_time() {
    let dir = TestDir::new();
    let neighbour = shared_task(1);
    let stuck = shared_task(2);
    let stuck_run = run_ref(&stuck, 2);
    let actions = vec![
        (
            &neighbour,
            send_later("2.5", &[shared_result(run_ref(&neighbour, 1), 1)]),
        ),
        (
            &stuck,
            send(&[leftover_cancel(stuck_run, 2), leftover_stuck(stuck_run, 2)]),
        ),
    ];
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());
    let mut stuck_answer = submit_shared(&pool, &stuck, long_work());

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(not_answered(&mut stuck_answer));

    assert_eq!(answer_of(neighbour_answer).await, reported(1));
    assert_eq!(answer_of(stuck_answer).await, reported(2));
    stop_shared(pool, spawner).await;
}

/// Python `TestWorkLeftAfterAReturn::test_the_backstop_leaves_a_live_child_whose_left_work_is_stuck`
/// and `::test_an_outside_kill_keeps_what_a_stuck_run_returned`: the
/// backstops pass a child that reported the left work stuck; a `SIGKILL`
/// from outside then ends the child, the stuck run keeps what it returned,
/// and only the neighbour takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stuck_left_work_survives_the_backstops_and_an_outside_kill_keeps_the_result() {
    let neighbour = shared_task(1);
    let stuck = shared_task(2);
    let stuck_run = run_ref(&stuck, 2);
    let script = format!(
        "{}{}{}{}exec sleep 120",
        boot(&shared_async_init(2)),
        super::scripted::read_frame(),
        super::scripted::read_frame(),
        send(&[leftover_cancel(stuck_run, 2), leftover_stuck(stuck_run, 2)]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();
    let mut neighbour_answer = submit_shared(&pool, &neighbour, long_work());
    let mut stuck_answer = submit_shared(&pool, &stuck, short_work());

    tokio::time::sleep_until((started + Duration::from_secs(12)).into()).await;
    assert!(!process_ended(pid.as_pid_t()));
    assert!(not_answered(&mut stuck_answer));
    assert!(not_answered(&mut neighbour_answer));
    // SAFETY: a signal to the child of this test, which is not reaped.
    let sent = unsafe { libc::kill(pid.as_pid_t(), libc::SIGKILL) };
    assert_eq!(sent, 0);

    assert_eq!(answer_of(stuck_answer).await, reported(2));
    let reason = crash_reason(answer_of(neighbour_answer).await);
    assert!(reason.contains("child died"), "{reason}");
    stop_shared(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// A blocked loop
// ---------------------------------------------------------------------------

/// Python `TestBlockedLoop::test_the_watchdog_names_the_culprit_and_the_child_is_killed`:
/// the run that held the loop gets its own code and an isolated retry; its
/// neighbour gets the neighbour code in its class. Neither was past its
/// deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_loop_names_the_culprit_and_its_neighbour() {
    let dir = TestDir::new();
    let neighbour = shared_task(1);
    let culprit = shared_task(2);
    let actions = vec![
        (&neighbour, ":".to_owned()),
        (
            &culprit,
            send(&[loop_blocked(LoopCulprit::Run(run_ref(&culprit, 2)))]),
        ),
    ];
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = async_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_async_init(2)),
    );

    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());
    let culprit_answer = submit_shared(&pool, &culprit, long_work());

    match answer_of(culprit_answer).await {
        AttemptEnd::KilledForLoop { code, retry, .. } => {
            assert_eq!(code, LoopKillCode::TaskBlockedLoop);
            assert_eq!(retry, RetryPlacement::Isolated);
        }
        other => panic!("expected the blocked-loop verdict, got {other:?}"),
    }
    match answer_of(neighbour_answer).await {
        AttemptEnd::KilledForLoop { code, retry, .. } => {
            assert_eq!(code, LoopKillCode::NeighbourBlockedLoop);
            assert_eq!(retry, RetryPlacement::SameClass);
        }
        other => panic!("expected the neighbour verdict, got {other:?}"),
    }
    stop_shared(pool, spawner).await;
}
