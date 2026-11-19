//! A shared thread pool driven by scripted children. A thread cannot be cancelled,
//! so the first deadline closes its child, and the child is killed once no
//! run in it has time left.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot::error::TryRecvError;
use uuid::Uuid;

use super::scripted::{
    act_on_runs, act_on_runs_then, answer_of, answer_runs, boot, ended, fail_boot, idle_child,
    input, isolated_config, isolated_init, layout, process_ended, read_frame, reported, run_ref,
    send, send_later, serving, shared_async_init, shared_config, shared_result,
    shared_result_with_rss, shared_thread_init, shared_timed_out, shell, short_of_descriptors,
    start_shared, submit_shared, take, task, thread_work, ticket, tickets, timeout_ms, until,
    TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    ChildFrame, ParentFrame, RefusalReason, RssMb, RssReading, SharedWork, StoredOptionsRefusal,
};
use crate::worker::executor::supervisor::{BootFailureCause, Spawner};
use crate::worker::runner_beats::LiveDispatches;

const SHORT_MS: u64 = 1_000;

fn long_work() -> SharedWork {
    thread_work(60_000)
}

fn short_work() -> SharedWork {
    thread_work(SHORT_MS)
}

fn thread_task(number: u128) -> ClaimedTask {
    task(number, ExecutionClass::SharedThread)
}

fn thread_pool(
    spawner: &Spawner,
    children: usize,
    config: SharedPoolConfig,
) -> (SharedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(0, 0, children)));
    let pool = start_shared(spawner, &board, config);
    (pool, board)
}

fn children(dir: &TestDir, seats: u64, actions: &[(&ClaimedTask, String)], runs: usize) -> String {
    format!(
        "{}{}{}",
        ticket(dir),
        boot(&shared_thread_init(seats)),
        act_on_runs(actions, runs)
    )
}

fn logged(dir: &TestDir, task: &ClaimedTask, action: String) -> String {
    format!(
        r#"echo "$n {}" >> "{}"; {action}"#,
        task.task_id,
        dir.file("runs")
    )
}

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

fn timed_out(milliseconds: u64, retry: RetryPlacement) -> AttemptEnd {
    AttemptEnd::TimedOut {
        timeout: timeout_ms(milliseconds),
        retry,
        cancel: TimeoutCancel::NoSignal,
    }
}

fn not_answered(receiver: &mut tokio::sync::oneshot::Receiver<AttemptEnd>) -> bool {
    matches!(receiver.try_recv(), Err(TryRecvError::Empty))
}

// ---------------------------------------------------------------------------
// Seats
// ---------------------------------------------------------------------------

/// Python `TestSeats::test_one_child_runs_its_seats_at_once`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_thread_child_runs_its_seats_at_once() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(thread_task).collect();
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
    let (pool, _board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(4)),
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

/// Python `TestSeats::test_a_free_seat_takes_the_next_task_at_once`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_free_thread_seat_takes_the_next_task_at_once() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=3).map(thread_task).collect();
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
    let (pool, _board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
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

/// Python `TestSeats::test_a_task_goes_to_the_child_with_the_fewest_tasks`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_tasks_spread_over_the_open_children_by_their_runs() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(thread_task).collect();
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
    let (pool, board) = thread_pool(
        &spawner,
        2,
        shared_config(shell(&script), shared_thread_init(3)),
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

/// Python `TestSeats::test_each_kind_of_shared_child_takes_only_its_own_tasks`:
/// an async pool and a thread pool on one board each serve the work of
/// their kind, each with its own seats.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_kind_of_shared_child_takes_only_its_own_tasks() {
    let async_task = task(1, ExecutionClass::SharedAsync);
    let thread = thread_task(2);
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(0, 1, 1)));
    let async_script = format!(
        "{}{}",
        boot(&shared_async_init(2)),
        answer_runs(
            &[(&async_task, vec![shared_result(run_ref(&async_task, 1), 1)])],
            1
        ),
    );
    let async_pool = start_shared(
        &spawner,
        &board,
        shared_config(shell(&async_script), shared_async_init(2)),
    );
    let thread_script = format!(
        "{}{}",
        boot(&shared_thread_init(2)),
        answer_runs(&[(&thread, vec![shared_result(run_ref(&thread, 1), 2)])], 1),
    );
    let threads = start_shared(
        &spawner,
        &board,
        shared_config(shell(&thread_script), shared_thread_init(2)),
    );

    let async_answer = submit_shared(
        &async_pool,
        &async_task,
        super::scripted::async_work(60_000, 1_000),
    );
    let thread_answer = submit_shared(&threads, &thread, long_work());

    assert_eq!(answer_of(async_answer).await, reported(1));
    assert_eq!(answer_of(thread_answer).await, reported(2));
    assert_ne!(serving(&board, 0).await, serving(&board, 1).await);
    assert_eq!(shared_capacity(&async_pool.service()), 2);
    assert_eq!(shared_capacity(&threads.service()), 2);
    let _ = threads.stop(Duration::from_secs(5)).await;
    let _ = async_pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestLoss::test_the_run_of_a_shared_async_child_ends_the_child`
/// (the parent half): the parent never sends work of the other kind; a
/// route that would is a defect and crashes loud.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "with work of another kind")]
async fn async_work_routed_to_the_thread_pool_is_a_defect() {
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = thread_pool(
        &spawner,
        1,
        shared_config(
            shell(&idle_child(&shared_thread_init(1))),
            shared_thread_init(1),
        ),
    );
    let _ = pool.submit(
        &thread_task(1),
        input(),
        super::scripted::async_work(60_000, 1_000),
    );
}

// ---------------------------------------------------------------------------
// Recycle
// ---------------------------------------------------------------------------

/// Python `TestRecycle::test_a_child_closes_after_its_tasks_per_seat`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_child_closes_after_its_tasks_per_seat() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=4).map(thread_task).collect();
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
        ..shared_config(shell(&script), shared_thread_init(2))
    };
    let (pool, board) = thread_pool(&spawner, 1, config);
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

/// Python `TestRecycle::test_a_child_above_its_memory_limit_is_replaced`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_child_above_its_memory_limit_is_replaced() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=2).map(thread_task).collect();
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
        ..shared_config(shell(&script), shared_thread_init(2))
    };
    let (pool, _board) = thread_pool(&spawner, 1, config);

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
// Deadlines
// ---------------------------------------------------------------------------

/// Python `TestDeadline::test_a_deadline_closes_the_child_and_a_neighbour_with_time_left_finishes`:
/// the first deadline closes the child, which counts its runs and takes no
/// new task; the kill waits for the neighbour, and the run whose deadline
/// closed the child times out with an isolated retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_closes_the_thread_child_and_its_neighbour_finishes_first() {
    let dir = TestDir::new();
    let late = thread_task(1);
    let neighbour = thread_task(2);
    let later = thread_task(3);
    let actions = vec![
        (&late, send(&[ChildFrame::DeadlineFired(run_ref(&late, 1))])),
        (
            &neighbour,
            send_later("2.5", &[shared_result(run_ref(&neighbour, 2), 2)]),
        ),
        (&later, send(&[shared_result(run_ref(&later, 3), 3)])),
    ];
    let script = children(&dir, 3, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(3)),
    );
    let first_child = serving(&board, 0).await;
    let late_answer = submit_shared(&pool, &late, short_work());
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    until("the child closes at the deadline", STATE_WAIT, || {
        matches!(board.phase(0), SlotPhase::Closing { .. })
    })
    .await;
    let mut later_answer = submit_shared(&pool, &later, long_work());

    assert_eq!(shared_capacity(&pool.service()), 2);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(not_answered(&mut later_answer));
    assert_eq!(answer_of(neighbour_answer).await, reported(2));
    assert_eq!(
        answer_of(late_answer).await,
        timed_out(SHORT_MS, RetryPlacement::Isolated)
    );
    assert_eq!(answer_of(later_answer).await, reported(3));
    ended(first_child.as_pid_t()).await;
    assert_eq!(tickets(&dir), 2);
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_the_kill_waits_for_the_last_deadline_in_the_child`:
/// the kill comes when the last run's deadline is reported; the run that
/// closed the child retries isolated, the other in its class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_kill_waits_for_the_last_deadline_in_the_thread_child() {
    let dir = TestDir::new();
    let first = thread_task(1);
    let second = thread_task(2);
    let actions = vec![
        (
            &first,
            send(&[ChildFrame::DeadlineFired(run_ref(&first, 1))]),
        ),
        (
            &second,
            send_later("2.5", &[ChildFrame::DeadlineFired(run_ref(&second, 2))]),
        ),
    ];
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
    );
    let started = Instant::now();

    let first_answer = submit_shared(&pool, &first, short_work());
    let second_answer = submit_shared(&pool, &second, thread_work(2_500));

    assert_eq!(
        answer_of(first_answer).await,
        timed_out(SHORT_MS, RetryPlacement::Isolated)
    );
    assert_eq!(
        answer_of(second_answer).await,
        timed_out(2_500, RetryPlacement::SameClass)
    );
    assert!(started.elapsed() >= Duration::from_millis(2_500));
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_a_task_that_ends_after_its_deadline_is_the_timeout`:
/// a neighbour with time left keeps the closed child alive, so the late
/// run ends by itself at its timeout; nothing is killed, and the child
/// retires by its stop frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_run_that_ends_after_its_deadline_times_out_and_the_child_retires() {
    let dir = TestDir::new();
    let late = thread_task(1);
    let neighbour = thread_task(2);
    let next = thread_task(3);
    let late_run = run_ref(&late, 1);
    let actions = vec![
        (
            &late,
            format!(
                "{}{}",
                send(&[ChildFrame::DeadlineFired(late_run)]),
                send_later("0.5", &[shared_timed_out(late_run)])
            ),
        ),
        (
            &neighbour,
            send_later("1.5", &[shared_result(run_ref(&neighbour, 2), 2)]),
        ),
        (&next, send(&[shared_result(run_ref(&next, 3), 3)])),
    ];
    let tail = format!(
        r#"{}touch "{}"; exit 0"#,
        take(&ParentFrame::Stop),
        dir.file("stopped")
    );
    let script = format!(
        "{}{}{}",
        ticket(&dir),
        boot(&shared_thread_init(2)),
        act_on_runs_then(&actions, 2, &tail),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
    );
    let _ = serving(&board, 0).await;

    let late_answer = submit_shared(&pool, &late, short_work());
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    assert_eq!(
        answer_of(late_answer).await,
        timed_out(SHORT_MS, RetryPlacement::SameClass)
    );
    assert_eq!(answer_of(neighbour_answer).await, reported(2));
    assert_eq!(
        answer_of(submit_shared(&pool, &next, long_work())).await,
        reported(3)
    );
    until("the first child read its stop frame", STATE_WAIT, || {
        dir.path().join("stopped").exists()
    })
    .await;
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_a_refusal_has_no_deadline_in_the_child` (the
/// parent half): a refusal ends with its result and leaves the child open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_on_a_thread_seat_ends_with_its_result_and_keeps_the_child_open() {
    let dir = TestDir::new();
    let refused = thread_task(1);
    let actions = vec![(&refused, send(&[shared_result(run_ref(&refused, 1), 1)]))];
    let script = children(&dir, 1, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(1)),
    );
    let refusal = SharedWork::Refusal(StoredOptionsRefusal::TimeoutValueInvalid(
        RefusalReason::cut("y"),
    ));

    let answer = answer_of(submit_shared(&pool, &refused, refusal)).await;

    assert_eq!(answer, reported(1));
    assert!(matches!(board.phase(0), SlotPhase::Serving { .. }));
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_the_backstop_kills_a_child_frozen_by_the_gil`:
/// a child that reports nothing is killed by the backstop of the run past
/// its deadline; its neighbour, with time left, takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_backstop_kills_a_frozen_thread_child_and_its_neighbour_takes_the_crash_path() {
    let frozen = thread_task(1);
    let neighbour = thread_task(2);
    let script = format!(
        "{}{}{}exec sleep 120",
        boot(&shared_thread_init(2)),
        read_frame(),
        read_frame(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
    );

    let frozen_answer = submit_shared(&pool, &frozen, short_work());
    let neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    assert_eq!(
        answer_of(frozen_answer).await,
        timed_out(SHORT_MS, RetryPlacement::SameClass)
    );
    let _ = crash_reason(answer_of(neighbour_answer).await);
    stop_shared(pool, spawner).await;
}

/// Python `TestDeadline::test_the_backstop_leaves_a_child_that_reported_the_deadline`
/// and `TestStop::test_the_task_that_closed_the_child_retries_isolated_at_the_drain`:
/// the backstop passes a child that reported the deadline; the drain
/// deadline of the stop kills it, the run that closed it keeps its
/// isolated retry, and the neighbour takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_reported_the_deadline_survives_the_backstop_and_the_drain_kill_isolates_the_closer(
) {
    let closer = thread_task(1);
    let neighbour = thread_task(2);
    let script = format!(
        "{}{}{}{}exec sleep 120",
        boot(&shared_thread_init(2)),
        read_frame(),
        read_frame(),
        send(&[ChildFrame::DeadlineFired(run_ref(&closer, 1))]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();
    let mut closer_answer = submit_shared(&pool, &closer, short_work());
    let mut neighbour_answer = submit_shared(&pool, &neighbour, long_work());

    tokio::time::sleep_until((started + Duration::from_secs(12)).into()).await;

    assert!(!process_ended(pid.as_pid_t()));
    assert!(not_answered(&mut closer_answer));
    assert!(not_answered(&mut neighbour_answer));
    let report = pool.stop(Duration::from_millis(500)).await;
    assert_eq!(
        report,
        PoolStopReport::KilledAtDrainDeadline {
            killed: vec![closer.task_id, neighbour.task_id]
        }
    );
    assert_eq!(
        answer_of(closer_answer).await,
        timed_out(SHORT_MS, RetryPlacement::Isolated)
    );
    let _ = crash_reason(answer_of(neighbour_answer).await);
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Loss
// ---------------------------------------------------------------------------

/// Python `TestLoss::test_a_crash_ends_every_task_of_the_child` and
/// `::test_a_task_that_escapes_the_seat_runner_ends_the_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_child_that_dies_ends_every_run_on_the_crash_path() {
    for (death, exit) in [("kill -9 $$", "SIGKILL"), ("exit 1", "exit code 1")] {
        let dir = TestDir::new();
        let tasks: Vec<ClaimedTask> = (1..=5).map(thread_task).collect();
        let mut actions: Vec<(&ClaimedTask, String)> = tasks[..3]
            .iter()
            .map(|task| (task, ":".to_owned()))
            .collect();
        actions.push((&tasks[3], format!("sleep 0.2; {death}")));
        actions.push((&tasks[4], send(&[shared_result(run_ref(&tasks[4], 5), 5)])));
        let script = children(&dir, 4, &actions, 4);
        let spawner = Spawner::start().unwrap();
        let (pool, _board) = thread_pool(
            &spawner,
            1,
            shared_config(shell(&script), shared_thread_init(4)),
        );

        let receivers: Vec<_> = tasks[..4]
            .iter()
            .map(|task| submit_shared(&pool, task, long_work()))
            .collect();

        for receiver in receivers {
            let reason = crash_reason(answer_of(receiver).await);
            assert!(reason.contains(exit), "{reason}");
        }
        assert_eq!(
            answer_of(submit_shared(&pool, &tasks[4], long_work())).await,
            reported(5)
        );
        assert_eq!(tickets(&dir), 2);
        stop_shared(pool, spawner).await;
    }
}

// ---------------------------------------------------------------------------
// Capacity
// ---------------------------------------------------------------------------

/// Python `TestCapacity::test_open_children_count_every_seat`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_thread_children_count_every_seat() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        2,
        shared_config(
            shell(&idle_child(&shared_thread_init(5))),
            shared_thread_init(5),
        ),
    );
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    assert_eq!(shared_capacity(&pool.service()), 10);
    stop_shared(pool, spawner).await;
}

/// Python `TestCapacity::test_a_down_child_counts_nothing`: the thread
/// slots come after the isolated and the async slots; a down thread slot
/// counts no seat, and each other kind keeps its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_down_thread_child_counts_nothing() {
    let dir = TestDir::new();
    let init = shared_thread_init(5);
    let thread_script = format!(
        "{}case $n in 1) {};; *) {};; esac",
        ticket(&dir),
        idle_child(&init),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(1, 1, 2)));
    let isolated = IsolatedPool::start(
        isolated_config(shell(&idle_child(&isolated_init()))),
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );
    let async_pool = start_shared(
        &spawner,
        &board,
        shared_config(
            shell(&idle_child(&shared_async_init(3))),
            shared_async_init(3),
        ),
    );
    let threads = start_shared(&spawner, &board, shared_config(shell(&thread_script), init));

    until("a thread slot is down", STATE_WAIT, || {
        board.down_slots().len() == 1
    })
    .await;
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    let down = board.down_slots();
    assert!(down[0].0 >= 2, "the down slot is {}", down[0].0);
    assert!(matches!(
        down[0].1.cause,
        BootFailureCause::ResourceShortage(_)
    ));
    assert_eq!(shared_capacity(&threads.service()), 5);
    assert_eq!(shared_capacity(&async_pool.service()), 3);
    assert_eq!(isolated_capacity(&isolated.service()), 1);
    let _ = threads.stop(Duration::from_secs(5)).await;
    let _ = async_pool.stop(Duration::from_secs(5)).await;
    let _ = isolated.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Stop
// ---------------------------------------------------------------------------

/// Python `TestStop::test_stop_lets_thread_tasks_finish`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_lets_thread_runs_finish() {
    let dir = TestDir::new();
    let first = thread_task(1);
    let actions = vec![(
        &first,
        format!(
            "sleep 0.3; {}",
            send(&[shared_result(run_ref(&first, 1), 1)])
        ),
    )];
    let script = children(&dir, 2, &actions, 1);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
    );
    let _ = serving(&board, 0).await;
    let receiver = submit_shared(&pool, &first, long_work());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let report = pool.stop(Duration::from_secs(10)).await;

    assert_eq!(report, PoolStopReport::Drained);
    assert_eq!(answer_of(receiver).await, reported(1));
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStop::test_stop_kills_a_thread_child_past_the_drain`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_drain_deadline_kills_a_thread_child_and_its_runs_take_the_crash_path() {
    let dir = TestDir::new();
    let tasks: Vec<ClaimedTask> = (1..=2).map(thread_task).collect();
    let actions: Vec<(&ClaimedTask, String)> =
        tasks.iter().map(|task| (task, ":".to_owned())).collect();
    let script = children(&dir, 2, &actions, 2);
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(shell(&script), shared_thread_init(2)),
    );
    let _ = serving(&board, 0).await;
    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_shared(&pool, task, long_work()))
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let report = pool.stop(Duration::from_millis(300)).await;

    assert_eq!(
        report,
        PoolStopReport::KilledAtDrainDeadline {
            killed: tasks.iter().map(|task| task.task_id).collect()
        }
    );
    for receiver in receivers {
        let _ = crash_reason(answer_of(receiver).await);
    }
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStop::test_a_task_after_stop_is_not_dispatched`
/// (`SharedPool::stop` takes the pool, so the stop begins on the board).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_after_the_stop_began_is_not_sent_to_a_thread_child() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = thread_pool(
        &spawner,
        1,
        shared_config(
            shell(&idle_child(&shared_thread_init(1))),
            shared_thread_init(1),
        ),
    );
    let _ = serving(&board, 0).await;

    board.begin_stop();
    let refused = pool.submit(&thread_task(1), input(), long_work());

    assert!(matches!(
        refused,
        Submitted::NotDispatched(NotRunCause::PoolStopping)
    ));
    stop_shared(pool, spawner).await;
}
