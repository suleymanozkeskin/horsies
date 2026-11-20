//! The runs of an isolated slot with scripted children,
//! and the end of a task whose body returned with work left on its loop.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use tokio::sync::oneshot::error::TryRecvError;

use super::isolated::ISOLATED_LEFTOVER_BACKSTOP;
use super::scripted::{
    answer_of, answer_runs, boot, ended, idle_child, isolated_config, isolated_init, ok_envelope,
    queued, read_frame, reported, result_frame, send, serving, shell, start_isolated, started_at,
    submit_isolated, take, task, ticket, tickets, timeout_ms, until, TestDir, ANSWER_WAIT,
    SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::worker::executor::protocol::{
    encode_child_frame, BodyEnd, BodyReturnedFrame, ChildAfterFrame, ChildFrame, IsolatedWork,
    LeftWorkEnd, LeftWorkEndedFrame, LeftWorkStuckFrame, LeftoverName, LoopHoldsTasks, NoLeftWork,
    ParentFrame, ResultEnvelopeText, ResultFrame, RssReading, RunEnd, StopReason, TaskInputText,
    CORE_STOPPED_EXIT_CODE,
};
use crate::worker::executor::supervisor::Spawner;

/// Answer the runs in this order only: a run of another task ends the
/// child with exit code 3.
fn ordered_runs(answers: &[(&ClaimedTask, Vec<ChildFrame>)]) -> String {
    let steps: String = answers
        .iter()
        .map(|(task, frames)| {
            format!(
                "{}case $meta in *{}*) {};; *) exit 3;; esac; ",
                read_frame(),
                task.task_id,
                send(frames)
            )
        })
        .collect();
    format!("{steps}{}exit 3", read_frame())
}

fn left_tasks() -> LoopHoldsTasks {
    LoopHoldsTasks::new(NonZeroU64::MIN, vec![LeftoverName::cut("_write_after")]).unwrap()
}

/// The body of `task` returned `value` and left one task on its loop.
fn body_returned(task: &ClaimedTask, value: u32) -> ChildFrame {
    ChildFrame::BodyReturned(BodyReturnedFrame {
        task_id: task.task_id,
        returned: BodyEnd::Result(ok_envelope(value)),
        left: left_tasks(),
        started_at: started_at(),
    })
}

fn left_work_ended(task: &ClaimedTask, end: LeftWorkEnd) -> ChildFrame {
    ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
        task_id: task.task_id,
        end,
        rss: RssReading::Unreadable,
    })
}

fn left_work_stuck(task: &ClaimedTask) -> ChildFrame {
    ChildFrame::LeftWorkStuck(LeftWorkStuckFrame {
        task_id: task.task_id,
        still: left_tasks(),
    })
}

async fn stop_pool(pool: IsolatedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

/// The ready frame gives the first RSS reading of the slot; the RSS of a
/// result frame of the same child, which serves on, replaces it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rss_of_a_result_frame_is_the_last_reading_of_the_slot() {
    let first = task(1, ExecutionClass::Isolated);
    let rss = RssReading::Mb(crate::worker::executor::protocol::RssMb::new(42.5).unwrap());
    let result = ChildFrame::Result(ResultFrame {
        task_id: first.task_id,
        end: RunEnd::Result(ok_envelope(1)),
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
        rss,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    });
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        ordered_runs(&[(&first, vec![result])])
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let child = serving(&board, 0).await;
    let slot_reading = |rss: RssReading| {
        vec![ChildRss {
            slot: 0,
            class: ExecutionClass::Isolated,
            pid: child,
            rss,
        }]
    };
    assert_eq!(board.child_rss(), slot_reading(RssReading::Unreadable));

    assert_eq!(
        answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await,
        reported(1)
    );
    until(
        "the slot holds the RSS of the result frame",
        STATE_WAIT,
        || board.child_rss() == slot_reading(rss),
    )
    .await;
    stop_pool(pool, spawner).await;
}

/// Python `TestTaskRuns::test_queued_task_is_not_assigned_while_the_slot_is_busy`:
/// the first child dies with the first task; the second task was not sent
/// to it while it was busy, so it runs on the next child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queued_task_waits_for_the_busy_child_and_runs_on_the_next() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{}sleep 0.5; exit 9;; *) {}{};; esac",
        ticket(&dir),
        boot(&init),
        read_frame(),
        boot(&init),
        answer_runs(&[(&second, vec![result_frame(&second, 2)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);

    match answer_of(first_answer).await {
        AttemptEnd::Crashed { reason } => {
            assert!(reason.as_str().contains("exit code 9"), "{reason}");
        }
        other => panic!("expected the crash path, got {other:?}"),
    }
    assert_eq!(answer_of(second_answer).await, reported(2));
    assert_eq!(tickets(&dir), 2);
    stop_pool(pool, spawner).await;
}

/// Python `TestTaskRuns::test_tasks_run_in_submit_order_on_one_slot`. The
/// pool queues every task, as the Python pool of that test does (its
/// pending limit is 100 000); the limit is not what this test checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tasks_run_in_submit_order_on_one_slot() {
    let tasks: Vec<ClaimedTask> = (1..=20)
        .map(|number| task(number, ExecutionClass::Isolated))
        .collect();
    let answers: Vec<(&ClaimedTask, Vec<ChildFrame>)> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| (task, vec![result_frame(task, value)]))
        .collect();
    let script = format!("{}{}", boot(&isolated_init()), ordered_runs(&answers));
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        pending_limit: NonZeroUsize::new(tasks.len()).unwrap(),
        ..isolated_config(shell(&script))
    };
    let (pool, _board) = start_isolated(&spawner, 1, config);

    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_isolated(&pool, task, IsolatedDeadline::NoDeadline))
        .collect();

    for (receiver, value) in receivers.into_iter().zip(1_u32..) {
        assert_eq!(answer_of(receiver).await, reported(value));
    }
    stop_pool(pool, spawner).await;
}

/// The envelope of a value that is one JSON string of `bytes` bytes.
fn large_envelope(bytes: usize) -> ResultEnvelopeText {
    ResultEnvelopeText::new(format!(
        r#"{{"__h_task_result__":true,"ok":"{}","err":null}}"#,
        "é".repeat(bytes / 2)
    ))
}

/// Write the result frame of `task` with `envelope` to `path`, for a child
/// to send with `cat`: its octal text would not fit in the arguments.
fn write_result_file(path: &std::path::Path, task: &ClaimedTask, envelope: &ResultEnvelopeText) {
    let frame = ChildFrame::Result(ResultFrame {
        task_id: task.task_id,
        end: RunEnd::Result(envelope.clone()),
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    });
    std::fs::write(path, encode_child_frame(&frame).unwrap().to_bytes()).unwrap();
}

/// Python `TestTaskRuns::test_large_body_round_trip`: an input and a result
/// of 8 MiB each cross whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_input_and_a_large_result_cross_whole() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let envelope = large_envelope(8 << 20);
    let frame_file = dir.path().join("result.frame");
    write_result_file(&frame_file, &first, &envelope);
    let script = format!(
        r#"{}{}cat "{}" >&"$0"; {}exit 3"#,
        boot(&isolated_init()),
        read_frame(),
        frame_file.display(),
        read_frame(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let large_input = TaskInputText::new(format!("\"{}\"", "a".repeat(8 << 20)));

    let receiver = queued(pool.submit(
        &first,
        large_input,
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    ));

    let answer = answer_of(receiver).await;
    let expected = AttemptEnd::Result {
        envelope,
        start: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
    };
    assert!(answer == expected, "the large result changed on its way");
    stop_pool(pool, spawner).await;
}

/// Python `TestTaskRuns::test_parent_loop_stays_free_during_a_large_result`:
/// on a runtime of one thread, a 64 MiB result does not hold the thread.
#[tokio::test]
async fn the_runtime_stays_free_during_a_large_result() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let envelope = large_envelope(64 << 20);
    let frame_file = dir.path().join("result.frame");
    write_result_file(&frame_file, &first, &envelope);
    let script = format!(
        r#"{}{}cat "{}" >&"$0"; {}exit 3"#,
        boot(&isolated_init()),
        read_frame(),
        frame_file.display(),
        read_frame(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let mut receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let started = Instant::now();
    let mut worst = Duration::ZERO;
    let mut last = Instant::now();

    let answer = loop {
        match receiver.try_recv() {
            Ok(answer) => break answer,
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Closed) => panic!("the answer channel closed with no answer"),
        }
        assert!(
            started.elapsed() < ANSWER_WAIT,
            "no answer within the test wait"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
        let now = Instant::now();
        worst = worst.max(now - last);
        last = now;
    };

    let expected = AttemptEnd::Result {
        envelope,
        start: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
    };
    assert!(answer == expected);
    assert!(
        worst < Duration::from_secs(2),
        "the runtime was held {worst:?}"
    );
    stop_pool(pool, spawner).await;
}

/// Python `TestTaskRuns::test_a_task_that_leaves_no_loop_task_keeps_its_child`
/// and `TestLeftWork::test_a_def_task_keeps_its_child_and_reports_an_empty_loop`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_with_no_left_work_keeps_the_child() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let empty_loop = ChildFrame::Result(ResultFrame {
        task_id: second.task_id,
        end: RunEnd::Result(ok_envelope(2)),
        started_at: crate::events::AttemptStart::Reported {
            started_at: started_at(),
        },
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::LoopHoldsNoTask,
        child: ChildAfterFrame::ServesOn,
    });
    let script = format!(
        "{}{}{}",
        ticket(&dir),
        boot(&isolated_init()),
        ordered_runs(&[
            (&first, vec![result_frame(&first, 1)]),
            (&second, vec![empty_loop]),
        ]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);

    assert_eq!(answer_of(first_answer).await, reported(1));
    assert_eq!(answer_of(second_answer).await, reported(2));
    assert_eq!(tickets(&dir), 1);
    stop_pool(pool, spawner).await;
}

/// Python `TestLeftWork::test_a_run_that_stopped_before_running_writes_no_marker`
/// (the parent half; the marker is the child's): a run that stopped before
/// RUNNING is answered as such.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_stopped_before_running_is_answered_as_such() {
    let first = task(1, ExecutionClass::Isolated);
    let stopped = ChildFrame::Result(ResultFrame {
        task_id: first.task_id,
        end: RunEnd::StoppedBeforeRunning(StopReason::ClaimLost),
        started_at: crate::events::AttemptStart::NotReported,
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    });
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        answer_runs(&[(&first, vec![stopped])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(
        answer,
        AttemptEnd::StoppedBeforeRunning(StopReason::ClaimLost)
    );
    stop_pool(pool, spawner).await;
}

/// A result frame that says the child ends itself (its core stopped)
/// answers its task, and the slot sends that child no other task: the
/// second task, queued at once, runs on a new child. The first child
/// reports the release of its start, waits 0.5 s, notes its end in a file
/// and exits with the core-stopped code. A run frame sent to it in that
/// time is never read, and its task would take the crash path; a kill in
/// that time would leave no note.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_ends_itself_gets_no_next_task() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let released = ChildFrame::Result(ResultFrame {
        task_id: first.task_id,
        end: RunEnd::StoppedBeforeRunning(StopReason::OwnershipUnconfirmed),
        started_at: crate::events::AttemptStart::NotReported,
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::EndsItself,
    });
    let script = format!(
        r#"{}case $n in 1) {}{}{}sleep 0.5; echo ended >> "{}"; exit {CORE_STOPPED_EXIT_CODE};; *) {}{};; esac"#,
        ticket(&dir),
        boot(&init),
        read_frame(),
        send(&[released]),
        dir.file("first-child"),
        boot(&init),
        answer_runs(&[(&second, vec![result_frame(&second, 2)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);

    assert_eq!(
        answer_of(first_answer).await,
        AttemptEnd::StoppedBeforeRunning(StopReason::OwnershipUnconfirmed)
    );
    assert_eq!(answer_of(second_answer).await, reported(2));
    assert_eq!(dir.lines("first-child"), vec!["ended".to_owned()]);
    assert_eq!(tickets(&dir), 2);
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Work left on the loop
// ---------------------------------------------------------------------------

/// Python `TestTaskRuns::test_a_task_that_leaves_loop_tasks_retires_its_child`,
/// `TestLeftWork::test_work_that_ends_within_the_grace_ends_before_the_outcome`
/// and `::test_work_alive_after_the_grace_is_cancelled_before_the_outcome`:
/// the task ends with what its body returned only after the left work
/// ended, and the next task runs on a new child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn left_work_ends_the_task_with_what_the_body_returned_and_retires_the_child() {
    for end in [LeftWorkEnd::InGrace, LeftWorkEnd::Cancelled] {
        let dir = TestDir::new();
        let first = task(1, ExecutionClass::Isolated);
        let second = task(2, ExecutionClass::Isolated);
        let init = isolated_init();
        let script = format!(
            "{}case $n in 1) {}{}{}sleep 0.5; {}{}exit 3;; *) {}{};; esac",
            ticket(&dir),
            boot(&init),
            read_frame(),
            send(&[body_returned(&first, 1)]),
            send(&[left_work_ended(&first, end)]),
            read_frame(),
            boot(&init),
            answer_runs(&[(&second, vec![result_frame(&second, 2)])], 1),
        );
        let spawner = Spawner::start().unwrap();
        let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
        let _ = serving(&board, 0).await;
        let started = Instant::now();

        let first_answer =
            answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

        assert!(started.elapsed() >= Duration::from_millis(500), "{end:?}");
        assert_eq!(first_answer, reported(1), "{end:?}");
        let second_answer = answer_of(submit_isolated(
            &pool,
            &second,
            IsolatedDeadline::NoDeadline,
        ))
        .await;
        assert_eq!(second_answer, reported(2), "{end:?}");
        assert_eq!(tickets(&dir), 2, "{end:?}");
        stop_pool(pool, spawner).await;
    }
}

/// Python `TestLeftWork::test_work_that_swallows_every_cancel_gets_its_child_killed`:
/// stuck left work kills the child; the task ends with what its body
/// returned after the exit, and the next task runs on a new child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stuck_left_work_kills_the_child_and_keeps_what_the_body_returned() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{}{}exec sleep 60;; *) {}{};; esac",
        ticket(&dir),
        boot(&init),
        read_frame(),
        send(&[body_returned(&first, 1), left_work_stuck(&first)]),
        boot(&init),
        answer_runs(&[(&second, vec![result_frame(&second, 2)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let pid = serving(&board, 0).await;

    let first_answer =
        answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(first_answer, reported(1));
    ended(pid.as_pid_t()).await;
    let second_answer = answer_of(submit_isolated(
        &pool,
        &second,
        IsolatedDeadline::NoDeadline,
    ))
    .await;
    assert_eq!(second_answer, reported(2));
    stop_pool(pool, spawner).await;
}

/// Python `TestLeftWork::test_a_child_that_reports_nothing_after_the_return_is_killed`:
/// a child that reports neither the end nor the stuck state of the left
/// work is killed at the backstop; the task keeps what its body returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_silent_after_the_return_is_killed_at_the_backstop() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}{}exec sleep 120",
        boot(&isolated_init()),
        read_frame(),
        send(&[body_returned(&first, 1)]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let pid = serving(&board, 0).await;
    let started = Instant::now();

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert!(started.elapsed() >= ISOLATED_LEFTOVER_BACKSTOP);
    assert_eq!(answer, reported(1));
    ended(pid.as_pid_t()).await;
    stop_pool(pool, spawner).await;
}

/// Python `TestLeftWork::test_a_child_that_dies_after_the_return_keeps_what_the_body_returned`
/// and `::test_a_system_exit_of_the_left_work_ends_the_child_at_once` (the
/// parent half).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_dies_after_the_return_keeps_what_the_body_returned() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}{}exit 3",
        boot(&isolated_init()),
        read_frame(),
        send(&[body_returned(&first, 1)]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    stop_pool(pool, spawner).await;
}

/// Python `TestLeftWork::test_a_deadline_kill_after_the_return_kills_nothing`
/// and `::test_the_deadline_does_not_end_a_task_whose_body_returned`: the
/// deadline passes while the child waits for the left work; the task ends
/// with what its body returned, and the child exits by its stop frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_does_not_apply_after_the_body_returned() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        r#"{}{}{}sleep 1.5; {}{}touch "{}"; exit 0"#,
        boot(&isolated_init()),
        read_frame(),
        send(&[body_returned(&first, 1)]),
        send(&[left_work_ended(&first, LeftWorkEnd::Cancelled)]),
        take(&ParentFrame::Stop),
        dir.file("stopped"),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let _ = serving(&board, 0).await;
    let started = Instant::now();

    let answer = answer_of(submit_isolated(
        &pool,
        &first,
        IsolatedDeadline::After(timeout_ms(1_000)),
    ))
    .await;

    assert!(started.elapsed() >= Duration::from_millis(1_500));
    assert_eq!(answer, reported(1));
    until("the child read its stop frame", STATE_WAIT, || {
        dir.path().join("stopped").exists()
    })
    .await;
    stop_pool(pool, spawner).await;
}

/// A scripted child with no task exits at the stop frame, so a pool of them
/// stops at once (a check of the script steps the tests above use).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_scripted_child_exits_at_the_stop_frame() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );
    let pid = serving(&board, 0).await;
    let started = Instant::now();

    let report = pool.stop(Duration::from_secs(30)).await;

    assert_eq!(report, PoolStopReport::Drained);
    assert!(started.elapsed() < Duration::from_secs(5));
    ended(pid.as_pid_t()).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}
