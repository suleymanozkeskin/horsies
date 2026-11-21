//! Faults of a child or of a frame, kills
//! from outside during a boot, the stop of the pool, a pool failure in a
//! running pool, and a mixed run.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use super::scripted::{
    act_on_runs, answer_of, answer_runs, boot, ended, idle_child, input, isolated_config,
    isolated_init, printf_bytes, process_ended, read_frame, reported, result_frame, send, serving,
    shell, start_isolated, submit_isolated, take_init, task, ticket, tickets, timeout_ms, until,
    TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{IsolatedWork, ProcessId};
use crate::worker::executor::supervisor::{
    BootFailure, BootFailureCause, ChildCommand, ChildExit, LossCause, ReapedExit, Spawner,
    CHILD_BOOT_ATTEMPTS,
};

async fn stop_pool(pool: IsolatedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

fn crash_reason(end: AttemptEnd) -> String {
    match end {
        AttemptEnd::Crashed { reason } => reason.into_string(),
        other => panic!("expected the crash path, got {other:?}"),
    }
}

/// Wait until the pools fail, and give the failure.
async fn failure_of(board: &PoolBoard) -> PoolFailure {
    until("the pool failed", STATE_WAIT, || {
        matches!(board.health(), PoolHealth::Failed(_))
    })
    .await;
    match board.health() {
        PoolHealth::Failed(failure) => failure,
        other => panic!("expected a failed pool, got {other:?}"),
    }
}

fn exhausted(failure: PoolFailure) -> BootFailure {
    match failure {
        PoolFailure::BootAttemptsExhausted { attempts, last } => {
            assert_eq!(attempts, CHILD_BOOT_ATTEMPTS);
            last
        }
        other => panic!("expected the boot attempts to run out, got {other:?}"),
    }
}

/// A boot that ends by a `SIGKILL` before the ready frame.
fn killed_before_ready() -> BootFailureCause {
    BootFailureCause::LostBeforeReady {
        loss: LossCause::ChildDied,
        exit: ChildExit::Reaped(ReapedExit::KilledBySignal(libc::SIGKILL)),
    }
}

fn one_per_child() -> TaskLimit {
    TaskLimit::After(NonZeroU64::MIN)
}

// ---------------------------------------------------------------------------
// Faults of a child or of a frame
// ---------------------------------------------------------------------------

/// Python `TestChildFaults::test_zero_length_header_kills_the_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_of_zero_length_kills_the_child() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        r#"{}{}printf '{}' >&"$0"; exec sleep 60"#,
        boot(&isolated_init()),
        read_frame(),
        printf_bytes(&[0; 12]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let reason =
        crash_reason(answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await);

    assert!(reason.contains("bad frame"), "{reason}");
    stop_pool(pool, spawner).await;
}

/// A task whose run frame cannot be encoded: its metadata is over the bound.
fn unsendable(number: u128) -> ClaimedTask {
    ClaimedTask {
        task_name: "x".repeat(2 << 20),
        ..task(number, ExecutionClass::Isolated)
    }
}

/// Python `TestSlotDefects::test_a_defect_costs_only_its_task`: a run frame
/// that cannot be encoded is a slot defect: it kills its child, only its
/// task takes the crash path with the defect as its cause, and the next
/// task runs on a new child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_that_cannot_be_sent_costs_only_its_task() {
    let dir = TestDir::new();
    let next = task(2, ExecutionClass::Isolated);
    let script = format!(
        "{}{}{}",
        ticket(&dir),
        boot(&isolated_init()),
        answer_runs(&[(&next, vec![result_frame(&next, 2)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let first_child = serving(&board, 0).await;

    let defect = answer_of(submit_isolated(
        &pool,
        &unsendable(1),
        IsolatedDeadline::NoDeadline,
    ))
    .await;
    let answer = answer_of(submit_isolated(&pool, &next, IsolatedDeadline::NoDeadline)).await;

    let reason = crash_reason(defect);
    assert!(reason.contains("the worker slot failed"), "{reason}");
    assert!(process_ended(first_child.as_pid_t()));
    assert_eq!(answer, reported(2));
    assert_eq!(tickets(&dir), 2);
    assert_eq!(board.health(), PoolHealth::Serving);
    stop_pool(pool, spawner).await;
}

/// Python `TestSlotDefects::test_repeated_defects_fail_the_pool` and
/// `::test_a_finished_task_clears_the_count`: two defects and a finished
/// task leave the pool serving; three defects in a row fail it, and the
/// failure names the slot and the last defect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_send_defects_in_a_row_fail_the_pool() {
    let finished = task(100, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        answer_runs(&[(&finished, vec![result_frame(&finished, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    for number in 1..=2 {
        let _ = answer_of(submit_isolated(
            &pool,
            &unsendable(number),
            IsolatedDeadline::NoDeadline,
        ))
        .await;
    }
    let answer = answer_of(submit_isolated(
        &pool,
        &finished,
        IsolatedDeadline::NoDeadline,
    ))
    .await;
    assert_eq!(answer, reported(1));
    assert_eq!(board.health(), PoolHealth::Serving);
    for number in 3..=4 {
        let _ = answer_of(submit_isolated(
            &pool,
            &unsendable(number),
            IsolatedDeadline::NoDeadline,
        ))
        .await;
    }
    assert_eq!(board.health(), PoolHealth::Serving);
    let _ = answer_of(submit_isolated(
        &pool,
        &unsendable(5),
        IsolatedDeadline::NoDeadline,
    ))
    .await;

    match failure_of(&board).await {
        PoolFailure::SlotDefectsRepeated { slot, detail } => {
            assert_eq!(slot, 0);
            assert!(detail.as_str().contains("could not be encoded"), "{detail}");
        }
        other => panic!("expected repeated slot defects, got {other:?}"),
    }
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Kills from outside during a boot
// ---------------------------------------------------------------------------

/// Python `TestBootKills::test_kills_of_a_replacement_do_not_stop_the_pool`:
/// a replacement killed by `SIGKILL` before its ready frame does not count
/// while the first generation is in and another child serves; the slot
/// waits and boots again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outside_kills_of_a_replacement_do_not_stop_the_pool() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let kills = CHILD_BOOT_ATTEMPTS + 2;
    let killed_boots: Vec<String> = (3..3 + kills).map(|n| n.to_string()).collect();
    let script = format!(
        "{}case $n in {}) {}kill -9 $$;; *) {}{};; esac",
        ticket(&dir),
        killed_boots.join("|"),
        take_init(),
        boot(&init),
        answer_runs(
            &[
                (&first, vec![result_frame(&first, 1)]),
                (&second, vec![result_frame(&second, 2)]),
            ],
            1,
        ),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: one_per_child(),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 2, config);
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    let first_answer =
        answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    let all_boots = usize::try_from(kills + 3).unwrap();
    until(
        "the slot boots after the kills",
        Duration::from_secs(60),
        || {
            tickets(&dir) == all_boots
                && board
                    .phases_of(ExecutionClass::Isolated)
                    .iter()
                    .all(|phase| matches!(phase, SlotPhase::Serving { .. }))
        },
    )
    .await;
    let second_answer = answer_of(submit_isolated(
        &pool,
        &second,
        IsolatedDeadline::NoDeadline,
    ))
    .await;

    assert_eq!(first_answer, reported(1));
    assert_eq!(second_answer, reported(2));
    assert_eq!(board.health(), PoolHealth::Serving);
    stop_pool(pool, spawner).await;
}

/// Python `TestBootKills::test_a_kill_counts_when_no_other_child_serves`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_outside_kill_counts_when_no_other_child_serves() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}case $n in 1) {}{};; *) {}kill -9 $$;; esac",
        ticket(&dir),
        boot(&isolated_init()),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
        take_init(),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: one_per_child(),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 1, config);

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    let last = exhausted(failure_of(&board).await);

    assert_eq!(answer, reported(1));
    assert_eq!(last.cause, killed_before_ready());
    stop_pool(pool, spawner).await;
}

/// Python `TestBootKills::test_a_kill_during_the_first_start_still_counts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_outside_kill_during_the_first_start_counts() {
    let script = format!("{}kill -9 $$", take_init());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let last = exhausted(failure_of(&board).await);

    assert_eq!(last.cause, killed_before_ready());
    stop_pool(pool, spawner).await;
}

/// Python `TestBootKills::test_a_replacement_that_cannot_boot_still_counts`:
/// only a `SIGKILL` is spared; another death counts while the other child
/// serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_that_cannot_boot_counts_while_another_child_serves() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}case $n in 1|2) {}{};; *) {}exit 1;; esac",
        ticket(&dir),
        boot(&isolated_init()),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
        take_init(),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: one_per_child(),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 2, config);
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    let last = exhausted(failure_of(&board).await);

    assert_eq!(answer, reported(1));
    assert_eq!(
        last.cause,
        BootFailureCause::LostBeforeReady {
            loss: LossCause::ChildDied,
            exit: ChildExit::Reaped(ReapedExit::ExitedWith(1)),
        }
    );
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// The stop during a boot
// ---------------------------------------------------------------------------

/// Python `TestStopDuringBoot::test_stop_ends_the_child_and_the_start`,
/// `::test_stop_starts_no_further_child` and
/// `::test_a_child_that_starts_during_the_stop_is_ended`: the stop ends a
/// child that boots and starts no other; the slot checks the stop in the
/// same wait as the ready frame, so a child that starts as the stop begins
/// is ended the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_ends_a_booting_child_and_starts_no_other() {
    let dir = TestDir::new();
    let script = format!(
        r#"{}echo $$ > "{}"; {}exec sleep 60"#,
        ticket(&dir),
        dir.file("booting.pid"),
        take_init(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    until("a child boots", STATE_WAIT, || {
        !dir.lines("booting.pid").is_empty()
    })
    .await;
    let booting: libc::pid_t = dir.lines("booting.pid")[0].trim().parse().unwrap();
    assert_eq!(board.phase(0), SlotPhase::Booting);
    let started = Instant::now();

    let report = pool.stop(Duration::from_secs(30)).await;

    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(report, PoolStopReport::Drained);
    ended(booting).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(tickets(&dir), 1);
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStopDuringBoot::test_a_replaced_child_does_not_count_as_another_slot`:
/// the first generation is in only when every slot booted once; a slot
/// that booted twice does not stand for another slot.
#[test]
fn a_replaced_child_does_not_count_as_another_slot_in_the_first_generation() {
    let board = PoolBoard::new(super::scripted::layout(2, 0, 0));
    let pid = ProcessId::new(10).unwrap();
    board.set(0, SlotPhase::Serving { pid });
    board.set(0, SlotPhase::Retiring);
    board.set(0, SlotPhase::Serving { pid });
    assert!(!board.pool_at_failure(1).first_generation_in);
    board.set(1, SlotPhase::Serving { pid });
    assert!(board.pool_at_failure(0).first_generation_in);
}

// ---------------------------------------------------------------------------
// The stop
// ---------------------------------------------------------------------------

/// Python `TestStop::test_queued_tasks_are_released_at_once`: at the stop a
/// task that waits for a slot is answered at once; the task in flight
/// ends with its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_releases_waiting_tasks_at_once() {
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        act_on_runs(
            &[(
                &first,
                format!("sleep 1; {}", send(&[result_frame(&first, 1)]))
            )],
            1,
        ),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let _ = serving(&board, 0).await;
    let running = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let waiting = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();

    let stopping = tokio::spawn(pool.stop(Duration::from_secs(10)));
    let waiting_answer = answer_of(waiting).await;

    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(
        waiting_answer,
        AttemptEnd::NotDispatched(NotRunCause::PoolStopping)
    );
    assert_eq!(answer_of(running).await, reported(1));
    assert_eq!(stopping.await.unwrap(), PoolStopReport::Drained);
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestStop::test_queue_past_its_limit_refuses_the_task`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_at_its_pending_limit_refuses_the_next_task() {
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let third = task(3, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        act_on_runs(
            &[
                (
                    &first,
                    format!("sleep 1; {}", send(&[result_frame(&first, 1)]))
                ),
                (&second, send(&[result_frame(&second, 2)])),
            ],
            2,
        ),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        pending_limit: NonZeroUsize::MIN,
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 1, config);
    let _ = serving(&board, 0).await;
    let running = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let waiting = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);
    let refused = pool.submit(
        &third,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    );

    assert!(matches!(
        refused,
        Submitted::NotDispatched(NotRunCause::QueueFull)
    ));
    assert_eq!(answer_of(running).await, reported(1));
    assert_eq!(answer_of(waiting).await, reported(2));
    stop_pool(pool, spawner).await;
}

/// Python `TestStop::test_submit_after_stop_is_not_dispatched`: the stop of
/// the pools refuses every later task (`IsolatedPool::stop` takes the pool,
/// so the stop begins on the board here).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_after_the_stop_began_is_not_dispatched() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );
    let _ = serving(&board, 0).await;

    board.begin_stop();
    let refused = pool.submit(
        &task(1, ExecutionClass::Isolated),
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    );

    assert!(matches!(
        refused,
        Submitted::NotDispatched(NotRunCause::PoolStopping)
    ));
    assert_eq!(pool.health(), PoolHealth::Stopping);
    stop_pool(pool, spawner).await;
}

/// Python `TestStop::test_stop_leaves_no_child_running`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_leaves_no_child_running() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        3,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );
    let mut children = Vec::new();
    for index in 0..3 {
        children.push(serving(&board, index).await);
    }

    let report = pool.stop(Duration::from_secs(5)).await;

    assert_eq!(report, PoolStopReport::Drained);
    for child in children {
        ended(child.as_pid_t()).await;
    }
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// A pool failure in a running pool
// ---------------------------------------------------------------------------

/// Python `TestPoolFailure::test_replacement_failure_is_loud_and_releases_queued_tasks`:
/// the only slot cannot spawn its replacement; the failed spawns count,
/// the pool fails, the waiting task is released, and a later task is
/// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_replacement_fails_the_pool_and_releases_waiting_tasks() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let program = dir.path().join("sh");
    std::os::unix::fs::symlink(crate::test_support::SCRIPT_SHELL, &program).unwrap();
    let script = format!(
        r#"{}{}rm "{}"; {}"#,
        boot(&isolated_init()),
        read_frame(),
        program.display(),
        send(&[result_frame(&first, 1)]),
    );
    let command = ChildCommand {
        program: program.clone(),
        args: vec![
            "-c".into(),
            format!("{script}{}exit 3", read_frame()).into(),
        ],
    };
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: one_per_child(),
        ..isolated_config(command)
    };
    let (pool, board) = start_isolated(&spawner, 1, config);

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let waiting = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);
    let last = exhausted(failure_of(&board).await);

    assert_eq!(answer_of(first_answer).await, reported(1));
    assert_eq!(last.cause, BootFailureCause::SpawnFailed);
    assert_eq!(
        answer_of(waiting).await,
        AttemptEnd::NotDispatched(NotRunCause::PoolFailed)
    );
    assert!(matches!(
        pool.submit(
            &task(3, ExecutionClass::Isolated),
            input(),
            IsolatedWork::Run,
            IsolatedDeadline::NoDeadline,
        ),
        Submitted::NotDispatched(NotRunCause::PoolFailed)
    ));
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// A mixed run
// ---------------------------------------------------------------------------

/// How a task of the mixed run ends in its child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    Result,
    Crash,
    Hang,
    Garbage,
}

/// Python `TestChaos::test_every_task_gets_its_predicted_outcome`: a mix of
/// results, crashes, hangs past the deadline and bad frames on two slots;
/// each task gets the end its fate predicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_task_gets_its_predicted_end_in_a_mixed_run() {
    let fates = [
        Fate::Result,
        Fate::Crash,
        Fate::Result,
        Fate::Hang,
        Fate::Garbage,
        Fate::Result,
        Fate::Result,
        Fate::Crash,
        Fate::Hang,
        Fate::Result,
        Fate::Garbage,
        Fate::Result,
    ];
    let tasks: Vec<ClaimedTask> = (1..=12)
        .map(|number| task(number, ExecutionClass::Isolated))
        .collect();
    let actions: Vec<(&ClaimedTask, String)> = tasks
        .iter()
        .zip(fates)
        .zip(1_u32..)
        .map(|((task, fate), value)| {
            let action = match fate {
                Fate::Result => send(&[result_frame(task, value)]),
                Fate::Crash => "exit 7".to_owned(),
                Fate::Hang => "exec sleep 60".to_owned(),
                Fate::Garbage => format!(
                    r#"printf '{}' >&"$0"; exec sleep 60"#,
                    printf_bytes(&[0xff; 12])
                ),
            };
            (task, action)
        })
        .collect();
    let script = format!("{}{}", boot(&isolated_init()), act_on_runs(&actions, 12));
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));

    let receivers: Vec<_> = tasks
        .iter()
        .map(|task| submit_isolated(&pool, task, IsolatedDeadline::After(timeout_ms(1_000))))
        .collect();

    for ((receiver, fate), value) in receivers.into_iter().zip(fates).zip(1_u32..) {
        let end = answer_of(receiver).await;
        match fate {
            Fate::Result => assert_eq!(end, reported(value)),
            Fate::Crash => assert!(crash_reason(end).contains("exit code 7")),
            Fate::Hang => assert_eq!(
                end,
                AttemptEnd::TimedOut {
                    timeout: timeout_ms(1_000),
                    retry: RetryPlacement::SameClass,
                    cancel: TimeoutCancel::NoSignal,
                    seen: crate::worker::executor::slots::TimeoutSeen::AtKill,
                }
            ),
            Fate::Garbage => assert!(crash_reason(end).contains("bad frame")),
        }
    }
    stop_pool(pool, spawner).await;
}
