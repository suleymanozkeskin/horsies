//! Ports of Python `tests/unit/test_slot_pool.py` rows of `TestBoot` and
//! `TestDownSlots`: the boot of isolated slots with scripted children, the
//! restart rules in a running pool, down slots, and the stop of a booting
//! or down slot.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::{Duration, Instant};

use super::booting::{
    attempt_failed_line, back_in_service_line, init_for_slot, outside_kill_line, slot_down_line,
    DownRecord,
};
use super::scripted::{
    answer_of, answer_runs, boot, boot_failed, ended, fail_boot, idle_child, input,
    isolated_config, isolated_init, reported, result_frame, serving, shared_async_init,
    shared_thread_init, shell, short_of_descriptors, start_isolated, submit_isolated, take_init,
    task, ticket, tickets, until, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::worker::children::CHILD_PROCESS_NAME;
use crate::worker::executor::protocol::{
    InitFrame, IsolatedWork, ReportedBootCause, ShortageErrno,
};
use crate::worker::executor::supervisor::{
    BootFailure, BootFailureCause, ChildCommand, ChildExit, LossCause, ReapedExit, Spawner,
    CHILD_BOOT_ATTEMPTS,
};

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

/// The last failed boot of a pool whose boot attempts ran out.
fn exhausted(failure: PoolFailure) -> BootFailure {
    match failure {
        PoolFailure::BootAttemptsExhausted { attempts, last } => {
            assert_eq!(attempts, CHILD_BOOT_ATTEMPTS);
            last
        }
        other => panic!("expected the boot attempts to run out, got {other:?}"),
    }
}

const EMFILE_DETAIL: &str = "OSError: [Errno 24] Too many open files";

async fn stop_pool(pool: IsolatedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

/// Python `TestBoot::test_start_reports_one_baseline_per_child` (the slot
/// half; the baseline report of the start is in `start_tests.rs`): each slot
/// boots a child of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_slot_boots_its_own_child() {
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(
        &spawner,
        2,
        isolated_config(shell(&idle_child(&isolated_init()))),
    );

    let first = serving(&board, 0).await;
    let second = serving(&board, 1).await;

    assert_ne!(first, second);
    assert_eq!(isolated_capacity(&pool.service()), 2);
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_hook_failure_stops_at_once`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_hook_that_fails_at_boot_fails_the_pool_at_once() {
    let spawner = Spawner::start().unwrap();
    let started = Instant::now();
    let (pool, board) = start_isolated(
        &spawner,
        1,
        isolated_config(shell(&format!("{}exit 173", take_init()))),
    );

    assert_eq!(failure_of(&board).await, PoolFailure::ChildHookFailed);
    assert!(started.elapsed() < Duration::from_secs(5));
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_boot_failure_is_bounded_and_named`: a child that
/// dies before its ready frame counts; the third counted attempt fails the
/// pool with the cause and the exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_boot_that_fails_three_times_fails_the_pool_with_its_cause() {
    let dir = TestDir::new();
    let script = format!("{}{}exit 1", ticket(&dir), take_init());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let last = exhausted(failure_of(&board).await);

    assert_eq!(
        last.cause,
        BootFailureCause::LostBeforeReady {
            loss: LossCause::ChildDied,
            exit: ChildExit::Reaped(ReapedExit::ExitedWith(1)),
        }
    );
    assert!(
        last.detail.as_str().contains("exit code 1"),
        "{}",
        last.detail
    );
    assert_eq!(tickets(&dir), 3);
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_transient_boot_failure_is_retried`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_boot_that_fails_twice_then_boots_serves() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}case $n in 1|2) {}exit 1;; *) {}{};; esac",
        ticket(&dir),
        take_init(),
        boot(&isolated_init()),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    assert_eq!(tickets(&dir), 3);
    assert_eq!(board.health(), PoolHealth::Serving);
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_a_task_loop_failure_on_the_only_slot_uses_every_attempt`
/// and `TestBoot::test_no_task_reaches_a_child_that_has_no_task_loop`: a
/// cause that can clear counts when no other slot can serve; the pool
/// fails with the reported cause, and a waiting task never reaches a
/// child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shortage_on_the_only_slot_counts_every_attempt_and_fails_waiting_tasks() {
    let script = fail_boot(&isolated_init(), &short_of_descriptors());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let waiting = submit_isolated(
        &pool,
        &task(1, ExecutionClass::Isolated),
        IsolatedDeadline::NoDeadline,
    );

    let last = exhausted(failure_of(&board).await);

    assert_eq!(
        last,
        BootFailure::new(
            BootFailureCause::ResourceShortage(ShortageErrno::Emfile),
            EMFILE_DETAIL,
        )
    );
    match board.phase(0) {
        SlotPhase::Down(down) => assert_eq!(down.failed_boots.get(), CHILD_BOOT_ATTEMPTS),
        other => panic!("the last counted attempt leaves the slot down, got {other:?}"),
    }
    assert_eq!(
        answer_of(waiting).await,
        AttemptEnd::NotDispatched(NotRunCause::PoolFailed)
    );
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_a_transient_task_loop_failure_at_boot_is_retried`:
/// the slot is down after the first boot, and its next child serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shortage_that_clears_boots_the_slot_and_it_serves() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {};; *) {}{};; esac",
        ticket(&dir),
        fail_boot(&init, &short_of_descriptors()),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    until("the slot is down", STATE_WAIT, || {
        matches!(board.phase(0), SlotPhase::Down(_))
    })
    .await;
    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    assert!(matches!(board.phase(0), SlotPhase::Serving { .. }));
    assert_eq!(tickets(&dir), 2);
    assert_eq!(board.health(), PoolHealth::Serving);
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_a_replacement_on_the_only_slot_that_cannot_create_its_loop_fails_the_pool`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_short_of_resources_on_the_only_slot_fails_the_pool() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 1, config);

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    let waiting = pool.submit(
        &second,
        input(),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    );
    let last = exhausted(failure_of(&board).await);

    assert_eq!(answer, reported(1));
    assert_eq!(
        last.cause,
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
    );
    match waiting {
        Submitted::Queued(receiver) => assert_eq!(
            answer_of(receiver).await,
            AttemptEnd::NotDispatched(NotRunCause::PoolFailed)
        ),
        Submitted::NotDispatched(cause) => assert_eq!(cause, NotRunCause::PoolFailed),
    }
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_a_spawn_of_a_missing_interpreter_counts`: a spawn
/// failure repeats, so it counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_program_that_does_not_exist_fails_the_pool_after_three_attempts() {
    let command = ChildCommand {
        program: "/nonexistent/horsies-child".into(),
        args: Vec::new(),
    };
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(command));

    let last = exhausted(failure_of(&board).await);

    assert_eq!(last.cause, BootFailureCause::SpawnFailed);
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_a_missing_interpreter_stops_the_worker_while_another_slot_serves`:
/// the program goes away; slot 1 replaces its killed child, every spawn
/// fails the same way, and the failed spawns count although slot 0 serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spawn_failure_counts_while_another_slot_serves() {
    let dir = TestDir::new();
    let program = dir.path().join("sh");
    std::os::unix::fs::symlink("/bin/sh", &program).unwrap();
    let command = ChildCommand {
        program: program.clone(),
        args: vec!["-c".into(), idle_child(&isolated_init()).into()],
    };
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(command));
    let _ = serving(&board, 0).await;
    let replaced = serving(&board, 1).await;

    std::fs::remove_file(&program).unwrap();
    // SAFETY: a signal to a child of this test that serves, so it is not
    // reaped yet.
    let sent = unsafe { libc::kill(replaced.as_pid_t(), libc::SIGKILL) };
    assert_eq!(sent, 0);
    let last = exhausted(failure_of(&board).await);

    assert_eq!(last.cause, BootFailureCause::SpawnFailed);
    stop_pool(pool, spawner).await;
}

/// Python `TestBoot::test_a_loop_policy_that_raises_stops_the_worker_while_another_slot_serves`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_repeating_boot_cause_counts_while_another_slot_serves() {
    let dir = TestDir::new();
    let init = isolated_init();
    let no_loop = boot_failed(
        ReportedBootCause::TaskLoopCreateFailed,
        "NotImplementedError",
        "this policy cannot build a loop here",
    );
    let script = format!(
        "{}case $n in 1) {};; *) {};; esac",
        ticket(&dir),
        idle_child(&init),
        fail_boot(&init, &no_loop),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));

    let last = exhausted(failure_of(&board).await);

    assert_eq!(
        last,
        BootFailure::new(
            BootFailureCause::TaskLoopCreateFailed,
            "NotImplementedError: this policy cannot build a loop here",
        )
    );
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Down slots
// ---------------------------------------------------------------------------

/// Python `TestDownSlots::test_the_first_start_is_degraded_when_one_slot_boots`
/// (the slot half; the degraded start of the first generation is in
/// `start_tests.rs`): one slot is down with its cause, the other serves, the
/// down slot leaves the budget, and the pool does not fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_slot_short_of_resources_is_down_while_the_other_serves() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));

    until("one slot is down and the other serves", STATE_WAIT, || {
        let phases = board.phases_of(ExecutionClass::Isolated);
        phases
            .iter()
            .any(|phase| matches!(phase, SlotPhase::Down(_)))
            && phases
                .iter()
                .any(|phase| matches!(phase, SlotPhase::Serving { .. }))
    })
    .await;

    let down = board.down_slots();
    assert_eq!(down.len(), 1);
    assert_eq!(
        down[0].1.cause,
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
    );
    assert_eq!(down[0].1.detail, EMFILE_DETAIL);
    assert_eq!(isolated_capacity(&pool.service()), 1);
    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    assert_eq!(answer, reported(1));
    assert_eq!(board.health(), PoolHealth::Serving);
    stop_pool(pool, spawner).await;
}

/// Python `TestDownSlots::test_the_first_start_stops_when_no_slot_boots`:
/// every slot is down, so the failed boots count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_whose_every_slot_is_short_at_the_start_fails() {
    let script = fail_boot(&isolated_init(), &short_of_descriptors());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));

    let last = exhausted(failure_of(&board).await);

    assert_eq!(
        last.cause,
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
    );
    stop_pool(pool, spawner).await;
}

/// Python `TestDownSlots::test_a_down_slot_returns_to_service_and_its_boots_do_not_count`:
/// a retired child's replacement is short twice while the other slot
/// serves, then boots; the pool never fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_down_slot_returns_to_service_and_its_failed_boots_do_not_count() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 3|4) {};; *) {}{};; esac",
        ticket(&dir),
        fail_boot(&init, &short_of_descriptors()),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 2, config);
    let _ = serving(&board, 0).await;
    let _ = serving(&board, 1).await;

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    until(
        "both slots serve after two failed boots",
        STATE_WAIT,
        || {
            tickets(&dir) == 5
                && board
                    .phases_of(ExecutionClass::Isolated)
                    .iter()
                    .all(|phase| matches!(phase, SlotPhase::Serving { .. }))
        },
    )
    .await;

    assert_eq!(answer, reported(1));
    assert_eq!(board.health(), PoolHealth::Serving);
    assert!(board.down_slots().is_empty());
    stop_pool(pool, spawner).await;
}

/// Python `TestDownSlots::test_a_pool_where_every_slot_is_down_fails`: both
/// children retire and no replacement can boot; with no slot that serves,
/// the failed boots count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_whose_every_replacement_is_short_fails() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1|2) {}{};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        answer_runs(
            &[
                (&first, vec![result_frame(&first, 1)]),
                (&second, vec![result_frame(&second, 2)]),
            ],
            1,
        ),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, board) = start_isolated(&spawner, 2, config);

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);

    assert_eq!(answer_of(first_answer).await, reported(1));
    assert_eq!(answer_of(second_answer).await, reported(2));
    let last = exhausted(failure_of(&board).await);
    assert_eq!(
        last.cause,
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile)
    );
    stop_pool(pool, spawner).await;
}

/// Python `TestDownSlots::test_stop_ends_the_booting_child_of_a_down_slot`:
/// a down slot keeps that state while its next child boots; the stop kills
/// that child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_ends_the_booting_child_of_a_down_slot() {
    let dir = TestDir::new();
    let init = isolated_init();
    let script = format!(
        r#"{}case $n in 1) {};; 2) {};; *) echo $$ > "{}"; {}exec sleep 60;; esac"#,
        ticket(&dir),
        idle_child(&init),
        fail_boot(&init, &short_of_descriptors()),
        dir.file("booting.pid"),
        take_init(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));
    until("the down slot boots its next child", STATE_WAIT, || {
        !dir.lines("booting.pid").is_empty()
    })
    .await;
    let booting: libc::pid_t = dir.lines("booting.pid")[0].trim().parse().unwrap();
    assert_eq!(board.down_slots().len(), 1);
    let started = Instant::now();

    let report = pool.stop(Duration::from_secs(30)).await;

    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(report, PoolStopReport::Drained);
    ended(booting).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Python `TestDownSlots::test_stop_ends_a_down_slot_at_once`: the wait of
/// a down slot before its next boot does not hold up the stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_does_not_wait_for_a_down_slot() {
    let dir = TestDir::new();
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {};; *) {};; esac",
        ticket(&dir),
        idle_child(&init),
        fail_boot(&init, &short_of_descriptors()),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 2, isolated_config(shell(&script)));
    until("a slot is down", STATE_WAIT, || {
        board.down_slots().len() == 1
    })
    .await;
    let started = Instant::now();

    let report = pool.stop(Duration::from_secs(30)).await;

    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(report, PoolStopReport::Drained);
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

// ---------------------------------------------------------------------------
// Log lines
// ---------------------------------------------------------------------------

fn emfile_failure() -> BootFailure {
    BootFailure::new(
        BootFailureCause::ResourceShortage(ShortageErrno::Emfile),
        EMFILE_DETAIL,
    )
}

/// Python `TestBoot::test_a_task_loop_failure_on_the_only_slot_uses_every_attempt`
/// and `tests/e2e/test_task_loop_boot.py`: the line of a slot that is down
/// names the slot, the cause, the detail, the failed boots, the counted
/// attempts and the wait before the next boot.
#[test]
fn the_slot_down_line_names_the_slot_the_cause_and_the_counts() {
    let down = DownRecord {
        counted: 3,
        failed_boots: NonZeroU32::new(3).unwrap(),
        wait: Duration::from_secs(4),
    };

    assert_eq!(
        slot_down_line(0, down, &emfile_failure()),
        "Slot 0 is down: the OS ran short of a resource (EMFILE): OSError: [Errno 24] Too \
         many open files. Failed boots: 3, counted attempts: 3/3. Next boot in 4.0s"
    );
}

/// Python `_record_failed_boot`: the line of a failed attempt of a cause
/// that repeats names the count, the bound, the slot and the failure.
#[test]
fn the_attempt_failed_line_names_the_count_the_slot_and_the_failure() {
    let failure = BootFailure::new(
        BootFailureCause::AppLoadFailed,
        "ModuleNotFoundError: No module named 'app'",
    );

    assert_eq!(
        attempt_failed_line(1, 2, &failure),
        "Child boot attempt 2/3 failed on slot 1: the app or a task module could not be \
         imported: ModuleNotFoundError: No module named 'app'"
    );
}

/// Python `TestDownSlots::test_a_down_slot_returns_to_service_and_its_boots_do_not_count`
/// (`back in service after 2 failed boots`) and `_log_back_in_service`.
#[test]
fn the_back_in_service_line_names_the_failed_boots_and_the_cause() {
    let down = DownState {
        cause: BootFailureCause::ResourceShortage(ShortageErrno::Emfile),
        detail: EMFILE_DETAIL.to_owned(),
        down_since: chrono::Utc::now(),
        failed_boots: NonZeroU32::new(2).unwrap(),
    };

    assert_eq!(
        back_in_service_line(1, &down),
        "Slot 1 is back in service after 2 failed boots (the OS ran short of a resource \
         (EMFILE))"
    );
}

/// Python `_boot_child`: the line of a booting replacement that a kill from
/// outside ended while another child serves.
#[test]
fn the_outside_kill_line_names_the_slot_and_the_kills_in_a_row() {
    assert_eq!(
        outside_kill_line(2, 4),
        "Replacement child on slot 2 was killed during boot, 4 in a row"
    );
}

// ---------------------------------------------------------------------------
// Child names
// ---------------------------------------------------------------------------

/// The init frame of `template` with the process name of the worker.
fn named_worker_child(template: InitFrame) -> InitFrame {
    InitFrame {
        process_name: CHILD_PROCESS_NAME.to_owned(),
        ..template
    }
}

/// Python `_Slot.init_for` and `_SharedSlot.init_for`: a slot sends the init
/// frame of its pool with the name of the slot, so a Python child's
/// `multiprocessing.current_process().name` is `horsies-child-<slot>`, and
/// `horsies-child-shared-<slot>` or `horsies-child-thread-<slot>` for a
/// shared child. The slot is the board index. Only the name changes.
#[test]
fn each_slot_names_its_child_in_the_init_frame() {
    let isolated = named_worker_child(isolated_init());
    let shared_async = named_worker_child(shared_async_init(2));
    let shared_thread = named_worker_child(shared_thread_init(2));

    let first = init_for_slot(&isolated, 0);
    assert_eq!(first.process_name, "horsies-child-0");
    assert_eq!(
        InitFrame {
            process_name: CHILD_PROCESS_NAME.to_owned(),
            ..first
        },
        isolated
    );
    assert_eq!(init_for_slot(&isolated, 1).process_name, "horsies-child-1");
    assert_eq!(
        init_for_slot(&shared_async, 2).process_name,
        "horsies-child-shared-2"
    );
    assert_eq!(
        init_for_slot(&shared_thread, 3).process_name,
        "horsies-child-thread-3"
    );
}
