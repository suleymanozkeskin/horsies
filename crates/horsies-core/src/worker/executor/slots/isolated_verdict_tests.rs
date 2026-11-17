//! Ports of Python `tests/unit/test_slot_pool.py` rows of `TestVerdicts`,
//! `TestKill`, `TestKillAndWait` and `TestRecycle`: how a task ends when
//! its isolated child dies, sends a bad frame, or passes its deadline, and
//! when a child is replaced.
//!
//! Python kills a task through its slot handle; the core has no kill from
//! outside the slot, so the kill rows run through the deadline of the
//! task, the one kill the core sends to a busy child.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::scripted::{
    act_on_runs, answer_of, answer_runs, boot, idle_child, isolated_config, isolated_init,
    printf_bytes, process_ended, read_frame, reported, result_frame, send, serving, serving_pid,
    shell, start_isolated, submit_isolated, task, ticket, tickets, timeout_ms, until, TestDir,
    SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::broker::attempt_writes::RetryPlacement;
use crate::worker::executor::protocol::{
    encode_child_frame, ChildFrame, NoLeftWork, ResultFrame, RssMb, RssReading, RunEnd,
};
use crate::worker::executor::supervisor::Spawner;

async fn stop_pool(pool: IsolatedPool, spawner: Spawner) {
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// The reason of a task that took the crash path.
fn crash_reason(end: AttemptEnd) -> String {
    match end {
        AttemptEnd::Crashed { reason } => reason.into_string(),
        other => panic!("expected the crash path, got {other:?}"),
    }
}

/// Wait until `name` in `dir` holds a pid, and give it.
async fn pid_in(dir: &TestDir, name: &str) -> libc::pid_t {
    until("the child wrote a pid", STATE_WAIT, || {
        dir.lines(name)
            .first()
            .is_some_and(|line| line.trim().parse::<libc::pid_t>().is_ok())
    })
    .await;
    dir.lines(name)[0].trim().parse().unwrap()
}

fn sigkill(pid: libc::pid_t) {
    // SAFETY: a signal to a process that this test started.
    let sent = unsafe { libc::kill(pid, libc::SIGKILL) };
    assert_eq!(sent, 0, "SIGKILL to {pid}");
}

// ---------------------------------------------------------------------------
// Verdicts
// ---------------------------------------------------------------------------

/// Run a target task with `action` on one of four slots, beside three
/// siblings that each run 1 s and return. Every sibling must end with its
/// result; the target's end is returned.
async fn end_beside_siblings(action: &str) -> AttemptEnd {
    let target = task(9, ExecutionClass::Isolated);
    let siblings: Vec<ClaimedTask> = (1..=3)
        .map(|number| task(number, ExecutionClass::Isolated))
        .collect();
    let mut actions: Vec<(&ClaimedTask, String)> = siblings
        .iter()
        .zip(1_u32..)
        .map(|(sibling, value)| {
            (
                sibling,
                format!("sleep 1; {}", send(&[result_frame(sibling, value)])),
            )
        })
        .collect();
    actions.push((&target, action.to_owned()));
    let script = format!("{}{}", boot(&isolated_init()), act_on_runs(&actions, 4));
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 4, isolated_config(shell(&script)));
    for index in 0..4 {
        let _ = serving(&board, index).await;
    }

    let receivers: Vec<_> = siblings
        .iter()
        .map(|sibling| submit_isolated(&pool, sibling, IsolatedDeadline::NoDeadline))
        .collect();
    let target_end = answer_of(submit_isolated(
        &pool,
        &target,
        IsolatedDeadline::NoDeadline,
    ))
    .await;

    for (receiver, value) in receivers.into_iter().zip(1_u32..) {
        assert_eq!(answer_of(receiver).await, reported(value));
    }
    stop_pool(pool, spawner).await;
    target_end
}

/// Python `TestVerdicts::test_clean_exit_with_no_result_is_lost` and
/// `::test_exception_in_the_task_runner_is_lost`: a child that exits with
/// its task and no result takes the crash path with its exit code; its
/// siblings are not touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_that_exits_with_its_task_takes_the_crash_path_beside_healthy_siblings() {
    for code in [0, 1] {
        let reason = crash_reason(end_beside_siblings(&format!("exit {code}")).await);
        assert!(reason.contains("child died"), "{reason}");
        assert!(reason.contains(&format!("exit code {code}")), "{reason}");
    }
}

/// Python `TestVerdicts::test_sigkill_is_lost_with_the_signal`, and on
/// Linux `::test_real_segfault_is_lost_with_the_signal` and
/// `::test_abort_is_lost_with_the_signal` (a signal that would open the
/// crash dialog of macOS is sent on Linux only, as in Python).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_ended_by_a_signal_takes_the_crash_path_with_the_signal() {
    let mut signals = vec![("KILL", "SIGKILL")];
    if cfg!(target_os = "linux") {
        signals.push(("SEGV", "SIGSEGV"));
        signals.push(("ABRT", "SIGABRT"));
    }
    for (signal, name) in signals {
        let reason = crash_reason(end_beside_siblings(&format!("kill -{signal} $$")).await);
        assert!(reason.contains(name), "{reason}");
    }
}

/// Python `TestVerdicts::test_death_mid_frame_reports_a_partial_frame` and
/// `::test_half_a_frame_still_unread_is_reported_partial` (the slot half).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_death_in_the_middle_of_a_frame_is_reported_partial() {
    let target = task(9, ExecutionClass::Isolated);
    let bytes = encode_child_frame(&result_frame(&target, 1))
        .unwrap()
        .to_bytes();
    let action = format!(
        r#"printf '{}' >&"$0"; exit 9"#,
        printf_bytes(&bytes[..bytes.len() / 2])
    );

    let reason = crash_reason(end_beside_siblings(&action).await);

    assert!(reason.contains("partial result discarded"), "{reason}");
}

/// Python `TestVerdicts::test_garbage_kills_only_that_child`,
/// `::test_malformed_metadata_kills_only_that_child` and
/// `::test_result_for_another_task_kills_only_that_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_frame_kills_only_its_child() {
    let garbage = printf_bytes(&[0xff; 12]);
    let mut not_json = vec![0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0];
    not_json.extend_from_slice(b"not json!!");
    let other_task = task(77, ExecutionClass::Isolated);
    let another_result = encode_child_frame(&result_frame(&other_task, 1))
        .unwrap()
        .to_bytes();
    for bytes in [
        garbage,
        printf_bytes(&not_json),
        printf_bytes(&another_result),
    ] {
        let action = format!(r#"printf '{bytes}' >&"$0"; exec sleep 60"#);
        let reason = crash_reason(end_beside_siblings(&action).await);
        assert!(reason.contains("bad frame"), "{reason}");
    }
}

/// Python `TestVerdicts::test_complete_result_wins_over_the_death_after_it`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_complete_result_wins_over_the_death_after_it() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}{}exit 0",
        boot(&isolated_init()),
        read_frame(),
        send(&[result_frame(&first, 1)]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    stop_pool(pool, spawner).await;
}

/// Python `TestVerdicts::test_forked_helper_does_not_hide_a_death`: a
/// helper process holds the socket open, so no end of file comes; the
/// death of the child is still seen at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_helper_that_holds_the_socket_does_not_hide_a_death() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        r#"{}{}sleep 20 & echo $$ > "{}"; exec sleep 60"#,
        boot(&isolated_init()),
        read_frame(),
        dir.file("child.pid"),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let child = pid_in(&dir, "child.pid").await;
    let killed_at = Instant::now();

    sigkill(child);
    let reason = crash_reason(answer_of(receiver).await);

    assert!(killed_at.elapsed() < Duration::from_secs(2));
    assert!(reason.contains("SIGKILL"), "{reason}");
    stop_pool(pool, spawner).await;
}

/// Python `TestVerdicts::test_forked_helper_does_not_lose_a_finished_result`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_helper_that_holds_the_socket_does_not_lose_a_finished_result() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}sleep 10 & {}exit 0",
        boot(&isolated_init()),
        read_frame(),
        send(&[result_frame(&first, 1)]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    stop_pool(pool, spawner).await;
}

/// Python `TestVerdicts::test_task_dispatched_to_a_dying_child_is_reported_lost`:
/// the child exits after its result; the next task runs on a new child, or
/// reached the dying child and takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_for_a_child_that_exits_after_its_result_runs_or_takes_the_crash_path() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{}{}exit 0;; *) {}{};; esac",
        ticket(&dir),
        boot(&init),
        read_frame(),
        send(&[result_frame(&first, 1)]),
        boot(&init),
        answer_runs(&[(&second, vec![result_frame(&second, 2)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);

    assert_eq!(answer_of(first_answer).await, reported(1));
    match answer_of(second_answer).await {
        AttemptEnd::Crashed { .. } => {}
        other => assert_eq!(other, reported(2)),
    }
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Kills
// ---------------------------------------------------------------------------

/// Python `TestKill::test_kill_stops_code_that_holds_the_gil`: the deadline
/// kill ends a child that never yields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_kills_a_child_that_spins() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        "{}{}while :; do :; done",
        boot(&isolated_init()),
        read_frame()
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(
        &pool,
        &first,
        IsolatedDeadline::After(timeout_ms(1_000)),
    ))
    .await;

    assert_eq!(
        answer,
        AttemptEnd::TimedOut {
            timeout: timeout_ms(1_000),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    stop_pool(pool, spawner).await;
}

/// Python `TestKill::test_kill_after_the_result_touches_nothing` and
/// `TestKillAndWait::test_a_finished_task_reports_the_slot_answer`: the
/// deadline of a task that ended cannot reach the next task on the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deadline_ends_with_its_task() {
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        act_on_runs(
            &[
                (&first, send(&[result_frame(&first, 1)])),
                (
                    &second,
                    format!("sleep 1.5; {}", send(&[result_frame(&second, 2)]))
                ),
            ],
            2,
        ),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::After(timeout_ms(1_000)));
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::NoDeadline);

    assert_eq!(answer_of(first_answer).await, reported(1));
    assert_eq!(answer_of(second_answer).await, reported(2));
    stop_pool(pool, spawner).await;
}

/// Python `TestKill::test_kill_before_assignment_touches_nothing` and
/// `TestKillAndWait::test_a_task_no_child_took_has_nothing_to_kill`: the
/// deadline starts when the task is sent, not while it waits for its slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_starts_when_the_task_is_sent() {
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let script = format!(
        "{}{}",
        boot(&isolated_init()),
        act_on_runs(
            &[
                (
                    &first,
                    format!("sleep 1.5; {}", send(&[result_frame(&first, 1)]))
                ),
                (&second, send(&[result_frame(&second, 2)])),
            ],
            2,
        ),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let first_answer = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let second_answer = submit_isolated(&pool, &second, IsolatedDeadline::After(timeout_ms(1_000)));

    assert_eq!(answer_of(first_answer).await, reported(1));
    assert_eq!(answer_of(second_answer).await, reported(2));
    stop_pool(pool, spawner).await;
}

/// Python `TestKill::test_crash_cleans_up_task_subprocesses`: a child that
/// dies with its task takes its subprocesses with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_ends_the_subprocesses_of_the_child() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        r#"{}{}sleep 60 & echo $! > "{}"; echo $$ > "{}"; exec sleep 60"#,
        boot(&isolated_init()),
        read_frame(),
        dir.file("sub.pid"),
        dir.file("child.pid"),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let receiver = submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline);
    let subprocess = pid_in(&dir, "sub.pid").await;
    let child = pid_in(&dir, "child.pid").await;
    assert!(!process_ended(subprocess));

    sigkill(child);

    let _ = crash_reason(answer_of(receiver).await);
    until("the subprocess ended", STATE_WAIT, || {
        process_ended(subprocess)
    })
    .await;
    stop_pool(pool, spawner).await;
}

/// Python `TestKill::test_kill_works_on_a_stopped_child`: a child stopped
/// with half a frame written is killed at the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_kills_a_stopped_child() {
    let first = task(1, ExecutionClass::Isolated);
    let bytes = encode_child_frame(&result_frame(&first, 1))
        .unwrap()
        .to_bytes();
    let script = format!(
        r#"{}{}printf '{}' >&"$0"; kill -STOP $$"#,
        boot(&isolated_init()),
        read_frame(),
        printf_bytes(&bytes[..bytes.len() / 2]),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let answer = answer_of(submit_isolated(
        &pool,
        &first,
        IsolatedDeadline::After(timeout_ms(1_000)),
    ))
    .await;

    assert_eq!(
        answer,
        AttemptEnd::TimedOut {
            timeout: timeout_ms(1_000),
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
    stop_pool(pool, spawner).await;
}

/// Python `TestKill::test_a_lost_channel_needs_a_bound_like_any_hang`: a
/// child that closes its socket and lives on is killed after the grace;
/// its task takes the crash path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_closes_its_socket_takes_the_crash_path() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!(
        r#"{}{}eval "exec $0>&-"; exec sleep 60"#,
        boot(&isolated_init()),
        read_frame(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));

    let reason =
        crash_reason(answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await);

    assert!(reason.contains("channel to the child was lost"), "{reason}");
    stop_pool(pool, spawner).await;
}

/// Python `TestKillAndWait::test_the_exit_of_the_killed_child_is_seen`: the
/// end of a task past its deadline comes only after the exit of its killed
/// child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_end_comes_after_the_exit_of_the_killed_child() {
    let first = task(1, ExecutionClass::Isolated);
    let script = format!("{}{}exec sleep 60", boot(&isolated_init()), read_frame());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let pid = serving(&board, 0).await;

    let answer = answer_of(submit_isolated(
        &pool,
        &first,
        IsolatedDeadline::After(timeout_ms(1_000)),
    ))
    .await;

    assert!(matches!(answer, AttemptEnd::TimedOut { .. }), "{answer:?}");
    assert!(process_ended(pid.as_pid_t()));
    stop_pool(pool, spawner).await;
}

// ---------------------------------------------------------------------------
// Recycle
// ---------------------------------------------------------------------------

/// Children that log `<boot number> <task id>` for each run, and answer it.
fn logging_children(dir: &TestDir, tasks: &[ClaimedTask], runs: usize, rss: RssReading) -> String {
    let log = dir.file("runs");
    let actions: Vec<(&ClaimedTask, String)> = tasks
        .iter()
        .zip(1_u32..)
        .map(|(task, value)| {
            let result = ChildFrame::Result(ResultFrame {
                task_id: task.task_id,
                end: RunEnd::Result(super::scripted::ok_envelope(value)),
                rss,
                task_loop: NoLeftWork::NoTaskLoop,
            });
            (
                task,
                format!(
                    r#"echo "$n {}" >> "{log}"; {}"#,
                    task.task_id,
                    send(&[result])
                ),
            )
        })
        .collect();
    format!(
        "{}{}{}",
        ticket(dir),
        boot(&isolated_init()),
        act_on_runs(&actions, runs)
    )
}

/// Run `tasks` in order on one slot, and give the boot number of the child
/// that ran each.
async fn child_of_each_task(
    tasks: &[ClaimedTask],
    tasks_per_child: TaskLimit,
    memory_per_child: MemoryLimit,
    rss: RssReading,
) -> Vec<String> {
    let dir = TestDir::new();
    let runs = match tasks_per_child {
        TaskLimit::After(limit) => usize::try_from(limit.get()).unwrap(),
        TaskLimit::Off => tasks.len(),
    };
    let script = logging_children(&dir, tasks, runs, rss);
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child,
        memory_per_child,
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
    let by_task: HashMap<Uuid, String> = dir
        .lines("runs")
        .iter()
        .map(|line| {
            let (child, task_id) = line.split_once(' ').unwrap();
            (Uuid::parse_str(task_id).unwrap(), child.to_owned())
        })
        .collect();
    tasks
        .iter()
        .map(|task| by_task[&task.task_id].clone())
        .collect()
}

fn tasks(count: u128) -> Vec<ClaimedTask> {
    (1..=count)
        .map(|number| task(number, ExecutionClass::Isolated))
        .collect()
}

/// Python `TestRecycle::test_task_count_replaces_the_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_count_limit_replaces_the_child() {
    let children = child_of_each_task(
        &tasks(5),
        TaskLimit::After(NonZeroU64::new(2).unwrap()),
        MemoryLimit::Off,
        RssReading::Unreadable,
    )
    .await;

    assert_eq!(children[0], children[1]);
    assert_eq!(children[2], children[3]);
    assert_ne!(children[1], children[2]);
    assert_ne!(children[3], children[4]);
}

/// Python `TestRecycle::test_a_limit_of_one_runs_every_task_on_a_new_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_limit_of_one_runs_every_task_on_a_new_child() {
    let children = child_of_each_task(
        &tasks(3),
        TaskLimit::After(NonZeroU64::MIN),
        MemoryLimit::Off,
        RssReading::Unreadable,
    )
    .await;

    assert_ne!(children[0], children[1]);
    assert_ne!(children[1], children[2]);
    assert_ne!(children[0], children[2]);
}

/// Python `TestRecycle::test_memory_limit_replaces_the_child`: a reported
/// RSS at the limit retires the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_memory_limit_replaces_the_child() {
    let children = child_of_each_task(
        &tasks(2),
        TaskLimit::Off,
        MemoryLimit::AtMb(NonZeroU64::MIN),
        RssReading::Mb(RssMb::new(5.0).unwrap()),
    )
    .await;

    assert_ne!(children[0], children[1]);
}

/// Python `TestRecycle::test_zero_limit_keeps_the_child`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_limit_keeps_the_child() {
    let children = child_of_each_task(
        &tasks(5),
        TaskLimit::Off,
        MemoryLimit::Off,
        RssReading::Unreadable,
    )
    .await;

    assert!(
        children.iter().all(|child| child == &children[0]),
        "{children:?}"
    );
}

/// Python `TestRecycle::test_recycle_does_not_kill_a_background_process_of_a_task`:
/// a retired child exits by its stop frame, so a process that its task
/// started on purpose lives on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retire_spares_a_background_process_of_a_task() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let second = task(2, ExecutionClass::Isolated);
    let script = format!(
        "{}{}{}",
        ticket(&dir),
        boot(&isolated_init()),
        act_on_runs(
            &[
                (
                    &first,
                    format!(
                        r#"sleep 30 & echo $! > "{}"; {}"#,
                        dir.file("background.pid"),
                        send(&[result_frame(&first, 1)])
                    )
                ),
                (&second, send(&[result_frame(&second, 2)])),
            ],
            1,
        ),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, _board) = start_isolated(&spawner, 1, config);

    let first_answer =
        answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    let second_answer = answer_of(submit_isolated(
        &pool,
        &second,
        IsolatedDeadline::NoDeadline,
    ))
    .await;

    assert_eq!(first_answer, reported(1));
    assert_eq!(second_answer, reported(2));
    assert_eq!(tickets(&dir), 2);
    let background = pid_in(&dir, "background.pid").await;
    assert!(!process_ended(background));
    sigkill(background);
    stop_pool(pool, spawner).await;
}

/// Python `TestRecycle::test_lost_idle_child_is_replaced`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_idle_child_is_replaced() {
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {};; *) {}{};; esac",
        ticket(&dir),
        idle_child(&init),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = start_isolated(&spawner, 1, isolated_config(shell(&script)));
    let lost = serving(&board, 0).await;

    sigkill(lost.as_pid_t());
    until("a new child serves", STATE_WAIT, || {
        serving_pid(&board, 0).is_some_and(|pid| pid != lost)
    })
    .await;
    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;

    assert_eq!(answer, reported(1));
    assert_eq!(tickets(&dir), 2);
    stop_pool(pool, spawner).await;
}
