//! The points of the slots of a worker on scripted children: a child that
//! sends its ready frame is started once; a slot that goes down after a
//! shortage is down once and recovered once when its next child is ready; a
//! slot that replaces its child tells the replacement once, with its reason.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use super::scripted::{
    act_on_runs, answer_of, answer_runs, async_work, boot, fail_boot, idle_child, isolated_config,
    isolated_init, layout, loop_blocked, read_frame, reported, result_frame, run_ref, send,
    serving, shared_async_init, shared_config, shell, short_of_descriptors, start_shared,
    submit_isolated, submit_shared, task, ticket, tickets, until, Shown, TestDir,
    SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::core::boot_failure::ShortageErrno;
use crate::events::{AnyEvent, ReplaceReason, SlotDownCause};
use crate::telemetry::catalog::MetricId;
use crate::telemetry::registry::SampleValue;
use crate::test_support::telemetry::WatchedTelemetry;
use crate::worker::executor::protocol::{LoopCulprit, CORE_STOPPED_EXIT_CODE};
use crate::worker::executor::supervisor::Spawner;
use crate::worker::runner_beats::LiveDispatches;

/// An isolated pool of one slot on a board that tells `watched`.
fn watched_pool(
    spawner: &Spawner,
    watched: &WatchedTelemetry,
    script: &str,
) -> (IsolatedPool, Arc<PoolBoard>) {
    watched_isolated(spawner, watched, isolated_config(shell(script)))
}

/// An isolated pool of one slot with `config`, on a board that tells
/// `watched`.
fn watched_isolated(
    spawner: &Spawner,
    watched: &WatchedTelemetry,
    config: IsolatedPoolConfig,
) -> (IsolatedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(1, 0, 0)).with_telemetry(watched.telemetry.clone()));
    let pool = IsolatedPool::start(
        config,
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );
    (pool, board)
}

/// The one `ChildReplaced` of `events`, between the `ChildStarted` of the
/// child it replaced and the `ChildStarted` of the next child, checked for
/// both pids, slot 0 and `kind`.
fn replaced_once(events: &[AnyEvent], kind: ExecutionClass) -> ReplaceReason {
    match events {
        [AnyEvent::ChildStarted(old), AnyEvent::ChildReplaced(replaced), AnyEvent::ChildStarted(new)] =>
        {
            assert_eq!(replaced.child_pid, old.child_pid);
            assert_ne!(new.child_pid, old.child_pid);
            assert_eq!(replaced.child_execution, kind);
            assert_eq!(replaced.slot.get(), 0);
            replaced.reason
        }
        other => panic!("ChildStarted, ChildReplaced, ChildStarted, not {other:?}"),
    }
}

/// The `horsies.worker.children.replaced` series of `watched`: the kind and
/// reason texts, and the count of each.
fn replacements(watched: &WatchedTelemetry) -> Vec<(Vec<String>, u64)> {
    counts(watched, MetricId::ChildrenReplaced)
}

fn series_key(kind: &str, reason: &str) -> Vec<String> {
    vec![kind.to_owned(), reason.to_owned()]
}

/// Stop `pool`, then check that the stop replaced no child.
async fn stop_without_replacement(
    pool: IsolatedPool,
    spawner: Spawner,
    watched: &mut WatchedTelemetry,
) {
    let _report = pool.stop(Duration::from_secs(5)).await;
    let after_stop = watched.events();
    assert!(
        !after_stop
            .iter()
            .any(|event| matches!(event, AnyEvent::ChildReplaced(_))),
        "the stop of the pool replaced a child: {after_stop:?}"
    );
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

/// The events that the points emit, read until there are `count` of them
/// (a point emits after the board changed), within [`STATE_WAIT`].
async fn events_until(watched: &mut WatchedTelemetry, count: usize) -> Vec<AnyEvent> {
    let deadline = tokio::time::Instant::now() + STATE_WAIT;
    let mut events = Vec::new();
    while events.len() < count {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} of {count} events within {STATE_WAIT:?}: {events:?}",
            events.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        events.extend(watched.events());
    }
    events
}

/// The count of each series of a histogram.
fn counts(watched: &WatchedTelemetry, metric: MetricId) -> Vec<(Vec<String>, u64)> {
    watched
        .series(metric)
        .into_iter()
        .map(|(labels, value)| match value {
            SampleValue::Histogram { buckets, .. } => (labels, buckets.iter().sum()),
            SampleValue::Counter(count) => (labels, count),
            other => panic!("no gauge here, not {other:?}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_sends_its_ready_frame_is_started_once() {
    let mut watched = WatchedTelemetry::new();
    let script = format!("{}{}exit 3", boot(&isolated_init()), read_frame());
    let spawner = Spawner::start().unwrap();
    let (pool, board) = watched_pool(&spawner, &watched, &script);

    let pid = serving(&board, 0).await;

    match events_until(&mut watched, 1).await.as_slice() {
        [AnyEvent::ChildStarted(started)] => {
            assert_eq!(started.child_execution, ExecutionClass::Isolated);
            assert_eq!(started.slot.get(), 0);
            assert_eq!(started.child_pid, pid);
        }
        other => panic!("one ChildStarted, not {other:?}"),
    }
    let isolated = vec!["ISOLATED".to_owned()];
    assert_eq!(
        counts(&watched, MetricId::ChildrenStarted),
        vec![(isolated.clone(), 1)]
    );
    assert_eq!(
        counts(&watched, MetricId::ChildBootDuration),
        vec![(isolated, 1)]
    );
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_down_after_a_shortage_is_down_once_then_started_and_recovered_once() {
    let mut watched = WatchedTelemetry::new();
    let dir = TestDir::new();
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {};; *) {}{}exit 3;; esac",
        ticket(&dir),
        fail_boot(&init, &short_of_descriptors()),
        boot(&init),
        read_frame(),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, board) = watched_pool(&spawner, &watched, &script);

    let pid = serving(&board, 0).await;

    match events_until(&mut watched, 3).await.as_slice() {
        [AnyEvent::SlotDown(down), AnyEvent::ChildStarted(started), AnyEvent::SlotRecovered(recovered)] =>
        {
            assert_eq!(
                down.cause,
                SlotDownCause::ResourceShortage {
                    errno: ShortageErrno::Emfile
                }
            );
            assert_eq!(down.slot.get(), 0);
            assert_eq!(started.child_pid, pid);
            assert_eq!(recovered.slot.get(), 0);
            assert_eq!(recovered.child_execution, ExecutionClass::Isolated);
        }
        other => panic!("SlotDown, ChildStarted, SlotRecovered, not {other:?}"),
    }
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_telemetry_off_a_boot_tells_nothing() {
    let mut watched = WatchedTelemetry::new();
    let script = format!("{}{}exit 3", boot(&isolated_init()), read_frame());
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(1, 0, 0)));
    let pool = IsolatedPool::start(
        isolated_config(shell(&script)),
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );

    let _pid = serving(&board, 0).await;

    assert!(watched.events().is_empty());
    assert!(watched.series(MetricId::ChildrenStarted).is_empty());
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_at_its_task_limit_is_replaced_once_for_its_task_count() {
    let mut watched = WatchedTelemetry::new();
    let dir = TestDir::new();
    let first = task(1, ExecutionClass::Isolated);
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}{};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        answer_runs(&[(&first, vec![result_frame(&first, 1)])], 1),
        idle_child(&init),
    );
    let spawner = Spawner::start().unwrap();
    let config = IsolatedPoolConfig {
        tasks_per_child: TaskLimit::After(NonZeroU64::MIN),
        ..isolated_config(shell(&script))
    };
    let (pool, _board) = watched_isolated(&spawner, &watched, config);

    let answer = answer_of(submit_isolated(&pool, &first, IsolatedDeadline::NoDeadline)).await;
    let events = events_until(&mut watched, 3).await;

    assert_eq!(answer, reported(1));
    assert_eq!(
        replaced_once(&events, ExecutionClass::Isolated),
        ReplaceReason::TaskCount
    );
    assert_eq!(
        replacements(&watched),
        vec![(series_key("ISOLATED", "task_count"), 1)]
    );
    stop_without_replacement(pool, spawner, &mut watched).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_dies_while_it_waits_is_replaced_once_as_died() {
    let mut watched = WatchedTelemetry::new();
    let dir = TestDir::new();
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}exit 3;; *) {};; esac",
        ticket(&dir),
        boot(&init),
        idle_child(&init),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = watched_pool(&spawner, &watched, &script);

    let events = events_until(&mut watched, 3).await;

    assert_eq!(
        replaced_once(&events, ExecutionClass::Isolated),
        ReplaceReason::ChildDied
    );
    assert_eq!(
        replacements(&watched),
        vec![(series_key("ISOLATED", "child_died"), 1)]
    );
    stop_without_replacement(pool, spawner, &mut watched).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_ends_with_the_core_stopped_code_is_replaced_once_as_core_stopped() {
    let mut watched = WatchedTelemetry::new();
    let dir = TestDir::new();
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}exit {CORE_STOPPED_EXIT_CODE};; *) {};; esac",
        ticket(&dir),
        boot(&init),
        idle_child(&init),
    );
    let spawner = Spawner::start().unwrap();
    let (pool, _board) = watched_pool(&spawner, &watched, &script);

    let events = events_until(&mut watched, 3).await;

    assert_eq!(
        replaced_once(&events, ExecutionClass::Isolated),
        ReplaceReason::CoreStopped
    );
    assert_eq!(
        replacements(&watched),
        vec![(series_key("ISOLATED", "core_stopped"), 1)]
    );
    stop_without_replacement(pool, spawner, &mut watched).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_child_killed_for_a_blocked_loop_is_replaced_once_as_loop_blocked() {
    let mut watched = WatchedTelemetry::new();
    let culprit = task(1, ExecutionClass::SharedAsync);
    let init = shared_async_init(1);
    let blocked = send(&[loop_blocked(LoopCulprit::Run(run_ref(&culprit, 1)))]);
    let script = format!("{}{}", boot(&init), act_on_runs(&[(&culprit, blocked)], 1));
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(0, 1, 0)).with_telemetry(watched.telemetry.clone()));
    let pool = start_shared(&spawner, &board, shared_config(shell(&script), init));

    let answer = answer_of(submit_shared(&pool, &culprit, async_work(60_000, 1_000))).await;
    let events = events_until(&mut watched, 3).await;

    assert!(
        matches!(answer, AttemptEnd::KilledForLoop { .. }),
        "{answer:?}"
    );
    assert_eq!(
        replaced_once(&events, ExecutionClass::SharedAsync),
        ReplaceReason::LoopBlocked
    );
    assert_eq!(
        replacements(&watched),
        vec![(series_key("SHARED_ASYNC", "loop_blocked"), 1)]
    );
    let _report = pool.stop(Duration::from_secs(5)).await;
    assert!(!watched
        .events()
        .iter()
        .any(|event| matches!(event, AnyEvent::ChildReplaced(_))));
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_telemetry_off_a_replacement_tells_nothing() {
    let mut watched = WatchedTelemetry::new();
    let dir = TestDir::new();
    let init = isolated_init();
    let script = format!(
        "{}case $n in 1) {}exit 3;; *) {};; esac",
        ticket(&dir),
        boot(&init),
        idle_child(&init),
    );
    let spawner = Spawner::start().unwrap();
    let board = Arc::new(PoolBoard::new(layout(1, 0, 0)));
    let pool = IsolatedPool::start(
        isolated_config(shell(&script)),
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );

    until(
        "a second child booted",
        STATE_WAIT,
        &[Shown::Board(&board), Shown::Tickets(&dir)],
        || tickets(&dir) >= 2,
    )
    .await;
    let _pid = serving(&board, 0).await;

    assert!(watched.events().is_empty());
    assert!(watched.series(MetricId::ChildrenReplaced).is_empty());
    let _report = pool.stop(Duration::from_secs(5)).await;
    let _stopped = spawner.stop(SPAWNER_STOP_WAIT);
}
