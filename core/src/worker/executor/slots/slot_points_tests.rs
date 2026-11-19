//! The points of the slots of a worker on scripted children: a child that
//! sends its ready frame is started once; a slot that goes down after a
//! shortage is down once and recovered once when its next child is ready.

use std::sync::Arc;
use std::time::Duration;

use super::scripted::{
    boot, fail_boot, isolated_config, isolated_init, layout, read_frame, serving, shell,
    short_of_descriptors, ticket, TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use super::*;
use crate::core::boot_failure::ShortageErrno;
use crate::events::{Event, SlotDownCause};
use crate::telemetry::catalog::MetricId;
use crate::telemetry::registry::SampleValue;
use crate::test_support::telemetry::WatchedTelemetry;
use crate::worker::executor::supervisor::Spawner;
use crate::worker::runner_beats::LiveDispatches;

/// An isolated pool of one slot on a board that tells `watched`.
fn watched_pool(
    spawner: &Spawner,
    watched: &WatchedTelemetry,
    script: &str,
) -> (IsolatedPool, Arc<PoolBoard>) {
    let board = Arc::new(PoolBoard::new(layout(1, 0, 0)).with_telemetry(watched.telemetry.clone()));
    let pool = IsolatedPool::start(
        isolated_config(shell(script)),
        Arc::clone(&board),
        spawner.handle(),
        LiveDispatches::default(),
    );
    (pool, board)
}

/// The events that the points emit, read until there are `count` of them
/// (a point emits after the board changed), within [`STATE_WAIT`].
async fn events_until(watched: &mut WatchedTelemetry, count: usize) -> Vec<Event> {
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
        [Event::ChildStarted(started)] => {
            assert_eq!(started.child_kind, ExecutionClass::Isolated);
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
        [Event::SlotDown(down), Event::ChildStarted(started), Event::SlotRecovered(recovered)] => {
            assert_eq!(
                down.cause,
                SlotDownCause::ResourceShortage {
                    errno: ShortageErrno::Emfile
                }
            );
            assert_eq!(down.slot.get(), 0);
            assert_eq!(started.child_pid, pid);
            assert_eq!(recovered.slot.get(), 0);
            assert_eq!(recovered.child_kind, ExecutionClass::Isolated);
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
