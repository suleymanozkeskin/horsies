//! The telemetry value of a worker: the drops of the event queue at its
//! bound and at a gone reader, the order of the queue, and no event built
//! without a handler child.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::Utc;

use super::*;
use crate::core::task::Execution;
use crate::events::{SlotIndex, SlotRecovered};
use crate::telemetry::registry::SampleValue;
use crate::test_support::telemetry::{series_of, worker_registry};

/// An event of slot `slot`.
fn slot_event(slot: u32) -> Event {
    Event::SlotRecovered(SlotRecovered {
        at: Utc::now(),
        child_kind: Execution::Isolated,
        slot: SlotIndex::try_from(slot).unwrap(),
    })
}

fn capacity(events: usize) -> NonZeroUsize {
    NonZeroUsize::new(events).unwrap()
}

#[tokio::test]
async fn an_offer_to_a_full_queue_drops_the_event_and_counts_queue_full() {
    let registry = worker_registry();
    let (on, mut queue) = TelemetryOn::with_event_queue(Arc::clone(&registry), capacity(2))
        .expect("the telemetry of a worker registry");

    let emitted: Vec<Emitted> = (0..3).map(|slot| on.emit(|| slot_event(slot))).collect();

    assert_eq!(
        emitted,
        vec![
            Emitted::Offered(Offer::Queued),
            Emitted::Offered(Offer::Queued),
            Emitted::Offered(Offer::Dropped(EventDropCause::QueueFull)),
        ]
    );
    assert_eq!(
        series_of(&registry, MetricId::EventsDropped),
        vec![(vec!["queue_full".to_owned()], SampleValue::Counter(1))]
    );
    match queue.next_batch(capacity(8)).await {
        EventBatch::Events(events) => assert_eq!(slots_of(&events), vec![0, 1]),
        EventBatch::LinkClosed => panic!("the queue holds two events"),
    }
}

/// The slot of each event of `events`, in order.
fn slots_of(events: &[Event]) -> Vec<u32> {
    events
        .iter()
        .map(|event| match event {
            Event::SlotRecovered(recovered) => recovered.slot.get(),
            other => panic!("the queue holds only slot events, not {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn an_offer_after_the_reader_is_gone_drops_the_event_and_counts_child_down() {
    let registry = worker_registry();
    let (on, queue) = TelemetryOn::with_event_queue(Arc::clone(&registry), capacity(2))
        .expect("the telemetry of a worker registry");
    drop(queue);

    let emitted = on.emit(|| slot_event(0));

    assert_eq!(
        emitted,
        Emitted::Offered(Offer::Dropped(EventDropCause::ChildDown))
    );
    assert_eq!(
        series_of(&registry, MetricId::EventsDropped),
        vec![(vec!["child_down".to_owned()], SampleValue::Counter(1))]
    );
}

#[tokio::test]
async fn the_queue_ends_after_the_link_is_gone_and_its_events_are_read() {
    let (on, mut queue) = TelemetryOn::with_event_queue(worker_registry(), capacity(4))
        .expect("the telemetry of a worker registry");
    let _first = on.emit(|| slot_event(0));
    drop(on);

    let first = queue.next_batch(capacity(4)).await;
    let second = queue.next_batch(capacity(4)).await;

    assert!(
        matches!(&first, EventBatch::Events(events) if events.len() == 1),
        "{first:?}"
    );
    assert!(matches!(second, EventBatch::LinkClosed), "{second:?}");
}

#[test]
fn with_no_handler_child_a_point_builds_no_event() {
    let registry = worker_registry();
    let on = TelemetryOn::metrics_only(Arc::clone(&registry))
        .expect("the telemetry of a worker registry");
    let built = Cell::new(false);

    let emitted = on.emit(|| {
        built.set(true);
        slot_event(0)
    });

    assert_eq!(emitted, Emitted::NotBuilt);
    assert!(!built.get());
    assert!(series_of(&registry, MetricId::EventsDropped).is_empty());
}

#[test]
fn the_reaper_counts_its_failures_only_with_telemetry_on() {
    let off = WorkerTelemetry::Off.recovery_failures();
    let on = WorkerTelemetry::On(Arc::new(
        TelemetryOn::metrics_only(worker_registry()).expect("the telemetry of a worker registry"),
    ))
    .recovery_failures();

    assert!(matches!(off, OptionalCounter::NotRecorded));
    assert!(matches!(on, OptionalCounter::Recording(_)));
}

/// A plain task of `execution` at attempt 3, on queue `default`.
fn claimed_task(execution: Execution) -> crate::worker::executor::slots::ClaimedTask {
    use crate::worker::executor::slots::{ClaimedTask, RetryFacts, TaskKind};
    ClaimedTask {
        task_id: uuid::Uuid::from_u128(7),
        task_name: "app.tasks.send_email".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc::now(),
        execution,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 2,
            max_retries: 3,
        },
        options: crate::worker::retry::read_stored_options(None),
        trace: crate::events::RunTrace::NoCarrier,
    }
}

/// A placement of a task on an isolated child, one second after `read`.
fn placed_after(read: crate::broker::claims::ClaimRead) -> crate::worker::executor::slots::Placed {
    use crate::worker::executor::protocol::ProcessId;
    crate::worker::executor::slots::Placed {
        child_pid: ProcessId::new(4242).unwrap(),
        child_kind: Execution::Isolated,
        at: read.at + std::time::Duration::from_secs(1),
        wall: read.wall + chrono::Duration::seconds(1),
    }
}

#[test]
fn a_placed_task_gives_one_attempt_placed_with_its_child_and_one_slot_wait() {
    use crate::worker::executor::slots::ChildPlacement;
    let mut watched = crate::test_support::telemetry::WatchedTelemetry::new();
    let task = claimed_task(Execution::SharedAsync);
    let read = crate::broker::claims::ClaimRead::now();
    let placed = placed_after(read);

    watched
        .telemetry
        .attempt_placed(&task, read, &ChildPlacement::Placed(placed));

    let events = watched.events();
    assert_eq!(events.len(), 1, "{events:?}");
    match &events[0] {
        Event::AttemptPlaced(event) => {
            assert_eq!(event.task_id, task.task_id);
            assert_eq!(event.execution, Execution::SharedAsync);
            assert_eq!(event.child_kind, Execution::Isolated);
            assert_eq!(event.child_pid, placed.child_pid);
            assert_eq!(event.attempt.get(), 3);
            assert_eq!(event.at, placed.wall);
            assert_eq!(event.slot_wait, std::time::Duration::from_secs(1));
        }
        other => panic!("the placement emits AttemptPlaced, not {other:?}"),
    }
    match watched.series(MetricId::TaskSlotWaitDuration).as_slice() {
        [(labels, SampleValue::Histogram { buckets, .. })] => {
            assert_eq!(labels, &vec!["SHARED_ASYNC".to_owned()]);
            assert_eq!(buckets.iter().sum::<u64>(), 1);
        }
        other => panic!("one slot wait series, not {other:?}"),
    }
}

#[test]
fn a_task_that_no_child_took_or_telemetry_off_gives_no_placement() {
    use crate::worker::executor::slots::{ChildPlacement, PlacementReceipt, PlacementWatch};
    let mut watched = crate::test_support::telemetry::WatchedTelemetry::new();
    let task = claimed_task(Execution::Isolated);
    let read = crate::broker::claims::ClaimRead::now();

    watched
        .telemetry
        .attempt_placed(&task, read, &ChildPlacement::NotPlaced);
    WorkerTelemetry::Off.attempt_placed(&task, read, &ChildPlacement::Placed(placed_after(read)));

    assert!(watched.events().is_empty());
    assert!(watched.series(MetricId::TaskSlotWaitDuration).is_empty());
    assert!(matches!(
        WorkerTelemetry::Off.placement_watch(),
        (PlacementWatch::Unwatched, PlacementReceipt::Unwatched)
    ));
}
