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
use crate::telemetry::declared::{DeclaredNames, ErrorCodeValues};
use crate::telemetry::registry::{ProcessFacts, RegistryLabels, SampleValue, WorkerFacts};

/// The registry of a worker with no declared name.
fn worker_registry() -> Arc<Registry> {
    let registry = Registry::for_worker(RegistryLabels {
        tasks: DeclaredNames::none(),
        queues: DeclaredNames::none(),
        handlers: DeclaredNames::none(),
        schedules: DeclaredNames::none(),
        error_codes: ErrorCodeValues::none(),
        process: ProcessFacts::Worker(WorkerFacts {
            version: "0.8.0".to_owned(),
            profile: "default".to_owned(),
            worker_id: "worker-1".to_owned(),
        }),
    })
    .expect("the registry of a worker builds");
    Arc::new(registry)
}

/// The series of `metric` in `registry`: the label texts and the value of
/// each, sorted by label texts.
fn series_of(registry: &Registry, metric: MetricId) -> Vec<(Vec<String>, SampleValue)> {
    let mut series: Vec<(Vec<String>, SampleValue)> = registry
        .snapshot()
        .into_iter()
        .find(|family| family.metric == metric)
        .expect("the registry holds the metric")
        .series
        .into_iter()
        .map(|series| (series.label_texts, series.value))
        .collect();
    series.sort_by(|left, right| left.0.cmp(&right.0));
    series
}

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
