//! The supervision of the handler child with scripted children: the boot,
//! a ready timeout, a failed start hook, a loss and the boot after it, a
//! full queue, the filter by type, the `handler_stats` frames, the stop
//! drain, and the snapshot form of each state.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use serde_json::json;

use super::*;
use crate::core::task::Execution;
use crate::events::{decode_event, ChildStarted, Event, EventTypeName, SlotIndex, SlotRecovered};
use crate::telemetry::declared::{DeclaredNames, ErrorCodeValues};
use crate::telemetry::registry::{
    ProcessFacts, Registry, RegistryLabels, SampleValue, WorkerFacts,
};
use crate::telemetry::worker::{DeclaredCodesByTask, Emitted, Offer, SentEventTypes, TelemetryOn};
use crate::test_support::telemetry::series_of;
use crate::worker::executor::protocol::{
    encode_parent_frame, ChildConnection, ChildFrame, ChildKind, ChildLogLevel, ChildProgram,
    DatabaseUrl, HandlerChild, HandlerCounts, HandlerCountsList, HandlerName, HandlerStatsFrame,
    ParentFrame, ProcessId, ReadyFrame, RssMb, SpanReadCounts, TaskSendPool, HEADER_BYTES,
};
use crate::worker::executor::slots::scripted::{
    boot_any_kind, read_bytes, send, shell, take_init, ticket, tickets, until, ScriptCount,
    TestDir, SPAWNER_STOP_WAIT, STATE_WAIT,
};
use crate::worker::executor::supervisor::{
    BootFailureCause, ChildCommand, LossCause, Spawner, CHILD_KILL_WAIT,
};

/// The handler names that the registry of a test declares.
const ON_END: &str = "app.hooks.on_end";

/// Bounds for tests: a short flush interval, and a stop that a child which
/// takes the stop frame ends well within.
fn bounds() -> HandlerChildBounds {
    HandlerChildBounds {
        ready: Duration::from_secs(10),
        flush_interval: Duration::from_millis(5),
        stop_flush: Duration::from_secs(2),
        stop_exit: Duration::from_secs(5),
    }
}

/// The init frame of a handler child.
fn handler_init() -> crate::worker::executor::protocol::InitFrame {
    crate::worker::executor::protocol::InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
        process_name: HANDLER_CHILD_PROCESS_NAME.to_owned(),
        child_kind: ChildKind::Handler(HandlerChild {
            task_send_pool: TaskSendPool::new(1).unwrap(),
        }),
    }
}

/// A worker registry that declares the handler [`ON_END`].
fn handler_registry() -> Arc<Registry> {
    let registry = Registry::for_worker(RegistryLabels {
        tasks: DeclaredNames::none(),
        queues: DeclaredNames::none(),
        handlers: DeclaredNames::new([ON_END]).unwrap(),
        schedules: DeclaredNames::none(),
        error_codes: ErrorCodeValues::none(),
        process: ProcessFacts::Worker(WorkerFacts {
            version: "0.8.0".to_owned(),
            profile: "default".to_owned(),
            worker_id: "worker-1".to_owned(),
        }),
    })
    .unwrap();
    Arc::new(registry)
}

/// A running handler child of a test: the points' side of its event queue,
/// its registry, its supervision and the spawner.
struct Harness {
    points: TelemetryOn,
    registry: Arc<Registry>,
    run: HandlerChildRun,
    spawner: Spawner,
}

impl Harness {
    /// The handler child of `command`, sent the event types `sent` through a
    /// queue of `capacity` events.
    fn start(
        command: ChildCommand,
        bounds: HandlerChildBounds,
        sent: SentEventTypes,
        capacity: usize,
    ) -> Self {
        let registry = handler_registry();
        let (points, queue) = TelemetryOn::with_event_queue(
            Arc::clone(&registry),
            DeclaredCodesByTask::none(),
            NonZeroUsize::new(capacity).unwrap(),
            sent,
        )
        .unwrap();
        let spawner = Spawner::start().unwrap();
        let config = HandlerChildConfig {
            command,
            init: handler_init(),
            cpus: crate::worker::executor::placement::ChildCpus::Inherited,
            kill_wait: CHILD_KILL_WAIT,
            bounds,
        };
        let metrics = HandlerChildMetrics::of(&registry).unwrap();
        let run = HandlerChildRun::start(config, queue, metrics, spawner.handle());
        Self {
            points,
            registry,
            run,
            spawner,
        }
    }

    /// Offer the `SlotRecovered` event of slot `slot`.
    fn offer(&self, slot: u32) -> Emitted {
        self.points.emit(|| slot_recovered(slot))
    }

    /// The state of the child now.
    fn state(&self) -> HandlerChildState {
        self.run.view().state()
    }

    /// Stop the child within the bound of the test, then the spawner.
    async fn stop(self) -> (HandlerChildStopped, Duration) {
        let began = Instant::now();
        let stopped = tokio::time::timeout(STATE_WAIT, self.run.stop())
            .await
            .expect("the handler child stops within the test wait");
        let took = began.elapsed();
        let _report = self.spawner.stop(SPAWNER_STOP_WAIT);
        (stopped, took)
    }
}

fn slot_recovered(slot: u32) -> SlotRecovered {
    SlotRecovered {
        at: Utc::now(),
        child_kind: Execution::Isolated,
        slot: SlotIndex::try_from(slot).unwrap(),
    }
}

/// Shell text that reads each parent frame and keeps its body as the file
/// `frame-<n>` in `dir`, in order, through a rename, so a reader never sees
/// a part of one. At the stop frame it exits 0.
fn record_frames(dir: &TestDir) -> String {
    let stop_meta = encode_parent_frame(&ParentFrame::Stop)
        .unwrap()
        .head()
        .len()
        - HEADER_BYTES;
    let dir = dir.path().display();
    format!(
        r#"i=0; while :; do set -- $({header} | od -An -tu1); m=$(( $1 * 16777216 + $2 * 65536 + $3 * 256 + $4 )); b=$(( $9 * 16777216 + ${{10}} * 65536 + ${{11}} * 256 + ${{12}} )); meta=$({meta}); if [ "$m" -eq {stop_meta} ] && [ "$b" -eq 0 ]; then exit 0; fi; {body} > "{dir}/frame.part"; i=$((i + 1)); mv "{dir}/frame.part" "{dir}/frame-$i"; done"#,
        header = read_bytes(ScriptCount::Known(HEADER_BYTES)),
        meta = read_bytes(ScriptCount::Variable("m")),
        body = read_bytes(ScriptCount::Variable("b")),
    )
}

/// The events of every frame that the child kept in `dir`, in order.
fn recorded(dir: &TestDir) -> Vec<Event> {
    let mut frames: Vec<(u32, std::path::PathBuf)> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            let name = path.file_name()?.to_str()?.to_owned();
            let number = name.strip_prefix("frame-")?.parse().ok()?;
            Some((number, path))
        })
        .collect();
    frames.sort();
    frames
        .iter()
        .flat_map(|(_number, path)| {
            let body = std::fs::read_to_string(path).unwrap();
            let items: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
            items
                .iter()
                .map(|item| decode_event(item).unwrap())
                .collect::<Vec<Event>>()
        })
        .collect()
}

/// The slot of each `SlotRecovered` event of `events`.
fn recovered_slots(events: &[Event]) -> Vec<u32> {
    events
        .iter()
        .map(|event| match event {
            Event::SlotRecovered(recovered) => recovered.slot.get(),
            other => panic!("the child takes only SlotRecovered here, not {other:?}"),
        })
        .collect()
}

/// A handler child that boots, then keeps the body of each frame in `dir`.
fn recording_child(dir: &TestDir) -> ChildCommand {
    shell(&format!("{}{}", boot_any_kind(), record_frames(dir)))
}

/// Wait until the child kept `count` events in `dir`.
async fn until_recorded(dir: &TestDir, count: usize) {
    until("the child kept the events", STATE_WAIT, || {
        recorded(dir).len() >= count
    })
    .await;
}

// ---------------------------------------------------------------------------
// Boot and events
// ---------------------------------------------------------------------------

/// A handler child boots, is up, and takes the events in the order of their
/// offers; the stop frame ends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_child_boots_and_takes_the_events_in_order() {
    let dir = TestDir::new();
    let child = Harness::start(recording_child(&dir), bounds(), SentEventTypes::all(), 64);

    let first = child.run.first_boot().await;
    let offers: Vec<Emitted> = (0..3).map(|slot| child.offer(slot)).collect();
    until_recorded(&dir, 3).await;

    assert!(matches!(first, HandlerChildState::Up { .. }), "{first:?}");
    assert!(offers
        .iter()
        .all(|offer| *offer == Emitted::Offered(Offer::Queued)));
    assert_eq!(recovered_slots(&recorded(&dir)), vec![0, 1, 2]);
    assert!(series_of(&child.registry, MetricId::EventsDropped).is_empty());
    let (stopped, took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
    assert!(took < Duration::from_secs(3), "{took:?}");
}

/// A child with no ready frame in time stays down: the wait of the worker
/// returns within the ready bound, no second child boots, and each later
/// event drops and counts `child_down`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_not_ready_in_time_stays_down_and_the_worker_goes_on() {
    let dir = TestDir::new();
    let script = format!("{}{}sleep 30", ticket(&dir), take_init());
    let ready = Duration::from_millis(300);
    let child = Harness::start(
        shell(&script),
        HandlerChildBounds { ready, ..bounds() },
        SentEventTypes::all(),
        64,
    );

    let began = Instant::now();
    let _first = child.run.first_boot().await;
    assert!(began.elapsed() < ready + Duration::from_secs(2));
    let settled = child.run.view().settled(STATE_WAIT).await;

    match settled {
        HandlerChildState::Down(down) => {
            assert_eq!(
                down.cause,
                HandlerDownCause::BootFailed(BootFailureCause::NotReadyInTime)
            );
            assert_eq!(down.next, AfterDown::StaysDown);
        }
        other => panic!("the child is down, not {other:?}"),
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(tickets(&dir), 1);
    assert_eq!(
        child.offer(0),
        Emitted::Offered(Offer::Dropped(EventDropCause::ChildDown))
    );
    assert_eq!(
        series_of(&child.registry, MetricId::EventsDropped),
        vec![(vec!["child_down".to_owned()], SampleValue::Counter(1))]
    );
    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

/// A start hook that fails (exit code 173) leaves the child down with no
/// second boot; the worker is not stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_start_hook_leaves_the_child_down_with_no_second_boot() {
    let dir = TestDir::new();
    let script = format!("{}{}exit 173", ticket(&dir), take_init());
    let child = Harness::start(shell(&script), bounds(), SentEventTypes::all(), 64);

    let settled = child.run.view().settled(STATE_WAIT).await;

    match settled {
        HandlerChildState::Down(down) => {
            assert_eq!(down.cause, HandlerDownCause::StartHookFailed);
            assert_eq!(down.next, AfterDown::StaysDown);
        }
        other => panic!("the child is down, not {other:?}"),
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(tickets(&dir), 1);
    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

/// A child that dies after its ready frame is down for the first backoff,
/// then a new child boots and takes the events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_lost_after_its_ready_frame_boots_again_after_the_backoff() {
    let dir = TestDir::new();
    let script = format!(
        "{}{}case $n in 1) exit 3;; esac; {}",
        ticket(&dir),
        boot_any_kind(),
        record_frames(&dir),
    );
    let child = Harness::start(shell(&script), bounds(), SentEventTypes::all(), 64);

    until("the first child is lost", STATE_WAIT, || {
        matches!(child.state(), HandlerChildState::Down(_))
    })
    .await;
    let HandlerChildState::Down(down) = child.state() else {
        panic!("the child is down");
    };
    assert_eq!(down.cause, HandlerDownCause::Lost(LossCause::ChildDied));
    assert_eq!(
        down.next,
        AfterDown::BootsAgainAfter(Duration::from_secs(1))
    );
    until("the second child is up", STATE_WAIT, || {
        matches!(child.state(), HandlerChildState::Up { .. })
    })
    .await;
    assert_eq!(tickets(&dir), 2);
    assert_eq!(child.offer(7), Emitted::Offered(Offer::Queued));
    until_recorded(&dir, 1).await;
    assert_eq!(recovered_slots(&recorded(&dir)), vec![7]);
    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

/// While the child boots, the events wait in the queue; past its capacity
/// they drop and count `queue_full`. The child gets the ones that waited.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_queue_drops_and_counts_queue_full() {
    let dir = TestDir::new();
    let go = dir.file("go");
    let ready = send(&[ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: crate::worker::executor::protocol::RssReading::Unreadable,
    })]);
    let script = format!(
        r#"{}while [ ! -e "{go}" ]; do sleep 0.05; done; {ready}{}"#,
        take_init(),
        record_frames(&dir),
    );
    let child = Harness::start(shell(&script), bounds(), SentEventTypes::all(), 4);

    let offers: Vec<Emitted> = (0..6).map(|slot| child.offer(slot)).collect();
    std::fs::write(&go, b"").unwrap();
    until_recorded(&dir, 4).await;

    assert_eq!(
        offers[4..],
        [
            Emitted::Offered(Offer::Dropped(EventDropCause::QueueFull)),
            Emitted::Offered(Offer::Dropped(EventDropCause::QueueFull)),
        ]
    );
    assert_eq!(recovered_slots(&recorded(&dir)), vec![0, 1, 2, 3]);
    assert_eq!(
        series_of(&child.registry, MetricId::EventsDropped),
        vec![(vec!["queue_full".to_owned()], SampleValue::Counter(2))]
    );
    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

/// Only the event types that a handler takes reach the child: an event of
/// another type is not built, not sent and not counted as a drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_event_types_that_the_child_takes_reach_it() {
    let dir = TestDir::new();
    let child = Harness::start(
        recording_child(&dir),
        bounds(),
        SentEventTypes::of([EventTypeName::SlotRecovered]),
        64,
    );
    let _first = child.run.first_boot().await;

    let started = child.points.emit(|| ChildStarted {
        at: Utc::now(),
        child_kind: Execution::Isolated,
        slot: SlotIndex::try_from(0).unwrap(),
        child_pid: ProcessId::new(42).unwrap(),
        boot_duration: Duration::from_millis(3),
    });
    let recovered = child.offer(1);
    until_recorded(&dir, 1).await;

    assert_eq!(started, Emitted::TypeNotSent);
    assert_eq!(recovered, Emitted::Offered(Offer::Queued));
    assert_eq!(recovered_slots(&recorded(&dir)), vec![1]);
    assert!(series_of(&child.registry, MetricId::EventsDropped).is_empty());
    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

/// The stop writes the events that wait, then the stop frame; the child
/// exits at it, well before the stop exit bound. No event drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stop_writes_the_waiting_events_then_the_stop_frame() {
    let dir = TestDir::new();
    let long_flush = HandlerChildBounds {
        flush_interval: Duration::from_secs(20),
        ..bounds()
    };
    let child = Harness::start(recording_child(&dir), long_flush, SentEventTypes::all(), 64);
    let _first = child.run.first_boot().await;

    let offers: Vec<Emitted> = (0..5).map(|slot| child.offer(slot)).collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(recorded(&dir).is_empty());
    let registry = Arc::clone(&child.registry);
    let (stopped, took) = child.stop().await;

    assert!(offers
        .iter()
        .all(|offer| *offer == Emitted::Offered(Offer::Queued)));
    assert_eq!(stopped, HandlerChildStopped::Ended);
    assert!(took < Duration::from_secs(4), "{took:?}");
    assert_eq!(recovered_slots(&recorded(&dir)), vec![0, 1, 2, 3, 4]);
    assert!(series_of(&registry, MetricId::EventsDropped).is_empty());
}

/// A child that does not exit after the stop frame is killed after the
/// stop exit bound. Its events fit the socket, so none drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_does_not_exit_is_killed_after_the_stop_exit_bound() {
    let ready = send(&[ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: crate::worker::executor::protocol::RssReading::Unreadable,
    })]);
    let script = format!("{}{ready}sleep 60", take_init());
    let short_exit = HandlerChildBounds {
        stop_exit: Duration::from_millis(300),
        ..bounds()
    };
    let child = Harness::start(shell(&script), short_exit, SentEventTypes::all(), 64);
    let _first = child.run.first_boot().await;

    let _offers: Vec<Emitted> = (0..3).map(|slot| child.offer(slot)).collect();
    let registry = Arc::clone(&child.registry);
    let (stopped, took) = child.stop().await;

    assert_eq!(stopped, HandlerChildStopped::Ended);
    assert!(took < Duration::from_secs(3), "{took:?}");
    assert!(series_of(&registry, MetricId::EventsDropped).is_empty());
}

// ---------------------------------------------------------------------------
// handler_stats
// ---------------------------------------------------------------------------

fn stats(handlers: &[(&str, u64, u64)], rss_mib: f64) -> ChildFrame {
    stats_with_reads(handlers, SpanReadCounts::default(), rss_mib)
}

fn stats_with_reads(
    handlers: &[(&str, u64, u64)],
    span_reads: SpanReadCounts,
    rss_mib: f64,
) -> ChildFrame {
    ChildFrame::HandlerStats(HandlerStatsFrame {
        handlers: HandlerCountsList::new(
            handlers
                .iter()
                .map(|(name, errors, dropped)| HandlerCounts {
                    handler: HandlerName::cut(name),
                    errors: *errors,
                    dropped: *dropped,
                })
                .collect(),
        )
        .unwrap(),
        span_reads,
        rss: crate::worker::executor::protocol::RssReading::Mb(RssMb::new(rss_mib).unwrap()),
    })
}

/// The `handler_stats` frames hold totals: each counter gets the growth of
/// its handler's total; a name that the registry does not declare counts
/// under `(unknown)`; the RSS of the child is the gauge of the handler
/// child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_stats_frames_give_the_handler_counters_and_the_rss() {
    let dir = TestDir::new();
    let script = format!(
        "{}{}{}{}",
        boot_any_kind(),
        send(&[stats(&[(ON_END, 2, 1)], 10.0)]),
        send(&[stats(&[(ON_END, 5, 1), ("app.hooks.other", 1, 0)], 12.0)]),
        record_frames(&dir),
    );
    let child = Harness::start(shell(&script), bounds(), SentEventTypes::all(), 64);

    let registry = Arc::clone(&child.registry);
    until("the second stats frame is recorded", STATE_WAIT, || {
        series_of(&registry, MetricId::HandlerErrors)
            == vec![
                (vec!["(unknown)".to_owned()], SampleValue::Counter(1)),
                (vec![ON_END.to_owned()], SampleValue::Counter(5)),
            ]
    })
    .await;

    assert_eq!(
        series_of(&registry, MetricId::HandlerDropped),
        vec![
            (vec!["(unknown)".to_owned()], SampleValue::Counter(0)),
            (vec![ON_END.to_owned()], SampleValue::Counter(1)),
        ]
    );
    assert_eq!(
        series_of(&registry, MetricId::MemoryRss),
        vec![(
            vec!["handler_child".to_owned()],
            SampleValue::Gauge(12.0 * 1_048_576.0)
        )]
    );
    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

/// The span read counts of the `handler_stats` frames are totals: each
/// reason of `horsies.spans.worker_ended.read_failures` gets the growth of
/// its total.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_stats_frames_give_the_span_read_failures() {
    let dir = TestDir::new();
    let script = format!(
        "{}{}{}{}",
        boot_any_kind(),
        send(&[stats_with_reads(
            &[],
            SpanReadCounts {
                read_failed: 2,
                not_read: 0,
            },
            10.0
        )]),
        send(&[stats_with_reads(
            &[],
            SpanReadCounts {
                read_failed: 3,
                not_read: 4,
            },
            10.0
        )]),
        record_frames(&dir),
    );
    let child = Harness::start(shell(&script), bounds(), SentEventTypes::all(), 64);

    let registry = Arc::clone(&child.registry);
    until("the second stats frame is recorded", STATE_WAIT, || {
        series_of(&registry, MetricId::SpanReadFailures)
            == vec![
                (vec!["not_read".to_owned()], SampleValue::Counter(4)),
                (vec!["read_failed".to_owned()], SampleValue::Counter(3)),
            ]
    })
    .await;

    let (stopped, _took) = child.stop().await;
    assert_eq!(stopped, HandlerChildStopped::Ended);
}

// ---------------------------------------------------------------------------
// Snapshot form
// ---------------------------------------------------------------------------

/// Each state of the handler child has its form in the `telemetry`
/// column.
#[test]
fn each_state_has_its_snapshot_form() {
    let since = Utc.with_ymd_and_hms(2026, 11, 19, 9, 30, 0).unwrap();
    let cases = [
        (HandlerChildState::Off, json!({"state": "off"})),
        (HandlerChildState::Starting, json!({"state": "starting"})),
        (
            HandlerChildState::Up {
                pid: ProcessId::new(4242).unwrap(),
            },
            json!({"state": "up", "pid": 4242}),
        ),
        (
            HandlerChildState::Down(HandlerChildDown {
                cause: HandlerDownCause::BootFailed(BootFailureCause::NotReadyInTime),
                detail: "no ready frame after 30s".to_owned(),
                since,
                next: AfterDown::StaysDown,
            }),
            json!({
                "state": "down",
                "cause": {"kind": "boot_failed", "boot": "not_ready_in_time"},
                "detail": "no ready frame after 30s",
                "down_since": "2026-11-19T09:30:00.000000+00:00",
                "next": {"kind": "stays_down"},
            }),
        ),
        (
            HandlerChildState::Down(HandlerChildDown {
                cause: HandlerDownCause::Lost(LossCause::ChildDied),
                detail: "child died: exit code 3".to_owned(),
                since,
                next: AfterDown::BootsAgainAfter(Duration::from_secs(2)),
            }),
            json!({
                "state": "down",
                "cause": {"kind": "lost", "loss": "child_died"},
                "detail": "child died: exit code 3",
                "down_since": "2026-11-19T09:30:00.000000+00:00",
                "next": {"kind": "boots_again", "after_ms": 2000},
            }),
        ),
        (
            HandlerChildState::Down(HandlerChildDown {
                cause: HandlerDownCause::StartHookFailed,
                detail: "the start hook of the child failed or hung".to_owned(),
                since,
                next: AfterDown::StaysDown,
            }),
            json!({
                "state": "down",
                "cause": {"kind": "start_hook_failed"},
                "detail": "the start hook of the child failed or hung",
                "down_since": "2026-11-19T09:30:00.000000+00:00",
                "next": {"kind": "stays_down"},
            }),
        ),
    ];
    for (state, handler_child) in cases {
        assert_eq!(handler_child_json(&state), handler_child, "{state:?}");
    }
}

/// A resource shortage names its errno.
#[test]
fn a_resource_shortage_names_its_errno() {
    use crate::worker::executor::protocol::ShortageErrno;

    let down = HandlerChildState::Down(HandlerChildDown {
        cause: HandlerDownCause::BootFailed(BootFailureCause::ResourceShortage(
            ShortageErrno::Emfile,
        )),
        detail: "Too many open files".to_owned(),
        since: Utc.with_ymd_and_hms(2026, 11, 19, 9, 30, 0).unwrap(),
        next: AfterDown::BootsAgainAfter(Duration::from_secs(1)),
    });

    assert_eq!(
        handler_child_json(&down)["cause"],
        json!({"kind": "boot_failed", "boot": "resource_shortage", "errno": "EMFILE"})
    );
}

/// The view of a worker with no handler child is off at once.
#[tokio::test]
async fn the_view_of_no_handler_child_is_off() {
    let view = HandlerChildView::off();

    assert_eq!(view.state(), HandlerChildState::Off);
    assert_eq!(
        view.settled(Duration::from_millis(10)).await,
        HandlerChildState::Off
    );
}
