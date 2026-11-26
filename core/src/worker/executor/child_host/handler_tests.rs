//! The handler child of a Rust binary: the registration of the handlers,
//! the dispatch by event type, a handler that fails or panics, a handler
//! that blocks its thread, the `handler_stats` frames, the stop, and the
//! start hooks. No database and no process: the handlers run on their
//! threads in the test process, and the service writes to a socket pair.

use std::num::NonZeroUsize;
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::ended_spans::{EndedSpanScope, EndedSpans};
use super::handler_host::{
    boot_and_serve_handlers_on, decode_wire_event, serve_handlers, HandlerChildTimes,
    UndecodedEvent,
};
use super::handlers::{
    is_logged, Drained, EventHandlerRefused, EventHandlerSet, HandlerChildApp, HandlerError,
    HANDLER_ERROR_LOG_EVERY,
};
use super::host::{
    ChildHookFailure, ChildHostExit, ChildStartFacts, ChildStartFuture, ChildStartHook,
    NamedChildStartHook,
};
use super::link::{read_frame, FrameWriter};
use super::panic::install_panic_hook;
use super::seats::ServeEnd;
use crate::core::config::bounded::BoundedU32;
use crate::core::error::ErrorCode;
use crate::core::task::Execution;
use crate::events::{AnyEvent, EventDecodeError, EventTypeName, SlotRecovered, TaskClaimed};
use crate::process_exit::ProcessExit;
use crate::worker::executor::protocol::{
    decode_child_frame, BoundedText, ChildConnection, ChildFrame, ChildKind, ChildLogLevel,
    ChildProgram, DatabaseUrl, EventBatch, EventsFrame, FrameReader, HandlerChild, HandlerCounts,
    InitFrame, ParentFrame, TaskSendPool, WireEvent, MAX_EVENT_HANDLERS,
};
use crate::worker_manifest::EventHandlers;

/// The longest wait of a test for a handler.
const WAIT: Duration = Duration::from_secs(10);

/// A queue of 16 events: room for every event of a test that does not fill
/// a queue on purpose.
fn roomy() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn time() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_000_123_456_789)
}

fn slot_recovered(slot: u32) -> SlotRecovered {
    SlotRecovered {
        at: time(),
        child_execution: Execution::Isolated,
        slot: BoundedU32::try_from(slot).unwrap(),
    }
}

fn task_claimed(attempt: u32) -> TaskClaimed {
    TaskClaimed {
        at: time(),
        task_id: Uuid::from_u128(u128::from(attempt)),
        task_name: BoundedText::new("send_email".to_owned()).unwrap(),
        queue: BoundedText::new("default".to_owned()).unwrap(),
        execution: Execution::Isolated,
        attempt: BoundedU32::try_from(attempt).unwrap(),
        priority: BoundedU32::try_from(100).unwrap(),
        queue_wait: Duration::from_millis(5),
    }
}

async fn ignore_claimed(_event: TaskClaimed) -> Result<(), HandlerError> {
    Ok(())
}

fn ignore_slot(_event: SlotRecovered) -> Result<(), HandlerError> {
    Ok(())
}

/// The next value of `receiver`, at most [`WAIT`].
fn next<T>(receiver: &Receiver<T>) -> T {
    receiver
        .recv_timeout(WAIT)
        .unwrap_or_else(|error| panic!("no value within {WAIT:?}: {error}"))
}

/// Every value that `receiver` holds now.
fn drained<T>(receiver: &Receiver<T>) -> Vec<T> {
    receiver.try_iter().collect()
}

fn small_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The events frame of `events`, in order.
fn events_frame(events: &[AnyEvent]) -> EventsFrame {
    let wire: Vec<WireEvent> = events
        .iter()
        .map(|event| WireEvent::of(event).unwrap())
        .collect();
    EventsFrame {
        events: EventBatch::new(&wire).unwrap(),
    }
}

/// The counts of `handler`: its name, errors and drops.
fn counts(handler: &str, errors: u64, dropped: u64) -> HandlerCounts {
    HandlerCounts {
        handler: BoundedText::new(handler.to_owned()).unwrap(),
        errors,
        dropped,
    }
}

#[test]
fn a_handler_set_refuses_a_bad_name_a_repeated_name_and_one_handler_too_many() {
    let mut set = EventHandlerSet::new();
    set.add_blocking::<SlotRecovered>("audit.slots".to_owned(), ignore_slot)
        .unwrap();
    assert_eq!(
        set.add_async::<TaskClaimed>("audit.slots".to_owned(), ignore_claimed),
        Err(EventHandlerRefused::RepeatedName {
            name: "audit.slots".to_owned()
        })
    );
    for bad in [String::new(), "(unknown)".to_owned(), "x".repeat(256)] {
        let refused = set.add_async::<TaskClaimed>(bad.clone(), ignore_claimed);
        assert!(
            matches!(refused, Err(EventHandlerRefused::InvalidName { .. })),
            "{bad:?}: {refused:?}"
        );
    }
    assert_eq!(set.len(), 1);
    for index in 1..MAX_EVENT_HANDLERS {
        set.add_blocking::<SlotRecovered>(format!("audit.slots.{index}"), ignore_slot)
            .unwrap();
    }
    assert_eq!(set.len(), MAX_EVENT_HANDLERS);
    assert_eq!(
        set.add_async::<TaskClaimed>("one.more".to_owned(), ignore_claimed),
        Err(EventHandlerRefused::TooMany {
            name: "one.more".to_owned()
        })
    );
    assert_eq!(set.len(), MAX_EVENT_HANDLERS);
}

/// Each refusal kind has the code of the same refusal of the Python
/// registration: `HRS-225` for a name, `HRS-224` for a repeated name,
/// `HRS-226` for a 33rd handler. Each is a config code.
#[test]
fn each_handler_refusal_has_the_code_of_the_python_refusal() {
    let cases = [
        (
            EventHandlerRefused::InvalidName {
                cause: "empty".to_owned(),
            },
            ErrorCode::EventHandlerInvalidName,
            "HRS-225",
        ),
        (
            EventHandlerRefused::RepeatedName {
                name: "audit".to_owned(),
            },
            ErrorCode::EventHandlerDuplicateName,
            "HRS-224",
        ),
        (
            EventHandlerRefused::TooMany {
                name: "one.more".to_owned(),
            },
            ErrorCode::EventHandlerLimit,
            "HRS-226",
        ),
    ];
    for (refused, code, text) in cases {
        assert_eq!(refused.code(), code, "{refused:?}");
        assert_eq!(refused.code().as_code_str(), text, "{refused:?}");
        assert!(refused.code().is_config_error(), "{refused:?}");
    }
}

#[test]
fn the_manifest_names_each_handler_and_its_event_type_in_registration_order() {
    let mut set = EventHandlerSet::new();
    assert_eq!(set.manifest(), EventHandlers::NoHandlers);
    set.add_async::<TaskClaimed>("audit.claims".to_owned(), ignore_claimed)
        .unwrap();
    set.add_blocking::<SlotRecovered>("audit.slots".to_owned(), ignore_slot)
        .unwrap();
    let names: Vec<(&str, EventTypeName)> = set.names().collect();
    assert_eq!(
        names,
        vec![
            ("audit.claims", EventTypeName::TaskClaimed),
            ("audit.slots", EventTypeName::SlotRecovered),
        ]
    );
    assert_eq!(
        serde_json::to_value(set.manifest()).unwrap(),
        serde_json::json!({
            "kind": "HANDLERS",
            "handlers": [
                {"name": "audit.claims", "event_type": "TaskClaimed"},
                {"name": "audit.slots", "event_type": "SlotRecovered"}
            ]
        })
    );
}

/// Each handler gets the events of its own type only, in the order of the
/// offers; two handlers of one type each get every event of it.
#[test]
fn each_handler_takes_only_the_events_of_its_type() {
    let (claims_seen, claims) = channel::<u32>();
    let (slots_seen, slots) = channel::<u32>();
    let (slots_again_seen, slots_again) = channel::<u32>();
    let mut set = EventHandlerSet::new();
    set.add_async::<TaskClaimed>(
        "claims".to_owned(),
        async move |event: TaskClaimed| -> Result<(), HandlerError> {
            let _sent = claims_seen.send(event.attempt.get());
            Ok(())
        },
    )
    .unwrap();
    set.add_blocking::<SlotRecovered>("slots".to_owned(), move |event| {
        let _sent = slots_seen.send(event.slot.get());
        Ok(())
    })
    .unwrap();
    set.add_async::<SlotRecovered>(
        "slots.again".to_owned(),
        async move |event: SlotRecovered| -> Result<(), HandlerError> {
            let _sent = slots_again_seen.send(event.slot.get());
            Ok(())
        },
    )
    .unwrap();
    let running = set.start(roomy()).unwrap();
    for event in [
        AnyEvent::from(task_claimed(1)),
        AnyEvent::from(slot_recovered(3)),
        AnyEvent::from(task_claimed(2)),
        AnyEvent::from(slot_recovered(4)),
    ] {
        running.offer(&event);
    }
    assert_eq!([next(&claims), next(&claims)], [1, 2]);
    assert_eq!([next(&slots), next(&slots)], [3, 4]);
    assert_eq!([next(&slots_again), next(&slots_again)], [3, 4]);
    let tallies = running.tallies();
    assert_eq!(
        small_runtime().block_on(running.stop(WAIT)),
        Drained::AllFinished
    );
    assert!(drained(&claims).is_empty());
    assert!(drained(&slots).is_empty());
    assert_eq!(
        tallies.counts().handlers(),
        [
            counts("claims", 0, 0),
            counts("slots", 0, 0),
            counts("slots.again", 0, 0),
        ]
    );
}

/// A handler that panics or returns an error is counted, and takes its
/// next event: an async handler and a blocking handler alike.
#[test]
fn a_handler_that_panics_or_fails_is_counted_and_takes_its_next_event() {
    install_panic_hook();
    let (async_seen, async_slots) = channel::<u32>();
    let (blocking_seen, blocking_slots) = channel::<u32>();
    let mut set = EventHandlerSet::new();
    set.add_async::<SlotRecovered>(
        "async.flaky".to_owned(),
        async move |event: SlotRecovered| -> Result<(), HandlerError> {
            match event.slot.get() {
                0 => panic!("the async handler panicked at slot 0"),
                1 => Err(HandlerError::new("the async handler failed at slot 1")),
                slot => {
                    let _sent = async_seen.send(slot);
                    Ok(())
                }
            }
        },
    )
    .unwrap();
    set.add_blocking::<SlotRecovered>("blocking.flaky".to_owned(), move |event| {
        match event.slot.get() {
            0 => panic!("the blocking handler panicked at slot 0"),
            1 => Err(HandlerError::new("the blocking handler failed at slot 1")),
            slot => {
                let _sent = blocking_seen.send(slot);
                Ok(())
            }
        }
    })
    .unwrap();
    let running = set.start(roomy()).unwrap();
    for slot in 0..=2 {
        running.offer(&AnyEvent::from(slot_recovered(slot)));
    }
    assert_eq!(next(&async_slots), 2);
    assert_eq!(next(&blocking_slots), 2);
    let tallies = running.tallies();
    assert_eq!(
        tallies.counts().handlers(),
        [counts("async.flaky", 2, 0), counts("blocking.flaky", 2, 0)]
    );
    // Both threads live: each takes one more event after its failures.
    running.offer(&AnyEvent::from(slot_recovered(5)));
    assert_eq!(next(&async_slots), 5);
    assert_eq!(next(&blocking_slots), 5);
    assert_eq!(
        small_runtime().block_on(running.stop(WAIT)),
        Drained::AllFinished
    );
}

/// An async handler that blocks its thread fills its own queue: the events
/// past its bound drop and count for it alone, and another handler of the
/// same type takes every event. Released, it takes the events of its queue.
#[test]
fn a_blocking_handler_drops_its_events_and_the_others_take_theirs() {
    let (stuck_took, stuck_slots) = channel::<u32>();
    let (release, released) = channel::<()>();
    let released = Arc::new(Mutex::new(released));
    let (free_took, free_slots) = channel::<u32>();
    let mut set = EventHandlerSet::new();
    set.add_async::<SlotRecovered>(
        "stuck".to_owned(),
        async move |event: SlotRecovered| -> Result<(), HandlerError> {
            let _sent = stuck_took.send(event.slot.get());
            // Blocks the thread of this handler, and its runtime, until the
            // test releases it.
            let _released = released.lock().unwrap().recv();
            Ok(())
        },
    )
    .unwrap();
    set.add_blocking::<SlotRecovered>("free".to_owned(), move |event| {
        let _sent = free_took.send(event.slot.get());
        Ok(())
    })
    .unwrap();
    let capacity = NonZeroUsize::new(2).unwrap();
    let running = set.start(capacity).unwrap();
    running.offer(&AnyEvent::from(slot_recovered(0)));
    assert_eq!(next(&stuck_slots), 0);
    assert_eq!(next(&free_slots), 0);
    for slot in 1..=5 {
        running.offer(&AnyEvent::from(slot_recovered(slot)));
        // The free handler takes each event before the next offer.
        assert_eq!(next(&free_slots), slot);
    }
    let tallies = running.tallies();
    // Slots 1 and 2 wait in the stuck queue; slots 3, 4 and 5 dropped.
    assert_eq!(
        tallies.counts().handlers(),
        [counts("stuck", 0, 3), counts("free", 0, 0)]
    );
    assert!(drained(&stuck_slots).is_empty());
    drop(release);
    assert_eq!([next(&stuck_slots), next(&stuck_slots)], [1, 2]);
    assert_eq!(
        small_runtime().block_on(running.stop(WAIT)),
        Drained::AllFinished
    );
}

/// At the stop, a handler that does not take the events of its queue within
/// the drain bound is named; the stop does not wait for it.
#[test]
fn the_stop_names_a_handler_that_does_not_drain_in_time() {
    let (took, taken) = channel::<u32>();
    let (release, released) = channel::<()>();
    let released = Arc::new(Mutex::new(released));
    let mut set = EventHandlerSet::new();
    set.add_blocking::<SlotRecovered>("slow".to_owned(), move |event| {
        let _sent = took.send(event.slot.get());
        let _released = released.lock().unwrap().recv();
        Ok(())
    })
    .unwrap();
    let running = set.start(roomy()).unwrap();
    running.offer(&AnyEvent::from(slot_recovered(0)));
    assert_eq!(next(&taken), 0);
    let drain = Duration::from_millis(100);
    let started = std::time::Instant::now();
    assert_eq!(
        small_runtime().block_on(running.stop(drain)),
        Drained::Unfinished(vec!["slow".to_owned()])
    );
    assert!(started.elapsed() < WAIT);
    drop(release);
}

/// The service: each event of an `events` frame reaches the handlers of its
/// type; a `handler_stats` frame goes out each interval with the totals of
/// each handler; at the stop frame the handlers drain and the last frame
/// holds the final totals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_service_hands_out_the_events_and_reports_the_totals_of_each_handler() {
    let (ours, theirs) = UnixStream::pair().unwrap();
    let reading = std::thread::spawn(move || {
        let mut reader = FrameReader::new();
        let mut stream = &theirs;
        let mut frames = Vec::new();
        while let Ok(raw) = read_frame(&mut stream, &mut reader) {
            frames.push(decode_child_frame(&raw.meta, raw.body).unwrap());
        }
        frames
    });
    let writer = FrameWriter::start(ours, 8, ProcessExit::Standard).unwrap();
    let (slots_seen, slots) = channel::<u32>();
    let mut set = EventHandlerSet::new();
    set.add_blocking::<SlotRecovered>("slots".to_owned(), move |event| {
        let _sent = slots_seen.send(event.slot.get());
        Ok(())
    })
    .unwrap();
    set.add_async::<TaskClaimed>(
        "claims.failing".to_owned(),
        async |_event: TaskClaimed| -> Result<(), HandlerError> {
            Err(HandlerError::new("the claim was not written"))
        },
    )
    .unwrap();
    let running = set.start(roomy()).unwrap();
    let (parent, mut frames) = tokio::sync::mpsc::channel(8);
    let times = HandlerChildTimes {
        stats_every: Duration::from_millis(50),
        drain: WAIT,
        last_stats: WAIT,
    };
    let service = tokio::spawn(async move {
        serve_handlers(running, &EndedSpans::none(), writer, &mut frames, times).await
    });
    let batch = events_frame(&[
        AnyEvent::from(slot_recovered(1)),
        AnyEvent::from(task_claimed(1)),
        AnyEvent::from(slot_recovered(2)),
    ]);
    parent.send(ParentFrame::Events(batch)).await.unwrap();
    let seen = tokio::task::block_in_place(|| [next(&slots), next(&slots)]);
    assert_eq!(seen, [1, 2]);
    // At least one interval passes: a stats frame of the interval goes out.
    tokio::time::sleep(Duration::from_millis(200)).await;
    parent.send(ParentFrame::Stop).await.unwrap();
    assert_eq!(service.await.unwrap(), ServeEnd::Stopped);
    // The service dropped the writer, so the socket ends after its frames.
    let written = tokio::task::spawn_blocking(move || reading.join().unwrap())
        .await
        .unwrap();
    assert!(written.len() >= 2, "{written:?}");
    let stats: Vec<_> = written
        .iter()
        .map(|frame| match frame {
            ChildFrame::HandlerStats(stats) => stats,
            other => panic!("a handler child sends handler_stats frames only, not {other:?}"),
        })
        .collect();
    let last = stats.last().unwrap();
    assert_eq!(
        last.handlers.handlers(),
        [counts("slots", 0, 0), counts("claims.failing", 1, 0)]
    );
    for frame in &stats {
        assert_eq!(frame.handlers.handlers().len(), 2);
    }
}

/// A run or init frame is not a frame of a handler child: the service ends
/// with the bad-frame end at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_init_frame_after_the_boot_ends_the_service_as_a_bad_frame() {
    let (ours, _theirs) = UnixStream::pair().unwrap();
    let writer = FrameWriter::start(ours, 8, ProcessExit::Standard).unwrap();
    let mut set = EventHandlerSet::new();
    set.add_blocking::<SlotRecovered>("slots".to_owned(), ignore_slot)
        .unwrap();
    let running = set.start(roomy()).unwrap();
    let (parent, mut frames) = tokio::sync::mpsc::channel(8);
    let init = InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://user:secret@db:5432/app".to_owned()),
        connection: ChildConnection::default(),
        process_name: "horsies-child-handler".to_owned(),
        child_kind: ChildKind::Handler(HandlerChild {
            task_send_pool: TaskSendPool::new(1).unwrap(),
        }),
    };
    parent.send(ParentFrame::Init(init)).await.unwrap();
    let times = HandlerChildTimes {
        stats_every: Duration::from_secs(60),
        ..HandlerChildTimes::CHILD
    };
    let end = serve_handlers(running, &EndedSpans::none(), writer, &mut frames, times).await;
    assert_eq!(end, ServeEnd::BadParentFrame);
}

/// The handler child runs its own start hooks, in registration order, before
/// its handlers start; the first failure ends the child with exit code 173
/// and runs no later hook.
#[test]
fn the_start_hooks_of_the_handler_child_run_in_order_and_a_failure_exits_173() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recording = |name: &'static str| {
        let calls = Arc::clone(&calls);
        NamedChildStartHook {
            name: name.to_owned(),
            start: Box::new(move |_facts| -> ChildStartFuture {
                Box::pin(async move {
                    calls.lock().unwrap().push(name);
                    Ok(())
                })
            }),
        }
    };
    let failing = NamedChildStartHook {
        name: "exporter".to_owned(),
        start: Box::new(|_facts| -> ChildStartFuture {
            Box::pin(async {
                Err(ChildHookFailure {
                    cause: "the exporter did not start".to_owned(),
                })
            })
        }),
    };
    let mut handlers = EventHandlerSet::new();
    handlers
        .add_blocking::<SlotRecovered>("slots".to_owned(), ignore_slot)
        .unwrap();
    let app = HandlerChildApp {
        handlers,
        start_hook: ChildStartHook::Run(vec![recording("first"), failing, recording("third")]),
        ended_spans: EndedSpanScope::NoSpans,
    };
    let (ours, _theirs) = UnixStream::pair().unwrap();
    let exit = boot_and_serve_handlers_on(
        &runtime,
        app,
        &ours,
        FrameReader::new(),
        ProcessExit::Standard,
    );
    assert_eq!(exit, ChildHostExit::ChildHookFailed);
    assert_eq!(exit.code(), 173);
    assert_eq!(*calls.lock().unwrap(), vec!["first"]);
    runtime.shutdown_background();
}

/// A start hook of the handler child gets the facts of a handler child.
#[test]
fn a_start_hook_of_the_handler_child_gets_the_handler_facts() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recording = {
        let seen = Arc::clone(&seen);
        NamedChildStartHook {
            name: "exporter".to_owned(),
            start: Box::new(move |facts| -> ChildStartFuture {
                Box::pin(async move {
                    seen.lock().unwrap().push(facts);
                    // The failure ends the boot here, so the test needs no
                    // parent.
                    Err(ChildHookFailure {
                        cause: "the test stops the boot".to_owned(),
                    })
                })
            }),
        }
    };
    let app = HandlerChildApp {
        handlers: EventHandlerSet::new(),
        start_hook: ChildStartHook::Run(vec![recording]),
        ended_spans: EndedSpanScope::NoSpans,
    };
    let (ours, _theirs) = UnixStream::pair().unwrap();
    let exit = boot_and_serve_handlers_on(
        &runtime,
        app,
        &ours,
        FrameReader::new(),
        ProcessExit::Standard,
    );
    assert_eq!(exit, ChildHostExit::ChildHookFailed);
    assert_eq!(*seen.lock().unwrap(), vec![ChildStartFacts::Handler]);
    runtime.shutdown_background();
}

/// The first failure or drop of a handler is logged, then one per
/// `HANDLER_ERROR_LOG_EVERY`.
#[test]
fn the_first_failure_and_then_one_per_bound_are_logged() {
    assert!(is_logged(1));
    assert!(!is_logged(2));
    assert!(!is_logged(HANDLER_ERROR_LOG_EVERY - 1));
    assert!(is_logged(HANDLER_ERROR_LOG_EVERY));
    assert!(!is_logged(HANDLER_ERROR_LOG_EVERY + 1));
    assert!(is_logged(HANDLER_ERROR_LOG_EVERY * 2));
}

/// An event of a frame decodes on its own: a text that is not JSON and an
/// unknown event type each reach no handler, with their cause.
#[test]
fn an_event_decodes_on_its_own_and_a_bad_event_names_its_cause() {
    let event = AnyEvent::from(slot_recovered(7));
    let wire = WireEvent::of(&event).unwrap();
    assert_eq!(decode_wire_event(wire.as_str()).unwrap(), event);
    assert!(matches!(
        decode_wire_event("[\"SlotRecovered\","),
        Err(UndecodedEvent::NotJson(_))
    ));
    assert!(matches!(
        decode_wire_event("[\"NoSuchEvent\"]"),
        Err(UndecodedEvent::Refused(
            EventDecodeError::UnknownEventType { .. }
        ))
    ));
}

/// A handler's `?` takes any error; its text is the logged cause.
#[test]
fn a_handler_error_keeps_the_text_of_its_cause() {
    fn parse(text: &str) -> Result<u32, HandlerError> {
        Ok(text.parse::<u32>()?)
    }
    let refused = parse("seven").unwrap_err();
    assert_eq!(refused.cause(), "invalid digit found in string");
    assert_eq!(refused.to_string(), "invalid digit found in string");
    assert_eq!(
        HandlerError::new("the exporter is down").cause(),
        "the exporter is down"
    );
}

/// A binary with no handler gives its handler child no handler and no hook.
#[test]
fn an_app_without_handlers_has_no_handler_and_no_hook() {
    let app = HandlerChildApp::without_handlers();
    assert!(app.handlers.is_empty());
    assert!(matches!(app.start_hook, ChildStartHook::NoHook));
}
