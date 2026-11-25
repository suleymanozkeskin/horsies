//! Tests of the child host: the arguments, the exit codes, the result
//! envelope, the run gate of thread seats, the end of a run on a seat, the
//! poll watch and the host loop watch, and the blocking frame read.

use std::ffi::OsString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::event_loop::{HostCulprit, HostLoopWatched};
use super::link::{read_frame, FrameWriter, QueuedFrames, ReadEnd};
use super::run::{refusal_end, result_end};
use super::seats::{
    end_seat_run, end_seat_run_blocking, BodyStart, DeadlineFire, GateCancel, GateEnd,
    InFlightRuns, PollWatch, RunGate,
};
use super::*;
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::result::TaskResult;
use crate::events::AttemptStart;
use crate::worker::executor::protocol::{
    encode_parent_frame, ChildAfterFrame, ChildFrame, DispatchNumber, FrameReader, LoopCulprit,
    ParentFrame, RefusalReason, RssReading, RunEnd, RunRef, SeatEnd, SharedResultFrame,
    StoredOptionsRefusal, LOOP_BLOCK_LIMIT,
};

/// The polls of a wait for a seat to free its run, and the time between two.
const SEAT_FREE_POLLS: u32 = 400;
const SEAT_FREE_POLL_INTERVAL: Duration = Duration::from_millis(25);

fn arguments(texts: &[&str]) -> Vec<OsString> {
    texts.iter().map(OsString::from).collect()
}

#[test]
fn a_binary_without_the_marker_runs_as_the_worker() {
    assert_eq!(child_mode(arguments(&["app"])), ChildMode::Worker);
    assert_eq!(
        child_mode(arguments(&["app", "worker", "--queues", "a"])),
        ChildMode::Worker
    );
    assert_eq!(child_mode(Vec::<OsString>::new()), ChildMode::Worker);
}

#[test]
fn the_marker_takes_the_socket_and_the_worker_pid() {
    match child_mode(arguments(&["app", CHILD_MODE_ARGUMENT, "7", "4242"])) {
        ChildMode::Child(args) => {
            assert_eq!(args.socket_fd, 7);
            assert_eq!(args.worker_pid.get(), 4242);
        }
        other => panic!("expected child mode, got {other:?}"),
    }
}

#[test]
fn child_mode_arguments_that_cannot_be_used_are_named() {
    assert_eq!(
        child_mode(arguments(&["app", CHILD_MODE_ARGUMENT, "7"])),
        ChildMode::BadArguments(ChildArgsError::Missing)
    );
    assert_eq!(
        child_mode(arguments(&["app", CHILD_MODE_ARGUMENT, "seven", "4242"])),
        ChildMode::BadArguments(ChildArgsError::SocketNotADescriptor {
            found: "seven".to_owned()
        })
    );
    assert_eq!(
        child_mode(arguments(&["app", CHILD_MODE_ARGUMENT, "-1", "4242"])),
        ChildMode::BadArguments(ChildArgsError::SocketNotADescriptor {
            found: "-1".to_owned()
        })
    );
    assert_eq!(
        child_mode(arguments(&["app", CHILD_MODE_ARGUMENT, "7", "0"])),
        ChildMode::BadArguments(ChildArgsError::WorkerPidNotAPid {
            found: "0".to_owned()
        })
    );
    assert_eq!(
        child_mode(arguments(&["app", CHILD_MODE_ARGUMENT, "7", "4242", "x"])),
        ChildMode::BadArguments(ChildArgsError::ExtraArgument {
            found: "x".to_owned()
        })
    );
}

#[test]
fn every_end_has_its_protocol_code() {
    assert_eq!(ChildHostExit::Stopped.code(), 0);
    assert_eq!(ChildHostExit::ParentGone.code(), 71);
    assert_eq!(ChildHostExit::BadParentFrame.code(), 72);
    assert_eq!(ChildHostExit::BadArguments.code(), 73);
    assert_eq!(ChildHostExit::BootFailed.code(), 75);
    assert_eq!(ChildHostExit::ProtocolMismatch.code(), 78);
    assert_eq!(ChildHostExit::ChildHookFailed.code(), 173);
}

fn envelope_of(end: RunEnd) -> String {
    match end {
        RunEnd::Result(envelope) => envelope.into_string(),
        other => panic!("expected a result envelope, got {other:?}"),
    }
}

#[test]
fn a_value_becomes_the_ok_envelope_with_its_text_kept() {
    let envelope = envelope_of(result_end(TaskResult::Ok(br#"{"b":1,"a":2}"#.to_vec())));
    // The form of Python 0.7.0 `dumps_json(encode_task_result(..))`.
    assert_eq!(
        envelope,
        r#"{"__h_task_result__":true,"ok":{"b":1,"a":2},"err":null}"#
    );
}

#[test]
fn a_value_that_is_not_json_is_a_serialization_error() {
    let envelope = envelope_of(result_end(TaskResult::Ok(b"{not json".to_vec())));
    let parsed: TaskResult<serde_json::Value> = serde_json::from_str(&envelope).unwrap();
    match parsed {
        TaskResult::Err(error) => {
            assert_eq!(error.error_code.to_string(), "WORKER_SERIALIZATION_ERROR");
        }
        TaskResult::Ok(_) => panic!("expected an error envelope"),
    }
}

#[test]
fn a_task_error_becomes_the_error_envelope() {
    let error = TaskError::builtin(OperationalErrorCode::TaskException, "boom");
    let envelope = envelope_of(result_end(TaskResult::Err(error)));
    assert!(
        envelope.starts_with(r#"{"__h_task_result__":true,"ok":null,"err":{"#),
        "{envelope}"
    );
    assert!(envelope.contains("TASK_EXCEPTION"), "{envelope}");
}

#[test]
fn a_refusal_fails_the_task_with_its_reason() {
    let envelope = envelope_of(refusal_end(&StoredOptionsRefusal::TimeoutValueInvalid(
        RefusalReason::cut("stored timeout_ms is below 1000 ms"),
    )));
    assert!(
        envelope.contains("WORKER_SERIALIZATION_ERROR"),
        "{envelope}"
    );
    assert!(
        envelope.contains("stored timeout_ms is below 1000 ms"),
        "{envelope}"
    );
}

/// The body of a thread run returns first: a later deadline and a later
/// operator cancel change nothing.
#[test]
fn a_thread_run_whose_body_returned_first_ignores_the_deadline_and_a_cancel() {
    let gate = RunGate::new();
    assert_eq!(gate.begin_body(), BodyStart::Start);
    assert_eq!(gate.finish(), GateEnd::Returned);
    assert_eq!(gate.fire_deadline(), DeadlineFire::Lost);
    assert_eq!(gate.cancel(), GateCancel::RunEnded);
}

/// The deadline of a thread run fires first: the return reads it, and a
/// later operator cancel is ignored.
#[test]
fn a_thread_run_whose_deadline_fired_first_ignores_a_cancel() {
    let gate = RunGate::new();
    assert_eq!(gate.begin_body(), BodyStart::Start);
    assert_eq!(gate.fire_deadline(), DeadlineFire::Won);
    assert_eq!(gate.cancel(), GateCancel::RunEnded);
    assert_eq!(gate.fire_deadline(), DeadlineFire::Lost);
    assert_eq!(gate.finish(), GateEnd::DeadlineFired);
}

/// An operator cancel of a thread run whose body runs wins over a later
/// deadline, and the return reads it.
#[test]
fn an_operator_cancel_while_the_thread_body_runs_wins_over_the_deadline() {
    let gate = RunGate::new();
    assert_eq!(gate.begin_body(), BodyStart::Start);
    assert_eq!(gate.cancel(), GateCancel::BodyRunning);
    assert_eq!(gate.cancel(), GateCancel::RunEnded);
    assert_eq!(gate.fire_deadline(), DeadlineFire::Lost);
    assert_eq!(gate.finish(), GateEnd::CancelFired);
}

/// An operator cancel before the body starts keeps the body from starting,
/// also for a run that ends with no body.
#[test]
fn an_operator_cancel_before_the_thread_body_keeps_it_from_starting() {
    let gate = RunGate::new();
    assert_eq!(gate.cancel(), GateCancel::BeforeBody);
    assert_eq!(gate.begin_body(), BodyStart::CancelledBeforeBody);

    let no_body = RunGate::new();
    assert_eq!(no_body.cancel(), GateCancel::BeforeBody);
    assert_eq!(no_body.end_without_body(), BodyStart::CancelledBeforeBody);
}

/// A thread run that ends with no body closes its gate to a later cancel.
#[test]
fn a_thread_run_with_no_body_ignores_a_later_cancel() {
    let gate = RunGate::new();
    assert_eq!(gate.end_without_body(), BodyStart::Start);
    assert_eq!(gate.cancel(), GateCancel::RunEnded);
}

fn seat_run(number: u64) -> RunRef {
    RunRef {
        dispatch: DispatchNumber::new(number).expect("a dispatch number"),
        task_id: Uuid::from_u128(u128::from(number)),
    }
}

/// The end of `run` on a seat.
fn seat_end(run: RunRef) -> SharedResultFrame {
    SharedResultFrame {
        run,
        end: SeatEnd::TimedOut,
        started_at: AttemptStart::NotReported,
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    }
}

/// A writer whose queue of one frame holds a frame of the run `earlier`:
/// the next send waits until the test takes that frame.
async fn full_writer(earlier: RunRef) -> (FrameWriter, QueuedFrames) {
    let (writer, queued) = FrameWriter::without_thread(1);
    writer.send(ChildFrame::CancelFired(earlier)).await;
    assert_eq!(queued.count(), 1, "the queue is full");
    (writer, queued)
}

/// Return once no run is in flight.
async fn until_no_run_in_flight(in_flight: &AtomicUsize) {
    for _ in 0..SEAT_FREE_POLLS {
        match in_flight.load(Ordering::Acquire) {
            0 => return,
            _ => tokio::time::sleep(SEAT_FREE_POLL_INTERVAL).await,
        }
    }
    panic!("the seat was not freed while the end of its run waited to be queued");
}

/// #2296: the seat of a shared async run is free before the end of the run
/// is queued. The parent frees the seat when it reads the end, and may send
/// `stop` or a new run at once; a seat still taken made the child exit with
/// the bad-parent-frame code. The queue is full, so the end waits to be
/// queued while the test reads the count of runs in flight.
#[tokio::test]
async fn an_async_seat_is_free_before_the_end_of_its_run_is_queued() {
    let (earlier, run) = (seat_run(1), seat_run(2));
    let (writer, mut queued) = full_writer(earlier).await;
    let in_flight = Arc::new(AtomicUsize::new(1));
    let cancels: InFlightRuns<()> = InFlightRuns::new();
    let seat_in_flight = Arc::clone(&in_flight);
    let seat = tokio::spawn(async move {
        end_seat_run(&writer, &seat_in_flight, &cancels, seat_end(run)).await;
    });
    until_no_run_in_flight(&in_flight).await;
    assert_eq!(queued.count(), 1, "the end waits behind the full queue");
    assert_eq!(queued.take().await, ChildFrame::CancelFired(earlier));
    assert_eq!(queued.take().await, ChildFrame::SharedResult(seat_end(run)));
    seat.await.expect("the seat task ends");
}

/// #2296: the seat of a shared thread run is free before the end of the run
/// is queued, as for an async seat; the end goes out from the seat thread.
#[tokio::test]
async fn a_thread_seat_is_free_before_the_end_of_its_run_is_queued() {
    let (earlier, run) = (seat_run(3), seat_run(4));
    let (writer, mut queued) = full_writer(earlier).await;
    let in_flight = Arc::new(AtomicUsize::new(1));
    let cancels: InFlightRuns<()> = InFlightRuns::new();
    let seat_in_flight = Arc::clone(&in_flight);
    let seat = std::thread::spawn(move || {
        end_seat_run_blocking(&writer, &seat_in_flight, &cancels, seat_end(run));
    });
    until_no_run_in_flight(&in_flight).await;
    assert_eq!(queued.count(), 1, "the end waits behind the full queue");
    assert_eq!(queued.take().await, ChildFrame::CancelFired(earlier));
    assert_eq!(queued.take().await, ChildFrame::SharedResult(seat_end(run)));
    seat.join().expect("the seat thread ends");
}

#[test]
fn a_blocked_poll_is_reported_once_with_its_run() {
    let epoch = Instant::now()
        .checked_sub(Duration::from_secs(60))
        .unwrap_or_else(Instant::now);
    let watch = PollWatch::without_thread(epoch);
    let run = RunRef {
        dispatch: DispatchNumber::new(3).unwrap(),
        task_id: Uuid::nil(),
    };
    let polls = watch.register(run);
    let poll_started = polls.start_of(0, "app.tasks.spin");
    assert!(watch.blocked_reports().is_empty(), "no poll in progress");
    // A poll that started 1 ms after the epoch, about 60 s ago.
    poll_started.store(1, Ordering::Release);
    let reports = watch.blocked_reports();
    assert_eq!(reports.len(), 1);
    match &reports[0] {
        ChildFrame::LoopBlocked(blocked) => {
            assert_eq!(blocked.culprit, LoopCulprit::Run(run));
            assert!(blocked.blocked_ms >= 5_000);
            assert_eq!(blocked.location.as_str(), "app.tasks.spin");
        }
        other => panic!("expected a loop report, got {other:?}"),
    }
    assert!(watch.blocked_reports().is_empty(), "reported once");
}

/// #875 (Python: the loop watchdog names the run of the mark that the
/// blocking code carries): a poll of work that a body started with
/// `spawn_in_run` is timed under the body's run. Its block is reported with
/// that run as the culprit and the work's name as the location, while the
/// body waits; the run is reported once.
#[test]
fn a_blocked_poll_of_work_of_a_run_is_reported_with_its_run() {
    let epoch = Instant::now()
        .checked_sub(Duration::from_secs(60))
        .unwrap_or_else(Instant::now);
    let watch = PollWatch::without_thread(epoch);
    let run = RunRef {
        dispatch: DispatchNumber::new(4).unwrap(),
        task_id: Uuid::from_u128(4),
    };
    let polls = watch.register(run);
    let body_started = polls.start_of(0, "app.tasks.fetch");
    let work_started = polls.start_of(1, "fetch.page");
    let later_work_started = polls.start_of(2, "fetch.other_page");
    assert!(watch.blocked_reports().is_empty(), "no poll in progress");
    // The body waits for its work. The work's poll started 1 ms after the
    // epoch, about 60 s ago; another piece of work polls on another thread
    // and started 30 s after the epoch.
    assert_eq!(body_started.load(Ordering::Acquire), 0);
    work_started.store(1, Ordering::Release);
    later_work_started.store(30_001, Ordering::Release);
    let reports = watch.blocked_reports();
    assert_eq!(reports.len(), 1);
    match &reports[0] {
        ChildFrame::LoopBlocked(blocked) => {
            assert_eq!(blocked.culprit, LoopCulprit::Run(run));
            assert!(blocked.blocked_ms >= 59_000);
            assert_eq!(blocked.location.as_str(), "fetch.page");
        }
        other => panic!("expected a loop report, got {other:?}"),
    }
    assert!(watch.blocked_reports().is_empty(), "reported once");
}

/// The frame of a blocked host loop names the run of the held task while
/// that run is in flight; a held task with no run in flight, and samples
/// that named no run, give no named culprit.
#[test]
fn a_host_loop_frame_names_the_run_in_flight_of_the_held_task() {
    let watch = PollWatch::without_thread(Instant::now());
    let run = RunRef {
        dispatch: DispatchNumber::new(5).unwrap(),
        task_id: Uuid::from_u128(5),
    };
    let _polls = watch.register(run);
    let culprit_of = |culprit: HostCulprit| match watch.host_loop_frame(5_000, culprit) {
        ChildFrame::LoopBlocked(blocked) => {
            assert_eq!(blocked.blocked_ms, 5_000);
            assert_eq!(
                blocked.location.as_str(),
                "the event loop of the host program"
            );
            blocked.culprit
        }
        other => panic!("expected a loop report, got {other:?}"),
    };
    assert_eq!(
        culprit_of(HostCulprit::Run(run.task_id)),
        LoopCulprit::Run(run)
    );
    assert_eq!(
        culprit_of(HostCulprit::Run(Uuid::from_u128(6))),
        LoopCulprit::Unknown
    );
    assert_eq!(culprit_of(HostCulprit::Unknown), LoopCulprit::Unknown);
}

/// The watch of the main thread of a shared thread child: a host loop that
/// stops turning gives one `loop_blocked` frame after the block limit, with
/// no named culprit (the main thread holds no run), and no second frame.
#[tokio::test]
async fn the_main_thread_watch_reports_a_host_loop_that_stops_turning() {
    let (writer, mut queued) = FrameWriter::without_thread(16);
    let host = Arc::new(HostLoop::new());
    let started = Instant::now();
    let _watch = PollWatch::start(writer, HostLoopWatched::Watched(Arc::clone(&host)))
        .expect("the watch thread starts");
    let frame = tokio::time::timeout(Duration::from_secs(10), queued.take())
        .await
        .expect("one frame within 10 s");
    assert!(started.elapsed() >= LOOP_BLOCK_LIMIT);
    match frame {
        ChildFrame::LoopBlocked(blocked) => {
            assert_eq!(blocked.culprit, LoopCulprit::Unknown);
            assert!(blocked.blocked_ms >= 5_000, "{}", blocked.blocked_ms);
        }
        other => panic!("expected a loop report, got {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(queued.count(), 0, "one report for the block");
}

#[test]
fn a_blocking_read_takes_one_frame_and_names_the_eof() {
    let bytes = encode_parent_frame(&ParentFrame::Stop).unwrap().to_bytes();
    let mut reader = FrameReader::new();
    let mut stream: &[u8] = &bytes;
    let raw = read_frame(&mut stream, &mut reader).unwrap();
    assert_eq!(raw.meta, br#"{"kind":"stop"}"#);
    assert!(raw.body.is_empty());
    assert!(matches!(
        read_frame(&mut stream, &mut reader),
        Err(ReadEnd::Eof)
    ));
}

/// After a body returned with work left, the child
/// ends that work (the grace and the cancel rounds) and writes the
/// finalizing marker, whose statement a cancel ends at its bound, before
/// the parent's backstop kills it.
#[test]
fn the_parent_waits_past_the_end_of_left_work_and_the_marker() {
    let rounds =
        super::run_work::CANCEL_ROUND_INTERVAL * (1 + super::run_work::EXTRA_CANCEL_ROUNDS);
    let child_end = super::run_work::ISOLATED_LEFTOVER_GRACE
        + rounds
        + crate::broker::child_writes::FINALIZING_MARKER_END;
    assert!(
        crate::worker::executor::slots::isolated::ISOLATED_LEFTOVER_BACKSTOP > child_end,
        "{child_end:?}"
    );
}
