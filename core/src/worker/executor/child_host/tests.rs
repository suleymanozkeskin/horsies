//! Tests of the child host: the arguments, the exit codes, the result
//! envelope, the deadline gate of thread seats, the poll watch, and the
//! blocking frame read.

use std::ffi::OsString;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::link::{read_frame, ReadEnd};
use super::run::{refusal_end, result_end};
use super::seats::{DeadlineGate, GateEnd, PollWatch};
use super::*;
use crate::core::task::error::{OperationalErrorCode, TaskError};
use crate::core::task::result::TaskResult;
use crate::worker::executor::protocol::{
    encode_parent_frame, ChildFrame, DispatchNumber, FrameReader, LoopCulprit, ParentFrame,
    RefusalReason, RunEnd, RunRef, StoredOptionsRefusal,
};

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

#[test]
fn the_deadline_gate_lets_exactly_one_side_win() {
    let returned_first = DeadlineGate::new();
    assert_eq!(returned_first.finish(), GateEnd::Returned);
    assert!(!returned_first.fire());

    let fired_first = DeadlineGate::new();
    assert!(fired_first.fire());
    assert_eq!(fired_first.finish(), GateEnd::Fired);
    assert!(!fired_first.fire());
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
