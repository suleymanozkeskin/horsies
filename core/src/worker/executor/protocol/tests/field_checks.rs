//! Each field of a child frame and of a preload frame is checked: a frame
//! whose metadata field is changed is refused as malformed, never read with a
//! guessed value. Each form of a child frame round trips.

use std::num::NonZeroU64;

use serde_json::{json, Value};

use super::*;

/// The bytes of one child frame.
fn child_bytes(frame: &ChildFrame) -> Vec<u8> {
    encode_child_frame(frame).unwrap().to_bytes()
}

/// The metadata text and the body of one whole frame.
fn meta_text_and_body(frame: &[u8]) -> (String, Vec<u8>) {
    let (meta, body) = split(frame);
    (String::from_utf8(meta).unwrap(), body)
}

/// `frame` with the metadata field at `path` set to `value`, or left out
/// when `value` is `None`. `path` names a nested field with dots. The body
/// stays.
fn changed(frame: &[u8], path: &str, value: Option<&Value>) -> Vec<u8> {
    let (meta, body) = split(frame);
    let mut fields: Value = serde_json::from_slice(&meta).unwrap();
    let mut keys: Vec<&str> = path.split('.').collect();
    let last = keys.pop().unwrap();
    let mut object = &mut fields;
    for key in keys {
        object = object.get_mut(key).unwrap();
    }
    let object = object.as_object_mut().unwrap();
    match value {
        Some(value) => {
            let _replaced = object.insert(last.to_owned(), value.clone());
        }
        None => {
            let _removed = object.shift_remove(last);
        }
    }
    framed(&fields.to_string(), &body)
}

/// Each case changes one field of `frame`; the parent refuses every result
/// as malformed.
fn assert_each_change_refused(frame: &[u8], cases: &[(&str, Option<Value>)]) {
    for (path, value) in cases {
        let decoded = decode_child_bytes(&changed(frame, path, value.as_ref()));
        assert!(
            matches!(decoded, Err(ChildFrameRefusal::Malformed(_))),
            "{path} = {value:?}: {decoded:?}"
        );
    }
}

/// Malformed forms of a field that counts and names the asyncio tasks on a
/// task loop.
fn bad_held() -> Vec<Value> {
    vec![
        Value::Null,
        json!({"names": ["a"]}),
        json!({"count": 0, "names": []}),
        json!({"count": true, "names": ["a"]}),
        json!({"count": "2", "names": ["a", "b"]}),
        json!({"count": 1}),
        json!({"count": 1, "names": "a"}),
        json!({"count": 1, "names": [1]}),
        json!({"count": 2, "names": ["a"]}),
        json!({"count": 1, "names": ["a", "b"]}),
        json!({
            "count": MAX_LEFTOVER_TASK_NAMES + 1,
            "names": vec!["a"; MAX_LEFTOVER_TASK_NAMES + 1],
        }),
        json!({"count": 1, "names": ["n".repeat(MAX_LEFTOVER_NAME_CHARS + 1)]}),
    ]
}

/// Malformed forms of what a task body returned, for a frame whose body
/// holds a result envelope.
fn bad_returned() -> Vec<Value> {
    vec![
        Value::Null,
        json!({"kind": "timed_out"}),
        json!({"kind": "worker_failure", "detail": 3}),
        json!({"kind": "stopped_before_running", "reason": "CLAIM_LOST"}),
    ]
}

fn returned_result() -> BodyEnd {
    BodyEnd::Result(envelope(r#"{"x":1}"#))
}

/// Malformed forms of the `started_at` of a frame that ends a run: the
/// start of the attempt, `reported` with its time or `not_reported`.
fn bad_start() -> Vec<Option<Value>> {
    vec![
        None,
        Some(Value::Null),
        Some(json!("2026-11-16T09:30:01Z")),
        Some(json!({"kind": "started"})),
        Some(json!({"kind": "reported"})),
        Some(json!({"kind": "reported", "time": "yesterday"})),
        Some(json!({"kind": "reported", "time": 1700000000})),
        Some(json!({"kind": "not_reported", "time": "2026-11-16T09:30:01Z"})),
    ]
}

/// Malformed forms of the `started_at` of a frame whose body ran: the time
/// of the start write, as RFC 3339 text.
fn bad_start_time() -> Vec<Option<Value>> {
    vec![
        None,
        Some(Value::Null),
        Some(json!(1700000000)),
        Some(json!("yesterday")),
        Some(json!({"kind": "reported", "time": "2026-11-16T09:30:01Z"})),
    ]
}

/// Python `TestSharedFrames::test_every_shared_result_field_is_checked`.
#[test]
fn every_shared_result_field_is_checked() {
    let frame = child_bytes(&ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref(7, T1),
        end: SeatEnd::TimedOut,
        started_at: crate::events::AttemptStart::Reported {
            started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        },
        rss: rss(1.0),
        child: ChildAfterFrame::ServesOn,
    }));
    assert_each_change_refused(
        &frame,
        &[
            ("dispatch", Some(json!(-1))),
            ("dispatch", Some(json!(0))),
            ("task_id", Some(json!(5))),
            ("end", Some(Value::Null)),
            ("end", Some(json!({"kind": "unknown"}))),
            (
                "end",
                Some(json!({"kind": "stopped_before_running", "reason": 3})),
            ),
            ("end", Some(json!({"kind": "worker_failure", "detail": 3}))),
            ("rss_mb", Some(json!("x"))),
            ("rss_mb", Some(json!(true))),
            ("child", None),
            ("child", Some(json!("retires"))),
            ("child", Some(json!(1))),
        ],
    );
    let cases: Vec<(&str, Option<Value>)> = bad_start()
        .into_iter()
        .map(|value| ("started_at", value))
        .collect();
    assert_each_change_refused(&frame, &cases);
}

/// Python `TestSharedFrames::test_a_timed_out_end_carries_no_body`.
#[test]
fn a_timed_out_seat_end_carries_no_body() {
    let frame = child_bytes(&ChildFrame::SharedResult(SharedResultFrame {
        run: run_ref(7, T1),
        end: SeatEnd::TimedOut,
        started_at: crate::events::AttemptStart::Reported {
            started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        },
        rss: RssReading::Unreadable,
        child: ChildAfterFrame::ServesOn,
    }));
    let (meta, body) = meta_text_and_body(&frame);
    assert!(body.is_empty());
    assert_eq!(
        malformed_child(&framed(&meta, b"stray")),
        MalformedFrame::UnexpectedBody {
            kind: "timed_out",
            body_bytes: 5,
        },
    );
}

/// Python `TestChildKindPool::test_a_shared_run_frame_is_refused_by_the_child_decoder`.
#[test]
fn the_child_decoder_refuses_a_parent_frame() {
    let shared_run = encode_parent_frame(&ParentFrame::SharedRun(SharedRunFrame {
        dispatch: DispatchNumber::new(7).unwrap(),
        task_id: task_id(T1),
        task_name: "t".to_owned(),
        queue_name: default_queue(),
        attempt: AttemptNumber::SMALLEST,
        worker_id: "w".to_owned(),
        claimed_at: claim_time(),
        orphan_self_heal: OrphanSelfHeal::Off,
        work: SharedWork::AsyncTask {
            timeout_ms: TaskTimeoutMs::new(30_000).unwrap(),
            cancel_grace_ms: CancelGraceMs::new(5_000).unwrap(),
        },
        input: TaskInputText::new("{}".to_owned()),
        workflow: RunWorkflow::NotInWorkflow,
        trace: crate::events::RunTrace::NoCarrier,
    }))
    .unwrap()
    .to_bytes();
    let decoded = decode_child_bytes(&shared_run);
    assert!(
        matches!(
            decoded,
            Err(ChildFrameRefusal::Malformed(
                MalformedFrame::UnknownKind { .. }
            ))
        ),
        "{decoded:?}"
    );
}

/// Python `TestChildKindPool::test_every_stuck_report_field_is_checked`.
#[test]
fn every_stuck_report_field_is_checked() {
    let frame = child_bytes(&ChildFrame::RunStuck(RunStuckFrame {
        run: run_ref(3, T1),
        work: StuckWork::new(2, 0, names(&["a", "b"])).unwrap(),
    }));
    assert_each_change_refused(
        &frame,
        &[
            ("tasks", Some(json!(-1))),
            ("executor_items", Some(json!(-1))),
            ("executor_items", Some(json!(true))),
            ("names", Some(json!(["a"]))),
            ("names", Some(json!(["a", "b", "c"]))),
            (
                "names",
                Some(json!(["a", "x".repeat(MAX_LEFTOVER_NAME_CHARS + 1)])),
            ),
            ("dispatch", Some(json!(0))),
        ],
    );
    let nothing_alive = changed(
        &changed(&frame, "tasks", Some(&json!(0))),
        "names",
        Some(&json!([])),
    );
    assert!(matches!(
        decode_child_bytes(&nothing_alive),
        Err(ChildFrameRefusal::Malformed(
            MalformedFrame::BrokenRule { .. }
        ))
    ));
}

/// Python `TestChildKindPool::test_every_blocked_loop_field_is_checked`.
#[test]
fn every_blocked_loop_field_is_checked() {
    let frame = child_bytes(&ChildFrame::LoopBlocked(LoopBlockedFrame {
        culprit: LoopCulprit::Unknown,
        blocked_ms: 5000,
        location: BlockedWhere::new("x".to_owned()).unwrap(),
    }));
    assert_each_change_refused(
        &frame,
        &[
            ("culprit", Some(Value::Null)),
            (
                "culprit",
                Some(json!({"kind": "run", "dispatch": 0, "task_id": T1})),
            ),
            ("culprit", Some(json!({"kind": "run", "dispatch": 3}))),
            ("culprit", Some(json!({"kind": "other"}))),
            ("blocked_ms", Some(json!(-1))),
            ("blocked_ms", Some(json!(1.5))),
            (
                "where",
                Some(json!("x".repeat(MAX_BLOCKED_WHERE_CHARS + 1))),
            ),
        ],
    );
}

/// The start of a leftover cancel is the time of its start write: every
/// malformed form of it is refused.
#[test]
fn every_leftover_cancel_start_is_checked() {
    let frame = child_bytes(&ChildFrame::LeftoverCancel(LeftoverCancelFrame {
        run: run_ref(3, T1),
        returned: returned_result(),
        started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    }));
    let cases: Vec<(&str, Option<Value>)> = bad_start_time()
        .into_iter()
        .map(|value| ("started_at", value))
        .collect();
    assert_each_change_refused(&frame, &cases);
}

/// Python `TestLeftoverFrames::test_a_leftover_cancel_without_a_finished_return_is_malformed`.
#[test]
fn a_leftover_cancel_needs_what_the_body_returned() {
    let frame = child_bytes(&ChildFrame::LeftoverCancel(LeftoverCancelFrame {
        run: run_ref(3, T1),
        returned: returned_result(),
        started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    }));
    let timed_out = changed(&frame, "returned", Some(&json!({"kind": "timed_out"})));
    let decoded = decode_child_bytes(&timed_out);
    assert!(
        matches!(
            decoded,
            Err(ChildFrameRefusal::Malformed(
                MalformedFrame::UnknownKind { .. }
            ))
        ),
        "{decoded:?}"
    );
}

fn leftover_stuck_bytes() -> Vec<u8> {
    child_bytes(&ChildFrame::LeftoverStuck(LeftoverStuckFrame {
        run: run_ref(3, T1),
        work: StuckWork::new(2, 1, names(&["keepalive", "refill"])).unwrap(),
        returned: returned_result(),
        started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    }))
}

/// Python `TestLeftoverFrames::test_every_leftover_stuck_field_is_checked`.
#[test]
fn every_leftover_stuck_field_is_checked() {
    let mut cases: Vec<(&str, Option<Value>)> = bad_returned()
        .into_iter()
        .map(|value| ("returned", Some(value)))
        .collect();
    cases.extend([
        ("tasks", Some(json!(-1))),
        ("executor_items", Some(json!(true))),
        ("names", Some(json!(["a"]))),
        (
            "names",
            Some(json!(["a", "x".repeat(MAX_LEFTOVER_NAME_CHARS + 1)])),
        ),
        ("dispatch", Some(json!(0))),
        ("task_id", Some(Value::Null)),
    ]);
    cases.extend(
        bad_start_time()
            .into_iter()
            .map(|value| ("started_at", value)),
    );
    assert_each_change_refused(&leftover_stuck_bytes(), &cases);
}

/// Python `TestLeftoverFrames::test_nothing_alive_is_no_stuck_report`.
#[test]
fn a_leftover_stuck_report_with_nothing_alive_is_refused() {
    let mut frame = leftover_stuck_bytes();
    for (path, value) in [
        ("tasks", json!(0)),
        ("executor_items", json!(0)),
        ("names", json!([])),
    ] {
        frame = changed(&frame, path, Some(&value));
    }
    assert!(matches!(
        decode_child_bytes(&frame),
        Err(ChildFrameRefusal::Malformed(
            MalformedFrame::BrokenRule { .. }
        ))
    ));
}

/// Python `TestLeftWorkFrames::test_every_return_field_is_checked`.
#[test]
fn every_body_returned_field_is_checked() {
    let frame = child_bytes(&ChildFrame::BodyReturned(BodyReturnedFrame {
        task_id: task_id(T1),
        returned: returned_result(),
        left: LoopHoldsTasks::new(NonZeroU64::new(2).unwrap(), names(&["flush", "keepalive"]))
            .unwrap(),
        started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    }));
    let mut cases: Vec<(&str, Option<Value>)> = vec![("task_id", Some(Value::Null))];
    cases.extend(
        bad_returned()
            .into_iter()
            .map(|value| ("returned", Some(value))),
    );
    cases.extend(bad_held().into_iter().map(|value| ("left", Some(value))));
    cases.extend(
        bad_start_time()
            .into_iter()
            .map(|value| ("started_at", value)),
    );
    assert_each_change_refused(&frame, &cases);
}

/// Python `TestLeftWorkFrames::test_every_end_field_is_checked`.
#[test]
fn every_left_work_end_field_is_checked() {
    let frame = child_bytes(&ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
        task_id: task_id(T1),
        end: LeftWorkEnd::InGrace,
        rss: rss(1.0),
    }));
    assert_each_change_refused(
        &frame,
        &[
            ("task_id", Some(Value::Null)),
            ("end", Some(Value::Null)),
            ("end", Some(json!("stuck"))),
            ("rss_mb", Some(json!("x"))),
            ("rss_mb", Some(json!(true))),
        ],
    );
}

fn left_work_stuck_frame() -> ChildFrame {
    ChildFrame::LeftWorkStuck(LeftWorkStuckFrame {
        task_id: task_id(T1),
        still: LoopHoldsTasks::new(NonZeroU64::new(1).unwrap(), names(&["a"])).unwrap(),
    })
}

/// Python `TestLeftWorkFrames::test_every_stuck_field_is_checked`.
#[test]
fn every_left_work_stuck_field_is_checked() {
    let mut cases: Vec<(&str, Option<Value>)> = vec![("task_id", Some(Value::Null))];
    cases.extend(bad_held().into_iter().map(|value| ("still", Some(value))));
    assert_each_change_refused(&child_bytes(&left_work_stuck_frame()), &cases);
}

/// Python `TestLeftWorkFrames::test_a_report_after_the_return_carries_no_result`.
#[test]
fn a_report_after_the_return_carries_no_body() {
    let ended = ChildFrame::LeftWorkEnded(LeftWorkEndedFrame {
        task_id: task_id(T1),
        end: LeftWorkEnd::Cancelled,
        rss: rss(1.0),
    });
    for (frame, kind) in [
        (ended, "left_work_ended"),
        (left_work_stuck_frame(), "left_work_stuck"),
    ] {
        let (meta, _) = meta_text_and_body(&child_bytes(&frame));
        assert_eq!(
            malformed_child(&framed(&meta, br#"{"x":1}"#)),
            MalformedFrame::UnexpectedBody {
                kind,
                body_bytes: 7,
            },
        );
    }
}

/// Python `TestChildFrames::test_ready_frame_round_trip`.
#[test]
fn the_ready_frame_carries_each_rss_reading() {
    for reading in [rss(12.5), rss(0.0), RssReading::Unreadable] {
        let frame = ChildFrame::Ready(ReadyFrame {
            pid: ProcessId::new(42).unwrap(),
            rss: reading,
        });
        assert_eq!(decode_child_bytes(&child_bytes(&frame)).unwrap(), frame);
    }
}

/// Python `TestChildFrames::test_invalid_utf8_body_is_malformed`.
#[test]
fn a_result_body_that_is_not_utf8_is_refused() {
    let frame = child_bytes(&ChildFrame::Result(ResultFrame {
        task_id: task_id(T1),
        end: RunEnd::Result(envelope("x")),
        started_at: crate::events::AttemptStart::Reported {
            started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        },
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    }));
    let (meta, _) = meta_text_and_body(&frame);
    assert_eq!(
        malformed_child(&framed(&meta, &[0xff])),
        MalformedFrame::BodyNotUtf8 { valid_up_to: 0 },
    );
}

/// Python `TestChildFrames::test_boot_failed_frame_round_trip`: every cause a
/// child reports, with any detail text.
#[test]
fn every_boot_cause_round_trips_with_any_detail() {
    let mut causes = vec![
        ReportedBootCause::AppLoadFailed,
        ReportedBootCause::ChildPoolSetupFailed,
        ReportedBootCause::TaskLoopCreateFailed,
        ReportedBootCause::ThreadStartRefused,
    ];
    causes.extend(ShortageErrno::ALL.map(ReportedBootCause::ResourceShortage));
    for cause in causes {
        for detail in ["[Errno 24] Too many open files", "", "\u{e9} \u{0}"] {
            let frame = ChildFrame::BootFailed(BootFailedFrame {
                cause,
                error_name: BootErrorName::new("OSError".to_owned()).unwrap(),
                detail: BootDetail::new(detail.to_owned()).unwrap(),
            });
            let bytes = child_bytes(&frame);
            let (_, body) = meta_text_and_body(&bytes);
            assert!(body.is_empty());
            assert_eq!(
                decode_child_bytes(&bytes).unwrap(),
                frame,
                "{cause:?} {detail:?}"
            );
        }
    }
}

/// Python `TestMalformedMetadata::test_both_decoders_refuse`.
#[test]
fn both_decoders_refuse_metadata_that_is_not_a_frame() {
    for meta in [
        "",
        "{",
        "[]",
        "1",
        "{}",
        r#"{"kind":1}"#,
        r#"{"kind":"unknown"}"#,
    ] {
        assert!(
            matches!(
                decode_child_frame(meta.as_bytes(), Vec::new()),
                Err(ChildFrameRefusal::Malformed(_))
            ),
            "child {meta:?}"
        );
        assert!(
            matches!(
                decode_parent_frame(meta.as_bytes(), Vec::new()),
                Err(ParentFrameRefusal::Malformed(_))
            ),
            "parent {meta:?}"
        );
    }
}

/// Python `TestMalformedMetadata::test_every_result_field_is_checked`. A
/// result frame is the end of a run that left no work on its task loop: work
/// left on the loop travels in a `body_returned` frame.
#[test]
fn every_result_field_is_checked() {
    let frame = child_bytes(&ChildFrame::Result(ResultFrame {
        task_id: task_id(T1),
        end: RunEnd::Result(envelope("x")),
        started_at: crate::events::AttemptStart::Reported {
            started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        },
        rss: rss(1.0),
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    }));
    assert_each_change_refused(
        &frame,
        &[
            ("task_id", Some(Value::Null)),
            (
                "end",
                Some(json!({"kind": "stopped_before_running", "reason": 1})),
            ),
            ("end", Some(json!({"kind": "worker_failure", "detail": 1}))),
            ("rss_mb", Some(json!("x"))),
            ("rss_mb", Some(json!(true))),
            ("task_loop", Some(Value::Null)),
            ("task_loop", Some(json!({"kind": "unknown"}))),
            (
                "task_loop",
                Some(json!({"kind": "held", "count": 1, "names": ["a"]})),
            ),
            (
                "task_loop",
                Some(json!({"kind": "unavailable", "detail": "OSError"})),
            ),
            ("child", None),
            ("child", Some(json!("retires"))),
            ("child", Some(json!(1))),
            // A result envelope comes after the start write.
            ("started_at", Some(json!({"kind": "not_reported"}))),
        ],
    );
    let cases: Vec<(&str, Option<Value>)> = bad_start()
        .into_iter()
        .map(|value| ("started_at", value))
        .collect();
    assert_each_change_refused(&frame, &cases);
}

/// Python `TestMalformedMetadata::test_every_boot_failed_field_is_checked`.
#[test]
fn every_boot_failed_field_is_checked() {
    let frame = child_bytes(&ChildFrame::BootFailed(BootFailedFrame {
        cause: ReportedBootCause::ResourceShortage(ShortageErrno::Emfile),
        error_name: BootErrorName::new("OSError".to_owned()).unwrap(),
        detail: BootDetail::new("no descriptor".to_owned()).unwrap(),
    }));
    assert_each_change_refused(
        &frame,
        &[
            ("error_name", Some(Value::Null)),
            ("error_name", Some(json!(24))),
            (
                "error_name",
                Some(json!("E".repeat(MAX_BOOT_ERROR_NAME_CHARS + 1))),
            ),
            ("detail", Some(Value::Null)),
            ("detail", Some(json!([]))),
            ("detail", Some(json!("d".repeat(MAX_BOOT_DETAIL_CHARS + 1)))),
            ("cause", Some(Value::Null)),
            ("cause", Some(json!("resource_shortage"))),
            ("cause", Some(json!({"kind": "unknown"}))),
            (
                "cause",
                Some(json!({"kind": "resource_shortage", "errno": "ENOENT"})),
            ),
            ("cause", Some(json!({"kind": "resource_shortage"}))),
            ("cause", Some(json!({"kind": "spawn_failed"}))),
        ],
    );
}

/// Python `TestStrictJson::test_non_finite_number_on_the_wire_is_malformed`.
#[test]
fn a_number_that_is_not_finite_is_not_json() {
    let decoded = decode_child_frame(
        br#"{"kind":"ready","protocol":3,"pid":1,"rss_mb":NaN}"#,
        Vec::new(),
    );
    assert!(
        matches!(
            decoded,
            Err(ChildFrameRefusal::Malformed(
                MalformedFrame::MetadataNotJson(_)
            ))
        ),
        "{decoded:?}"
    );
}

/// Python `TestPreloadFrames::test_bad_metadata_is_refused`.
#[test]
fn bad_preload_metadata_is_refused() {
    for meta in [
        "",
        "{",
        r#"{"kind":"unknown"}"#,
        r#"{"kind":"child_forked"}"#,
        r#"{"kind":"child_forked","pid":"1"}"#,
        r#"{"kind":"fork_failed","reason":1}"#,
    ] {
        assert!(
            decode_preload_frame(meta.as_bytes(), b"").is_err(),
            "{meta:?}"
        );
    }
}
