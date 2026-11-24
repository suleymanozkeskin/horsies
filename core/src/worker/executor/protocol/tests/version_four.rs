//! The content that version 4 adds: the operator cancel of a run. Pinned
//! bytes for `cancel_run`, `cancel_fired`, the seat end `cancelled` and
//! `run_cancelled`, a refusal for each malformed new field, and the named
//! mismatch of a version-3 peer.

use std::num::NonZeroU64;

use serde_json::{json, Value};

use super::*;

/// The frame refused for the metadata `meta` and `body`.
fn malformed_child_value(meta: &Value, body: &[u8]) -> MalformedFrame {
    malformed_child(&framed(&meta.to_string(), body))
}

fn malformed_parent_value(meta: &Value, body: &[u8]) -> MalformedFrame {
    malformed_parent(&framed(&meta.to_string(), body))
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

#[test]
fn this_build_speaks_version_four() {
    assert_eq!(PROTOCOL_VERSION, 4);
}

/// A version-3 child: its ready frame is named as a mismatch.
#[test]
fn a_version_three_ready_frame_is_a_protocol_mismatch() {
    let ready = framed(
        r#"{"kind":"ready","protocol":3,"pid":4242,"rss_mb":27.25}"#,
        b"",
    );
    assert_eq!(
        decode_child_bytes(&ready),
        Err(ChildFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::Named(3),
        })),
    );
}

/// A version-3 parent: its init frame is named as a mismatch.
#[test]
fn a_version_three_init_frame_is_a_protocol_mismatch() {
    let init = framed(
        concat!(
            r#"{"kind":"init","protocol":3,"program":{"kind":"rust"},"log_level":"info","#,
            r#""database_url":"postgresql://u@h/db","#,
            r#""connection":{"tcp_keepalives":{"kind":"off"},"#,
            r#""tcp_user_timeout":{"kind":"system_default"},"server_sessions":{"kind":"kept"}},"#,
            r#""process_name":"horsies-child","#,
            r#""child_kind":{"kind":"isolated","pool":{"min_size":0,"max_size":1,"#,
            r#""check":true},"task_send_pool":2}}"#,
        ),
        b"",
    );
    assert_eq!(
        decode_parent_bytes(&init),
        Err(ParentFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::Named(3),
        })),
    );
}

// ---------------------------------------------------------------------------
// cancel_run
// ---------------------------------------------------------------------------

#[test]
fn the_cancel_of_an_isolated_run_names_its_task() {
    assert_parent_bytes(
        &ParentFrame::CancelRun(CancelTarget::Isolated {
            task_id: task_id(T1),
        }),
        &framed(
            concat!(
                r#"{"kind":"cancel_run","target":{"kind":"isolated","#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000001"}}"#,
            ),
            b"",
        ),
    );
}

#[test]
fn the_cancel_of_a_shared_run_names_its_dispatch_and_task() {
    assert_parent_bytes(
        &ParentFrame::CancelRun(CancelTarget::Shared(run_ref(7, T2))),
        &framed(
            concat!(
                r#"{"kind":"cancel_run","target":{"kind":"shared","dispatch":7,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000002"}}"#,
            ),
            b"",
        ),
    );
}

fn cancel_run_meta(target: &Value) -> Value {
    json!({"kind": "cancel_run", "target": target})
}

#[test]
fn each_malformed_cancel_run_is_refused_with_its_field() {
    let isolated = json!({"kind": "isolated", "task_id": T1});
    let cases: Vec<(Value, &[u8], MalformedFrame)> = vec![
        (
            json!({"kind": "cancel_run"}),
            b"",
            MalformedFrame::MissingField {
                field: "target".to_owned(),
            },
        ),
        (
            json!({"kind": "cancel_run", "target": isolated, "reason": "operator"}),
            b"",
            MalformedFrame::UnknownField {
                field: "reason".to_owned(),
            },
        ),
        (
            cancel_run_meta(&isolated),
            b"{}",
            MalformedFrame::UnexpectedBody {
                kind: "cancel_run",
                body_bytes: 2,
            },
        ),
        (
            cancel_run_meta(&json!({"kind": "thread", "task_id": T1})),
            b"",
            MalformedFrame::UnknownKind {
                field: "target.kind".to_owned(),
                kind: BoundedText::cut("thread"),
            },
        ),
        (
            cancel_run_meta(&json!({"kind": "shared", "task_id": T1})),
            b"",
            MalformedFrame::MissingField {
                field: "target.dispatch".to_owned(),
            },
        ),
        (
            cancel_run_meta(&json!({"kind": "isolated", "task_id": T1, "dispatch": 1})),
            b"",
            MalformedFrame::UnknownField {
                field: "target.dispatch".to_owned(),
            },
        ),
        (
            cancel_run_meta(&json!({"kind": "shared", "dispatch": 0, "task_id": T1})),
            b"",
            MalformedFrame::OutOfBounds {
                field: "target.dispatch".to_owned(),
            },
        ),
    ];
    for (meta, body, expected) in cases {
        assert_eq!(malformed_parent_value(&meta, body), expected, "{meta}");
    }
}

// ---------------------------------------------------------------------------
// cancel_fired
// ---------------------------------------------------------------------------

#[test]
fn the_cancel_fired_report_names_its_run() {
    assert_child_bytes(
        &ChildFrame::CancelFired(run_ref(7, T2)),
        &framed(
            concat!(
                r#"{"kind":"cancel_fired","dispatch":7,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000002"}"#,
            ),
            b"",
        ),
    );
}

#[test]
fn a_cancel_fired_report_with_another_field_or_a_body_is_refused() {
    let meta = json!({"kind": "cancel_fired", "dispatch": 7, "task_id": T2});
    assert_eq!(
        malformed_child_value(&with_key(&meta, "end", json!("ran")), b""),
        MalformedFrame::UnknownField {
            field: "end".to_owned(),
        },
    );
    assert_eq!(
        malformed_child_value(&meta, b"x"),
        MalformedFrame::UnexpectedBody {
            kind: "run report",
            body_bytes: 1,
        },
    );
}

fn with_key(meta: &Value, key: &str, value: Value) -> Value {
    let mut meta = meta.clone();
    let _replaced = meta.as_object_mut().unwrap().insert(key.to_owned(), value);
    meta
}

// ---------------------------------------------------------------------------
// The seat end `cancelled`
// ---------------------------------------------------------------------------

#[test]
fn a_cancelled_seat_end_names_whether_the_body_ran() {
    for (body, text) in [
        (CancelledBody::Ran, "ran"),
        (CancelledBody::NotStarted, "not_started"),
    ] {
        assert_child_bytes(
            &ChildFrame::SharedResult(SharedResultFrame {
                run: run_ref(7, T2),
                end: SeatEnd::Cancelled(body),
                started_at: reported(),
                rss: rss(41.5),
                child: ChildAfterFrame::ServesOn,
            }),
            &framed(
                &format!(
                    concat!(
                        r#"{{"kind":"shared_result","dispatch":7,"#,
                        r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000002","#,
                        r#""end":{{"kind":"cancelled","body":"{}"}},"started_at":{},"#,
                        r#""rss_mb":41.5,"child":"serves_on"}}"#,
                    ),
                    text, REPORTED,
                ),
                b"",
            ),
        );
    }
}

fn cancelled_result_meta(end: &Value, started_at: &Value) -> Value {
    json!({
        "kind": "shared_result",
        "dispatch": 7,
        "task_id": T2,
        "end": end,
        "started_at": started_at,
        "rss_mb": null,
        "child": "serves_on",
    })
}

#[test]
fn a_cancelled_seat_end_reports_its_start() {
    assert_eq!(
        malformed_child_value(
            &cancelled_result_meta(
                &json!({"kind": "cancelled", "body": "ran"}),
                &json!({"kind": "not_reported"}),
            ),
            b"",
        ),
        MalformedFrame::BrokenRule {
            field: "started_at".to_owned(),
            rule: "a cancelled run had its start write returned, so it reports its start",
        },
    );
}

#[test]
fn each_malformed_cancelled_seat_end_is_refused_with_its_field() {
    let started_at = json!({"kind": "reported", "time": "2026-11-16T09:30:01.250Z"});
    let cases: Vec<(Value, &[u8], MalformedFrame)> = vec![
        (
            json!({"kind": "cancelled"}),
            b"",
            MalformedFrame::MissingField {
                field: "end.body".to_owned(),
            },
        ),
        (
            json!({"kind": "cancelled", "body": "dropped"}),
            b"",
            MalformedFrame::BrokenRule {
                field: "end.body".to_owned(),
                rule: "is not one of ran, not_started",
            },
        ),
        (
            json!({"kind": "cancelled", "body": "ran", "end": "x"}),
            b"",
            MalformedFrame::UnknownField {
                field: "end.end".to_owned(),
            },
        ),
        (
            json!({"kind": "cancelled", "body": "ran"}),
            b"{}",
            MalformedFrame::UnexpectedBody {
                kind: "cancelled",
                body_bytes: 2,
            },
        ),
    ];
    for (end, body, expected) in cases {
        assert_eq!(
            malformed_child_value(&cancelled_result_meta(&end, &started_at), body),
            expected,
            "{end}"
        );
    }
}

// ---------------------------------------------------------------------------
// run_cancelled
// ---------------------------------------------------------------------------

fn run_cancelled(body: CancelledBody, task_loop: CancelledLoop) -> ChildFrame {
    ChildFrame::RunCancelled(RunCancelledFrame {
        task_id: task_id(T1),
        body,
        started_at: start_time(),
        rss: rss(41.5),
        task_loop,
        child: ChildAfterFrame::ServesOn,
    })
}

#[test]
fn a_cancelled_isolated_run_reports_its_body_start_and_loop() {
    let held =
        LoopHoldsTasks::new(NonZeroU64::new(2).unwrap(), names(&["job_0", "job_1"])).unwrap();
    let cases = [
        (
            run_cancelled(
                CancelledBody::Ran,
                CancelledLoop::NoLeftWork(NoLeftWork::NoTaskLoop),
            ),
            r#""body":"ran","started_at":"2026-11-16T09:30:01.250Z","rss_mb":41.5,"task_loop":{"kind":"none"},"#,
        ),
        (
            run_cancelled(
                CancelledBody::NotStarted,
                CancelledLoop::NoLeftWork(NoLeftWork::LoopHoldsNoTask),
            ),
            r#""body":"not_started","started_at":"2026-11-16T09:30:01.250Z","rss_mb":41.5,"task_loop":{"kind":"empty"},"#,
        ),
        (
            run_cancelled(CancelledBody::Ran, CancelledLoop::WorkLeft(held)),
            r#""body":"ran","started_at":"2026-11-16T09:30:01.250Z","rss_mb":41.5,"task_loop":{"kind":"held","count":2,"names":["job_0","job_1"]},"#,
        ),
    ];
    for (frame, middle) in cases {
        assert_child_bytes(
            &frame,
            &framed(
                &format!(
                    concat!(
                        r#"{{"kind":"run_cancelled","#,
                        r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000001",{}"#,
                        r#""child":"serves_on"}}"#,
                    ),
                    middle
                ),
                b"",
            ),
        );
    }
}

fn run_cancelled_meta() -> Value {
    json!({
        "kind": "run_cancelled",
        "task_id": T1,
        "body": "ran",
        "started_at": "2026-11-16T09:30:01.250Z",
        "rss_mb": null,
        "task_loop": {"kind": "none"},
        "child": "serves_on",
    })
}

#[test]
fn each_malformed_run_cancelled_is_refused_with_its_field() {
    let meta = run_cancelled_meta();
    assert!(decode_child_bytes(&framed(&meta.to_string(), b"")).is_ok());
    let cases: Vec<(Value, &[u8], MalformedFrame)> = vec![
        (
            with_key(&meta, "body", json!("dropped")),
            b"",
            MalformedFrame::BrokenRule {
                field: "body".to_owned(),
                rule: "is not one of ran, not_started",
            },
        ),
        (
            with_key(&meta, "started_at", json!("yesterday")),
            b"",
            MalformedFrame::WrongType {
                field: "started_at".to_owned(),
                expected: ExpectedType::Rfc3339Time,
            },
        ),
        (
            with_key(
                &meta,
                "task_loop",
                json!({"kind": "held", "count": 2, "names": ["a"]}),
            ),
            b"",
            MalformedFrame::BrokenRule {
                field: "task_loop.names".to_owned(),
                rule: "must name min(count, 10) tasks",
            },
        ),
        (
            with_key(
                &meta,
                "task_loop",
                json!({"kind": "held", "count": 0, "names": []}),
            ),
            b"",
            MalformedFrame::OutOfBounds {
                field: "task_loop.count".to_owned(),
            },
        ),
        (
            with_key(&meta, "task_loop", json!({"kind": "none", "count": 1})),
            b"",
            MalformedFrame::UnknownField {
                field: "task_loop.count".to_owned(),
            },
        ),
        (
            with_key(&meta, "task_loop", json!({"kind": "full"})),
            b"",
            MalformedFrame::UnknownKind {
                field: "task_loop.kind".to_owned(),
                kind: BoundedText::cut("full"),
            },
        ),
        (
            with_key(&meta, "end", json!({"kind": "result"})),
            b"",
            MalformedFrame::UnknownField {
                field: "end".to_owned(),
            },
        ),
        (
            meta.clone(),
            b"{}",
            MalformedFrame::UnexpectedBody {
                kind: "run_cancelled",
                body_bytes: 2,
            },
        ),
    ];
    for (changed, body, expected) in cases {
        assert_eq!(malformed_child_value(&changed, body), expected, "{changed}");
    }
}
