//! The content that version 3 adds: the trace, the queue and the attempt of
//! the run frames, the start of the frames that end a run, the handler
//! kind, the `events` frame and the `handler_stats` frame. Pinned bytes for
//! each, a refusal for each malformed new field, and the named mismatch of
//! a version-2 peer.

use serde_json::{json, Value};

use super::*;
use crate::events::{decode_event, AttemptStart, RunTrace, TraceCarrierText};

/// A stored carrier, as the core's encoder writes it.
const CARRIER: &str =
    r#"{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}"#;

/// Two event wire forms of `core/tests/fixtures/event_wire/vectors.json`.
const CLAIM_RELEASED: &str = r#"["ClaimReleased",1700000000123456789,"0192f6a1-7b3c-7d4e-8f00-0123456789ab","send_email","default","QUEUE_FULL"]"#;
const SLOT_RECOVERED: &str = r#"["SlotRecovered",1700000000123456789,"SHARED_ASYNC",1]"#;

fn carrier_trace() -> RunTrace {
    RunTrace::Carrier {
        text: TraceCarrierText::new(CARRIER.to_owned()).unwrap(),
    }
}

/// The child frame refused for `meta` and `body`.
fn malformed_child_meta(meta: &Value, body: &[u8]) -> MalformedFrame {
    malformed_child(&framed(&meta.to_string(), body))
}

/// The parent frame refused for `meta` and `body`.
fn malformed_parent_meta(meta: &Value, body: &[u8]) -> MalformedFrame {
    malformed_parent(&framed(&meta.to_string(), body))
}

/// `meta` with `key` set to `value`, or without `key` for `None`.
fn with_field(meta: &Value, key: &str, value: Option<Value>) -> Value {
    let mut meta = meta.clone();
    let object = meta.as_object_mut().unwrap();
    match value {
        Some(value) => {
            let _replaced = object.insert(key.to_owned(), value);
        }
        None => {
            let _removed = object.shift_remove(key);
        }
    }
    meta
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

/// A version-2 child: its ready frame is named as a mismatch.
#[test]
fn a_version_two_ready_frame_is_a_protocol_mismatch() {
    let ready = framed(
        r#"{"kind":"ready","protocol":2,"pid":4242,"rss_mb":27.25}"#,
        b"",
    );
    assert_eq!(
        decode_child_bytes(&ready),
        Err(ChildFrameRefusal::ProtocolMismatch(ProtocolMismatch {
            received: ReceivedProtocol::Named(2),
        })),
    );
}

/// A version-2 parent: its init frame is named as a mismatch.
#[test]
fn a_version_two_init_frame_is_a_protocol_mismatch() {
    let init = framed(
        concat!(
            r#"{"kind":"init","protocol":2,"program":{"kind":"rust"},"log_level":"info","#,
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
            received: ReceivedProtocol::Named(2),
        })),
    );
}

// ---------------------------------------------------------------------------
// The trace of the run frames
// ---------------------------------------------------------------------------

#[test]
fn the_run_frames_carry_the_stored_trace_carrier() {
    assert_parent_bytes(
        &ParentFrame::Run(RunFrame {
            task_id: task_id(T5),
            task_name: "app.tasks.add".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            worker_id: "worker-1".to_owned(),
            claimed_at: whole_second_claim_time(),
            orphan_self_heal: OrphanSelfHeal::Off,
            work: IsolatedWork::Run,
            trace: carrier_trace(),
            input: TaskInputText::new("{}".to_owned()),
        }),
        &framed(
            concat!(
                r#"{"kind":"run","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.add","queue_name":"default","attempt":1,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"#,
                r#""work":{"kind":"run"},"#,
                r#""trace":{"kind":"carrier","text":"#,
                r#""{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}"}}"#,
            ),
            b"{}",
        ),
    );
    assert_parent_bytes(
        &ParentFrame::SharedRun(SharedRunFrame {
            dispatch: DispatchNumber::new(4).unwrap(),
            task_id: task_id(T5),
            task_name: "app.tasks.fetch".to_owned(),
            queue_name: default_queue(),
            attempt: AttemptNumber::SMALLEST,
            worker_id: "worker-1".to_owned(),
            claimed_at: whole_second_claim_time(),
            orphan_self_heal: OrphanSelfHeal::On,
            work: SharedWork::ThreadTask {
                timeout_ms: TaskTimeoutMs::new(5_000).unwrap(),
            },
            trace: carrier_trace(),
            input: TaskInputText::new("{}".to_owned()),
        }),
        &framed(
            concat!(
                r#"{"kind":"shared_run","dispatch":4,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.fetch","queue_name":"default","attempt":1,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":true,"#,
                r#""work":{"kind":"thread_task","timeout_ms":5000},"#,
                r#""trace":{"kind":"carrier","text":"#,
                r#""{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}"}}"#,
            ),
            b"{}",
        ),
    );
}

/// The metadata of an isolated run frame with no `trace`.
fn run_meta_without_trace() -> Value {
    json!({
        "kind": "run",
        "task_id": T1,
        "task_name": "t",
        "queue_name": "default",
        "attempt": 1,
        "worker_id": "w",
        "claimed_at": "2026-11-16T09:30:00Z",
        "orphan_self_heal": false,
        "work": {"kind": "run"},
    })
}

#[test]
fn each_malformed_trace_is_refused_with_its_field() {
    let over_bound = format!(
        r#"{{"k":"{}"}}"#,
        "v".repeat(crate::events::MAX_EVENT_TRACE_CARRIER_CHARS)
    );
    let cases: Vec<(Option<Value>, MalformedFrame)> = vec![
        (
            None,
            MalformedFrame::MissingField {
                field: "trace".to_owned(),
            },
        ),
        (
            Some(Value::Null),
            MalformedFrame::WrongType {
                field: "trace".to_owned(),
                expected: ExpectedType::Object,
            },
        ),
        (
            Some(json!({"kind": "remote"})),
            MalformedFrame::UnknownKind {
                field: "trace.kind".to_owned(),
                kind: BoundedText::cut("remote"),
            },
        ),
        (
            Some(json!({"kind": "no_carrier", "text": "{}"})),
            MalformedFrame::UnknownField {
                field: "trace.text".to_owned(),
            },
        ),
        (
            Some(json!({"kind": "carrier"})),
            MalformedFrame::MissingField {
                field: "trace.text".to_owned(),
            },
        ),
        (
            Some(json!({"kind": "carrier", "text": 5})),
            MalformedFrame::WrongType {
                field: "trace.text".to_owned(),
                expected: ExpectedType::Text,
            },
        ),
        (
            Some(json!({"kind": "carrier", "text": over_bound})),
            MalformedFrame::OutOfBounds {
                field: "trace.text".to_owned(),
            },
        ),
    ];
    for (trace, expected) in cases {
        let meta = with_field(&run_meta_without_trace(), "trace", trace.clone());
        assert_eq!(malformed_parent_meta(&meta, b"{}"), expected, "{trace:?}");
    }
}

/// The carrier text is one JSON object of texts, the form that the send
/// stores: anything else is refused, so a child never reads a carrier that
/// its propagator cannot take.
#[test]
fn a_carrier_text_that_is_not_an_object_of_texts_is_refused() {
    for text in [
        "[1]",
        r#"{"a":1}"#,
        r#"{"a":"1","a":"2"}"#,
        r#"{"a":"1""#,
        "",
    ] {
        let meta = with_field(
            &run_meta_without_trace(),
            "trace",
            Some(json!({"kind": "carrier", "text": text})),
        );
        assert_eq!(
            malformed_parent_meta(&meta, b"{}"),
            MalformedFrame::BrokenRule {
                field: "trace.text".to_owned(),
                rule: "is not one JSON object of texts",
            },
            "{text:?}"
        );
    }
}

#[test]
fn a_shared_run_frame_without_its_trace_is_refused() {
    let meta = json!({
        "kind": "shared_run",
        "dispatch": 1,
        "task_id": T1,
        "task_name": "t",
        "queue_name": "default",
        "attempt": 1,
        "worker_id": "w",
        "claimed_at": "2026-11-16T09:30:00Z",
        "orphan_self_heal": false,
        "work": {"kind": "thread_task", "timeout_ms": 5000},
    });
    assert_eq!(
        malformed_parent_meta(&meta, b"{}"),
        MalformedFrame::MissingField {
            field: "trace".to_owned(),
        },
    );
}

// ---------------------------------------------------------------------------
// The queue and the attempt of the run frames
// ---------------------------------------------------------------------------

#[test]
fn the_run_frames_carry_the_queue_and_the_attempt() {
    assert_parent_bytes(
        &ParentFrame::Run(RunFrame {
            task_id: task_id(T5),
            task_name: "app.tasks.add".to_owned(),
            queue_name: QueueName::new("emails".to_owned()).unwrap(),
            attempt: AttemptNumber::try_from(3).unwrap(),
            worker_id: "worker-1".to_owned(),
            claimed_at: whole_second_claim_time(),
            orphan_self_heal: OrphanSelfHeal::Off,
            work: IsolatedWork::Run,
            trace: RunTrace::NoCarrier,
            input: TaskInputText::new("{}".to_owned()),
        }),
        &framed(
            concat!(
                r#"{"kind":"run","task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.add","queue_name":"emails","attempt":3,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"#,
                r#""work":{"kind":"run"},"trace":{"kind":"no_carrier"}}"#,
            ),
            b"{}",
        ),
    );
    assert_parent_bytes(
        &ParentFrame::SharedRun(SharedRunFrame {
            dispatch: DispatchNumber::new(4).unwrap(),
            task_id: task_id(T5),
            task_name: "app.tasks.fetch".to_owned(),
            queue_name: QueueName::new("emails".to_owned()).unwrap(),
            attempt: AttemptNumber::try_from(2).unwrap(),
            worker_id: "worker-1".to_owned(),
            claimed_at: whole_second_claim_time(),
            orphan_self_heal: OrphanSelfHeal::Off,
            work: SharedWork::ThreadTask {
                timeout_ms: TaskTimeoutMs::new(5_000).unwrap(),
            },
            trace: RunTrace::NoCarrier,
            input: TaskInputText::new("{}".to_owned()),
        }),
        &framed(
            concat!(
                r#"{"kind":"shared_run","dispatch":4,"#,
                r#""task_id":"0190f5a2-7b1c-7d3e-8f00-000000000005","#,
                r#""task_name":"app.tasks.fetch","queue_name":"emails","attempt":2,"#,
                r#""worker_id":"worker-1","#,
                r#""claimed_at":"2026-11-16T09:30:00Z","orphan_self_heal":false,"#,
                r#""work":{"kind":"thread_task","timeout_ms":5000},"#,
                r#""trace":{"kind":"no_carrier"}}"#,
            ),
            b"{}",
        ),
    );
}

/// The largest attempt and a queue name of 52 characters round trip in both
/// run frames.
#[test]
fn the_largest_attempt_and_queue_name_round_trip_in_both_run_frames() {
    let queue_name = QueueName::new("q".repeat(crate::events::MAX_EVENT_QUEUE_NAME_CHARS)).unwrap();
    let run = ParentFrame::Run(RunFrame {
        task_id: task_id(T5),
        task_name: "t".to_owned(),
        queue_name: queue_name.clone(),
        attempt: AttemptNumber::LARGEST,
        worker_id: "w".to_owned(),
        claimed_at: claim_time(),
        orphan_self_heal: OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        trace: RunTrace::NoCarrier,
        input: TaskInputText::new("{}".to_owned()),
    });
    let shared = ParentFrame::SharedRun(SharedRunFrame {
        dispatch: DispatchNumber::new(3).unwrap(),
        task_id: task_id(T5),
        task_name: "t".to_owned(),
        queue_name,
        attempt: AttemptNumber::LARGEST,
        worker_id: "w".to_owned(),
        claimed_at: claim_time(),
        orphan_self_heal: OrphanSelfHeal::Off,
        work: SharedWork::ThreadTask {
            timeout_ms: TaskTimeoutMs::new(5_000).unwrap(),
        },
        trace: RunTrace::NoCarrier,
        input: TaskInputText::new("{}".to_owned()),
    });
    for frame in [run, shared] {
        let bytes = encode_parent_frame(&frame).unwrap().to_bytes();
        assert_eq!(decode_parent_bytes(&bytes).unwrap(), frame);
    }
}

/// The metadata of an isolated run frame with every field.
fn run_meta() -> Value {
    with_field(
        &run_meta_without_trace(),
        "trace",
        Some(json!({"kind": "no_carrier"})),
    )
}

/// The metadata of a shared run frame with every field.
fn shared_run_meta() -> Value {
    json!({
        "kind": "shared_run",
        "dispatch": 1,
        "task_id": T1,
        "task_name": "t",
        "queue_name": "default",
        "attempt": 1,
        "worker_id": "w",
        "claimed_at": "2026-11-16T09:30:00Z",
        "orphan_self_heal": false,
        "work": {"kind": "thread_task", "timeout_ms": 5000},
        "trace": {"kind": "no_carrier"},
    })
}

/// Each refusal of `cases`, for the field `key` of both run frames.
fn assert_run_field_refusals(key: &str, cases: &[(Option<Value>, MalformedFrame)]) {
    for meta in [run_meta(), shared_run_meta()] {
        for (value, expected) in cases {
            let changed = with_field(&meta, key, value.clone());
            assert_eq!(
                &malformed_parent_meta(&changed, b"{}"),
                expected,
                "{key}: {value:?} in {}",
                meta["kind"]
            );
        }
    }
}

#[test]
fn each_malformed_attempt_is_refused_with_its_field() {
    let field = || "attempt".to_owned();
    let past_the_largest = u64::from(crate::events::MAX_ATTEMPT) + 1;
    assert_run_field_refusals(
        "attempt",
        &[
            (None, MalformedFrame::MissingField { field: field() }),
            (
                Some(json!(0)),
                MalformedFrame::OutOfBounds { field: field() },
            ),
            (
                Some(json!(-1)),
                MalformedFrame::OutOfBounds { field: field() },
            ),
            (
                Some(json!(past_the_largest)),
                MalformedFrame::OutOfBounds { field: field() },
            ),
            (
                Some(json!("1")),
                MalformedFrame::WrongType {
                    field: field(),
                    expected: ExpectedType::WholeNumber,
                },
            ),
            (
                Some(json!(1.5)),
                MalformedFrame::WrongType {
                    field: field(),
                    expected: ExpectedType::WholeNumber,
                },
            ),
        ],
    );
}

#[test]
fn each_malformed_queue_name_is_refused_with_its_field() {
    let field = || "queue_name".to_owned();
    let over_bound = "q".repeat(crate::events::MAX_EVENT_QUEUE_NAME_CHARS + 1);
    assert_run_field_refusals(
        "queue_name",
        &[
            (None, MalformedFrame::MissingField { field: field() }),
            (
                Some(json!(over_bound)),
                MalformedFrame::OutOfBounds { field: field() },
            ),
            (
                Some(json!(5)),
                MalformedFrame::WrongType {
                    field: field(),
                    expected: ExpectedType::Text,
                },
            ),
        ],
    );
}

// ---------------------------------------------------------------------------
// The start of the frames that end a run
// ---------------------------------------------------------------------------

/// The metadata of a result frame that ends with `end` and starts with
/// `started_at`.
fn result_meta(end: &Value, started_at: &Value) -> Value {
    json!({
        "kind": "result",
        "task_id": T1,
        "end": end,
        "started_at": started_at,
        "rss_mb": null,
        "task_loop": {"kind": "none"},
        "child": "serves_on",
    })
}

/// The metadata of a shared result frame that ends with `end` and starts
/// with `started_at`.
fn shared_result_meta(end: &Value, started_at: &Value) -> Value {
    json!({
        "kind": "shared_result",
        "dispatch": 1,
        "task_id": T1,
        "end": end,
        "started_at": started_at,
        "rss_mb": null,
        "child": "serves_on",
    })
}

fn stopped_end() -> Value {
    json!({"kind": "stopped_before_running", "reason": "CLAIM_LOST"})
}

fn reported_start() -> Value {
    json!({"kind": "reported", "time": "2026-11-16T09:30:01.250Z"})
}

/// A stop before running has no start; a result envelope reports its
/// start. Each other combination is refused, in both result frames.
#[test]
fn a_start_that_does_not_fit_the_end_is_refused() {
    let no_start = MalformedFrame::BrokenRule {
        field: "started_at".to_owned(),
        rule: "a run that stopped before running has no start",
    };
    let start_owed = MalformedFrame::BrokenRule {
        field: "started_at".to_owned(),
        rule: "a run that ended with a result envelope reports its start",
    };
    let not_reported = json!({"kind": "not_reported"});
    let result_end = json!({"kind": "result"});
    assert_eq!(
        malformed_child_meta(&result_meta(&stopped_end(), &reported_start()), b""),
        no_start
    );
    assert_eq!(
        malformed_child_meta(&shared_result_meta(&stopped_end(), &reported_start()), b""),
        no_start
    );
    assert_eq!(
        malformed_child_meta(&result_meta(&result_end, &not_reported), b"{}"),
        start_owed
    );
    assert_eq!(
        malformed_child_meta(&shared_result_meta(&result_end, &not_reported), b"{}"),
        start_owed
    );
}

/// A worker failure may come before or after the start write, and a timed
/// out run reports its start or not: each combination reads.
#[test]
fn a_worker_failure_and_a_timeout_take_either_start() {
    let failure = json!({"kind": "worker_failure", "detail": "x"});
    for started_at in [reported_start(), json!({"kind": "not_reported"})] {
        let result = result_meta(&failure, &started_at);
        assert!(
            decode_child_bytes(&framed(&result.to_string(), b"")).is_ok(),
            "{result}"
        );
        let timed_out = shared_result_meta(&json!({"kind": "timed_out"}), &started_at);
        assert!(
            decode_child_bytes(&framed(&timed_out.to_string(), b"")).is_ok(),
            "{timed_out}"
        );
    }
}

#[test]
fn each_malformed_start_is_refused_with_its_field() {
    let result = result_meta(&json!({"kind": "result"}), &reported_start());
    let cases: Vec<(Option<Value>, MalformedFrame)> = vec![
        (
            None,
            MalformedFrame::MissingField {
                field: "started_at".to_owned(),
            },
        ),
        (
            Some(json!("2026-11-16T09:30:01Z")),
            MalformedFrame::WrongType {
                field: "started_at".to_owned(),
                expected: ExpectedType::Object,
            },
        ),
        (
            Some(json!({"kind": "started"})),
            MalformedFrame::UnknownKind {
                field: "started_at.kind".to_owned(),
                kind: BoundedText::cut("started"),
            },
        ),
        (
            Some(json!({"kind": "reported"})),
            MalformedFrame::MissingField {
                field: "started_at.time".to_owned(),
            },
        ),
        (
            Some(json!({"kind": "reported", "time": "yesterday"})),
            MalformedFrame::WrongType {
                field: "started_at.time".to_owned(),
                expected: ExpectedType::Rfc3339Time,
            },
        ),
        (
            Some(json!({"kind": "not_reported", "time": "2026-11-16T09:30:01Z"})),
            MalformedFrame::UnknownField {
                field: "started_at.time".to_owned(),
            },
        ),
    ];
    for (started_at, expected) in cases {
        let meta = with_field(&result, "started_at", started_at.clone());
        assert_eq!(
            malformed_child_meta(&meta, b"{}"),
            expected,
            "{started_at:?}"
        );
    }
}

/// A frame whose body ran carries the time of its start write, as RFC 3339
/// text: a missing or malformed time is refused with its field.
#[test]
fn a_body_that_ran_carries_the_time_of_its_start() {
    let returned = json!({
        "kind": "body_returned",
        "task_id": T1,
        "returned": {"kind": "result"},
        "started_at": "2026-11-16T09:30:01.250Z",
        "left": {"count": 1, "names": ["poll"]},
    });
    match decode_child_bytes(&framed(&returned.to_string(), b"{}")) {
        Ok(ChildFrame::BodyReturned(frame)) => assert_eq!(frame.started_at, start_time()),
        other => panic!("expected a body_returned frame, got {other:?}"),
    }
    assert_eq!(
        malformed_child_meta(&with_field(&returned, "started_at", None), b"{}"),
        MalformedFrame::MissingField {
            field: "started_at".to_owned(),
        },
    );
    assert_eq!(
        malformed_child_meta(
            &with_field(&returned, "started_at", Some(json!("yesterday"))),
            b"{}"
        ),
        MalformedFrame::WrongType {
            field: "started_at".to_owned(),
            expected: ExpectedType::Rfc3339Time,
        },
    );
}

/// The start of a run keeps every sub-second digit, so the parent reads the
/// exact time of the start write.
#[test]
fn a_reported_start_round_trips_exactly() {
    let started_at = start_time() + chrono::Duration::nanoseconds(789);
    let frame = ChildFrame::Result(ResultFrame {
        task_id: task_id(T1),
        end: RunEnd::Result(envelope("1")),
        started_at: AttemptStart::Reported { started_at },
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    });
    let bytes = encode_child_frame(&frame).unwrap().to_bytes();
    assert_eq!(decode_child_bytes(&bytes).unwrap(), frame);
}

// ---------------------------------------------------------------------------
// The handler kind
// ---------------------------------------------------------------------------

fn handler_init() -> InitFrame {
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
        process_name: "horsies-handlers".to_owned(),
        child_kind: ChildKind::Handler(HandlerChild {
            task_send_pool: TaskSendPool::new(2).unwrap(),
        }),
    }
}

#[test]
fn the_init_frame_of_a_handler_child() {
    assert_parent_bytes(
        &ParentFrame::Init(handler_init()),
        &framed(
            concat!(
                r#"{"kind":"init","protocol":4,"program":{"kind":"rust"},"log_level":"info","#,
                r#""database_url":"postgresql://u@h/db","#,
                r#""connection":{"tcp_keepalives":{"kind":"on","idle_seconds":30,"#,
                r#""interval_seconds":10,"probes":3},"tcp_user_timeout":{"kind":"after","#,
                r#""milliseconds":30000},"server_sessions":{"kind":"kept"}},"#,
                r#""process_name":"horsies-handlers","#,
                r#""child_kind":{"kind":"handler","task_send_pool":2}}"#,
            ),
            b"",
        ),
    );
}

#[test]
fn each_malformed_handler_kind_is_refused_with_its_field() {
    let bytes = encode_parent_frame(&ParentFrame::Init(handler_init()))
        .unwrap()
        .to_bytes();
    let (meta, _) = split(&bytes);
    let meta: Value = serde_json::from_slice(&meta).unwrap();
    let cases: Vec<(Value, MalformedFrame)> = vec![
        (
            json!({"kind": "handler"}),
            MalformedFrame::MissingField {
                field: "child_kind.task_send_pool".to_owned(),
            },
        ),
        (
            json!({"kind": "handler", "task_send_pool": 0}),
            MalformedFrame::OutOfBounds {
                field: "child_kind.task_send_pool".to_owned(),
            },
        ),
        (
            json!({"kind": "handler", "task_send_pool": 1, "seats": 4}),
            MalformedFrame::UnknownField {
                field: "child_kind.seats".to_owned(),
            },
        ),
    ];
    for (kind, expected) in cases {
        let changed = with_field(&meta, "child_kind", Some(kind.clone()));
        assert_eq!(malformed_parent_meta(&changed, b""), expected, "{kind}");
    }
}

// ---------------------------------------------------------------------------
// The events frame
// ---------------------------------------------------------------------------

/// The wire event of the wire text `text`.
fn wire_event(text: &str) -> WireEvent {
    let event = decode_event(&serde_json::from_str(text).unwrap()).unwrap();
    WireEvent::of(&event).unwrap()
}

#[test]
fn the_events_frame_carries_the_wire_form_of_each_event() {
    let events = [wire_event(CLAIM_RELEASED), wire_event(SLOT_RECOVERED)];
    assert_eq!(events[0].as_str(), CLAIM_RELEASED);
    assert_eq!(events[1].as_str(), SLOT_RECOVERED);
    let frame = ParentFrame::Events(EventsFrame {
        events: EventBatch::new(&events).unwrap(),
    });
    let body = format!("[{CLAIM_RELEASED},{SLOT_RECOVERED}]");
    assert_parent_bytes(&frame, &framed(r#"{"kind":"events"}"#, body.as_bytes()));
    match decode_parent_bytes(&framed(r#"{"kind":"events"}"#, body.as_bytes())) {
        Ok(ParentFrame::Events(decoded)) => assert_eq!(
            decoded.events.events().collect::<Vec<_>>(),
            vec![CLAIM_RELEASED, SLOT_RECOVERED]
        ),
        other => panic!("expected an events frame, got {other:?}"),
    }
}

/// The frame checks sizes only: an event that does not decode stays in the
/// batch, and the other events decode on their own.
#[test]
fn an_event_that_does_not_decode_leaves_the_others_to_their_own_decode() {
    let body = format!(r#"[["NoSuchEvent",1],{SLOT_RECOVERED}]"#);
    let decoded = match decode_parent_bytes(&framed(r#"{"kind":"events"}"#, body.as_bytes())) {
        Ok(ParentFrame::Events(decoded)) => decoded,
        other => panic!("expected an events frame, got {other:?}"),
    };
    let results: Vec<bool> = decoded
        .events
        .events()
        .map(|text| decode_event(&serde_json::from_str(text).unwrap()).is_ok())
        .collect();
    assert_eq!(results, vec![false, true]);
}

#[test]
fn each_malformed_events_frame_is_refused() {
    let events_meta = r#"{"kind":"events"}"#;
    let one_event = |bytes: usize| format!(r#"["{}"]"#, "e".repeat(bytes - 4));
    let too_many = format!("[{}]", vec![r#"["e"]"#; MAX_EVENTS_PER_FRAME + 1].join(","));
    let over_bound = format!("[{}]", one_event(MAX_EVENT_WIRE_BYTES + 1));
    let at_bound = format!("[{}]", one_event(MAX_EVENT_WIRE_BYTES));
    assert_eq!(
        malformed_parent(&framed(r#"{"kind":"events","count":1}"#, b"[[\"e\"]]")),
        MalformedFrame::UnknownField {
            field: "count".to_owned(),
        },
    );
    assert_eq!(
        malformed_parent(&framed(events_meta, &[0x5b, 0xff])),
        MalformedFrame::BodyNotUtf8 { valid_up_to: 1 },
    );
    assert!(matches!(
        malformed_parent(&framed(events_meta, b"[1,")),
        MalformedFrame::EventsBodyNotJson(_)
    ));
    assert_eq!(
        malformed_parent(&framed(events_meta, b"{}")),
        MalformedFrame::EventsBodyNotArray,
    );
    assert_eq!(
        malformed_parent(&framed(events_meta, b"[]")),
        MalformedFrame::EventCount { count: 0 },
    );
    assert_eq!(
        malformed_parent(&framed(events_meta, too_many.as_bytes())),
        MalformedFrame::EventCount {
            count: MAX_EVENTS_PER_FRAME + 1
        },
    );
    assert_eq!(
        malformed_parent(&framed(events_meta, over_bound.as_bytes())),
        MalformedFrame::EventOverBound {
            index: 0,
            bytes: MAX_EVENT_WIRE_BYTES + 1
        },
    );
    assert!(matches!(
        decode_parent_bytes(&framed(events_meta, at_bound.as_bytes())),
        Ok(ParentFrame::Events(_))
    ));
}

#[test]
fn a_batch_holds_one_to_the_most_events() {
    let event = wire_event(SLOT_RECOVERED);
    assert_eq!(
        EventBatch::new(&[]),
        Err(EventCountOutOfBounds { count: 0 })
    );
    let too_many = vec![event.clone(); MAX_EVENTS_PER_FRAME + 1];
    assert_eq!(
        EventBatch::new(&too_many),
        Err(EventCountOutOfBounds {
            count: MAX_EVENTS_PER_FRAME + 1
        })
    );
    let most = vec![event; MAX_EVENTS_PER_FRAME];
    assert_eq!(
        EventBatch::new(&most).unwrap().events().len(),
        MAX_EVENTS_PER_FRAME
    );
}

/// The wire form of an `AttemptEnded` of an attempt that the worker ended,
/// with the largest numbers, the longest names of each closed set, a task name of 255, a queue name of 52 and a worker id of 255
/// characters that JSON writes as 6 bytes each, and the internal carrier
/// `text`.
fn largest_attempt_ended(text: &str) -> crate::events::Event {
    let wire = json!([
        "AttemptEnded",
        i64::MAX,
        "0192f6a1-7b3c-7d4e-8f00-0123456789ab",
        "\u{1}".repeat(crate::events::MAX_EVENT_TASK_NAME_CHARS),
        "\u{1}".repeat(crate::events::MAX_EVENT_QUEUE_NAME_CHARS),
        "SHARED_THREAD",
        crate::events::MAX_ATTEMPT,
        [
            "DecidedByWorker",
            "NEIGHBOUR_BLOCKED_LOOP",
            "WORKFLOW_RECOVERY_LIMIT_REACHED",
            ["Carrier", text],
            i64::MAX,
            "SHARED_THREAD",
            "\u{1}".repeat(crate::events::MAX_EVENT_WORKER_ID_CHARS),
            "WORKFLOW_NOT_KNOWN"
        ],
        u64::MAX,
    ]);
    decode_event(&wire).unwrap()
}

/// A carrier that the send stores has at most
/// `MAX_TRACE_CARRIER_BYTES` bytes of printable ASCII, which JSON writes as
/// at most 2 bytes each: the largest event fits its bound.
#[test]
fn the_largest_event_of_a_stored_carrier_fits_its_bound() {
    let backslashes = "\\".repeat(crate::telemetry::trace_carrier::MAX_TRACE_CARRIER_BYTES);
    let event = WireEvent::of(&largest_attempt_ended(&backslashes)).unwrap();
    assert!(event.as_str().len() <= MAX_EVENT_WIRE_BYTES);
}

/// An event over its bound is refused with its type and its size; the
/// writer drops it and counts it.
#[test]
fn an_event_over_its_bound_is_refused() {
    let controls = "\u{0}".repeat(crate::events::MAX_EVENT_TRACE_CARRIER_CHARS);
    match WireEvent::of(&largest_attempt_ended(&controls)) {
        Err(WireEventRefusal::OverBound { event, bytes }) => {
            assert_eq!(event, "AttemptEnded");
            assert!(bytes > MAX_EVENT_WIRE_BYTES, "{bytes}");
        }
        other => panic!("expected the bound refusal, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The handler_stats frame
// ---------------------------------------------------------------------------

fn counts(handler: &str, errors: u64, dropped: u64) -> HandlerCounts {
    HandlerCounts {
        handler: HandlerName::new(handler.to_owned()).unwrap(),
        errors,
        dropped,
    }
}

#[test]
fn the_handler_stats_frame_carries_the_counts_of_each_handler() {
    assert_child_bytes(
        &ChildFrame::HandlerStats(HandlerStatsFrame {
            handlers: HandlerCountsList::new(vec![
                counts("app.handlers.on_claim", 0, 3),
                counts("app.handlers.on_end", 2, 0),
            ])
            .unwrap(),
            span_reads: SpanReadCounts {
                read_failed: 4,
                not_read: 1,
            },
            rss: rss(40.5),
        }),
        &framed(
            concat!(
                r#"{"kind":"handler_stats","handlers":["#,
                r#"{"handler":"app.handlers.on_claim","errors":0,"dropped":3},"#,
                r#"{"handler":"app.handlers.on_end","errors":2,"dropped":0}],"#,
                r#""span_read_failed":4,"span_not_read":1,"#,
                r#""rss_mb":40.5}"#,
            ),
            b"",
        ),
    );
    assert_child_bytes(
        &ChildFrame::HandlerStats(HandlerStatsFrame {
            handlers: HandlerCountsList::new(Vec::new()).unwrap(),
            span_reads: SpanReadCounts::default(),
            rss: RssReading::Unreadable,
        }),
        &framed(
            r#"{"kind":"handler_stats","handlers":[],"span_read_failed":0,"span_not_read":0,"rss_mb":null}"#,
            b"",
        ),
    );
}

#[test]
fn each_malformed_handler_stats_frame_is_refused_with_its_field() {
    let entry = |handler: &str| json!({"handler": handler, "errors": 0, "dropped": 0});
    let meta = json!({
        "kind": "handler_stats",
        "handlers": [entry("a")],
        "span_read_failed": 0,
        "span_not_read": 0,
        "rss_mb": 1.0,
    });
    let too_many: Vec<Value> = (0..=MAX_EVENT_HANDLERS)
        .map(|index| entry(&format!("h{index}")))
        .collect();
    let cases: Vec<(&str, Option<Value>, MalformedFrame)> = vec![
        (
            "handlers",
            None,
            MalformedFrame::MissingField {
                field: "handlers".to_owned(),
            },
        ),
        (
            "handlers",
            Some(json!({"a": 1})),
            MalformedFrame::WrongType {
                field: "handlers".to_owned(),
                expected: ExpectedType::ListOfObjects,
            },
        ),
        (
            "handlers",
            Some(json!(["a"])),
            MalformedFrame::WrongType {
                field: "handlers".to_owned(),
                expected: ExpectedType::ListOfObjects,
            },
        ),
        (
            "handlers",
            Some(Value::Array(too_many)),
            MalformedFrame::OutOfBounds {
                field: "handlers".to_owned(),
            },
        ),
        (
            "handlers",
            Some(json!([{"handler": "a", "errors": 0}])),
            MalformedFrame::MissingField {
                field: "handlers[0].dropped".to_owned(),
            },
        ),
        (
            "handlers",
            Some(json!([entry("a"), {"handler": "b", "errors": -1, "dropped": 0}])),
            MalformedFrame::OutOfBounds {
                field: "handlers[1].errors".to_owned(),
            },
        ),
        (
            "handlers",
            Some(json!([{"handler": "a", "errors": 1.5, "dropped": 0}])),
            MalformedFrame::WrongType {
                field: "handlers[0].errors".to_owned(),
                expected: ExpectedType::WholeNumber,
            },
        ),
        (
            "handlers",
            Some(json!([{"handler": "a", "errors": 0, "dropped": 0, "queue": 1}])),
            MalformedFrame::UnknownField {
                field: "handlers[0].queue".to_owned(),
            },
        ),
        (
            "handlers",
            Some(json!([entry(&"n".repeat(MAX_HANDLER_NAME_CHARS + 1))])),
            MalformedFrame::OutOfBounds {
                field: "handlers[0].handler".to_owned(),
            },
        ),
        (
            "handlers",
            Some(json!([entry("")])),
            MalformedFrame::BrokenRule {
                field: "handlers".to_owned(),
                rule: "a handler has an empty name",
            },
        ),
        (
            "handlers",
            Some(json!([entry("a"), entry("a")])),
            MalformedFrame::BrokenRule {
                field: "handlers".to_owned(),
                rule: "names a handler twice",
            },
        ),
        (
            "rss_mb",
            Some(json!("x")),
            MalformedFrame::WrongType {
                field: "rss_mb".to_owned(),
                expected: ExpectedType::Number,
            },
        ),
        (
            "span_read_failed",
            None,
            MalformedFrame::MissingField {
                field: "span_read_failed".to_owned(),
            },
        ),
        (
            "span_not_read",
            Some(json!(-1)),
            MalformedFrame::OutOfBounds {
                field: "span_not_read".to_owned(),
            },
        ),
    ];
    for (key, value, expected) in cases {
        let changed = with_field(&meta, key, value.clone());
        assert_eq!(
            malformed_child_meta(&changed, b""),
            expected,
            "{key} = {value:?}"
        );
    }
    assert_eq!(
        malformed_child_meta(&meta, b"{}"),
        MalformedFrame::UnexpectedBody {
            kind: "handler_stats",
            body_bytes: 2,
        },
    );
}

#[test]
fn a_list_of_handler_counts_names_each_handler_once() {
    let too_many: Vec<HandlerCounts> = (0..=MAX_EVENT_HANDLERS)
        .map(|index| counts(&format!("h{index}"), 0, 0))
        .collect();
    assert_eq!(
        HandlerCountsList::new(too_many),
        Err(HandlerCountsInvalid::TooMany {
            count: MAX_EVENT_HANDLERS + 1
        })
    );
    assert_eq!(
        HandlerCountsList::new(vec![counts("", 0, 0)]),
        Err(HandlerCountsInvalid::EmptyName)
    );
    assert_eq!(
        HandlerCountsList::new(vec![counts("a", 0, 0), counts("a", 1, 1)]),
        Err(HandlerCountsInvalid::RepeatedName {
            name: HandlerName::new("a".to_owned()).unwrap(),
        })
    );
    let most: Vec<HandlerCounts> = (0..MAX_EVENT_HANDLERS)
        .map(|index| counts(&format!("h{index}"), 0, 0))
        .collect();
    assert_eq!(
        HandlerCountsList::new(most).unwrap().handlers().len(),
        MAX_EVENT_HANDLERS
    );
}

/// The task children refuse the frames of a handler child: a handler child
/// sends `handler_stats`, and only a handler child takes `events`.
#[test]
fn the_frames_of_a_handler_child_have_their_own_kinds() {
    let stats = encode_child_frame(&ChildFrame::HandlerStats(HandlerStatsFrame {
        handlers: HandlerCountsList::new(Vec::new()).unwrap(),
        span_reads: SpanReadCounts::default(),
        rss: RssReading::Unreadable,
    }))
    .unwrap()
    .to_bytes();
    assert!(matches!(
        decode_parent_bytes(&stats),
        Err(ParentFrameRefusal::Malformed(
            MalformedFrame::UnknownKind { .. }
        ))
    ));
    let events = encode_parent_frame(&ParentFrame::Events(EventsFrame {
        events: EventBatch::new(&[wire_event(SLOT_RECOVERED)]).unwrap(),
    }))
    .unwrap()
    .to_bytes();
    assert!(matches!(
        decode_child_bytes(&events),
        Err(ChildFrameRefusal::Malformed(
            MalformedFrame::UnknownKind { .. }
        ))
    ));
}
