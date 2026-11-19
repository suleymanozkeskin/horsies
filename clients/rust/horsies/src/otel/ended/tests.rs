//! The span of an attempt that the worker ended, recorded with the SDK's
//! in-memory exporter: its parent, its times, its status and its attributes
//! for a timeout kill, a crash and a blocked loop; a task with no carrier
//! gets a root span; the switch of the app and the scope of the handler
//! child.

use std::collections::BTreeMap;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use horsies_core::events::{AttemptNumber, WorkerDecision, WorkflowPlace};
use horsies_core::{BodyCarrier, BuiltInTaskCode, Execution};
use opentelemetry::trace::{SpanId, SpanKind, TraceId, TracerProvider as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SpanData;
use uuid::Uuid;

use super::super::test_support::{
    attribute, finished_spans, recording_tracer_provider, span_named,
};
use super::*;

const TASK_NAME: &str = "app.tasks.charge";
const SPAN_NAME: &str = "run app.tasks.charge";
const WORKER_ID: &str = "worker-7";
const QUEUE_NAME: &str = "payments";
const SEND_TRACE_ID: u128 = 0x0af7_6519_16cd_43dd_8448_eb21_1c80_319c;
const SEND_SPAN_ID: u64 = 0xb7ad_6b71_6920_3331;
const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// The stored carrier of a send whose span is [`SEND_SPAN_ID`].
fn send_carrier() -> BodyCarrier {
    BodyCarrier::Carrier(BTreeMap::from([(
        "traceparent".to_owned(),
        TRACEPARENT.to_owned(),
    )]))
}

fn placed_at() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_000_000_123_456)
}

fn ended_at() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(1_700_000_030_500_654_321)
}

/// One way the worker ends an attempt: its reason, and the kind of the
/// child that ran it.
#[derive(Debug, Clone, Copy)]
struct WorkerEnd {
    reason: WorkerDecision,
    child_kind: Execution,
    code: &'static str,
}

const TIMEOUT_KILL: WorkerEnd = WorkerEnd {
    reason: WorkerDecision::TimedOut,
    child_kind: Execution::Isolated,
    code: "TASK_TIMEOUT",
};

const CRASH: WorkerEnd = WorkerEnd {
    reason: WorkerDecision::ChildLost,
    child_kind: Execution::SharedThread,
    code: "WORKER_CRASHED",
};

const BLOCKED_LOOP: WorkerEnd = WorkerEnd {
    reason: WorkerDecision::TaskBlockedLoop,
    child_kind: Execution::SharedAsync,
    code: "TASK_BLOCKED_LOOP",
};

/// Attempt 2 of the task `task_id`, which the worker ended for `end`.
fn ended_attempt<'a>(
    task_id: Uuid,
    carrier: &'a BodyCarrier,
    end: WorkerEnd,
    error_code: &'a BuiltInTaskCode,
    workflow: WorkflowPlace,
) -> EndedAttempt<'a> {
    EndedAttempt {
        task_id,
        task_name: TASK_NAME,
        queue_name: QUEUE_NAME,
        attempt: AttemptNumber::try_from(2).unwrap(),
        worker_id: WORKER_ID,
        execution: end.child_kind,
        workflow,
        carrier,
        reason: end.reason,
        error_code,
        placed_at: placed_at(),
        ended_at: ended_at(),
    }
}

/// Make the span of `attempt` with the W3C propagator on a recording
/// provider. Returns the ended spans.
fn record_ended(attempt: &EndedAttempt<'_>) -> Vec<SpanData> {
    let (provider, exporter) = recording_tracer_provider();
    make_ended_span(
        &provider.tracer("ended-tests"),
        &TraceContextPropagator::new(),
        attempt,
    );
    finished_spans(&provider, &exporter)
}

/// A timeout kill, a crash and a blocked loop each make one span: a child
/// of the send span that the stored carrier names, `run <task_name>` of
/// kind CONSUMER, from the placement to the end, status ERROR with the code
/// of the attempt row, the attributes of the span of a child with the kind
/// of the child, and `horsies.span.source` = `worker`.
#[test]
fn each_end_makes_one_span_under_the_send_with_its_times_status_and_attributes() {
    let carrier = send_carrier();
    for end in [TIMEOUT_KILL, CRASH, BLOCKED_LOOP] {
        let task_id = Uuid::new_v4();
        let code = end.reason.error_code();
        let spans = record_ended(&ended_attempt(
            task_id,
            &carrier,
            end,
            &code,
            WorkflowPlace::NotInWorkflow,
        ));

        assert_eq!(spans.len(), 1, "{end:?}: {spans:?}");
        let span = span_named(&spans, SPAN_NAME);
        assert_eq!(span.span_kind, SpanKind::Consumer, "{end:?}");
        assert_eq!(
            span.span_context.trace_id(),
            TraceId::from(SEND_TRACE_ID),
            "{end:?}"
        );
        assert_eq!(span.parent_span_id, SpanId::from(SEND_SPAN_ID), "{end:?}");
        assert!(span.parent_span_is_remote, "{end:?}");
        assert_eq!(span.start_time, SystemTime::from(placed_at()), "{end:?}");
        assert_eq!(span.end_time, SystemTime::from(ended_at()), "{end:?}");
        assert_eq!(span.status, Status::error(""), "{end:?}");
        let task_id_text = task_id.to_string();
        for (key, value) in [
            ("error.type", end.code),
            ("horsies.span.source", "worker"),
            ("horsies.task.id", task_id_text.as_str()),
            ("horsies.task.name", TASK_NAME),
            ("horsies.task.attempt", "2"),
            ("horsies.task.execution", end.child_kind.as_str()),
            ("horsies.worker.id", WORKER_ID),
            ("horsies.queue", QUEUE_NAME),
            ("messaging.system", "horsies"),
            ("messaging.operation.type", "process"),
            ("messaging.destination.name", QUEUE_NAME),
            ("messaging.message.id", task_id_text.as_str()),
        ] {
            assert_eq!(
                attribute(span, key).as_deref(),
                Some(value),
                "{end:?}: {key}"
            );
        }
        assert_eq!(span.attributes.len(), 12, "{end:?}: {:?}", span.attributes);
    }
}

/// The worker does not know the node of a workflow task: its span has no
/// workflow attribute, and the carrier keeps it in the trace of the send.
#[test]
fn a_workflow_task_has_no_workflow_attribute_and_stays_in_the_trace() {
    let carrier = send_carrier();
    let code = TIMEOUT_KILL.reason.error_code();
    let spans = record_ended(&ended_attempt(
        Uuid::new_v4(),
        &carrier,
        TIMEOUT_KILL,
        &code,
        WorkflowPlace::WorkflowNotKnown,
    ));

    let span = span_named(&spans, SPAN_NAME);
    for key in [
        "horsies.workflow.id",
        "horsies.workflow.node",
        "horsies.workflow.root_id",
    ] {
        assert_eq!(attribute(span, key), None, "{key}");
    }
    assert_eq!(span.span_context.trace_id(), TraceId::from(SEND_TRACE_ID));
}

/// A task with no stored carrier gets a root span.
#[test]
fn a_task_with_no_carrier_gets_a_root_span() {
    let carrier = BodyCarrier::NoCarrier;
    let code = CRASH.reason.error_code();
    let spans = record_ended(&ended_attempt(
        Uuid::new_v4(),
        &carrier,
        CRASH,
        &code,
        WorkflowPlace::NotInWorkflow,
    ));

    let span = span_named(&spans, SPAN_NAME);
    assert_eq!(span.parent_span_id, SpanId::INVALID);
    assert_ne!(span.span_context.trace_id(), TraceId::from(SEND_TRACE_ID));
}

/// With the feature `otel` an app makes the spans by default; the setter
/// turns them off; the handler child makes them only with `MakeSpans`.
#[test]
fn the_switch_of_the_app_chooses_the_scope_of_the_handler_child() {
    let mut app = Horsies::new(crate::AppConfig::for_database_url(
        "postgresql://localhost/horsies",
    ))
    .unwrap();
    assert_eq!(app.worker_ended_spans(), WorkerEndedSpans::MakeSpans);
    app.set_worker_ended_spans(WorkerEndedSpans::NoSpans);
    assert_eq!(app.worker_ended_spans(), WorkerEndedSpans::NoSpans);
    assert!(matches!(
        ended_span_scope(WorkerEndedSpans::MakeSpans),
        EndedSpanScope::MakeSpans(_)
    ));
    assert!(matches!(
        ended_span_scope(WorkerEndedSpans::NoSpans),
        EndedSpanScope::NoSpans
    ));
}
