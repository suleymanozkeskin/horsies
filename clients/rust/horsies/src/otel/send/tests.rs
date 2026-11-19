//! The spans of the sends and the starts, the carrier that they hand the
//! core, and the producer metrics, recorded with the SDK's in-memory
//! exporters.

use std::collections::BTreeMap;

use horsies_core::telemetry::catalog::{MetricKind, MetricUnit, Recorder};
use horsies_core::telemetry::trace_carrier::PropagatedTrace;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::noop::NoopTracer;
use opentelemetry::trace::{
    SpanContext, SpanId, TraceFlags, TraceId, TraceState, TracerProvider as _,
};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SpanData;
use uuid::Uuid;

use super::super::test_support::{
    attribute, finished_metrics, finished_spans, recording_meter_provider,
    recording_tracer_provider, span_named,
};
use super::*;

const TASK_NAME: &str = "app.tasks.charge";
const QUEUE: &str = "payments";
const WORKFLOW_NAME: &str = "nightly_billing";
const CALLER_TRACE_ID: u128 = 0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736;
const CALLER_SPAN_ID: u64 = 0x00f0_67aa_0ba9_02b7;

/// The context of a caller that runs inside a sampled span.
fn caller_context() -> Context {
    Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from(CALLER_TRACE_ID),
        SpanId::from(CALLER_SPAN_ID),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

fn send_subject(task_id: Uuid) -> TaskSendSubject<'static> {
    TaskSendSubject {
        task_id,
        task_name: TASK_NAME,
        queue_name: QUEUE,
    }
}

/// The carrier of the W3C propagator for the span `span` of the caller's
/// trace.
fn w3c_trace_of(span: &SpanData) -> SendTrace {
    SendTrace::Propagated(PropagatedTrace {
        carrier: BTreeMap::from([
            (
                "traceparent".to_owned(),
                format!(
                    "00-{}-{}-01",
                    span.span_context.trace_id(),
                    span.span_context.span_id()
                ),
            ),
            ("tracestate".to_owned(), String::new()),
        ]),
        trace_id: span.span_context.trace_id().to_bytes().to_vec(),
    })
}

/// Each data point of the sum `name`: its value and its attributes.
fn sum_points(metrics: &[ResourceMetrics], name: &str) -> Vec<(u64, BTreeMap<String, String>)> {
    metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(|point| (point.value(), attributes_of(point.attributes())))
                .collect::<Vec<_>>(),
            other => panic!("{name} is a u64 sum, not {other:?}"),
        })
        .collect()
}

/// Each data point of the histogram `name`: its count, its bounds and its
/// attributes.
fn histogram_points(
    metrics: &[ResourceMetrics],
    name: &str,
) -> Vec<(u64, Vec<f64>, BTreeMap<String, String>)> {
    metrics
        .iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(|point| {
                    (
                        point.count(),
                        point.bounds().collect(),
                        attributes_of(point.attributes()),
                    )
                })
                .collect::<Vec<_>>(),
            other => panic!("{name} is an f64 histogram, not {other:?}"),
        })
        .collect()
}

fn attributes_of<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
    attributes
        .map(|attribute| {
            (
                attribute.key.as_str().to_owned(),
                attribute.value.as_str().into_owned(),
            )
        })
        .collect()
}

fn labels(pairs: &[(LabelName, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(label, value)| (label.attribute_key().to_owned(), (*value).to_owned()))
        .collect()
}

/// The span of a send is a PRODUCER child of the caller's context, with
/// the send's attributes; the carrier that the core gets names that span.
#[test]
fn a_task_send_span_is_a_child_of_the_caller_and_its_carrier_names_it() {
    let (tracer_provider, span_exporter) = recording_tracer_provider();
    let (meter_provider, _metric_exporter) = recording_meter_provider();
    let task_id = Uuid::new_v4();
    let send = open_task_send(
        &tracer_provider.tracer("send-tests"),
        &TraceContextPropagator::new(),
        &meter_provider.meter("send-tests"),
        &caller_context(),
        &send_subject(task_id),
    );
    let trace = send.trace();
    send.end(CallEnd::Succeeded);

    let spans = finished_spans(&tracer_provider, &span_exporter);
    let span = span_named(&spans, "send app.tasks.charge");
    assert_eq!(span.span_kind, SpanKind::Producer);
    assert_eq!(span.span_context.trace_id(), TraceId::from(CALLER_TRACE_ID));
    assert_eq!(span.parent_span_id, SpanId::from(CALLER_SPAN_ID));
    assert_eq!(trace, w3c_trace_of(span));
    let task_id_text = task_id.to_string();
    for (key, value) in [
        ("messaging.system", "horsies"),
        ("messaging.operation.type", "send"),
        ("messaging.operation.name", "send"),
        ("messaging.destination.name", QUEUE),
        ("messaging.message.id", task_id_text.as_str()),
        ("horsies.task.id", task_id_text.as_str()),
        (LabelName::Task.attribute_key(), TASK_NAME),
        (LabelName::Queue.attribute_key(), QUEUE),
    ] {
        assert_eq!(attribute(span, key).as_deref(), Some(value), "{key}");
    }
    assert_eq!(span.status, Status::Unset);
}

/// A send with no valid span (no SDK, no caller span) hands the core no
/// carrier.
#[test]
fn a_send_with_no_valid_span_hands_the_core_no_carrier() {
    let (meter_provider, _metric_exporter) = recording_meter_provider();
    let send = open_task_send(
        &NoopTracer::new(),
        &TraceContextPropagator::new(),
        &meter_provider.meter("send-tests"),
        &Context::new(),
        &send_subject(Uuid::new_v4()),
    );
    assert_eq!(send.trace(), SendTrace::NoCarrier);
    send.end(CallEnd::Succeeded);
}

/// A send that returns an error sets the status ERROR with `error.type` the
/// send code, and counts the code as its result.
#[test]
fn a_failed_send_sets_the_error_status_and_counts_its_code() {
    let (tracer_provider, span_exporter) = recording_tracer_provider();
    let (meter_provider, metric_exporter) = recording_meter_provider();
    let send = open_task_send(
        &tracer_provider.tracer("send-tests"),
        &TraceContextPropagator::new(),
        &meter_provider.meter("send-tests"),
        &caller_context(),
        &send_subject(Uuid::new_v4()),
    );
    send.end(CallEnd::Failed(TaskSendErrorCode::EnqueueFailed));

    let spans = finished_spans(&tracer_provider, &span_exporter);
    let span = span_named(&spans, "send app.tasks.charge");
    assert_eq!(span.status, Status::error(""));
    assert_eq!(
        attribute(span, LabelName::ErrorCode.attribute_key()).as_deref(),
        Some("ENQUEUE_FAILED")
    );
    let metrics = finished_metrics(&meter_provider, &metric_exporter);
    assert_eq!(
        sum_points(&metrics, "horsies.task.sent"),
        vec![(
            1,
            labels(&[
                (LabelName::Task, TASK_NAME),
                (LabelName::Queue, QUEUE),
                (LabelName::Result, "ENQUEUE_FAILED"),
            ])
        )]
    );
}

/// A send that succeeds counts `ok`, and records one duration with the
/// queue label in the catalog's buckets, in seconds.
#[test]
fn a_sent_task_counts_ok_and_records_its_duration() {
    let (tracer_provider, _span_exporter) = recording_tracer_provider();
    let (meter_provider, metric_exporter) = recording_meter_provider();
    let send = open_task_send(
        &tracer_provider.tracer("send-tests"),
        &TraceContextPropagator::new(),
        &meter_provider.meter("send-tests"),
        &caller_context(),
        &send_subject(Uuid::new_v4()),
    );
    send.end(CallEnd::Succeeded);

    let metrics = finished_metrics(&meter_provider, &metric_exporter);
    assert_eq!(
        sum_points(&metrics, "horsies.task.sent"),
        vec![(
            1,
            labels(&[
                (LabelName::Task, TASK_NAME),
                (LabelName::Queue, QUEUE),
                (LabelName::Result, SEND_OK_RESULT),
            ])
        )]
    );
    let bounds: Vec<f64> = DURATION_BUCKET_BOUNDS
        .iter()
        .map(std::time::Duration::as_secs_f64)
        .collect();
    assert_eq!(
        histogram_points(&metrics, "horsies.task.send.duration"),
        vec![(1, bounds, labels(&[(LabelName::Queue, QUEUE)]))]
    );
}

/// The producer metrics that the client records are the catalog's: a
/// counter with the labels task, queue and result, and a histogram in
/// seconds over the duration buckets with the label queue. A change of
/// the catalog fails here before it reaches a send.
#[test]
fn the_producer_metrics_are_the_catalog_s() {
    let sent = MetricId::TaskSent.spec();
    assert_eq!(sent.name, "horsies.task.sent");
    assert_eq!(sent.kind, MetricKind::Counter);
    assert_eq!(sent.unit, MetricUnit::Count);
    assert_eq!(sent.recorder, Recorder::Producer);
    assert_eq!(
        sent.labels
            .iter()
            .map(|label| label.name)
            .collect::<Vec<_>>(),
        vec![LabelName::Task, LabelName::Queue, LabelName::Result]
    );
    let duration = MetricId::TaskSendDuration.spec();
    assert_eq!(duration.name, "horsies.task.send.duration");
    assert_eq!(
        duration.kind,
        MetricKind::Histogram(&DURATION_BUCKET_BOUNDS)
    );
    assert_eq!(duration.unit, MetricUnit::Seconds);
    assert_eq!(duration.recorder, Recorder::Producer);
    assert_eq!(
        duration
            .labels
            .iter()
            .map(|label| label.name)
            .collect::<Vec<_>>(),
        vec![LabelName::Queue]
    );
}

/// The span of a workflow start is a PRODUCER child of the caller's
/// context with the start's attributes, and its carrier names it; a start
/// that fails sets the status ERROR with `error.type` the start code.
#[test]
fn a_workflow_start_span_carries_the_start_and_its_error() {
    let (tracer_provider, span_exporter) = recording_tracer_provider();
    let workflow_id = Uuid::new_v4();
    let start = open_workflow_start(
        &tracer_provider.tracer("start-tests"),
        &TraceContextPropagator::new(),
        &caller_context(),
        &WorkflowStartSubject {
            workflow_id,
            workflow_name: WORKFLOW_NAME,
        },
    );
    let trace = start.trace();
    start.end(CallEnd::Failed(WorkflowStartErrorCode::WorkflowIdConflict));

    let spans = finished_spans(&tracer_provider, &span_exporter);
    let span = span_named(&spans, "start nightly_billing");
    assert_eq!(span.span_kind, SpanKind::Producer);
    assert_eq!(span.parent_span_id, SpanId::from(CALLER_SPAN_ID));
    assert_eq!(trace, w3c_trace_of(span));
    let workflow_id_text = workflow_id.to_string();
    for (key, value) in [
        ("messaging.system", "horsies"),
        ("messaging.operation.type", "send"),
        ("messaging.operation.name", "start"),
        ("horsies.workflow.id", workflow_id_text.as_str()),
        ("horsies.workflow.name", WORKFLOW_NAME),
        (LabelName::ErrorCode.attribute_key(), "WORKFLOW_ID_CONFLICT"),
    ] {
        assert_eq!(attribute(span, key).as_deref(), Some(value), "{key}");
    }
    assert_eq!(span.status, Status::error(""));
}
