//! The attempt span: its parent, its attributes, its status, and the spans
//! of a body under it, recorded with the SDK's in-memory exporter through a
//! `tracing-opentelemetry` layer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use horsies_core::events::AttemptNumber;
use horsies_core::{
    BodyCarrier, CustomCode, Execution, OperationalCode, TaskErrorCode, WorkflowNodeIndex,
    MAX_WORKFLOW_NODE_INDEX,
};
use opentelemetry::trace::{SpanId, SpanKind, TraceId, TracerProvider as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData};
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;
use uuid::Uuid;

use super::super::carrier::attempt_parent_of;
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
    BodyCarrier::Carrier(BTreeMap::from([
        ("traceparent".to_owned(), TRACEPARENT.to_owned()),
        ("tracestate".to_owned(), String::new()),
    ]))
}

/// The second attempt of a task that no workflow node links, on
/// [`QUEUE_NAME`].
fn run_with(task_id: Uuid, carrier: &BodyCarrier) -> BodyRun<'_> {
    BodyRun {
        task_id,
        task_name: TASK_NAME,
        queue_name: QUEUE_NAME,
        attempt: AttemptNumber::try_from(2).unwrap(),
        worker_id: WORKER_ID,
        execution: Execution::SharedAsync,
        workflow: RunWorkflow::NotInWorkflow,
        carrier,
    }
}

/// Run `body` with a subscriber whose only layer is `tracing-opentelemetry`
/// on a tracer of `provider`.
fn with_otel_layer<T>(provider: &SdkTracerProvider, body: impl FnOnce() -> T) -> T {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("attempt-tests")));
    tracing::subscriber::with_default(subscriber, body)
}

/// Open the attempt span of `run` with the W3C propagator, run `body`
/// inside it, close it with `end` and drop it. Returns the ended spans.
fn record_attempt(
    run: &BodyRun<'_>,
    end: &BodyOutcome<'_>,
    body: impl FnOnce(&tracing::Span),
) -> Vec<SpanData> {
    let (provider, exporter) = recording_tracer_provider();
    with_otel_layer(&provider, || {
        let parent = attempt_parent_of(run.carrier, &TraceContextPropagator::new());
        let span = attempt_span(run, parent);
        body(&span);
        close_attempt_span(&span, end);
    });
    finished_spans(&provider, &exporter)
}

#[tracing::instrument]
fn charge_card(cents: u32) -> u32 {
    tracing::info!("charging");
    cents
}

#[tracing::instrument]
async fn reserve_stock(items: u32) -> u32 {
    tokio::task::yield_now().await;
    items
}

/// The parent of the attempt span is the send span that the stored carrier
/// names; the span is `run <task_name>` of kind CONSUMER with the task's
/// attributes, its queue and its attempt; the keys of the task name, the
/// queue and `error.type` are the keys of the metric catalog. A task that
/// no workflow node links has no workflow attribute.
#[test]
fn the_attempt_span_is_a_child_of_the_send_span_with_the_task_attributes() {
    let task_id = Uuid::new_v4();
    let carrier = send_carrier();
    let spans = record_attempt(
        &run_with(task_id, &carrier),
        &BodyOutcome::Returned,
        |span| {
            span.in_scope(|| {});
        },
    );
    let attempt = span_named(&spans, SPAN_NAME);
    assert_eq!(attempt.span_kind, SpanKind::Consumer);
    assert_eq!(
        attempt.span_context.trace_id(),
        TraceId::from(SEND_TRACE_ID)
    );
    assert_eq!(attempt.parent_span_id, SpanId::from(SEND_SPAN_ID));
    assert!(attempt.parent_span_is_remote);
    let task_id_text = task_id.to_string();
    for (key, value) in [
        ("horsies.task.id", task_id_text.as_str()),
        (LabelName::Task.attribute_key(), TASK_NAME),
        (LabelName::Execution.attribute_key(), "SHARED_ASYNC"),
        (LabelName::WorkerId.attribute_key(), WORKER_ID),
        (LabelName::Queue.attribute_key(), QUEUE_NAME),
        ("horsies.task.attempt", "2"),
        ("messaging.system", "horsies"),
        ("messaging.operation.type", "process"),
        ("messaging.destination.name", QUEUE_NAME),
        ("messaging.message.id", task_id_text.as_str()),
    ] {
        assert_eq!(attribute(attempt, key).as_deref(), Some(value), "{key}");
    }
    for key in ["horsies.workflow.id", "horsies.workflow.node"] {
        assert_eq!(attribute(attempt, key), None, "{key}");
    }
    assert_eq!(attempt.status, Status::Unset);
    assert_eq!(
        attribute(attempt, LabelName::ErrorCode.attribute_key()),
        None
    );
}

/// The attempt span of a workflow task names the workflow, the index of
/// the node that links the task, the largest index included, and the root
/// of its tree.
#[test]
fn the_attempt_span_of_a_workflow_task_names_its_workflow_node_and_root() {
    let workflow_id = Uuid::from_u128(0xf007);
    let root_workflow_id = Uuid::from_u128(0xf001);
    let carrier = BodyCarrier::NoCarrier;
    for node_index in [7, MAX_WORKFLOW_NODE_INDEX] {
        let run = BodyRun {
            workflow: RunWorkflow::InWorkflow {
                workflow_id,
                node_index: WorkflowNodeIndex::try_from(node_index).unwrap(),
                root_workflow_id,
            },
            ..run_with(Uuid::new_v4(), &carrier)
        };
        let spans = record_attempt(&run, &BodyOutcome::Returned, |span| span.in_scope(|| {}));
        let attempt = span_named(&spans, SPAN_NAME);
        let workflow_id_text = workflow_id.to_string();
        let root_workflow_id_text = root_workflow_id.to_string();
        let node_index_text = node_index.to_string();
        for (key, value) in [
            ("horsies.workflow.id", workflow_id_text.as_str()),
            ("horsies.workflow.node", node_index_text.as_str()),
            ("horsies.workflow.root_id", root_workflow_id_text.as_str()),
        ] {
            assert_eq!(attribute(attempt, key).as_deref(), Some(value), "{key}");
        }
    }
}

/// A writer whose bytes go to one shared buffer.
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A log record of the body carries the fields of the attempt span, the
/// attempt among them: the text format of a `fmt` layer writes them in the
/// span context of the record.
#[test]
fn a_record_of_the_body_carries_the_attempt_of_its_span() {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let writer_buffer = Arc::clone(&buffer);
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(move || Captured(Arc::clone(&writer_buffer)));
    let carrier = BodyCarrier::NoCarrier;
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        let span = attempt_span(
            &run_with(Uuid::new_v4(), &carrier),
            AttemptParent::NoCarrier,
        );
        span.in_scope(|| tracing::warn!("the card was declined"));
    });
    let text = String::from_utf8(
        buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
    )
    .unwrap();
    let lines: Vec<&str> = text.lines().collect();
    let [line] = lines.as_slice() else {
        panic!("one line per record, not {lines:?}");
    };
    assert!(line.contains("the card was declined"), "{line}");
    assert!(line.contains("horsies.task.attempt=2"), "{line}");
}

/// A task with no stored carrier gets a root span of a new trace.
#[test]
fn an_attempt_with_no_carrier_is_a_root_span() {
    let carrier = BodyCarrier::NoCarrier;
    let spans = record_attempt(
        &run_with(Uuid::new_v4(), &carrier),
        &BodyOutcome::Returned,
        |span| span.in_scope(|| {}),
    );
    let attempt = span_named(&spans, SPAN_NAME);
    assert_eq!(attempt.parent_span_id, SpanId::INVALID);
    assert!(attempt.span_context.is_valid());
    assert_ne!(
        attempt.span_context.trace_id(),
        TraceId::from(SEND_TRACE_ID)
    );
}

/// An `#[instrument]` function that a blocking body calls inside the
/// attempt span is its child, in the send's trace.
#[test]
fn an_instrumented_function_of_a_blocking_body_nests_under_the_attempt_span() {
    let carrier = send_carrier();
    let spans = record_attempt(
        &run_with(Uuid::new_v4(), &carrier),
        &BodyOutcome::Returned,
        |span| {
            let charged = span.in_scope(|| charge_card(1_200));
            assert_eq!(charged, 1_200);
        },
    );
    let attempt = span_named(&spans, SPAN_NAME);
    let charge = span_named(&spans, "charge_card");
    assert_eq!(charge.parent_span_id, attempt.span_context.span_id());
    assert_eq!(
        charge.span_context.trace_id(),
        attempt.span_context.trace_id()
    );
}

/// An `#[instrument]` async function that an async body awaits, polled
/// inside the attempt span, is its child.
#[test]
fn an_instrumented_function_of_an_async_body_nests_under_the_attempt_span() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a test runtime");
    let carrier = send_carrier();
    let spans = record_attempt(
        &run_with(Uuid::new_v4(), &carrier),
        &BodyOutcome::Returned,
        |span| {
            let reserved =
                runtime.block_on(async { reserve_stock(3).await }.instrument(span.clone()));
            assert_eq!(reserved, 3);
        },
    );
    let attempt = span_named(&spans, SPAN_NAME);
    let reserve = span_named(&spans, "reserve_stock");
    assert_eq!(reserve.parent_span_id, attempt.span_context.span_id());
}

/// An error, a panic, a run past its deadline and a cancelled run set the
/// status ERROR with `error.type` the code; a returned value sets OK.
#[test]
fn the_end_of_the_body_sets_the_status_of_the_attempt_span() {
    let declined =
        TaskErrorCode::Custom(CustomCode::parse("DECLINED").expect("a free custom code"));
    let unhandled = TaskErrorCode::BuiltIn(OperationalCode::UnhandledException.into());
    let carrier = send_carrier();
    let error_type = LabelName::ErrorCode.attribute_key();
    for (end, expected) in [
        (
            BodyOutcome::Failed {
                error_code: &declined,
            },
            Some("DECLINED"),
        ),
        (
            BodyOutcome::Failed {
                error_code: &unhandled,
            },
            Some("UNHANDLED_EXCEPTION"),
        ),
        (BodyOutcome::TimedOut, Some("TASK_TIMEOUT")),
        (BodyOutcome::Cancelled, Some("TASK_CANCELLED")),
        (BodyOutcome::Returned, None),
    ] {
        let spans = record_attempt(&run_with(Uuid::new_v4(), &carrier), &end, |span| {
            span.in_scope(|| {});
        });
        let attempt = span_named(&spans, SPAN_NAME);
        assert_eq!(
            attribute(attempt, error_type).as_deref(),
            expected,
            "{end:?}"
        );
        match expected {
            Some(_) => assert_eq!(attempt.status, Status::error(""), "{end:?}"),
            None => assert_eq!(attempt.status, Status::Unset, "{end:?}"),
        }
    }
}

/// The warning events that a subscriber received.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warnings {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() == tracing::Level::WARN {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(event.metadata().target().to_owned());
        }
    }
}

/// With no `tracing-opentelemetry` layer, the span keeps no parent, and
/// the process logs that once.
#[test]
fn an_attempt_span_with_no_layer_logs_once_that_it_has_no_parent() {
    let warnings = Warnings::default();
    let logged = AtomicBool::new(false);
    let carrier = send_carrier();
    let run = run_with(Uuid::new_v4(), &carrier);
    let subscriber = tracing_subscriber::registry().with(warnings.clone());
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!(target: ATTEMPT_SPAN_TARGET, parent: None, "run");
        let refused = span
            .set_parent(opentelemetry::Context::new())
            .expect_err("no layer records the span");
        log_parent_not_set(&run, &refused, &logged);
        log_parent_not_set(&run, &refused, &logged);
    });
    assert_eq!(
        warnings
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len(),
        1
    );
}

/// The body scope of a worker child with the feature on opens attempt
/// spans.
#[test]
fn a_worker_child_runs_its_bodies_in_attempt_spans() {
    assert!(matches!(body_scope(), BodyScope::Spans(_)));
}
