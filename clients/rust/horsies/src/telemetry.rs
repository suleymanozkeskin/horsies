//! The telemetry of the client's task sends, workflow starts and task
//! bodies.
//!
//! - In both builds a worker child runs each task body inside one `tracing`
//!   span ([`body_span`], target [`ATTEMPT_SPAN_TARGET`]) with the fields
//!   `horsies.task.id`, `horsies.task.name` and `horsies.task.attempt`, so
//!   each log record of the body carries them.
//! - With the feature `otel`, that span is the attempt span: it gets its
//!   OpenTelemetry parent and attributes, and the send spans and the
//!   producer metrics go to the user's global OpenTelemetry providers
//!   (`horsies::otel`).
//! - Without it, the client records no OpenTelemetry span and no metric: a
//!   send, a start and a rerun carry no trace carrier, and no OpenTelemetry
//!   crate is compiled.
//!
//! The send paths call the same four items in both builds: `TaskSendSpan`,
//! `WorkflowStartSpan`, `rerun_trace` and `body_scope`. The handler child calls
//! `ended_span_scope` with the worker-ended span switch of the app, whose
//! default is `DEFAULT_WORKER_ENDED_SPANS`.

use horsies_core::worker_manifest::WorkerEndedSpans;
use horsies_core::BodyRun;
use uuid::Uuid;

#[cfg(feature = "otel")]
pub(crate) use crate::otel::{
    body_scope, ended_span_scope, rerun_trace, TaskSendSpan, WorkflowStartSpan,
};

#[cfg(not(feature = "otel"))]
pub(crate) use untraced::{
    body_scope, ended_span_scope, rerun_trace, TaskSendSpan, WorkflowStartSpan,
};

/// The worker-ended span switch of a new app with the feature `otel`: the
/// handler child makes the span of each attempt that the worker ended
/// (`Horsies::set_worker_ended_spans` changes it).
#[cfg(feature = "otel")]
pub(crate) const DEFAULT_WORKER_ENDED_SPANS: WorkerEndedSpans = WorkerEndedSpans::MakeSpans;

/// The worker-ended span switch of an app without the feature `otel`: no
/// span, and no handler child for spans. No setter changes it in this
/// build.
#[cfg(not(feature = "otel"))]
pub(crate) const DEFAULT_WORKER_ENDED_SPANS: WorkerEndedSpans = WorkerEndedSpans::NoSpans;

/// The `tracing` target of the span of each task body. A filter of the
/// binary's subscriber passes it at `INFO` so that every body has its span
/// and its records carry the task's fields; `horsies::cli::run` adds that
/// directive itself.
pub const ATTEMPT_SPAN_TARGET: &str = "horsies::attempt";

/// The directive of the filter of `horsies::cli::run` that keeps the span
/// of each task body at every log level.
#[cfg(feature = "cli")]
const ATTEMPT_SPAN_DIRECTIVE: &str = "horsies::attempt=info";

/// The filter directive that keeps the span of each task body at every log
/// level.
///
/// # Panics
///
/// Never: the directive is a fixed text that a test parses.
#[cfg(feature = "cli")]
pub(crate) fn attempt_span_directive() -> tracing_subscriber::filter::Directive {
    ATTEMPT_SPAN_DIRECTIVE
        .parse()
        .expect("the attempt span directive is a valid filter directive")
}

/// The span of the task body of `run`: target [`ATTEMPT_SPAN_TARGET`], level
/// `INFO`, name `run`, no `tracing` parent. Its fields `horsies.task.id`,
/// `horsies.task.name` and `horsies.task.attempt` are set, so each log
/// record of the body carries them. Its fields `otel.name` and `otel.kind`
/// stay empty unless the OpenTelemetry scope records them; an empty field
/// is not written in a record.
pub(crate) fn body_span(run: &BodyRun<'_>) -> tracing::Span {
    tracing::info_span!(
        target: ATTEMPT_SPAN_TARGET,
        parent: None,
        "run",
        otel.name = tracing::field::Empty,
        otel.kind = tracing::field::Empty,
        horsies.task.id = %run.task_id,
        horsies.task.name = run.task_name,
        horsies.task.attempt = run.attempt.get(),
    )
}

/// The facts of one task send that its span carries.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    not(feature = "otel"),
    expect(
        dead_code,
        reason = "only the send span of the `otel` feature reads the facts; \
                  without it the send builds the subject and opens no span"
    )
)]
pub(crate) struct TaskSendSubject<'a> {
    pub(crate) task_id: Uuid,
    pub(crate) task_name: &'a str,
    pub(crate) queue_name: &'a str,
}

/// The facts of one workflow start that its span carries.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    not(feature = "otel"),
    expect(
        dead_code,
        reason = "only the start span of the `otel` feature reads the facts; \
                  without it the start builds the subject and opens no span"
    )
)]
pub(crate) struct WorkflowStartSubject<'a> {
    pub(crate) workflow_id: Uuid,
    pub(crate) workflow_name: &'a str,
}

/// How the call of a span ended: with success, or with the code `C` of its
/// error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallEnd<C> {
    Succeeded,
    Failed(C),
}

/// The telemetry of a client built without the feature `otel`: the span of
/// each task body for its log records, and nothing else.
#[cfg(not(feature = "otel"))]
mod untraced {
    use std::sync::Arc;

    use horsies_core::telemetry::trace_carrier::SendTrace;
    use horsies_core::worker_manifest::WorkerEndedSpans;
    use horsies_core::{
        BodyOutcome, BodyRun, BodyScope, BodySpans, EndedSpanScope, TaskSendErrorCode,
        WorkflowStartErrorCode,
    };

    use super::{body_span, CallEnd, TaskSendSubject, WorkflowStartSubject};

    /// A task send with no span.
    #[derive(Debug)]
    pub(crate) struct TaskSendSpan;

    impl TaskSendSpan {
        /// No span is opened. Pure.
        pub(crate) fn open(_subject: &TaskSendSubject<'_>) -> TaskSendSpan {
            TaskSendSpan
        }

        /// The send carries no trace carrier.
        pub(crate) fn trace(&self) -> SendTrace {
            SendTrace::NoCarrier
        }

        /// Nothing is recorded.
        pub(crate) fn end(self, _end: CallEnd<TaskSendErrorCode>) {}
    }

    /// A workflow start with no span.
    #[derive(Debug)]
    pub(crate) struct WorkflowStartSpan;

    impl WorkflowStartSpan {
        /// No span is opened. Pure.
        pub(crate) fn open(_subject: &WorkflowStartSubject<'_>) -> WorkflowStartSpan {
            WorkflowStartSpan
        }

        /// The start carries no trace carrier.
        pub(crate) fn trace(&self) -> SendTrace {
            SendTrace::NoCarrier
        }

        /// Nothing is recorded.
        pub(crate) fn end(self, _end: CallEnd<WorkflowStartErrorCode>) {}
    }

    /// A rerun carries no trace carrier.
    pub(crate) fn rerun_trace() -> SendTrace {
        SendTrace::NoCarrier
    }

    /// A worker child runs each task body inside its [`body_span`].
    pub(crate) fn body_scope() -> BodyScope {
        BodyScope::Spans(Arc::new(BodyLogSpans))
    }

    /// The handler child of a client built without `otel` makes no span of
    /// an attempt that the worker ended, for either value: there is no
    /// tracer, and the switch of the app is `NoSpans` in this build.
    pub(crate) fn ended_span_scope(spans: WorkerEndedSpans) -> EndedSpanScope {
        match spans {
            WorkerEndedSpans::NoSpans | WorkerEndedSpans::MakeSpans => EndedSpanScope::NoSpans,
        }
    }

    /// The spans of the task bodies of a child built without `otel`: the
    /// plain span of each body, for its log records. How the body ended
    /// is not recorded.
    struct BodyLogSpans;

    impl BodySpans for BodyLogSpans {
        fn open(&self, run: &BodyRun<'_>) -> tracing::Span {
            body_span(run)
        }

        fn close(&self, _span: &tracing::Span, _end: &BodyOutcome<'_>) {}
    }
}

/// Without the feature `otel`: no send span, no carrier; each body runs
/// inside its plain span, whose fields its records carry.
#[cfg(all(test, not(feature = "otel")))]
mod tests {
    use horsies_core::telemetry::trace_carrier::SendTrace;
    use horsies_core::{BodyScope, TaskSendErrorCode, WorkflowStartErrorCode};

    use super::*;

    /// Without the feature `otel` an app makes no worker-ended span: its
    /// switch is `NoSpans`, so the worker starts no handler child for it, and
    /// the handler child calls no client for either value.
    #[test]
    fn without_the_otel_feature_no_worker_ended_span_is_made() {
        use horsies_core::EndedSpanScope;

        assert_eq!(DEFAULT_WORKER_ENDED_SPANS, WorkerEndedSpans::NoSpans);
        for spans in [WorkerEndedSpans::NoSpans, WorkerEndedSpans::MakeSpans] {
            assert!(matches!(ended_span_scope(spans), EndedSpanScope::NoSpans));
        }
    }

    #[test]
    fn without_the_otel_feature_a_send_a_start_and_a_rerun_carry_no_trace() {
        let send = TaskSendSpan::open(&TaskSendSubject {
            task_id: Uuid::new_v4(),
            task_name: "send_invoice",
            queue_name: "default",
        });
        assert_eq!(send.trace(), SendTrace::NoCarrier);
        send.end(CallEnd::Failed(TaskSendErrorCode::EnqueueFailed));
        let start = WorkflowStartSpan::open(&WorkflowStartSubject {
            workflow_id: Uuid::new_v4(),
            workflow_name: "nightly",
        });
        assert_eq!(start.trace(), SendTrace::NoCarrier);
        start.end(CallEnd::Failed(WorkflowStartErrorCode::EnqueueFailed));
        assert_eq!(rerun_trace(), SendTrace::NoCarrier);
    }

    /// A writer whose bytes go to one shared buffer.
    #[cfg(feature = "cli")]
    struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    #[cfg(feature = "cli")]
    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Without the feature `otel` a body runs inside its plain span, and a
    /// log record of the body carries the task id and the attempt: the
    /// text format of a `fmt` layer writes them in the span context of the
    /// record, and no `otel.*` field.
    #[cfg(feature = "cli")]
    #[test]
    fn without_the_otel_feature_a_record_of_the_body_carries_the_task_and_the_attempt() {
        use std::sync::{Arc, Mutex, PoisonError};

        use horsies_core::{BodyCarrier, Execution, RunWorkflow};
        use tracing_subscriber::layer::SubscriberExt;

        let BodyScope::Spans(spans) = body_scope() else {
            panic!("a worker child runs its bodies inside a span");
        };
        let task_id = Uuid::new_v4();
        let carrier = BodyCarrier::NoCarrier;
        let run = BodyRun {
            task_id,
            task_name: "app.tasks.charge",
            queue_name: "payments",
            attempt: horsies_core::events::AttemptNumber::try_from(3).unwrap(),
            worker_id: "worker-7",
            execution: Execution::Isolated,
            workflow: RunWorkflow::NotInWorkflow,
            carrier: &carrier,
        };
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer_buffer = Arc::clone(&buffer);
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(move || Captured(Arc::clone(&writer_buffer)));
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            let span = spans.open(&run);
            span.in_scope(|| tracing::warn!("the card was declined"));
            spans.close(&span, &horsies_core::BodyOutcome::Returned);
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
        assert!(
            line.contains(&format!("horsies.task.id={task_id}")),
            "{line}"
        );
        assert!(line.contains("horsies.task.attempt=3"), "{line}");
        assert!(!line.contains("otel."), "{line}");
    }
}

/// The filter directive of the runner, in both builds.
#[cfg(all(test, feature = "cli"))]
mod directive_tests {
    use super::*;

    /// The directive that `horsies::cli::run` adds parses, and names the
    /// target of the span of each body.
    #[test]
    fn the_attempt_span_directive_names_its_target() {
        let directive = attempt_span_directive();
        assert_eq!(directive.to_string(), ATTEMPT_SPAN_DIRECTIVE);
        assert!(ATTEMPT_SPAN_DIRECTIVE.starts_with(ATTEMPT_SPAN_TARGET));
    }
}
