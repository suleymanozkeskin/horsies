//! The `tracing-opentelemetry` layer of the client, on the global tracer
//! provider.

use opentelemetry::global::{self, BoxedSpan};
use opentelemetry::trace::{SpanBuilder, Tracer};
use opentelemetry::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use super::instrumentation_scope;

/// The tracer of the global tracer provider at each span: a provider set
/// after the layer was built (in an `on_child_start` hook) records the
/// spans that start after it.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct GlobalTracer;

impl Tracer for GlobalTracer {
    type Span = BoxedSpan;

    fn build_with_context(&self, builder: SpanBuilder, parent_cx: &Context) -> BoxedSpan {
        global::tracer_with_scope(instrumentation_scope()).build_with_context(builder, parent_cx)
    }
}

/// The `tracing-opentelemetry` layer that turns each `tracing` span into an
/// OpenTelemetry span of the global tracer provider, at the time the span
/// starts. The attempt span of a task body sets its parent through this
/// layer, so a subscriber that a binary installs itself adds it.
///
/// ```ignore
/// use tracing_subscriber::layer::SubscriberExt;
/// use tracing_subscriber::util::SubscriberInitExt;
///
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::EnvFilter::new("info"))
///     .with(tracing_subscriber::fmt::layer())
///     .with(horsies::otel::layer())
///     .init();
/// ```
pub fn layer<S>() -> impl Layer<S>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    tracing_opentelemetry::layer().with_tracer(GlobalTracer)
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanKind, TraceContextExt};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    use super::*;

    /// A provider set after the tracer was made records the spans that
    /// start after it, with the client's instrumentation scope. This test
    /// is the only one of the crate's unit tests that sets the global tracer
    /// provider; a send of another test may record a span in it too, so the
    /// span is found by its name.
    #[test]
    fn the_global_tracer_uses_the_provider_of_the_time_of_each_span() {
        const NAME: &str = "global tracer probe";
        let tracer = GlobalTracer;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let span = tracer.build_with_context(
            SpanBuilder::from_name(NAME).with_kind(SpanKind::Internal),
            &Context::new(),
        );
        let context = Context::new().with_span(span);
        context.span().end();
        provider
            .force_flush()
            .expect("the in-memory exporter flushes");
        let spans = exporter.get_finished_spans().expect("the finished spans");
        let after = spans
            .iter()
            .find(|span| span.name == NAME)
            .unwrap_or_else(|| panic!("the probe span is recorded: {spans:?}"));
        assert_eq!(
            after.instrumentation_scope.name(),
            super::super::INSTRUMENTATION_SCOPE
        );
        assert_eq!(
            after.instrumentation_scope.version(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }
}
