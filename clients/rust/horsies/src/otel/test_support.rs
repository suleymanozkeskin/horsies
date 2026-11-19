//! Test support of the OpenTelemetry module: a tracer provider and a meter
//! provider of the SDK that keep what they record in memory.

use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};

/// A tracer provider whose spans are exported, at their end, to the
/// returned exporter.
pub(super) fn recording_tracer_provider() -> (SdkTracerProvider, InMemorySpanExporter) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    (provider, exporter)
}

/// Every span that `provider` ended.
pub(super) fn finished_spans(
    provider: &SdkTracerProvider,
    exporter: &InMemorySpanExporter,
) -> Vec<SpanData> {
    provider
        .force_flush()
        .expect("the in-memory span exporter flushes");
    exporter
        .get_finished_spans()
        .expect("the in-memory span exporter holds its spans")
}

/// The one ended span named `name`.
pub(super) fn span_named<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let mut named = spans.iter().filter(|span| span.name == name);
    match (named.next(), named.next()) {
        (Some(span), None) => span,
        (None, _) => panic!("no span is named {name}: {spans:?}"),
        (Some(_), Some(_)) => panic!("two spans are named {name}: {spans:?}"),
    }
}

/// The text of the attribute `key` of `span`.
pub(super) fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.as_str().into_owned())
}

/// A meter provider whose metrics are exported to the returned exporter at
/// each flush.
pub(super) fn recording_meter_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    (provider, exporter)
}

/// Every metric that `provider` recorded, as of a flush now.
pub(super) fn finished_metrics(
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
) -> Vec<ResourceMetrics> {
    provider
        .force_flush()
        .expect("the in-memory metric exporter flushes");
    exporter
        .get_finished_metrics()
        .expect("the in-memory metric exporter holds its metrics")
}
