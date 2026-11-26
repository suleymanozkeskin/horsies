"""The apps of the telemetry levels with tracing, and the OpenTelemetry SDK
providers that discard what they get.

Only a level with tracing imports this module: it needs the tracing setting,
the event handlers and the OpenTelemetry SDK, which a build of the library
without telemetry does not have (``telemetry_levels.py``).

The providers are the SDK's own, with nothing after them:

- spans: ``TracerProvider`` with a ``BatchSpanProcessor`` (the processor of a
  production setup) and an exporter whose ``export`` returns success;
- metrics: ``MeterProvider`` with an ``InMemoryMetricReader`` that nothing
  collects. The SDK aggregates each sample when it is recorded.

So a cell measures the cost of making the spans and the samples, and not the
cost of a network export.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import cache
from typing import assert_never

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)

from horsies import AppConfig, Horsies, OtelTracing, WorkerEndedSpans
from horsies.telemetry import AttemptEnded

from crossclient.perf.public_calls import public_call_broker
from crossclient.perf.telemetry_levels import AppTelemetry
from crossclient.perf.worker_drain import (
    DRAIN_WORKER_PROFILES,
    DrainApp,
    DrainSender,
    drain_broker,
    with_drain_task,
)


class DiscardingSpanExporter(SpanExporter):
    """A span exporter that takes every batch and keeps nothing."""

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        _ = spans
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        _ = timeout_millis
        return True


@cache
def install_discarding_providers() -> None:
    """Set the global tracer provider and meter provider of this process to
    the SDK providers that discard. Once per process: a second call does
    nothing. A process that already has providers keeps them (the SDK logs
    a warning)."""
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(BatchSpanProcessor(DiscardingSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(MeterProvider(metric_readers=[InMemoryMetricReader()]))


def perf_attempt_ended(event: AttemptEnded) -> None:
    """The handler of the ``handler`` level: it returns at once, so a cell
    measures the delivery of the events, not the work of a handler."""
    _ = event


def tracing_setting(app_telemetry: AppTelemetry) -> OtelTracing:
    """The tracing setting of an app with tracing.

    Raises:
        AssertionError: ``AppTelemetry.NONE``, whose app is not built here:
            a defect of the caller.
    """
    match app_telemetry:
        case AppTelemetry.NONE:
            raise AssertionError('an app with no telemetry is not built here')
        case AppTelemetry.TRACING | AppTelemetry.TRACING_AND_HANDLER:
            return OtelTracing(worker_ended_spans=WorkerEndedSpans.NO_SPANS)
        case AppTelemetry.TRACING_HANDLER_AND_WORKER_ENDED_SPANS:
            return OtelTracing(worker_ended_spans=WorkerEndedSpans.MAKE_SPANS)
        case _ as unreachable:
            assert_never(unreachable)


def _with_hooks(app: Horsies, app_telemetry: AppTelemetry) -> Horsies:
    """Register the hooks and the handler of ``app_telemetry`` on ``app``.

    Raises:
        AssertionError: ``AppTelemetry.NONE``: a defect of the caller.
    """
    match app_telemetry:
        case AppTelemetry.NONE:
            raise AssertionError('an app with no telemetry is not built here')
        case AppTelemetry.TRACING:
            _ = app.on_child_start(install_discarding_providers)
            return app
        case AppTelemetry.TRACING_AND_HANDLER:
            _ = app.on_child_start(install_discarding_providers)
            _ = app.on_event(AttemptEnded)(perf_attempt_ended)
            return app
        case AppTelemetry.TRACING_HANDLER_AND_WORKER_ENDED_SPANS:
            _ = app.on_child_start(install_discarding_providers)
            _ = app.on_event(AttemptEnded)(perf_attempt_ended)
            _ = app.on_handler_child_start(install_discarding_providers)
            return app
        case _ as unreachable:
            assert_never(unreachable)


def telemetry_drain_app(dsn: str, app_telemetry: AppTelemetry) -> DrainApp:
    """The app of a drain with ``app_telemetry``: the app of
    ``worker_drain.drain_app`` with its tracing setting, its hooks and its
    handler. The worker and its children build it; the hooks set the
    providers in each child. No I/O."""
    app = Horsies(
        AppConfig(
            broker=drain_broker(dsn),
            worker_profiles=DRAIN_WORKER_PROFILES,
            tracing=tracing_setting(app_telemetry),
        ),
    )
    return with_drain_task(_with_hooks(app, app_telemetry))


def open_telemetry_drain_sender(dsn: str, app_telemetry: AppTelemetry) -> DrainSender:
    """The sender of the drains of ``app_telemetry``, in the harness process.
    Sets the discarding providers of this process, so each send makes its
    span and stores its trace carrier, as the producer of a traced app does.
    No I/O."""
    install_discarding_providers()
    return DrainSender(telemetry_drain_app(dsn, app_telemetry))


def telemetry_public_app(dsn: str, app_telemetry: AppTelemetry) -> Horsies:
    """The app of a public call with ``app_telemetry``, in the harness
    process. Sets the discarding providers of this process. No I/O."""
    install_discarding_providers()
    return Horsies(
        AppConfig(
            broker=public_call_broker(dsn),
            tracing=tracing_setting(app_telemetry),
        ),
    )
