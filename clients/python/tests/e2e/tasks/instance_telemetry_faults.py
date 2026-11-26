"""App instance of the telemetry fault tests.

The environment variable ``TELEMETRY_FAULT_ENV`` names one fault
(``TelemetryFault``). The worker and each of its children import this module
with the variable of the test, so the fault is in every process of the
worker. The test process imports it with no fault: it only sends the tasks.

- ``HANDLER_CRASHES``: the handler of ``AttemptEnded`` ends the handler child
  at each event.
- ``HANDLER_BLOCKS``: the handler of ``AttemptEnded`` never returns.
- ``EXPORTER_HANGS``: each task child exports its attempt spans to an
  exporter whose ``export`` never returns.
- ``COLLECTOR_ABSENT``: each task child exports its attempt spans to a
  Collector address where nothing listens: each export fails at once, as an
  OTLP exporter does when its Collector is down.

The snapshot interval is the smallest, 1 s, so the ``telemetry`` column of
the worker snapshot follows the state of the endpoint and the handler child.
"""

from __future__ import annotations

import os
import socket
import threading
from collections.abc import Sequence
from enum import Enum
from typing import Final, assert_never

from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result
from horsies.core.models.tracing import NoTracing, OtelTracing
from horsies.telemetry import AttemptEnded

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES

TELEMETRY_FAULT_ENV: Final = 'HORSIES_E2E_TELEMETRY_FAULT'
"""The environment variable that names the fault of a worker."""

COLLECTOR_PORT_ENV: Final = 'HORSIES_E2E_ABSENT_COLLECTOR_PORT'
"""The port of the absent Collector: a local port where nothing listens."""

HANDLER_CRASH_EXIT_CODE: Final = 17
"""The exit code of a handler child that the crashing handler ends."""

SNAPSHOT_INTERVAL_MS: Final = 1_000
"""The smallest snapshot interval of ``RecoveryConfig``."""

COLLECTOR_CONNECT_TIMEOUT_S: Final = 1.0
"""The bound of one connect of the exporter to the absent Collector."""


class TelemetryFault(Enum):
    """The fault of the worker of a test."""

    NONE = 'none'
    HANDLER_CRASHES = 'handler-crashes'
    HANDLER_BLOCKS = 'handler-blocks'
    EXPORTER_HANGS = 'exporter-hangs'
    COLLECTOR_ABSENT = 'collector-absent'


def fault_from_environment() -> TelemetryFault:
    """The fault that ``TELEMETRY_FAULT_ENV`` names; ``NONE`` when it is not
    set (the test process).

    Raises:
        RuntimeError: the variable names no fault: a defect of the test.
    """
    text = os.environ.get(TELEMETRY_FAULT_ENV, TelemetryFault.NONE.value)
    for fault in TelemetryFault:
        if fault.value == text:
            return fault
    raise RuntimeError(f'{TELEMETRY_FAULT_ENV}={text!r} names no telemetry fault')


class HangingSpanExporter(SpanExporter):
    """An exporter whose ``export`` never returns."""

    def __init__(self) -> None:
        self._never = threading.Event()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        _ = spans
        _ = self._never.wait()
        return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        return None


class AbsentCollectorExporter(SpanExporter):
    """An exporter that connects to the Collector at ``127.0.0.1:port`` for
    each batch. Nothing listens there, so each export fails."""

    def __init__(self, port: int) -> None:
        self._port = port

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        _ = spans
        try:
            with socket.create_connection(
                ('127.0.0.1', self._port),
                timeout=COLLECTOR_CONNECT_TIMEOUT_S,
            ):
                return SpanExportResult.SUCCESS
        except OSError:
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        return None


def _set_tracer_provider(exporter: SpanExporter) -> None:
    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def install_hanging_exporter() -> None:
    """The start hook of each task child with ``EXPORTER_HANGS``."""
    _set_tracer_provider(HangingSpanExporter())


def install_absent_collector_exporter() -> None:
    """The start hook of each task child with ``COLLECTOR_ABSENT``."""
    _set_tracer_provider(AbsentCollectorExporter(int(os.environ[COLLECTOR_PORT_ENV])))


def crash_handler_child(event: AttemptEnded) -> None:
    """End the handler child at once, as a crash does."""
    _ = event
    os._exit(HANDLER_CRASH_EXIT_CODE)


def block_handler(event: AttemptEnded) -> None:
    """Never return."""
    _ = event
    _ = threading.Event().wait()


FAULT: Final = fault_from_environment()

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')


def _tracing(fault: TelemetryFault) -> OtelTracing | NoTracing:
    match fault:
        case TelemetryFault.EXPORTER_HANGS | TelemetryFault.COLLECTOR_ABSENT:
            return OtelTracing()
        case (
            TelemetryFault.NONE
            | TelemetryFault.HANDLER_CRASHES
            | TelemetryFault.HANDLER_BLOCKS
        ):
            return NoTracing()
        case _ as unreachable:
            assert_never(unreachable)


config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
    recovery=RecoveryConfig(worker_state_snapshot_interval_ms=SNAPSHOT_INTERVAL_MS),
    worker_profiles=E2E_WORKER_PROFILES,
    tracing=_tracing(FAULT),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker  # pyright: ignore[reportPrivateUsage]
broker.app = app

match FAULT:
    case TelemetryFault.NONE:
        pass
    case TelemetryFault.HANDLER_CRASHES:
        _ = app.on_event(AttemptEnded)(crash_handler_child)
    case TelemetryFault.HANDLER_BLOCKS:
        _ = app.on_event(AttemptEnded)(block_handler)
    case TelemetryFault.EXPORTER_HANGS:
        _ = app.on_child_start(install_hanging_exporter)
    case TelemetryFault.COLLECTOR_ABSENT:
        _ = app.on_child_start(install_absent_collector_exporter)
    case _ as unreachable:
        assert_never(unreachable)


@app.task(task_name='e2e_telemetry_faults_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(task_name='e2e_telemetry_faults_add')
def add(*, left: int, right: int) -> Result[int, TaskError]:
    return Ok(left + right)


app.discover_tasks(['tests.e2e.tasks.instance_telemetry_faults'])
