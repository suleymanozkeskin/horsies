"""The telemetry levels of the overhead measurement, and what each one turns
on in the worker of a drain and in the producer of a public call.

Each level adds one part to the level before it:

| Level | Adds |
|---|---|
| ``off`` | nothing: no telemetry |
| ``metrics`` | ``--metrics-listen``: the worker records its metrics; nothing reads the endpoint during the drain |
| ``scraped`` | a reader of ``/metrics`` and ``/livez`` every ``SCRAPE_INTERVAL`` during the drain |
| ``tracing`` | ``OtelTracing(worker_ended_spans=NO_SPANS)``: send spans in the producer, attempt spans in the task children, each exported to an exporter that discards |
| ``handler`` | one ``def`` handler of ``AttemptEnded`` that returns at once: the worker runs its handler child |
| ``worker-ended-spans`` | ``worker_ended_spans=MAKE_SPANS``, with the discarding exporter in the handler child |

A run compares the level ``off`` (the baseline side) with one level (the
candidate side). The levels below ``tracing`` change only the worker, so a
public call scenario, which runs no worker, takes ``off`` or ``tracing`` only.

This module imports no part of the library. The app of a level with tracing
is built by ``telemetry_apps.py``, which only such a level imports, so the
harness runs the level ``off`` on a build of the library that has no
telemetry.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Final, assert_never

METRICS_LISTEN_HOST: Final = '127.0.0.1'
METRICS_LISTEN_PORT: Final = 19464
"""The address of the worker endpoint of a drain, ``--metrics-listen``. A
drain runs one worker at a time, so every drain binds the same port."""

SCRAPE_INTERVAL: Final = timedelta(seconds=1)
"""The time between two reads of the endpoint in the ``scraped`` level and
above: 15 times the common 15 s scrape interval, so each drain of a few
seconds sees several reads."""


class TelemetryLevel(Enum):
    """The telemetry of the candidate side of a run. Each level holds every
    part of the level before it."""

    OFF = 'off'
    METRICS = 'metrics'
    SCRAPED = 'scraped'
    TRACING = 'tracing'
    HANDLER = 'handler'
    WORKER_ENDED_SPANS = 'worker-ended-spans'


class AppTelemetry(Enum):
    """What the app config and the hooks of an app turn on. The value is the
    form in ``APP_TELEMETRY_VARIABLE``."""

    NONE = 'none'
    """``NoTracing``, no handler. The app of ``worker_drain_app.py`` and of
    ``public_calls.py``."""

    TRACING = 'tracing'
    """``OtelTracing(worker_ended_spans=NO_SPANS)``. The task children and
    the producer export their spans to an exporter that discards."""

    TRACING_AND_HANDLER = 'tracing-and-handler'
    """``TRACING``, and one ``def`` handler of ``AttemptEnded`` that returns
    at once."""

    TRACING_HANDLER_AND_WORKER_ENDED_SPANS = 'tracing-handler-and-worker-ended-spans'
    """``TRACING_AND_HANDLER`` with ``worker_ended_spans=MAKE_SPANS``. The
    handler child exports to an exporter that discards."""


APP_TELEMETRY_VARIABLE: Final = 'HORSIES_PERF_APP_TELEMETRY'
"""The environment variable that gives the worker of a drain, and each of its
children, the ``AppTelemetry`` of its app (``worker_drain_telemetry_app.py``)."""


def app_telemetry_from_environment() -> AppTelemetry:
    """The ``AppTelemetry`` that the harness gives the worker it starts.

    Raises:
        RuntimeError: ``APP_TELEMETRY_VARIABLE`` is not set, or holds no
            value of ``AppTelemetry``: a process that the harness did not
            start loads the app module of the worker.
    """
    text = os.environ.get(APP_TELEMETRY_VARIABLE, '')
    for app_telemetry in AppTelemetry:
        if app_telemetry.value == text:
            return app_telemetry
    raise RuntimeError(
        f'{APP_TELEMETRY_VARIABLE} is {text!r}, not one of '
        f'{[app_telemetry.value for app_telemetry in AppTelemetry]}: the '
        'measurement harness sets it for the worker that it starts'
    )


OFF_APP_LOCATOR: Final = 'crossclient.perf.worker_drain_app:app'
"""The app of a worker with ``AppTelemetry.NONE``: ``worker_drain_app.py``."""

TELEMETRY_APP_LOCATOR: Final = 'crossclient.perf.worker_drain_telemetry_app:app'
"""The app of a worker with tracing: ``worker_drain_telemetry_app.py``."""


@dataclass(frozen=True, slots=True)
class ListenOn:
    """The worker serves its endpoint on ``host:port``."""

    host: str
    port: int

    def argument(self) -> str:
        """The value of ``--metrics-listen``."""
        return f'{self.host}:{self.port}'


@dataclass(frozen=True, slots=True)
class NoEndpoint:
    """The worker serves no endpoint: no ``--metrics-listen``."""


type MetricsEndpoint = ListenOn | NoEndpoint


@dataclass(frozen=True, slots=True)
class ReadEvery:
    """The harness reads ``/metrics`` and ``/livez`` every ``interval`` while
    the drain runs."""

    interval: timedelta


@dataclass(frozen=True, slots=True)
class NotRead:
    """Nothing reads the endpoint while the drain runs."""


type DrainScrape = ReadEvery | NotRead


@dataclass(frozen=True, slots=True)
class WorkerTelemetry:
    """The telemetry of the worker of one side of a drain."""

    level: TelemetryLevel
    app: AppTelemetry
    endpoint: MetricsEndpoint
    scrape: DrainScrape

    def app_locator(self) -> str:
        """The app that ``horsies worker`` loads."""
        match self.app:
            case AppTelemetry.NONE:
                return OFF_APP_LOCATOR
            case (
                AppTelemetry.TRACING
                | AppTelemetry.TRACING_AND_HANDLER
                | AppTelemetry.TRACING_HANDLER_AND_WORKER_ENDED_SPANS
            ):
                return TELEMETRY_APP_LOCATOR
            case _ as unreachable:
                assert_never(unreachable)

    def worker_arguments(self) -> tuple[str, ...]:
        """The arguments of ``horsies worker`` after ``--log-level``."""
        match self.endpoint:
            case ListenOn() as listen:
                return (f'--metrics-listen={listen.argument()}',)
            case NoEndpoint():
                return ()
            case _ as unreachable:
                assert_never(unreachable)

    def environment(self) -> dict[str, str]:
        """The variables that the worker and its children need for the app."""
        match self.app:
            case AppTelemetry.NONE:
                return {}
            case (
                AppTelemetry.TRACING
                | AppTelemetry.TRACING_AND_HANDLER
                | AppTelemetry.TRACING_HANDLER_AND_WORKER_ENDED_SPANS
            ):
                return {APP_TELEMETRY_VARIABLE: self.app.value}
            case _ as unreachable:
                assert_never(unreachable)


_LISTEN: Final = ListenOn(host=METRICS_LISTEN_HOST, port=METRICS_LISTEN_PORT)
_SCRAPED: Final = ReadEvery(interval=SCRAPE_INTERVAL)


def worker_telemetry(level: TelemetryLevel) -> WorkerTelemetry:
    """The worker of ``level``. Pure."""
    match level:
        case TelemetryLevel.OFF:
            return WorkerTelemetry(level, AppTelemetry.NONE, NoEndpoint(), NotRead())
        case TelemetryLevel.METRICS:
            return WorkerTelemetry(level, AppTelemetry.NONE, _LISTEN, NotRead())
        case TelemetryLevel.SCRAPED:
            return WorkerTelemetry(level, AppTelemetry.NONE, _LISTEN, _SCRAPED)
        case TelemetryLevel.TRACING:
            return WorkerTelemetry(level, AppTelemetry.TRACING, _LISTEN, _SCRAPED)
        case TelemetryLevel.HANDLER:
            return WorkerTelemetry(
                level, AppTelemetry.TRACING_AND_HANDLER, _LISTEN, _SCRAPED
            )
        case TelemetryLevel.WORKER_ENDED_SPANS:
            return WorkerTelemetry(
                level,
                AppTelemetry.TRACING_HANDLER_AND_WORKER_ENDED_SPANS,
                _LISTEN,
                _SCRAPED,
            )
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class ProducerLevel:
    """The producer of a public call scenario at a level: its app."""

    app: AppTelemetry


@dataclass(frozen=True, slots=True)
class WorkerOnlyLevel:
    """A level that changes only the worker: a public call scenario, which
    runs no worker, does not take it."""

    level: TelemetryLevel


type ProducerTelemetry = ProducerLevel | WorkerOnlyLevel


def producer_telemetry(level: TelemetryLevel) -> ProducerTelemetry:
    """The producer of ``level``. Pure."""
    match level:
        case TelemetryLevel.OFF:
            return ProducerLevel(AppTelemetry.NONE)
        case TelemetryLevel.TRACING:
            return ProducerLevel(AppTelemetry.TRACING)
        case (
            TelemetryLevel.METRICS
            | TelemetryLevel.SCRAPED
            | TelemetryLevel.HANDLER
            | TelemetryLevel.WORKER_ENDED_SPANS
        ):
            return WorkerOnlyLevel(level)
        case _ as unreachable:
            assert_never(unreachable)


def level_description(level: TelemetryLevel) -> str:
    """What the candidate side of ``level`` turns on, for the summary."""
    match level:
        case TelemetryLevel.OFF:
            return 'no telemetry'
        case TelemetryLevel.METRICS:
            return (
                f'`--metrics-listen {_LISTEN.argument()}`: the worker records '
                'its metrics; nothing reads the endpoint during the drain'
            )
        case TelemetryLevel.SCRAPED:
            return (
                'metrics, and a read of `/metrics` and `/livez` every '
                f'{SCRAPE_INTERVAL.total_seconds():g} s during the drain'
            )
        case TelemetryLevel.TRACING:
            return (
                'scraped, and `OtelTracing(worker_ended_spans=NO_SPANS)`: send '
                'spans and producer metrics in the producer, attempt spans in '
                'the task children, each span exported by a '
                '`BatchSpanProcessor` to an exporter that discards'
            )
        case TelemetryLevel.HANDLER:
            return (
                'tracing, and one `def` handler of `AttemptEnded` that returns '
                'at once: the worker runs its handler child'
            )
        case TelemetryLevel.WORKER_ENDED_SPANS:
            return (
                'handler, and `worker_ended_spans=MAKE_SPANS` with the '
                'discarding exporter in the handler child'
            )
        case _ as unreachable:
            assert_never(unreachable)
