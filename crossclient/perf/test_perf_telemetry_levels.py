"""The telemetry levels of the overhead measurement: what each one turns on
in the worker of a drain and in the producer of a public call, and that the
harness runs the level ``off`` without the telemetry API of the library."""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Final

import pytest

from crossclient.perf.telemetry_levels import (
    APP_TELEMETRY_VARIABLE,
    OFF_APP_LOCATOR,
    TELEMETRY_APP_LOCATOR,
    AppTelemetry,
    ListenOn,
    NoEndpoint,
    NotRead,
    ProducerLevel,
    ReadEvery,
    TelemetryLevel,
    WorkerOnlyLevel,
    app_telemetry_from_environment,
    producer_telemetry,
    worker_telemetry,
)

pytestmark = [pytest.mark.unit]

IMPORT_PROBE_TIMEOUT_S: Final = 60
"""Seconds that the probe process gets to import the harness."""

CHECKOUT_ROOT: Final = Path(__file__).resolve().parents[2]

_LISTEN: Final = ListenOn(host='127.0.0.1', port=19464)
_READ: Final = ReadEvery(interval=timedelta(seconds=1))


@pytest.mark.parametrize(
    ('level', 'app', 'endpoint', 'scrape'),
    [
        (TelemetryLevel.OFF, AppTelemetry.NONE, NoEndpoint(), NotRead()),
        (TelemetryLevel.METRICS, AppTelemetry.NONE, _LISTEN, NotRead()),
        (TelemetryLevel.SCRAPED, AppTelemetry.NONE, _LISTEN, _READ),
        (TelemetryLevel.TRACING, AppTelemetry.TRACING, _LISTEN, _READ),
        (TelemetryLevel.HANDLER, AppTelemetry.TRACING_AND_HANDLER, _LISTEN, _READ),
        (
            TelemetryLevel.WORKER_ENDED_SPANS,
            AppTelemetry.TRACING_HANDLER_AND_WORKER_ENDED_SPANS,
            _LISTEN,
            _READ,
        ),
    ],
    ids=lambda value: value.value if isinstance(value, TelemetryLevel) else '',
)
def test_each_level_holds_the_parts_of_the_level_before_it(
    level: TelemetryLevel,
    app: AppTelemetry,
    endpoint: ListenOn | NoEndpoint,
    scrape: ReadEvery | NotRead,
) -> None:
    worker = worker_telemetry(level)
    assert (worker.level, worker.app, worker.endpoint, worker.scrape) == (
        level,
        app,
        endpoint,
        scrape,
    )


def test_a_worker_with_no_endpoint_gets_no_listen_argument() -> None:
    assert worker_telemetry(TelemetryLevel.OFF).worker_arguments() == ()


def test_a_worker_with_an_endpoint_gets_its_address() -> None:
    assert worker_telemetry(TelemetryLevel.METRICS).worker_arguments() == (
        '--metrics-listen=127.0.0.1:19464',
    )


def test_a_worker_with_no_tracing_loads_the_app_of_no_telemetry() -> None:
    worker = worker_telemetry(TelemetryLevel.SCRAPED)
    assert worker.app_locator() == OFF_APP_LOCATOR
    assert worker.environment() == {}


def test_a_worker_with_tracing_loads_the_telemetry_app_with_its_kind() -> None:
    worker = worker_telemetry(TelemetryLevel.HANDLER)
    assert worker.app_locator() == TELEMETRY_APP_LOCATOR
    assert worker.environment() == {
        APP_TELEMETRY_VARIABLE: AppTelemetry.TRACING_AND_HANDLER.value,
    }


def test_the_producer_takes_off_and_tracing() -> None:
    assert producer_telemetry(TelemetryLevel.OFF) == ProducerLevel(AppTelemetry.NONE)
    assert producer_telemetry(TelemetryLevel.TRACING) == ProducerLevel(
        AppTelemetry.TRACING
    )


@pytest.mark.parametrize(
    'level',
    [
        TelemetryLevel.METRICS,
        TelemetryLevel.SCRAPED,
        TelemetryLevel.HANDLER,
        TelemetryLevel.WORKER_ENDED_SPANS,
    ],
    ids=lambda level: level.value,
)
def test_a_level_that_changes_only_the_worker_is_not_a_producer_level(
    level: TelemetryLevel,
) -> None:
    assert producer_telemetry(level) == WorkerOnlyLevel(level)


def test_the_worker_reads_its_app_telemetry_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(APP_TELEMETRY_VARIABLE, AppTelemetry.TRACING.value)
    assert app_telemetry_from_environment() is AppTelemetry.TRACING


@pytest.mark.parametrize('value', [None, 'traced'], ids=['unset', 'unknown'])
def test_a_worker_without_a_known_app_telemetry_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
) -> None:
    match value:
        case None:
            monkeypatch.delenv(APP_TELEMETRY_VARIABLE, raising=False)
        case str():
            monkeypatch.setenv(APP_TELEMETRY_VARIABLE, value)
    with pytest.raises(RuntimeError, match=APP_TELEMETRY_VARIABLE):
        _ = app_telemetry_from_environment()


def test_the_harness_imports_no_telemetry_api_for_the_level_off() -> None:
    """The comparison with the build before telemetry runs this harness on a
    library without the tracing setting: the entry point must not import
    the module of the apps with tracing, nor OpenTelemetry."""
    probe = subprocess.run(
        [
            sys.executable,
            '-c',
            'import sys, crossclient.perf.__main__\n'
            'loaded = sorted(name for name in sys.modules if name == "opentelemetry"'
            ' or name.startswith("opentelemetry.")'
            ' or name == "crossclient.perf.telemetry_apps")\n'
            'print(loaded)',
        ],
        cwd=CHECKOUT_ROOT,
        capture_output=True,
        text=True,
        timeout=IMPORT_PROBE_TIMEOUT_S,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr
    assert probe.stdout.strip() == '[]'
