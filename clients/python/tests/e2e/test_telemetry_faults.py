"""E2E: telemetry faults against the three rules of telemetry.

Each test starts one worker with a telemetry fault and asserts the rules:

1. No task outcome changes: every task sent during the fault completes with
   its result (``_assert_wave_completes``).
2. The worker never stops: it runs through the fault, and exits 0 within
   ``WORKER_STOP_BOUND`` after SIGTERM (``_stop``).
3. The worker parent never waits for telemetry: each wave of tasks completes
   within ``WAVE_BOUND`` while the fault holds, and the faults that hold the
   endpoint or a handler hold it for longer than that.

The faults: the handler child crashes at each event; a handler blocks; the
endpoint port is busy at the start; the Collector is absent; the exporter
hangs; a trace carrier is over its bound; a slow scrape client holds every
connection of the endpoint.

The worker is the ``horsies`` script of this environment, started directly,
so its process is the worker parent: its exit code is the worker's.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sysconfig
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final, cast

import pytest
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import get_global_textmap, set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.propagators.textmap import (
    CarrierT,
    Getter,
    Setter,
    TextMapPropagator,
    default_getter,
    default_setter,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.tracing import OtelTracing
from horsies.core.task_decorator import TaskFunction, TaskHandle

from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.worker import kill_stale_workers, worker_env
from tests.e2e.tasks import instance_telemetry_faults as instance
from tests.e2e.tasks.instance_telemetry_faults import (
    COLLECTOR_PORT_ENV,
    TELEMETRY_FAULT_ENV,
    TelemetryFault,
)

pytestmark = [pytest.mark.e2e]

INSTANCE: Final = 'tests.e2e.tasks.instance_telemetry_faults:app'
PROFILE: Final = 'isolated-2'

ENDPOINT_MAX_CONNECTIONS: Final = 16
"""``ENDPOINT_MAX_CONNECTIONS`` of ``core/src/telemetry/endpoint.rs``."""
ENDPOINT_REQUEST_TIMEOUT: Final = timedelta(seconds=5)
"""``ENDPOINT_REQUEST_TIMEOUT`` of ``core/src/telemetry/endpoint.rs``."""
MAX_TRACE_CARRIER_KEYS: Final = 8
"""``MAX_TRACE_CARRIER_KEYS`` of ``core/src/telemetry/trace_carrier.rs``."""

READY_BOUND: Final = timedelta(seconds=60)
"""The start of the worker until its first task completes: the children and,
with a handler, the handler child (at most 30 s for its ready frame)."""
WAVE_TASKS: Final = 10
WAVE_BOUND: Final = timedelta(seconds=30)
"""The longest time of one wave of ``WAVE_TASKS`` no-op tasks. A worker that
waited for a blocked handler, a hung exporter or a held endpoint would not
complete a wave."""
WORKER_STOP_BOUND: Final = timedelta(seconds=45)
"""SIGTERM until the exit of the worker. The stop bounds each child: the
handler child gets 2 s (``HANDLER_STOP_FLUSH``) for its events, each child
10 s (``CHILD_STOP_TIMEOUT``) to exit after its stop frame, then a kill."""
STATE_BOUND: Final = timedelta(seconds=30)
"""The wait for a state in the worker snapshot, written every second, and
for the restart of a part after the slot backoff."""
ENDPOINT_UP_BOUND: Final = timedelta(seconds=90)
"""The wait for the endpoint after its port is free: the bind is tried again
after the slot backoff, at most 60 s (``SLOT_DOWN_MAX_DELAY``)."""
SERVER_CLOSE_BOUND: Final = ENDPOINT_REQUEST_TIMEOUT + timedelta(seconds=5)
"""The wait for the endpoint to close a connection that sends nothing."""
PLACE_FREED_PAUSE: Final = timedelta(seconds=1)
"""The wait after a read of the endpoint for the endpoint to free the place
of that connection under ``ENDPOINT_MAX_CONNECTIONS``."""
HTTP_TIMEOUT: Final = timedelta(seconds=2)
HTTP_BODY_MAX_BYTES: Final = 16 * 1024 * 1024
POLL_INTERVAL: Final = timedelta(milliseconds=200)
MAX_WAVES: Final = 10
"""The most waves that a test sends while it waits for a state of the
fault."""
LOG_TAIL_BYTES: Final = 8_192


# --- The worker ---------------------------------------------------------------


@dataclass(frozen=True)
class RunningWorker:
    """A worker that runs: its process and the file of its output."""

    process: subprocess.Popen[bytes]
    log_path: Path


@dataclass(frozen=True)
class WorkerStop:
    """The exit code of a stopped worker and the time it took to exit."""

    exit_code: int
    seconds: float


def _log_tail(path: Path) -> str:
    try:
        with path.open('rb') as log:
            log.seek(0, os.SEEK_END)
            log.seek(max(0, log.tell() - LOG_TAIL_BYTES))
            return log.read().decode('utf-8', 'replace')
    except OSError as error:
        return f'<log not readable: {error}>'


def _kill_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    _ = process.wait(timeout=10)


@contextmanager
def _worker(
    fault: TelemetryFault,
    *,
    extra_arguments: Sequence[str] = (),
    extra_environment: Mapping[str, str] | None = None,
) -> Generator[RunningWorker, None, None]:
    """Start ``horsies worker`` of the instance with ``fault``; on exit, kill
    the process group of a worker that a test did not stop, and print the
    tail of its log."""
    kill_stale_workers()
    environment = {
        **worker_env(),
        TELEMETRY_FAULT_ENV: fault.value,
        **(extra_environment or {}),
    }
    handle, name = tempfile.mkstemp(prefix='horsies-e2e-telemetry-', suffix='.log')
    process = subprocess.Popen(
        [
            str(Path(sysconfig.get_path('scripts')) / 'horsies'),
            'worker',
            INSTANCE,
            f'--profile={PROFILE}',
            '--loglevel=warning',
            *extra_arguments,
        ],
        stdout=handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=environment,
    )
    os.close(handle)
    worker = RunningWorker(process=process, log_path=Path(name))
    try:
        yield worker
    finally:
        if process.poll() is None:
            _kill_group(process)
        print(f'--- worker {process.pid} log tail ---\n{_log_tail(worker.log_path)}')
        worker.log_path.unlink(missing_ok=True)


def _stop(worker: RunningWorker) -> WorkerStop:
    """SIGTERM to the worker parent, and its exit within
    ``WORKER_STOP_BOUND``. Fails the test when it does not exit."""
    assert (
        worker.process.poll() is None
    ), f'the worker exited before its stop with {worker.process.returncode}'
    started = time.monotonic()
    worker.process.send_signal(signal.SIGTERM)
    try:
        exit_code = worker.process.wait(timeout=WORKER_STOP_BOUND.total_seconds())
    except subprocess.TimeoutExpired:
        pytest.fail(f'the worker did not exit within {WORKER_STOP_BOUND} of SIGTERM')
    return WorkerStop(exit_code=exit_code, seconds=time.monotonic() - started)


def _assert_stops_clean(worker: RunningWorker) -> None:
    """Rule 2: the worker ran through the fault and stops as a worker with
    no fault stops."""
    stop = _stop(worker)
    assert stop.exit_code == 0, f'exit {stop.exit_code} after SIGTERM'


def _assert_running(worker: RunningWorker) -> None:
    assert (
        worker.process.poll() is None
    ), f'the worker stopped during the fault with {worker.process.returncode}'


# --- Tasks --------------------------------------------------------------------


async def _wait_ready(worker: RunningWorker) -> None:
    """The first healthcheck task completes within ``READY_BOUND``."""
    handle = unwrap_send(await instance.healthcheck.send_async())
    deadline = time.monotonic() + READY_BOUND.total_seconds()
    while time.monotonic() < deadline:
        _assert_running(worker)
        result = await handle.get_async(timeout_ms=1_000)
        if result.is_ok():
            assert result.ok_value == 'ready'
            return
    pytest.fail(f'the worker did not run its first task within {READY_BOUND}')


async def _send_wave(wave: int) -> list[tuple[TaskHandle[int], int]]:
    sent: list[tuple[TaskHandle[int], int]] = []
    for index in range(WAVE_TASKS):
        handle = unwrap_send(await instance.add.send_async(left=wave, right=index))
        sent.append((handle, wave + index))
    return sent


async def _assert_wave_completes(wave: int) -> None:
    """Rules 1 and 3: every task of a new wave completes with its result
    within ``WAVE_BOUND``."""
    started = time.monotonic()
    for handle, expected in await _send_wave(wave):
        remaining_ms = int(
            max(0.0, WAVE_BOUND.total_seconds() - (time.monotonic() - started)) * 1_000
        )
        result = await handle.get_async(timeout_ms=max(1, remaining_ms))
        assert result.is_ok(), f'task {handle.task_id} of wave {wave}: {result}'
        assert result.ok_value == expected


# --- The snapshot and the endpoint --------------------------------------------


async def _database_now(session: AsyncSession) -> datetime:
    now = (await session.execute(text('SELECT now()'))).scalar_one()
    await session.commit()
    return cast(datetime, now)


async def _telemetry_states(
    session: AsyncSession,
    since: datetime,
    part: str,
) -> list[dict[str, object]]:
    """The ``part`` member of the ``telemetry`` column of each snapshot
    written since ``since``, oldest first."""
    rows = (
        await session.execute(
            text(
                'SELECT telemetry FROM horsies_worker_states '
                'WHERE snapshot_at >= :since ORDER BY snapshot_at'
            ),
            {'since': since},
        )
    ).scalars()
    states = [cast(dict[str, dict[str, object]], row)[part] for row in rows]
    await session.commit()
    return states


async def _wait_for_state(
    session: AsyncSession,
    since: datetime,
    part: str,
    state: str,
) -> dict[str, object]:
    """The first snapshot since ``since`` whose ``part`` is in ``state``,
    within ``STATE_BOUND``."""
    deadline = time.monotonic() + STATE_BOUND.total_seconds()
    seen: list[dict[str, object]] = []
    while time.monotonic() < deadline:
        seen = await _telemetry_states(session, since, part)
        for member in seen:
            if member.get('state') == state:
                return member
        await asyncio.sleep(POLL_INTERVAL.total_seconds())
    pytest.fail(f'no snapshot with {part} {state} within {STATE_BOUND}; seen: {seen}')


@dataclass(frozen=True)
class HttpAnswer:
    status: int
    body: bytes


@dataclass(frozen=True)
class HttpNoAnswer:
    cause: str


def _get(address: str, path: str) -> HttpAnswer | HttpNoAnswer:
    try:
        with urllib.request.urlopen(
            f'http://{address}{path}', timeout=HTTP_TIMEOUT.total_seconds()
        ) as answer:
            return HttpAnswer(
                status=answer.status, body=answer.read(HTTP_BODY_MAX_BYTES)
            )
    except urllib.error.HTTPError as refused:
        return HttpAnswer(status=refused.code, body=refused.read(HTTP_BODY_MAX_BYTES))
    except (urllib.error.URLError, OSError) as failure:
        return HttpNoAnswer(cause=f'{type(failure).__name__}: {failure}')


async def _wait_for_metrics(address: str, bound: timedelta) -> bytes:
    """The body of the first ``/metrics`` answer 200 within ``bound``."""
    deadline = time.monotonic() + bound.total_seconds()
    last: HttpAnswer | HttpNoAnswer = HttpNoAnswer(cause='no request sent')
    while time.monotonic() < deadline:
        last = await asyncio.to_thread(_get, address, '/metrics')
        match last:
            case HttpAnswer(status=200, body=body):
                return body
            case HttpAnswer() | HttpNoAnswer():
                await asyncio.sleep(POLL_INTERVAL.total_seconds())
    pytest.fail(f'/metrics of {address} did not answer 200 within {bound}: {last}')


def _counter(exposition: bytes, name: str, label: str) -> float:
    """The value of the series ``name{label}`` in a Prometheus text answer;
    0 when the series is absent (no sample yet)."""
    prefix = f'{name}{{{label}}} '
    for line in exposition.decode('utf-8', 'replace').splitlines():
        if line.startswith(prefix):
            return float(line[len(prefix) :])
    return 0.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        return int(probe.getsockname()[1])


@pytest.fixture
async def clean_snapshots(session: AsyncSession) -> AsyncIterator[datetime]:
    """Delete the snapshots of earlier workers; give the database time of
    the start of the test."""
    await session.execute(text('DELETE FROM horsies_worker_states'))
    await session.commit()
    yield await _database_now(session)


# --- The faults ---------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='session')
async def test_a_handler_child_that_crashes_at_each_event_changes_no_task(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    session: AsyncSession,
    clean_snapshots: datetime,
) -> None:
    """The handler child ends at its first event, boots again after the
    slot backoff, and ends again. The worker claims through it, the events
    of the down times drop and count ``child_down``, and the snapshot shows
    the handler child down."""
    address = f'127.0.0.1:{_free_port()}'
    with _worker(
        TelemetryFault.HANDLER_CRASHES,
        extra_arguments=(f'--metrics-listen={address}',),
    ) as worker:
        await _wait_ready(worker)
        down: dict[str, object] | None = None
        dropped = 0.0
        for wave in range(MAX_WAVES):
            await _assert_wave_completes(wave)
            states = await _telemetry_states(session, clean_snapshots, 'handler_child')
            down = next((s for s in states if s.get('state') == 'down'), down)
            exposition = await _wait_for_metrics(address, STATE_BOUND)
            dropped = _counter(
                exposition,
                'horsies_telemetry_events_dropped_total',
                'cause="child_down"',
            )
            if down is not None and dropped > 0:
                break
        assert down is not None, 'no snapshot showed the handler child down'
        assert cast(dict[str, object], down['cause'])['kind'] == 'lost'
        assert dropped > 0
        _assert_running(worker)
        _assert_stops_clean(worker)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_handler_that_never_returns_changes_no_task(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    session: AsyncSession,
    clean_snapshots: datetime,
) -> None:
    """The handler of ``AttemptEnded`` blocks on its first event and never
    returns. Its events wait in its own queue; the worker claims, the
    handler child stays up, and the stop does not wait for the handler."""
    address = f'127.0.0.1:{_free_port()}'
    with _worker(
        TelemetryFault.HANDLER_BLOCKS,
        extra_arguments=(f'--metrics-listen={address}',),
    ) as worker:
        await _wait_ready(worker)
        for wave in range(3):
            await _assert_wave_completes(wave)
        match await asyncio.to_thread(_get, address, '/livez'):
            case HttpAnswer(status=200):
                pass
            case other:
                pytest.fail(f'/livez while the handler blocks: {other}')
        states = await _telemetry_states(session, clean_snapshots, 'handler_child')
        assert states, 'the worker wrote no snapshot'
        assert states[-1]['state'] == 'up', states[-1]
        _assert_stops_clean(worker)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_busy_endpoint_port_changes_no_task_and_binds_when_free(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    session: AsyncSession,
    clean_snapshots: datetime,
) -> None:
    """Another process holds the port of ``--metrics-listen`` at the start.
    The worker starts and claims, the snapshot shows the endpoint down with
    a failed bind, and the endpoint serves once the port is free."""
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.bind(('127.0.0.1', 0))
    holder.listen()
    address = f'127.0.0.1:{int(holder.getsockname()[1])}'
    try:
        with _worker(
            TelemetryFault.NONE,
            extra_arguments=(f'--metrics-listen={address}',),
        ) as worker:
            await _wait_ready(worker)
            await _assert_wave_completes(0)
            down = await _wait_for_state(session, clean_snapshots, 'endpoint', 'down')
            assert cast(dict[str, object], down['cause'])['kind'] == 'bind_failed'
            holder.close()
            await _assert_wave_completes(1)
            exposition = await _wait_for_metrics(address, ENDPOINT_UP_BOUND)
            assert b'horsies_task_attempts_total' in exposition
            _ = await _wait_for_state(session, clean_snapshots, 'endpoint', 'serving')
            _assert_stops_clean(worker)
    finally:
        holder.close()


@pytest.mark.asyncio(loop_scope='session')
async def test_an_absent_collector_changes_no_task(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
) -> None:
    """Each task child exports its attempt spans to a Collector address
    where nothing listens. Each export fails; the tasks complete, and the
    stop of the children is not held by the exporter."""
    with _worker(
        TelemetryFault.COLLECTOR_ABSENT,
        extra_environment={COLLECTOR_PORT_ENV: str(_free_port())},
    ) as worker:
        await _wait_ready(worker)
        for wave in range(3):
            await _assert_wave_completes(wave)
        _assert_stops_clean(worker)


@pytest.mark.asyncio(loop_scope='session')
async def test_a_hung_exporter_changes_no_task_and_the_flush_bound_holds(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
) -> None:
    """Each task child exports its attempt spans to an exporter that never
    returns. The spans wait in the batch processor and the tasks complete;
    at the stop, the flush of each child ends at its bound and the worker
    exits within ``WORKER_STOP_BOUND``."""
    with _worker(TelemetryFault.EXPORTER_HANGS) as worker:
        await _wait_ready(worker)
        for wave in range(3):
            await _assert_wave_completes(wave)
        _assert_stops_clean(worker)


class _ExtraKeysPropagator(TextMapPropagator):
    """Writes ``count`` keys of its own into each carrier."""

    def __init__(self, count: int) -> None:
        self._count = count

    def extract(
        self,
        carrier: CarrierT,
        context: Context | None = None,
        getter: Getter[CarrierT] = default_getter,
    ) -> Context:
        _ = (carrier, getter)
        return context if context is not None else Context()

    def inject(
        self,
        carrier: CarrierT,
        context: Context | None = None,
        setter: Setter[CarrierT] = default_setter,
    ) -> None:
        _ = context
        for index in range(self._count):
            setter.set(carrier, f'x-e2e-extra-{index}', 'value')

    @property
    def fields(self) -> set[str]:
        return {f'x-e2e-extra-{index}' for index in range(self._count)}


@contextmanager
def _propagator(extra_keys: int) -> Generator[None, None, None]:
    """The W3C propagator with ``extra_keys`` keys more, for the sends of
    the block."""
    previous = get_global_textmap()
    set_global_textmap(
        CompositePropagator(
            [TraceContextTextMapPropagator(), _ExtraKeysPropagator(extra_keys)]
        )
    )
    try:
        yield
    finally:
        set_global_textmap(previous)


@dataclass(frozen=True)
class TracedProducer:
    """A producer app with tracing, and its task with the name of the
    instance task ``add``, so the worker of the instance runs what it
    sends."""

    app: Horsies
    add: TaskFunction[..., int]


def _traced_producer() -> TracedProducer:
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(database_url=SecretStr(instance.DB_URL)),
            tracing=OtelTracing(),
        ),
    )

    @app.task(task_name='e2e_telemetry_faults_add')
    def traced_add(*, left: int, right: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=left + right)

    return TracedProducer(app=app, add=traced_add)


async def _stored_carrier(session: AsyncSession, task_id: str) -> object:
    carrier = (
        await session.execute(
            text('SELECT trace_carrier FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).scalar_one()
    await session.commit()
    return carrier


@pytest.mark.asyncio(loop_scope='session')
async def test_a_carrier_over_its_bound_is_not_stored_and_changes_no_task(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    session: AsyncSession,
) -> None:
    """A send whose propagator writes more than ``MAX_TRACE_CARRIER_KEYS``
    keys succeeds and stores no carrier; a send under the bound stores its
    carrier. A worker then runs both tasks to their results."""
    trace.set_tracer_provider(TracerProvider())
    producer = _traced_producer()
    try:
        with _propagator(extra_keys=0):
            under = unwrap_send(await producer.add.send_async(left=1, right=2))
        with _propagator(extra_keys=MAX_TRACE_CARRIER_KEYS):
            over = unwrap_send(await producer.add.send_async(left=3, right=4))
    finally:
        _ = await producer.app.get_broker().close_async()
    stored_under = await _stored_carrier(session, str(under.task_id))
    assert isinstance(stored_under, str)
    assert 'traceparent' in json.loads(stored_under)
    assert await _stored_carrier(session, str(over.task_id)) is None

    with _worker(TelemetryFault.NONE) as worker:
        await _wait_ready(worker)
        for handle, expected in ((under, 3), (over, 7)):
            result = await handle.get_async(
                timeout_ms=int(WAVE_BOUND.total_seconds() * 1_000)
            )
            assert result.is_ok(), result
            assert result.ok_value == expected
        _assert_stops_clean(worker)


def _held_connections(address: str, count: int) -> list[socket.socket]:
    host, _, port = address.rpartition(':')
    held: list[socket.socket] = []
    for _ in range(count):
        connection = socket.create_connection(
            (host, int(port)), timeout=HTTP_TIMEOUT.total_seconds()
        )
        held.append(connection)
    return held


def _closed_by_server(connection: socket.socket, bound: timedelta) -> bool:
    """Whether the server closes ``connection`` within ``bound`` with no
    answer: a read gives the end of the stream, or the server reset it."""
    connection.settimeout(bound.total_seconds())
    try:
        return connection.recv(1) == b''
    except ConnectionResetError:
        return True
    except TimeoutError:
        return False


@pytest.mark.asyncio(loop_scope='session')
async def test_a_slow_scrape_client_changes_no_task_and_the_request_bounds_hold(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
) -> None:
    """A client opens ``ENDPOINT_MAX_CONNECTIONS`` connections to the
    endpoint and sends nothing. A connection over the bound is closed with
    no answer, each held connection is closed after
    ``ENDPOINT_REQUEST_TIMEOUT``, and ``/metrics`` answers after. The tasks
    complete while the client holds the endpoint."""
    address = f'127.0.0.1:{_free_port()}'
    with _worker(
        TelemetryFault.NONE,
        extra_arguments=(f'--metrics-listen={address}',),
    ) as worker:
        await _wait_ready(worker)
        _ = await _wait_for_metrics(address, STATE_BOUND)
        # The read above closed its connection after the answer; the pause
        # lets the endpoint free its place before the client takes them all.
        await asyncio.sleep(PLACE_FREED_PAUSE.total_seconds())
        held = _held_connections(address, ENDPOINT_MAX_CONNECTIONS)
        try:
            over = _held_connections(address, 1)[0]
            try:
                assert _closed_by_server(over, HTTP_TIMEOUT)
            finally:
                over.close()
            await _assert_wave_completes(0)
            closed = await asyncio.gather(
                *(
                    asyncio.to_thread(_closed_by_server, connection, SERVER_CLOSE_BOUND)
                    for connection in held
                )
            )
            assert all(closed), closed
        finally:
            for connection in held:
                connection.close()
        exposition = await _wait_for_metrics(address, STATE_BOUND)
        assert b'horsies_task_attempts_total' in exposition
        await _assert_wave_completes(1)
        _assert_stops_clean(worker)
