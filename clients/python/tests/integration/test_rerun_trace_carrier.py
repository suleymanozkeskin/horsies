"""The trace carrier of a rerun on PostgreSQL.

`Horsies.rerun_task_async` and `Horsies.rerun_task` of an app with
`OtelTracing` store, on the new task, the carrier that the configured
propagator injects for the caller's current span, and its trace id. Outside
a span, with `NoTracing`, and through the free `rerun_task`, the new task
stores none. With `NoTracing` the app rerun runs in a child interpreter that
refuses every `opentelemetry` module, and loads none.

The caller's span is a non-recording span of a fixed span context, so the
carrier is known before the rerun. Each test sets the global propagator to
the W3C propagator and restores the one that it found.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Final
from uuid import UUID

import pytest
import pytest_asyncio
from opentelemetry import propagate, trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import SecretStr
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.app import Horsies
from horsies.core.history.rerun.operations import (
    RerunEnqueued,
    RerunOutcome,
    RerunTask,
    rerun_task,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tracing import NoTracing, OtelTracing

from tests.integration.task_history_harness import (
    HistorySchema,
    prepare_move_storage,
    terminalization_schema_fixture,
)
from tests.integration.test_task_history_rerun import (
    CLASS_DAYS,
    CLASS_KEY,
    policy,
    seed_failed_source,
)

pytestmark = [pytest.mark.integration]

CALLER_TRACE_ID: Final = 0x4BF9_2F35_77B3_4DA6_A3CE_929D_0E0E_4736
"""The trace of the caller of each rerun."""

CALLER_SPAN_ID: Final = 0x00F0_67AA_0BA9_02B7
"""The span of the caller of each rerun."""

CALLER_CARRIER: Final = (
    '{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}'
)
"""The carrier that the W3C propagator injects for the caller's span, as the
core stores it."""

UNREACHABLE_BROKER_URL: Final = 'postgresql://user:pass@127.0.0.1:1/none'
"""The broker URL of each app: a rerun runs on the caller's connection and
never opens the broker."""

terminalization_schema = terminalization_schema_fixture('task_history_it_rerun_trace')


@dataclass(frozen=True, slots=True)
class StoredTrace:
    """The trace columns of a task row."""

    carrier: str | None
    trace_id: bytes | None


CALLER_TRACE: Final = StoredTrace(
    carrier=CALLER_CARRIER,
    trace_id=CALLER_TRACE_ID.to_bytes(16, 'big'),
)

NO_STORED_TRACE: Final = StoredTrace(carrier=None, trace_id=None)


@pytest_asyncio.fixture()
async def python_family(terminalization_schema: HistorySchema) -> None:
    """The family row that the install of a Python broker records: the core
    reruns only in a database of its own family."""
    async with terminalization_schema.engine.begin() as connection:
        _ = await connection.execute(
            text(
                'INSERT INTO horsies_client_family (only_row, family, recorded_at) '
                "VALUES (TRUE, 'PYTHON', now())"
            )
        )


@pytest.fixture()
def w3c_propagator() -> Generator[None]:
    """The W3C propagator as the global propagator, for one test."""
    previous = propagate.get_global_textmap()
    propagate.set_global_textmap(TraceContextTextMapPropagator())
    yield
    propagate.set_global_textmap(previous)


@contextmanager
def inside_caller_span() -> Generator[None]:
    """The caller's span as the current span."""
    span = NonRecordingSpan(
        SpanContext(
            trace_id=CALLER_TRACE_ID,
            span_id=CALLER_SPAN_ID,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        ),
    )
    with trace.use_span(span):
        yield


def app_with(tracing: OtelTracing | NoTracing) -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_BROKER_URL)),
            tracing=tracing,
        ),
    )


def command(source: str) -> RerunTask:
    return RerunTask(source_task_id=UUID(source), deadline=None)


def enqueued(outcome: RerunOutcome, source: str) -> UUID:
    """The new task id of an enqueued rerun of `source`."""
    assert isinstance(outcome, RerunEnqueued), outcome
    assert outcome.source_task_id == UUID(source)
    return outcome.new_task_id


_STORED_TRACE_SQL: Final = text(
    'SELECT trace_carrier, trace_id FROM horsies_tasks '
    'WHERE id = CAST(:task_id AS uuid)'
)


def stored_trace_of(carrier: object, trace_id: object) -> StoredTrace:
    """The stored trace of the two column values; a `bytea` value read as a
    `memoryview` is taken as `bytes`."""
    assert carrier is None or isinstance(carrier, str), carrier
    match trace_id:
        case None | bytes():
            stored_id = trace_id
        case memoryview():
            stored_id = trace_id.tobytes()
        case _:
            raise AssertionError(f'trace_id is not bytea: {trace_id!r}')
    return StoredTrace(carrier=carrier, trace_id=stored_id)


async def stored_trace(connection: AsyncConnection, task_id: UUID) -> StoredTrace:
    """The stored trace of the live task `task_id`."""
    row = (await connection.execute(_STORED_TRACE_SQL, {'task_id': str(task_id)})).one()
    return stored_trace_of(row.trace_carrier, row.trace_id)


def stored_trace_blocking(connection: Connection, task_id: UUID) -> StoredTrace:
    """`stored_trace` on a blocking connection."""
    row = connection.execute(_STORED_TRACE_SQL, {'task_id': str(task_id)}).one()
    return stored_trace_of(row.trace_carrier, row.trace_id)


async def seeded_source(connection: AsyncConnection) -> str:
    """A failed source whose move retained its input."""
    await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
    return await seed_failed_source(connection)


@pytest.mark.usefixtures('python_family', 'w3c_propagator')
class TestAppRerunCarrier:
    @pytest.mark.asyncio
    async def test_a_rerun_inside_a_span_stores_its_carrier_and_trace_id(
        self, terminalization_schema: HistorySchema
    ) -> None:
        app = app_with(OtelTracing())
        async with terminalization_schema.engine.begin() as connection:
            source = await seeded_source(connection)
            with inside_caller_span():
                outcome = await app.rerun_task_async(connection, command(source), policy())

            new_task_id = enqueued(outcome, source)
            assert await stored_trace(connection, new_task_id) == CALLER_TRACE

    @pytest.mark.asyncio
    async def test_a_blocking_rerun_inside_a_span_stores_its_carrier_and_trace_id(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            source = await seeded_source(connection)
        engine = create_engine(
            terminalization_schema.engine.url,
            connect_args={'options': f'-csearch_path={terminalization_schema.schema_name}'},
        )
        app = app_with(OtelTracing())
        try:
            with engine.begin() as connection:
                with inside_caller_span():
                    outcome = app.rerun_task(connection, command(source), policy())

                new_task_id = enqueued(outcome, source)
                assert stored_trace_blocking(connection, new_task_id) == CALLER_TRACE
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_a_rerun_outside_a_span_stores_no_carrier(
        self, terminalization_schema: HistorySchema
    ) -> None:
        app = app_with(OtelTracing())
        async with terminalization_schema.engine.begin() as connection:
            source = await seeded_source(connection)
            outcome = await app.rerun_task_async(connection, command(source), policy())

            new_task_id = enqueued(outcome, source)
            assert await stored_trace(connection, new_task_id) == NO_STORED_TRACE

    @pytest.mark.asyncio
    async def test_a_rerun_with_tracing_off_stores_no_carrier(
        self, terminalization_schema: HistorySchema
    ) -> None:
        app = app_with(NoTracing())
        async with terminalization_schema.engine.begin() as connection:
            source = await seeded_source(connection)
            with inside_caller_span():
                outcome = await app.rerun_task_async(connection, command(source), policy())

            new_task_id = enqueued(outcome, source)
            assert await stored_trace(connection, new_task_id) == NO_STORED_TRACE

    @pytest.mark.asyncio
    async def test_the_free_rerun_inside_a_span_stores_no_carrier(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            source = await seeded_source(connection)
            with inside_caller_span():
                outcome = await rerun_task(connection, command(source), policy())

            new_task_id = enqueued(outcome, source)
            assert await stored_trace(connection, new_task_id) == NO_STORED_TRACE


_NO_TRACING_RERUN = textwrap.dedent(
    '''
    import os
    import sys
    from datetime import timedelta
    from uuid import UUID

    class BlockOpenTelemetry:
        """Refuses every opentelemetry module, as an environment without
        the API does."""

        def find_spec(self, name, path=None, target=None):
            if name == 'opentelemetry' or name.startswith('opentelemetry.'):
                raise ModuleNotFoundError(f'No module named {name!r}', name=name)
            return None

    sys.meta_path.insert(0, BlockOpenTelemetry())

    from pydantic import SecretStr
    from sqlalchemy import create_engine

    from horsies import (
        AppConfig,
        Horsies,
        PostgresConfig,
        RerunEnqueued,
        RerunEnqueuePolicy,
        RerunTask,
    )

    app = Horsies(
        AppConfig(
            broker=PostgresConfig(database_url=SecretStr(os.environ['RERUN_BROKER_URL'])),
        ),
    )
    engine = create_engine(
        os.environ['RERUN_DATABASE_URL'],
        connect_args={'options': '-csearch_path=' + os.environ['RERUN_SCHEMA']},
    )
    with engine.begin() as connection:
        outcome = app.rerun_task(
            connection,
            RerunTask(source_task_id=UUID(os.environ['RERUN_SOURCE']), deadline=None),
            RerunEnqueuePolicy(
                retention=timedelta(days=int(os.environ['RERUN_DAYS'])),
                retain_rerun_input=True,
                reservation_window=timedelta(hours=24),
            ),
        )
    engine.dispose()
    assert isinstance(outcome, RerunEnqueued), outcome
    loaded = sorted(
        name for name in sys.modules
        if name == 'opentelemetry' or name.startswith('opentelemetry.')
    )
    print(outcome.new_task_id)
    print(loaded)
    ''',
)


@pytest.mark.usefixtures('python_family')
class TestNoTracingRerun:
    @pytest.mark.asyncio
    async def test_a_rerun_with_no_tracing_loads_no_opentelemetry_module(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            source = await seeded_source(connection)

        result = subprocess.run(
            [sys.executable, '-c', _NO_TRACING_RERUN],
            capture_output=True,
            text=True,
            check=False,
            env={
                **os.environ,
                'RERUN_BROKER_URL': UNREACHABLE_BROKER_URL,
                'RERUN_DATABASE_URL': terminalization_schema.engine.url.render_as_string(
                    hide_password=False,
                ),
                'RERUN_SCHEMA': terminalization_schema.schema_name,
                'RERUN_SOURCE': source,
                'RERUN_DAYS': str(CLASS_DAYS),
            },
        )

        assert result.returncode == 0, result.stderr
        new_task_id, loaded = result.stdout.strip().splitlines()
        assert loaded == '[]'
        async with terminalization_schema.engine.connect() as connection:
            assert await stored_trace(connection, UUID(new_task_id)) == NO_STORED_TRACE
