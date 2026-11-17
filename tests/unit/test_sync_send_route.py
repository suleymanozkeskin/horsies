"""Where a sync ``enqueue`` writes its row (``PostgresBroker.send_route``),
and the row that the calling thread writes (``_enqueue_on_caller_thread``)."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import SecretStr

from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.enqueue_task import (
    EnqueueOutcome,
    EnqueueRow,
    Inserted,
    Keyed,
    SendKind,
    Unkeyed,
)
from horsies.core.brokers.postgres import (
    PostgresBroker,
    SyncSendRoute,
)
from horsies.core.brokers.result_types import BrokerInvalidInput
from horsies.core.history.identity.keys import ScopedIdempotencyKey
from horsies.core.models.broker import PostgresConfig
from horsies.core.process_role import ProcessRole
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]

URL = 'postgresql://user:pass@localhost/db'
TASK_ID = UUID('0190a5a0-0000-7000-8000-000000000001')


@pytest.mark.parametrize(
    ('role', 'initialized', 'route'),
    [
        (ProcessRole.RUNS_NO_TASK_BODIES, True, SyncSendRoute.CALLER_THREAD),
        (ProcessRole.RUNS_NO_TASK_BODIES, False, SyncSendRoute.LOOP_RUNNER),
        (ProcessRole.RUNS_TASK_BODIES, True, SyncSendRoute.LOOP_RUNNER),
    ],
    ids=['producer', 'schema_not_checked', 'worker_child'],
)
def test_send_route(
    monkeypatch: pytest.MonkeyPatch,
    role: ProcessRole,
    initialized: bool,
    route: SyncSendRoute,
) -> None:
    monkeypatch.setattr(postgres_module, 'process_role', lambda: role)
    broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(URL)),
        assume_initialized=initialized,
    )

    assert broker.send_route() is route


def _written_rows(
    broker: PostgresBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> list[EnqueueRow]:
    """Replace the statement on the calling thread; collect each row."""
    rows: list[EnqueueRow] = []

    def _write(row: EnqueueRow, send_kind: SendKind) -> EnqueueOutcome:
        _ = send_kind
        rows.append(row)
        return Inserted()

    monkeypatch.setattr(broker, '_enqueue_task_on_caller_thread', _write)
    return rows


def _send_on_caller_thread(
    broker: PostgresBroker,
    idempotency_key: str | None,
) -> object:
    return broker._enqueue_on_caller_thread(
        'my_task',
        'default',
        task_id=TASK_ID,
        send_kind=SendKind.FIRST,
        input_json='{"value": 1}',
        priority=100,
        sent_at=datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc),
        enqueued_at=None,
        enqueue_delay_seconds=None,
        good_until=None,
        task_options=None,
        retention_class_key=None,
        retain_rerun_input=None,
        idempotency_key=idempotency_key,
    )


def test_a_keyed_send_on_the_calling_thread_writes_its_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(URL)))
    rows = _written_rows(broker, monkeypatch)

    result = _send_on_caller_thread(broker, 'order-17')

    assert result == Ok(TASK_ID)
    match rows:
        case [EnqueueRow(key=Keyed(digest=digest, window=window))]:
            assert digest == ScopedIdempotencyKey(task_name='my_task', key='order-17').digest
            assert window == broker.idempotency_reservation_window
        case _:
            raise AssertionError(f'expected one keyed row, got {rows!r}')


def test_an_unkeyed_send_on_the_calling_thread_writes_no_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(URL)))
    rows = _written_rows(broker, monkeypatch)

    result = _send_on_caller_thread(broker, None)

    assert result == Ok(TASK_ID)
    match rows:
        case [EnqueueRow(key=Unkeyed())]:
            pass
        case _:
            raise AssertionError(f'expected one unkeyed row, got {rows!r}')


def test_a_refused_key_on_the_calling_thread_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(URL)))
    rows = _written_rows(broker, monkeypatch)

    result = _send_on_caller_thread(broker, '')

    match result:
        case Err(BrokerInvalidInput()):
            pass
        case _:
            raise AssertionError(f'expected INVALID_INPUT, got {result!r}')
    assert rows == []
