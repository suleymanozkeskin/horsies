"""Where a sync ``enqueue`` writes its row (``PostgresBroker.send_route``),
and the driver form of the enqueue INSERT."""

from __future__ import annotations

import re

import pytest
from pydantic import SecretStr

from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.postgres import (
    ENQUEUE_INSERT_DRIVER_SQL,
    ENQUEUE_INSERT_SQL,
    PostgresBroker,
    SyncSendRoute,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.process_role import ProcessRole

pytestmark = [pytest.mark.unit]

URL = 'postgresql+psycopg://user:pass@localhost/db'


@pytest.mark.parametrize(
    ('role', 'idempotency_key', 'initialized', 'route'),
    [
        (ProcessRole.RUNS_NO_TASK_BODIES, None, True, SyncSendRoute.CALLER_THREAD),
        (ProcessRole.RUNS_NO_TASK_BODIES, 'order-17', True, SyncSendRoute.LOOP_RUNNER),
        (ProcessRole.RUNS_NO_TASK_BODIES, None, False, SyncSendRoute.LOOP_RUNNER),
        (ProcessRole.RUNS_TASK_BODIES, None, True, SyncSendRoute.LOOP_RUNNER),
    ],
    ids=['producer', 'keyed', 'schema_not_checked', 'worker_child'],
)
def test_send_route(
    monkeypatch: pytest.MonkeyPatch,
    role: ProcessRole,
    idempotency_key: str | None,
    initialized: bool,
    route: SyncSendRoute,
) -> None:
    monkeypatch.setattr(postgres_module, 'process_role', lambda: role)
    broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(URL)),
        assume_initialized=initialized,
    )

    assert broker.send_route(idempotency_key) is route


def test_the_driver_insert_binds_the_same_parameters() -> None:
    """The psycopg form keeps every bind of the SQLAlchemy statement, as
    ``%(name)s``, and nothing else that psycopg would read as a bind."""
    driver_binds = re.findall(r'%\((\w+)\)s', ENQUEUE_INSERT_DRIVER_SQL)

    assert set(driver_binds) == set(ENQUEUE_INSERT_SQL.compile().params)
    assert ENQUEUE_INSERT_DRIVER_SQL.count('%') == len(driver_binds)
