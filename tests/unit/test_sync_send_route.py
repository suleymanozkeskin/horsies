"""Where a sync ``enqueue`` writes its row (``PostgresBroker.send_route``)."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.postgres import (
    PostgresBroker,
    SyncSendRoute,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.process_role import ProcessRole

pytestmark = [pytest.mark.unit]

URL = 'postgresql://user:pass@localhost/db'


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
