"""Dual-run of the claim-time workflow check: the session statements against ``horsies_workflow_claimed_tasks_settle``.

Reference: ``tests/dualrun/reference/claim_settle.py``. Candidate:
``ClaimMixin._filter_nonrunnable_workflow_tasks`` (``core/worker/claiming.py``).
The seed holds claimed workflow tasks of one worker in a PAUSED, a CANCELLED
and a RUNNING workflow, and a task of the PAUSED and of the CANCELLED
workflow that another worker holds. Each side checks the claims of the
worker.

The caller pair runs the same seed with the Python method body before the
switch (``tests/dualrun/reference/claim_settle_caller.py``) against the
method on the core (``CoreHandle.settle_claimed_workflow_tasks_async``).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, LiteralString

import psycopg
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies._core import CoreHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import Err, Ok
from horsies.core.worker.claiming import ClaimMixin
from tests.dualrun.capture import Connection
from tests.dualrun.clients import install_as_a_client, open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import claim_settle as reference
from tests.dualrun.reference import claim_settle_caller as caller_reference
from tests.dualrun.workflow_seeds import (
    OnError,
    TopLevel,
    WorkflowSeed,
    claim_node_task,
    cover_history,
    end_workflow,
    pause_workflow_row,
    run_or_fail,
    start_tree,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER: Final = 'worker-dualrun-settle'
_OTHER_WORKER: Final = 'worker-dualrun-settle-other'

_CLAIMED_AT_SQL: Final[LiteralString] = """
    SELECT claimed_at FROM horsies_tasks WHERE id = %(task)s
"""


@dataclass(frozen=True, slots=True)
class _Seed:
    claims: tuple[tuple[uuid.UUID, datetime], ...]
    labels: dict[uuid.UUID, str]


def _claimed_at(connection: Connection, task_id: uuid.UUID) -> datetime:
    record = connection.execute(_CLAIMED_AT_SQL, {'task': task_id}).fetchone()
    assert record is not None and isinstance(record[0], datetime), (
        f'task {task_id} has no claim time'
    )
    return record[0]


def _workflow(connection: Connection, name: str) -> uuid.UUID:
    workflow_id = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=workflow_id,
                name=name,
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, f'{name}.mine'),
                    task_node(1, f'{name}.other'),
                    task_node(2, f'{name}.waiting', 0),
                ),
            ),
        ],
    )
    return workflow_id


def _seed(connection: Connection) -> _Seed:
    """Claims of the worker in a PAUSED, a CANCELLED and a RUNNING workflow,
    and claims of another worker in the first two."""
    paused = _workflow(connection, 'settle.paused')
    cancelled = _workflow(connection, 'settle.cancelled')
    running = _workflow(connection, 'settle.running')
    labels: dict[uuid.UUID, str] = {}
    claims: list[tuple[uuid.UUID, datetime]] = []
    for workflow_id, name in ((paused, 'paused'), (cancelled, 'cancelled'), (running, 'running')):
        mine = claim_node_task(connection, workflow_id, 0, _WORKER)
        labels[mine] = f'{name}.mine'
        claims.append((mine, _claimed_at(connection, mine)))
    for workflow_id, name in ((paused, 'paused'), (cancelled, 'cancelled')):
        other = claim_node_task(connection, workflow_id, 1, _OTHER_WORKER)
        labels[other] = f'{name}.other'
        claims.append((other, _claimed_at(connection, other)))
    pause_workflow_row(connection, paused)
    end_workflow(connection, cancelled, 'CANCELLED')
    return _Seed(claims=tuple(claims), labels=labels)


def _report(seed: _Seed, dispatched: list[uuid.UUID]) -> tuple[str, ...]:
    return tuple(seed.labels[task_id] for task_id in dispatched)


def _reference(database: DualRunDatabase, seed: _Seed) -> tuple[str, ...]:
    cover_history(database)
    install_as_a_client(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        return _report(seed, reference.settle(connection, worker_id=_WORKER, claims=seed.claims))


class _SettleProbe(ClaimMixin):
    """The claim mixin with the attributes that the check reads: the session
    factory (the Python statement) and the core handle (the core)."""

    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], broker: PostgresBroker,
    ) -> None:
        self.sf = session_factory
        self.worker_instance_id = _WORKER
        self._probe_handle = broker._core_handle  # pyright: ignore[reportPrivateUsage]

    @property
    def core_handle(self) -> CoreHandle:
        return self._probe_handle


async def _candidate_async(database: DualRunDatabase, seed: _Seed) -> tuple[str, ...]:
    async with open_broker(database) as broker:
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        rows: list[dict[str, Any]] = [
            {'id': str(task_id), 'is_workflow_task': True, 'claimed_at': claimed_at}
            for task_id, claimed_at in seed.claims
        ]
        dispatched = await _SettleProbe(
            resources.session_factory, broker,
        )._filter_nonrunnable_workflow_tasks(rows)  # pyright: ignore[reportPrivateUsage]
        return _report(seed, [uuid.UUID(str(row['id'])) for row in dispatched])


def _candidate(database: DualRunDatabase, seed: _Seed) -> tuple[str, ...]:
    cover_history(database)
    return asyncio.run(_candidate_async(database, seed))


def test_claim_settle_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_claimed_tasks_settle', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == ('running.mine',)
    assert run.candidate.report == ('running.mine',)


async def _caller_reference_async(database: DualRunDatabase, seed: _Seed) -> tuple[str, ...]:
    async with open_broker(database) as broker:
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        rows: list[dict[str, Any]] = [
            {'id': str(task_id), 'is_workflow_task': True, 'claimed_at': claimed_at}
            for task_id, claimed_at in seed.claims
        ]
        dispatched = await caller_reference.filter_nonrunnable_workflow_tasks(
            _SettleProbe(resources.session_factory, broker), rows,
        )
        return _report(seed, [uuid.UUID(str(row['id'])) for row in dispatched])


def _caller_reference(database: DualRunDatabase, seed: _Seed) -> tuple[str, ...]:
    cover_history(database)
    return asyncio.run(_caller_reference_async(database, seed))


def test_the_settle_on_the_core_writes_what_the_python_method_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='python method', act=_caller_reference),
            OperationSide(name='core method', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == ('running.mine',)
    assert run.candidate.report == ('running.mine',)
