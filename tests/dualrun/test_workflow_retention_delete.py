"""Dual-run of the workflow retention delete: the Python reaper's batches against the core's.

Reference: ``tests/dualrun/reference/workflow_retention_caller.py``.
Candidate: ``ReaperMixin._delete_expired_workflows`` on the core
(``CoreHandle.delete_expired_workflows_async``). The seed holds two
workflows that finished at a fixed old instant, one that finished now, one
marked COMPLETED at that instant that still has a PENDING task, and a
RUNNING workflow. Each side deletes the workflows past a retention of 48 hours in
batches of 50.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, LiteralString

import psycopg
import pytest

from horsies.core.worker.reaper import ReaperMixin
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_retention_caller as reference
from tests.dualrun.workflow_seeds import (
    OnError,
    TopLevel,
    WorkflowSeed,
    cover_history,
    end_workflow,
    finish_node_task,
    run_node_task,
    run_or_fail,
    start_tree,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER: Final = 'worker-dualrun-retention'
_RESULT: Final = '{"ok": 1}'
_RETENTION_HOURS: Final = 48
_BATCH_SIZE: Final = 50
_BUDGET_S: Final = 60.0

_OLD_END: Final = datetime(2026, 1, 1, tzinfo=timezone.utc)
"""The end time of the old workflows: a fixed instant, far past the
retention, so a retained row compares exactly on both sides."""

_AGE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET completed_at = %(at)s, updated_at = %(at)s
    WHERE id = %(wf)s
"""


@dataclass(frozen=True, slots=True)
class _Seed:
    old_done: tuple[uuid.UUID, uuid.UUID]
    recent_done: uuid.UUID
    old_live: uuid.UUID


def _one_node_workflow(connection: Connection, name: str) -> uuid.UUID:
    workflow_id = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=workflow_id,
                name=name,
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, f'{name}.task'),),
            ),
        ],
    )
    return workflow_id


def _age(connection: Connection, workflow_id: uuid.UUID) -> None:
    cursor = connection.execute(_AGE_SQL, {'wf': workflow_id, 'at': _OLD_END})
    assert cursor.rowcount == 1, f'workflow {workflow_id} is absent'


def _seed(connection: Connection) -> _Seed:
    """Five one-node workflows; the finishes come after the history readers
    are published (``_finish``)."""
    old_done = (
        _one_node_workflow(connection, 'retention.old_done_a'),
        _one_node_workflow(connection, 'retention.old_done_b'),
    )
    recent_done = _one_node_workflow(connection, 'retention.recent_done')
    old_live = _one_node_workflow(connection, 'retention.old_live')
    end_workflow(connection, old_live, 'COMPLETED')
    _age(connection, old_live)
    _ = _one_node_workflow(connection, 'retention.running')
    return _Seed(old_done=old_done, recent_done=recent_done, old_live=old_live)


def _finish(database: DualRunDatabase, seed: _Seed) -> None:
    """The finished workflows: each task runs and finishes (its workflow
    completes), then the two old ones get the fixed old end time."""
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            for workflow_id in (*seed.old_done, seed.recent_done):
                _ = run_node_task(connection, workflow_id, 0, _WORKER)
                _ = finish_node_task(connection, workflow_id, 0, _WORKER, _RESULT)
            for workflow_id in seed.old_done:
                _age(connection, workflow_id)


class _Reaper(ReaperMixin):
    """The reaper mixin alone: the workflow delete reads no worker state."""


async def _reference_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        return await reference.delete_expired_workflows(
            _Reaper(),
            broker,
            retention_hours=_RETENTION_HOURS,
            batch_size=_BATCH_SIZE,
            deadline_monotonic=time.monotonic() + _BUDGET_S,
        )


def _reference(database: DualRunDatabase, seed: _Seed) -> int:
    _finish(database, seed)
    return asyncio.run(_reference_async(database))


async def _candidate_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        return await _Reaper()._delete_expired_workflows(  # pyright: ignore[reportPrivateUsage]
            broker,
            retention_hours=_RETENTION_HOURS,
            batch_size=_BATCH_SIZE,
            deadline_monotonic=time.monotonic() + _BUDGET_S,
        )


def _candidate(database: DualRunDatabase, seed: _Seed) -> int:
    _finish(database, seed)
    return asyncio.run(_candidate_async(database))


def test_the_core_retention_delete_deletes_what_the_python_reaper_deleted(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='python reaper', act=_reference),
            OperationSide(name='core', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == 2
    assert run.candidate.report == 2
