# pyright: reportPrivateUsage=false
"""Dual-run of the phase-2 raise switch: the session statements against
``horsies_workflow_phase2_record_raise``.

Reference: ``tests/dualrun/reference/workflow_phase2_raise.py``. Candidate:
``_record_raised_row`` (``core/workflows/phase2_recovery.py``). Attempt
bound 3. The seed holds one phase-2 record per outcome: at the bound with
no other live node (the workflow fails), at the bound with a live node, under
the bound, at the bound of a PAUSED workflow; and one task id with no record.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, assert_never

import psycopg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from horsies.core.workflows.phase2_recovery import (
    _RaisedRow,
    _RaisedRowNotRecorded,
    _RaisedRowRecorded,
    _record_raised_row,
)
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_phase2_raise as reference
from tests.dualrun.workflow_seeds import (
    OnError,
    TopLevel,
    WorkflowSeed,
    cover_history,
    insert_phase2_pending,
    pause_workflow_row,
    run_or_fail,
    start_tree,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_BOUND: Final = 3


@dataclass(frozen=True, slots=True)
class _Seed:
    records: tuple[tuple[uuid.UUID, uuid.UUID], ...]
    """(task, workflow) of each record, as discovery gives them; the last
    task has no record, with an unknown workflow."""


def _one_workflow(
    connection: Connection, name: str, *nodes: tuple[int, str, tuple[int, ...]],
) -> uuid.UUID:
    workflow_id = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=workflow_id,
                name=name,
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=tuple(task_node(index, task, *deps) for index, task, deps in nodes),
            ),
        ],
    )
    return workflow_id


def _seed(connection: Connection) -> _Seed:
    """One record per outcome of the raise, in the order the sides record them."""
    fails = _one_workflow(
        connection, 'raise.fails', (0, 'raise.fails.source', ()), (1, 'raise.fails.waiting', (0,)),
    )
    live = _one_workflow(
        connection, 'raise.live', (0, 'raise.live.source', ()), (1, 'raise.live.other', ()),
    )
    counted = _one_workflow(connection, 'raise.counted', (0, 'raise.counted.source', ()))
    paused = _one_workflow(connection, 'raise.paused', (0, 'raise.paused.source', ()))
    pause_workflow_row(connection, paused)
    return _Seed(
        records=(
            (insert_phase2_pending(connection, fails, 0, _BOUND - 1), fails),
            (insert_phase2_pending(connection, live, 0, _BOUND - 1), live),
            (insert_phase2_pending(connection, counted, 0, 0), counted),
            (insert_phase2_pending(connection, paused, 0, _BOUND - 1), paused),
            (uuid.uuid4(), uuid.uuid4()),
        ),
    )


def _without_task_id(report: reference.RaiseReport, task: uuid.UUID) -> reference.RaiseReport:
    """The report with the record's task id, which differs per database, as ``<task>``."""
    match report:
        case reference.Recorded(quarantined=quarantined, quarantine_refusal=str() as refusal):
            return reference.Recorded(
                quarantined=quarantined,
                quarantine_refusal=refusal.replace(str(task), '<task>'),
            )
        case reference.Recorded() | reference.NotRecorded():
            return report
        case _ as unreachable:
            assert_never(unreachable)


def _reference(database: DualRunDatabase, seed: _Seed) -> tuple[reference.RaiseReport, ...]:
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        return tuple(
            _without_task_id(reference.record_raise(connection, task, _BOUND), task)
            for task, _ in seed.records
        )


def _report(raised: _RaisedRow) -> reference.RaiseReport:
    match raised:
        case _RaisedRowRecorded(quarantined=quarantined, quarantine_refusal=refusal):
            return reference.Recorded(quarantined=quarantined, quarantine_refusal=refusal)
        case _RaisedRowNotRecorded():
            return reference.NotRecorded()
        case _ as unreachable:
            assert_never(unreachable)


async def _candidate_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.RaiseReport, ...]:
    engine = create_async_engine(sqlalchemy_url(database))
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        reports: list[reference.RaiseReport] = []
        for task, workflow in seed.records:
            raised = await _record_raised_row(
                sessions,
                task_id=str(task),
                workflow_id=str(workflow),
                task_index=0,
                exc=RuntimeError('boom'),
                quarantine_after_attempts=_BOUND,
            )
            reports.append(_without_task_id(_report(raised), task))
        return tuple(reports)
    finally:
        await engine.dispose()


def _candidate(database: DualRunDatabase, seed: _Seed) -> tuple[reference.RaiseReport, ...]:
    cover_history(database)
    return asyncio.run(_candidate_async(database, seed))


def test_the_raise_record_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_phase2_record_raise', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.candidate.report == run.reference.report
    fails, live, counted, paused, absent = run.reference.report
    refused = reference.Recorded(
        quarantined=False,
        quarantine_refusal=(
            '<task>: quarantine refused with SOURCE_ABSENT: history row absent at locator'
        ),
    )
    assert fails == refused
    assert live == refused
    assert counted == reference.Recorded(quarantined=False, quarantine_refusal=None)
    assert paused == reference.NotRecorded()
    assert absent == reference.NotRecorded()
