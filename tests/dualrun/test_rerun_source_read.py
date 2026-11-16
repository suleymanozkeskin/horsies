"""Dual-run of the rerun source read moving into ``horsies_rerun_source_read``.

One fresh baseline database holds every source state (the read writes
nothing, so both sides read the same rows):

- reference side: ``reference/rerun_source.py``, the class check and the
  detail read that ``rerun_task`` sent before the switch;
- candidate side: ``read_rerun_source``, one call of
  ``horsies_rerun_source_read`` (0006_history_functions.sql).

Sources: a live task, a failed task with its input envelope, a completed
task, a workflow task, a history row whose attempt snapshot does not decode,
an absent id that is not a UUIDv7, and an absent UUIDv7 id; and an
unregistered retention class. The answers must be equal, except the absence
reason of the UUIDv7 id: the candidate classifies it by
``horsies_task_absence_reason`` (a decided change), the reference answered
``NOT_CLASSIFIED`` for every absence.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, LiteralString

import psycopg
import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.history.reads.detail import TaskAbsenceReason
from horsies.core.history.rerun.operations import (
    RerunClassUnregistered,
    RerunSource,
    RerunSourceAbsent,
    RerunSourceCorrupt,
    RerunSourceLive,
    RerunSourceRead,
    read_rerun_source,
)
from horsies.core.types.result import Err, Ok
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.reference.rerun_source import read_rerun_source_before
from tests.dualrun.seeds import (
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    cover,
    insert_task,
    insert_workflow_task,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

CLASS_KEY: Final = 'standard_30d'
UNREGISTERED_CLASS_KEY: Final = 'standard_29d'
WORKER: Final = 'worker-dualrun'
_AGO: Final = timedelta(minutes=1)

_MOVE_SQL: Final[LiteralString] = """
    SELECT horsies_move_task_to_history(
        (SELECT t FROM horsies_tasks AS t WHERE t.id = %(task_id)s FOR UPDATE),
        %(status)s, %(kind)s, now(), %(result)s, %(error_code)s, %(reason)s
    )
"""
_BREAK_SNAPSHOT_SQL: Final[LiteralString] = """
    UPDATE horsies_task_history SET attempt_snapshot = '\\x00'::bytea WHERE task_id = %(task_id)s
"""


@dataclass(frozen=True, slots=True)
class Sources:
    """The source id of each state, and the class key of each read."""

    reads: tuple[tuple[str, str, str], ...]
    """(label, source id, class key)."""
    absent_v7: str


type Answer = tuple[str, ...]


def _running(name: str) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.RUNNING,
        worker_id=WORKER,
        claimed_ago=_AGO,
        started_ago=_AGO,
    )


def _moved(
    connection: psycopg.Connection[tuple[object, ...]],
    task_id: uuid.UUID,
    status: str,
    kind: str,
) -> None:
    _ = connection.execute(
        _MOVE_SQL,
        {
            'task_id': task_id,
            'status': status,
            'kind': kind,
            'result': '{"ok": 1}',
            'error_code': 'E1' if status == 'FAILED' else None,
            'reason': 'boom' if status == 'FAILED' else None,
        },
    )


def _seed(database: DualRunDatabase) -> Sources:
    match install_baseline_schema(database):
        case Err(failed):
            pytest.fail(f'install the baseline: {failed}')
        case Ok():
            pass
    cover(database.url, SeedCoverage.HISTORY_AND_BEATS)
    with psycopg.connect(database.url) as connection:
        live = insert_task(connection, TaskSeed(task_name='live', status=SeedStatus.PENDING))
        failed = insert_task(connection, _running('failed'))
        _moved(connection, failed.task_id, 'FAILED', 'FAIL_RUNNING')
        completed = insert_task(connection, _running('completed'))
        _moved(connection, completed.task_id, 'COMPLETED', 'COMPLETE_FUSED')
        workflow = insert_workflow_task(
            connection,
            _running('workflow'),
            WorkflowLink(workflow_status='RUNNING', node_status=None),
        )
        _moved(connection, workflow.task_id, 'CANCELLED', 'CANCEL_ORPHAN')
        corrupt = insert_task(connection, _running('corrupt'))
        _moved(connection, corrupt.task_id, 'FAILED', 'FAIL_RUNNING')
        _ = connection.execute(_BREAK_SNAPSHOT_SQL, {'task_id': corrupt.task_id})
        record = connection.execute('SELECT horsies_uuidv7()').fetchone()
        assert record is not None, 'horsies_uuidv7 returns one row'
        absent_v7 = str(record[0])
    return Sources(
        reads=(
            ('live', str(live.task_id), CLASS_KEY),
            ('failed', str(failed.task_id), CLASS_KEY),
            ('completed', str(completed.task_id), CLASS_KEY),
            ('workflow', str(workflow.task_id), CLASS_KEY),
            ('corrupt', str(corrupt.task_id), CLASS_KEY),
            ('absent, not a UUIDv7', str(uuid.uuid4()), CLASS_KEY),
            ('unregistered class', str(failed.task_id), UNREGISTERED_CLASS_KEY),
        ),
        absent_v7=absent_v7,
    )


def _answer(read: RerunSourceRead) -> Answer:
    """The read as comparable text: a corrupt source carries an exception, which compares by identity."""
    match read:
        case RerunClassUnregistered(class_key=class_key):
            return ('CLASS_UNREGISTERED', class_key)
        case RerunSourceLive(task_id=task_id):
            return ('LIVE', task_id)
        case RerunSourceAbsent(task_id=task_id, reason=reason):
            return ('ABSENT', task_id, reason.value)
        case RerunSourceCorrupt(task_id=task_id, exception=exception):
            return ('CORRUPT', task_id, str(exception))
        case RerunSource():
            return ('HISTORY', repr(read))


type SourceRead = Callable[[AsyncConnection, str, str], Awaitable[RerunSourceRead]]


async def _read_all(
    url: str,
    reads: tuple[tuple[str, str, str], ...],
    reader: SourceRead,
) -> dict[str, Answer]:
    engine = create_async_engine(url.replace('postgresql://', 'postgresql+psycopg://', 1))
    try:
        answers: dict[str, Answer] = {}
        async with engine.connect() as connection:
            for label, source_id, class_key in reads:
                answers[label] = _answer(await reader(connection, source_id, class_key))
                await connection.rollback()
        return answers
    finally:
        await engine.dispose()


def test_the_rerun_source_read_answers_as_the_statements_before_the_switch(
    empty_database: MakeDatabase,
) -> None:
    database = empty_database()
    sources = _seed(database)

    reference = asyncio.run(_read_all(database.url, sources.reads, read_rerun_source_before))
    candidate = asyncio.run(_read_all(database.url, sources.reads, read_rerun_source))

    assert candidate == reference
    assert [answer[0] for answer in candidate.values()] == [
        'LIVE', 'HISTORY', 'HISTORY', 'HISTORY', 'CORRUPT', 'ABSENT', 'CLASS_UNREGISTERED',
    ]


def test_an_absent_uuidv7_source_is_classified_by_the_absence_rule(
    empty_database: MakeDatabase,
) -> None:
    database = empty_database()
    sources = _seed(database)
    read = (('absent UUIDv7', sources.absent_v7, CLASS_KEY),)

    reference = asyncio.run(_read_all(database.url, read, read_rerun_source_before))
    candidate = asyncio.run(_read_all(database.url, read, read_rerun_source))

    assert reference == {
        'absent UUIDv7': ('ABSENT', sources.absent_v7, TaskAbsenceReason.NOT_CLASSIFIED.value),
    }
    assert candidate == {
        'absent UUIDv7': (
            'ABSENT', sources.absent_v7, TaskAbsenceReason.WITHIN_RETAINED_HISTORY.value,
        ),
    }
