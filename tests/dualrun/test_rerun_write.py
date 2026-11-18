"""Dual-run of the rerun write moving onto ``horsies_enqueue_task``.

Each pair seeds the same sources on two fresh baseline databases and runs a
sequence of reruns, each in its own caller transaction:

- reference side: ``reference/rerun_write.py``, the key claim
  (``horsies_key_reservation_claim``) and the INSERT that ``rerun_task`` sent
  before the switch;
- candidate side: the Python ``rerun_task`` on the function
  (``reference/rerun_caller.py``), one call of ``horsies_enqueue_task`` with
  the lineage ``RERUN`` (0004_lifecycle_functions.sql).

Caller pairs (the switch of ``rerun_task`` to the core), on the same seeds
and sequences: reference = the Python ``rerun_task`` on the function
(``reference/rerun_caller.py``); candidate = the production ``rerun_task``,
whose read and write the core builds and decides
(``horsies._core.rerun_read``, ``prepare_rerun_write``,
``decode_rerun_write``) and the test's connection runs. Both write through
``horsies_enqueue_task``, so the snapshots and the answers must be equal with
no exception.

Sequences: an unkeyed rerun; a keyed rerun sent twice (key replay); a keyed
rerun sent again with another deadline (key conflict); a rerun that the
caller rolls back; a live and a completed source (refusals). The snapshots
must be equal and the answers must be equal.

Each caller transaction waits before the rerun, so the start of the
transaction and the time of the rerun's statement differ. The answer records
whether ``enqueued_at`` and ``created_at`` are later than the start of the
transaction: both sides date both by the statement. Each answer also checks
that the stored fingerprint is the version-1 fingerprint of the row's own
columns.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Final, LiteralString, assert_never

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.history.identity.fingerprint import EnqueueCommandV1
from horsies.core.history.rerun.operations import (
    RerunEnqueued,
    RerunEnqueuePolicy,
    RerunKeyConflict,
    RerunKeyReplay,
    RerunNotEligible,
    RerunOutcome,
    RerunSourceLive,
    RerunTask,
    rerun_task,
)
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference.rerun_caller import rerun_task as python_rerun_task
from tests.dualrun.reference.rerun_write import rerun_task_before
from tests.dualrun.seeds import SeedCoverage, SeedStatus, TaskSeed, insert_task

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

WORKER: Final = 'worker-dualrun'
_AGO: Final = timedelta(minutes=1)
POLICY: Final = RerunEnqueuePolicy(
    retention=timedelta(days=30),
    retain_rerun_input=True,
    reservation_window=timedelta(hours=1),
)
CALLER_KEY: Final = 'rerun-key-1'
DEADLINE: Final = datetime(2099, 1, 1, tzinfo=UTC)
WAIT_BEFORE_RERUN_S: Final = 0.01
"""The caller transaction waits this long before the rerun, so the time of the
rerun's statement is later than the start of the transaction."""

_MOVE_SQL: Final[LiteralString] = """
    SELECT horsies_move_task_to_history(
        (SELECT t FROM horsies_tasks AS t WHERE t.id = %(task_id)s FOR UPDATE),
        %(status)s, %(kind)s, now(), %(result)s, %(error_code)s, %(reason)s
    )
"""

_WAIT_SQL: Final = text('SELECT pg_sleep(:seconds)')

_WRITTEN_ROW_SQL: Final = text(
    'SELECT task_name, queue_name, priority, input, good_until, task_options, '
    'retention_class_key, retain_rerun_input, rerun_of_task_id, rerun_root_task_id, '
    'command_fingerprint, enqueued_at > now() AS enqueued_by_statement, '
    'created_at > now() AS created_by_statement '
    'FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)'
)


class Source(Enum):
    """A seeded source of a rerun."""

    FAILED = 'failed'
    COMPLETED = 'completed'
    LIVE = 'live'


@dataclass(frozen=True, slots=True)
class Sources:
    """The task id of each seeded source on one database."""

    ids: Mapping[Source, uuid.UUID]


class TransactionEnd(Enum):
    COMMIT = 'commit'
    ROLLBACK = 'rollback'


@dataclass(frozen=True, slots=True)
class RerunStep:
    """One rerun in its own caller transaction."""

    source: Source
    caller_key: str | None
    deadline: datetime | None
    end: TransactionEnd


@dataclass(frozen=True, slots=True)
class Enqueued:
    """The rerun wrote its row; ids as labels, dates as facts."""

    new_task: str
    source: str
    root: str
    enqueued_by_statement: bool
    created_by_statement: bool
    fingerprint_of_row: bool


@dataclass(frozen=True, slots=True)
class KeyReplayed:
    existing_task: str


@dataclass(frozen=True, slots=True)
class KeyConflicted:
    source: str
    reserved_by: str


@dataclass(frozen=True, slots=True)
class Refused:
    """A refusal of the rerun's catalog, as its class name and source."""

    outcome: str
    source: str


type StepAnswer = Enqueued | KeyReplayed | KeyConflicted | Refused

type Rerun = Callable[[AsyncConnection, RerunTask, RerunEnqueuePolicy], Awaitable[RerunOutcome]]


def _running(name: str) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.RUNNING,
        worker_id=WORKER,
        claimed_ago=_AGO,
        started_ago=_AGO,
    )


def _move(connection: Connection, task_id: uuid.UUID, status: str, kind: str) -> None:
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


def _seed(connection: Connection) -> Sources:
    failed = insert_task(connection, _running('rerun.failed'))
    _move(connection, failed.task_id, 'FAILED', 'FAIL_RUNNING')
    completed = insert_task(connection, _running('rerun.completed'))
    _move(connection, completed.task_id, 'COMPLETED', 'COMPLETE_FUSED')
    live = insert_task(connection, TaskSeed(task_name='rerun.live', status=SeedStatus.PENDING))
    return Sources(
        ids={
            Source.FAILED: failed.task_id,
            Source.COMPLETED: completed.task_id,
            Source.LIVE: live.task_id,
        },
    )


def _label(task_id: uuid.UUID, sources: Sources, written: list[uuid.UUID]) -> str:
    """The seed name or the rerun position of ``task_id``."""
    for source, source_id in sources.ids.items():
        if source_id == task_id:
            return f'source {source.value}'
    if task_id in written:
        return f'rerun {written.index(task_id) + 1}'
    return 'unknown task'


def _row_fingerprint(row: Any) -> bytes:
    """The version-1 fingerprint of a rerun row's own columns."""
    return EnqueueCommandV1(
        task_name=row.task_name,
        queue_name=row.queue_name,
        priority=row.priority,
        positional_args_json=None,
        input_json=row.input,
        good_until=row.good_until,
        enqueue_delay_seconds=None,
        task_options_json=row.task_options,
        retention_class_key=row.retention_class_key,
        retain_rerun_input=row.retain_rerun_input,
        rerun_of_task_id=str(row.rerun_of_task_id),
        rerun_root_task_id=str(row.rerun_root_task_id),
    ).fingerprint


async def _enqueued(
    connection: AsyncConnection,
    outcome: RerunEnqueued,
    sources: Sources,
    written: list[uuid.UUID],
) -> Enqueued:
    row = (
        await connection.execute(_WRITTEN_ROW_SQL, {'task_id': str(outcome.new_task_id)})
    ).one()
    written.append(outcome.new_task_id)
    return Enqueued(
        new_task=_label(outcome.new_task_id, sources, written),
        source=_label(outcome.source_task_id, sources, written),
        root=_label(outcome.rerun_root_task_id, sources, written),
        enqueued_by_statement=row.enqueued_by_statement,
        created_by_statement=row.created_by_statement,
        fingerprint_of_row=bytes(row.command_fingerprint) == _row_fingerprint(row),
    )


async def _answer(
    connection: AsyncConnection,
    outcome: RerunOutcome,
    sources: Sources,
    written: list[uuid.UUID],
) -> StepAnswer:
    match outcome:
        case RerunEnqueued():
            return await _enqueued(connection, outcome, sources, written)
        case RerunKeyReplay(existing_task_id=existing):
            return KeyReplayed(existing_task=_label(existing, sources, written))
        case RerunKeyConflict(task_id=task_id, reserved_by_task_id=owner):
            return KeyConflicted(
                source=_label(task_id, sources, written),
                reserved_by=_label(owner, sources, written),
            )
        case RerunSourceLive(task_id=task_id) | RerunNotEligible(task_id=task_id):
            return Refused(
                outcome=type(outcome).__name__, source=_label(task_id, sources, written)
            )
        case _:
            raise AssertionError(f'a step of these sequences answered {outcome!r}')


async def _run_steps(
    database: DualRunDatabase,
    sources: Sources,
    rerun: Rerun,
    steps: tuple[RerunStep, ...],
) -> tuple[StepAnswer, ...]:
    engine = create_async_engine(sqlalchemy_url(database))
    answers: list[StepAnswer] = []
    written: list[uuid.UUID] = []
    try:
        for step in steps:
            async with engine.connect() as connection:
                transaction = await connection.begin()
                _ = await connection.execute(_WAIT_SQL, {'seconds': WAIT_BEFORE_RERUN_S})
                outcome = await rerun(
                    connection,
                    RerunTask(
                        source_task_id=sources.ids[step.source],
                        deadline=step.deadline,
                        caller_key=step.caller_key,
                    ),
                    POLICY,
                )
                answers.append(await _answer(connection, outcome, sources, written))
                match step.end:
                    case TransactionEnd.COMMIT:
                        await transaction.commit()
                    case TransactionEnd.ROLLBACK:
                        await transaction.rollback()
                    case _ as unreachable:
                        assert_never(unreachable)
        return tuple(answers)
    finally:
        await engine.dispose()


def _side(
    name: str, rerun: Rerun, steps: tuple[RerunStep, ...]
) -> OperationSide[Sources, tuple[StepAnswer, ...]]:
    def act(database: DualRunDatabase, sources: Sources) -> tuple[StepAnswer, ...]:
        return asyncio.run(_run_steps(database, sources, rerun, steps))

    return OperationSide(name=name, act=act)


def _pair(
    make_database: MakeDatabase, steps: tuple[RerunStep, ...]
) -> OperationDualRun[tuple[StepAnswer, ...], tuple[StepAnswer, ...]]:
    match dual_run(
        make_database,
        _seed,
        _side('key claim and INSERT', rerun_task_before, steps),
        _side('horsies_enqueue_task', python_rerun_task, steps),
        SeedCoverage.HISTORY_AND_BEATS,
    ):
        case Ok(pair):
            return pair
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


_FIRST_ENQUEUED: Final = Enqueued(
    new_task='rerun 1',
    source='source failed',
    root='source failed',
    enqueued_by_statement=True,
    created_by_statement=True,
    fingerprint_of_row=True,
)


def test_an_unkeyed_rerun_writes_the_row_of_before(empty_database: MakeDatabase) -> None:
    steps = (RerunStep(Source.FAILED, None, DEADLINE, TransactionEnd.COMMIT),)

    pair = _pair(empty_database, steps)

    assert pair.diff.entries == (), render_diff(pair.diff)
    assert pair.reference.report == (_FIRST_ENQUEUED,)
    assert pair.candidate.report == pair.reference.report


def test_a_keyed_rerun_sent_twice_replays_the_first(empty_database: MakeDatabase) -> None:
    keyed = RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.COMMIT)

    pair = _pair(empty_database, (keyed, keyed))

    assert pair.diff.entries == (), render_diff(pair.diff)
    assert pair.reference.report == (_FIRST_ENQUEUED, KeyReplayed(existing_task='rerun 1'))
    assert pair.candidate.report == pair.reference.report


def test_a_keyed_rerun_of_another_command_conflicts(empty_database: MakeDatabase) -> None:
    steps = (
        RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.COMMIT),
        RerunStep(Source.FAILED, CALLER_KEY, DEADLINE, TransactionEnd.COMMIT),
    )

    pair = _pair(empty_database, steps)

    assert pair.diff.entries == (), render_diff(pair.diff)
    assert pair.reference.report == (
        _FIRST_ENQUEUED,
        KeyConflicted(source='source failed', reserved_by='rerun 1'),
    )
    assert pair.candidate.report == pair.reference.report


def test_a_rolled_back_keyed_rerun_leaves_no_row_and_no_reservation(
    empty_database: MakeDatabase,
) -> None:
    steps = (
        RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.ROLLBACK),
        RerunStep(Source.FAILED, CALLER_KEY, DEADLINE, TransactionEnd.COMMIT),
    )

    pair = _pair(empty_database, steps)

    assert pair.diff.entries == (), render_diff(pair.diff)
    # The second rerun of another command finds no reservation of the first:
    # the rollback took the key claim with it.
    assert pair.reference.report == (
        _FIRST_ENQUEUED,
        replace(_FIRST_ENQUEUED, new_task='rerun 2'),
    )
    assert pair.candidate.report == pair.reference.report


def test_refused_sources_write_nothing_as_before(empty_database: MakeDatabase) -> None:
    steps = (
        RerunStep(Source.LIVE, CALLER_KEY, None, TransactionEnd.COMMIT),
        RerunStep(Source.COMPLETED, CALLER_KEY, None, TransactionEnd.COMMIT),
    )

    pair = _pair(empty_database, steps)

    assert pair.diff.entries == (), render_diff(pair.diff)
    assert pair.reference.report == (
        Refused(outcome='RerunSourceLive', source='source live'),
        Refused(outcome='RerunNotEligible', source='source completed'),
    )
    assert pair.candidate.report == pair.reference.report


# ---- The caller switch: the Python rerun against the core's steps ----


def _caller_pair(
    make_database: MakeDatabase, steps: tuple[RerunStep, ...]
) -> OperationDualRun[tuple[StepAnswer, ...], tuple[StepAnswer, ...]]:
    match dual_run(
        make_database,
        _seed,
        _side('Python rerun_task', python_rerun_task, steps),
        _side('rerun_task through the core', rerun_task, steps),
        SeedCoverage.HISTORY_AND_BEATS,
    ):
        case Ok(pair):
            return pair
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


_CALLER_SEQUENCES: Final = {
    'unkeyed': (RerunStep(Source.FAILED, None, DEADLINE, TransactionEnd.COMMIT),),
    'keyed twice': (
        RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.COMMIT),
        RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.COMMIT),
    ),
    'key conflict': (
        RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.COMMIT),
        RerunStep(Source.FAILED, CALLER_KEY, DEADLINE, TransactionEnd.COMMIT),
    ),
    'rolled back': (
        RerunStep(Source.FAILED, CALLER_KEY, None, TransactionEnd.ROLLBACK),
        RerunStep(Source.FAILED, CALLER_KEY, DEADLINE, TransactionEnd.COMMIT),
    ),
    'refused sources': (
        RerunStep(Source.LIVE, CALLER_KEY, None, TransactionEnd.COMMIT),
        RerunStep(Source.COMPLETED, CALLER_KEY, None, TransactionEnd.COMMIT),
    ),
}


@pytest.mark.parametrize('sequence', sorted(_CALLER_SEQUENCES))
def test_the_core_rerun_writes_and_answers_as_the_python_rerun(
    empty_database: MakeDatabase, sequence: str
) -> None:
    pair = _caller_pair(empty_database, _CALLER_SEQUENCES[sequence])

    assert pair.diff.entries == (), render_diff(pair.diff)
    assert pair.candidate.report == pair.reference.report
