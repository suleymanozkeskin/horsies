"""Dual-run of the reader publication and the miss classifier moving into the baseline SQL.

Both sides seed the same database state on a fresh baseline: the default
retention class, the history leaf of 2030-01-01, and live tasks. Then:

- reference side: ``reference/interim_miss_classifier.sql`` (the composite
  types and the array overload of the miss classifier) and
  ``reference/history_readers.sql`` (the statements that the Python
  publisher sent for this catalog);
- candidate side: ``StagedLoaderPublisher.republish``, which now calls
  ``horsies_publish_history_readers``.

Each side then moves the same tasks through
``horsies_move_task_to_history`` (the published duplicate guard runs inside
the move) and reads every task back: the Python state and result reads, the
guard, the provenance reader, and the miss classifier for each of the 15
kinds. The reference classifier receives the class of the kind as an array,
as each terminal write passed it; the candidate receives the kind.

The snapshots must be equal and the answers must be equal.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, LiteralString
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.history.commands import CreateDailyHistoryLeaf, LeafBounds, LeafRef
from horsies.core.history.identity.uuid7 import MonotonicUuid7Generator
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.reads.detail import TaskStateResult, read_task_state
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from horsies.core.history.reads.result import TaskResultRead, read_task_result
from horsies.core.lifecycle.operations import TerminalizationKind, equivalence_class_of
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.compare import compare_snapshots, render_diff
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.frozen_definitions import REFERENCE_FOLDER
from tests.dualrun.selfcheck import (
    SelfCheckRun,
    SelfCheckStepFailed,
    capture_run,
    read_run_start,
)

pytestmark = [pytest.mark.integration]

READERS_BEFORE_THE_SWITCH: Final = (
    REFERENCE_FOLDER / 'interim_miss_classifier.sql',
    REFERENCE_FOLDER / 'history_readers.sql',
)

CLASS_KEY: Final = 'standard_30d'
LEAF_LOWER: Final = datetime(2030, 1, 1, tzinfo=UTC)
LEAF_UPPER: Final = datetime(2030, 1, 2, tzinfo=UTC)
LEAF_NAME: Final = 'horsies_task_history_standard_30d_2030_01_01'
WORKER: Final = 'worker-dualrun'
OTHER_WORKER: Final = 'worker-other'


@dataclass(frozen=True, slots=True)
class SeededTask:
    """A live task of the seed, and how the act moves it (``None``: it stays live)."""

    name: str
    task_id: str
    class_key: str
    status: str
    move: tuple[str, str, datetime] | None
    """Terminal status, kind and terminal time of its move."""


@dataclass(frozen=True, slots=True)
class Seed:
    """The tasks of the seed and an id that no table holds."""

    tasks: tuple[SeededTask, ...]
    unknown_id: str

    def ids(self) -> tuple[tuple[str, str], ...]:
        return (*((task.name, task.task_id) for task in self.tasks), ('unknown', self.unknown_id))


@dataclass(frozen=True, slots=True)
class Provenance:
    """A history row that the provenance reader found."""

    status: str
    terminal_at: datetime
    terminalization_kind: str


@dataclass(frozen=True, slots=True)
class NoProvenance:
    """The provenance reader found no history row."""


@dataclass(frozen=True, slots=True)
class MissAnswer:
    """The outcome row of the miss classifier, without the task id."""

    outcome: str
    terminal_at: datetime | None
    terminalization_kind: str | None
    observed_status: str | None
    observed_worker_id: str | None
    guard_kind: str | None


@dataclass(frozen=True, slots=True)
class TaskAnswers:
    """What each read answered for one task id."""

    name: str
    state: TaskStateResult
    result: TaskResultRead
    guarded: bool
    provenance: Provenance | NoProvenance
    misses: tuple[tuple[str, str | None, MissAnswer], ...]
    """(kind, worker, answer) for each of the 15 kinds, without and with a worker."""


@dataclass(frozen=True, slots=True)
class SideRun:
    run: SelfCheckRun
    answers: tuple[TaskAnswers, ...]


type ProvenanceRead = Callable[[Connection, str], Provenance | NoProvenance]
type MissRead = Callable[[Connection, str, TerminalizationKind, str | None], MissAnswer]


def _v7_born_at(birth: datetime) -> str:
    milliseconds = int(birth.timestamp() * 1000)
    return MonotonicUuid7Generator(clock_ms=lambda: milliseconds).mint()


def _make_seed() -> Seed:
    def moved_at(hour: int) -> datetime:
        return datetime(2030, 1, 1, hour, tzinfo=UTC)

    tasks = (
        SeededTask(
            'forever_failed', _v7_born_at(datetime.now(UTC)), 'forever', 'RUNNING',
            ('FAILED', 'FAIL_RUNNING', datetime(2029, 6, 1, tzinfo=UTC)),
        ),
        SeededTask(
            'period_completed', _v7_born_at(datetime.now(UTC)), CLASS_KEY, 'RUNNING',
            ('COMPLETED', 'COMPLETE_FUSED', moved_at(10)),
        ),
        SeededTask(
            'fast_clock_cancelled', _v7_born_at(datetime(2031, 3, 1, tzinfo=UTC)), CLASS_KEY,
            'PENDING', ('CANCELLED', 'CANCEL_ADMIN', moved_at(11)),
        ),
        SeededTask(
            'not_v7_expired', str(uuid4()), CLASS_KEY, 'CLAIMED',
            ('EXPIRED', 'EXPIRE_CLAIMED', moved_at(12)),
        ),
        SeededTask('live_running', _v7_born_at(datetime.now(UTC)), CLASS_KEY, 'RUNNING', None),
    )
    return Seed(tasks=tasks, unknown_id=_v7_born_at(datetime.now(UTC)))


_REGISTER_CLASSES_SQL: Final[LiteralString] = (
    "SELECT count(*) FROM horsies_register_retention_classes('{}'::smallint[], 2::smallint)"
)

_INSERT_TASK_SQL: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT
        p.id, 'report.build', 'default', 50, '{"day":1}', NULL, 'ISOLATED', p.class_key, now(),
        now(), 1, sha256(convert_to(p.id::text, 'UTF8')), NULL, p.status, now(),
        CASE WHEN p.status = 'PENDING' THEN NULL ELSE now() END,
        CASE WHEN p.status = 'PENDING' THEN NULL ELSE p.worker END,
        CASE WHEN p.status = 'CLAIMED' THEN now() + interval '1 hour' END,
        CASE WHEN p.status = 'RUNNING' THEN now() END,
        NULL, NULL,
        CASE WHEN p.status = 'RUNNING' THEN 7 END,
        CASE WHEN p.status = 'RUNNING' THEN 'host-a' END,
        CASE WHEN p.status = 'RUNNING' THEN 'proc-a' END,
        0, 1, NULL, NULL,
        FALSE, now(), TRUE, NULL, NULL
    FROM (
        SELECT CAST(%(id)s AS uuid) AS id, CAST(%(class_key)s AS text) AS class_key,
               CAST(%(status)s AS text) AS status, CAST(%(worker)s AS text) AS worker
    ) AS p
"""

_TASK_LOCK_SQL: Final[LiteralString] = (
    'SELECT pg_advisory_xact_lock(hashtextextended(CAST(%(id)s AS uuid)::text, 731))'
)
_MOVE_SQL: Final[LiteralString] = """
    SELECT horsies_move_task_to_history(
        (SELECT t FROM horsies_tasks AS t WHERE t.id = CAST(%(id)s AS uuid) FOR UPDATE),
        %(status)s, %(kind)s, %(terminal_at)s, '{"done":1}', 'E1', 'reason'
    )
"""

_GUARD_SQL: Final[LiteralString] = 'SELECT horsies_task_history_duplicate_staged(%(id)s)'

_PROVENANCE_BEFORE_SQL: Final[LiteralString] = (
    'SELECT found, status, terminal_at, terminalization_kind '
    'FROM horsies_task_provenance_staged(%(id)s, FALSE)'
)
_PROVENANCE_AFTER_SQL: Final[LiteralString] = (
    'SELECT status, terminal_at, terminalization_kind FROM horsies_task_provenance_staged(%(id)s)'
)

_MISS_BEFORE_SQL: Final[LiteralString] = (
    'SELECT outcome, terminal_at, terminalization_kind, observed_status, observed_worker_id, guard_kind '
    'FROM horsies_terminalization_miss(%(id)s, %(kinds)s::text[], %(worker)s, NULL)'
)
_MISS_AFTER_SQL: Final[LiteralString] = (
    'SELECT outcome, terminal_at, terminalization_kind, observed_status, observed_worker_id, guard_kind '
    'FROM horsies_terminalization_miss(%(id)s, %(kind)s, %(worker)s, NULL)'
)


def _alchemy_url(database: DualRunDatabase) -> str:
    return database.url.replace('postgresql://', 'postgresql+psycopg://', 1)


async def _create_leaf(database: DualRunDatabase) -> None:
    engine = create_async_engine(_alchemy_url(database))
    try:
        async with engine.begin() as connection:
            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(
                    leaf=LeafRef(
                        leaf_name=LEAF_NAME,
                        class_key=CLASS_KEY,
                        bounds=LeafBounds(lower=LEAF_LOWER, upper=LEAF_UPPER),
                    )
                ),
                UnpublishedLoader(),
            )
        assert isinstance(outcome, LeafCreated), outcome
    finally:
        await engine.dispose()


def _write_seed(database: DualRunDatabase, connection: Connection, seed: Seed) -> None:
    """Classes, the 2030-01-01 leaf (not published) and the live tasks. Raises what the drivers raise.

    Each task is inserted in its own transaction, so the tasks differ in
    ``sent_at`` and the snapshot orders them without a tie.
    """
    _ = connection.execute(_REGISTER_CLASSES_SQL)
    asyncio.run(_create_leaf(database))
    for task in seed.tasks:
        with connection.transaction():
            _ = connection.execute(
                _INSERT_TASK_SQL,
                {
                    'id': task.task_id,
                    'class_key': task.class_key,
                    'status': task.status,
                    'worker': WORKER,
                },
            )


def _publish_before(database: DualRunDatabase, connection: Connection) -> None:
    for path in READERS_BEFORE_THE_SWITCH:
        text = path.read_text(encoding='utf-8')
        with connection.transaction():
            _ = connection.execute(text.encode('utf-8'))


async def _republish(database: DualRunDatabase) -> None:
    engine = create_async_engine(_alchemy_url(database))
    try:
        async with engine.begin() as connection:
            _ = await StagedLoaderPublisher().republish(connection)
    finally:
        await engine.dispose()


def _publish_after(database: DualRunDatabase, connection: Connection) -> None:
    asyncio.run(_republish(database))


def _provenance_before(connection: Connection, task_id: str) -> Provenance | NoProvenance:
    match connection.execute(_PROVENANCE_BEFORE_SQL, {'id': task_id}).fetchall():
        case [(True, str() as status, datetime() as terminal_at, str() as kind)]:
            return Provenance(status=status, terminal_at=terminal_at, terminalization_kind=kind)
        case [(False, None, None, None)]:
            return NoProvenance()
        case other:
            raise AssertionError(f'two-argument provenance of {task_id} returned {other!r}')


def _provenance_after(connection: Connection, task_id: str) -> Provenance | NoProvenance:
    match connection.execute(_PROVENANCE_AFTER_SQL, {'id': task_id}).fetchall():
        case [(str() as status, datetime() as terminal_at, str() as kind)]:
            return Provenance(status=status, terminal_at=terminal_at, terminalization_kind=kind)
        case []:
            return NoProvenance()
        case other:
            raise AssertionError(f'provenance of {task_id} returned {other!r}')


def _miss_answer(record: tuple[object, ...] | None, task_id: str) -> MissAnswer:
    match record:
        case (str() as outcome, terminal_at, kind, status, worker, guard) if (
            (terminal_at is None or isinstance(terminal_at, datetime))
            and (kind is None or isinstance(kind, str))
            and (status is None or isinstance(status, str))
            and (worker is None or isinstance(worker, str))
            and (guard is None or isinstance(guard, str))
        ):
            return MissAnswer(
                outcome=outcome,
                terminal_at=terminal_at,
                terminalization_kind=kind,
                observed_status=status,
                observed_worker_id=worker,
                guard_kind=guard,
            )
        case _:
            raise AssertionError(f'miss classifier of {task_id} returned {record!r}')


def _miss_before(
    connection: Connection, task_id: str, kind: TerminalizationKind, worker: str | None
) -> MissAnswer:
    kinds = sorted(member.value for member in equivalence_class_of(kind))
    record = connection.execute(
        _MISS_BEFORE_SQL, {'id': task_id, 'kinds': kinds, 'worker': worker}
    ).fetchone()
    return _miss_answer(record, task_id)


def _miss_before_with_the_kind_alone(
    connection: Connection, task_id: str, kind: TerminalizationKind, worker: str | None
) -> MissAnswer:
    """The control: the array names the kind alone, not its class."""
    record = connection.execute(
        _MISS_BEFORE_SQL, {'id': task_id, 'kinds': [kind.value], 'worker': worker}
    ).fetchone()
    return _miss_answer(record, task_id)


def _miss_after(
    connection: Connection, task_id: str, kind: TerminalizationKind, worker: str | None
) -> MissAnswer:
    record = connection.execute(
        _MISS_AFTER_SQL, {'id': task_id, 'kind': kind.value, 'worker': worker}
    ).fetchone()
    return _miss_answer(record, task_id)


async def _python_reads(
    database: DualRunDatabase, task_id: str
) -> tuple[TaskStateResult, TaskResultRead]:
    engine = create_async_engine(_alchemy_url(database))
    try:
        async with engine.connect() as connection:
            return await _reads_on(connection, task_id)
    finally:
        await engine.dispose()


async def _reads_on(
    connection: AsyncConnection, task_id: str
) -> tuple[TaskStateResult, TaskResultRead]:
    state = await read_task_state(connection, task_id=task_id)
    result = await read_task_result(connection, task_id=task_id)
    return state, result


def _answers(
    database: DualRunDatabase,
    connection: Connection,
    seed: Seed,
    provenance: ProvenanceRead,
    miss: MissRead,
) -> tuple[TaskAnswers, ...]:
    answers: list[TaskAnswers] = []
    for name, task_id in seed.ids():
        state, result = asyncio.run(_python_reads(database, task_id))
        match connection.execute(_GUARD_SQL, {'id': task_id}).fetchone():
            case (bool() as guarded,):
                pass
            case other:
                raise AssertionError(f'guard of {task_id} returned {other!r}')
        answers.append(
            TaskAnswers(
                name=name,
                state=state,
                result=result,
                guarded=guarded,
                provenance=provenance(connection, task_id),
                misses=tuple(
                    (kind.value, worker, miss(connection, task_id, kind, worker))
                    for kind in TerminalizationKind
                    for worker in (None, OTHER_WORKER)
                ),
            )
        )
    return tuple(answers)


@dataclass(frozen=True, slots=True)
class Side:
    """How one side publishes and reads."""

    publish: Callable[[DualRunDatabase, Connection], None]
    provenance: ProvenanceRead
    miss: MissRead


BEFORE: Final = Side(publish=_publish_before, provenance=_provenance_before, miss=_miss_before)
BEFORE_WITH_THE_KIND_ALONE: Final = Side(
    publish=_publish_before, provenance=_provenance_before, miss=_miss_before_with_the_kind_alone
)
AFTER: Final = Side(publish=_publish_after, provenance=_provenance_after, miss=_miss_after)


def _run_side(
    database: DualRunDatabase, seed: Seed, side: Side
) -> Result[SideRun, SelfCheckStepFailed]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_run_start(connection):
            case Err(failed):
                return Err(failed)
            case Ok(start):
                pass
        match install_baseline_schema(database):
            case Err(failed):
                return Err(SelfCheckStepFailed(step='install the baseline', cause=str(failed)))
            case Ok():
                pass
        try:
            _write_seed(database, connection, seed)
            side.publish(database, connection)
            with connection.transaction():
                for task in seed.tasks:
                    match task.move:
                        case (status, kind, terminal_at):
                            _ = connection.execute(_TASK_LOCK_SQL, {'id': task.task_id})
                            _ = connection.execute(
                                _MOVE_SQL,
                                {
                                    'id': task.task_id,
                                    'status': status,
                                    'kind': kind,
                                    'terminal_at': terminal_at,
                                },
                            )
                        case None:
                            pass
            answers = _answers(database, connection, seed, side.provenance, side.miss)
        except (psycopg.Error, SQLAlchemyError) as exc:
            return Err(SelfCheckStepFailed(step='seed, publish, move and read', cause=str(exc)))
        match capture_run(connection, start):
            case Err(failed):
                return Err(failed)
            case Ok(run):
                return Ok(SideRun(run=run, answers=answers))


def _ok(result: Result[SideRun, SelfCheckStepFailed]) -> SideRun:
    match result:
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_published_readers_and_the_miss_classifier_answer_as_before(
    empty_database: Callable[[], DualRunDatabase],
) -> None:
    seed = _make_seed()
    reference = _ok(_run_side(empty_database(), seed, BEFORE))
    candidate = _ok(_run_side(empty_database(), seed, AFTER))

    diff = compare_snapshots(reference.run.snapshot, candidate.run.snapshot)

    assert diff.entries == (), render_diff(diff)
    for before, after in zip(reference.answers, candidate.answers, strict=True):
        assert before == after, before.name
    by_name = {answer.name: answer for answer in candidate.answers}
    assert by_name['fast_clock_cancelled'].guarded
    assert isinstance(by_name['not_v7_expired'].provenance, Provenance)
    assert not by_name['unknown'].guarded
    outcomes = {
        miss.outcome for answer in candidate.answers for _, _, miss in answer.misses
    }
    assert outcomes == {
        'TASK_ABSENT',
        'ALREADY_APPLIED',
        'SOURCE_STATE_CONFLICT',
        'LOST_CLAIM',
    }


def _completion_miss(run: SideRun) -> MissAnswer:
    (answer,) = (answer for answer in run.answers if answer.name == 'period_completed')
    (miss,) = (
        miss
        for kind, worker, miss in answer.misses
        if kind == TerminalizationKind.COMPLETE_LOCKED.value and worker is None
    )
    return miss


def test_a_classifier_that_compares_kinds_instead_of_classes_shows_in_the_answers(
    empty_database: Callable[[], DualRunDatabase],
) -> None:
    seed = _make_seed()
    reference = _ok(_run_side(empty_database(), seed, BEFORE_WITH_THE_KIND_ALONE))
    candidate = _ok(_run_side(empty_database(), seed, AFTER))

    # A locked completion replayed over a fused completion is the same class.
    assert _completion_miss(candidate).outcome == 'ALREADY_APPLIED'
    assert _completion_miss(reference).guard_kind == 'FOREIGN_TERMINALIZATION'
    assert reference.answers != candidate.answers
