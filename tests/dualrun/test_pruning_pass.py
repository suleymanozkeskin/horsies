# pyright: reportPrivateUsage=false
"""Dual-run of the pruning pass moving onto the 0006 pruning functions.

Both sides seed the same state on a fresh baseline database (the default
period class and the heartbeat class with a 2 h horizon) and run one pass
with ``StagedLoaderPublisher``:

- reference side: ``reference/pruning.py``, the candidate reads, inspections,
  detach, record, publication and drop statements the pass sent before the
  switch;
- candidate side: ``prune_expired_partitions``, which calls
  ``horsies_prune_candidates``, ``horsies_begin_leaf_detach``,
  ``horsies_record_leaf_detached`` and ``horsies_drop_detached_leaf``
  (0006_history_functions.sql).

Seeded leaves (period leaves of ``standard_30d``, created and published by
``horsies_create_history_leaf``):

- 42 days old, a concurrent detach interrupted by a statement timeout: the
  pass finalizes it, then drops it;
- 41 days old, pinned by a pending phase-2 locator: refused;
- 40 days old, attached: detached and dropped, with its clock exception;
- today's leaf: not expired, untouched;
- an hourly heartbeat leaf 6 h old: detached and dropped.

The snapshots must be equal and the pass reports must be equal. Three cases
pin the rule differences of the switch; one of them seeds a 43-day-old leaf
detached out of band without a catalog record, so the published manifest
still names it.

Caller pairs (the switch of the reaper's pruning to the core), on the same
seeds: reference = the Python caller before the switch
(``reference/pruning_caller.py``: ``prune_expired_partitions`` on the broker
resources' ``partition_maintenance``, summarized); candidate =
``ReaperMixin._run_partition_pruning``, the pass in the core
(``CoreHandle.prune_expired_partitions_async``). The snapshots, the partition
tree included, and the summaries must be equal. An error entry of a
summary compares by its subject, its kind and its SQLSTATE, not by the
driver's message text: psycopg and the core's driver write different texts
for one refusal, and the text is not part of the contract.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final, LiteralString

import psycopg
import psycopg.errors
import pytest
from psycopg import sql
from pydantic import SecretStr
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker

from horsies.core.history.commands import (
    PRUNE_BOUNDS,
    CreateDailyHistoryLeaf,
    LeafBounds,
    LeafRef,
    PruneBounds,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.maintenance.pruning import (
    PrunePass,
    PrunePassSummary,
    prune_expired_partitions,
)
from horsies.core.history.outcomes import LeafCreated, PruneReach
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.partitions.publication import (
    LoaderPublication,
    UnpublishedLoader,
)
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.reaper import ReaperMixin
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import compare_snapshots, render_diff
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.reference import pruning_caller
from tests.dualrun.reference.pruning import prune_expired_partitions_before
from tests.dualrun.selfcheck import (
    SelfCheckRun,
    SelfCheckStepFailed,
    capture_run,
    read_run_start,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

CLASS_KEY: Final = 'standard_30d'
PARENT: Final = 'horsies_task_history_standard_30d'
INTERRUPT_TIMEOUT: Final = '200ms'
"""Statement timeout that interrupts the seeded concurrent detach while a reader holds the parent."""
ATTEMPTS: Final = 2
"""A pair whose two runs start in different UTC hours runs again once: the
fixed leaf names then normalize to different offsets."""

type PrunePassRun = Callable[
    [PartitionMaintenanceDatabase, LoaderPublication, PruneBounds], Awaitable[PrunePass]
]


@dataclass(frozen=True, slots=True)
class Anchors:
    """The lower anchors of the seeded leaves, fixed for both sides of one pair."""

    today: datetime
    heartbeat_hour: datetime

    def history(self, days_ago: int) -> datetime:
        return self.today - timedelta(days=days_ago)


@dataclass(frozen=True, slots=True)
class Seeds:
    """Which states one pair seeds besides today's leaf and the expired heartbeat leaf."""

    expired_history: bool
    """The three expired period leaves (42 to 40 days old) and their states."""
    out_of_band_detach: bool
    """A 43-day-old period leaf, detached out of band without a catalog record."""
    blocked_interrupted: bool
    """A pending locator also pins the interrupted detach (42 days old)."""
    unpublished_leaf: bool
    """Tomorrow's leaf, created without a reader publication."""


STANDARD: Final = Seeds(
    expired_history=True, out_of_band_detach=False, blocked_interrupted=False, unpublished_leaf=False
)


def _anchors() -> Anchors:
    now = datetime.now(UTC)
    return Anchors(
        today=now.replace(hour=0, minute=0, second=0, microsecond=0),
        heartbeat_hour=now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=6),
    )


def _leaf_name(lower: datetime) -> str:
    return f'{PARENT}_{lower:%Y_%m_%d}'


_REGISTER_SQL: Final[LiteralString] = (
    "SELECT count(*) FROM horsies_register_retention_classes('{}'::smallint[], 2::smallint)"
)
_CREATE_HISTORY_LEAF_SQL: Final[LiteralString] = (
    'SELECT outcome FROM horsies_create_history_leaf(%(class_key)s, %(lower)s)'
)
_CREATE_HEARTBEAT_LEAF_SQL: Final[LiteralString] = (
    'SELECT outcome FROM horsies_create_heartbeat_leaf(%(lower)s)'
)
_WORKFLOW_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        %(workflow_id)s, 'dualrun.prune', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL,
        NULL, NULL, %(workflow_id)s, 0, now(), now(), now(), NULL, now()
    )
"""
_NODE_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflow_tasks (
        downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
        priority, dependencies,
        args_from, workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options,
        max_retries, execution, good_until, retention_class_key, status, task_id, is_subworkflow,
        sub_workflow_id, sub_workflow_name, sub_definition_key, sub_workflow_params,
        sub_workflow_summary, result, error, created_at, started_at, completed_at
    ) VALUES (
        'NONE', %(node_id)s, %(workflow_id)s, 0, 'node-0', 'dualrun.move', '{}', 'default', 50, '{}',
        NULL, NULL, FALSE, 'all', NULL, NULL,
        0, 'ISOLATED', NULL, %(class_key)s, 'RUNNING', %(task_id)s, FALSE,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, now(), NULL, NULL
    )
"""
_PENDING_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflow_phase2_pending (
        task_id, workflow_id, workflow_node_row_id, terminalization_kind, recovery_source,
        history_class, history_anchor, history_schema_version, result_digest, quarantine_task_id,
        created_at, attempt_count, last_attempt_at, last_failure_class
    ) VALUES (
        %(task_id)s, %(workflow_id)s, %(node_id)s, 'COMPLETE_LOCKED', 'HISTORY',
        %(class_key)s, %(anchor)s, 1, sha256('{}'::bytea), NULL,
        now(), 0, NULL, NULL
    )
"""
_CLOCK_EXCEPTION_SQL: Final[LiteralString] = """
    INSERT INTO horsies_task_history_clock_exceptions (task_id, retention_class_key, terminal_at)
    VALUES (%(task_id)s, %(class_key)s, %(terminal_at)s)
"""


def _created(connection: Connection, statement: LiteralString, params: dict[str, object]) -> None:
    match connection.execute(statement, params).fetchone():
        case ('CREATED',):
            pass
        case other:
            raise AssertionError(f'leaf creation returned {other!r}')


def _pin(connection: Connection, lower: datetime) -> None:
    ids = {
        'workflow_id': uuid.uuid4(),
        'node_id': uuid.uuid4(),
        'task_id': uuid.uuid4(),
        'class_key': CLASS_KEY,
        'anchor': lower + timedelta(hours=1),
    }
    with connection.transaction():
        _ = connection.execute(_WORKFLOW_SQL, ids)
        _ = connection.execute(_NODE_SQL, ids)
        _ = connection.execute(_PENDING_SQL, ids)


def _detach_sql(lower: datetime, *, concurrently: bool) -> sql.Composed:
    statement = sql.SQL('ALTER TABLE {parent} DETACH PARTITION {leaf}').format(
        parent=sql.Identifier(PARENT), leaf=sql.Identifier(_leaf_name(lower))
    )
    match concurrently:
        case True:
            return statement + sql.SQL(' CONCURRENTLY')
        case False:
            return statement


def _interrupt_detach(database: DualRunDatabase, lower: datetime) -> None:
    """Leave a concurrent detach of the leaf pending: a reader holds the parent
    while the detach waits, and a statement timeout cancels the wait."""
    with (
        psycopg.connect(database.url) as reader,
        psycopg.connect(database.url, autocommit=True) as detacher,
    ):
        _ = reader.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier(PARENT)))
        _ = detacher.execute(
            sql.SQL('SET statement_timeout = {}').format(sql.Literal(INTERRUPT_TIMEOUT))
        )
        with pytest.raises(psycopg.errors.QueryCanceled):
            _ = detacher.execute(_detach_sql(lower, concurrently=True))
        reader.rollback()


async def _create_unpublished_leaf(database: DualRunDatabase, lower: datetime) -> None:
    engine = create_async_engine(sqlalchemy_url(database))
    try:
        async with engine.begin() as connection:
            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(
                    leaf=LeafRef(
                        leaf_name=_leaf_name(lower),
                        class_key=CLASS_KEY,
                        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
                    )
                ),
                UnpublishedLoader(),
            )
        assert isinstance(outcome, LeafCreated), outcome
    finally:
        await engine.dispose()


def _seed_expired_history(
    database: DualRunDatabase, connection: Connection, anchors: Anchors, seeds: Seeds
) -> None:
    for days_ago in (42, 41, 40):
        _created(
            connection,
            _CREATE_HISTORY_LEAF_SQL,
            {'class_key': CLASS_KEY, 'lower': anchors.history(days_ago)},
        )
    _interrupt_detach(database, anchors.history(42))
    _pin(connection, anchors.history(41))
    if seeds.blocked_interrupted:
        _pin(connection, anchors.history(42))
    _ = connection.execute(
        _CLOCK_EXCEPTION_SQL,
        {
            'task_id': uuid.uuid4(),
            'class_key': CLASS_KEY,
            'terminal_at': anchors.history(40) + timedelta(hours=3),
        },
    )


def _seed_out_of_band_detach(connection: Connection, anchors: Anchors) -> None:
    _created(
        connection,
        _CREATE_HISTORY_LEAF_SQL,
        {'class_key': CLASS_KEY, 'lower': anchors.history(43)},
    )
    _ = connection.execute(_detach_sql(anchors.history(43), concurrently=False))


def _seed(
    database: DualRunDatabase, connection: Connection, anchors: Anchors, seeds: Seeds
) -> None:
    """The classes, the leaves and their states. Raises what the drivers raise."""
    _ = connection.execute(_REGISTER_SQL)
    _created(
        connection, _CREATE_HISTORY_LEAF_SQL, {'class_key': CLASS_KEY, 'lower': anchors.today}
    )
    _created(connection, _CREATE_HEARTBEAT_LEAF_SQL, {'lower': anchors.heartbeat_hour})
    if seeds.expired_history:
        _seed_expired_history(database, connection, anchors, seeds)
    if seeds.out_of_band_detach:
        _seed_out_of_band_detach(connection, anchors)
    if seeds.unpublished_leaf:
        asyncio.run(_create_unpublished_leaf(database, anchors.history(-1)))


async def _pass(database: DualRunDatabase, run: PrunePassRun) -> PrunePass:
    engine = create_async_engine(sqlalchemy_url(database))
    try:
        return await run(
            PartitionMaintenanceDatabase(engine, connection_capacity=2),
            StagedLoaderPublisher(),
            PRUNE_BOUNDS,
        )
    finally:
        await engine.dispose()


@dataclass(frozen=True, slots=True)
class SideRun[R]:
    run: SelfCheckRun
    report: R
    unrecorded_after_pass: tuple[str, ...]
    """Leaves without a parent whose detach the catalog does not record after
    the pass. The capture reads a detached relation only through its catalog
    record, so the side records these detaches after this read."""


_UNRECORDED_DETACHED_SQL: Final[LiteralString] = """
    SELECT c.leaf_name
    FROM horsies_task_history_leaf_catalog AS c
    WHERE c.detached_at IS NULL AND c.dropped_at IS NULL
      AND to_regclass(quote_ident(c.leaf_name)) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM pg_inherits AS i WHERE i.inhrelid = to_regclass(quote_ident(c.leaf_name))
      )
    ORDER BY c.leaf_name
"""
_RECORD_DETACHED_SQL: Final[LiteralString] = """
    UPDATE horsies_task_history_leaf_catalog SET detached_at = now() WHERE leaf_name = ANY(%(names)s)
"""


def _record_unrecorded_detaches(connection: Connection) -> tuple[str, ...]:
    names = tuple(
        str(record[0]) for record in connection.execute(_UNRECORDED_DETACHED_SQL).fetchall()
    )
    _ = connection.execute(_RECORD_DETACHED_SQL, {'names': list(names)})
    return names


def _run_side[R](
    database: DualRunDatabase,
    anchors: Anchors,
    seeds: Seeds,
    act: Callable[[DualRunDatabase], Coroutine[object, object, R]],
) -> Result[SideRun[R], SelfCheckStepFailed]:
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
            _seed(database, connection, anchors, seeds)
            report = asyncio.run(act(database))
            unrecorded = _record_unrecorded_detaches(connection)
        except (psycopg.Error, SQLAlchemyError) as exc:
            return Err(SelfCheckStepFailed(step='seed and prune', cause=str(exc)))
        match capture_run(connection, start):
            case Err(failed):
                return Err(failed)
            case Ok(captured):
                return Ok(SideRun(run=captured, report=report, unrecorded_after_pass=unrecorded))


def _ok[R](result: Result[SideRun[R], SelfCheckStepFailed]) -> SideRun[R]:
    match result:
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _same_hour[R](first: SideRun[R], second: SideRun[R]) -> bool:
    hour = 3600
    return first.run.window.start.timestamp() // hour == second.run.window.end.timestamp() // hour


def _act(run: PrunePassRun) -> Callable[[DualRunDatabase], Coroutine[object, object, PrunePass]]:
    return lambda database: _pass(database, run)


def _sides[R](
    make_database: MakeDatabase,
    seeds: Seeds,
    reference_act: Callable[[DualRunDatabase], Coroutine[object, object, R]],
    candidate_act: Callable[[DualRunDatabase], Coroutine[object, object, R]],
) -> tuple[SideRun[R], SideRun[R]]:
    for attempt in range(1, ATTEMPTS + 1):
        anchors = _anchors()
        reference = _ok(_run_side(make_database(), anchors, seeds, reference_act))
        candidate = _ok(_run_side(make_database(), anchors, seeds, candidate_act))
        if attempt < ATTEMPTS and not _same_hour(reference, candidate):
            continue
        return reference, candidate
    raise AssertionError('the loop returns on its last attempt')


def _pair(
    make_database: MakeDatabase, seeds: Seeds
) -> tuple[SideRun[PrunePass], SideRun[PrunePass]]:
    return _sides(
        make_database,
        seeds,
        _act(prune_expired_partitions_before),
        _act(prune_expired_partitions),
    )


def _entries(report: PrunePass) -> list[tuple[str, str, str | None]]:
    return [
        (entry.leaf_name, type(entry.detach).__name__, type(entry.drop).__name__ if entry.drop else None)
        for entry in (*report.heartbeat_swept, *report.history_swept)
    ]


def test_the_pruning_pass_acts_as_the_statements_before_the_switch(
    empty_database: MakeDatabase,
) -> None:
    reference, candidate = _pair(empty_database, STANDARD)

    diff = compare_snapshots(reference.run.snapshot, candidate.run.snapshot)

    assert diff.entries == (), render_diff(diff)
    assert candidate.report == reference.report
    anchors = _anchors()
    assert reference.unrecorded_after_pass == candidate.unrecorded_after_pass == ()
    assert candidate.report.finalized_leaves == (_leaf_name(anchors.history(42)),)
    assert _entries(candidate.report) == [
        (f'horsies_heartbeats_{anchors.heartbeat_hour:%Y_%m_%d_%H}', 'LeafDetached', 'LeafDropped'),
        (_leaf_name(anchors.history(42)), 'LeafDetached', 'LeafDropped'),
        (_leaf_name(anchors.history(41)), 'LeafPendingBlocked', None),
        (_leaf_name(anchors.history(40)), 'LeafDetached', 'LeafDropped'),
    ]
    assert candidate.report.errors == ()
    assert candidate.report.reach is PruneReach.WITHIN_BOUNDS


def test_a_blocked_interrupted_detach_is_also_a_finalize_refusal(
    empty_database: MakeDatabase,
) -> None:
    """Rule difference: the candidate read names every DETACH_PENDING leaf; the
    pass before the switch inspected each candidate and finalized only the
    unblocked ones, so a blocked pending detach was a sweep refusal alone."""
    reference, candidate = _pair(
        empty_database,
        Seeds(
            expired_history=True,
            out_of_band_detach=False,
            blocked_interrupted=True,
            unpublished_leaf=False,
        ),
    )
    blocked = _leaf_name(_anchors().history(42))

    diff = compare_snapshots(reference.run.snapshot, candidate.run.snapshot)

    assert diff.entries == (), render_diff(diff)
    assert f'finalize {blocked}: LeafPendingBlocked' not in reference.report.refusals
    assert f'finalize {blocked}: LeafPendingBlocked' in candidate.report.refusals
    assert f'{blocked}: LeafPendingBlocked' in reference.report.refusals
    assert f'{blocked}: LeafPendingBlocked' in candidate.report.refusals
    assert candidate.report.history_swept == reference.report.history_swept


def test_a_detach_publishes_only_when_the_manifest_names_the_leaf(
    empty_database: MakeDatabase,
) -> None:
    """Rule difference: before the switch a detach republished whenever the
    readers were stale; now the record publishes only when the manifest names
    the detached leaf. A leaf created without publication leaves the readers
    stale; the heartbeat detach republished them before, and does not now."""
    reference, candidate = _pair(
        empty_database,
        Seeds(
            expired_history=False,
            out_of_band_detach=False,
            blocked_interrupted=False,
            unpublished_leaf=True,
        ),
    )

    diff = compare_snapshots(reference.run.snapshot, candidate.run.snapshot)

    assert candidate.report == reference.report
    assert [entry.leaf_name for entry in candidate.report.heartbeat_swept] == [
        f'horsies_heartbeats_{_anchors().heartbeat_hour:%Y_%m_%d_%H}'
    ]
    assert {entry.table for entry in diff.entries} == {'horsies_task_lookup_manifest'}, (
        render_diff(diff)
    )


def test_a_leaf_detached_out_of_band_is_recorded_and_dropped(
    empty_database: MakeDatabase,
) -> None:
    """Rule difference: before the switch the pass never recorded a detach
    made out of band, so the published manifest kept naming the leaf and its
    drop was refused on every pass. Now ``horsies_begin_leaf_detach`` records
    the detach and publishes the readers without the leaf, and the same pass
    drops it."""
    reference, candidate = _pair(
        empty_database,
        Seeds(
            expired_history=False,
            out_of_band_detach=True,
            blocked_interrupted=False,
            unpublished_leaf=False,
        ),
    )
    anchors = _anchors()
    heartbeat = (
        f'horsies_heartbeats_{anchors.heartbeat_hour:%Y_%m_%d_%H}',
        'LeafDetached',
        'LeafDropped',
    )
    detached = _leaf_name(anchors.history(43))

    assert reference.unrecorded_after_pass == (detached,)
    assert candidate.unrecorded_after_pass == ()
    assert _entries(reference.report) == [
        heartbeat,
        (detached, 'LeafDetached', 'DropRefusedLoaderReferences'),
    ]
    assert _entries(candidate.report) == [heartbeat, (detached, 'LeafDetached', 'LeafDropped')]
    assert candidate.report.errors == ()


# ---- The caller switch: the Python reaper caller against the core ----

WORKER_CONNECTIONS: Final = 4
"""The pool size of the worker broker of a caller pair."""

OBJECT_NOT_IN_PREREQUISITE_STATE: Final = '55000'
"""The SQLSTATE of a concurrent detach on a parent that already has a detach
pending: with the interrupted detach blocked, the next expired leaf of the
parent meets it."""

_DRIVER_REFUSAL: Final = re.compile(r'^\(psycopg\.errors\.(?P<error_class>\w+)\) ')
"""A statement that the server refused, as SQLAlchemy writes psycopg's error."""
_CORE_REFUSAL: Final = re.compile(
    r'statement `?\w+`? was refused: (?:cancelled after the bound of [^:]*: )?'
    r'ERROR (?P<sqlstate>[0-9A-Z]{5}): '
)
"""A statement that the server refused, as the core writes it."""


@dataclass(frozen=True, slots=True)
class RefusedStatement:
    """An error entry of a statement that the server refused."""

    subject: str
    """The text before the cause: the leaf, with ``finalize`` or
    ``heartbeat`` in front for those phases."""
    sqlstate: str


@dataclass(frozen=True, slots=True)
class UnreadError:
    """An error entry in neither refusal form. It compares by its whole text."""

    text: str


type ErrorKind = RefusedStatement | UnreadError


@dataclass(frozen=True, slots=True)
class ComparedSummary:
    """A ``PrunePassSummary`` whose errors compare by their kind and code."""

    finalized_leaves: tuple[str, ...]
    detached_count: int
    dropped_count: int
    refusals: tuple[str, ...]
    errors: tuple[ErrorKind, ...]
    reach: PruneReach


def _driver_sqlstate(error_class: str) -> str | None:
    """The SQLSTATE of a psycopg error class name, or None for a name that
    psycopg does not define with one."""
    found: object = getattr(psycopg.errors, error_class, None)
    match found:
        case type() if issubclass(found, psycopg.Error):
            return found.sqlstate
        case _:
            return None


def _error_kind(entry: str) -> ErrorKind:
    """The kind of one error entry of a summary: ``<subject>: <cause>``."""
    match entry.split(': ', 1):
        case [subject, cause]:
            pass
        case _:
            return UnreadError(text=entry)
    driver = _DRIVER_REFUSAL.match(cause)
    if driver is not None:
        match _driver_sqlstate(driver.group('error_class')):
            case str() as sqlstate:
                return RefusedStatement(subject=subject, sqlstate=sqlstate)
            case None:
                return UnreadError(text=entry)
    core = _CORE_REFUSAL.search(cause)
    if core is not None:
        return RefusedStatement(subject=subject, sqlstate=core.group('sqlstate'))
    return UnreadError(text=entry)


def _compared(summary: PrunePassSummary) -> ComparedSummary:
    return ComparedSummary(
        finalized_leaves=summary.finalized_leaves,
        detached_count=summary.detached_count,
        dropped_count=summary.dropped_count,
        refusals=summary.refusals,
        errors=tuple(_error_kind(entry) for entry in summary.errors),
        reach=summary.reach,
    )


def _expected_errors(seeds: Seeds) -> tuple[ErrorKind, ...]:
    """The errors of a caller pass on ``seeds``: none, or with the blocked
    interrupted detach the refused detach of the 40-day leaf."""
    match seeds.blocked_interrupted:
        case True:
            return (
                RefusedStatement(
                    subject=_leaf_name(_anchors().history(40)),
                    sqlstate=OBJECT_NOT_IN_PREREQUISITE_STATE,
                ),
            )
        case False:
            return ()


async def _caller_pass(database: DualRunDatabase, python_caller: bool) -> PrunePassSummary:
    broker = PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(database.url),
            pool_size=WORKER_CONNECTIONS,
            max_overflow=0,
        )
    )
    try:
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'broker resources: {limit.describe()}')
            case Ok(resources):
                pass
        match python_caller:
            case True:
                return await pruning_caller.run_partition_pruning(
                    ReaperMixin(), broker, resources
                )
            case False:
                return await ReaperMixin()._run_partition_pruning(broker, resources)
    finally:
        _ = await broker.close_async()


def _caller_act(python_caller: bool) -> Callable[[DualRunDatabase], Coroutine[object, object, PrunePassSummary]]:
    return lambda database: _caller_pass(database, python_caller)


def _caller_pair(
    make_database: MakeDatabase, seeds: Seeds
) -> tuple[SideRun[PrunePassSummary], SideRun[PrunePassSummary]]:
    return _sides(make_database, seeds, _caller_act(True), _caller_act(False))


@pytest.mark.parametrize(
    'seeds',
    [
        STANDARD,
        Seeds(
            expired_history=True,
            out_of_band_detach=False,
            blocked_interrupted=True,
            unpublished_leaf=False,
        ),
        Seeds(
            expired_history=False,
            out_of_band_detach=True,
            blocked_interrupted=False,
            unpublished_leaf=False,
        ),
        Seeds(
            expired_history=False,
            out_of_band_detach=False,
            blocked_interrupted=False,
            unpublished_leaf=True,
        ),
    ],
    ids=['standard', 'blocked-interrupted', 'out-of-band-detach', 'unpublished-leaf'],
)
def test_the_core_reaper_pass_prunes_as_the_python_reaper_pass(
    empty_database: MakeDatabase, seeds: Seeds
) -> None:
    reference, candidate = _caller_pair(empty_database, seeds)

    diff = compare_snapshots(reference.run.snapshot, candidate.run.snapshot)

    assert diff.entries == (), render_diff(diff)
    assert _compared(candidate.report) == _compared(reference.report)
    assert candidate.unrecorded_after_pass == reference.unrecorded_after_pass == ()
    assert _compared(reference.report).errors == _expected_errors(seeds)


def test_the_core_reaper_pass_reports_the_standard_seeds(
    empty_database: MakeDatabase,
) -> None:
    """The summary of the standard seeds, with Python's refusal text and counts."""
    _, candidate = _caller_pair(empty_database, STANDARD)
    anchors = _anchors()

    assert candidate.report == PrunePassSummary(
        finalized_leaves=(_leaf_name(anchors.history(42)),),
        detached_count=3,
        dropped_count=3,
        refusals=(f'{_leaf_name(anchors.history(41))}: LeafPendingBlocked',),
        errors=(),
        reach=PruneReach.WITHIN_BOUNDS,
    )
    assert candidate.report.acted
