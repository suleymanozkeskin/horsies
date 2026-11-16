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

- 43 days old, detached out of band without a catalog record, so the
  published manifest still names it: its drop is refused;
- 42 days old, a concurrent detach interrupted by a statement timeout: the
  pass finalizes it, then drops it;
- 41 days old, pinned by a pending phase-2 locator: refused;
- 40 days old, attached: detached and dropped, with its clock exception;
- today's leaf: not expired, untouched;
- an hourly heartbeat leaf 6 h old: detached and dropped.

The snapshots must be equal and the pass reports must be equal. Two cases pin
the rule differences of the switch.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final, LiteralString

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.history.commands import (
    PRUNE_BOUNDS,
    CreateDailyHistoryLeaf,
    LeafBounds,
    LeafRef,
    PruneBounds,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.maintenance.pruning import PrunePass, prune_expired_partitions
from horsies.core.history.outcomes import LeafCreated, PruneReach
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.partitions.publication import (
    LoaderPublication,
    UnpublishedLoader,
)
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import compare_snapshots, render_diff
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
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
    """The four expired period leaves (43 to 40 days old) and their states."""
    blocked_interrupted: bool
    """A pending locator also pins the interrupted detach (42 days old)."""
    unpublished_leaf: bool
    """Tomorrow's leaf, created without a reader publication."""


STANDARD: Final = Seeds(expired_history=True, blocked_interrupted=False, unpublished_leaf=False)


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
    for days_ago in (43, 42, 41, 40):
        _created(
            connection,
            _CREATE_HISTORY_LEAF_SQL,
            {'class_key': CLASS_KEY, 'lower': anchors.history(days_ago)},
        )
    _ = connection.execute(_detach_sql(anchors.history(43), concurrently=False))
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
class SideRun:
    run: SelfCheckRun
    report: PrunePass
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


def _run_side(
    database: DualRunDatabase, anchors: Anchors, seeds: Seeds, run: PrunePassRun
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
            _seed(database, connection, anchors, seeds)
            report = asyncio.run(_pass(database, run))
            unrecorded = _record_unrecorded_detaches(connection)
        except (psycopg.Error, SQLAlchemyError) as exc:
            return Err(SelfCheckStepFailed(step='seed and prune', cause=str(exc)))
        match capture_run(connection, start):
            case Err(failed):
                return Err(failed)
            case Ok(captured):
                return Ok(SideRun(run=captured, report=report, unrecorded_after_pass=unrecorded))


def _ok(result: Result[SideRun, SelfCheckStepFailed]) -> SideRun:
    match result:
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _same_hour(first: SideRun, second: SideRun) -> bool:
    hour = 3600
    return first.run.window.start.timestamp() // hour == second.run.window.end.timestamp() // hour


def _pair(make_database: MakeDatabase, seeds: Seeds) -> tuple[SideRun, SideRun]:
    for attempt in range(1, ATTEMPTS + 1):
        anchors = _anchors()
        reference = _ok(
            _run_side(make_database(), anchors, seeds, prune_expired_partitions_before)
        )
        candidate = _ok(_run_side(make_database(), anchors, seeds, prune_expired_partitions))
        if attempt < ATTEMPTS and not _same_hour(reference, candidate):
            continue
        return reference, candidate
    raise AssertionError('the loop returns on its last attempt')


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
    assert reference.unrecorded_after_pass == candidate.unrecorded_after_pass == (
        _leaf_name(anchors.history(43)),
    )
    assert candidate.report.finalized_leaves == (_leaf_name(anchors.history(42)),)
    assert _entries(candidate.report) == [
        (f'horsies_heartbeats_{anchors.heartbeat_hour:%Y_%m_%d_%H}', 'LeafDetached', 'LeafDropped'),
        (_leaf_name(anchors.history(43)), 'LeafDetached', 'DropRefusedLoaderReferences'),
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
        Seeds(expired_history=True, blocked_interrupted=True, unpublished_leaf=False),
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
        Seeds(expired_history=False, blocked_interrupted=False, unpublished_leaf=True),
    )

    diff = compare_snapshots(reference.run.snapshot, candidate.run.snapshot)

    assert candidate.report == reference.report
    assert [entry.leaf_name for entry in candidate.report.heartbeat_swept] == [
        f'horsies_heartbeats_{_anchors().heartbeat_hour:%Y_%m_%d_%H}'
    ]
    assert {entry.table for entry in diff.entries} == {'horsies_task_lookup_manifest'}, (
        render_diff(diff)
    )
