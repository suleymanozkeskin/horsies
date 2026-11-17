# pyright: reportPrivateUsage=false
"""Dual-run of the coverage pass moving onto the 0008 coverage functions.

Each pair seeds one state on two fresh baseline databases and runs
``maintain_partition_coverage`` (history horizon 2 days, heartbeat horizon
2 hours, the declared class ``standard_7d``):

- reference side: the production orchestration with the four steps before
  the switch (``reference/coverage.py``): the set-based probe statement, the
  Python registration, and the Python leaf creation with an unpublished
  loader;
- candidate side: ``maintain_partition_coverage`` on
  ``horsies_coverage_probe``, ``horsies_register_retention_classes``,
  ``horsies_create_history_leaf`` and ``horsies_create_heartbeat_leaf``
  (0008_history_maintenance_functions.sql).

States: a fresh install; a damaged set (a changed heartbeat horizon, a
missing history index, a missing heartbeat index, a stale manifest); a set
whose newest leaves are absent; a heartbeat leaf whose catalog row has
another index shape version. The snapshots must be equal and the outcomes
must be equal, except where a created history leaf is published by its own
creation (a decided rule): the candidate's final step then finds the
readers current and reports ``republished=False``.

The caller switch adds a second pair with the same seeds: reference = the
Python callers before the switch (``reference/coverage_caller.py``: the
reaper's periodic pass and the worker's startup pass on
``partition_maintenance``), candidate = the callers after it, which run the
pass in the core: ``ReaperMixin._run_partition_coverage`` itself, and the
body of ``Worker._ensure_startup_coverage`` (``ensure_startup_coverage_in_core``
with the broker's core handle) against the body of the reference
(``ensure_startup_coverage_in_database``). The gate rule pairs hold the coverage
gate in another session during the pass: with one connection neither side
takes the gate and both repair; with four both find it busy.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Final, LiteralString

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    CoverageEnsureFailed,
    CoverageOutcome,
    CoveredForever,
    CoveredUntil,
    StartupCoverageRefused,
    ensure_startup_coverage_in_core,
    ensure_startup_coverage_in_database,
    maintain_partition_coverage,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.types.result import Err, Ok
from horsies.core.worker.reaper import ReaperMixin
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.reference import coverage_caller
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, SideRun, dual_run
from tests.dualrun.reference.coverage import statements_before

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

HISTORY_HORIZON_DAYS: Final = 2
HEARTBEAT_HORIZON_HOURS: Final = 2
DECLARED: Final = (('standard_7d', timedelta(days=7)),)


@dataclass(frozen=True, slots=True)
class CoveredDaysAfterStart:
    """`CoveredUntil` as whole days after the start of the run's UTC day."""

    days: int


type HistoryCoverageAnswer = CoveredForever | CoveredDaysAfterStart


def _history_coverage_answer(
    coverage: CoveredForever | CoveredUntil,
    day: datetime,
) -> HistoryCoverageAnswer:
    match coverage:
        case CoveredForever():
            return coverage
        case CoveredUntil(at=at):
            return CoveredDaysAfterStart(days=(at - day).days)


@dataclass(frozen=True, slots=True)
class Answer:
    """A coverage outcome with its instants as offsets from the run start.

    A failure's refusal text names Python outcome reprs, which differ in
    their detail text; ``refusal_kinds`` keeps the conflict kinds it names.
    """

    kind: str
    created_history_leaves: int | None
    created_heartbeat_leaves: int | None
    republished: bool | None
    heartbeat_covered_now: bool
    history_coverage: HistoryCoverageAnswer | None
    heartbeats_covered_hours: int | None
    stage: str | None
    class_key: str | None
    refusal_kinds: tuple[str, ...]
    absent_leaves: tuple[str, ...]


_CONFLICT_KINDS: Final = (
    'METADATA_MISMATCH',
    'PHYSICAL_NONCONFORMANT',
    'RELATION_WITHOUT_CATALOG',
    'CATALOG_WITHOUT_RELATION',
    'FOREIGN_INDEX',
)


def _answer(outcome: CoverageOutcome, run_start: datetime) -> Answer:
    start = run_start.astimezone(UTC)
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    hour = start.replace(minute=0, second=0, microsecond=0)
    match outcome:
        case CoverageEnsured():
            return Answer(
                kind='ENSURED',
                created_history_leaves=outcome.created_history_leaves,
                created_heartbeat_leaves=outcome.created_heartbeat_leaves,
                republished=outcome.republished,
                heartbeat_covered_now=outcome.heartbeat_covered_now,
                history_coverage=_history_coverage_answer(
                    outcome.history_covered_through,
                    day,
                ),
                heartbeats_covered_hours=int(
                    (outcome.heartbeats_covered_through - hour) / timedelta(hours=1)
                ),
                stage=None,
                class_key=None,
                refusal_kinds=(),
                absent_leaves=outcome.absent_leaves,
            )
        case CoverageEnsureFailed():
            return Answer(
                kind='FAILED',
                created_history_leaves=None,
                created_heartbeat_leaves=None,
                republished=None,
                heartbeat_covered_now=outcome.heartbeat_covered_now,
                history_coverage=None,
                heartbeats_covered_hours=None,
                stage=outcome.stage,
                class_key=outcome.class_key,
                refusal_kinds=tuple(kind for kind in _CONFLICT_KINDS if kind in outcome.refusal),
                absent_leaves=outcome.absent_leaves,
            )


async def _maintain(database: DualRunDatabase) -> CoverageOutcome:
    engine = create_async_engine(sqlalchemy_url(database))
    try:
        return await maintain_partition_coverage(
            PartitionMaintenanceDatabase(engine, connection_capacity=2),
            history_horizon_days=HISTORY_HORIZON_DAYS,
            heartbeat_horizon_hours=HEARTBEAT_HORIZON_HOURS,
            declared_classes=DECLARED,
        )
    finally:
        await engine.dispose()


def _before(database: DualRunDatabase, _seed: None) -> CoverageOutcome:
    with statements_before():
        return asyncio.run(_maintain(database))


def _after(database: DualRunDatabase, _seed: None) -> CoverageOutcome:
    return asyncio.run(_maintain(database))


REFERENCE: Final = OperationSide[None, CoverageOutcome](name='reference', act=_before)
CANDIDATE: Final = OperationSide[None, CoverageOutcome](name='candidate', act=_after)


# ---- Seeds ----

_REGISTER_SQL: Final[LiteralString] = (
    "SELECT count(*) FROM horsies_register_retention_classes('{7}'::smallint[], %(hours)s::smallint)"
)
_CREATE_HISTORY_LEAVES_SQL: Final[LiteralString] = """
    SELECT count(*) FILTER (WHERE c.outcome <> 'CREATED')
    FROM (VALUES ('standard_7d'), ('standard_30d')) AS k(class_key)
    CROSS JOIN generate_series(0, %(last_day)s) AS d(day_offset)
    CROSS JOIN LATERAL horsies_create_history_leaf(
        k.class_key, date_trunc('day', now(), 'UTC') + d.day_offset * interval '1 day'
    ) AS c
"""
_CREATE_HEARTBEAT_LEAVES_SQL: Final[LiteralString] = """
    SELECT count(*) FILTER (WHERE c.outcome <> 'CREATED')
    FROM generate_series(0, %(last_hour)s) AS h(hour_offset)
    CROSS JOIN LATERAL horsies_create_heartbeat_leaf(
        date_trunc('hour', now(), 'UTC') + h.hour_offset * interval '1 hour'
    ) AS c
"""
_DROP_TODAY_HISTORY_INDEX_SQL: Final[LiteralString] = """
    SELECT format('DROP INDEX %I', leaf_name || '_task_idx')
    FROM horsies_task_history_leaf_catalog
    WHERE class_key = 'standard_30d' AND lower_anchor = date_trunc('day', now(), 'UTC')
"""
_DROP_CURRENT_HEARTBEAT_INDEX_SQL: Final[LiteralString] = """
    SELECT format('DROP INDEX %I', leaf_name || '_probe_idx')
    FROM horsies_task_history_leaf_catalog
    WHERE class_key = 'heartbeats' AND lower_anchor = date_trunc('hour', now(), 'UTC')
"""
_STALE_MANIFEST_SQL: Final[LiteralString] = (
    'UPDATE horsies_task_lookup_manifest SET probe_position = probe_position + 100'
)
_OLD_SHAPE_VERSION_SQL: Final[LiteralString] = """
    UPDATE horsies_task_history_leaf_catalog SET index_schema_version = 0
    WHERE class_key = 'heartbeats' AND lower_anchor = date_trunc('hour', now(), 'UTC')
"""


def _require_all_created(record: tuple[object, ...] | None) -> None:
    match record:
        case (0,):
            pass
        case other:
            raise AssertionError(f'{other!r} seeded leaves were not created')


def _create_leaves(connection: Connection, *, last_day: int, last_hour: int) -> None:
    _require_all_created(
        connection.execute(_CREATE_HISTORY_LEAVES_SQL, {'last_day': last_day}).fetchone()
    )
    _require_all_created(
        connection.execute(_CREATE_HEARTBEAT_LEAVES_SQL, {'last_hour': last_hour}).fetchone()
    )


def _run_ddl(connection: Connection, query: LiteralString) -> None:
    match connection.execute(query).fetchone():
        case (str() as statement,):
            _ = connection.execute(statement.encode())
        case other:
            raise AssertionError(f'the seed DDL read returned {other!r}')


def _fresh(_connection: Connection) -> None:
    return None


def _damaged(connection: Connection) -> None:
    _ = connection.execute(_REGISTER_SQL, {'hours': 5})
    _create_leaves(connection, last_day=HISTORY_HORIZON_DAYS, last_hour=HEARTBEAT_HORIZON_HOURS)
    _run_ddl(connection, _DROP_TODAY_HISTORY_INDEX_SQL)
    _run_ddl(connection, _DROP_CURRENT_HEARTBEAT_INDEX_SQL)
    _ = connection.execute(_STALE_MANIFEST_SQL)


def _newest_absent(connection: Connection) -> None:
    _ = connection.execute(_REGISTER_SQL, {'hours': HEARTBEAT_HORIZON_HOURS})
    _create_leaves(
        connection, last_day=HISTORY_HORIZON_DAYS - 1, last_hour=HEARTBEAT_HORIZON_HOURS - 1
    )


def _old_shape_version(connection: Connection) -> None:
    _ = connection.execute(_REGISTER_SQL, {'hours': HEARTBEAT_HORIZON_HOURS})
    _create_leaves(connection, last_day=HISTORY_HORIZON_DAYS, last_hour=HEARTBEAT_HORIZON_HOURS)
    _ = connection.execute(_OLD_SHAPE_VERSION_SQL)


# ---- Runs ----


def _pair(
    make_database: MakeDatabase, seed: Callable[[Connection], None]
) -> OperationDualRun[CoverageOutcome, CoverageOutcome]:
    match dual_run(make_database, seed, REFERENCE, CANDIDATE):
        case Ok(pair):
            return pair
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _answers(run: SideRun[CoverageOutcome]) -> Answer:
    return _answer(run.report, run.run.window.start)


def test_a_fresh_install_is_covered_as_before(empty_database: MakeDatabase) -> None:
    pair = _pair(empty_database, _fresh)

    assert pair.diff.entries == (), render_diff(pair.diff)
    reference = _answers(pair.reference)
    candidate = _answers(pair.candidate)
    assert candidate.kind == 'ENSURED'
    assert candidate.created_history_leaves == 2 * (HISTORY_HORIZON_DAYS + 1)
    assert candidate.created_heartbeat_leaves == HEARTBEAT_HORIZON_HOURS + 1
    # The period classes end with their newest daily leaf; the forever leaf
    # has no end and does not move the answer.
    assert candidate.history_coverage == CoveredDaysAfterStart(
        days=HISTORY_HORIZON_DAYS + 1,
    )
    # Decided rule: each created history leaf is published by its own call.
    assert reference.republished is True
    assert candidate.republished is False
    assert candidate == replace(reference, republished=False)


def test_a_damaged_set_is_repaired_as_before(empty_database: MakeDatabase) -> None:
    pair = _pair(empty_database, _damaged)

    assert pair.diff.entries == (), render_diff(pair.diff)
    candidate = _answers(pair.candidate)
    assert candidate == _answers(pair.reference)
    assert candidate.kind == 'ENSURED'
    assert candidate.republished is True
    assert candidate.created_history_leaves == 0


def test_absent_newest_leaves_are_created_and_published_by_their_creation(
    empty_database: MakeDatabase,
) -> None:
    pair = _pair(empty_database, _newest_absent)

    assert pair.diff.entries == (), render_diff(pair.diff)
    reference = _answers(pair.reference)
    candidate = _answers(pair.candidate)
    assert candidate.created_history_leaves == 2
    assert candidate.created_heartbeat_leaves == 1
    assert reference.republished is True
    assert candidate.republished is False
    assert candidate == replace(reference, republished=False)


def test_a_leaf_of_another_shape_version_fails_the_pass_as_before(
    empty_database: MakeDatabase,
) -> None:
    pair = _pair(empty_database, _old_shape_version)

    assert pair.diff.entries == (), render_diff(pair.diff)
    candidate = _answers(pair.candidate)
    assert candidate == _answers(pair.reference)
    assert candidate.kind == 'FAILED'
    assert candidate.class_key == 'heartbeats'
    assert candidate.refusal_kinds == ('METADATA_MISMATCH',)


# ---- The caller switch: the Python callers against the core ----

RETENTION: Final = RetentionConfig(
    history_leaf_horizon_days=HISTORY_HORIZON_DAYS,
    heartbeat_leaf_horizon_hours=HEARTBEAT_HORIZON_HOURS,
    retention_periods=(timedelta(days=7),),
)
"""The worker's retention config: the horizons and the declared class of the pairs above."""

COVERAGE_GATE_SQL: Final[LiteralString] = (
    "SELECT pg_try_advisory_xact_lock(hashtextextended('horsies:partition-coverage:v1', 1601))"
)


@dataclass(frozen=True, slots=True)
class StartupReady:
    """The startup pass let the boot go on, with this outcome."""

    answer: Answer


@dataclass(frozen=True, slots=True)
class StartupRefused:
    """The startup pass refused the boot: no heartbeat leaf covers now."""

    answer: Answer


type StartupAnswer = StartupReady | StartupRefused

class GateHolder:
    """Holds the coverage gate in a transaction of its own session, as a
    startup pass of another worker does."""

    def __init__(self, database: DualRunDatabase) -> None:
        self._connection = psycopg.connect(database.url)

    def __enter__(self) -> GateHolder:
        match self._connection.execute(COVERAGE_GATE_SQL).fetchone():
            case (True,):
                return self
            case other:
                raise AssertionError(f'the coverage gate was not free: {other!r}')

    def __exit__(self, *_exc: object) -> None:
        self._connection.rollback()
        self._connection.close()


def _worker_broker(database: DualRunDatabase, connections: int) -> PostgresBroker:
    """A broker whose engines and core pools hold ``connections`` connections."""
    return PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(database.url),
            pool_size=connections,
            max_overflow=0,
        )
    )


async def _periodic_pass(
    database: DualRunDatabase,
    connections: int,
    python_caller: bool,
) -> CoverageOutcome:
    broker = _worker_broker(database, connections)
    try:
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'broker resources: {limit.describe()}')
            case Ok(resources):
                pass
        match python_caller:
            case True:
                return await coverage_caller.run_partition_coverage(
                    ReaperMixin(), broker, resources, RETENTION
                )
            case False:
                return await ReaperMixin()._run_partition_coverage(
                    broker, resources, RETENTION
                )
    finally:
        _ = await broker.close_async()


async def _startup_pass(
    database: DualRunDatabase,
    connections: int,
    python_caller: bool,
) -> CoverageOutcome | StartupCoverageRefused:
    broker = _worker_broker(database, connections)
    try:
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'broker resources: {limit.describe()}')
            case Ok(resources):
                pass
        # The two method bodies, with the broker's resources and handle.
        match python_caller:
            case True:
                return await ensure_startup_coverage_in_database(
                    resources.partition_maintenance,
                    history_horizon_days=RETENTION.history_leaf_horizon_days,
                    heartbeat_horizon_hours=RETENTION.heartbeat_leaf_horizon_hours,
                    declared_classes=RETENTION.registrable_classes(),
                )
            case False:
                return await ensure_startup_coverage_in_core(
                    broker._core_handle,
                    history_horizon_days=RETENTION.history_leaf_horizon_days,
                    heartbeat_horizon_hours=RETENTION.heartbeat_leaf_horizon_hours,
                    declared_classes=RETENTION.registrable_classes(),
                )
    finally:
        _ = await broker.close_async()


def _periodic_side(python_caller: bool) -> OperationSide[None, CoverageOutcome]:
    def act(database: DualRunDatabase, _seed: None) -> CoverageOutcome:
        return asyncio.run(_periodic_pass(database, 4, python_caller))

    name = 'Python reaper caller' if python_caller else 'ReaperMixin._run_partition_coverage'
    return OperationSide(name=name, act=act)


def _startup_side(
    python_caller: bool, connections: int, gate_held: bool
) -> OperationSide[None, CoverageOutcome | StartupCoverageRefused]:
    def act(
        database: DualRunDatabase, _seed: None
    ) -> CoverageOutcome | StartupCoverageRefused:
        match gate_held:
            case True:
                with GateHolder(database):
                    return asyncio.run(_startup_pass(database, connections, python_caller))
            case False:
                return asyncio.run(_startup_pass(database, connections, python_caller))

    name = 'Python startup caller' if python_caller else 'ensure_startup_coverage_in_core'
    return OperationSide(name=name, act=act)


def _startup_answer(
    outcome: CoverageOutcome | StartupCoverageRefused, run_start: datetime
) -> StartupAnswer:
    match outcome:
        case StartupCoverageRefused(outcome=refused):
            return StartupRefused(answer=_answer(refused, run_start))
        case CoverageEnsured() | CoverageEnsureFailed():
            return StartupReady(answer=_answer(outcome, run_start))


def _caller_pair[R](
    make_database: MakeDatabase,
    seed: Callable[[Connection], None],
    reference: OperationSide[None, R],
    candidate: OperationSide[None, R],
) -> OperationDualRun[R, R]:
    match dual_run(make_database, seed, reference, candidate):
        case Ok(pair):
            return pair
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


_SEEDS: Final = (
    ('fresh install', _fresh),
    ('damaged set', _damaged),
    ('absent newest leaves', _newest_absent),
    ('another shape version', _old_shape_version),
)


@pytest.mark.parametrize(('label', 'seed'), _SEEDS, ids=[label for label, _ in _SEEDS])
def test_the_core_reaper_pass_covers_as_the_python_reaper_pass(
    empty_database: MakeDatabase,
    label: str,
    seed: Callable[[Connection], None],
) -> None:
    pair = _caller_pair(empty_database, seed, _periodic_side(True), _periodic_side(False))

    assert pair.diff.entries == (), f'{label}: {render_diff(pair.diff)}'
    assert _answers(pair.candidate) == _answers(pair.reference), label


@pytest.mark.parametrize(('label', 'seed'), _SEEDS, ids=[label for label, _ in _SEEDS])
def test_the_core_startup_pass_covers_as_the_python_startup_pass(
    empty_database: MakeDatabase,
    label: str,
    seed: Callable[[Connection], None],
) -> None:
    pair = _caller_pair(
        empty_database, seed, _startup_side(True, 4, False), _startup_side(False, 4, False)
    )

    assert pair.diff.entries == (), f'{label}: {render_diff(pair.diff)}'
    reference = _startup_answer(pair.reference.report, pair.reference.run.window.start)
    candidate = _startup_answer(pair.candidate.report, pair.candidate.run.window.start)
    assert candidate == reference, label


def test_one_connection_takes_no_gate_on_both_sides(empty_database: MakeDatabase) -> None:
    """Python's ``coverage_gate_enabled``: with one connection a startup pass
    does not take the gate, so a gate that another session holds does not
    stop the repair."""
    pair = _caller_pair(
        empty_database, _fresh, _startup_side(True, 1, True), _startup_side(False, 1, True)
    )

    assert pair.diff.entries == (), render_diff(pair.diff)
    reference = _startup_answer(pair.reference.report, pair.reference.run.window.start)
    candidate = _startup_answer(pair.candidate.report, pair.candidate.run.window.start)
    match reference:
        case StartupReady(answer=answer):
            assert answer.kind == 'ENSURED'
        case StartupRefused():
            pytest.fail(f'one connection repairs the set: {reference!r}')
    assert candidate == reference


def test_a_held_gate_refuses_a_cold_startup_on_both_sides(empty_database: MakeDatabase) -> None:
    """With four connections a startup pass takes the gate; another session
    holds it, so neither side repairs, and a fresh install has no heartbeat
    leaf: both refuse the boot at the stage ``coverage_gate_busy``."""
    pair = _caller_pair(
        empty_database, _fresh, _startup_side(True, 4, True), _startup_side(False, 4, True)
    )

    assert pair.diff.entries == (), render_diff(pair.diff)
    reference = _startup_answer(pair.reference.report, pair.reference.run.window.start)
    candidate = _startup_answer(pair.candidate.report, pair.candidate.run.window.start)
    match reference:
        case StartupRefused(answer=answer):
            assert answer.stage == 'coverage_gate_busy'
        case StartupReady():
            pytest.fail(f'a busy gate repairs nothing: {reference!r}')
    assert candidate == reference
