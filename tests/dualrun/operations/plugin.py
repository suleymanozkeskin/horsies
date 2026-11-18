"""The pytest plugin of the integration dual-run: ``--dual-run=<module>``.

With the option, each integration test (under ``tests/integration``) runs
twice through the ``implementation`` fixture: first with the Python reference
of each operation of the module's table, then on the core. A test that the
module lists as core-only runs once, on the core; one that it lists as
reference-only runs once, on the reference. Each run counts
the calls of the table's operations; the terminal summary lists the tests
that called at least one operation (the informative ones) and the count of
the others, then the calls of each operation of the table on the reference
and on the core. Without the option the fixture changes nothing.

Before each run the session database goes back to its seed state
(``tests/dualrun/seed_reset.py``), so that no run sees the rows, classes,
leaves or readers of the run before it.

A test that runs as a pair (reference, then core) is compared: each case
reads its run start from the database clock and listens on the module's
notification channels before its body. After a passing body, the Python
case captures the committed state and its notifications
(``capture_with_notifications``) when it called an operation of the table,
and leaves a record (``tests/dualrun/pair_outcomes.py``). The core case
captures the same way and compares; a difference fails it as
``DualRunMismatch`` or ``ClockBoundaryStraddle``, and a missing or failed
Python case fails it as ``ReferenceRunMissing``. A capture that fails fails
its case, and the core case of its pair, as ``DualRunCaptureFailed`` with the
subject and the cause; it never counts as an equal snapshot. A capture reads
at most ``TABLE_ROW_LIMIT`` rows per table, and the pair of a test in
``LARGE_SEED_TESTS`` at most ``LARGE_SEED_ROW_LIMIT``. The pair of a test in
``PAIR_RULES`` compares the fact of its rule in the place of the stored
values that hold it (``tests/dualrun/pair_rules.py``). The pair of a test
in a module's ``DECIDED_DIFFERENCES`` compares the whole snapshot; a
difference whose every entry its decided fact explains makes the core case an
expected failure (``xfail``) that names the fact, its decision and the
difference. Every other pair compares the whole snapshot. The first run of
the session installs the baseline, runs the seed sequence of the integration
tests (``ensure_history_seedable``) and saves that state. ``--dual-run`` needs the
session database: the PgBouncer lane (``HORSIES_PGBOUNCER_TEST=1``) has none
and is refused.

The two cases of a pair share the key ``_pair_key``: the case's node id
without the id of ``implementation``. In each case of a pair, the ``uuid4``
that test code holds draws from a generator seeded by that key and by
``PROCESS_DRAW_SALT`` (``_install_paired_draws``), so a name that a test makes
with a random part is the same in both cases and compares exactly. Test code
is every module of the ``tests`` package outside ``tests.dualrun``; the
harness, the Python reference and the library keep the system ``uuid4``.

Rule for dual-runs at once on one server: each process draws its own salt
from the system once, and both cases of a pair run in one process (the
record of the Python case lives in the process). So two processes that run
the same test at once draw other names, for example of the isolated
databases that a test creates (``tests/integration/isolated_database.py``),
also with one database prefix. Runs that share a server still set
different prefixes (``HORSIES_TEST_DATABASE_PREFIX``,
``tests/session_database.py``), so that no run sweeps or drops a database of
another run.

This plugin imports no ``horsies`` module at import: the tables name their
functions by path, and the fixture loads them.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import random
import sys
import types
import uuid
from collections.abc import Callable, Generator, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final, assert_never, cast

import psycopg
import pytest

from tests.dualrun.operations.broker import BROKER_OPERATIONS, BROKER_REFERENCE_ONLY_TESTS
from tests.dualrun.operations.history import HISTORY_OPERATIONS
from tests.dualrun.operations.lifecycle import (
    LIFECYCLE_CORE_ONLY_TESTS,
    LIFECYCLE_DECIDED_DIFFERENCES,
    LIFECYCLE_OPERATIONS,
)
from tests.dualrun.operations.monitoring_and_web import (
    MONITORING_AND_WEB_CORE_ONLY_TESTS,
    MONITORING_AND_WEB_OPERATIONS,
)
from tests.dualrun.operations.scheduler import SCHEDULER_OPERATIONS
from tests.dualrun.operations.workflow_engine import (
    WORKFLOW_ENGINE_CORE_ONLY_TESTS,
    WORKFLOW_ENGINE_OPERATIONS,
)
from tests.dualrun.operations.rebinding import (
    CallCounts,
    install_core_run,
    install_python_run,
    resolve_operation,
)
from tests.dualrun.operations.table import (
    CoreOnlyTest,
    DecidedDifference,
    DualRunOperation,
    Implementation,
    LargeSeedTest,
    PairCompare,
    PairFact,
    PairRule,
    ReferenceOnlyTest,
    SwitchModule,
    WholeSnapshot,
)

from tests.dualrun.catalog import QUEUE_CHANNEL_PREFIX, TASK_CHANNELS, WORKFLOW_CHANNELS

if TYPE_CHECKING:
    from tests.dualrun.capture import CaptureFailure, NotificationListener
    from horsies.core.types.result import Result
    from tests.dualrun.normalize import NormalizedSnapshot
    from tests.dualrun.pair_outcomes import PairKey, ReferenceRecord
    from tests.dualrun.seed_reset import SeedState
    from tests.dualrun.values import RunWindow

DUAL_RUN_OPTION: Final = '--dual-run'
DUAL_RUN_TESTS: Final = Path('tests') / 'integration'
"""The directory, under the root directory, whose tests dual-run."""

OPERATION_TABLES: Final[Mapping[SwitchModule, tuple[DualRunOperation, ...]]] = {
    SwitchModule.LIFECYCLE: LIFECYCLE_OPERATIONS,
    SwitchModule.HISTORY: HISTORY_OPERATIONS,
    SwitchModule.BROKER: BROKER_OPERATIONS,
    SwitchModule.MONITORING_AND_WEB: MONITORING_AND_WEB_OPERATIONS,
    SwitchModule.WORKFLOW_ENGINE: WORKFLOW_ENGINE_OPERATIONS,
    SwitchModule.SCHEDULER: SCHEDULER_OPERATIONS,
}

CORE_ONLY_TESTS: Final[Mapping[SwitchModule, tuple[CoreOnlyTest, ...]]] = {
    SwitchModule.LIFECYCLE: LIFECYCLE_CORE_ONLY_TESTS,
    SwitchModule.HISTORY: (),
    SwitchModule.BROKER: (),
    SwitchModule.MONITORING_AND_WEB: MONITORING_AND_WEB_CORE_ONLY_TESTS,
    SwitchModule.WORKFLOW_ENGINE: WORKFLOW_ENGINE_CORE_ONLY_TESTS,
    SwitchModule.SCHEDULER: (),
}
"""The tests that run once, on the core, in the dual-run of each module."""

REFERENCE_ONLY_TESTS: Final[Mapping[SwitchModule, tuple[ReferenceOnlyTest, ...]]] = {
    SwitchModule.LIFECYCLE: (),
    SwitchModule.HISTORY: (),
    SwitchModule.BROKER: BROKER_REFERENCE_ONLY_TESTS,
    SwitchModule.MONITORING_AND_WEB: (),
    SwitchModule.WORKFLOW_ENGINE: (),
    SwitchModule.SCHEDULER: (),
}
"""The tests that run once, on the Python reference, in the dual-run of each module."""

DECIDED_DIFFERENCES: Final[Mapping[SwitchModule, tuple[DecidedDifference, ...]]] = {
    SwitchModule.LIFECYCLE: LIFECYCLE_DECIDED_DIFFERENCES,
    SwitchModule.HISTORY: (),
    SwitchModule.BROKER: (),
    SwitchModule.MONITORING_AND_WEB: (),
    SwitchModule.WORKFLOW_ENGINE: (),
    SwitchModule.SCHEDULER: (),
}
"""The tests whose pairs can differ by a decided behaviour difference in the
dual-run of each module: the difference is in the module's switched
operations, so the other modules run the core on both sides."""

LARGE_SEED_ROW_LIMIT: Final = 25_000
"""Most rows per table that a capture of a ``LARGE_SEED_TESTS`` pair reads.
The largest seed is 20,000 rows of one table (``test_class_coverage.py``,
``test_task_row_plans.py``)."""

LARGE_SEED_TESTS: Final[tuple[LargeSeedTest, ...]] = (
    LargeSeedTest(
        node_id=(
            'tests/integration/test_class_coverage.py::TestClassReadsUseTheClaimIndex::'
            'test_the_stream_and_head_scans_probe_the_pending_class_index'
        ),
        reason='20,000 PENDING tasks, so that the planner picks the pending class index',
    ),
    LargeSeedTest(
        node_id=(
            'tests/integration/test_monitoring_queries.py::TestListBounds::'
            'test_schedules_past_the_bound_are_cut_and_the_cut_is_logged'
        ),
        reason='LIST_ROW_BOUND + 1 schedule rows, one past the bound of the list',
    ),
    LargeSeedTest(
        node_id=(
            'tests/integration/test_monitoring_queries.py::TestListBounds::'
            'test_workflow_names_past_the_bound_are_cut_and_the_cut_is_logged'
        ),
        reason='LIST_ROW_BOUND + 1 root workflows, one past the bound of the list',
    ),
    LargeSeedTest(
        node_id=(
            'tests/integration/test_monitoring_queries.py::TestListBounds::'
            'test_the_fields_of_the_cut_warning_are_attributes_of_the_record'
        ),
        reason='LIST_ROW_BOUND + 1 schedule rows, one past the bound of the list',
    ),
    LargeSeedTest(
        node_id=(
            'tests/integration/test_task_list_bounds.py::'
            'test_expired_list_stops_at_the_bound_in_deadline_then_id_order'
        ),
        reason='TASK_LIST_ROW_BOUND + 1 expired tasks, one past the bound of the list',
    ),
    LargeSeedTest(
        node_id=(
            'tests/integration/test_task_list_bounds.py::'
            'test_stale_list_stops_at_the_bound_in_heartbeat_then_id_order'
        ),
        reason='TASK_LIST_ROW_BOUND + 1 stale tasks, one past the bound of the list',
    ),
    LargeSeedTest(
        node_id=(
            'tests/integration/test_task_row_plans.py::'
            'test_the_finalizing_marker_reads_its_row_through_the_primary_key'
        ),
        reason='20,000 tasks, so that the planner reads the row through the primary key',
    ),
)
"""The tests whose pairs capture up to ``LARGE_SEED_ROW_LIMIT`` rows per table."""

PAIR_RULES: Final[tuple[PairRule, ...]] = (
    PairRule(
        node_id=(
            'tests/integration/test_monitoring_task_actions.py::TestClaimCancelRace::'
            'test_concurrent_claim_and_cancel_leave_one_consistent_outcome'
        ),
        fact=PairFact.ONE_RACE_WINNER,
        reason=(
            'the claim and the cancel run at once; either can take the PENDING row '
            'first in any run'
        ),
    ),
    PairRule(
        node_id=(
            'tests/integration/test_thread_lifecycle_writes.py::'
            'test_seat_threads_share_a_pool_smaller_than_the_seats'
        ),
        fact=PairFact.HEARTBEAT_SENDERS,
        reason=(
            'six seat threads write their runner heartbeats at once; the send order '
            'and the number of beats follow the run time'
        ),
    ),
    PairRule(
        node_id=(
            'tests/integration/test_enqueue_idempotency_key.py::'
            'test_concurrent_sends_with_two_commands_classify_against_the_winner'
        ),
        fact=PairFact.ONE_KEYED_SEND_WINNER,
        reason=(
            'the sends of one key, with two commands, run at once; the send that '
            'commits first wins, in any order in any run'
        ),
    ),
    PairRule(
        node_id=(
            'tests/integration/test_task_lock_order.py::'
            'test_a_free_race_of_finalize_and_stale_recovery_never_deadlocks'
        ),
        fact=PairFact.ONE_MOVER_PER_ROUND,
        reason=(
            'each round runs the finalize and the stale recovery at once on one '
            'task; either can take the row first in any run'
        ),
    ),
)
"""The tests whose pairs compare the fact of their rule (``PairRule``)."""

_BASE_NOTIFICATION_CHANNELS: Final = (
    *TASK_CHANNELS,
    *WORKFLOW_CHANNELS,
    f'{QUEUE_CHANNEL_PREFIX}default',
)
"""The task and workflow channels, and the wake channel of the default queue."""

NOTIFICATION_CHANNELS: Final[Mapping[SwitchModule, tuple[str, ...]]] = {
    module: _BASE_NOTIFICATION_CHANNELS for module in SwitchModule
}
"""The channels that each case of a module's pair listens on. A module whose
tests send to other queues adds their ``task_queue_<queue>`` channels here."""

_COUNTS_KEY: Final = pytest.StashKey[dict[str, int]]()
"""Calls of the table's operations per test id (both runs together)."""

_OPERATION_COUNTS_KEY: Final = pytest.StashKey[dict[tuple[str, Implementation], int]]()
"""Calls per operation name and run (every test together)."""

_SEED_KEY: Final = pytest.StashKey['SeedState']()
"""The seed state of the session database, saved at the first run."""

_REFERENCES_KEY: Final = pytest.StashKey[dict['PairKey', 'ReferenceRecord']]()
"""The record of each Python case whose core case has not run yet."""

_PAIR_CASE_KEY: Final = pytest.StashKey['PairCase']()
"""On a test item: the case of a pair, from its setup to its capture."""

PGBOUNCER_LANE_VARIABLE: Final = 'HORSIES_PGBOUNCER_TEST'
"""Set to ``1`` in the PgBouncer lane, which has no session database (``tests/session_database.py``)."""

TEST_CODE_PACKAGE: Final = 'tests'
"""The package whose modules are test code for the paired draws."""

DRAW_SALT_BYTES: Final = 16
"""The length of the salt of the paired draws."""

PROCESS_DRAW_SALT: Final = os.urandom(DRAW_SALT_BYTES)
"""The salt of the paired draws of this process, drawn once from the system.
Both cases of a pair run in one process and draw the same values; two
processes draw other values for one pair."""

HARNESS_PACKAGE: Final = 'tests.dualrun'
"""The harness and the Python reference of each operation: they keep the
system ``uuid4``, because the reference is code under test."""


@dataclass(frozen=True, slots=True)
class SingleRun:
    """The test runs once, as shipped: no ``--dual-run``, or a test outside
    ``tests/integration``."""


@dataclass(frozen=True, slots=True)
class DualRunCase:
    """One run of a dual-run test."""

    module: SwitchModule
    implementation: Implementation
    counts: CallCounts


type ImplementationRun = SingleRun | DualRunCase


@dataclass(frozen=True, slots=True)
class PairCase:
    """One case of a pair, between its setup and its capture."""

    key: PairKey
    implementation: Implementation
    counts: CallCounts
    url: str
    start: datetime
    listener: NotificationListener
    row_limit: int
    compare: PairCompare


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        DUAL_RUN_OPTION,
        action='store',
        default=None,
        choices=[module.value for module in SwitchModule],
        help=(
            'run each integration test twice: on the Python reference of the '
            "module's switched operations, then on the core"
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    if selected_module(config) is None:
        return
    match os.environ.get(PGBOUNCER_LANE_VARIABLE):
        case '1':
            raise pytest.UsageError(
                f'{DUAL_RUN_OPTION} resets the session database before each run; '
                f'the PgBouncer lane ({PGBOUNCER_LANE_VARIABLE}=1) has no session database'
            )
        case _:
            pass


def selected_module(config: pytest.Config) -> SwitchModule | None:
    """The module of ``--dual-run``, or ``None`` without the option."""
    value: object = config.getoption(DUAL_RUN_OPTION)
    match value:
        case None:
            return None
        case str():
            return SwitchModule(value)
        case _:
            raise AssertionError(f'{DUAL_RUN_OPTION} holds {value!r}')


def _dual_runs(config: pytest.Config, test_path: Path) -> bool:
    return test_path.is_relative_to(config.rootpath / DUAL_RUN_TESTS)


def _implementations(module: SwitchModule, node_id: str) -> list[Implementation]:
    """The runs of the test ``node_id`` in the dual-run of ``module``: the
    core alone for a core-only test, the reference alone for a
    reference-only test, else the reference and then the core. A test in
    both lists is a defect of the tables."""
    core_only = any(test.node_id == node_id for test in CORE_ONLY_TESTS[module])
    reference_only = any(test.node_id == node_id for test in REFERENCE_ONLY_TESTS[module])
    match (core_only, reference_only):
        case (True, False):
            return [Implementation.CORE]
        case (False, True):
            return [Implementation.PYTHON_REFERENCE]
        case (False, False):
            return [Implementation.PYTHON_REFERENCE, Implementation.CORE]
        case (True, True):
            raise AssertionError(
                f'{node_id} is listed as core-only and as reference-only for {module.value}'
            )


@pytest.hookimpl(tryfirst=True)
def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize ``implementation`` before every other parameter of the
    test (its markers, its fixture parameters), so the id of the
    implementation is the first part of each case id (``_pair_key``)."""
    if selected_module(metafunc.config) is None:
        return
    if 'implementation' not in metafunc.fixturenames:
        return
    if not _dual_runs(metafunc.config, metafunc.definition.path):
        return
    module = selected_module(metafunc.config)
    assert module is not None, 'the dual-run option was checked above'
    implementations = _implementations(module, metafunc.definition.nodeid)
    metafunc.parametrize(
        'implementation',
        implementations,
        indirect=True,
        ids=[each.value for each in implementations],
    )


def _test_item(request: pytest.FixtureRequest) -> pytest.Item:
    """The test item of a function-scoped request. pytest does not type ``node``."""
    node: object = getattr(request, 'node')
    assert isinstance(node, pytest.Item), f'a function fixture request has a test item: {node!r}'
    return node


def _record_counts(
    config: pytest.Config,
    node_id: str,
    implementation: Implementation,
    counts: CallCounts,
) -> None:
    by_test = config.stash.setdefault(_COUNTS_KEY, {})
    base_id = node_id.rsplit('[', 1)[0]
    by_test[base_id] = by_test.get(base_id, 0) + counts.total()
    by_operation = config.stash.setdefault(_OPERATION_COUNTS_KEY, {})
    for operation, calls in counts.by_operation.items():
        key = (operation, implementation)
        by_operation[key] = by_operation.get(key, 0) + calls


def _isolate_process_registries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give each run of a test its own copy of the workflow registries of
    this process, so the second run does not see the definitions that the
    first one registered (``HRS-017 ... already registered``). The teardown
    of the run restores the registries from before it."""
    import importlib
    from weakref import WeakValueDictionary

    registry = importlib.import_module('horsies.core.workflows.registry')
    for name in ('_workflow_defs_by_key', '_active_specs'):
        current: object = getattr(registry, name)
        assert isinstance(current, dict), f'horsies.core.workflows.registry.{name} is a dict'
        copied: dict[object, object] = dict(cast(dict[object, object], current))
        monkeypatch.setattr(registry, name, copied)
    nodes: object = getattr(registry, '_nodes_by_spec')
    assert isinstance(nodes, WeakValueDictionary), (
        'horsies.core.workflows.registry._nodes_by_spec is a WeakValueDictionary'
    )
    copied_nodes: WeakValueDictionary[object, object] = WeakValueDictionary(
        cast(WeakValueDictionary[object, object], nodes),
    )
    monkeypatch.setattr(registry, '_nodes_by_spec', copied_nodes)


async def _install_and_seed(url: str) -> None:
    """Install the baseline on ``url`` and run the seed sequence of the
    integration tests, as the ``broker`` fixture of
    ``tests/integration/conftest.py`` does. Fails the test on a refusal."""
    from pydantic import SecretStr

    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.broker import PostgresConfig
    from horsies.core.types.result import Err, Ok
    from tests.broker_resources_support import current_resources
    from tests.integration.history_seeding import ensure_history_seedable

    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        match await broker.ensure_schema_initialized():
            case Err(refused):
                pytest.fail(f'dual-run seed: the install refused the session database: {refused}')
            case Ok():
                pass
        async with current_resources(broker).engine.begin() as connection:
            await ensure_history_seedable(connection)
    finally:
        closed = await broker.close_async()
    match closed:
        case Err(failed):
            pytest.fail(f'dual-run seed: closing the seed broker failed: {failed}')
        case Ok():
            pass


def _session_connection(url: str) -> psycopg.Connection[tuple[object, ...]]:
    """A new connection to the session database. Fails the test when it cannot connect."""
    from horsies.core.utils.url import to_psycopg_url

    try:
        return psycopg.connect(to_psycopg_url(url))
    except psycopg.Error as exc:
        pytest.fail(f'dual-run reset: connecting to the session database failed: {exc}')


def _saved_seed(config: pytest.Config, url: str) -> SeedState:
    """The seed state of the session; the first call installs, seeds and saves it."""
    from horsies.core.types.result import Err, Ok
    from tests.dualrun.seed_reset import save_seed

    match config.stash.get(_SEED_KEY, None):
        case None:
            pass
        case seed:
            return seed
    asyncio.run(_install_and_seed(url))
    connection = _session_connection(url)
    try:
        match save_seed(connection):
            case Err(error):
                pytest.fail(f'dual-run seed: saving the seed state failed: {error}')
            case Ok(seed):
                config.stash[_SEED_KEY] = seed
                return seed
    finally:
        connection.close()


def _reset_session_database(config: pytest.Config) -> str:
    """Bring the session database to its seed state before a run; its URL.
    Fails the test when the reset fails."""
    from horsies.core.types.result import Err, Ok
    from tests.dualrun.seed_reset import reset_to_seed
    from tests.session_database import session_database_url

    url = session_database_url()
    seed = _saved_seed(config, url)
    connection = _session_connection(url)
    try:
        match reset_to_seed(connection, seed):
            case Err(error):
                pytest.fail(f'dual-run reset: the session database did not go back to its seed: {error}')
            case Ok():
                return url
    finally:
        connection.close()


def _base_node_id(node_id: str) -> str:
    """A node id without its parameters."""
    return node_id.split('[', 1)[0]


def _pair_key(item: pytest.Item) -> PairKey:
    """The key that the two cases of a pair share: the case's node id without
    the id of ``implementation``.

    The ids of the other parameters are the same in both cases. Their
    indices are not: pytest numbers a direct parameter by the position of the
    case in the whole product, so ``[python-X]`` and ``[core-X]`` hold
    different indices of ``X``. ``pytest_generate_tests`` parametrizes
    ``implementation`` first, so its id starts the case id; a case id that
    does not start with it is a defect of this plugin (asserted).
    """
    from tests.dualrun.pair_outcomes import PairKey

    assert isinstance(item, pytest.Function), f'a dual-run case is a test function: {item!r}'
    implementation: object = item.callspec.params.get('implementation')
    assert isinstance(implementation, Implementation), (
        f'{item.nodeid}: a dual-run case has an implementation parameter, not {implementation!r}'
    )
    case_id = item.callspec.id
    first = implementation.value
    prefix = f'{first}-'
    base = _base_node_id(item.nodeid)
    match case_id:
        case _ if case_id == first:
            return PairKey(case_name=base)
        case _ if case_id.startswith(prefix):
            return PairKey(case_name=f'{base}[{case_id.removeprefix(prefix)}]')
        case _:
            raise AssertionError(
                f'{item.nodeid}: the case id {case_id!r} does not start with the '
                f'implementation id {first!r}'
            )


def _paired_uuid4(key: PairKey, salt: bytes) -> Callable[[], uuid.UUID]:
    """A ``uuid4`` whose draws come from a generator seeded by ``salt`` and
    ``key``: with one salt, the n-th draw of a pair's Python case equals the
    n-th draw of its core case; another salt draws other values. Each draw is
    a version 4 UUID."""
    digest = hashlib.sha256(salt + key.case_name.encode('utf-8')).digest()
    generator = random.Random(int.from_bytes(digest, 'big'))

    def draw() -> uuid.UUID:
        return uuid.UUID(int=generator.getrandbits(128), version=4)

    return draw


def _is_test_code(module_name: str) -> bool:
    """Whether ``module_name`` is a module of ``TEST_CODE_PACKAGE`` outside ``HARNESS_PACKAGE``."""
    in_tests = module_name == TEST_CODE_PACKAGE or module_name.startswith(
        f'{TEST_CODE_PACKAGE}.'
    )
    in_harness = module_name == HARNESS_PACKAGE or module_name.startswith(
        f'{HARNESS_PACKAGE}.'
    )
    return in_tests and not in_harness


def _install_paired_draws(
    key: PairKey, salt: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """For this case, give test code a ``uuid4`` that draws from
    ``_paired_uuid4(key, salt)``.

    In each loaded module of test code (``_is_test_code``), a global that
    holds the ``uuid`` module gets a copy of it whose ``uuid4`` is the paired
    draw, and a global that holds ``uuid.uuid4`` gets the paired draw. The
    teardown of ``monkeypatch`` restores them. Does not change the ``uuid``
    module itself: the library and the Python reference keep the system
    draw. A ``uuid`` imported inside a function body, or a module loaded
    after this call, keeps the system draw.
    """
    system_uuid4 = uuid.uuid4
    draw = _paired_uuid4(key, salt)
    paired_uuid = types.ModuleType(uuid.__name__, uuid.__doc__)
    vars(paired_uuid).update(vars(uuid))
    vars(paired_uuid)['uuid4'] = draw
    for name, module in tuple(sys.modules.items()):
        if not _is_test_code(name):
            continue
        for attribute, value in tuple(vars(module).items()):
            match value:
                case types.ModuleType() if value is uuid:
                    monkeypatch.setattr(module, attribute, paired_uuid)
                case _ if value is system_uuid4:
                    monkeypatch.setattr(module, attribute, draw)
                case _:
                    pass


def _open_pair_case(
    item: pytest.Item,
    key: PairKey,
    module: SwitchModule,
    implementation: Implementation,
    counts: CallCounts,
    url: str,
) -> PairCase:
    """Read the run start and open the listener of a pair's case. Fails the test when either fails."""
    from horsies.core.types.result import Err, Ok
    from horsies.core.utils.url import to_psycopg_url
    from tests.dualrun.capture import open_listener, read_database_clock

    connection = _session_connection(url)
    try:
        match read_database_clock(connection):
            case Err(failed):
                pytest.fail(f'dual-run capture: reading the run start failed: {failed}')
            case Ok(start):
                pass
    finally:
        connection.close()
    match open_listener(to_psycopg_url(url), NOTIFICATION_CHANNELS[module]):
        case Err(failed):
            pytest.fail(f'dual-run capture: opening the notification listener failed: {failed}')
        case Ok(listener):
            pass
    return PairCase(
        key=key,
        implementation=implementation,
        counts=counts,
        url=url,
        start=start,
        listener=listener,
        row_limit=_row_limit(_base_node_id(item.nodeid)),
        compare=_pair_compare(module, _base_node_id(item.nodeid)),
    )


def _row_limit(node_id: str) -> int:
    """The most rows per table that the captures of the test ``node_id`` read."""
    match any(test.node_id == node_id for test in LARGE_SEED_TESTS):
        case True:
            return LARGE_SEED_ROW_LIMIT
        case False:
            from tests.dualrun.capture import TABLE_ROW_LIMIT

            return TABLE_ROW_LIMIT


def _pair_compare(module: SwitchModule, node_id: str) -> PairCompare:
    """How the pair of the test ``node_id`` compares in the dual-run of
    ``module``: its rule in ``PAIR_RULES``, or its entry in the module's
    ``DECIDED_DIFFERENCES``, else the whole snapshot. Two entries for one test
    are a defect of the tables."""
    entries: list[PairCompare] = [
        *(rule for rule in PAIR_RULES if rule.node_id == node_id),
        *(decided for decided in DECIDED_DIFFERENCES[module] if decided.node_id == node_id),
    ]
    match entries:
        case []:
            return WholeSnapshot()
        case [entry]:
            return entry
        case _:
            raise AssertionError(
                f'{node_id} has {len(entries)} pair rules and decided differences '
                f'for {module.value}'
            )


def _compare_note(compare: PairCompare) -> str:
    """The pair rule or decided difference of a failure text, or nothing for
    the whole snapshot."""
    match compare:
        case WholeSnapshot():
            return ''
        case PairRule(fact=fact):
            return f' under the pair rule {fact.name} ({fact.value})'
        case DecidedDifference(fact=fact):
            return (
                f' under the decided difference {fact.name}, with entries that it '
                'does not explain'
            )


def _decided_text(decided: DecidedDifference, rendered_diff: str) -> str:
    """The expected-failure text of a pair whose difference a decided fact
    explains: the fact, its decision and the difference.

    The core case raises ``pytest.xfail.Exception`` with it, not
    ``pytest.xfail``: under ``--runxfail`` that function does nothing, and
    the exception still reports the case, as a failure."""
    return (
        f'DecidedDifference {decided.fact.name} ({decided.fact.value}). '
        f'Decision: {decided.decision}.\n{rendered_diff}'
    )


def _capture(
    case: PairCase,
) -> Result[tuple[NormalizedSnapshot, RunWindow], CaptureFailure]:
    """The snapshot and the run window of a case after its body, or the
    capture error. Fails the test when it cannot connect."""
    from horsies.core.types.result import Err, Ok
    from tests.dualrun.capture import (
        capture_with_notifications,
        collect_notifications,
        read_database_clock,
    )
    from tests.dualrun.values import make_run_window

    connection = _session_connection(case.url)
    try:
        match read_database_clock(connection):
            case Err(failed):
                return Err(failed)
            case Ok(end):
                pass
        match make_run_window(case.start, end):
            case Err(invalid):
                return Err(invalid)
            case Ok(window):
                pass
        match collect_notifications(case.listener, connection):
            case Err(failed):
                return Err(failed)
            case Ok(notifications):
                pass
        match capture_with_notifications(
            connection, notifications, window, row_limit=case.row_limit
        ):
            case Err(failed):
                return Err(failed)
            case Ok(snapshot):
                return Ok((snapshot, window))
    finally:
        connection.close()


def _capture_failed(implementation: Implementation, error: CaptureFailure) -> str:
    """The failure text of a capture error: a harness error, not a test outcome."""
    from tests.dualrun.capture import render_capture_failure

    return (
        f'DualRunCaptureFailed: the capture after the {implementation.value} case '
        f'failed: {render_capture_failure(error)}'
    )


def _references(config: pytest.Config) -> dict[PairKey, ReferenceRecord]:
    return config.stash.setdefault(_REFERENCES_KEY, {})


def _after_reference_body(config: pytest.Config, case: PairCase) -> None:
    """Leave the record of a passing Python case. Fails the test when its capture fails."""
    from horsies.core.types.result import Err, Ok
    from tests.dualrun.pair_outcomes import (
        ReferenceCaptured,
        ReferenceCaptureFailed,
        ReferenceNotInformative,
    )

    match case.counts.total():
        case 0:
            _references(config)[case.key] = ReferenceNotInformative()
            return
        case _:
            pass
    match _capture(case):
        case Ok((snapshot, window)):
            _references(config)[case.key] = ReferenceCaptured(snapshot=snapshot, window=window)
        case Err(error):
            _references(config)[case.key] = ReferenceCaptureFailed(error=error)
            pytest.fail(_capture_failed(case.implementation, error))


def _after_core_body(config: pytest.Config, case: PairCase) -> None:
    """Compare a passing core case with its Python case. Fails the test on a
    difference, a missing reference, or a capture error of either case."""
    from horsies.core.types.result import Err, Ok
    from tests.dualrun.compare import render_diff
    from tests.dualrun.pair_outcomes import (
        ClockBoundaryStraddle,
        CompareSnapshots,
        DecidedDifferenceSeen,
        DualRunMismatch,
        NoCompare,
        NoReference,
        PairEqual,
        ReferenceNotCaptured,
        ReferenceRunMissing,
        core_step,
        judge_pair,
    )

    match core_step(_references(config).pop(case.key, NoReference())):
        case NoCompare():
            return
        case ReferenceRunMissing(found=found):
            pytest.fail(f'ReferenceRunMissing: the Python case left {found}; nothing to compare')
        case ReferenceNotCaptured(error=error):
            pytest.fail(
                f'{_capture_failed(Implementation.PYTHON_REFERENCE, error)}; nothing to compare'
            )
        case CompareSnapshots(reference=reference):
            pass
    match _capture(case):
        case Err(error):
            pytest.fail(_capture_failed(case.implementation, error))
        case Ok((snapshot, window)):
            pass
    note = _compare_note(case.compare)
    match judge_pair(reference, snapshot, window, case.compare):
        case PairEqual():
            return
        case DecidedDifferenceSeen(diff=diff, decided=decided):
            raise pytest.xfail.Exception(_decided_text(decided, render_diff(diff)))
        case DualRunMismatch(diff=diff):
            pytest.fail(f'DualRunMismatch{note}:\n{render_diff(diff)}')
        case ClockBoundaryStraddle(diff=diff, reference_window=left, candidate_window=right):
            pytest.fail(
                f'ClockBoundaryStraddle: a run window crossed an hour (reference {left}, '
                f'core {right}); the snapshots differ{note}:\n{render_diff(diff)}'
            )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Generator[None, object, object]:
    """After a passing body of a pair's case: capture, and compare in the core case.

    The capture runs before the fixture teardown. A Python case that skips
    or fails leaves that record for its core case.
    """
    case = item.stash.get(_PAIR_CASE_KEY, None)
    if case is None:
        return (yield)
    from tests.dualrun.pair_outcomes import ReferenceFailed, ReferenceSkipped

    try:
        result = yield
        match case.implementation:
            case Implementation.PYTHON_REFERENCE:
                _after_reference_body(item.config, case)
            case Implementation.CORE:
                _after_core_body(item.config, case)
    except pytest.skip.Exception:
        if case.implementation is Implementation.PYTHON_REFERENCE:
            _references(item.config)[case.key] = ReferenceSkipped()
        raise
    except BaseException:
        if case.implementation is Implementation.PYTHON_REFERENCE:
            _references(item.config).setdefault(case.key, ReferenceFailed())
        raise
    return result


@pytest.fixture(autouse=True)
def implementation(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[ImplementationRun]:
    """The implementation of this run; with ``--dual-run``, installs it."""
    module = selected_module(request.config)
    chosen: object = getattr(request, 'param', None)
    match (module, chosen):
        case (SwitchModule() as switched, Implementation() as implementation_run):
            pass
        case _:
            yield SingleRun()
            return
    url = _reset_session_database(request.config)
    item = _test_item(request)
    counts = CallCounts()
    operations = tuple(resolve_operation(each) for each in OPERATION_TABLES[switched])
    _isolate_process_registries(monkeypatch)
    match implementation_run:
        case Implementation.PYTHON_REFERENCE:
            install_python_run(operations, counts, monkeypatch)
        case Implementation.CORE:
            install_core_run(operations, counts, monkeypatch)
        case _ as unreachable:
            assert_never(unreachable)
    pair = len(_implementations(switched, _base_node_id(item.nodeid))) == 2
    if pair:
        key = _pair_key(item)
        _install_paired_draws(key, PROCESS_DRAW_SALT, monkeypatch)
        item.stash[_PAIR_CASE_KEY] = _open_pair_case(
            item, key, switched, implementation_run, counts, url
        )
    try:
        yield DualRunCase(module=switched, implementation=implementation_run, counts=counts)
    finally:
        if pair:
            item.stash[_PAIR_CASE_KEY].listener.connection.close()
            del item.stash[_PAIR_CASE_KEY]
    _record_counts(request.config, item.nodeid, implementation_run, counts)


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    config = terminalreporter.config
    module = selected_module(config)
    if module is None:
        return
    by_test = config.stash.get(_COUNTS_KEY, dict[str, int]())
    informative = sorted(test_id for test_id, calls in by_test.items() if calls > 0)
    terminalreporter.section(f'dual-run {module.value}: calls of the operation table')
    terminalreporter.write_line(
        f'{len(informative)} test(s) called an operation; '
        f'{len(by_test) - len(informative)} called none (not informative for this module)'
    )
    for test_id in informative:
        terminalreporter.write_line(f'  {test_id}: {by_test[test_id]} call(s)')
    by_operation = config.stash.get(_OPERATION_COUNTS_KEY, dict[tuple[str, Implementation], int]())
    terminalreporter.write_line('calls per operation, on the reference and on the core:')
    for operation in OPERATION_TABLES[module]:
        reference_calls = by_operation.get((operation.name, Implementation.PYTHON_REFERENCE), 0)
        core_calls = by_operation.get((operation.name, Implementation.CORE), 0)
        terminalreporter.write_line(
            f'  {operation.name}: {reference_calls} on the reference, {core_calls} on the core'
        )
