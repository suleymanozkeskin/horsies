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
Python case fails it as ``ReferenceRunMissing``. The first run of the session installs
the baseline, runs the seed sequence of the integration tests
(``ensure_history_seedable``) and saves that state. ``--dual-run`` needs the
session database: the PgBouncer lane (``HORSIES_PGBOUNCER_TEST=1``) has none
and is refused.

The two cases of a pair share the key ``_pair_key``: the case's node id
without the id of ``implementation``. In each case of a pair, the ``uuid4``
that test code holds draws from a generator seeded by that key
(``_install_paired_draws``), so a name that a test makes with a random part
is the same in both cases and compares exactly. Test code is every module of
the ``tests`` package outside ``tests.dualrun``; the harness, the Python
reference and the library keep the system ``uuid4``.

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
    DualRunOperation,
    Implementation,
    ReferenceOnlyTest,
    SwitchModule,
)

from tests.dualrun.catalog import QUEUE_CHANNEL_PREFIX, TASK_CHANNELS, WORKFLOW_CHANNELS

if TYPE_CHECKING:
    from tests.dualrun.capture import NotificationListener
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


def _paired_uuid4(key: PairKey) -> Callable[[], uuid.UUID]:
    """A ``uuid4`` whose draws come from a generator seeded by ``key``: the
    n-th draw of a pair's Python case equals the n-th draw of its core case.
    Each draw is a version 4 UUID."""
    digest = hashlib.sha256(key.case_name.encode('utf-8')).digest()
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


def _install_paired_draws(key: PairKey, monkeypatch: pytest.MonkeyPatch) -> None:
    """For this case, give test code a ``uuid4`` that draws from ``_paired_uuid4(key)``.

    In each loaded module of test code (``_is_test_code``), a global that
    holds the ``uuid`` module gets a copy of it whose ``uuid4`` is the paired
    draw, and a global that holds ``uuid.uuid4`` gets the paired draw. The
    teardown of ``monkeypatch`` restores them. Does not change the ``uuid``
    module itself: the library and the Python reference keep the system
    draw. A ``uuid`` imported inside a function body, or a module loaded
    after this call, keeps the system draw.
    """
    system_uuid4 = uuid.uuid4
    draw = _paired_uuid4(key)
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
    )


def _capture(case: PairCase) -> tuple[NormalizedSnapshot, RunWindow]:
    """The snapshot and the run window of a case after its body. Fails the test on a capture error."""
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
                pytest.fail(f'dual-run capture: reading the run end failed: {failed}')
            case Ok(end):
                pass
        match make_run_window(case.start, end):
            case Err(invalid):
                pytest.fail(f'dual-run capture: {invalid}')
            case Ok(window):
                pass
        match collect_notifications(case.listener, connection):
            case Err(failed):
                pytest.fail(f'dual-run capture: reading the notifications failed: {failed}')
            case Ok(notifications):
                pass
        match capture_with_notifications(connection, notifications, window):
            case Err(failed):
                pytest.fail(f'dual-run capture: {failed}')
            case Ok(snapshot):
                return snapshot, window
    finally:
        connection.close()


def _references(config: pytest.Config) -> dict[PairKey, ReferenceRecord]:
    return config.stash.setdefault(_REFERENCES_KEY, {})


def _after_reference_body(config: pytest.Config, case: PairCase) -> None:
    """Leave the record of a passing Python case."""
    from tests.dualrun.pair_outcomes import ReferenceCaptured, ReferenceNotInformative

    match case.counts.total():
        case 0:
            _references(config)[case.key] = ReferenceNotInformative()
        case _:
            snapshot, window = _capture(case)
            _references(config)[case.key] = ReferenceCaptured(snapshot=snapshot, window=window)


def _after_core_body(config: pytest.Config, case: PairCase) -> None:
    """Compare a passing core case with its Python case. Fails the test on a difference or a missing reference."""
    from tests.dualrun.compare import render_diff
    from tests.dualrun.pair_outcomes import (
        ClockBoundaryStraddle,
        CompareSnapshots,
        DualRunMismatch,
        NoCompare,
        NoReference,
        PairEqual,
        ReferenceRunMissing,
        core_step,
        judge_pair,
    )

    match core_step(_references(config).pop(case.key, NoReference())):
        case NoCompare():
            return
        case ReferenceRunMissing(found=found):
            pytest.fail(f'ReferenceRunMissing: the Python case left {found}; nothing to compare')
        case CompareSnapshots(reference=reference):
            pass
    snapshot, window = _capture(case)
    match judge_pair(reference, snapshot, window):
        case PairEqual():
            return
        case DualRunMismatch(diff=diff):
            pytest.fail(f'DualRunMismatch:\n{render_diff(diff)}')
        case ClockBoundaryStraddle(diff=diff, reference_window=left, candidate_window=right):
            pytest.fail(
                f'ClockBoundaryStraddle: a run window crossed an hour (reference {left}, '
                f'core {right}); the snapshots differ:\n{render_diff(diff)}'
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
        _install_paired_draws(key, monkeypatch)
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
