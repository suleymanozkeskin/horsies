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
leaves or readers of the run before it. The first run of the session installs
the baseline, runs the seed sequence of the integration tests
(``ensure_history_seedable``) and saves that state. ``--dual-run`` needs the
session database: the PgBouncer lane (``HORSIES_PGBOUNCER_TEST=1``) has none
and is refused.

This plugin imports no ``horsies`` module at import: the tables name their
functions by path, and the fixture loads them.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
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

if TYPE_CHECKING:
    from tests.dualrun.seed_reset import SeedState

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

_COUNTS_KEY: Final = pytest.StashKey[dict[str, int]]()
"""Calls of the table's operations per test id (both runs together)."""

_OPERATION_COUNTS_KEY: Final = pytest.StashKey[dict[tuple[str, Implementation], int]]()
"""Calls per operation name and run (every test together)."""

_SEED_KEY: Final = pytest.StashKey['SeedState']()
"""The seed state of the session database, saved at the first run."""

PGBOUNCER_LANE_VARIABLE: Final = 'HORSIES_PGBOUNCER_TEST'
"""Set to ``1`` in the PgBouncer lane, which has no session database (``tests/session_database.py``)."""


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


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
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


def _reset_session_database(config: pytest.Config) -> None:
    """Bring the session database to its seed state before a run. Fails the test when the reset fails."""
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
                pass
    finally:
        connection.close()


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
    _reset_session_database(request.config)
    operations = tuple(resolve_operation(each) for each in OPERATION_TABLES[switched])
    _isolate_process_registries(monkeypatch)
    counts = CallCounts()
    match implementation_run:
        case Implementation.PYTHON_REFERENCE:
            install_python_run(operations, counts, monkeypatch)
        case Implementation.CORE:
            install_core_run(operations, counts, monkeypatch)
        case _ as unreachable:
            assert_never(unreachable)
    yield DualRunCase(module=switched, implementation=implementation_run, counts=counts)
    _record_counts(request.config, _test_item(request).nodeid, implementation_run, counts)


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
