"""The pytest plugin of the integration dual-run: ``--dual-run=<module>``.

With the option, each integration test (under ``tests/integration``) runs
twice through the ``implementation`` fixture: first with the Python reference
of each operation of the module's table, then on the core. A test that the
module lists as core-only runs once, on the core; one that it lists as
reference-only runs once, on the reference. Each run counts
the calls of the table's operations; the terminal summary lists the tests
that called at least one operation (the informative ones) and the count of
the others. Without the option the fixture changes nothing.

This plugin imports no ``horsies`` module at import: the tables name their
functions by path, and the fixture loads them.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never, cast

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

DUAL_RUN_OPTION: Final = '--dual-run'
DUAL_RUN_TESTS: Final = Path('tests') / 'integration'
"""The directory, under the root directory, whose tests dual-run."""

OPERATION_TABLES: Final[Mapping[SwitchModule, tuple[DualRunOperation, ...]]] = {
    SwitchModule.LIFECYCLE: LIFECYCLE_OPERATIONS,
    SwitchModule.HISTORY: HISTORY_OPERATIONS,
    SwitchModule.BROKER: BROKER_OPERATIONS,
    SwitchModule.MONITORING_AND_WEB: MONITORING_AND_WEB_OPERATIONS,
    SwitchModule.WORKFLOW_ENGINE: WORKFLOW_ENGINE_OPERATIONS,
}

CORE_ONLY_TESTS: Final[Mapping[SwitchModule, tuple[CoreOnlyTest, ...]]] = {
    SwitchModule.LIFECYCLE: LIFECYCLE_CORE_ONLY_TESTS,
    SwitchModule.HISTORY: (),
    SwitchModule.BROKER: (),
    SwitchModule.MONITORING_AND_WEB: MONITORING_AND_WEB_CORE_ONLY_TESTS,
    SwitchModule.WORKFLOW_ENGINE: WORKFLOW_ENGINE_CORE_ONLY_TESTS,
}
"""The tests that run once, on the core, in the dual-run of each module."""

REFERENCE_ONLY_TESTS: Final[Mapping[SwitchModule, tuple[ReferenceOnlyTest, ...]]] = {
    SwitchModule.LIFECYCLE: (),
    SwitchModule.HISTORY: (),
    SwitchModule.BROKER: BROKER_REFERENCE_ONLY_TESTS,
    SwitchModule.MONITORING_AND_WEB: (),
    SwitchModule.WORKFLOW_ENGINE: (),
}
"""The tests that run once, on the Python reference, in the dual-run of each module."""

_COUNTS_KEY: Final = pytest.StashKey[dict[str, int]]()
"""Calls of the table's operations per test id (both runs together)."""


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


def _record_counts(config: pytest.Config, node_id: str, counts: CallCounts) -> None:
    by_test = config.stash.setdefault(_COUNTS_KEY, {})
    base_id = node_id.rsplit('[', 1)[0]
    by_test[base_id] = by_test.get(base_id, 0) + counts.total()


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
    _record_counts(request.config, _test_item(request).nodeid, counts)


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
