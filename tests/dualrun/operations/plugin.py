"""The pytest plugin of the integration dual-run: ``--dual-run=<module>``.

With the option, each integration test (under ``tests/integration``) runs
twice through the ``implementation`` fixture: first with the Python reference
of each operation of the module's table, then on the core. Each run counts
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
from typing import Final, assert_never

import pytest

from tests.dualrun.operations.broker import BROKER_OPERATIONS
from tests.dualrun.operations.lifecycle import LIFECYCLE_OPERATIONS
from tests.dualrun.operations.rebinding import (
    CallCounts,
    install_core_run,
    install_python_run,
    resolve_operation,
)
from tests.dualrun.operations.table import (
    DualRunOperation,
    Implementation,
    SwitchModule,
)

DUAL_RUN_OPTION: Final = '--dual-run'
DUAL_RUN_TESTS: Final = Path('tests') / 'integration'
"""The directory, under the root directory, whose tests dual-run."""

OPERATION_TABLES: Final[Mapping[SwitchModule, tuple[DualRunOperation, ...]]] = {
    SwitchModule.LIFECYCLE: LIFECYCLE_OPERATIONS,
    SwitchModule.BROKER: BROKER_OPERATIONS,
}

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


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if selected_module(metafunc.config) is None:
        return
    if 'implementation' not in metafunc.fixturenames:
        return
    if not _dual_runs(metafunc.config, metafunc.definition.path):
        return
    metafunc.parametrize(
        'implementation',
        [Implementation.PYTHON_REFERENCE, Implementation.CORE],
        indirect=True,
        ids=[Implementation.PYTHON_REFERENCE.value, Implementation.CORE.value],
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
