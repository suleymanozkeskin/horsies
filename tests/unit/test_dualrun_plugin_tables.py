"""The test lists of the integration dual-run plugin.

Each listed node id names a test that exists, and no test is both core-only
and reference-only for one module. A wrong id runs the test twice, and its
run on the other implementation trips. A wrong id in ``LARGE_SEED_TESTS``
leaves its test at the default capture limit.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Final

import pytest

from tests.dualrun.operations.plugin import (
    CORE_ONLY_TESTS,
    LARGE_SEED_ROW_LIMIT,
    LARGE_SEED_TESTS,
    REFERENCE_ONLY_TESTS,
    _implementations,  # pyright: ignore[reportPrivateUsage]
    _row_limit,  # pyright: ignore[reportPrivateUsage]
)
from tests.dualrun.operations.table import Implementation, SwitchModule

pytestmark = [pytest.mark.unit]

ROOT: Final = Path(__file__).resolve().parents[2]
"""The repository root, which the node ids are relative to."""

type Definition = ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef


def _named(body: list[ast.stmt], name: str) -> list[Definition]:
    return [
        node
        for node in body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]


def _defines(node_id: str) -> bool:
    """Whether the file of ``node_id`` defines its class and function path."""
    path, *names = node_id.split('::')
    body: list[ast.stmt] = ast.parse((ROOT / path).read_text(encoding='utf-8')).body
    for name in names:
        match _named(body, name):
            case [definition]:
                body = definition.body
            case _:
                return False
    return bool(names)


def _listed() -> list[tuple[SwitchModule, str]]:
    return [
        (module, test.node_id)
        for tables in (CORE_ONLY_TESTS, REFERENCE_ONLY_TESTS)
        for module, tests in tables.items()
        for test in tests
    ]


@pytest.mark.parametrize(('module', 'node_id'), _listed())
def test_each_listed_test_exists(module: SwitchModule, node_id: str) -> None:
    assert _defines(node_id), f'{module.value}: {node_id} names no test'


@pytest.mark.parametrize('node_id', [test.node_id for test in LARGE_SEED_TESTS])
def test_each_large_seed_test_exists_and_reads_the_large_limit(node_id: str) -> None:
    assert _defines(node_id), f'{node_id} names no test'
    assert _row_limit(node_id) == LARGE_SEED_ROW_LIMIT


def test_a_test_outside_the_large_seed_list_reads_the_default_limit() -> None:
    from tests.dualrun.capture import TABLE_ROW_LIMIT

    node_id = 'tests/integration/test_claim_function.py::test_per_queue_cap_never_over_claims'

    assert _defines(node_id)
    assert _row_limit(node_id) == TABLE_ROW_LIMIT < LARGE_SEED_ROW_LIMIT


def test_no_test_is_core_only_and_reference_only() -> None:
    for module in SwitchModule:
        core_only = {test.node_id for test in CORE_ONLY_TESTS[module]}
        reference_only = {test.node_id for test in REFERENCE_ONLY_TESTS[module]}
        assert core_only.isdisjoint(reference_only), module


def test_a_listed_test_runs_once_on_its_implementation() -> None:
    reference_only = REFERENCE_ONLY_TESTS[SwitchModule.BROKER][0].node_id
    core_only = CORE_ONLY_TESTS[SwitchModule.LIFECYCLE][0].node_id

    assert _implementations(SwitchModule.BROKER, reference_only) == [
        Implementation.PYTHON_REFERENCE
    ]
    assert _implementations(SwitchModule.LIFECYCLE, core_only) == [Implementation.CORE]
    assert _implementations(SwitchModule.HISTORY, reference_only) == [
        Implementation.PYTHON_REFERENCE,
        Implementation.CORE,
    ]


def _undo_calls(path: Path) -> list[int]:
    """The lines of every ``<receiver>.undo()`` call in ``path``."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == 'undo'
    ]


def test_no_integration_test_undoes_the_patches_of_the_dual_run() -> None:
    """The dual-run puts each reference in place with the test's own
    ``monkeypatch`` fixture. ``monkeypatch.undo()`` in a test also removes
    that rebinding, so the rest of its Python case runs production code. A
    test scopes a patch of its own with ``pytest.MonkeyPatch.context()``."""
    found = {
        str(path.relative_to(ROOT)): lines
        for path in sorted((ROOT / 'tests' / 'integration').rglob('*.py'))
        if (lines := _undo_calls(path))
    }
    assert found == {}
