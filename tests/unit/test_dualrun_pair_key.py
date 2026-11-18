"""The pair key of the integration dual-run, and the ``uuid4`` draws that the
two cases of a pair share (``tests/dualrun/operations/plugin.py``).

The pair key tests collect a small integration directory with
``--dual-run=broker`` in an inner pytest session. No case runs, so no
database is used.
"""

from __future__ import annotations

import sys
import types
import uuid
from typing import Final

import pytest

from tests.dualrun.operations.plugin import (
    DRAW_SALT_BYTES,
    PROCESS_DRAW_SALT,
    _install_paired_draws,  # pyright: ignore[reportPrivateUsage]
    _pair_key,  # pyright: ignore[reportPrivateUsage]
)
from tests.dualrun.pair_outcomes import PairKey

pytest_plugins = ('pytester',)

pytestmark = [pytest.mark.unit]

_PAIRED_TESTS: Final = '''
import pytest


@pytest.mark.parametrize('command', ['same command', 'other command'])
def test_direct(command: str) -> None:
    pass


@pytest.mark.parametrize('first', [1, 2])
@pytest.mark.parametrize('second', ['x'])
def test_stacked(first: int, second: str) -> None:
    pass


@pytest.fixture(params=['one', 'two'])
def through_fixture(request: pytest.FixtureRequest) -> object:
    return request.param


def test_fixture_parameter(through_fixture: object) -> None:
    pass


def test_plain() -> None:
    pass
'''
"""Tests with a direct parameter, two stacked direct parameters, a fixture
parameter and no parameter. pytest numbers each direct parameter by the
position of the case in the whole product, so its index differs between the
Python case and the core case."""

_MODULE: Final = 'tests/integration/test_paired.py'


def _keys_of_collected_cases(pytester: pytest.Pytester) -> dict[str, list[str]]:
    """The pair key of each collected case, mapped to the case names that share it."""
    pytester.makeini('[pytest]\nasyncio_default_fixture_loop_scope = function\n')
    pytester.makepyfile(**{'tests/integration/test_paired': _PAIRED_TESTS})
    # pytest does not annotate the ``plugins`` parameter of ``inline_run``.
    recorder = pytester.inline_run(  # pyright: ignore[reportUnknownMemberType]
        '--collect-only', '-p', 'tests.dualrun.operations.plugin', '--dual-run=broker'
    )
    keys: dict[str, list[str]] = {}
    for call in recorder.getcalls('pytest_itemcollected'):
        item: object = vars(call)['item']
        assert isinstance(item, pytest.Item), item
        keys.setdefault(_pair_key(item).case_name, []).append(item.name)
    return {key: sorted(names) for key, names in keys.items()}


def test_the_python_case_and_the_core_case_of_each_parameter_share_one_key(
    pytester: pytest.Pytester,
) -> None:
    assert _keys_of_collected_cases(pytester) == {
        f'{_MODULE}::test_direct[same command]': [
            'test_direct[core-same command]',
            'test_direct[python-same command]',
        ],
        f'{_MODULE}::test_direct[other command]': [
            'test_direct[core-other command]',
            'test_direct[python-other command]',
        ],
        f'{_MODULE}::test_stacked[x-1]': [
            'test_stacked[core-x-1]',
            'test_stacked[python-x-1]',
        ],
        f'{_MODULE}::test_stacked[x-2]': [
            'test_stacked[core-x-2]',
            'test_stacked[python-x-2]',
        ],
        f'{_MODULE}::test_fixture_parameter[one]': [
            'test_fixture_parameter[core-one]',
            'test_fixture_parameter[python-one]',
        ],
        f'{_MODULE}::test_fixture_parameter[two]': [
            'test_fixture_parameter[core-two]',
            'test_fixture_parameter[python-two]',
        ],
        f'{_MODULE}::test_plain': ['test_plain[core]', 'test_plain[python]'],
    }


_PROBE_TEST_MODULE: Final = 'tests.integration.paired_draws_probe'
_PROBE_HARNESS_MODULE: Final = 'tests.dualrun.paired_draws_probe'
_PROBE_LIBRARY_MODULE: Final = 'horsies.paired_draws_probe'


def _probe_module(name: str, monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """A loaded module ``name`` that holds the ``uuid`` module and ``uuid.uuid4``."""
    module = types.ModuleType(name)
    vars(module)['uuid'] = uuid
    vars(module)['uuid4'] = uuid.uuid4
    monkeypatch.setitem(sys.modules, name, module)
    return module


def _case_draws(
    key: PairKey, test_code: types.ModuleType, salt: bytes = PROCESS_DRAW_SALT
) -> list[uuid.UUID]:
    """Three draws of ``test_code`` in one case of the pair ``key``, in the
    process whose salt is ``salt``: two through the ``uuid`` module it holds,
    one through the ``uuid4`` it holds."""
    with pytest.MonkeyPatch.context() as case:
        _install_paired_draws(key, salt, case)
        held_module: object = vars(test_code)['uuid']
        held_uuid4: object = vars(test_code)['uuid4']
        assert isinstance(held_module, types.ModuleType)
        assert callable(held_uuid4)
        draws: list[object] = [
            getattr(held_module, 'uuid4')(),
            getattr(held_module, 'uuid4')(),
            held_uuid4(),
        ]
    return [draw for draw in draws if isinstance(draw, uuid.UUID)]


def test_both_cases_of_a_pair_draw_the_same_uuid4_in_test_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_uuid = uuid
    system_uuid4 = uuid.uuid4
    test_code = _probe_module(_PROBE_TEST_MODULE, monkeypatch)
    key = PairKey(case_name='tests/integration/test_x.py::test_y[same command]')

    python_case = _case_draws(key, test_code)
    core_case = _case_draws(key, test_code)
    other_pair = _case_draws(
        PairKey(case_name='tests/integration/test_x.py::test_y[other command]'),
        test_code,
    )

    assert len(python_case) == 3, python_case
    assert python_case == core_case
    assert len(set(python_case)) == len(python_case)
    assert all(draw.version == 4 for draw in python_case)
    assert other_pair != python_case
    assert vars(test_code)['uuid'] is system_uuid
    assert vars(test_code)['uuid4'] is system_uuid4


def test_two_processes_draw_other_values_for_one_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two dual-runs at once run one test each in their own process, with
    their own salt: the names that the test draws, for example of its
    isolated databases, differ."""
    test_code = _probe_module(_PROBE_TEST_MODULE, monkeypatch)
    key = PairKey(case_name='tests/integration/test_x.py::test_y')
    first_process = bytes(DRAW_SALT_BYTES)
    second_process = bytes([1]) * DRAW_SALT_BYTES

    first = _case_draws(key, test_code, first_process)
    second = _case_draws(key, test_code, second_process)

    assert len(PROCESS_DRAW_SALT) == DRAW_SALT_BYTES
    assert first == _case_draws(key, test_code, first_process)
    assert len(first) == len(second) == 3
    assert set(first).isdisjoint(second)


def test_the_harness_and_the_library_keep_the_system_uuid4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_uuid = uuid
    system_uuid4 = uuid.uuid4
    harness = _probe_module(_PROBE_HARNESS_MODULE, monkeypatch)
    library = _probe_module(_PROBE_LIBRARY_MODULE, monkeypatch)
    with pytest.MonkeyPatch.context() as case:
        _install_paired_draws(
            PairKey(case_name='tests/integration/test_x.py::test_y'),
            PROCESS_DRAW_SALT,
            case,
        )
        for module in (harness, library):
            assert vars(module)['uuid'] is system_uuid, module.__name__
            assert vars(module)['uuid4'] is system_uuid4, module.__name__
        assert getattr(system_uuid, 'uuid4') is system_uuid4
