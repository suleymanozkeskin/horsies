"""The one name prefix of the databases that the test suites create."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid

import pytest

from tests.session_database import (
    DATABASE_PREFIX_VARIABLE,
    DEFAULT_DATABASE_PREFIX,
    _checked_name,
    database_name_prefix,
)

pytestmark = [pytest.mark.unit]


def test_unset_gives_the_default_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(DATABASE_PREFIX_VARIABLE, raising=False)
    assert database_name_prefix() == DEFAULT_DATABASE_PREFIX == 'horsies_test_'


def test_empty_gives_the_default_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DATABASE_PREFIX_VARIABLE, '')
    assert database_name_prefix() == DEFAULT_DATABASE_PREFIX


def test_a_valid_value_is_the_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DATABASE_PREFIX_VARIABLE, 'pyexit_')
    assert database_name_prefix() == 'pyexit_'


@pytest.mark.parametrize(
    'value',
    [
        'pyexit',
        'Pyexit_',
        '_pyexit_',
        '1pyexit_',
        'py-exit_',
        '../postgres_',
        'a' * 24 + '_',
    ],
)
def test_an_invalid_value_stops_the_run(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv(DATABASE_PREFIX_VARIABLE, value)
    with pytest.raises(RuntimeError, match=DATABASE_PREFIX_VARIABLE):
        database_name_prefix()


def test_the_longest_valid_prefix_is_24_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    value = 'a' * 23 + '_'
    monkeypatch.setenv(DATABASE_PREFIX_VARIABLE, value)
    assert database_name_prefix() == value


def test_only_session_names_of_the_prefix_may_be_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DATABASE_PREFIX_VARIABLE, 'pyexit_')
    own = f'pyexit_{uuid.uuid4().hex}'
    assert _checked_name(own) == own
    for foreign in (
        f'horsies_test_{uuid.uuid4().hex}',
        f'pyexit_iso_{uuid.uuid4().hex}',
        f'pyexit_{uuid.uuid4().hex.upper()}',
        'pyexit_',
    ):
        with pytest.raises(RuntimeError, match='refuse to drop'):
            _checked_name(foreign)
