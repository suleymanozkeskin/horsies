"""The server URL of the harness runs: the variables, their order and the local default."""

from __future__ import annotations

import pytest

from tests.parity.snapshot.databases import server_url

pytestmark = [pytest.mark.unit]


def test_server_url_is_the_first_variable_when_both_are_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('HORSIES_TEST_DATABASE_URL', 'postgresql://first@db-a:5433/one')
    monkeypatch.setenv('HORSES_E2E_DB_URL', 'postgresql://second@db-b:5434/two')

    assert server_url() == 'postgresql://first@db-a:5433/one'


def test_server_url_skips_an_empty_first_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('HORSIES_TEST_DATABASE_URL', '')
    monkeypatch.setenv('HORSES_E2E_DB_URL', 'postgresql://second@db-b:5434/two')

    assert server_url() == 'postgresql://second@db-b:5434/two'


def test_server_url_without_a_variable_is_the_local_default_with_the_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('HORSIES_TEST_DATABASE_URL', raising=False)
    monkeypatch.delenv('HORSES_E2E_DB_URL', raising=False)
    monkeypatch.setenv('DB_PASSWORD', 'secret')

    assert server_url() == 'postgresql://postgres:secret@localhost:5432/horsies'
