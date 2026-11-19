"""Fixtures of the dual-run snapshot tests: fresh ``dualrun_`` databases on the test server."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest

from horsies.core.types.result import Err, Ok
from crossclient.parity.snapshot.databases import (
    DualRunDatabase,
    create_database,
    drop_database,
)
from tests.session_database import base_database_url


@pytest.fixture(scope='session')
def dualrun_server_url() -> str:
    """URL of the test server (``HORSIES_TEST_DATABASE_URL``, else the local default)."""
    return base_database_url()


@pytest.fixture
def empty_database(dualrun_server_url: str) -> Iterator[Callable[[], DualRunDatabase]]:
    """A factory of empty ``dualrun_`` databases. Each one is dropped at teardown."""
    created: list[DualRunDatabase] = []

    def make() -> DualRunDatabase:
        match create_database(dualrun_server_url):
            case Err(failed):
                pytest.fail(f'{failed.step.value} {failed.database}: {failed.cause}')
            case Ok(database):
                created.append(database)
                return database

    yield make
    failures = [
        failed
        for database in created
        for failed in [drop_database(dualrun_server_url, database)]
        if isinstance(failed, Err)
    ]
    assert not failures, f'dropping dual-run databases failed: {failures}'
