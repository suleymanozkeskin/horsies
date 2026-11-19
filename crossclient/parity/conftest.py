"""Fixtures of the parity runs: fresh `dualrun_` databases on the test server."""

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


@pytest.fixture
def empty_database() -> Iterator[Callable[[], DualRunDatabase]]:
    """A factory of empty `dualrun_` databases. Each one is dropped at teardown."""
    server = base_database_url()
    created: list[DualRunDatabase] = []

    def make() -> DualRunDatabase:
        match create_database(server):
            case Err(failed):
                pytest.fail(f'{failed.step.value} {failed.database}: {failed.cause}')
            case Ok(database):
                created.append(database)
                return database

    yield make
    failures = [
        failed
        for database in created
        for failed in [drop_database(server, database)]
        if isinstance(failed, Err)
    ]
    assert not failures, f'dropping parity databases failed: {failures}'
