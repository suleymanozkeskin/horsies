"""The core handle of a worker's child, built in the test process.

The start of a run in a child and its miss path go through the one core
handle of the child process (``child_pool._get_child_core_handle``). A test
that runs them against a database of its own puts a handle for that
database in place of the handle of the process for the time of a block.
A test of the bound of a start waits for the start statements of the
handle to end before it reads the row (``wait_for_start_statements_to_end``).
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Final

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies._core import CoreChildHandle
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.worker import child_pool

TEST_CHILD_POOL: Final = ResolvedPool(min_size=0, max_size=2, check=False)
"""The pool of a child core handle of the tests: the default bounds of an
isolated child pool (``_initialize_worker_pool``)."""


@contextmanager
def child_core_handle_for(
    database_url: str,
    pool: ResolvedPool = TEST_CHILD_POOL,
) -> Generator[CoreChildHandle]:
    """A core handle of a child for ``database_url`` with the bounds of
    ``pool``, in place of the handle of this process until the block ends;
    the handle of before comes back."""
    previous = child_pool._child_core_handle
    child_pool._child_core_handle = None
    child_pool._initialize_child_core_handle(
        PostgresConfig(database_url=SecretStr(database_url)),
        pool,
    )
    try:
        yield child_pool._get_child_core_handle()
    finally:
        child_pool._child_core_handle = previous


START_STATEMENT_END_WAIT_S: Final = 10.0
"""The longest wait of a test for the start statements of other sessions to
end."""

_START_STATEMENT_POLL_S: Final = 0.05


async def wait_for_start_statements_to_end(session: AsyncSession) -> int:
    """Wait until no other session runs a ``horsies_start_attempt``
    statement, read every 50 ms for at most ``START_STATEMENT_END_WAIT_S``;
    return the count of such statements at the last read (0 once they
    ended). A start that waited on a row lock and that no bound cancelled
    runs to its end once the lock is gone, so its write shows after this.
    Commits the reads of ``session``."""
    ends_by = time.monotonic() + START_STATEMENT_END_WAIT_S
    while True:
        running = (
            await session.execute(
                text("""
                    SELECT count(*) FROM pg_stat_activity
                    WHERE pid <> pg_backend_pid()
                      AND state = 'active'
                      AND query LIKE '%horsies_start_attempt%'
                """),
            )
        ).scalar_one()
        await session.commit()
        if running == 0 or time.monotonic() >= ends_by:
            return int(running)
        await asyncio.sleep(_START_STATEMENT_POLL_S)
