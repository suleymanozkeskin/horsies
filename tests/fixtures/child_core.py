"""The core handle of a worker's child, built in the test process.

Every write of a child (the start of a run, its miss path and the finalizing
marker) goes through the one core handle of the child process
(``child_pool._get_child_core_handle``), which the child builds from its
init frame (``CoreChildHandle.from_init``). A test builds that frame with
``child_init_frame``. A test that runs the writes against a database of its
own puts a handle for that database in place of the handle of the process
for the time of a block. ``child_database_url`` names the database of the
handle in place: the reference of the start of a child
(``tests.dualrun.reference.stop_before_start_caller``) runs its statements
on a psycopg pool of its own on that database.
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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies import _core as core
from horsies._core import CoreChildHandle
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.utils.url import to_psycopg_url
from horsies.core.worker import child_pool
from tests.fixtures import core_frames

TEST_CHILD_POOL: Final = ResolvedPool(min_size=0, max_size=2, check=False)
"""The pool of a child core handle of the tests: the default bounds of the
pool of an isolated child."""


_HANDLE_URLS: list[str] = []
"""The database of each core handle of a child that this process put in
place, the one in place last."""


def child_database_url() -> str:
    """The database URL of the core handle of a child in place in this
    process. No handle in place is a defect of the test."""
    assert _HANDLE_URLS, 'no core handle of a child is in place in this process'
    return _HANDLE_URLS[-1]


def install_child_core_handle(
    database_url: str,
    pool: ResolvedPool = TEST_CHILD_POOL,
) -> None:
    """Build the core handle of the child of this process for
    ``database_url`` and keep it until ``drop_child_core_handle``; a process
    that holds a handle keeps it."""
    child_pool._initialize_child_core_handle(child_init_frame(database_url, pool))
    _HANDLE_URLS.append(to_psycopg_url(database_url))


def drop_child_core_handle() -> None:
    """Forget the core handle that ``install_child_core_handle`` built."""
    child_pool._drop_child_core_handle()
    _ = _HANDLE_URLS.pop()


def child_init_frame(
    database_url: str,
    pool: ResolvedPool = TEST_CHILD_POOL,
) -> core.InitFrame:
    """The init frame of an isolated child for ``database_url`` with the
    bounds of ``pool``, as the core decodes it."""
    return core_frames.init_frame(
        database_url=to_psycopg_url(database_url),
        child_kind=core_frames.isolated_kind(
            child_pool=core_frames.pool(
                min_size=pool.min_size,
                max_size=pool.max_size,
                check=pool.check,
            ),
        ),
    )


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
    child_pool._initialize_child_core_handle(child_init_frame(database_url, pool))
    _HANDLE_URLS.append(to_psycopg_url(database_url))
    try:
        yield child_pool._get_child_core_handle()
    finally:
        _ = _HANDLE_URLS.pop()
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
