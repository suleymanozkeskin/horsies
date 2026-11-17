"""The core handle of a worker's child, built in the test process.

The start of a run in a child and its miss path go through the one core
handle of the child process (``child_pool._get_child_core_handle``). A test
that runs them against a database of its own puts a handle for that
database in place of the handle of the process for the time of a block.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Final

from pydantic import SecretStr

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
