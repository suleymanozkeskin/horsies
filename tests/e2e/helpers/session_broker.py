"""Initialization of the e2e session brokers.

Kept outside ``tests/e2e/conftest.py``: the conftest creates the session
database when it is imported, and unit tests import this function with no
database.
"""

from __future__ import annotations

import os

from horsies.core.brokers.postgres import PostgresBroker

from tests.broker_resources_support import current_resources


async def initialize_session_broker(
    brk: PostgresBroker,
    *,
    seed_history: bool = False,
) -> None:
    """Initialize the schema, and seed the history coverage when asked.

    The PgBouncer-only lane has no default PostgreSQL service on port 5432.
    Its marked smoke test constructs and initializes a dedicated split-URL
    broker, so the default-topology singletons must remain untouched there.
    """
    if os.environ.get('HORSIES_PGBOUNCER_TEST') == '1':
        return
    schema = await brk.ensure_schema_initialized()
    assert schema.is_ok(), schema
    if seed_history:
        from tests.integration.history_seeding import ensure_history_seedable

        async with current_resources(brk).engine.begin() as connection:
            await ensure_history_seedable(connection)
