"""The schema signature is session-independent, proven both ways.

The fourth instance of the session-rendering family, built fixed from
day one rather than fixed after firing. The presence half proves the
raw capture — the same query without the UTC pin — genuinely diverges
between the probe timezones on a relation carrying a timestamptz
default and a timestamptz CHECK; the invariance half proves the pinned
capture is identical across the same sessions. Both sides of UTC,
because a single-timezone test proves nothing.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.transcode.signature import (
    RELATION_SCHEMA_SIGNATURE_SQL,
    relation_schema_signature,
)

from tests.integration.task_history_harness import (
    HistorySchema,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration]

WEST_OF_UTC = 'Etc/GMT+12'
EAST_OF_UTC = 'Etc/GMT-12'

history_schema = task_history_schema_fixture(
    'task_history_it_transcode_sig'
)

RELATION_DDL = """
CREATE TABLE transcode_sig_probe (
    id integer PRIMARY KEY,
    recorded_at timestamptz NOT NULL
        DEFAULT TIMESTAMPTZ '2026-06-01 00:00:00+00',
    CHECK (recorded_at >= TIMESTAMPTZ '2026-01-01 00:00:00+00')
)
"""


async def set_session_timezone(
    connection: AsyncConnection, timezone_name: str
) -> None:
    await connection.execute(
        text("SELECT set_config('timezone', :tz, false)"),
        {'tz': timezone_name},
    )


async def probe_oid(connection: AsyncConnection) -> int:
    await connection.execute(text(RELATION_DDL))
    oid = (
        await connection.execute(
            text("SELECT to_regclass('transcode_sig_probe')::oid")
        )
    ).scalar_one()
    assert oid is not None
    return int(oid)


class TestPresenceHalf:
    @pytest.mark.asyncio
    async def test_raw_capture_diverges_across_the_probe_timezones(
        self, history_schema: HistorySchema
    ) -> None:
        """The unpinned query — the prototype's exact capture — must
        produce different hashes across the probes, or the pin guards
        nothing and can be reconsidered."""
        async with history_schema.engine.begin() as connection:
            oid = await probe_oid(connection)
            raw: dict[str, str] = {}
            for timezone_name in (WEST_OF_UTC, EAST_OF_UTC):
                await set_session_timezone(connection, timezone_name)
                raw[timezone_name] = (
                    await connection.execute(
                        text(RELATION_SCHEMA_SIGNATURE_SQL),
                        {'relation_oid': oid},
                    )
                ).scalar_one()
            assert raw[WEST_OF_UTC] != raw[EAST_OF_UTC]


class TestInvarianceHalf:
    @pytest.mark.asyncio
    async def test_pinned_capture_is_identical_across_sessions(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            oid = await probe_oid(connection)
            pinned: dict[str, str | None] = {}
            for timezone_name in (WEST_OF_UTC, EAST_OF_UTC):
                await set_session_timezone(connection, timezone_name)
                pinned[timezone_name] = await relation_schema_signature(
                    connection, oid
                )
            assert pinned[WEST_OF_UTC] is not None
            assert pinned[WEST_OF_UTC] == pinned[EAST_OF_UTC]

    @pytest.mark.asyncio
    async def test_absent_relation_is_none_and_structure_still_hashes(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            absent = await relation_schema_signature(connection, 999999999)
            assert absent is None
            oid = await probe_oid(connection)
            before = await relation_schema_signature(connection, oid)
            await connection.execute(
                text(
                    'ALTER TABLE transcode_sig_probe '
                    'ADD COLUMN note text'
                )
            )
            after = await relation_schema_signature(connection, oid)
            # The signature still detects genuine structural change.
            assert before != after
