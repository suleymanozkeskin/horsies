"""Key-reservation operations against real PostgreSQL.

Exercises the ratified reservation contract through the installed
functions: claim/replay/conflict under one key, terminalization starting
the snapshotted window at terminal time with ownership verification,
expired-terminal reuse, and bounded cleanup that never touches live
reservations. Window expiry is produced by terminalizing with a past
terminal time — the functions take time as data, so no test sleeps.
"""

from __future__ import annotations

from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.identity.reservations import (
    ReservationApplied,
    ReservationConflict,
    ReservationReplay,
    claim_key_reservation,
    cleanup_expired_reservations,
    terminalize_key_reservation,
)

from tests.integration.task_history_harness import (
    HistorySchema,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration]

history_schema = task_history_schema_fixture('task_history_it_reservations')

KEY_DIGEST = sha256(b'scoped-key').digest()
FINGERPRINT = sha256(b'command').digest()
OTHER_FINGERPRINT = sha256(b'different-command').digest()
HOUR_SECONDS = 3_600


async def claim(
    connection: AsyncConnection,
    *,
    task_id: str,
    fingerprint: bytes = FINGERPRINT,
) -> object:
    return await claim_key_reservation(
        connection,
        key_digest=KEY_DIGEST,
        key_scope_version=1,
        reservation_window_seconds=HOUR_SECONDS,
        fingerprint_version=1,
        fingerprint=fingerprint,
        task_id=task_id,
    )


class TestClaimReplayConflict:
    @pytest.mark.asyncio
    async def test_first_claim_applies(
        self, history_schema: HistorySchema
    ) -> None:
        task_id = str(uuid4())
        async with history_schema.engine.begin() as connection:
            assert await claim(connection, task_id=task_id) == (
                ReservationApplied(task_id=task_id)
            )

    @pytest.mark.asyncio
    async def test_same_fingerprint_replays_the_committed_task(
        self, history_schema: HistorySchema
    ) -> None:
        original = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await claim(connection, task_id=original)
            outcome = await claim(connection, task_id=str(uuid4()))
            assert outcome == ReservationReplay(task_id=original)

    @pytest.mark.asyncio
    async def test_different_fingerprint_is_a_typed_conflict(
        self, history_schema: HistorySchema
    ) -> None:
        original = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await claim(connection, task_id=original)
            outcome = await claim(
                connection, task_id=str(uuid4()), fingerprint=OTHER_FINGERPRINT
            )
            assert outcome == ReservationConflict(
                task_id=original, observed_fingerprint_version=1
            )

    @pytest.mark.asyncio
    async def test_window_above_maximum_is_rejected_before_mutation(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.connect() as connection:
            with pytest.raises(DBAPIError, match='30 days'):
                await claim_key_reservation(
                    connection,
                    key_digest=KEY_DIGEST,
                    key_scope_version=1,
                    reservation_window_seconds=31 * 86_400,
                    fingerprint_version=1,
                    fingerprint=FINGERPRINT,
                    task_id=str(uuid4()),
                )


class TestTerminalizationAndReuse:
    @pytest.mark.asyncio
    async def test_terminalize_verifies_ownership(
        self, history_schema: HistorySchema
    ) -> None:
        owner = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await claim(connection, task_id=owner)
            assert not await terminalize_key_reservation(
                connection, key_digest=KEY_DIGEST, task_id=str(uuid4())
            )
            assert await terminalize_key_reservation(
                connection, key_digest=KEY_DIGEST, task_id=owner
            )
            assert not await terminalize_key_reservation(
                connection, key_digest=KEY_DIGEST, task_id=owner
            )

    @pytest.mark.asyncio
    async def test_unexpired_terminal_reservation_still_replays(
        self, history_schema: HistorySchema
    ) -> None:
        original = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await claim(connection, task_id=original)
            await terminalize_key_reservation(
                connection, key_digest=KEY_DIGEST, task_id=original
            )
            outcome = await claim(connection, task_id=str(uuid4()))
            assert outcome == ReservationReplay(task_id=original)

    @pytest.mark.asyncio
    async def test_expired_terminal_reservation_is_reused(
        self, history_schema: HistorySchema
    ) -> None:
        original = str(uuid4())
        replacement = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await claim(connection, task_id=original)
            await connection.execute(
                text(
                    'SELECT horsies_key_reservation_terminalize('
                    ':digest, CAST(:task_id AS uuid), '
                    "statement_timestamp() - interval '2 hours')"
                ),
                {'digest': KEY_DIGEST, 'task_id': original},
            )
            outcome = await claim(
                connection,
                task_id=replacement,
                fingerprint=OTHER_FINGERPRINT,
            )
            assert outcome == ReservationApplied(task_id=replacement)


class TestCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_deletes_only_expired_terminal_rows(
        self, history_schema: HistorySchema
    ) -> None:
        expired = str(uuid4())
        live = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await claim(connection, task_id=expired)
            await connection.execute(
                text(
                    'SELECT horsies_key_reservation_terminalize('
                    ':digest, CAST(:task_id AS uuid), '
                    "statement_timestamp() - interval '2 hours')"
                ),
                {'digest': KEY_DIGEST, 'task_id': expired},
            )
            await claim_key_reservation(
                connection,
                key_digest=sha256(b'live-key').digest(),
                key_scope_version=1,
                reservation_window_seconds=HOUR_SECONDS,
                fingerprint_version=1,
                fingerprint=FINGERPRINT,
                task_id=live,
            )
            assert (
                await cleanup_expired_reservations(connection, batch_size=10)
                == 1
            )
            assert (
                await cleanup_expired_reservations(connection, batch_size=10)
                == 0
            )
            remaining = (
                await connection.execute(
                    text(
                        'SELECT task_id FROM horsies_key_reservations '
                        "WHERE disposition = 'LIVE'"
                    )
                )
            ).scalar_one()
            assert str(remaining) == live
