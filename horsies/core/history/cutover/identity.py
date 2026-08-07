"""Attempt-identity normalization: the relocation's type prerequisite.

Production `horsies_task_attempts.task_id` is varchar — the fixture
stand-ins modeled it as uuid, which masked the type until the
relocation ran against the real schema. The attempt encoder takes a
uuid parameter and compares it against this column, so the column
must be uuid before the relocation (which calls the encoder per row)
can run. Reversible: `ALTER ... USING task_id::uuid` loses nothing
and the inverse cast restores the varchar shape exactly.

The foreign key to the live table cannot survive the conversion (a
key across differing types is unrepresentable) and is NOT re-created
here: the live table's own identity conversion happens at the
tighten stage — after relocation has shrunk the table — and the key
is restored there with both sides uuid. Between the two steps the
program is offline and nothing writes attempts.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..terminalization.move import LIVE_ATTEMPTS


async def normalize_attempt_identity(connection: AsyncConnection) -> None:
    """Convert the attempts identity column to uuid; idempotent."""
    already_uuid = (
        await connection.execute(
            text(
                """
                SELECT atttypid = 'uuid'::regtype
                FROM pg_attribute
                WHERE attrelid = CAST(:relation AS regclass)
                  AND attname = 'task_id'
                """
            ),
            {'relation': LIVE_ATTEMPTS},
        )
    ).scalar_one()
    if bool(already_uuid):
        return
    constraint = (
        await connection.execute(
            text(
                """
                SELECT conname FROM pg_constraint
                WHERE conrelid = CAST(:relation AS regclass)
                  AND contype = 'f'
                """
            ),
            {'relation': LIVE_ATTEMPTS},
        )
    ).scalar_one_or_none()
    if constraint is not None:
        await connection.execute(
            text(
                f'ALTER TABLE {LIVE_ATTEMPTS} '
                f'DROP CONSTRAINT "{constraint}"'
            )
        )
    await connection.execute(
        text(
            f'ALTER TABLE {LIVE_ATTEMPTS} '
            'ALTER COLUMN task_id TYPE uuid USING task_id::uuid'
        )
    )
