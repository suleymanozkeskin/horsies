"""Stage 3: drain verification — read-only, typed, abortable.

The operator stops the fleet; this stage proves nothing is in
flight. Every check is a read; a blocked verdict lists what still
moves, and aborting here restarts the old fleet against an unchanged
schema. PENDING rows are not violations — they wait, and the
relocation ignores them.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..terminalization.move import LIVE_TASKS


@dataclass(frozen=True, slots=True)
class DrainVerified:
    """Nothing in flight; the program may proceed."""

    pending_rows: int


@dataclass(frozen=True, slots=True)
class DrainBlocked:
    """The fleet is not drained; every count names its violation."""

    claimed_rows: int
    running_rows: int
    finalizing_rows: int
    recent_heartbeats: int


async def verify_drained(
    connection: AsyncConnection,
    *,
    heartbeat_quiet_seconds: float = 60.0,
) -> DrainVerified | DrainBlocked:
    counts = (
        await connection.execute(
            text(
                f"""
                SELECT
                    count(*) FILTER (WHERE status = 'CLAIMED')
                        AS claimed_rows,
                    count(*) FILTER (WHERE status = 'RUNNING')
                        AS running_rows,
                    count(*) FILTER (
                        WHERE status IN ('CLAIMED', 'RUNNING')
                          AND finalizing_at IS NOT NULL
                    ) AS finalizing_rows,
                    count(*) FILTER (WHERE status = 'PENDING')
                        AS pending_rows
                FROM {LIVE_TASKS}
                """
            )
        )
    ).one()
    recent_heartbeats = int(
        (
            await connection.execute(
                text(
                    'SELECT count(*) FROM horsies_heartbeats '
                    'WHERE sent_at > statement_timestamp() '
                    "- make_interval(secs => :quiet)"
                ),
                {'quiet': heartbeat_quiet_seconds},
            )
        ).scalar_one()
    )
    claimed = int(counts.claimed_rows)
    running = int(counts.running_rows)
    finalizing = int(counts.finalizing_rows)
    if claimed or running or finalizing or recent_heartbeats:
        return DrainBlocked(
            claimed_rows=claimed,
            running_rows=running,
            finalizing_rows=finalizing,
            recent_heartbeats=recent_heartbeats,
        )
    return DrainVerified(pending_rows=int(counts.pending_rows))
