"""The claim of a dispatch, for tests that call the parent-side worker paths directly.

``claim_at`` is the claim of a row with no retry facts. ``row_claim`` reads
the claim of a seeded row as ``horsies_claim`` returns it: its claim time and
its retry facts.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.worker.dispatch_claim import DispatchClaim


def claim_at(claimed_at: datetime) -> DispatchClaim:
    """The claim at ``claimed_at`` of a row with retry count 0, no retries and no options."""
    return DispatchClaim(
        claimed_at=claimed_at,
        retry_count=0,
        max_retries=0,
        task_options=None,
    )


async def row_claim(session: AsyncSession, task_id: str) -> DispatchClaim:
    """The claim of the live row ``task_id``: its claim time and retry facts.

    The row must hold a claim time; a row without one fails the test.
    """
    row = (
        await session.execute(
            text(
                'SELECT claimed_at, retry_count, max_retries, task_options '
                'FROM horsies_tasks WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).one()
    assert row.claimed_at is not None, f'task {task_id} holds no claim time'
    return DispatchClaim(
        claimed_at=row.claimed_at,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        task_options=row.task_options,
    )
