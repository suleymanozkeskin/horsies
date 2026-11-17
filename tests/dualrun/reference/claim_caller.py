# pyright: reportPrivateUsage=false
"""The claim statement and the start count of the claim loop, before they
called the core.

``ClaimMixin._claim_rows`` and ``ClaimMixin._count_queue_starts``
(``python/horsies/core/worker/claiming.py``) sent ``HORSIES_CLAIM_SQL`` and
``COUNT_QUEUE_STARTS_SQL`` on the claim loop's held connection
(``_claim_runner``) and decoded their rows (``read_claim_rows``,
``read_queue_starts``). The statements, the decoders, ``_claim_params`` and
the held connection stay in the package. The functions below are the two
methods' bodies. Each takes the claim mixin as its first parameter, so the
dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from collections.abc import Sequence

from horsies.core.utils.db import SingleStatement, rows_or_raise
from horsies.core.worker.claiming import ClaimMixin, ClaimRows, read_claim_rows
from horsies.core.worker.queue_cap_hold import StartedClaim, read_queue_starts
from horsies.core.worker.sql import COUNT_QUEUE_STARTS_SQL, HORSIES_CLAIM_SQL


async def claim_rows(self: ClaimMixin) -> ClaimRows:
    """One claim pass: the rows it took, in dispatch order, and the rows
    its plan chose and its take did not get.

    The per-pass critical section (advisory-lock acquisition, cap
    counts, budget math, windowed claim) is one ``horsies_claim``
    statement in autocommit, as its own transaction, so the
    transaction-scoped advisory lock ends with the statement: it is
    never held across a client round trip, and a stalled worker cannot
    freeze the cluster mid-pass. It runs on the claim loop's held
    connection: one checkout for the passes that follow each other,
    given back after an idle period.

    Raises:
        Exception: DB errors propagate to the claim pass.
    """
    claim_result = rows_or_raise(
        await self._claim_runner.run(
            SingleStatement(
                statement=HORSIES_CLAIM_SQL,
                parameters=self._claim_params(),
            ),
        ),
    )
    return read_claim_rows(claim_result.columns, claim_result.rows)

async def count_queue_starts(
    self: ClaimMixin,
    queues: Sequence[str],
    started: Sequence[StartedClaim],
) -> dict[str, int]:
    """The count of each capped queue of one start check: its RUNNING
    rows plus this worker's started rows that are not RUNNING yet. One
    statement (``COUNT_QUEUE_STARTS_SQL``) on the claim loop's held
    connection; ``queues`` is not empty.

    Raises:
        Exception: DB errors propagate to the claim pass.
    """
    counted = rows_or_raise(
        await self._claim_runner.run(
            SingleStatement(
                statement=COUNT_QUEUE_STARTS_SQL,
                parameters={
                    'queues': list(queues),
                    'wid': self.worker_instance_id,
                    'started_ids': [claim.task_id for claim in started],
                },
            ),
        ),
    )
    return read_queue_starts(counted.columns, counted.rows)
