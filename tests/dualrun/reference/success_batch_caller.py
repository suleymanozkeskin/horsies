"""The worker's plain-success batch write, before it called the core.

``FinalizeMixin._write_fused_batch`` (``python/horsies/core/worker/finalize.py``)
ran ``apply_fused_batch`` (``python/horsies/core/lifecycle/persistence.py``) on
the runner that the finalize batcher gave it: the held connection of a steady
flusher, or a checkout per statement for a surge flusher. Both run the same
statement in autocommit. ``write_fused_batch`` below runs it on a checkout per
statement of the worker's session factory. It takes the worker as its first
parameter, so the dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from horsies.core.lifecycle.outcomes import TerminalizationOutcome
from horsies.core.lifecycle.persistence import (
    BatchWriteError,
    FusedCompletionBatch,
    apply_fused_batch,
)
from horsies.core.types.result import Result
from horsies.core.utils.db import PooledStatements
from horsies.core.worker.finalize import FinalizeMixin


async def write_fused_batch(
    self: FinalizeMixin,
    batch: FusedCompletionBatch,
) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
    """One batch statement in autocommit on the worker's session factory."""
    return await apply_fused_batch(PooledStatements(self.sf), batch)
