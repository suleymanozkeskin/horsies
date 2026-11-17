"""The operation table of the lifecycle switch.

One row per Python entry point that calls a lifecycle function of the core.
Every operation of this switch runs in pool mode: the core commits its own
statement on its own pool. Run: ``pytest tests/integration --dual-run=lifecycle``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

LIFECYCLE_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='stale CLAIMED requeue',
        production_path='horsies.core.brokers.postgres:PostgresBroker.requeue_stale_claimed',
        reference_path='tests.dualrun.reference.requeue_caller:requeue_stale_claimed',
        mode=PoolMode(),
        core_methods=('requeue_stale_claimed_async',),
    ),
    DualRunOperation(
        name='claim release, same class',
        production_path='horsies.core.worker.dispatch:DispatchMixin._requeue_claimed_task',
        reference_path='tests.dualrun.reference.release_caller:requeue_claimed_task',
        mode=PoolMode(),
        core_methods=('release_claims_async',),
    ),
    DualRunOperation(
        name='claim release as ISOLATED',
        production_path='horsies.core.worker.claiming:ClaimMixin._return_rows_as_isolated',
        reference_path='tests.dualrun.reference.release_caller:return_rows_as_isolated',
        mode=PoolMode(),
        core_methods=('release_claims_async',),
    ),
)
