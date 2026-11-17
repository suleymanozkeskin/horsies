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
    DualRunOperation(
        name='monitoring cancel',
        production_path='horsies.monitoring.task_actions:cancel_task',
        reference_path='tests.dualrun.reference.cancel_caller:cancel_task',
        mode=PoolMode(),
        core_methods=('cancel_task_async',),
    ),
    DualRunOperation(
        name='stale RUNNING recovery',
        production_path='horsies.core.brokers.postgres:PostgresBroker.mark_stale_tasks_as_failed',
        reference_path='tests.dualrun.reference.stale_running_caller:mark_stale_tasks_as_failed',
        mode=PoolMode(),
        core_methods=('mark_stale_running_async',),
    ),
    DualRunOperation(
        name='failed attempt of a reported result',
        production_path='horsies.core.worker.finalize:FinalizeMixin._persist_failed_attempt',
        reference_path='tests.dualrun.reference.failed_attempt_caller:persist_failed_attempt',
        mode=PoolMode(),
        core_methods=('finish_failed_attempt_async',),
    ),
    DualRunOperation(
        name='end of an attempt without a result',
        production_path='horsies.core.worker.dispatch:DispatchMixin._write_ended_attempt',
        reference_path='tests.dualrun.reference.failed_attempt_caller:write_ended_attempt',
        mode=PoolMode(),
        core_methods=('finish_ended_attempt_async',),
    ),
)
