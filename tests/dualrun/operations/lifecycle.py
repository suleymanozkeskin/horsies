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
    DualRunOperation(
        name='workflow task success',
        production_path='horsies.core.worker.finalize:FinalizeMixin._finish_workflow_success',
        reference_path='tests.dualrun.reference.workflow_success_caller:finish_workflow_success',
        mode=PoolMode(),
        core_methods=('finish_workflow_success_async',),
    ),
    DualRunOperation(
        name='plain-success batch',
        production_path='horsies.core.worker.finalize:FinalizeMixin._write_fused_batch',
        reference_path='tests.dualrun.reference.success_batch_caller:write_fused_batch',
        mode=PoolMode(),
        core_methods=('complete_successes_async',),
    ),
    DualRunOperation(
        name='pending expiry',
        production_path='horsies.core.brokers.postgres:PostgresBroker.expire_pending_tasks',
        reference_path='tests.dualrun.reference.expiry_and_orphans_caller:expire_pending_tasks',
        mode=PoolMode(),
        core_methods=('expire_pending_tasks_async',),
    ),
    DualRunOperation(
        name='orphan audit page',
        production_path=(
            'horsies.core.brokers.postgres:PostgresBroker.audit_orphaned_workflow_tasks'
        ),
        reference_path=(
            'tests.dualrun.reference.expiry_and_orphans_caller:audit_orphaned_workflow_tasks'
        ),
        mode=PoolMode(),
        core_methods=('audit_orphaned_workflow_tasks_async',),
    ),
)
