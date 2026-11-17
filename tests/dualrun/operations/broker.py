"""The operation table of the broker switch.

One row per Python entry point of the broker module that calls the core.
Every operation of this switch runs in pool mode: the core runs its own
statements on its own pool. Run: ``pytest tests/integration --dual-run=broker``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

BROKER_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='database ping',
        production_path='horsies.core.brokers.postgres:PostgresBroker.ping_database_async',
        reference_path='tests.dualrun.reference.ping_caller:ping_database_async',
        mode=PoolMode(),
        core_methods=('ping_database_async',),
    ),
    DualRunOperation(
        name='schema install',
        production_path='horsies.core.brokers.postgres:PostgresBroker.ensure_schema_initialized',
        reference_path='tests.dualrun.reference.install_caller:ensure_schema_initialized',
        mode=PoolMode(),
        core_methods=('install_schema_async',),
    ),
    DualRunOperation(
        name='schema install before an operation',
        production_path='horsies.core.brokers.postgres:PostgresBroker._ensure_initialized',
        reference_path='tests.dualrun.reference.install_caller:ensure_initialized',
        mode=PoolMode(),
        core_methods=('install_schema_async',),
    ),
    DualRunOperation(
        name='task info',
        production_path='horsies.core.brokers.postgres:PostgresBroker.get_task_info_async',
        reference_path='tests.dualrun.reference.task_info_caller:get_task_info_async',
        mode=PoolMode(),
        core_methods=('task_info_async',),
    ),
    DualRunOperation(
        name='task info, blocking',
        production_path='horsies.core.brokers.postgres:PostgresBroker.get_task_info',
        reference_path='tests.dualrun.reference.task_info_caller:get_task_info',
        mode=PoolMode(),
        core_methods=('task_info',),
    ),
    DualRunOperation(
        name='result read and wait',
        production_path='horsies.core.brokers.postgres:PostgresBroker.get_raw_result_record_async',
        reference_path='tests.dualrun.reference.result_caller:get_raw_result_record_async',
        mode=PoolMode(),
        core_methods=('raw_result_record_async',),
    ),
    DualRunOperation(
        name='result read and wait, blocking',
        production_path='horsies.core.brokers.postgres:PostgresBroker.get_raw_result_record',
        reference_path='tests.dualrun.reference.result_caller:get_raw_result_record',
        mode=PoolMode(),
        core_methods=('raw_result_record',),
    ),
    DualRunOperation(
        name='send',
        production_path='horsies.core.brokers.postgres:PostgresBroker.enqueue_async',
        reference_path='tests.dualrun.reference.enqueue_caller:enqueue_async',
        mode=PoolMode(),
        core_methods=('enqueue_async',),
    ),
    DualRunOperation(
        name='send, blocking',
        production_path='horsies.core.brokers.postgres:PostgresBroker.enqueue',
        reference_path='tests.dualrun.reference.enqueue_caller:enqueue',
        mode=PoolMode(),
        core_methods=('enqueue',),
    ),
    DualRunOperation(
        name='claim pass',
        production_path='horsies.core.worker.claiming:ClaimMixin._claim_rows',
        reference_path='tests.dualrun.reference.claim_caller:claim_rows',
        mode=PoolMode(),
        core_methods=('claim_pass_async',),
    ),
    DualRunOperation(
        name='soft-cap start count',
        production_path='horsies.core.worker.claiming:ClaimMixin._count_queue_starts',
        reference_path='tests.dualrun.reference.claim_caller:count_queue_starts',
        mode=PoolMode(),
        core_methods=('count_queue_starts_async',),
    ),
)
