"""The operation table of the broker switch.

One row per Python entry point of the broker module that calls the core.
Every operation of this switch runs in pool mode: the core runs its own
statements on its own pool. Run: ``pytest tests/integration --dual-run=broker``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode, ReferenceOnlyTest

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

_SYNC_SEND_REASON: Final = (
    'the autouse fixture python_send puts the reference enqueue_caller.enqueue and '
    'enqueue_async in the place of the broker send methods, and the test observes that send'
)

BROKER_REFERENCE_ONLY_TESTS: Final[tuple[ReferenceOnlyTest, ...]] = (
    ReferenceOnlyTest(
        node_id=(
            'tests/integration/test_task_result_history_fallback.py::TestStagedResultReader::test_a_history_result_is_one_statement'
        ),
        reason=(
            "warms up through result_caller.get_raw_result_record_async and counts the statements "
            "of the reference's _probe_result_row on the SQLAlchemy engine"
        ),
    ),
    ReferenceOnlyTest(
        node_id=(
            'tests/integration/test_task_result_history_fallback.py::TestStagedResultReader::test_a_live_task_is_one_statement'
        ),
        reason=(
            "warms up through result_caller.get_raw_result_record_async and counts the statements "
            "of the reference's _probe_result_row on the SQLAlchemy engine"
        ),
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_a_producer_send_writes_on_the_calling_thread',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_a_keyed_send_writes_on_the_calling_thread',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_task_code_in_a_worker_child_stays_on_the_loop_runner',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_a_taken_task_id_is_answered_on_the_calling_thread',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_a_dead_connection_fails_its_send_as_retryable_then_the_next_send_reconnects',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_sends_from_many_threads_each_write_their_row',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id='tests/integration/test_sync_send_caller_thread.py::test_close_gives_the_sync_send_connections_back',
        reason=_SYNC_SEND_REASON,
    ),
    ReferenceOnlyTest(
        node_id=(
            'tests/integration/test_held_connection.py::test_the_claim_passes_of_a_worker_share_one_checkout'
        ),
        reason=(
            'puts the reference claim_caller.claim_rows in the place of ClaimMixin._claim_rows '
            "and counts the checkouts of the worker's SQLAlchemy pool, which the reference "
            'claim statement holds'
        ),
    ),
    ReferenceOnlyTest(
        node_id=(
            'tests/integration/test_claim_pass_counts.py::TestClaimPassCounts::test_empty_capped_pass_statement_count'
        ),
        reason=(
            'puts the reference claim_caller.claim_rows in the place of ClaimMixin._claim_rows '
            'and counts its statements on the SQLAlchemy engine'
        ),
    ),
    ReferenceOnlyTest(
        node_id=(
            'tests/integration/test_autocommit_outcomes.py::test_a_dead_connection_fails_the_claim_pass_as_retryable'
        ),
        reason=(
            'puts the reference claim_caller.claim_rows in the place of ClaimMixin._claim_rows '
            "and kills the backend of the worker's SQLAlchemy pool, which the reference claim "
            'statement uses'
        ),
    ),
)
"""The tests that run once, on the Python reference, in the dual-run of this module."""
