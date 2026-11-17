"""The operation table of the lifecycle switch.

One row per Python entry point that calls a lifecycle function of the core.
Every operation of this switch runs in pool mode: the core commits its own
statement on its own pool. Run: ``pytest tests/integration --dual-run=lifecycle``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import CoreOnlyTest, DualRunOperation, PoolMode

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


LIFECYCLE_CORE_ONLY_TESTS: Final[tuple[CoreOnlyTest, ...]] = (
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_autocommit_outcomes.py::test_a_dead_connection_of_the_engine_does_not_reach_the_core_completion'
        ),
        reason="a dead connection of the engine does not reach the core's completion; the reference completes on that engine",
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_autocommit_outcomes.py::test_a_lost_reply_is_retried_and_classified_on_the_replay'
        ),
        reason="drops the reply of the core's batch statement and reads its replay",
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_bad_row_rolls_back_its_batch_and_each_row_is_written_alone_once'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_batch_applies_every_row_and_answers_each_finalizer'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_batch_writes_each_result_text_as_given'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_claim_generation_keeps_its_microseconds_and_its_offset'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_lone_row_runs_the_batch_function_with_one_row'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_lost_reply_of_a_batch_is_classified_by_the_replay_of_each_row'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_a_replay_of_a_whole_batch_is_already_applied_and_writes_nothing'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_each_row_keeps_its_own_fence_in_one_batch'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_the_row_is_written_only_after_its_attempt_ended'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_finalize_batch.py::test_two_flushers_and_workflow_node_finalizes_do_not_deadlock'
        ),
        reason='calls complete_fused_batch_in_core or reads the core batch statement; the reference writes through horsies_complete_task_fused',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_monitoring_task_actions.py::TestDatabaseFailure::test_cancel_reports_a_retryable_db_failure'
        ),
        reason='expects the core exception class CoreConnectionError in the error',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_worker_retry_scheduling.py::test_good_until_is_judged_under_the_lock'
        ),
        reason='patches finish_failed_attempt_in_core, the core path of the failed attempt',
    ),
)
"""The tests of this module that run once, on the core (``CoreOnlyTest``)."""
