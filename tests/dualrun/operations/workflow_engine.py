"""The operation table of the workflow engine switch.

One row per Python entry point of the workflow engine that calls the core.
Every operation of this switch runs in pool mode: the core commits its own
statements on its own pool. Run:
``pytest tests/integration --dual-run=workflow-engine``.

The start of a run in a child and its stop before start call the core
handle of the child (``CoreChildHandle``), which each child process builds
once, not a ``CoreHandle``: their rows trip no core method.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import CoreOnlyTest, DualRunOperation, PoolMode

WORKFLOW_ENGINE_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='workflow start',
        production_path='horsies.core.workflows.lifecycle:start_workflow_async',
        reference_path='tests.dualrun.reference.workflow_start_caller:start_workflow_async',
        mode=PoolMode(),
        core_methods=('start_workflow_async',),
    ),
    DualRunOperation(
        name='workflow resume',
        production_path='horsies.core.workflows.lifecycle:resume_workflow',
        reference_path='tests.dualrun.reference.workflow_resume_caller:resume_workflow',
        mode=PoolMode(),
        core_methods=('resume_workflow_async',),
    ),
    DualRunOperation(
        name='workflow phase 2',
        production_path='horsies.core.worker.finalize:FinalizeMixin._finalize_workflow_phase',
        reference_path='tests.dualrun.reference.workflow_phase2_caller:finalize_workflow_phase',
        mode=PoolMode(),
        core_methods=('finish_workflow_node_async',),
    ),
    DualRunOperation(
        name='workflow parent advance',
        production_path='horsies.core.worker.finalize:FinalizeMixin._drive_parent_propagations',
        reference_path='tests.dualrun.reference.workflow_phase2_caller:drive_parent_propagations',
        mode=PoolMode(),
        core_methods=('advance_workflow_parents_async',),
    ),
    DualRunOperation(
        name='workflow phase-2 recovery',
        production_path='horsies.core.worker.reaper:ReaperMixin._recover_phase2',
        reference_path='tests.dualrun.reference.phase2_recovery_caller:recover_phase2',
        mode=PoolMode(),
        core_methods=('recover_workflow_phase2_async',),
    ),
    DualRunOperation(
        name='workflow recovery pass',
        production_path='horsies.core.worker.reaper:ReaperMixin._recover_workflows',
        reference_path='tests.dualrun.reference.recovery_pass_caller:recover_workflows',
        mode=PoolMode(),
        core_methods=('recover_workflows_async',),
    ),
    DualRunOperation(
        name='workflow claim-time settle',
        production_path='horsies.core.worker.claiming:ClaimMixin._filter_nonrunnable_workflow_tasks',
        reference_path='tests.dualrun.reference.claim_settle_caller:filter_nonrunnable_workflow_tasks',
        mode=PoolMode(),
        core_methods=('settle_claimed_workflow_tasks_async',),
    ),
    DualRunOperation(
        name='workflow task stop before start',
        production_path='horsies.core.worker.child_runner:_confirm_ownership_and_set_running',
        reference_path=(
            'tests.dualrun.reference.stop_before_start_caller:_confirm_ownership_and_set_running'
        ),
        mode=PoolMode(),
        core_methods=(),
    ),
    DualRunOperation(
        name='workflow task stop before start, shared async child',
        production_path='horsies.core.worker.shared_lifecycle:confirm_until_not_resumed',
        reference_path='tests.dualrun.reference.stop_before_start_caller:confirm_until_not_resumed',
        mode=PoolMode(),
        core_methods=(),
    ),
    DualRunOperation(
        name='workflow pause',
        production_path='horsies.core.workflows.lifecycle:pause_workflow',
        reference_path='tests.dualrun.reference.workflow_pause_caller:pause_workflow',
        mode=PoolMode(),
        core_methods=('pause_workflow_async',),
    ),
    DualRunOperation(
        name='workflow cancel',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.cancel_async',
        reference_path='tests.dualrun.reference.workflow_cancel_caller:cancel_async',
        mode=PoolMode(),
        core_methods=('cancel_workflow_async',),
    ),
    DualRunOperation(
        name='workflow retention delete',
        production_path='horsies.core.worker.reaper:ReaperMixin._delete_expired_workflows',
        reference_path='tests.dualrun.reference.workflow_retention_caller:delete_expired_workflows',
        mode=PoolMode(),
        core_methods=('delete_expired_workflows_async',),
    ),
    DualRunOperation(
        name='paused workflow expiry',
        production_path='horsies.core.worker.reaper:ReaperMixin._expire_paused_workflows',
        reference_path='tests.dualrun.reference.paused_expiry_caller:expire_paused_workflows',
        mode=PoolMode(),
        core_methods=('expire_paused_workflows_async',),
    ),
    DualRunOperation(
        name='workflow handle status',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.status_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:status_async',
        mode=PoolMode(),
        core_methods=('workflow_status_async',),
    ),
    DualRunOperation(
        name='workflow handle get',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.get_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:get_async',
        mode=PoolMode(),
        core_methods=('wait_workflow_result_async',),
    ),
    DualRunOperation(
        name='workflow handle result for',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.result_for_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:result_for_async',
        mode=PoolMode(),
        core_methods=('workflow_node_result_async',),
    ),
    DualRunOperation(
        name='workflow handle results',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.results_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:results_async',
        mode=PoolMode(),
        core_methods=('workflow_node_results_async',),
    ),
    DualRunOperation(
        name='workflow handle tasks',
        production_path='horsies.core.models.workflow.handle:WorkflowHandle.tasks_async',
        reference_path='tests.dualrun.reference.workflow_handle_reads:tasks_async',
        mode=PoolMode(),
        core_methods=('workflow_nodes_async',),
    ),
)


_CORE_STATEMENT_BOUND: Final = (
    'asserts the statement bound of the core start (shared_lifecycle.RUNNING_STATEMENTS_TIMEOUT_S, '
    'then the cancel request); the reference start sends no bound and waits for the row lock'
)
_CORE_STOP_STATEMENT: Final = (
    "waits for a backend that runs the core's horsies_workflow_task_stop_before_start; the "
    'reference stop before start runs horsies_abandon_owned_node in its own transaction'
)
_CORE_RESUME_FAILURE: Final = (
    'asserts the failure class that the core resume gives a database error '
    '(CoreServerError); the reference resume gives the psycopg error'
)

WORKFLOW_ENGINE_CORE_ONLY_TESTS: Final[tuple[CoreOnlyTest, ...]] = (
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_worker_finalize_two_phase.py::test_a_failed_phase2_keeps_the_outbox_row'
        ),
        reason='injects the phase-2 fault on the core path; the reference phase 2 does not take it',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_worker_finalize_two_phase.py::test_finalize_phase2_failure_keeps_terminal_task_result_durable'
        ),
        reason='injects the phase-2 fault on the core path; the reference phase 2 does not take it',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_worker_finalize_workflow_phase.py::test_a_failed_phase_two_is_a_phase2_error'
        ),
        reason='injects the phase-2 fault on the core path; the reference phase 2 does not take it',
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_pause_abandon_after_resume.py::test_child_starts_a_task_resumed_before_its_abandon'
        ),
        reason=(
            'commits the resume inside CoreChildHandle.stop_workflow_task_before_start; the '
            'reference stop before start does not call it, so no resume comes before its abandon'
        ),
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_shared_lifecycle_writes.py::test_a_running_update_that_waits_on_a_row_lock_ends_at_its_bound'
        ),
        reason=_CORE_STATEMENT_BOUND,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_thread_lifecycle_writes.py::test_a_refusal_whose_update_waits_on_a_row_lock_ends_at_its_bound'
        ),
        reason=_CORE_STATEMENT_BOUND,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_thread_lifecycle_writes.py::test_the_running_update_of_a_refusal_ends_at_the_bound_of_the_core'
        ),
        reason=_CORE_STATEMENT_BOUND,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_thread_lifecycle_writes.py::test_a_seat_that_gets_its_connection_after_the_deadline_leaves_the_row_claimed'
        ),
        reason=(
            'a start of another seat holds the one connection of the core pool of the child '
            '(child_core_handle_for); the reference start takes a connection of the sync '
            'pool of the child, so the seat does not wait for it'
        ),
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_task_lock_order.py::test_an_isolated_abandon_waits_for_the_workflow_without_task_locks'
        ),
        reason=_CORE_STOP_STATEMENT,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_task_lock_order.py::test_a_shared_async_abandon_waits_for_the_workflow_without_task_locks'
        ),
        reason=_CORE_STOP_STATEMENT,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_workflow_handle.py::TestResumeCommitSplit::test_recovery_db_error_returns_resume_recovery_failed'
        ),
        reason=_CORE_RESUME_FAILURE,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_workflow_handle.py::TestResumeCommitSplit::test_db_error_before_the_commit_returns_db_operation_failed'
        ),
        reason=_CORE_RESUME_FAILURE,
    ),
    CoreOnlyTest(
        node_id=(
            'tests/integration/test_workflow_handle.py::TestListenerFallbackContract::test_handle_get_async_propagates_programming_error'
        ),
        reason=(
            'injects the TypeError into CoreHandle.wait_workflow_result_async; the reference '
            'wait does not call it'
        ),
    ),
)
"""The tests of this module that run once, on the core (``CoreOnlyTest``)."""
