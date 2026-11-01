"""Type pin: the monitoring query and task action error variants.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a cause that
``raise ... from`` accepts without a match, a field on the wrong variant,
``NoCause()`` as a cause, an optional status, or a string compare on the
parsed history status fails the check.
"""

from __future__ import annotations

from horsies import NoCause, TaskStatus
from horsies.core.history.reads.detail import HistoryTaskDetail
from horsies.monitoring import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryError,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
    TaskActionDbOperationFailed,
    TaskActionError,
    TaskActionEventLoopLimit,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionStoredDataInvalid,
    TaskActionWorkflowTask,
)


def chain_the_query_database_cause(db_failed: MonitoringQueryDbOperationFailed) -> None:
    raise RuntimeError('query failed') from db_failed.exception


def chain_the_query_contract_cause(
    contract: MonitoringQueryHistoryContractViolation,
) -> None:
    raise RuntimeError('query failed') from contract.exception


def chain_a_query_cause_without_a_match(error: MonitoringQueryError) -> None:
    raise RuntimeError('query failed') from error.exception  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def query_fields_on_their_variants(
    db_failed: MonitoringQueryDbOperationFailed,
    loop_limit: MonitoringQueryEventLoopLimit,
) -> None:
    _ = loop_limit.loop_limit
    _ = loop_limit.exception  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    _ = db_failed.loop_limit  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]


def query_causes_are_exceptions() -> None:
    _ = MonitoringQueryDbOperationFailed(
        message='failed',
        retryable=True,
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = MonitoringQueryHistoryContractViolation(
        message='failed',
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_task_action_database_cause(db_failed: TaskActionDbOperationFailed) -> None:
    raise RuntimeError('cancel failed') from db_failed.exception


def chain_the_task_action_stored_data_cause(stored: TaskActionStoredDataInvalid) -> None:
    raise RuntimeError('cancel failed') from stored.exception


def chain_a_task_action_cause_without_a_match(error: TaskActionError) -> None:
    raise RuntimeError('cancel failed') from error.exception  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def task_action_fields_on_their_variants(
    not_found: TaskActionNotFound,
    not_cancellable: TaskActionNotCancellable,
    workflow_task: TaskActionWorkflowTask,
    loop_limit: TaskActionEventLoopLimit,
) -> None:
    _ = not_cancellable.current_status
    _ = workflow_task.current_status
    _ = loop_limit.loop_limit
    _ = not_found.current_status  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    _ = not_cancellable.loop_limit  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]


def the_status_is_total() -> None:
    _ = TaskActionNotCancellable(
        message='refused',
        task_id='id',
        current_status=TaskStatus.COMPLETED,
    )
    _ = TaskActionNotCancellable(
        message='refused',
        task_id='id',
        current_status=None,  # pyright: ignore[reportArgumentType]
    )
    _ = TaskActionWorkflowTask(
        message='refused',
        task_id='id',
        current_status=None,  # pyright: ignore[reportArgumentType]
    )


def task_action_causes_are_exceptions() -> None:
    _ = TaskActionDbOperationFailed(
        message='failed',
        retryable=True,
        task_id='id',
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = TaskActionStoredDataInvalid(
        message='failed',
        task_id='id',
        exception=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def the_history_status_is_parsed(detail: HistoryTaskDetail) -> None:
    _ = detail.status is TaskStatus.COMPLETED
    _ = detail.status == 'COMPLETED'  # pyright: ignore[reportUnnecessaryComparison]
