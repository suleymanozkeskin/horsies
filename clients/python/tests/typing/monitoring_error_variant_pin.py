"""Type pin: the monitoring query and task action error variants.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing calls its functions. Each
line that pyright must refuse carries an ignore comment for its rule. The
config makes an unneeded ignore comment an error, so a cause that
``raise ... from`` accepts without a match, a field on the wrong variant,
``NoCause()`` as a cause, an optional status, or a string compare on the
parsed status of a task read (``TaskInfo.status``, live or from history)
fails the check.
"""

from __future__ import annotations

from uuid import UUID

from horsies import NoCause, TaskInfo, TaskStatus
from horsies.monitoring import (
    MonitoringQueryDatabaseOperationFailed,
    MonitoringQueryError,
    MonitoringQueryHistoryContractViolation,
    TaskActionDatabaseOperationFailed,
    TaskActionError,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionOutcomeUnknown,
    TaskActionStoredDataInvalid,
    TaskActionIsWorkflowTask,
)

TASK_ID = UUID('0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718')


def chain_the_query_database_cause(db_failed: MonitoringQueryDatabaseOperationFailed) -> None:
    raise RuntimeError('query failed') from db_failed.cause


def chain_the_query_contract_cause(
    contract: MonitoringQueryHistoryContractViolation,
) -> None:
    raise RuntimeError('query failed') from contract.cause


def every_query_variant_has_a_cause(error: MonitoringQueryError) -> None:
    raise RuntimeError('query failed') from error.cause


def query_causes_are_exceptions() -> None:
    _ = MonitoringQueryDatabaseOperationFailed(
        message='failed',
        retryable=True,
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = MonitoringQueryHistoryContractViolation(
        message='failed',
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def chain_the_task_action_database_cause(db_failed: TaskActionDatabaseOperationFailed) -> None:
    raise RuntimeError('cancel failed') from db_failed.cause


def chain_the_task_action_unknown_outcome_cause(unknown: TaskActionOutcomeUnknown) -> None:
    raise RuntimeError('cancel outcome unknown') from unknown.cause


def chain_the_task_action_stored_data_cause(stored: TaskActionStoredDataInvalid) -> None:
    raise RuntimeError('cancel failed') from stored.cause


def chain_a_task_action_cause_without_a_match(error: TaskActionError) -> None:
    raise RuntimeError('cancel failed') from error.exception  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]


def task_action_fields_on_their_variants(
    not_found: TaskActionNotFound,
    not_cancellable: TaskActionNotCancellable,
    workflow_task: TaskActionIsWorkflowTask,
) -> None:
    _ = not_cancellable.current_status
    _ = workflow_task.current_status
    _ = not_found.current_status  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]


def the_status_is_total() -> None:
    _ = TaskActionNotCancellable(
        message='refused',
        task_id=TASK_ID,
        current_status=TaskStatus.COMPLETED,
    )
    _ = TaskActionNotCancellable(
        message='refused',
        task_id=TASK_ID,
        current_status=None,  # pyright: ignore[reportArgumentType]
    )
    _ = TaskActionIsWorkflowTask(
        message='refused',
        task_id=TASK_ID,
        current_status=None,  # pyright: ignore[reportArgumentType]
    )


def task_action_causes_are_exceptions() -> None:
    _ = TaskActionDatabaseOperationFailed(
        message='failed',
        retryable=True,
        task_id=TASK_ID,
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = TaskActionOutcomeUnknown(
        message='failed',
        retryable=True,
        task_id=TASK_ID,
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )
    _ = TaskActionStoredDataInvalid(
        message='failed',
        task_id=TASK_ID,
        cause=NoCause(),  # pyright: ignore[reportArgumentType]
    )


def the_history_status_is_parsed(info: TaskInfo[object]) -> None:
    _ = info.status is TaskStatus.COMPLETED
    _ = info.status == 'COMPLETED'  # pyright: ignore[reportUnnecessaryComparison]
