"""Public catalog paths re-export the classes generated from horsies-core.

`horsies-codegen` renders the code enums of horsies-core into
`clients/python/horsies/_catalog/`. A public module re-exports a generated class only when
the core catalog holds exactly the members of the Python catalog it
replaces. This test pins both facts for each re-exported class: the public
name is the generated class itself, and its members and values are the
ones the public API had before the move. For the lifecycle statuses it
also pins the terminal-state constants and `is_terminal`.
"""

from __future__ import annotations

import enum
from typing import Final

import pytest

import horsies._catalog.broker
import horsies._catalog.history
import horsies._catalog.monitoring
import horsies._catalog.status
import horsies._catalog.task_codes
import horsies._catalog.task_send
import horsies._catalog.workflow_handle
import horsies._catalog.workflow_start
from horsies.core.brokers import result_types
from horsies.core.models import task_send_types, tasks
from horsies.core.models.workflow import enums, handle_types
from horsies.core.types import status
from horsies.core.workflows import start_types
from horsies.monitoring import errors, task_actions

pytestmark = [pytest.mark.unit]

# (public class, generated class, member names in order). Every member's
# value equals its name.
REEXPORTS: Final[tuple[tuple[type[enum.Enum], type[enum.Enum], tuple[str, ...]], ...]] = (
    (
        result_types.BrokerErrorCode,
        horsies._catalog.broker.BrokerErrorCode,
        (
            'SCHEMA_INIT_FAILED',
            'ENQUEUE_FAILED',
            'PAYLOAD_MISMATCH',
            'IDEMPOTENCY_KEY_CONFLICT',
            'TASK_INFO_QUERY_FAILED',
            'MONITORING_QUERY_FAILED',
            'DB_PING_FAILED',
            'WORKER_PING_FAILED',
            'CLEANUP_FAILED',
            'CLOSE_FAILED',
            'LISTENER_START_FAILED',
            'LISTENER_SUBSCRIBE_FAILED',
            'NO_BROKER',
            'NO_TYPE_AVAILABLE',
            'INVALID_JSON_PAYLOAD',
            'INVALID_TASK_OPTIONS',
            'ASYNC_CONTEXT',
            'INVALID_INPUT',
            'LISTENER_DISABLED',
            'WAIT_INSIDE_TASK',
        ),
    ),
    (
        task_send_types.TaskSendErrorCode,
        horsies._catalog.task_send.TaskSendErrorCode,
        (
            'SEND_SUPPRESSED',
            'ASYNC_CONTEXT',
            'VALIDATION_FAILED',
            'ENQUEUE_FAILED',
            'PAYLOAD_MISMATCH',
            'IDEMPOTENCY_KEY_CONFLICT',
            'PAYLOAD_TOO_LARGE',
        ),
    ),
    (
        start_types.WorkflowStartErrorCode,
        horsies._catalog.workflow_start.WorkflowStartErrorCode,
        (
            'BROKER_NOT_CONFIGURED',
            'VALIDATION_FAILED',
            'WORKFLOW_ID_CONFLICT',
            'WORKFLOW_COMMAND_CONFLICT',
            'ENQUEUE_FAILED',
            'ASYNC_CONTEXT',
        ),
    ),
    (
        handle_types.HandleErrorCode,
        horsies._catalog.workflow_handle.HandleErrorCode,
        (
            'WORKFLOW_NOT_FOUND',
            'DB_OPERATION_FAILED',
            'STORED_DATA_INVALID',
            'RESUME_RECOVERY_FAILED',
            'ASYNC_CONTEXT',
        ),
    ),
    (
        errors.MonitoringQueryErrorCode,
        horsies._catalog.monitoring.MonitoringQueryErrorCode,
        ('DB_OPERATION_FAILED', 'HISTORY_CONTRACT_VIOLATION'),
    ),
    (
        task_actions.TaskActionErrorCode,
        horsies._catalog.monitoring.TaskActionErrorCode,
        (
            'TASK_NOT_FOUND',
            'TASK_NOT_CANCELLABLE',
            'TASK_IS_WORKFLOW_TASK',
            'DB_OPERATION_FAILED',
            'STORED_DATA_INVALID',
        ),
    ),
    (
        tasks.OperationalErrorCode,
        horsies._catalog.task_codes.OperationalErrorCode,
        (
            'UNHANDLED_EXCEPTION',
            'TASK_EXCEPTION',
            'WORKER_CRASHED',
            'BROKER_ERROR',
            'WORKER_RESOLUTION_ERROR',
            'WORKER_SERIALIZATION_ERROR',
            'RESULT_DESERIALIZATION_ERROR',
            'WORKFLOW_ENQUEUE_FAILED',
            'SUBWORKFLOW_LOAD_FAILED',
            'WORKFLOW_RECOVERY_LIMIT_REACHED',
            'SUBWORKFLOW_DEPTH_LIMIT_REACHED',
            'TASK_BLOCKED_LOOP',
            'NEIGHBOUR_BLOCKED_LOOP',
            'WORKFLOW_START_INCOMPLETE',
        ),
    ),
    (
        tasks.ContractCode,
        horsies._catalog.task_codes.ContractCode,
        ('RETURN_TYPE_MISMATCH', 'WORKFLOW_CTX_MISSING_ID', 'NO_TYPE_AVAILABLE'),
    ),
    (
        tasks.RetrievalCode,
        horsies._catalog.task_codes.RetrievalCode,
        (
            'WAIT_TIMEOUT',
            'TASK_NOT_FOUND',
            'WORKFLOW_NOT_FOUND',
            'RESULT_NOT_AVAILABLE',
            'RESULT_NOT_READY',
            'ASYNC_CONTEXT',
            'INVALID_INPUT',
            'WAIT_INSIDE_TASK',
        ),
    ),
    (
        tasks.OutcomeCode,
        horsies._catalog.task_codes.OutcomeCode,
        (
            'TASK_CANCELLED',
            'TASK_EXPIRED',
            'TASK_TIMEOUT',
            'WORKFLOW_PAUSED',
            'WORKFLOW_EXPIRED',
            'WORKFLOW_FAILED',
            'WORKFLOW_CANCELLED',
            'UPSTREAM_SKIPPED',
            'SUBWORKFLOW_FAILED',
            'WORKFLOW_SUCCESS_CASE_NOT_MET',
            'WORKFLOW_OUTPUT_SKIPPED',
        ),
    ),
    (
        status.TaskStatus,
        horsies._catalog.status.TaskStatus,
        ('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'),
    ),
    (
        status.TaskAttemptOutcome,
        horsies._catalog.status.TaskAttemptOutcome,
        ('COMPLETED', 'FAILED', 'WORKER_FAILURE', 'CANCELLED'),
    ),
    (
        enums.WorkflowStatus,
        horsies._catalog.status.WorkflowStatus,
        ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PAUSED', 'CANCELLED', 'EXPIRED'),
    ),
    (
        enums.WorkflowNodeStatus,
        horsies._catalog.status.WorkflowNodeStatus,
        ('PENDING', 'READY', 'ENQUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED'),
    ),
    (
        horsies.TaskAbsenceReason,
        horsies._catalog.history.TaskAbsenceReason,
        ('PREDATES_RETAINED_HISTORY', 'WITHIN_RETAINED_HISTORY', 'NOT_CLASSIFIED'),
    ),
)


@pytest.mark.parametrize(
    ('public', 'generated', 'members'),
    REEXPORTS,
    ids=[public.__qualname__ for public, _generated, _members in REEXPORTS],
)
def test_public_path_is_the_generated_class_with_the_same_members(
    public: type[enum.Enum],
    generated: type[enum.Enum],
    members: tuple[str, ...],
) -> None:
    assert public is generated
    assert tuple((member.name, member.value) for member in public) == tuple(
        (name, name) for name in members
    )


# The one re-exported class whose public base is `Enum` alone; every other
# one is `str, Enum`.
PLAIN_ENUM_CLASSES: Final[frozenset[type[enum.Enum]]] = frozenset({status.TaskStatus})


def test_every_re_exported_class_keeps_its_public_base() -> None:
    for public, _generated, _members in REEXPORTS:
        expected = (
            (enum.Enum,) if public in PLAIN_ENUM_CLASSES else (str, enum.Enum)
        )
        assert public.__bases__ == expected, public.__qualname__


# (public constant, generated constant, status class, terminal member names).
TERMINAL_STATES: Final[
    tuple[tuple[frozenset[enum.Enum], frozenset[enum.Enum], type[enum.Enum], frozenset[str]], ...]
] = (
    (
        status.TASK_TERMINAL_STATES,
        horsies._catalog.status.TASK_TERMINAL_STATES,
        status.TaskStatus,
        frozenset({'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'}),
    ),
    (
        enums.WORKFLOW_TERMINAL_STATES,
        horsies._catalog.status.WORKFLOW_TERMINAL_STATES,
        enums.WorkflowStatus,
        frozenset({'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'}),
    ),
    (
        enums.WORKFLOW_NODE_TERMINAL_STATES,
        horsies._catalog.status.WORKFLOW_NODE_TERMINAL_STATES,
        enums.WorkflowNodeStatus,
        frozenset({'COMPLETED', 'FAILED', 'SKIPPED'}),
    ),
)


@pytest.mark.parametrize(
    ('public', 'generated', 'status_class', 'terminal_names'),
    TERMINAL_STATES,
    ids=[status_class.__qualname__ for _p, _g, status_class, _n in TERMINAL_STATES],
)
def test_public_terminal_states_are_the_generated_constant(
    public: frozenset[enum.Enum],
    generated: frozenset[enum.Enum],
    status_class: type[enum.Enum],
    terminal_names: frozenset[str],
) -> None:
    assert public is generated
    assert type(public) is frozenset
    assert public == frozenset(status_class[name] for name in terminal_names)


@pytest.mark.parametrize(
    ('public', 'status_class'),
    [(public, status_class) for public, _g, status_class, _n in TERMINAL_STATES],
    ids=[status_class.__qualname__ for _p, _g, status_class, _n in TERMINAL_STATES],
)
def test_is_terminal_holds_exactly_for_the_terminal_members(
    public: frozenset[enum.Enum],
    status_class: type[enum.Enum],
) -> None:
    for member in status_class:
        assert getattr(member, 'is_terminal') is (member in public), member


def test_the_built_in_registry_holds_every_generated_member() -> None:
    # `BUILTIN_CODE_REGISTRY` is built from the four re-exported families;
    # `WORKFLOW_START_INCOMPLETE` is a built-in name, so a user code cannot
    # take it.
    families = (
        tasks.OperationalErrorCode,
        tasks.ContractCode,
        tasks.RetrievalCode,
        tasks.OutcomeCode,
    )
    expected = {member.value: member for family in families for member in family}
    assert tasks.BUILTIN_CODE_REGISTRY == expected
    assert (
        tasks.BUILTIN_CODE_REGISTRY['WORKFLOW_START_INCOMPLETE']
        is tasks.OperationalErrorCode.WORKFLOW_START_INCOMPLETE
    )
    with pytest.raises(ValueError, match='reserved built-in error code'):
        tasks.TaskError(error_code='WORKFLOW_START_INCOMPLETE')
