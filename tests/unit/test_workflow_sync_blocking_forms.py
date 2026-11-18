# pyright: reportPrivateUsage=false
"""The sync workflow facades call the core's blocking forms.

``WorkflowHandle``'s sync methods, ``start_workflow``,
``pause_workflow_sync`` and ``resume_workflow_sync`` share one outcome
mapping with their async forms. For each catalog outcome of the core's
record, and for each refusal before the core call, the sync facade returns
the outcome of its async form (``outcome_shape``), calls the blocking form
of the core handle once (or not at all for a refusal), and never calls the
broker's sync runner.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.codec.json_io import SerializationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import NodeKey, TaskNode, WorkflowSpec
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import is_ok
from horsies.core.workflows.lifecycle import (
    pause_workflow,
    pause_workflow_sync,
    resume_workflow,
    resume_workflow_sync,
    start_workflow,
    start_workflow_async,
)
from tests.sync_facade_support import outcome_shape

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL: Final = 'postgresql://u:p@127.0.0.1:1/db'
WORKFLOW_ID: Final = '5d0e2f4a-6b8c-4d1e-9f3a-5b7d9f1a3c5e'
WORKFLOW_KEY: Final = uuid.UUID(WORKFLOW_ID)
NOT_A_UUID: Final = 'not-a-uuid'
NODE_ID: Final = 'blocking_forms:0'
NODE_KEY: Final[NodeKey[int]] = NodeKey(NODE_ID)
TASK_NAME: Final = 'blocking_forms.value'
SENT_AT: Final = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
CONNECTION_LOST: Final = core.CoreConnectionError('workflow', 'connection lost', True)
CORE_REFUSAL: Final = core.TooManyPendingCalls('CoreHandle.call', 65_536)

APP: Final = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
    ),
)


@APP.task(task_name=TASK_NAME)
def value_task(*, value: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value)


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = WORKFLOW_KEY
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _handle_failed(code: str) -> MagicMock:
    return _record(
        core.HandleFailed,
        code=code,
        message=f'{code}: connection lost',
        retryable=code == 'DB_OPERATION_FAILED',
        exception=CONNECTION_LOST,
    )


def _ok_envelope(value: int) -> str:
    return f'{{"__h_task_result__": true, "ok": {value}, "err": null}}'


@dataclass(frozen=True)
class CoreAnswers:
    """A core handle whose blocking form ``method`` and its awaitable form
    answer ``answer``: a record, or an exception that the call raises; and
    the broker's sync runner, a mock."""

    handle: MagicMock
    runner: MagicMock


def _answering(method: str, answer: object) -> CoreAnswers:
    handle = MagicMock()
    match answer:
        case BaseException():
            setattr(handle, method, MagicMock(side_effect=answer))
            setattr(handle, f'{method}_async', AsyncMock(side_effect=answer))
        case _:
            setattr(handle, method, MagicMock(return_value=answer))
            setattr(handle, f'{method}_async', AsyncMock(return_value=answer))
    broker = APP.get_broker()
    runner = MagicMock()
    broker._core_handle = handle  # type: ignore[assignment]
    broker._loop_runner = runner
    return CoreAnswers(handle=handle, runner=runner)


def _workflow_handle(workflow_id: str = WORKFLOW_ID) -> WorkflowHandle[int]:
    return WorkflowHandle(workflow_id=workflow_id, broker=APP.get_broker(), out_type=int)


async def _awaited(awaitable: Awaitable[object]) -> object:
    return await awaitable


@dataclass(frozen=True)
class Facade:
    """A sync workflow facade, its async form, the blocking form of the core
    handle that it calls with its arguments, and each record of its
    catalog."""

    blocking: Callable[[WorkflowHandle[int]], object]
    awaited: Callable[[WorkflowHandle[int]], Awaitable[object]]
    blocking_form: str
    arguments: tuple[object, ...]
    keywords: dict[str, object]
    answers: dict[str, object]


HANDLE_FAILURES: Final[dict[str, object]] = {
    'db_operation_failed': _handle_failed('DB_OPERATION_FAILED'),
    'stored_data_invalid': _handle_failed('STORED_DATA_INVALID'),
    'core_cannot_run_the_call': CORE_REFUSAL,
}

FACADES: Final[dict[str, Facade]] = {
    'WorkflowHandle.status': Facade(
        blocking=lambda handle: handle.status(),
        awaited=lambda handle: handle.status_async(),
        blocking_form='workflow_status',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'running': _record(core.WorkflowStatusRead, status='RUNNING'),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'WorkflowHandle.get': Facade(
        blocking=lambda handle: handle.get(timeout_ms=250),
        awaited=lambda handle: handle.get_async(timeout_ms=250),
        blocking_form='wait_workflow_result',
        arguments=(WORKFLOW_KEY,),
        keywords={'timeout_ms': 250},
        answers={
            'completed': _record(core.WorkflowCompletedRead, result_json=_ok_envelope(5)),
            'completed_without_a_result': _record(core.WorkflowCompletedRead, result_json=None),
            'completed_with_a_corrupt_result': _record(
                core.WorkflowCompletedRead, result_json='{',
            ),
            'failed_without_an_error': _record(
                core.WorkflowEndedRead, status='FAILED', error_json=None,
            ),
            'cancelled_with_a_corrupt_error': _record(
                core.WorkflowEndedRead, status='CANCELLED', error_json='{',
            ),
            'paused': _record(core.WorkflowPausedRead),
            'timed_out': _record(core.WaitTimedOut),
            'absent': _record(core.WorkflowAbsent),
            'read_failed': _handle_failed('DB_OPERATION_FAILED'),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'WorkflowHandle.result_for': Facade(
        blocking=lambda handle: handle.result_for(NODE_KEY),
        awaited=lambda handle: handle.result_for_async(NODE_KEY),
        blocking_form='workflow_node_result',
        arguments=(WORKFLOW_KEY, NODE_ID),
        keywords={},
        answers={
            'stored': _record(
                core.StoredNodeResultRecord,
                node_id=NODE_ID,
                task_name=TASK_NAME,
                result_json=_ok_envelope(5),
                is_subworkflow=False,
                sub_definition_key=None,
            ),
            'stored_corrupt': _record(
                core.StoredNodeResultRecord,
                node_id=NODE_ID,
                task_name=TASK_NAME,
                result_json='{',
                is_subworkflow=False,
                sub_definition_key=None,
            ),
            'not_stored': _record(core.NodeResultNotStored, node_id=NODE_ID),
            'read_failed': _handle_failed('DB_OPERATION_FAILED'),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'WorkflowHandle.results': Facade(
        blocking=lambda handle: handle.results(),
        awaited=lambda handle: handle.results_async(),
        blocking_form='workflow_node_results',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'results': _record(
                core.WorkflowNodeResultsRead,
                nodes=[
                    _record(
                        core.StoredNodeResultRecord,
                        node_id=NODE_ID,
                        task_name=TASK_NAME,
                        result_json=_ok_envelope(5),
                        is_subworkflow=False,
                        sub_definition_key=None,
                    ),
                    _record(
                        core.StoredNodeResultRecord,
                        node_id='blocking_forms:1',
                        task_name=TASK_NAME,
                        result_json='{',
                        is_subworkflow=False,
                        sub_definition_key=None,
                    ),
                ],
            ),
            'no_results': _record(core.WorkflowNodeResultsRead, nodes=[]),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'WorkflowHandle.tasks': Facade(
        blocking=lambda handle: handle.tasks(),
        awaited=lambda handle: handle.tasks_async(),
        blocking_form='workflow_nodes',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'nodes': _record(
                core.WorkflowNodesRead,
                nodes=[
                    _record(
                        core.StoredNodeRecord,
                        node_id=NODE_ID,
                        task_index=0,
                        task_name=TASK_NAME,
                        status='COMPLETED',
                        result_json=_ok_envelope(5),
                        started_at=SENT_AT,
                        completed_at=SENT_AT,
                        sub_workflow_id=None,
                        sub_workflow_summary_json=None,
                        is_subworkflow=False,
                        sub_definition_key=None,
                    ),
                ],
            ),
            'a_corrupt_summary': _record(
                core.WorkflowNodesRead,
                nodes=[
                    _record(
                        core.StoredNodeRecord,
                        node_id=NODE_ID,
                        task_index=0,
                        task_name=TASK_NAME,
                        status='RUNNING',
                        result_json=None,
                        started_at=SENT_AT,
                        completed_at=None,
                        sub_workflow_id=None,
                        sub_workflow_summary_json='{',
                        is_subworkflow=False,
                        sub_definition_key=None,
                    ),
                ],
            ),
            'no_nodes': _record(core.WorkflowNodesRead, nodes=[]),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'WorkflowHandle.cancel': Facade(
        blocking=lambda handle: handle.cancel(),
        awaited=lambda handle: handle.cancel_async(),
        blocking_form='cancel_workflow',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'cancelled': _record(core.WorkflowCancelled),
            'not_cancellable': _record(core.WorkflowNotCancellable),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'WorkflowHandle.pause': Facade(
        blocking=lambda handle: handle.pause(),
        awaited=lambda handle: handle.pause_async(),
        blocking_form='pause_workflow',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'paused': _record(core.WorkflowPaused),
            'not_running': _record(core.WorkflowNotRunning),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'pause_workflow_sync': Facade(
        blocking=lambda handle: pause_workflow_sync(handle.broker, handle.workflow_id),
        awaited=lambda handle: pause_workflow(handle.broker, handle.workflow_id),
        blocking_form='pause_workflow',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'paused': _record(core.WorkflowPaused),
            'not_running': _record(core.WorkflowNotRunning),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'WorkflowHandle.resume': Facade(
        blocking=lambda handle: handle.resume(),
        awaited=lambda handle: handle.resume_async(),
        blocking_form='resume_workflow',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'resumed': _record(core.WorkflowResumed),
            'not_paused': _record(core.WorkflowNotPaused),
            'recovery_failed': _record(
                core.WorkflowResumeRecoveryFailed,
                message='recovery pass failed',
                exception=CONNECTION_LOST,
            ),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
    'resume_workflow_sync': Facade(
        blocking=lambda handle: resume_workflow_sync(handle.broker, handle.workflow_id),
        awaited=lambda handle: resume_workflow(handle.broker, handle.workflow_id),
        blocking_form='resume_workflow',
        arguments=(WORKFLOW_KEY,),
        keywords={},
        answers={
            'resumed': _record(core.WorkflowResumed),
            'not_paused': _record(core.WorkflowNotPaused),
            'recovery_failed': _record(
                core.WorkflowResumeRecoveryFailed,
                message='recovery pass failed',
                exception=CONNECTION_LOST,
            ),
            'absent': _record(core.WorkflowAbsent),
            **HANDLE_FAILURES,
        },
    ),
}

CASES: Final = [(facade, answer) for facade, entry in FACADES.items() for answer in entry.answers]


@pytest.mark.parametrize(('facade', 'answer'), CASES, ids=[f'{f}-{a}' for f, a in CASES])
def test_a_sync_facade_gives_the_outcome_of_its_async_form(facade: str, answer: str) -> None:
    entry = FACADES[facade]
    answers = _answering(entry.blocking_form, entry.answers[answer])

    blocking = entry.blocking(_workflow_handle())
    awaited = asyncio.run(_awaited(entry.awaited(_workflow_handle())))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    getattr(answers.handle, entry.blocking_form).assert_called_once_with(
        *entry.arguments, **entry.keywords,
    )
    answers.runner.call.assert_not_called()


@pytest.mark.parametrize('facade', list(FACADES))
def test_an_id_that_is_not_a_uuid_is_refused_as_the_async_form_refuses_it(facade: str) -> None:
    """No core call in either form."""
    entry = FACADES[facade]
    answers = _answering(entry.blocking_form, next(iter(entry.answers.values())))

    blocking = entry.blocking(_workflow_handle(NOT_A_UUID))
    awaited = asyncio.run(_awaited(entry.awaited(_workflow_handle(NOT_A_UUID))))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    getattr(answers.handle, entry.blocking_form).assert_not_called()
    getattr(answers.handle, f'{entry.blocking_form}_async').assert_not_called()


@pytest.mark.parametrize('facade', list(FACADES))
def test_a_defect_of_the_call_propagates_from_both_forms(facade: str) -> None:
    """An exception that is not a CoreError is not a catalog outcome."""
    entry = FACADES[facade]
    defect = TypeError('stub defect')
    _ = _answering(entry.blocking_form, defect)

    with pytest.raises(TypeError) as blocking:
        entry.blocking(_workflow_handle())
    with pytest.raises(TypeError) as awaited:
        asyncio.run(_awaited(entry.awaited(_workflow_handle())))

    assert blocking.value is defect
    assert awaited.value is defect


def test_get_refuses_a_timeout_as_get_async_refuses_it() -> None:
    """A refused timeout is INVALID_INPUT before any core call, in both forms."""
    answers = _answering('wait_workflow_result', _record(core.WaitTimedOut))

    blocking = _workflow_handle().get(timeout_ms=-1)
    awaited = asyncio.run(_workflow_handle().get_async(timeout_ms=-1))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    answers.handle.wait_workflow_result.assert_not_called()


def test_get_with_no_limit_passes_no_limit() -> None:
    answers = _answering('wait_workflow_result', _record(core.WaitTimedOut))

    _ = _workflow_handle().get()

    answers.handle.wait_workflow_result.assert_called_once_with(WORKFLOW_KEY, timeout_ms=None)


def test_result_for_refuses_a_node_without_an_id_as_its_async_form() -> None:
    """A node with no node_id is WORKFLOW_CTX_MISSING_ID before any core call."""
    answers = _answering('workflow_node_result', _record(core.NodeResultNotStored))
    node: TaskNode[int] = TaskNode(fn=value_task, kwargs={'value': 1})

    blocking = _workflow_handle().result_for(node)
    awaited = asyncio.run(_workflow_handle().result_for_async(node))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    answers.handle.workflow_node_result.assert_not_called()


# =============================================================================
# start_workflow: CoreHandle.start_workflow
# =============================================================================


def _spec(
    name: str = 'blocking_forms.flow',
    definition_key: str = 'blocking_forms.flow.v1',
) -> WorkflowSpec[int]:
    node: TaskNode[int] = TaskNode(fn=value_task, kwargs={'value': 1})
    return APP.workflow(name, [node], output=node, definition_key=definition_key)


START_ANSWERS: Final[dict[str, object]] = {
    'started': _record(core.WorkflowStarted),
    'exists': _record(core.WorkflowExists),
    'id_taken': _record(core.WorkflowIdTaken, existing_name='other.flow'),
    'start_failed': _record(
        core.WorkflowStartFailed,
        message='connection refused',
        retryable=True,
        exception=CONNECTION_LOST,
    ),
    'core_cannot_run_the_call': CORE_REFUSAL,
}


@pytest.mark.parametrize('answer', list(START_ANSWERS))
def test_start_workflow_gives_the_outcome_of_start_workflow_async(answer: str) -> None:
    answers = _answering('start_workflow', START_ANSWERS[answer])
    broker = APP.get_broker()

    with patch('horsies._core.WorkflowStartRequest') as request_class:
        blocking = start_workflow(
            _spec(), broker, WORKFLOW_ID, SENT_AT, resend_on_transient_err=True,
        )
        blocking_request = request_class.call_args
        awaited = asyncio.run(
            start_workflow_async(
                _spec(), broker, WORKFLOW_ID, SENT_AT, resend_on_transient_err=True,
            ),
        )
        awaited_request = request_class.call_args

    assert outcome_shape(blocking) == outcome_shape(awaited)
    assert blocking_request == awaited_request
    assert blocking_request.kwargs['resend_on_transient_err'] is True
    assert blocking_request.kwargs['workflow_id'] == WORKFLOW_KEY
    answers.handle.start_workflow.assert_called_once_with(request_class.return_value)
    answers.runner.call.assert_not_called()


START_REFUSALS: Final[dict[str, Callable[[], tuple[WorkflowSpec[int], str]]]] = {
    'an_id_that_is_not_a_uuid': lambda: (_spec(), NOT_A_UUID),
    'spec_data_that_does_not_encode': lambda: (
        _spec('blocking_forms.\ud800', 'blocking_forms.surrogate.v1'),
        WORKFLOW_ID,
    ),
}


@pytest.mark.parametrize('refusal', list(START_REFUSALS))
def test_start_workflow_refuses_as_start_workflow_async_refuses(refusal: str) -> None:
    """A refusal of the prevalidation or of the spec data makes no core call."""
    answers = _answering('start_workflow', _record(core.WorkflowStarted))
    broker = APP.get_broker()
    spec, workflow_id = START_REFUSALS[refusal]()

    blocking = start_workflow(spec, broker, workflow_id, SENT_AT)
    awaited = asyncio.run(start_workflow_async(spec, broker, workflow_id, SENT_AT))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    answers.handle.start_workflow.assert_not_called()
    answers.handle.start_workflow_async.assert_not_called()


def test_an_exception_of_a_child_build_propagates_from_both_start_forms() -> None:
    """The core raises the exception of a child's build_with again; both
    forms let it propagate."""
    raised = SerializationError('task options of a child root do not parse')
    _ = _answering('start_workflow', raised)
    broker = APP.get_broker()

    with pytest.raises(SerializationError) as blocking:
        start_workflow(_spec(), broker, WORKFLOW_ID, SENT_AT)
    with pytest.raises(SerializationError) as awaited:
        asyncio.run(start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT))

    assert blocking.value is raised
    assert awaited.value is raised


def test_a_minted_id_is_one_uuid_for_the_request_and_the_handle() -> None:
    """With no workflow_id the sync start mints one id, as the async form does."""
    _ = _answering('start_workflow', _record(core.WorkflowStarted))

    with patch('horsies._core.WorkflowStartRequest') as request_class:
        started = start_workflow(_spec(), APP.get_broker())

    assert is_ok(started), started
    minted = started.ok_value.workflow_id
    assert request_class.call_args.kwargs['workflow_id'] == uuid.UUID(minted)
