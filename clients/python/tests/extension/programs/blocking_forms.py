"""Each blocking form of ``CoreHandle`` that the sync facades call gives the
outcome of its awaitable form.

A case runs the same steps twice: once with the blocking forms on the main
thread, once with the awaitable forms on an event loop (``asyncio.run``). A
write changes the state that the next step reads, so each run has its own
workflow of the same spec. The report holds the description of each step's
outcome for both runs (``describe``).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Final, assert_never

from pydantic import SecretStr

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import TaskNode
from horsies.core.brokers.result_types import (
    DatabaseInvalidInput,
    DatabaseInvalidJsonPayload,
    DatabaseListenerDisabled,
    DatabaseOperationFailed,
)
from horsies.core.models.workflow.handle_types import (
    WorkflowHandleDatabaseOperationFailed,
    WorkflowHandleResumeRecoveryFailed,
    WorkflowHandleStoredDataInvalid,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.workflows.start_types import (
    WorkflowStartEnqueueFailed,
    WorkflowStartIdConflict,
)
from horsies.core.workflows.spec_data import workflow_spec_json
from horsies.core.workflows.start_tree import top_node_rows
from tests.extension.programs.report import JsonValue, Report, run_case
from tests.extension.programs.support import database_url, manifest_json

UNREACHABLE_URL: Final = 'postgresql://extension@127.0.0.1:1/never_connected'
"""A database that refuses every connect at once."""

PING_WAIT_MS: Final = 200
"""The reply window of a ping, in milliseconds; no worker runs, so no reply
comes."""

STALE_THRESHOLD_MINUTES: Final = 1
"""The threshold of the stale list."""

WORKFLOW_NAME: Final = 'tests.extension.blocking_forms'
OTHER_WORKFLOW_NAME: Final = 'tests.extension.blocking_forms.other'
NODE_ID: Final = f'{WORKFLOW_NAME}:0'

APP: Final = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        database=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)),
    ),
)


@APP.task(name='tests.extension.blocking_forms.value')
def value_task(*, value: int) -> Result[int, TaskError]:
    return Ok(value)


def _start_request(name: str, workflow_id: uuid.UUID) -> core.WorkflowStartRequest:
    """The start of a workflow of one task node, named ``name``.

    Raises:
        RuntimeError: the spec data does not encode (a defect of the builder).
    """
    node: TaskNode[int] = TaskNode(function=value_task, kwargs={'value': 1}, node_id=NODE_ID)
    spec = APP.workflow(name, [node], output=node, definition_key=f'{name}.v1')
    match workflow_spec_json(spec, top_node_rows(APP.get_broker(), spec)):
        case Ok(ok_value=spec_json):
            pass
        case Err(err_value=refused):
            raise RuntimeError(f'the spec data of {name} does not encode: {refused}')
        case _ as unreachable:
            assert_never(unreachable)
    return core.WorkflowStartRequest(
        spec_json=spec_json,
        workflow_id=workflow_id,
        sent_at=datetime.now(UTC),
        resend_on_transient_error=False,
        trace=core.NoTraceCarrier(),
    )


def _refusing_source(request: core.ChildSpecRequest) -> core.ChildSpecBuilt | core.ChildSpecRefused:
    """A child spec source for specs with no sub-workflow node: never called."""
    raise AssertionError(f'no child build was expected: {request!r}')


def _handle(url: str) -> core.CoreHandle:
    handle = core.CoreHandle(
        manifest_json(url, schema='MIGRATE_AND_VALIDATE', echo=False),
        pool_profile='PRODUCER',
    )
    handle.register_child_spec_source(_refusing_source)
    return handle


def describe(outcome: object) -> JsonValue:
    """The record class of an outcome and the facts that the facades read."""
    name = type(outcome).__name__
    match outcome:
        case core.WorkflowStatusRead(status=status):
            return [name, status]
        case core.WorkflowEndedRead(status=status, error_json=error_json):
            return [name, status, error_json is None]
        case core.WorkflowCompletedRead(result_json=result_json):
            return [name, result_json]
        case WorkflowStartIdConflict(existing_workflow_name=existing_name):
            return [name, existing_name]
        case core.WorkflowNodesRead(nodes=nodes):
            return [name, [[node.node_id, node.task_index, node.status] for node in nodes]]
        case core.WorkflowNodeResultsRead(nodes=nodes):
            return [name, [node.node_id for node in nodes]]
        case (
            WorkflowHandleDatabaseOperationFailed()
            | WorkflowHandleStoredDataInvalid()
            | WorkflowHandleResumeRecoveryFailed()
        ):
            return [name, outcome.code.value, outcome.retryable]
        case WorkflowStartEnqueueFailed(retryable=retryable):
            return [name, retryable]
        case (
            DatabaseOperationFailed()
            | DatabaseInvalidInput()
            | DatabaseInvalidJsonPayload()
            | DatabaseListenerDisabled()
        ):
            return [name, outcome.code.value, outcome.retryable]
        case core.WorkerStatesRecord(states=states):
            return [name, len(states)]
        case core.WorkerPongsRecord(pongs=pongs):
            return [name, len(pongs)]
        case core.ExecutionClassCoverageRecord(live_workers=live_workers, streams=streams):
            return [name, live_workers, len(streams)]
        case core.ExecutionClassReportRecord(streams=streams, workers=workers):
            return [name, len(streams), len(workers)]
        case (
            core.StaleTaskListComplete(rows=rows)
            | core.StaleTaskListTruncated(rows=rows)
        ):
            return [name, len(rows)]
        case (
            core.ExpiredTaskListComplete(rows=rows)
            | core.ExpiredTaskListTruncated(rows=rows)
        ):
            return [name, len(rows)]
        case _:
            return [name]


type Step = tuple[str, Callable[[], object], Callable[[], Awaitable[object]]]
"""A step: its name, its blocking call, and its awaitable call."""


def _steps_report(blocking_steps: list[Step], awaited_steps: list[Step]) -> Report:
    """Run the blocking steps in order, then the awaitable steps in order on
    one event loop; the outcome of each step of both runs."""
    blocking: list[JsonValue] = [[name, describe(call())] for name, call, _ in blocking_steps]

    async def run_awaited() -> list[JsonValue]:
        return [[name, describe(await call())] for name, _, call in awaited_steps]

    awaited = asyncio.run(run_awaited())
    return {'blocking': blocking, 'awaited': awaited}


def _workflow_steps(handle: core.CoreHandle, workflow_id: uuid.UUID) -> list[Step]:
    """The life of one workflow: start, the reads, pause, resume, cancel,
    and the wait at each stage."""
    start = _start_request(WORKFLOW_NAME, workflow_id)
    other = _start_request(OTHER_WORKFLOW_NAME, workflow_id)
    return [
        ('start', lambda: handle.start_workflow(start), lambda: handle.start_workflow_async(start)),
        ('start_again', lambda: handle.start_workflow(start), lambda: handle.start_workflow_async(start)),
        ('start_other_name', lambda: handle.start_workflow(other), lambda: handle.start_workflow_async(other)),
        ('status', lambda: handle.workflow_status(workflow_id), lambda: handle.workflow_status_async(workflow_id)),
        (
            'wait_no_time',
            lambda: handle.wait_workflow_result(workflow_id, timeout_ms=0),
            lambda: handle.wait_workflow_result_async(workflow_id, timeout_ms=0),
        ),
        (
            'node_result',
            lambda: handle.workflow_node_result(workflow_id, NODE_ID),
            lambda: handle.workflow_node_result_async(workflow_id, NODE_ID),
        ),
        (
            'node_result_no_such_node',
            lambda: handle.workflow_node_result(workflow_id, 'no_such_node'),
            lambda: handle.workflow_node_result_async(workflow_id, 'no_such_node'),
        ),
        (
            'node_results',
            lambda: handle.workflow_node_results(workflow_id),
            lambda: handle.workflow_node_results_async(workflow_id),
        ),
        ('nodes', lambda: handle.workflow_nodes(workflow_id), lambda: handle.workflow_nodes_async(workflow_id)),
        ('pause', lambda: handle.pause_workflow(workflow_id), lambda: handle.pause_workflow_async(workflow_id)),
        ('pause_again', lambda: handle.pause_workflow(workflow_id), lambda: handle.pause_workflow_async(workflow_id)),
        (
            'wait_paused',
            lambda: handle.wait_workflow_result(workflow_id, timeout_ms=0),
            lambda: handle.wait_workflow_result_async(workflow_id, timeout_ms=0),
        ),
        ('resume', lambda: handle.resume_workflow(workflow_id), lambda: handle.resume_workflow_async(workflow_id)),
        ('resume_again', lambda: handle.resume_workflow(workflow_id), lambda: handle.resume_workflow_async(workflow_id)),
        ('cancel', lambda: handle.cancel_workflow(workflow_id), lambda: handle.cancel_workflow_async(workflow_id)),
        ('cancel_again', lambda: handle.cancel_workflow(workflow_id), lambda: handle.cancel_workflow_async(workflow_id)),
        (
            'wait_no_limit',
            lambda: handle.wait_workflow_result(workflow_id, timeout_ms=None),
            lambda: handle.wait_workflow_result_async(workflow_id, timeout_ms=None),
        ),
    ]


def _absent_steps(handle: core.CoreHandle, workflow_id: uuid.UUID) -> list[Step]:
    """Each workflow operation on an id that no workflow has."""
    return [
        ('status', lambda: handle.workflow_status(workflow_id), lambda: handle.workflow_status_async(workflow_id)),
        (
            'wait',
            lambda: handle.wait_workflow_result(workflow_id, timeout_ms=0),
            lambda: handle.wait_workflow_result_async(workflow_id, timeout_ms=0),
        ),
        (
            'node_result',
            lambda: handle.workflow_node_result(workflow_id, NODE_ID),
            lambda: handle.workflow_node_result_async(workflow_id, NODE_ID),
        ),
        (
            'node_results',
            lambda: handle.workflow_node_results(workflow_id),
            lambda: handle.workflow_node_results_async(workflow_id),
        ),
        ('nodes', lambda: handle.workflow_nodes(workflow_id), lambda: handle.workflow_nodes_async(workflow_id)),
        ('pause', lambda: handle.pause_workflow(workflow_id), lambda: handle.pause_workflow_async(workflow_id)),
        ('resume', lambda: handle.resume_workflow(workflow_id), lambda: handle.resume_workflow_async(workflow_id)),
        ('cancel', lambda: handle.cancel_workflow(workflow_id), lambda: handle.cancel_workflow_async(workflow_id)),
    ]


def _monitoring_steps(handle: core.CoreHandle) -> list[Step]:
    """The worker state reads, the class reads, the task lists, and the
    worker ping."""
    return [
        ('worker_states', handle.worker_states, handle.worker_states_async),
        (
            'worker_state',
            lambda: handle.worker_state('tests.extension.no_such_worker'),
            lambda: handle.worker_state_async('tests.extension.no_such_worker'),
        ),
        (
            'worker_state_history',
            lambda: handle.worker_state_history('tests.extension.no_such_worker', limit=None),
            lambda: handle.worker_state_history_async('tests.extension.no_such_worker', limit=None),
        ),
        (
            'worker_state_history_refused',
            lambda: handle.worker_state_history('tests.extension.no_such_worker', limit=0),
            lambda: handle.worker_state_history_async('tests.extension.no_such_worker', limit=0),
        ),
        ('execution_class_coverage', handle.execution_class_coverage, handle.execution_class_coverage_async),
        ('execution_class_report', handle.execution_class_report, handle.execution_class_report_async),
        (
            'stale_tasks',
            lambda: handle.stale_tasks(STALE_THRESHOLD_MINUTES),
            lambda: handle.stale_tasks_async(STALE_THRESHOLD_MINUTES),
        ),
        (
            'stale_tasks_refused',
            lambda: handle.stale_tasks(-1),
            lambda: handle.stale_tasks_async(-1),
        ),
        ('expired_tasks', handle.expired_tasks, handle.expired_tasks_async),
        (
            'ping',
            lambda: handle.ping_workers(
                target_worker_id=None, wait_ms=PING_WAIT_MS, min_responses=None,
            ),
            lambda: handle.ping_workers_async(
                target_worker_id=None, wait_ms=PING_WAIT_MS, min_responses=None,
            ),
        ),
        (
            'ping_refused',
            lambda: handle.ping_workers(
                target_worker_id=None, wait_ms=0, min_responses=None,
            ),
            lambda: handle.ping_workers_async(
                target_worker_id=None, wait_ms=0, min_responses=None,
            ),
        ),
    ]


def workflow_operations() -> Report:
    """The life of a workflow, run on workflow A with the blocking forms and
    on workflow B with the awaitable forms; then each operation on an id
    that no workflow has."""
    handle = _handle(database_url())
    lives = _steps_report(
        _workflow_steps(handle, uuid.uuid4()), _workflow_steps(handle, uuid.uuid4()),
    )
    absent = uuid.uuid4()
    absent_steps = _absent_steps(handle, absent)
    absents = _steps_report(absent_steps, absent_steps)
    _ = handle.close()
    return {'lives': lives, 'absent': absents}


def monitoring_operations() -> Report:
    """The monitoring reads and the ping, each blocking form next to its
    awaitable form."""
    handle = _handle(database_url())
    steps = _monitoring_steps(handle)
    report = _steps_report(steps, steps)
    _ = handle.close()
    return report


def unreachable_database() -> Report:
    """Each operation on a handle whose database refuses the connect."""
    handle = _handle(UNREACHABLE_URL)
    workflow_id = uuid.uuid4()
    start = _start_request(WORKFLOW_NAME, workflow_id)
    steps: list[Step] = [
        ('start', lambda: handle.start_workflow(start), lambda: handle.start_workflow_async(start)),
        *_absent_steps(handle, workflow_id),
        *_monitoring_steps(handle),
    ]
    return _steps_report(steps, steps)


if __name__ == '__main__':
    run_case(
        {
            'workflow_operations': workflow_operations,
            'monitoring_operations': monitoring_operations,
            'unreachable_database': unreachable_database,
        },
    )
