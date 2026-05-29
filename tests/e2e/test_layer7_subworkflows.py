"""Layer 7 e2e tests: subworkflow lifecycle matrix."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable, cast

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    encode_task_result,
)
from horsies.core.models.tasks import (
    OutcomeCode,
    SubWorkflowError,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    SubWorkflowSummary,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowHandle,
    WorkflowStatus,
    WorkflowTaskStatus,
)
from horsies.core.types.result import is_err, is_ok
from tests.e2e.helpers.assertions import (
    assert_err,
    assert_ok,
    start_ok_sync,
    unwrap_send,
)
from tests.e2e.helpers.worker import run_worker
from tests.e2e.helpers.workflow import get_workflow_status, wait_for_workflow_completion
from tests.e2e.tasks import workflows as wf_tasks
from tests.e2e.tasks.basic import healthcheck
from tests.e2e.tasks.instance import app


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope='function'),
]

def _make_ready_check() -> Callable[[], bool]:
    from horsies.core.task_decorator import TaskHandle

    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = unwrap_send(healthcheck.send())
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


async def _wait_until(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout_s: float = 15.0,
    poll_interval: float = 0.05,
) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(poll_interval)
    return await predicate()


def _unwrap_handle_result(result: Any) -> Any:
    if is_err(result):
        pytest.fail(f'handle operation failed: {result.err_value}')
    return result.ok_value


def _load_json(raw: str | None) -> Any:
    assert raw is not None
    loaded = loads_json(raw)
    assert is_ok(loaded), f'JSON decode failed: {loaded}'
    return loaded.ok_value


def _decode_task_result_column(
    raw: str | None,
    ok_type: Any,
) -> TaskResult[Any, TaskError]:
    return decode_task_result(_load_json(raw), ok_type)


def _decode_error_column(raw: str | None) -> TaskError:
    return decode_task_error(_load_json(raw))


def _decode_summary_column(raw: str | None) -> SubWorkflowSummary[Any]:
    loaded = _load_json(raw)
    assert isinstance(loaded, dict)
    return SubWorkflowSummary.from_json(loaded)


def _assert_subworkflow_error(
    result: TaskResult[Any, TaskError],
    *,
    child_id: str | None = None,
) -> SubWorkflowError:
    assert result.is_err(), f'expected Err(SubWorkflowError), got {result}'
    err = result.err
    assert isinstance(err, SubWorkflowError), f'expected SubWorkflowError, got {err}'
    assert err.error_code == OutcomeCode.SUBWORKFLOW_FAILED
    if child_id is not None:
        assert err.sub_workflow_id == child_id
    assert err.sub_workflow_summary.status == WorkflowStatus.FAILED
    assert err.sub_workflow_summary.failed_tasks >= 1
    return err


def _workflow_spec(
    name: str,
    tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
    *,
    output: TaskNode[Any] | SubWorkflowNode[Any] | None = None,
    success_policy: SuccessPolicy | None = None,
    on_error: OnError = OnError.FAIL,
) -> Any:
    return app.workflow(
        name=f'e2e_layer7_{name}',
        tasks=tasks,
        output=output,
        success_policy=success_policy,
        on_error=on_error,
        definition_key=f'tests.e2e.layer7.{name}.v1',
    )


def _task_result_json(result: TaskResult[Any, TaskError], ok_type: Any) -> str:
    dumped = dumps_json(encode_task_result(result, ok_type))
    assert is_ok(dumped), f'TaskResult encode failed: {dumped}'
    return dumped.ok_value


def _rows_by_node(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row['node_id']): row for row in rows}


async def _wait_for_node_status(
    broker: PostgresBroker,
    workflow_id: str,
    node_id: str,
    expected: set[str],
    *,
    timeout_s: float = 15.0,
) -> dict[str, Any]:
    found: dict[str, Any] | None = None

    async def _matches() -> bool:
        nonlocal found
        rows = _rows_by_node(await _workflow_task_rows(broker, workflow_id))
        row = rows.get(node_id)
        if row is None:
            return False
        found = row
        return row['status'] in expected

    matched = await _wait_until(_matches, timeout_s=timeout_s)
    assert matched, f'{node_id} did not reach {expected}; last row={found}'
    assert found is not None
    return found


async def _wait_for_subworkflow_id(
    broker: PostgresBroker,
    workflow_id: str,
    node_id: str,
    *,
    timeout_s: float = 15.0,
) -> str:
    child_id: str | None = None

    async def _has_child() -> bool:
        nonlocal child_id
        rows = _rows_by_node(await _workflow_task_rows(broker, workflow_id))
        row = rows.get(node_id)
        if row is None:
            return False
        raw_child_id = row['sub_workflow_id']
        if isinstance(raw_child_id, str):
            child_id = raw_child_id
            return True
        return False

    matched = await _wait_until(_has_child, timeout_s=timeout_s)
    assert matched, f'{node_id} never received a child workflow id'
    assert child_id is not None
    return child_id


async def _run_workflow_recovery(broker: PostgresBroker) -> int:
    from horsies.core.workflows.recovery import recover_stuck_workflows

    async with broker.session_factory() as session:
        recovered = await recover_stuck_workflows(session, broker)
        await session.commit()
    return recovered


async def _wait_for_workflow_status(
    broker: PostgresBroker,
    workflow_id: str,
    expected: set[str],
    *,
    timeout_s: float = 15.0,
) -> str:
    last_status = 'UNKNOWN'

    async def _matches() -> bool:
        nonlocal last_status
        last_status = await get_workflow_status(broker.session_factory, workflow_id)
        return last_status in expected

    matched = await _wait_until(_matches, timeout_s=timeout_s)
    assert matched, f'{workflow_id} did not reach {expected}; last={last_status}'
    return last_status


async def _mark_workflow_task_completed(
    broker: PostgresBroker,
    workflow_id: str,
    node_id: str,
    value: Any,
    ok_type: Any,
) -> None:
    result_json = _task_result_json(TaskResult(ok=value), ok_type)
    async with broker.session_factory() as session:
        row_result = await session.execute(
            text("""
                SELECT task_id
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND node_id = :node_id
            """),
            {'wf_id': workflow_id, 'node_id': node_id},
        )
        row = row_result.fetchone()
        assert row is not None, f'workflow task {node_id} not found'
        task_id = row.task_id
        if isinstance(task_id, str):
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED', result = :result, updated_at = NOW()
                    WHERE id = :task_id
                """),
                {'task_id': task_id, 'result': result_json},
            )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = :result, completed_at = NOW()
                WHERE workflow_id = :wf_id AND node_id = :node_id
            """),
            {'wf_id': workflow_id, 'node_id': node_id, 'result': result_json},
        )
        await session.commit()


async def _workflow_row(
    broker: PostgresBroker,
    workflow_id: str,
) -> dict[str, Any]:
    async with broker.session_factory() as session:
        result = await session.execute(
            text("""
                SELECT id, name, status, result, error, parent_workflow_id,
                       parent_task_index, depth, root_workflow_id
                FROM horsies_workflows
                WHERE id = :wf_id
            """),
            {'wf_id': workflow_id},
        )
        row = result.mappings().one()
        return dict(row)


async def _workflow_tree_rows(
    broker: PostgresBroker,
    root_workflow_id: str,
) -> list[dict[str, Any]]:
    async with broker.session_factory() as session:
        result = await session.execute(
            text("""
                SELECT id, name, status, result, error, parent_workflow_id,
                       parent_task_index, depth, root_workflow_id
                FROM horsies_workflows
                WHERE id = :root_id OR root_workflow_id = :root_id
                ORDER BY depth, name
            """),
            {'root_id': root_workflow_id},
        )
        return [dict(row) for row in result.mappings().all()]


async def _workflow_task_rows(
    broker: PostgresBroker,
    workflow_id: str,
) -> list[dict[str, Any]]:
    async with broker.session_factory() as session:
        result = await session.execute(
            text("""
                SELECT task_index, node_id, task_name, status, result,
                       error, task_id, is_subworkflow, sub_workflow_id,
                       sub_workflow_name, sub_workflow_summary
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id
                ORDER BY task_index
            """),
            {'wf_id': workflow_id},
        )
        return [dict(row) for row in result.mappings().all()]


async def test_subworkflow_success_full_surface_matrix(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_full_surface)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert_ok(result, expected_value=11)

        task_rows = await _workflow_task_rows(broker, handle.workflow_id)
        assert len(task_rows) == 1
        parent_task = task_rows[0]
        assert parent_task['status'] == 'COMPLETED'
        assert parent_task['is_subworkflow'] is True
        child_id = parent_task['sub_workflow_id']
        assert isinstance(child_id, str)

        stored_result = _decode_task_result_column(parent_task['result'], int)
        assert_ok(stored_result, expected_value=11)

        summary = _decode_summary_column(parent_task['sub_workflow_summary'])
        assert summary.status == WorkflowStatus.COMPLETED
        assert summary.output == 11
        assert summary.total_tasks == 1
        assert summary.completed_tasks == 1

        child_row = await _workflow_row(broker, child_id)
        assert child_row['status'] == 'COMPLETED'
        assert child_row['parent_workflow_id'] == handle.workflow_id
        assert child_row['parent_task_index'] == parent_task['task_index']
        assert child_row['depth'] == 1
        assert child_row['root_workflow_id'] == handle.workflow_id

        task_infos = _unwrap_handle_result(handle.tasks())
        assert len(task_infos) == 1
        task_info = task_infos[0]
        assert task_info.status == WorkflowTaskStatus.COMPLETED
        assert task_info.sub_workflow_id == child_id
        assert task_info.sub_workflow_summary is not None
        assert task_info.sub_workflow_summary.status == WorkflowStatus.COMPLETED
        assert task_info.result is not None
        assert_ok(task_info.result, expected_value=11)

        by_node = handle.result_for(wf_tasks.node_e2e_sub_success_child)
        assert_ok(by_node, expected_value=11)


async def test_subworkflow_failure_fail_policy_preserves_error_everywhere(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_failure_fail_policy)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'FAILED'

        task_rows = await _workflow_task_rows(broker, handle.workflow_id)
        assert len(task_rows) == 1
        parent_task = task_rows[0]
        child_id = parent_task['sub_workflow_id']
        assert isinstance(child_id, str)
        assert parent_task['status'] == 'FAILED'

        stored_result = _decode_task_result_column(parent_task['result'], int)
        _assert_subworkflow_error(stored_result, child_id=child_id)

        workflow = await _workflow_row(broker, handle.workflow_id)
        workflow_error = _decode_error_column(workflow['error'])
        assert isinstance(workflow_error, SubWorkflowError)
        assert workflow_error.sub_workflow_id == child_id

        handle_result = handle.get(timeout_ms=2000)
        _assert_subworkflow_error(handle_result, child_id=child_id)

        task_infos = _unwrap_handle_result(handle.tasks())
        assert len(task_infos) == 1
        task_info = task_infos[0]
        assert task_info.status == WorkflowTaskStatus.FAILED
        assert task_info.sub_workflow_id == child_id
        assert task_info.sub_workflow_summary is not None
        assert task_info.sub_workflow_summary.status == WorkflowStatus.FAILED
        assert task_info.result is not None
        _assert_subworkflow_error(task_info.result, child_id=child_id)

        by_node = handle.result_for(wf_tasks.node_e2e_sub_failure_child)
        _assert_subworkflow_error(by_node, child_id=child_id)


async def test_subworkflow_failure_pause_policy_preserves_error_after_resume(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_failure_pause_policy)

        async def _is_paused() -> bool:
            status = await get_workflow_status(
                broker.session_factory, handle.workflow_id
            )
            return status == 'PAUSED'

        paused = await _wait_until(_is_paused, timeout_s=15.0)
        assert paused, 'workflow should auto-pause after child workflow failure'

        paused_result = handle.get(timeout_ms=1000)
        assert_err(paused_result, expected_code='WORKFLOW_PAUSED')

        task_rows = await _workflow_task_rows(broker, handle.workflow_id)
        parent_task = task_rows[0]
        child_id = parent_task['sub_workflow_id']
        assert isinstance(child_id, str)
        assert parent_task['status'] == 'FAILED'
        _assert_subworkflow_error(
            _decode_task_result_column(parent_task['result'], int),
            child_id=child_id,
        )

        workflow = await _workflow_row(broker, handle.workflow_id)
        paused_error = _decode_error_column(workflow['error'])
        assert isinstance(paused_error, SubWorkflowError)
        assert paused_error.sub_workflow_id == child_id

        resume_result = handle.resume()
        assert is_ok(resume_result), f'resume failed: {resume_result}'
        assert resume_result.ok_value is True

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'FAILED'

        final_result = handle.get(timeout_ms=2000)
        _assert_subworkflow_error(final_result, child_id=child_id)


async def test_successful_subworkflow_is_available_through_workflow_ctx(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_ctx_probe)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected probe success, got {result}'
        payload = cast(dict[str, Any], result.unwrap())
        assert payload['result_ok'] is True
        assert payload['result_value'] == 11
        assert payload['result_error_code'] is None
        assert payload['summary_lookup_ok'] is True
        assert payload['summary_status'] == 'COMPLETED'
        assert payload['summary_output'] == 11
        assert payload['summary_completed_tasks'] == 1


async def test_failed_subworkflow_flows_through_args_from_and_workflow_ctx(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_failure_args_ctx_probe)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected probe success, got {result}'
        payload = cast(dict[str, Any], result.unwrap())
        assert payload['args_lookup_ok'] is True
        assert payload['args_is_subworkflow_error'] is True
        assert payload['ctx_lookup_ok'] is True
        assert payload['ctx_is_subworkflow_error'] is True
        assert payload['ids_match'] is True
        assert payload['summary_lookup_ok'] is True
        assert payload['summary_status'] == 'FAILED'
        assert payload['summary_failed_tasks'] == 1


async def test_outputless_success_policy_keeps_failed_subworkflow_result(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_outputless_success_policy)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected outputless result map, got {result}'
        terminal_results = cast(dict[str, TaskResult[Any, TaskError]], result.unwrap())

        ok_result = terminal_results[wf_tasks.node_e2e_sub_outputless_ok.node_id]
        assert_ok(ok_result, expected_value='required_ok')

        failed_result = terminal_results[
            wf_tasks.node_e2e_sub_outputless_fail_child.node_id
        ]
        child_id = (await _workflow_task_rows(broker, handle.workflow_id))[1][
            'sub_workflow_id'
        ]
        assert isinstance(child_id, str)
        _assert_subworkflow_error(failed_result, child_id=child_id)


async def test_parallel_subworkflows_preserve_independent_success_and_failure(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_parallel_success_failure)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        task_rows = await _workflow_task_rows(broker, handle.workflow_id)
        assert len(task_rows) == 2
        by_node = {row['node_id']: row for row in task_rows}

        ok_row = by_node[wf_tasks.node_e2e_sub_parallel_ok_child.node_id]
        fail_row = by_node[wf_tasks.node_e2e_sub_parallel_fail_child.node_id]
        assert ok_row['status'] == 'COMPLETED'
        assert fail_row['status'] == 'FAILED'
        assert ok_row['sub_workflow_id'] != fail_row['sub_workflow_id']

        assert_ok(_decode_task_result_column(ok_row['result'], int), expected_value=11)
        _assert_subworkflow_error(
            _decode_task_result_column(fail_row['result'], int),
            child_id=fail_row['sub_workflow_id'],
        )

        ok_summary = _decode_summary_column(ok_row['sub_workflow_summary'])
        fail_summary = _decode_summary_column(fail_row['sub_workflow_summary'])
        assert ok_summary.status == WorkflowStatus.COMPLETED
        assert fail_summary.status == WorkflowStatus.FAILED


async def test_nested_subworkflow_success_records_depth_root_and_result(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_nested_success)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)

        tree = await _workflow_tree_rows(broker, handle.workflow_id)
        assert len(tree) == 3
        root = next(row for row in tree if row['id'] == handle.workflow_id)
        middle = next(row for row in tree if row['depth'] == 1)
        leaf = next(row for row in tree if row['depth'] == 2)

        assert root['parent_workflow_id'] is None
        assert middle['parent_workflow_id'] == handle.workflow_id
        assert middle['root_workflow_id'] == handle.workflow_id
        assert leaf['parent_workflow_id'] == middle['id']
        assert leaf['root_workflow_id'] == handle.workflow_id
        assert middle['status'] == 'COMPLETED'
        assert leaf['status'] == 'COMPLETED'


async def test_nested_subworkflow_failure_surfaces_each_child_error(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_nested_failure)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'FAILED'

        root_tasks = await _workflow_task_rows(broker, handle.workflow_id)
        middle_id = root_tasks[0]['sub_workflow_id']
        assert isinstance(middle_id, str)

        root_result = handle.get(timeout_ms=2000)
        root_error = _assert_subworkflow_error(root_result, child_id=middle_id)
        assert root_error.sub_workflow_summary.failed_tasks == 1

        middle = await _workflow_row(broker, middle_id)
        middle_error = _decode_error_column(middle['error'])
        assert isinstance(middle_error, SubWorkflowError)
        assert middle_error.sub_workflow_summary.failed_tasks == 1

        middle_tasks = await _workflow_task_rows(broker, middle_id)
        leaf_id = middle_tasks[0]['sub_workflow_id']
        assert isinstance(leaf_id, str)
        middle_task_result = _decode_task_result_column(middle_tasks[0]['result'], int)
        _assert_subworkflow_error(middle_task_result, child_id=leaf_id)


async def test_subworkflow_build_with_args_from_receives_typed_task_result(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_build_with_args_from)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=37)


async def test_subworkflow_allow_failed_deps_passes_failed_task_result_to_build_with(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(
            wf_tasks.spec_subworkflow_allow_failed_deps_to_build_with,
        )

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected child report, got {result}'
        payload = cast(dict[str, Any], result.unwrap())
        assert payload['input_is_err'] is True
        assert payload['input_error_code'] == 'E2E_UPSTREAM_FAIL'
        assert payload['input_value'] is None


async def test_subworkflow_static_kwargs_preserve_build_with_types(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_static_kwargs_preserve_types)

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected typed kwarg report, got {result}'
        payload = cast(dict[str, Any], result.unwrap())
        assert payload['saw_dog_at_build'] is True
        assert payload['type_name_at_build'] == 'E2ESubDog'
        assert payload['kind_at_build'] == 'dog'
        assert payload['bark_volume_at_build'] == 7


async def test_subworkflow_join_any_first_success(
    broker: PostgresBroker,
) -> None:
    fast = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_any_fast_success',
        kwargs={'value': 'fast'},
    )
    slow = TaskNode(
        fn=wf_tasks.slow_mark_task,
        node_id='l7_any_slow_success',
        kwargs={'value': 'slow', 'delay_ms': 1_000},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_any_child',
        waits_for=[fast, slow],
        join='any',
    )
    spec = _workflow_spec(
        'join_any_first_success',
        [fast, slow, child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[slow.node_id or '']['status'] in {'ENQUEUED', 'RUNNING'}

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=20.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[child.node_id or '']['sub_workflow_id'] == child_id
        assert rows[child.node_id or '']['status'] == 'COMPLETED'


async def test_subworkflow_join_any_all_fail(
    broker: PostgresBroker,
) -> None:
    fail_a = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_any_fail_a',
        kwargs={'error_code': 'L7_ANY_FAIL_A'},
    )
    fail_b = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_any_fail_b',
        kwargs={'error_code': 'L7_ANY_FAIL_B'},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_any_all_fail_child',
        waits_for=[fail_a, fail_b],
        join='any',
    )
    spec = _workflow_spec(
        'join_any_all_fail',
        [fail_a, fail_b, child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[fail_a.node_id or '']['status'] == 'FAILED'
        assert rows[fail_b.node_id or '']['status'] == 'FAILED'
        assert rows[child.node_id or '']['status'] == 'SKIPPED'
        assert rows[child.node_id or '']['sub_workflow_id'] is None


async def test_subworkflow_join_quorum_met(
    broker: PostgresBroker,
) -> None:
    ok_a = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_quorum_met_a',
        kwargs={'value': 'A'},
    )
    ok_b = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_quorum_met_b',
        kwargs={'value': 'B'},
    )
    fail_c = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_quorum_met_c_fail',
        kwargs={'error_code': 'L7_QUORUM_OPTIONAL_FAIL'},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_quorum_met_child',
        waits_for=[ok_a, ok_b, fail_c],
        join='quorum',
        min_success=2,
    )
    spec = _workflow_spec(
        'join_quorum_met',
        [ok_a, ok_b, fail_c, child],
        output=child,
        success_policy=SuccessPolicy(
            cases=[SuccessCase(required=[child])],
            optional=[fail_c],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[ok_a.node_id or '']['status'] == 'COMPLETED'
        assert rows[ok_b.node_id or '']['status'] == 'COMPLETED'
        assert rows[fail_c.node_id or '']['status'] == 'FAILED'
        assert rows[child.node_id or '']['status'] == 'COMPLETED'


async def test_subworkflow_join_quorum_unmet(
    broker: PostgresBroker,
) -> None:
    ok_a = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_quorum_unmet_a',
        kwargs={'value': 'A'},
    )
    fail_b = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_quorum_unmet_b',
        kwargs={'error_code': 'L7_QUORUM_FAIL_B'},
    )
    fail_c = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_quorum_unmet_c',
        kwargs={'error_code': 'L7_QUORUM_FAIL_C'},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_quorum_unmet_child',
        waits_for=[ok_a, fail_b, fail_c],
        join='quorum',
        min_success=2,
    )
    spec = _workflow_spec(
        'join_quorum_unmet',
        [ok_a, fail_b, fail_c, child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[ok_a.node_id or '']['status'] == 'COMPLETED'
        assert rows[fail_b.node_id or '']['status'] == 'FAILED'
        assert rows[fail_c.node_id or '']['status'] == 'FAILED'
        assert rows[child.node_id or '']['status'] == 'SKIPPED'


async def test_subworkflow_join_quorum_impossible(
    broker: PostgresBroker,
) -> None:
    fail_a = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_quorum_impossible_a',
        kwargs={'error_code': 'L7_QI_FAIL_A'},
    )
    fail_b = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_quorum_impossible_b',
        kwargs={'error_code': 'L7_QI_FAIL_B'},
    )
    slow_c = TaskNode(
        fn=wf_tasks.slow_mark_task,
        node_id='l7_quorum_impossible_slow_c',
        kwargs={'value': 'slow_c', 'delay_ms': 1_000},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_quorum_impossible_child',
        waits_for=[fail_a, fail_b, slow_c],
        join='quorum',
        min_success=2,
    )
    spec = _workflow_spec(
        'join_quorum_impossible',
        [fail_a, fail_b, slow_c, child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        skipped = await _wait_for_node_status(
            broker,
            handle.workflow_id,
            child.node_id or '',
            {'SKIPPED'},
        )
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert skipped['sub_workflow_id'] is None
        assert rows[slow_c.node_id or '']['status'] in {'ENQUEUED', 'RUNNING'}

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=20.0,
        )
        assert status == 'FAILED'


async def test_subworkflow_skip_task_fail_to_sub_skip(
    broker: PostgresBroker,
) -> None:
    source = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_skip_source_task',
        kwargs={'error_code': 'L7_TASK_FAILS_BEFORE_SUB'},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_skip_sub_after_task_fail',
        waits_for=[source],
    )
    spec = _workflow_spec(
        'skip_task_fail_to_sub_skip',
        [source, child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[source.node_id or '']['status'] == 'FAILED'
        assert rows[child.node_id or '']['status'] == 'SKIPPED'


async def test_subworkflow_skip_sub_fail_to_task_skip(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailWorkflow,
        node_id='l7_skip_failed_sub',
    )
    downstream = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_skip_task_after_failed_sub',
        waits_for=[child],
        kwargs={'value': 'should_skip'},
    )
    spec = _workflow_spec(
        'skip_sub_fail_to_task_skip',
        [child, downstream],
        output=downstream,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[child.node_id or '']['status'] == 'FAILED'
        assert rows[downstream.node_id or '']['status'] == 'SKIPPED'


async def test_subworkflow_skip_allow_failed_deps(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(
            wf_tasks.spec_subworkflow_allow_failed_deps_to_build_with,
        )

        status = await wait_for_workflow_completion(
            broker.session_factory,
            handle.workflow_id,
            timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[wf_tasks.node_e2e_sub_failed_input_source.node_id]['status'] == (
            'FAILED'
        )
        assert rows[wf_tasks.node_e2e_sub_failed_input_child.node_id]['status'] == (
            'COMPLETED'
        )
        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected child report, got {result}'
        payload = cast(dict[str, Any], result.unwrap())
        assert payload['input_is_err'] is True
        assert payload['input_error_code'] == 'E2E_UPSTREAM_FAIL'


async def test_subworkflow_skip_chain_skip(
    broker: PostgresBroker,
) -> None:
    source = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_chain_source_fail',
        kwargs={'error_code': 'L7_CHAIN_SOURCE_FAIL'},
    )
    first_sub = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_chain_first_sub_skipped',
        waits_for=[source],
    )
    middle = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_chain_middle_task_skipped',
        kwargs={'value': 'middle'},
        waits_for=[first_sub],
    )
    second_sub = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_chain_second_sub_skipped',
        waits_for=[middle],
    )
    spec = _workflow_spec(
        'skip_chain_skip',
        [source, first_sub, middle, second_sub],
        output=second_sub,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[source.node_id or '']['status'] == 'FAILED'
        assert rows[first_sub.node_id or '']['status'] == 'SKIPPED'
        assert rows[middle.node_id or '']['status'] == 'SKIPPED'
        assert rows[second_sub.node_id or '']['status'] == 'SKIPPED'


async def test_subworkflow_cancel_running_child_cascade(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubSlowWorkflow,
        node_id='l7_cancel_running_child',
        kwargs={'value': 'cancel_me', 'delay_ms': 2_000},
    )
    spec = _workflow_spec(
        'cancel_running_child_cascade',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        await _wait_for_workflow_status(broker, child_id, {'RUNNING'})

        cancel_result = handle.cancel()
        assert is_ok(cancel_result), f'cancel failed: {cancel_result}'
        root_status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert root_status == 'CANCELLED'
        assert await get_workflow_status(
            broker.session_factory, child_id,
        ) == 'CANCELLED'


async def test_subworkflow_cancel_before_start(
    broker: PostgresBroker,
) -> None:
    gate = TaskNode(
        fn=wf_tasks.slow_mark_task,
        node_id='l7_cancel_before_start_gate',
        kwargs={'value': 'gate', 'delay_ms': 2_000},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_cancel_before_start_child',
        waits_for=[gate],
    )
    spec = _workflow_spec(
        'cancel_before_start',
        [gate, child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        await _wait_for_node_status(
            broker, handle.workflow_id, gate.node_id or '', {'ENQUEUED', 'RUNNING'},
        )
        cancel_result = handle.cancel()
        assert is_ok(cancel_result), f'cancel failed: {cancel_result}'

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'CANCELLED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[child.node_id or '']['status'] == 'SKIPPED'
        assert rows[child.node_id or '']['sub_workflow_id'] is None


async def test_subworkflow_cancel_after_complete_noop(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_full_surface)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)

        cancel_result = handle.cancel()
        assert is_ok(cancel_result), f'cancel failed: {cancel_result}'
        assert await get_workflow_status(
            broker.session_factory, handle.workflow_id,
        ) == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)


async def test_subworkflow_cancel_nested_cascade(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubNestedSlowMiddleWorkflow,
        node_id='l7_cancel_nested_middle',
    )
    spec = _workflow_spec(
        'cancel_nested_cascade',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        middle_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )

        async def _has_leaf() -> bool:
            tree = await _workflow_tree_rows(broker, handle.workflow_id)
            return any(row['depth'] == 2 for row in tree)

        assert await _wait_until(_has_leaf, timeout_s=15.0)
        tree = await _workflow_tree_rows(broker, handle.workflow_id)
        leaf_id = next(row['id'] for row in tree if row['depth'] == 2)

        cancel_result = handle.cancel()
        assert is_ok(cancel_result), f'cancel failed: {cancel_result}'
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'CANCELLED'
        assert await get_workflow_status(broker.session_factory, middle_id) == (
            'CANCELLED'
        )
        assert await get_workflow_status(broker.session_factory, leaf_id) == (
            'CANCELLED'
        )


async def test_subworkflow_pause_running_child(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubSlowWorkflow,
        node_id='l7_pause_running_child',
        kwargs={'value': 'pause_me', 'delay_ms': 1_000},
    )
    spec = _workflow_spec(
        'pause_running_child',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        pause_result = handle.pause()
        assert is_ok(pause_result), f'pause failed: {pause_result}'
        assert pause_result.ok_value is True
        assert await get_workflow_status(
            broker.session_factory, handle.workflow_id,
        ) == 'PAUSED'
        assert await get_workflow_status(broker.session_factory, child_id) == 'PAUSED'
        assert_err(handle.get(timeout_ms=1000), expected_code='WORKFLOW_PAUSED')


async def test_subworkflow_pause_resume_completes(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubSlowWorkflow,
        node_id='l7_pause_resume_child',
        kwargs={'value': 'resume_me', 'delay_ms': 500},
    )
    spec = _workflow_spec(
        'pause_resume_completes',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        pause_result = handle.pause()
        assert is_ok(pause_result), f'pause failed: {pause_result}'
        assert pause_result.ok_value is True

        resume_result = handle.resume()
        assert is_ok(resume_result), f'resume failed: {resume_result}'
        assert resume_result.ok_value is True

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=20.0,
        )
        assert status == 'COMPLETED'
        assert await get_workflow_status(broker.session_factory, child_id) == (
            'COMPLETED'
        )
        assert_ok(handle.get(timeout_ms=2000), expected_value='resume_me')


async def test_subworkflow_pause_then_cancel(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubSlowWorkflow,
        node_id='l7_pause_then_cancel_child',
        kwargs={'value': 'pause_cancel_me', 'delay_ms': 2_000},
    )
    spec = _workflow_spec(
        'pause_then_cancel',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        pause_result = handle.pause()
        assert is_ok(pause_result), f'pause failed: {pause_result}'
        assert pause_result.ok_value is True

        cancel_result = handle.cancel()
        assert is_ok(cancel_result), f'cancel failed: {cancel_result}'
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'CANCELLED'
        assert await get_workflow_status(broker.session_factory, child_id) == (
            'CANCELLED'
        )


async def test_subworkflow_recovery_ready_unstarted(
    broker: PostgresBroker,
) -> None:
    gate = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_recovery_ready_gate',
        kwargs={'value': 'gate'},
    )
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_recovery_ready_child',
        waits_for=[gate],
    )
    spec = _workflow_spec(
        'recovery_ready_unstarted',
        [gate, child],
        output=child,
    )

    handle = start_ok_sync(spec)
    await _mark_workflow_task_completed(
        broker, handle.workflow_id, gate.node_id or '', 'gate', str,
    )
    async with broker.session_factory() as session:
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY'
                WHERE workflow_id = :wf_id AND node_id = :node_id
            """),
            {'wf_id': handle.workflow_id, 'node_id': child.node_id},
        )
        await session.commit()

    recovered = await _run_workflow_recovery(broker)
    assert recovered >= 1
    child_id = await _wait_for_subworkflow_id(
        broker, handle.workflow_id, child.node_id or '',
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert await get_workflow_status(broker.session_factory, child_id) == (
            'COMPLETED'
        )
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)


async def test_subworkflow_recovery_child_complete_parent_not_updated(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_full_surface)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

    rows = await _workflow_task_rows(broker, handle.workflow_id)
    child_id = rows[0]['sub_workflow_id']
    assert isinstance(child_id, str)
    async with broker.session_factory() as session:
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', result = NULL, error = NULL,
                    completed_at = NULL, updated_at = NOW()
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'RUNNING', result = NULL,
                    sub_workflow_summary = NULL, completed_at = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

    recovered = await _run_workflow_recovery(broker)
    assert recovered >= 1
    assert await get_workflow_status(
        broker.session_factory, handle.workflow_id,
    ) == 'COMPLETED'
    assert_ok(handle.get(timeout_ms=2000), expected_value=11)


async def test_subworkflow_recovery_parent_crashed_mid_child(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubSlowWorkflow,
        node_id='l7_recovery_mid_child',
        kwargs={'value': 'recovered_child', 'delay_ms': 1_000},
    )
    spec = _workflow_spec(
        'recovery_parent_crashed_mid_child',
        [child],
        output=child,
    )

    handle = start_ok_sync(spec)
    child_id = await _wait_for_subworkflow_id(
        broker, handle.workflow_id, child.node_id or '',
    )
    child_tasks = await _workflow_task_rows(broker, child_id)
    assert len(child_tasks) == 1
    child_task_node_id = child_tasks[0]['node_id']
    assert isinstance(child_task_node_id, str)
    await _mark_workflow_task_completed(
        broker, child_id, child_task_node_id, 'recovered_child', str,
    )

    recovered = await _run_workflow_recovery(broker)
    assert recovered >= 1
    assert await get_workflow_status(broker.session_factory, child_id) == 'COMPLETED'
    assert await get_workflow_status(
        broker.session_factory, handle.workflow_id,
    ) == 'COMPLETED'
    assert_ok(handle.get(timeout_ms=2000), expected_value='recovered_child')


async def test_subworkflow_recovery_outputless_recovery(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_outputless_success_policy)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

    async with broker.session_factory() as session:
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', result = NULL, error = NULL,
                    completed_at = NULL, updated_at = NOW()
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

    recovered = await _run_workflow_recovery(broker)
    assert recovered >= 1
    assert await get_workflow_status(
        broker.session_factory, handle.workflow_id,
    ) == 'COMPLETED'

    result = handle.get(timeout_ms=2000)
    assert result.is_ok(), f'expected recovered outputless result map, got {result}'
    terminal_results = cast(dict[str, TaskResult[Any, TaskError]], result.unwrap())
    assert_ok(
        terminal_results[wf_tasks.node_e2e_sub_outputless_ok.node_id],
        expected_value='required_ok',
    )
    failed_result = terminal_results[
        wf_tasks.node_e2e_sub_outputless_fail_child.node_id
    ]
    _assert_subworkflow_error(failed_result)


async def test_subworkflow_child_retries_succeed_eventually(
    broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    counter_file = tmp_path / 'sub_retry_success.txt'
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubRetryWorkflow,
        node_id='l7_retry_success_child',
        kwargs={
            'counter_file': str(counter_file),
            'succeed_on_attempt': 2,
        },
    )
    spec = _workflow_spec(
        'child_retries_succeed_eventually',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=45.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value='succeeded_on_attempt_2')

    assert counter_file.read_text().strip() == '2'


async def test_subworkflow_child_retries_exhaust(
    broker: PostgresBroker,
    tmp_path: Path,
) -> None:
    counter_file = tmp_path / 'sub_retry_exhaust.txt'
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubRetryWorkflow,
        node_id='l7_retry_exhaust_child',
        kwargs={
            'counter_file': str(counter_file),
            'succeed_on_attempt': 10,
        },
    )
    spec = _workflow_spec(
        'child_retries_exhaust',
        [child],
        output=child,
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=75.0,
        )
        assert status == 'FAILED'
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        _assert_subworkflow_error(handle.get(timeout_ms=2000), child_id=child_id)

    assert counter_file.read_text().strip() == '4'


async def test_subworkflow_child_good_until_expired(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubExpiredWorkflow,
        node_id='l7_good_until_expired_child',
        kwargs={'expires_in_ms': -100},
    )
    spec = _workflow_spec(
        'child_good_until_expired',
        [child],
        output=child,
    )

    handle = start_ok_sync(spec)
    child_id = await _wait_for_subworkflow_id(
        broker, handle.workflow_id, child.node_id or '',
    )
    expired = await broker.expire_pending_tasks()
    assert is_ok(expired), f'expire_pending_tasks failed: {expired}'
    assert expired.ok_value >= 1

    recovered = await _run_workflow_recovery(broker)
    assert recovered >= 1
    assert await get_workflow_status(broker.session_factory, child_id) == 'FAILED'
    assert await get_workflow_status(
        broker.session_factory, handle.workflow_id,
    ) == 'FAILED'
    child_rows = await _workflow_task_rows(broker, child_id)
    assert child_rows[0]['status'] == 'FAILED'
    parent_rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
    parent_result = _decode_task_result_column(
        parent_rows[child.node_id or '']['result'],
        str,
    )
    _assert_subworkflow_error(parent_result, child_id=child_id)


async def test_subworkflow_success_policy_required_fail_workflow_fail(
    broker: PostgresBroker,
) -> None:
    child = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailWorkflow,
        node_id='l7_policy_required_fail_child',
    )
    spec = _workflow_spec(
        'success_policy_required_fail_workflow_fail',
        [child],
        output=child,
        success_policy=SuccessPolicy(cases=[SuccessCase(required=[child])]),
    )

    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, child.node_id or '',
        )
        _assert_subworkflow_error(handle.get(timeout_ms=2000), child_id=child_id)


async def test_subworkflow_success_policy_optional_fail_workflow_complete(
    broker: PostgresBroker,
) -> None:
    required = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_policy_optional_required',
        kwargs={'value': 'required_ok'},
    )
    optional = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailWorkflow,
        node_id='l7_policy_optional_failed_sub',
    )
    spec = _workflow_spec(
        'success_policy_optional_fail_workflow_complete',
        [required, optional],
        output=required,
        success_policy=SuccessPolicy(
            cases=[SuccessCase(required=[required])],
            optional=[optional],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value='required_ok')
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[optional.node_id or '']['status'] == 'FAILED'


async def test_subworkflow_success_policy_multi_case_any(
    broker: PostgresBroker,
) -> None:
    case_fail = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailWorkflow,
        node_id='l7_policy_case_fail_sub',
    )
    case_ok = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_policy_case_ok_sub',
    )
    spec = _workflow_spec(
        'success_policy_multi_case_any',
        [case_fail, case_ok],
        output=case_ok,
        success_policy=SuccessPolicy(
            cases=[
                SuccessCase(required=[case_fail]),
                SuccessCase(required=[case_ok]),
            ],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value=11)
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        assert rows[case_fail.node_id or '']['status'] == 'FAILED'
        assert rows[case_ok.node_id or '']['status'] == 'COMPLETED'


async def test_subworkflow_success_policy_mixed_required_optional(
    broker: PostgresBroker,
) -> None:
    required_sub = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_policy_mixed_required_sub',
    )
    required_task = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_policy_mixed_required_task',
        kwargs={'value': 'task_ok'},
    )
    optional_sub = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailWorkflow,
        node_id='l7_policy_mixed_optional_sub',
    )
    spec = _workflow_spec(
        'success_policy_mixed_required_optional',
        [required_sub, required_task, optional_sub],
        output=required_task,
        success_policy=SuccessPolicy(
            cases=[SuccessCase(required=[required_sub, required_task])],
            optional=[optional_sub],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=3, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'
        assert_ok(handle.get(timeout_ms=2000), expected_value='task_ok')

        results = _unwrap_handle_result(handle.results())
        assert_ok(results[required_sub.node_id or ''], expected_value=11)
        optional_child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, optional_sub.node_id or '',
        )
        _assert_subworkflow_error(
            results[optional_sub.node_id or ''],
            child_id=optional_child_id,
        )


async def test_subworkflow_reconstructed_handle_status(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_full_surface)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

    reconstructed: WorkflowHandle[int] = WorkflowHandle(
        workflow_id=handle.workflow_id,
        broker=broker,
        out_type=int,
    )
    reconstructed_status = _unwrap_handle_result(reconstructed.status())
    assert reconstructed_status == WorkflowStatus.COMPLETED
    assert_ok(reconstructed.get(timeout_ms=2000), expected_value=11)


async def test_subworkflow_reconstructed_handle_tasks(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_full_surface)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

    reconstructed: WorkflowHandle[int] = WorkflowHandle(
        workflow_id=handle.workflow_id,
        broker=broker,
        out_type=int,
    )
    task_infos = _unwrap_handle_result(reconstructed.tasks())
    assert len(task_infos) == 1
    task_info = task_infos[0]
    assert task_info.status == WorkflowTaskStatus.COMPLETED
    assert isinstance(task_info.sub_workflow_id, str)
    assert task_info.sub_workflow_summary is not None
    assert task_info.sub_workflow_summary.status == WorkflowStatus.COMPLETED
    assert task_info.result is not None
    assert_ok(task_info.result, expected_value=11)


async def test_subworkflow_reconstructed_handle_results(
    broker: PostgresBroker,
) -> None:
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = start_ok_sync(wf_tasks.spec_subworkflow_success_full_surface)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

    reconstructed: WorkflowHandle[int] = WorkflowHandle(
        workflow_id=handle.workflow_id,
        broker=broker,
        out_type=int,
    )
    results = _unwrap_handle_result(reconstructed.results())
    assert set(results) == {wf_tasks.node_e2e_sub_success_child.node_id}
    assert_ok(
        results[wf_tasks.node_e2e_sub_success_child.node_id],
        expected_value=11,
    )


async def test_subworkflow_deep_outputless_nested_failed_optional(
    broker: PostgresBroker,
) -> None:
    required = TaskNode(
        fn=wf_tasks.mark_task,
        node_id='l7_deep_outputless_required',
        kwargs={'value': 'required_ok'},
    )
    nested_fail = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubNestedFailMiddleWorkflow,
        node_id='l7_deep_outputless_nested_fail',
    )
    spec = _workflow_spec(
        'deep_outputless_nested_failed_optional',
        [required, nested_fail],
        success_policy=SuccessPolicy(
            cases=[SuccessCase(required=[required])],
            optional=[nested_fail],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected outputless result map, got {result}'
        terminal_results = cast(dict[str, TaskResult[Any, TaskError]], result.unwrap())
        assert_ok(
            terminal_results[required.node_id or ''],
            expected_value='required_ok',
        )
        nested_child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, nested_fail.node_id or '',
        )
        _assert_subworkflow_error(
            terminal_results[nested_fail.node_id or ''],
            child_id=nested_child_id,
        )
        tree = await _workflow_tree_rows(broker, handle.workflow_id)
        assert any(row['depth'] == 2 for row in tree)


async def test_subworkflow_deep_outputless_all_failed_required(
    broker: PostgresBroker,
) -> None:
    fail_a = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailWorkflow,
        node_id='l7_deep_all_failed_a',
    )
    fail_b = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubNestedFailMiddleWorkflow,
        node_id='l7_deep_all_failed_b',
    )
    spec = _workflow_spec(
        'deep_outputless_all_failed_required',
        [fail_a, fail_b],
        success_policy=SuccessPolicy(
            cases=[SuccessCase(required=[fail_a, fail_b])],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'
        rows = _rows_by_node(await _workflow_task_rows(broker, handle.workflow_id))
        for node in (fail_a, fail_b):
            row = rows[node.node_id or '']
            assert row['status'] == 'FAILED'
            child_id = row['sub_workflow_id']
            assert isinstance(child_id, str)
            _assert_subworkflow_error(
                _decode_task_result_column(row['result'], int),
                child_id=child_id,
            )
        assert isinstance(handle.get(timeout_ms=2000).err, SubWorkflowError)


async def test_subworkflow_deep_mixed_paths(
    broker: PostgresBroker,
) -> None:
    ok_sub = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubOkWorkflow,
        node_id='l7_deep_mixed_ok_sub',
    )
    failed_source = TaskNode(
        fn=wf_tasks.fail_task,
        node_id='l7_deep_mixed_failed_source',
        kwargs={'error_code': 'L7_DEEP_MIXED_SOURCE_FAIL'},
    )
    allow_failed_sub = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubFailedInputWorkflow,
        node_id='l7_deep_mixed_allow_failed_sub',
        waits_for=[failed_source],
        args_from={'value': failed_source},
        allow_failed_deps=True,
    )
    nested_optional_fail = SubWorkflowNode(
        workflow_def=wf_tasks.E2ESubNestedFailMiddleWorkflow,
        node_id='l7_deep_mixed_nested_optional_fail',
    )
    spec = _workflow_spec(
        'deep_mixed_paths',
        [ok_sub, failed_source, allow_failed_sub, nested_optional_fail],
        success_policy=SuccessPolicy(
            cases=[SuccessCase(required=[ok_sub, allow_failed_sub])],
            optional=[failed_source, nested_optional_fail],
        ),
    )

    with run_worker(DEFAULT_INSTANCE, processes=4, ready_check=_make_ready_check()):
        handle = start_ok_sync(spec)
        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=20.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=2000)
        assert result.is_ok(), f'expected outputless result map, got {result}'
        terminal_results = cast(dict[str, TaskResult[Any, TaskError]], result.unwrap())
        assert_ok(terminal_results[ok_sub.node_id or ''], expected_value=11)

        report = terminal_results[allow_failed_sub.node_id or '']
        assert report.is_ok(), f'expected allow-failed report, got {report}'
        payload = cast(dict[str, Any], report.unwrap())
        assert payload['input_is_err'] is True
        assert payload['input_error_code'] == 'L7_DEEP_MIXED_SOURCE_FAIL'

        nested_child_id = await _wait_for_subworkflow_id(
            broker, handle.workflow_id, nested_optional_fail.node_id or '',
        )
        _assert_subworkflow_error(
            terminal_results[nested_optional_fail.node_id or ''],
            child_id=nested_child_id,
        )
