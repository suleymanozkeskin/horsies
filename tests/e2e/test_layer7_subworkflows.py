"""Layer 7 e2e tests: subworkflow lifecycle matrix."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, cast

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.serde import loads_json
from horsies.core.codec.typed import decode_task_error, decode_task_result
from horsies.core.models.tasks import (
    OutcomeCode,
    SubWorkflowError,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    SubWorkflowSummary,
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
