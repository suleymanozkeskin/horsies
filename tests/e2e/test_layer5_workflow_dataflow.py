"""Layer 5 e2e tests: workflow data flow (args_from, workflow_ctx, SKIPPED/FAILED propagation).

Tests verify:
- args_from wiring and serialization across worker boundaries
- WorkflowContext.result_for() in real execution
- SKIPPED vs FAILED propagation semantics
- Mixed args_from and workflow_ctx usage

All tests use DB polling to avoid event loop conflicts with PostgresListener.
"""

from __future__ import annotations

from typing import Any, Callable, cast

import pytest

from horsies.core.brokers.postgres import PostgresBroker

from horsies.core.models.tasks import TaskResult

from tests.e2e.helpers.assertions import assert_err, assert_ok
from tests.e2e.helpers.worker import run_worker
from tests.e2e.helpers.workflow import (
    get_workflow_tasks,
    wait_for_workflow_completion,
)
from tests.e2e.tasks import workflows as wf_tasks
from tests.e2e.tasks.basic import healthcheck


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'


def _unwrap_task_value(value: Any) -> Any:
    """Unwrap TaskResult value from workflow output."""
    if isinstance(value, TaskResult):
        task_result = cast(TaskResult[Any, Any], value)
        if task_result.is_ok():
            return task_result.unwrap()
        return task_result  # Return error result as-is for inspection
    return value


def _make_ready_check() -> Callable[[], bool]:
    """Create a ready check that sends healthcheck task."""
    from horsies.core.task_decorator import TaskHandle

    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = healthcheck.send()
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


# =============================================================================
# T5.1: args_from single dependency (A → B)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_args_from_single_dependency(broker: PostgresBroker) -> None:
    """T5.1: B receives A's result via args_from and computes correctly."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_args_from_single: A produces 5, B doubles it to 10
        handle = wf_tasks.spec_args_from_single.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Get final result
        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # B should have doubled A's value: 5 * 2 = 10
        output = cast(dict[str, Any], result.unwrap())
        # Terminal task is B at index 1
        for key, value in output.items():
            if 'e2e_wf_double' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 10, f'Expected doubled value 10, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find double task result in output')


# =============================================================================
# T5.2: args_from multiple dependencies (A, B → C)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_args_from_multiple_dependencies(broker: PostgresBroker) -> None:
    """T5.2: C receives A and B's results via args_from and sums correctly."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_args_from_multi: A produces 10, B produces 20, C sums to 30
        handle = wf_tasks.spec_args_from_multi.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # C should have summed A and B: 10 + 20 = 30
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_sum_two' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 30, f'Expected sum 30, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find sum_two task result in output')


# =============================================================================
# T5.3: workflow_ctx result_for (A → B)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_ctx_single_dependency(broker: PostgresBroker) -> None:
    """T5.3: B accesses A's result via workflow_ctx.result_for()."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_ctx_single: A produces 42, B reads it via workflow_ctx
        handle = wf_tasks.spec_ctx_single.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # B should have read A's value: 42
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_ctx_reader' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 42, f'Expected ctx value 42, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find ctx_reader task result in output')


# =============================================================================
# T5.4: workflow_ctx with multiple deps (A, B → C)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_ctx_multiple_dependencies(broker: PostgresBroker) -> None:
    """T5.4: C accesses both A and B via workflow_ctx and sums them."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_ctx_multi: A produces 100, B produces 200, C sums via ctx to 300
        handle = wf_tasks.spec_ctx_multi.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # C should have summed A and B via ctx: 100 + 200 = 300
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_ctx_sum' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 300, f'Expected ctx sum 300, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find ctx_sum task result in output')


# =============================================================================
# T5.5: FAILED propagation (allow_failed_deps=True)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_failed_propagation(broker: PostgresBroker) -> None:
    """T5.5: B runs despite A failure and receives original error."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_failed_propagation: A fails with DELIBERATE_FAIL, B has allow_failed_deps=True
        handle = wf_tasks.spec_failed_propagation.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Check task statuses
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        # A should be FAILED
        assert (
            db_tasks[0]['status'] == 'FAILED'
        ), f"A should be FAILED, got {db_tasks[0]['status']}"

        # B should be COMPLETED (it ran despite A's failure)
        assert (
            db_tasks[1]['status'] == 'COMPLETED'
        ), f"B should be COMPLETED, got {db_tasks[1]['status']}"

        # Verify B received A's original error (not UPSTREAM_SKIPPED)
        result = handle.get(timeout_ms=1000)
        assert_ok(result)
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_check_upstream_skipped' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 'received_error_DELIBERATE_FAIL', (
                    f'B should have received DELIBERATE_FAIL error, got {unwrapped}'
                )
                break
        else:
            pytest.fail('Could not find check_upstream_skipped task result in output')


# =============================================================================
# T5.6: SKIPPED propagation (UPSTREAM_SKIPPED sentinel)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_skipped_propagation(broker: PostgresBroker) -> None:
    """T5.6: C receives UPSTREAM_SKIPPED when B is SKIPPED due to A failure."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # spec_skipped_propagation: A fails, B is SKIPPED (no allow_failed_deps),
        # C has allow_failed_deps=True and should see UPSTREAM_SKIPPED for B
        handle = wf_tasks.spec_skipped_propagation.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Check task statuses
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        # A should be FAILED
        assert (
            db_tasks[0]['status'] == 'FAILED'
        ), f"A should be FAILED, got {db_tasks[0]['status']}"

        # B should be SKIPPED (default allow_failed_deps=False)
        assert (
            db_tasks[1]['status'] == 'SKIPPED'
        ), f"B should be SKIPPED, got {db_tasks[1]['status']}"

        # C should be COMPLETED and received UPSTREAM_SKIPPED
        assert (
            db_tasks[2]['status'] == 'COMPLETED'
        ), f"C should be COMPLETED, got {db_tasks[2]['status']}"

        # Verify C's result indicates it received UPSTREAM_SKIPPED
        result = handle.get(timeout_ms=1000)
        assert_ok(result)
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_check_upstream_skipped' in key:
                unwrapped = _unwrap_task_value(value)
                assert (
                    unwrapped == 'received_upstream_skipped'
                ), f'C should have received UPSTREAM_SKIPPED, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find check_upstream_skipped task result in output')


# =============================================================================
# T5.7: Join + ctx deps terminal gating
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_join_ctx_gating(broker: PostgresBroker) -> None:
    """T5.7: C waits for B (ctx dep) even though join=any and A completes first."""
    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        # spec_join_ctx_gating: A (100ms), B (300ms), C waits for both with join=any
        # but workflow_ctx_from=[B], so C must wait for B
        handle = wf_tasks.spec_join_ctx_gating.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        # Check task statuses
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 3

        # All should be COMPLETED
        for task in db_tasks:
            assert (
                task['status'] == 'COMPLETED'
            ), f"Task should be COMPLETED, got {task['status']}"

        # Assert ctx gating: C must start after B completes (even though join=any)
        _, b_task, c_task = db_tasks
        assert b_task['completed_at'] is not None
        assert c_task['started_at'] is not None
        assert (
            c_task['started_at'] > b_task['completed_at']
        ), 'C should start only after ctx dep B is terminal'

        # Verify C actually received B's result value via ctx
        result = handle.get(timeout_ms=1000)
        assert_ok(result)
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_ctx_reader_str' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 'completed_B', (
                    f"C should have read B's result 'completed_B', got {unwrapped}"
                )
                break
        else:
            pytest.fail('Could not find ctx_reader_str task result in output')


# =============================================================================
# T5.8: Complex fan-out with mixed args_from and ctx
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_mixed_args_from_and_ctx(broker: PostgresBroker) -> None:
    """T5.8: D receives data from both args_from (B) and workflow_ctx (C)."""
    with run_worker(DEFAULT_INSTANCE, processes=2, ready_check=_make_ready_check()):
        # spec_mixed_dataflow: A produces 7, B doubles (14), C produces 3
        # D receives B via args_from and C via workflow_ctx
        handle = wf_tasks.spec_mixed_dataflow.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # D should return {"from_args": 14, "from_ctx": 3}
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_mixed_reader' in key:
                unwrapped = _unwrap_task_value(value)
                assert isinstance(
                    unwrapped, dict
                ), f'Expected dict, got {type(unwrapped)}'
                value_dict = cast(dict[str, Any], unwrapped)
                assert (
                    value_dict.get('from_args') == 14
                ), f"Expected from_args=14 (7*2), got {value_dict.get('from_args')}"
                assert (
                    value_dict.get('from_ctx') == 3
                ), f"Expected from_ctx=3, got {value_dict.get('from_ctx')}"
                break
        else:
            pytest.fail('Could not find mixed_reader task result in output')


# =============================================================================
# T5.9: Transitive args_from chain (A → B → C)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_args_from_transitive_chain(broker: PostgresBroker) -> None:
    """T5.9: args_from result survives 2-hop serialization: A→B(double)→C(double)."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        # A produces 3, B doubles to 6, C doubles to 12
        handle = wf_tasks.spec_args_from_chain.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # C should have doubled B's doubled value: 3 * 2 * 2 = 12
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_double_chain' in key:
                unwrapped = _unwrap_task_value(value)
                assert unwrapped == 12, f'Expected 3*2*2=12, got {unwrapped}'
                break
        else:
            pytest.fail('Could not find double_chain task result in output')


# =============================================================================
# T5.10: Failed dep with args_from skips downstream, workflow FAILED
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_args_from_failed_dep_skips_and_fails_workflow(
    broker: PostgresBroker,
) -> None:
    """T5.10: A fails, B (allow_failed_deps=False) is SKIPPED, workflow FAILED."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_args_from_fail_workflow.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'FAILED'

        # Verify task statuses
        db_tasks = await get_workflow_tasks(broker.session_factory, handle.workflow_id)
        assert len(db_tasks) == 2

        assert (
            db_tasks[0]['status'] == 'FAILED'
        ), f"A should be FAILED, got {db_tasks[0]['status']}"
        assert (
            db_tasks[1]['status'] == 'SKIPPED'
        ), f"B should be SKIPPED, got {db_tasks[1]['status']}"

        # handle.get() should surface the failure error code
        result = handle.get(timeout_ms=1000)
        assert_err(result, 'DATAFLOW_FAIL')


# =============================================================================
# T5.11: args_from with dict result (serialization round-trip)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_args_from_dict_serialization(broker: PostgresBroker) -> None:
    """T5.11: Nested dict survives serialization through args_from injection."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_args_from_dict.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_receive_dict' in key:
                unwrapped = _unwrap_task_value(value)
                assert isinstance(
                    unwrapped, dict,
                ), f'Expected dict, got {type(unwrapped)}'
                value_dict = cast(dict[str, Any], unwrapped)
                assert value_dict.get('key') == 'test_value', (
                    f"Expected key='test_value', got {value_dict.get('key')}"
                )
                assert value_dict.get('count') == 42, (
                    f"Expected count=42, got {value_dict.get('count')}"
                )
                assert value_dict.get('nested') == {'inner': True}, (
                    f"Expected nested={{'inner': True}}, got {value_dict.get('nested')}"
                )
                break
        else:
            pytest.fail('Could not find receive_dict task result in output')


# =============================================================================
# T5.12: Same node via both args_from and workflow_ctx (dual injection)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_dual_injection_same_node(broker: PostgresBroker) -> None:
    """T5.12: B reads A via both args_from and workflow_ctx without interference."""
    with run_worker(DEFAULT_INSTANCE, ready_check=_make_ready_check()):
        handle = wf_tasks.spec_dual_injection.start()

        status = await wait_for_workflow_completion(
            broker.session_factory, handle.workflow_id, timeout_s=15.0,
        )
        assert status == 'COMPLETED'

        result = handle.get(timeout_ms=1000)
        assert_ok(result)

        # Both paths should deliver the same value (77)
        output = cast(dict[str, Any], result.unwrap())
        for key, value in output.items():
            if 'e2e_wf_dual_reader' in key:
                unwrapped = _unwrap_task_value(value)
                assert isinstance(
                    unwrapped, dict,
                ), f'Expected dict, got {type(unwrapped)}'
                value_dict = cast(dict[str, Any], unwrapped)
                assert value_dict.get('from_args') == 77, (
                    f"Expected from_args=77, got {value_dict.get('from_args')}"
                )
                assert value_dict.get('from_ctx') == 77, (
                    f"Expected from_ctx=77, got {value_dict.get('from_ctx')}"
                )
                break
        else:
            pytest.fail('Could not find dual_reader task result in output')
