"""Workflow task definitions for e2e tests (Layer 4-6).

IMPORTANT: TaskNode and WorkflowSpec instances MUST be at module scope
for the worker process to resolve TaskNode.index correctly.
"""

from __future__ import annotations

import time
from typing import Any

from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import TaskNode, SuccessPolicy, SuccessCase

from tests.e2e.tasks.instance import app


# =============================================================================
# Task Definitions
# =============================================================================


@app.task(task_name='e2e_wf_step')
def step_task(step: str) -> TaskResult[str, TaskError]:
    """Simple workflow step that returns the step name."""
    # Minimum 50ms execution to ensure distinguishable timestamps
    time.sleep(0.05)
    return TaskResult(ok=f'completed_{step}')


@app.task(task_name='e2e_wf_slow_step')
def slow_step_task(step: str, delay_ms: int = 500) -> TaskResult[str, TaskError]:
    """Workflow step with configurable delay for timing tests."""
    time.sleep(delay_ms / 1000)
    return TaskResult(ok=f'completed_{step}')


@app.task(task_name='e2e_wf_final_result')
def final_result_task() -> TaskResult[dict[str, Any], TaskError]:
    """Task returning a dict result for output task tests."""
    time.sleep(0.05)
    return TaskResult(ok={'final': 'result', 'count': 42})


@app.task(task_name='e2e_wf_fail')
def fail_task(error_code: str) -> TaskResult[Any, TaskError]:
    """Always fails with the specified error code."""
    time.sleep(0.05)
    return TaskResult(
        err=TaskError(error_code=error_code, message=f'Failed: {error_code}')
    )


# =============================================================================
# L4.1: Linear Chain (A → B → C)
# =============================================================================

node_linear_a = TaskNode(fn=step_task, args=('A',))
node_linear_b = TaskNode(fn=step_task, args=('B',), waits_for=[node_linear_a])
node_linear_c = TaskNode(fn=step_task, args=('C',), waits_for=[node_linear_b])

spec_linear = app.workflow(
    name='e2e_linear',
    tasks=[node_linear_a, node_linear_b, node_linear_c],
)


# =============================================================================
# L4.2: Fan-Out (root → B, C, D)
# =============================================================================

node_fanout_root = TaskNode(fn=step_task, args=('root',))
node_fanout_b = TaskNode(
    fn=slow_step_task,
    args=('B',),
    kwargs={'delay_ms': 500},
    waits_for=[node_fanout_root],
)
node_fanout_c = TaskNode(
    fn=slow_step_task,
    args=('C',),
    kwargs={'delay_ms': 500},
    waits_for=[node_fanout_root],
)
node_fanout_d = TaskNode(
    fn=slow_step_task,
    args=('D',),
    kwargs={'delay_ms': 500},
    waits_for=[node_fanout_root],
)

spec_fanout = app.workflow(
    name='e2e_fanout',
    tasks=[node_fanout_root, node_fanout_b, node_fanout_c, node_fanout_d],
)


# =============================================================================
# L4.3: Fan-In (A, B, C → AGG)
# =============================================================================

node_fanin_a = TaskNode(fn=slow_step_task, args=('A',), kwargs={'delay_ms': 200})
node_fanin_b = TaskNode(fn=slow_step_task, args=('B',), kwargs={'delay_ms': 400})
node_fanin_c = TaskNode(fn=slow_step_task, args=('C',), kwargs={'delay_ms': 600})
node_fanin_agg = TaskNode(
    fn=step_task, args=('AGG',), waits_for=[node_fanin_a, node_fanin_b, node_fanin_c]
)

spec_fanin = app.workflow(
    name='e2e_fanin',
    tasks=[node_fanin_a, node_fanin_b, node_fanin_c, node_fanin_agg],
)


# =============================================================================
# L4.4: Diamond (A → B, C → D)
# =============================================================================

node_diamond_a = TaskNode(fn=step_task, args=('A',))
node_diamond_b = TaskNode(
    fn=slow_step_task,
    args=('B',),
    kwargs={'delay_ms': 200},
    waits_for=[node_diamond_a],
)
node_diamond_c = TaskNode(
    fn=slow_step_task,
    args=('C',),
    kwargs={'delay_ms': 400},
    waits_for=[node_diamond_a],
)
node_diamond_d = TaskNode(
    fn=step_task, args=('D',), waits_for=[node_diamond_b, node_diamond_c]
)

spec_diamond = app.workflow(
    name='e2e_diamond',
    tasks=[node_diamond_a, node_diamond_b, node_diamond_c, node_diamond_d],
)


# =============================================================================
# L4.5: Complex DAG
#
#     A
#    /|\
#   B C D
#   |X|
#   E F
#    \|
#     G
# =============================================================================

node_complex_a = TaskNode(fn=step_task, args=('A',))
node_complex_b = TaskNode(fn=step_task, args=('B',), waits_for=[node_complex_a])
node_complex_c = TaskNode(fn=step_task, args=('C',), waits_for=[node_complex_a])
node_complex_d = TaskNode(fn=step_task, args=('D',), waits_for=[node_complex_a])
node_complex_e = TaskNode(
    fn=step_task, args=('E',), waits_for=[node_complex_b, node_complex_c]
)
node_complex_f = TaskNode(
    fn=step_task, args=('F',), waits_for=[node_complex_c, node_complex_d]
)
node_complex_g = TaskNode(
    fn=step_task, args=('G',), waits_for=[node_complex_e, node_complex_f]
)

spec_complex = app.workflow(
    name='e2e_complex_dag',
    tasks=[
        node_complex_a,
        node_complex_b,
        node_complex_c,
        node_complex_d,
        node_complex_e,
        node_complex_f,
        node_complex_g,
    ],
)


# =============================================================================
# L4.6: Output Task (explicit output vs default terminal results)
# =============================================================================

node_output_step1 = TaskNode(fn=step_task, args=('step1',))
node_output_step2 = TaskNode(
    fn=step_task, args=('step2',), waits_for=[node_output_step1]
)
node_output_final = TaskNode(fn=final_result_task, waits_for=[node_output_step2])

spec_output = app.workflow(
    name='e2e_output',
    tasks=[node_output_step1, node_output_step2, node_output_final],
    output=node_output_final,
)


# =============================================================================
# L4.8: Single-Node Workflow (boundary)
# =============================================================================

node_single = TaskNode(fn=step_task, args=('ONLY',))

spec_single_node = app.workflow(
    name='e2e_single_node',
    tasks=[node_single],
)


# =============================================================================
# L4.9: Linear Failure Cascade (A → B(fail) → C(skipped))
# =============================================================================

node_lfm_a = TaskNode(fn=step_task, args=('A',))
node_lfm_b = TaskNode(fn=fail_task, args=('MID_FAIL',), waits_for=[node_lfm_a])
node_lfm_c = TaskNode(fn=step_task, args=('C',), waits_for=[node_lfm_b])

spec_linear_fail_mid = app.workflow(
    name='e2e_linear_fail_mid',
    tasks=[node_lfm_a, node_lfm_b, node_lfm_c],
)


# =============================================================================
# L4.10: Fan-Out with One Failing Branch (root → B(fail), C, D)
# =============================================================================

node_fof_root = TaskNode(fn=step_task, args=('root',))
node_fof_b = TaskNode(
    fn=fail_task, args=('BRANCH_FAIL',), waits_for=[node_fof_root],
)
node_fof_c = TaskNode(
    fn=slow_step_task, args=('C',), kwargs={'delay_ms': 300},
    waits_for=[node_fof_root],
)
node_fof_d = TaskNode(
    fn=slow_step_task, args=('D',), kwargs={'delay_ms': 300},
    waits_for=[node_fof_root],
)

spec_fanout_one_fail = app.workflow(
    name='e2e_fanout_one_fail',
    tasks=[node_fof_root, node_fof_b, node_fof_c, node_fof_d],
)


# =============================================================================
# L4.11: Fan-In with Failing Dep (A(fail), B, C → AGG(skipped))
# =============================================================================

node_fif_a = TaskNode(fn=fail_task, args=('FI_FAIL',))
node_fif_b = TaskNode(fn=slow_step_task, args=('B',), kwargs={'delay_ms': 200})
node_fif_c = TaskNode(fn=slow_step_task, args=('C',), kwargs={'delay_ms': 200})
node_fif_agg = TaskNode(
    fn=step_task, args=('AGG',),
    waits_for=[node_fif_a, node_fif_b, node_fif_c],
)

spec_fanin_one_fail = app.workflow(
    name='e2e_fanin_one_fail',
    tasks=[node_fif_a, node_fif_b, node_fif_c, node_fif_agg],
)


# =============================================================================
# L4.12-13: Slow Linear (cancellation + timeout tests)
# =============================================================================

node_slow_a = TaskNode(fn=slow_step_task, args=('A',), kwargs={'delay_ms': 200})
node_slow_b = TaskNode(
    fn=slow_step_task, args=('B',), kwargs={'delay_ms': 1000},
    waits_for=[node_slow_a],
)
node_slow_c = TaskNode(
    fn=slow_step_task, args=('C',), kwargs={'delay_ms': 1000},
    waits_for=[node_slow_b],
)

spec_slow_linear = app.workflow(
    name='e2e_slow_linear',
    tasks=[node_slow_a, node_slow_b, node_slow_c],
)


# =============================================================================
# L4.7: Task Expiry (good_until)
# =============================================================================
# Note: These specs are created dynamically in tests since good_until
# requires absolute timestamps calculated at test runtime.
# See test_layer4_workflow_structure.py for test_workflow_task_expires_before_claim


# =============================================================================
# L5: Workflow Data Flow Tasks
# =============================================================================

from horsies.core.models.workflow import WorkflowContext


@app.task(task_name='e2e_wf_produce_int')
def produce_int_task(value: int) -> TaskResult[int, TaskError]:
    """Produces an integer value."""
    time.sleep(0.05)
    return TaskResult(ok=value)


@app.task(task_name='e2e_wf_double')
def double_task(input_result: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
    """Doubles the input value from args_from injection."""
    time.sleep(0.05)
    if input_result.is_err():
        return TaskResult(err=input_result.err_value)
    return TaskResult(ok=input_result.unwrap() * 2)


@app.task(task_name='e2e_wf_sum_two')
def sum_two_task(
    first: TaskResult[int, TaskError],
    second: TaskResult[int, TaskError],
) -> TaskResult[int, TaskError]:
    """Sums two values from args_from injection."""
    time.sleep(0.05)
    if first.is_err():
        return TaskResult(err=first.err_value)
    if second.is_err():
        return TaskResult(err=second.err_value)
    return TaskResult(ok=first.unwrap() + second.unwrap())


@app.task(task_name='e2e_wf_fail_with')
def fail_with_task(error_code: str) -> TaskResult[Any, TaskError]:
    """Returns an error with the specified code."""
    time.sleep(0.05)
    return TaskResult(
        err=TaskError(error_code=error_code, message=f'Failed with {error_code}')
    )


@app.task(task_name='e2e_wf_ctx_reader')
def ctx_reader_task(
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[int, TaskError]:
    """Reads integer value from workflow context."""
    time.sleep(0.05)
    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(error_code='NO_CTX', message='WorkflowContext not provided')
        )
    result = workflow_ctx.result_for(node_ctx_a.key())
    if result.is_ok():
        return TaskResult(ok=result.unwrap())
    return TaskResult(err=result.err_value)


@app.task(task_name='e2e_wf_ctx_reader_str')
def ctx_reader_str_task(
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[str, TaskError]:
    """Reads string value from workflow context."""
    time.sleep(0.05)
    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(error_code='NO_CTX', message='WorkflowContext not provided')
        )
    for node_id in workflow_ctx._results_by_id:
        result = workflow_ctx._results_by_id[node_id]
        if hasattr(result, 'is_ok') and result.is_ok():
            return TaskResult(ok=str(result.unwrap()))
        elif hasattr(result, 'is_err') and result.is_err():
            return TaskResult(err=result.err_value)
    return TaskResult(
        err=TaskError(error_code='NO_RESULT', message='No results in context')
    )


@app.task(task_name='e2e_wf_ctx_sum')
def ctx_sum_task(
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[int, TaskError]:
    """Sums all integer results from workflow context."""
    time.sleep(0.05)
    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(error_code='NO_CTX', message='WorkflowContext not provided')
        )
    first = workflow_ctx.result_for(node_ctx_multi_a.key())
    second = workflow_ctx.result_for(node_ctx_multi_b.key())
    if first.is_err():
        return TaskResult(err=first.err_value)
    if second.is_err():
        return TaskResult(err=second.err_value)
    return TaskResult(ok=first.unwrap() + second.unwrap())


@app.task(task_name='e2e_wf_check_upstream_skipped')
def check_upstream_skipped_task(
    input_result: TaskResult[Any, TaskError],
) -> TaskResult[str, TaskError]:
    """Checks if input is UPSTREAM_SKIPPED error."""
    time.sleep(0.05)
    if input_result.is_err():
        err = input_result.err
        if err is not None and err.error_code == 'UPSTREAM_SKIPPED':
            return TaskResult(ok='received_upstream_skipped')
        return TaskResult(ok=f"received_error_{err.error_code if err else 'unknown'}")
    return TaskResult(ok=f'received_value_{input_result.unwrap()}')


@app.task(task_name='e2e_wf_passthrough')
def passthrough_task(value: int) -> TaskResult[int, TaskError]:
    """Simple passthrough that returns the input value."""
    time.sleep(0.05)
    return TaskResult(ok=value)


@app.task(task_name='e2e_wf_mixed_reader')
def mixed_reader_task(
    from_args: TaskResult[int, TaskError],
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[dict[str, int], TaskError]:
    """Reads from both args_from and workflow_ctx."""
    time.sleep(0.05)
    result: dict[str, int] = {}

    # Get value from args_from
    if from_args.is_err():
        return TaskResult(err=from_args.err_value)
    result['from_args'] = from_args.unwrap()

    # Get value from workflow_ctx
    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(error_code='NO_CTX', message='WorkflowContext not provided')
        )

    for node_id in workflow_ctx._results_by_id:
        ctx_result = workflow_ctx._results_by_id[node_id]
        if hasattr(ctx_result, 'is_ok') and ctx_result.is_ok():
            result['from_ctx'] = ctx_result.unwrap()
            break

    return TaskResult(ok=result)


@app.task(task_name='e2e_wf_produce_dict')
def produce_dict_task(value: str) -> TaskResult[dict[str, Any], TaskError]:
    """Produces a dict with nested structure."""
    time.sleep(0.05)
    return TaskResult(ok={'key': value, 'count': 42, 'nested': {'inner': True}})


@app.task(task_name='e2e_wf_receive_dict')
def receive_dict_task(
    input_result: TaskResult[dict[str, Any], TaskError],
) -> TaskResult[dict[str, Any], TaskError]:
    """Receives dict via args_from and returns it unchanged."""
    time.sleep(0.05)
    if input_result.is_err():
        return TaskResult(err=input_result.err_value)
    return TaskResult(ok=input_result.unwrap())


@app.task(task_name='e2e_wf_dual_reader')
def dual_reader_task(
    from_args: TaskResult[int, TaskError],
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[dict[str, int], TaskError]:
    """Reads same node's result via both args_from and workflow_ctx."""
    time.sleep(0.05)
    if from_args.is_err():
        return TaskResult(err=from_args.err_value)
    args_value = from_args.unwrap()

    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(error_code='NO_CTX', message='WorkflowContext not provided'),
        )
    ctx_result = workflow_ctx.result_for(node_dual_a.key())
    if ctx_result.is_err():
        return TaskResult(err=ctx_result.err_value)

    return TaskResult(ok={'from_args': args_value, 'from_ctx': ctx_result.unwrap()})


# =============================================================================
# L5.1: args_from single dependency (A → B)
# =============================================================================

node_df_a = TaskNode(fn=produce_int_task, args=(5,))
node_df_b = TaskNode(
    fn=double_task,
    node_id='e2e_wf_double',
    waits_for=[node_df_a],
    args_from={'input_result': node_df_a},
)

spec_args_from_single = app.workflow(
    name='e2e_args_from_single',
    tasks=[node_df_a, node_df_b],
)


# =============================================================================
# L5.2: args_from multiple dependencies (A, B → C)
# =============================================================================

node_df_multi_a = TaskNode(fn=produce_int_task, args=(10,))
node_df_multi_b = TaskNode(fn=produce_int_task, args=(20,))
node_df_multi_c = TaskNode(
    fn=sum_two_task,
    node_id='e2e_wf_sum_two',
    waits_for=[node_df_multi_a, node_df_multi_b],
    args_from={'first': node_df_multi_a, 'second': node_df_multi_b},
)

spec_args_from_multi = app.workflow(
    name='e2e_args_from_multi',
    tasks=[node_df_multi_a, node_df_multi_b, node_df_multi_c],
)


# =============================================================================
# L5.3: workflow_ctx result_for (A → B)
# =============================================================================

node_ctx_a = TaskNode(fn=produce_int_task, args=(42,))
node_ctx_b = TaskNode(
    fn=ctx_reader_task,
    node_id='e2e_wf_ctx_reader',
    waits_for=[node_ctx_a],
    workflow_ctx_from=[node_ctx_a],
)

spec_ctx_single = app.workflow(
    name='e2e_ctx_single',
    tasks=[node_ctx_a, node_ctx_b],
)


# =============================================================================
# L5.4: workflow_ctx with multiple deps (A, B → C)
# =============================================================================

node_ctx_multi_a = TaskNode(fn=produce_int_task, args=(100,))
node_ctx_multi_b = TaskNode(fn=produce_int_task, args=(200,))
node_ctx_multi_c = TaskNode(
    fn=ctx_sum_task,
    node_id='e2e_wf_ctx_sum',
    waits_for=[node_ctx_multi_a, node_ctx_multi_b],
    workflow_ctx_from=[node_ctx_multi_a, node_ctx_multi_b],
)

spec_ctx_multi = app.workflow(
    name='e2e_ctx_multi',
    tasks=[node_ctx_multi_a, node_ctx_multi_b, node_ctx_multi_c],
)


# =============================================================================
# L5.5: FAILED propagation (A fails → B with allow_failed_deps)
# =============================================================================

node_failed_a = TaskNode(fn=fail_with_task, args=('DELIBERATE_FAIL',))
node_failed_b = TaskNode(
    fn=check_upstream_skipped_task,  # Use task that returns success even with error input
    node_id='e2e_wf_check_upstream_skipped',
    waits_for=[node_failed_a],
    args_from={'input_result': node_failed_a},
    allow_failed_deps=True,
)

spec_failed_propagation = app.workflow(
    name='e2e_failed_propagation',
    tasks=[node_failed_a, node_failed_b],
    # Use success_policy: workflow succeeds if terminal task B completes
    success_policy=SuccessPolicy(
        cases=[SuccessCase(required=[node_failed_b])],
        optional=[node_failed_a],  # A is expected to fail
    ),
)


# =============================================================================
# L5.6: SKIPPED propagation (A fails → B skipped → C sees UPSTREAM_SKIPPED)
# =============================================================================

node_skipped_a = TaskNode(fn=fail_with_task, args=('CAUSE_SKIP',))
node_skipped_b = TaskNode(
    fn=passthrough_task,
    args=(999,),
    waits_for=[node_skipped_a],
    # allow_failed_deps=False (default) means B will be SKIPPED when A fails
)
node_skipped_c = TaskNode(
    fn=check_upstream_skipped_task,
    node_id='e2e_wf_check_upstream_skipped',
    waits_for=[node_skipped_b],
    args_from={'input_result': node_skipped_b},
    allow_failed_deps=True,
)

spec_skipped_propagation = app.workflow(
    name='e2e_skipped_propagation',
    tasks=[node_skipped_a, node_skipped_b, node_skipped_c],
    # Use success_policy: workflow succeeds if terminal task C completes
    success_policy=SuccessPolicy(
        cases=[SuccessCase(required=[node_skipped_c])],
        optional=[node_skipped_a, node_skipped_b],  # A fails, B gets skipped
    ),
)


# =============================================================================
# L5.7: Join + ctx deps terminal gating (A, B → C with join=any, ctx from B)
# =============================================================================

node_join_ctx_a = TaskNode(fn=slow_step_task, args=('A',), kwargs={'delay_ms': 100})
node_join_ctx_b = TaskNode(fn=slow_step_task, args=('B',), kwargs={'delay_ms': 300})
node_join_ctx_c = TaskNode(
    fn=ctx_reader_str_task,  # Use string reader since slow_step_task returns strings
    node_id='e2e_wf_ctx_reader_str',
    waits_for=[node_join_ctx_a, node_join_ctx_b],
    workflow_ctx_from=[node_join_ctx_b],  # C needs B's result
    join='any',
)

spec_join_ctx_gating = app.workflow(
    name='e2e_join_ctx_gating',
    tasks=[node_join_ctx_a, node_join_ctx_b, node_join_ctx_c],
)


# =============================================================================
# L5.8: Complex fan-out with mixed args_from and ctx
#       A → {B, C} → D (D uses args_from from B and ctx for C)
# =============================================================================

node_mixed_a = TaskNode(fn=produce_int_task, args=(7,))
node_mixed_b = TaskNode(
    fn=double_task,
    waits_for=[node_mixed_a],
    args_from={'input_result': node_mixed_a},
)
node_mixed_c = TaskNode(
    fn=produce_int_task,
    args=(3,),
    waits_for=[node_mixed_a],
)
node_mixed_d = TaskNode(
    fn=mixed_reader_task,
    node_id='e2e_wf_mixed_reader',
    waits_for=[node_mixed_b, node_mixed_c],
    args_from={'from_args': node_mixed_b},
    workflow_ctx_from=[node_mixed_c],
)

spec_mixed_dataflow = app.workflow(
    name='e2e_mixed_dataflow',
    tasks=[node_mixed_a, node_mixed_b, node_mixed_c, node_mixed_d],
)


# =============================================================================
# L5.9: Transitive args_from chain (A → B → C)
# =============================================================================

node_chain_a = TaskNode(fn=produce_int_task, args=(3,))
node_chain_b = TaskNode(
    fn=double_task,
    waits_for=[node_chain_a],
    args_from={'input_result': node_chain_a},
)
node_chain_c = TaskNode(
    fn=double_task,
    node_id='e2e_wf_double_chain',
    waits_for=[node_chain_b],
    args_from={'input_result': node_chain_b},
)

spec_args_from_chain = app.workflow(
    name='e2e_args_from_chain',
    tasks=[node_chain_a, node_chain_b, node_chain_c],
)


# =============================================================================
# L5.10: Failed dep with args_from, no success_policy (workflow FAILED)
# =============================================================================

node_fail_np_a = TaskNode(fn=fail_with_task, args=('DATAFLOW_FAIL',))
node_fail_np_b = TaskNode(
    fn=double_task,
    waits_for=[node_fail_np_a],
    args_from={'input_result': node_fail_np_a},
    # allow_failed_deps=False (default): B should be SKIPPED
)

spec_args_from_fail_workflow = app.workflow(
    name='e2e_args_from_fail_workflow',
    tasks=[node_fail_np_a, node_fail_np_b],
)


# =============================================================================
# L5.11: args_from with dict result (serialization round-trip)
# =============================================================================

node_dict_a = TaskNode(fn=produce_dict_task, args=('test_value',))
node_dict_b = TaskNode(
    fn=receive_dict_task,
    node_id='e2e_wf_receive_dict',
    waits_for=[node_dict_a],
    args_from={'input_result': node_dict_a},
)

spec_args_from_dict = app.workflow(
    name='e2e_args_from_dict',
    tasks=[node_dict_a, node_dict_b],
)


# =============================================================================
# L5.12: Same node via both args_from and workflow_ctx (dual injection)
# =============================================================================

node_dual_a = TaskNode(fn=produce_int_task, args=(77,))
node_dual_b = TaskNode(
    fn=dual_reader_task,
    node_id='e2e_wf_dual_reader',
    waits_for=[node_dual_a],
    args_from={'from_args': node_dual_a},
    workflow_ctx_from=[node_dual_a],
)

spec_dual_injection = app.workflow(
    name='e2e_dual_injection',
    tasks=[node_dual_a, node_dual_b],
)


# =============================================================================
# L6: Workflow Advanced Semantics Tasks
# =============================================================================

import os
from horsies.core.models.workflow import SuccessPolicy, SuccessCase
from horsies.core.models.tasks import RetryPolicy


@app.task(task_name='e2e_wf_mark')
def mark_task(
    value: str,
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[str, TaskError]:
    """Returns the provided value."""
    time.sleep(0.05)
    _ = workflow_ctx
    return TaskResult(ok=value)


@app.task(task_name='e2e_wf_slow_mark')
def slow_mark_task(value: str, delay_ms: int = 200) -> TaskResult[str, TaskError]:
    """Returns value after sleeping for delay_ms."""
    time.sleep(delay_ms / 1000)
    return TaskResult(ok=value)


@app.task(
    task_name='e2e_wf_retry_then_ok',
    retry_policy=RetryPolicy.exponential(base_seconds=1, max_retries=3, auto_retry_for=['RETRY_NEEDED']),
)
def retry_then_ok_task(
    counter_file: str, succeed_on_attempt: int = 2
) -> TaskResult[str, TaskError]:
    """
    Fails until attempt number reaches succeed_on_attempt.
    Uses a file to track attempts across retries.
    """
    time.sleep(0.05)

    # Read current attempt count
    attempt = 1
    if os.path.exists(counter_file):
        with open(counter_file, 'r') as f:
            attempt = int(f.read().strip()) + 1

    # Write updated attempt count
    with open(counter_file, 'w') as f:
        f.write(str(attempt))

    if attempt < succeed_on_attempt:
        return TaskResult(
            err=TaskError(
                error_code='RETRY_NEEDED',
                message=f'Attempt {attempt}, need {succeed_on_attempt}',
            )
        )

    return TaskResult(ok=f'succeeded_on_attempt_{attempt}')


# =============================================================================
# L6.1: Join=any with ctx gating (A, B → C with join=any, ctx from B)
# Already defined above as spec_join_ctx_gating in L5.7
# =============================================================================


# =============================================================================
# L6.2: Join=quorum with ctx gating (A, B, C → D with join=quorum, ctx from C)
# =============================================================================

node_quorum_a = TaskNode(fn=slow_mark_task, args=('A',), kwargs={'delay_ms': 100})
node_quorum_b = TaskNode(fn=slow_mark_task, args=('B',), kwargs={'delay_ms': 150})
node_quorum_c = TaskNode(fn=slow_mark_task, args=('C',), kwargs={'delay_ms': 300})
node_quorum_d = TaskNode(
    fn=ctx_reader_str_task,  # Use string-aware ctx reader
    waits_for=[node_quorum_a, node_quorum_b, node_quorum_c],
    workflow_ctx_from=[node_quorum_c],  # D needs C's result for ctx
    join='quorum',
    min_success=2,
)

spec_quorum_ctx_gating = app.workflow(
    name='e2e_quorum_ctx_gating',
    tasks=[node_quorum_a, node_quorum_b, node_quorum_c, node_quorum_d],
)


# =============================================================================
# L6.3: run_when / skip_when precedence
# =============================================================================

# For conditions to work, TaskNodes must be at module scope
node_cond_a = TaskNode(fn=produce_int_task, args=(100,))

# skip_when takes priority: if skip_when returns True, task is SKIPPED
# even if run_when would return True
node_cond_b_skip = TaskNode(
    fn=mark_task,
    kwargs={'value': 'should_be_skipped'},
    waits_for=[node_cond_a],
    workflow_ctx_from=[node_cond_a],
    skip_when=lambda _: True,  # Always skip
    run_when=lambda _: True,  # Would run, but skip_when takes priority
)

# Downstream receives UPSTREAM_SKIPPED
node_cond_c = TaskNode(
    fn=check_upstream_skipped_task,
    node_id='e2e_wf_check_upstream_skipped',
    waits_for=[node_cond_b_skip],
    args_from={'input_result': node_cond_b_skip},
    allow_failed_deps=True,
)

spec_skip_when_priority = app.workflow(
    name='e2e_skip_when_priority',
    tasks=[node_cond_a, node_cond_b_skip, node_cond_c],
)

# run_when=False causes skip
node_run_when_a = TaskNode(fn=produce_int_task, args=(50,))
node_run_when_b = TaskNode(
    fn=mark_task,
    kwargs={'value': 'should_be_skipped'},
    waits_for=[node_run_when_a],
    workflow_ctx_from=[node_run_when_a],
    run_when=lambda _: False,  # Never run
)
node_run_when_c = TaskNode(
    fn=check_upstream_skipped_task,
    waits_for=[node_run_when_b],
    args_from={'input_result': node_run_when_b},
    allow_failed_deps=True,
)

spec_run_when_false = app.workflow(
    name='e2e_run_when_false',
    tasks=[node_run_when_a, node_run_when_b, node_run_when_c],
)


# =============================================================================
# L6.4-L6.5: Pause/Resume semantics
# =============================================================================

# A longer pipeline for pause testing
node_pause_a = TaskNode(fn=slow_mark_task, args=('A',), kwargs={'delay_ms': 100})
node_pause_b = TaskNode(
    fn=slow_mark_task, args=('B',), kwargs={'delay_ms': 200}, waits_for=[node_pause_a]
)
node_pause_c = TaskNode(
    fn=slow_mark_task, args=('C',), kwargs={'delay_ms': 200}, waits_for=[node_pause_b]
)
node_pause_d = TaskNode(
    fn=slow_mark_task, args=('D',), kwargs={'delay_ms': 100}, waits_for=[node_pause_c]
)

spec_pausable = app.workflow(
    name='e2e_pausable',
    tasks=[node_pause_a, node_pause_b, node_pause_c, node_pause_d],
)


# =============================================================================
# L6.6: Success policy satisfied (A required, B optional)
# =============================================================================

node_sp_a = TaskNode(fn=mark_task, args=('required_A',))
node_sp_b_fail = TaskNode(fn=fail_task, args=('OPTIONAL_FAIL',))

spec_success_policy_satisfied = app.workflow(
    name='e2e_success_policy_satisfied',
    tasks=[node_sp_a, node_sp_b_fail],
    success_policy=SuccessPolicy(
        cases=[SuccessCase(required=[node_sp_a])],
        optional=[node_sp_b_fail],
    ),
)


# =============================================================================
# L6.7: Success policy not met (A and B required, B fails)
# =============================================================================

node_sp_req_a = TaskNode(fn=mark_task, args=('required_A',))
node_sp_req_b_fail = TaskNode(fn=fail_task, args=('REQUIRED_FAIL',))

spec_success_policy_not_met = app.workflow(
    name='e2e_success_policy_not_met',
    tasks=[node_sp_req_a, node_sp_req_b_fail],
    success_policy=SuccessPolicy(
        cases=[SuccessCase(required=[node_sp_req_a, node_sp_req_b_fail])],
    ),
)


# =============================================================================
# L6.8: Multiple success cases (case1: A required, case2: B required)
# =============================================================================

node_sp_multi_a_fail = TaskNode(fn=fail_task, args=('CASE1_FAIL',))
node_sp_multi_b = TaskNode(fn=mark_task, args=('case2_ok',))

spec_success_policy_multi = app.workflow(
    name='e2e_success_policy_multi',
    tasks=[node_sp_multi_a_fail, node_sp_multi_b],
    success_policy=SuccessPolicy(
        cases=[
            SuccessCase(
                required=[node_sp_multi_a_fail]
            ),  # Case 1: A must succeed (will fail)
            SuccessCase(
                required=[node_sp_multi_b]
            ),  # Case 2: B must succeed (will pass)
        ],
    ),
)


# =============================================================================
# L6.9: Recovery test workflow
# Note: Recovery tests are more about simulating stuck states,
# done dynamically in tests
# =============================================================================


# =============================================================================
# L6.10: Workflow task retries (task with retry policy)
# Note: counter_file is passed dynamically in tests
# =============================================================================

# This spec is created dynamically in tests since counter_file varies


# =============================================================================
# L6.2b: Join=quorum impossible (A fails, B fails, C ok → D with min_success=3)
# =============================================================================

node_qi_a = TaskNode(fn=fail_task, args=('QI_FAIL_A',))
node_qi_b = TaskNode(fn=fail_task, args=('QI_FAIL_B',))
node_qi_c = TaskNode(fn=mark_task, args=('C_ok',))
node_qi_d = TaskNode(
    fn=mark_task,
    args=('D_should_skip',),
    waits_for=[node_qi_a, node_qi_b, node_qi_c],
    join='quorum',
    min_success=3,
)

spec_quorum_impossible = app.workflow(
    name='e2e_quorum_impossible',
    tasks=[node_qi_a, node_qi_b, node_qi_c, node_qi_d],
)


# =============================================================================
# L6.2c: Join=any with all deps failing (A fails, B fails → C with join=any)
# =============================================================================

node_jaf_a = TaskNode(fn=fail_task, args=('JAF_FAIL_A',))
node_jaf_b = TaskNode(fn=fail_task, args=('JAF_FAIL_B',))
node_jaf_c = TaskNode(
    fn=mark_task,
    args=('C_should_skip',),
    waits_for=[node_jaf_a, node_jaf_b],
    join='any',
)

spec_join_any_all_fail = app.workflow(
    name='e2e_join_any_all_fail',
    tasks=[node_jaf_a, node_jaf_b, node_jaf_c],
)


# =============================================================================
# L6.11: on_error=PAUSE (A → B(fail) → C, auto-pause on B failure)
# =============================================================================

from horsies.core.models.workflow import OnError

node_oep_a = TaskNode(fn=step_task, args=('A',))
node_oep_b = TaskNode(fn=fail_task, args=('PAUSE_FAIL',), waits_for=[node_oep_a])
node_oep_c = TaskNode(fn=step_task, args=('C',), waits_for=[node_oep_b])

spec_on_error_pause = app.workflow(
    name='e2e_on_error_pause',
    tasks=[node_oep_a, node_oep_b, node_oep_c],
    on_error=OnError.PAUSE,
)


# =============================================================================
# L6.3c: run_when raises exception -> task FAILED
# =============================================================================


def _raise_runtime_error(_: WorkflowContext) -> bool:
    raise RuntimeError('Condition evaluation blew up')


node_ce_a = TaskNode(fn=produce_int_task, args=(42,))
node_ce_b = TaskNode(
    fn=mark_task,
    kwargs={'value': 'should_skip'},
    waits_for=[node_ce_a],
    workflow_ctx_from=[node_ce_a],
    run_when=_raise_runtime_error,
)

spec_condition_exception = app.workflow(
    name='e2e_condition_exception',
    tasks=[node_ce_a, node_ce_b],
)


# =============================================================================
# L6.3d: run_when=True → task runs normally
# =============================================================================

node_rwt_a = TaskNode(fn=produce_int_task, args=(42,))
node_rwt_b = TaskNode(
    fn=mark_task,
    kwargs={'value': 'should_run'},
    waits_for=[node_rwt_a],
    workflow_ctx_from=[node_rwt_a],
    run_when=lambda _: True,
)

spec_run_when_true = app.workflow(
    name='e2e_run_when_true',
    tasks=[node_rwt_a, node_rwt_b],
)
