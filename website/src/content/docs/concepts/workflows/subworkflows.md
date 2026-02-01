---
title: Workflows and Subworkflows
summary: Defining, composing, and running workflows with SubWorkflowNode.
related: [workflow-semantics, result-handling]
tags: [concepts, workflows, subworkflows, composition]
---

## 1. What is a Workflow?

A **workflow** is a directed acyclic graph (DAG) of tasks. Each node in the DAG is either:

- **TaskNode**: runs a single task function
- **SubWorkflowNode**: runs an entire child workflow as a composite unit

Workflows provide:
- Dependency ordering (`waits_for`)
- Data flow between tasks (`args_from`)
- Conditional execution (`run_when`, `skip_when`)
- Error handling policies (`on_error`, `allow_failed_deps`)
- Success criteria (`SuccessPolicy`)

---

## 2. Defining a Workflow

### 2.1 Class-Based Definition (Recommended)

```python
from horsies import Horsies, WorkflowDefinition, TaskNode, TaskResult, TaskError

app = Horsies()

@app.task()
def fetch_data(url: str) -> TaskResult[dict, TaskError]:
    # fetch logic...
    return TaskResult(ok={"data": "..."})

@app.task()
def transform(raw: TaskResult[dict, TaskError]) -> TaskResult[str, TaskError]:
    if raw.is_err():
        return TaskResult(err=raw.err_value)
    return TaskResult(ok=str(raw.ok_value))

@app.task()
def store(transformed: str) -> TaskResult[bool, TaskError]:
    # store logic...
    return TaskResult(ok=True)


class DataPipeline(WorkflowDefinition[bool]):
    """
    A simple ETL pipeline: fetch -> transform -> store.

    The generic parameter [bool] indicates the workflow output type.
    """
    name = "data_pipeline"

    fetch = TaskNode(fn=fetch_data, args=("https://api.example.com",))
    transform = TaskNode(
        fn=transform,
        waits_for=[fetch],
        args_from={"raw": fetch},
    )
    store = TaskNode(
        fn=store,
        waits_for=[transform],
        args_from={"transformed": transform},
    )

    class Meta:
        output = store  # Workflow result comes from this node
```

**Key points:**
- `name` is required and must be unique
- Nodes are class attributes (order doesn't matter, DAG is built from `waits_for`)
- `Meta.output` defines which node's result becomes the workflow result

### 2.2 Starting a Workflow

```python
# Build and start
spec = DataPipeline.build(app)
handle = await start_workflow_async(spec, broker)

# Wait for completion
result = await handle.get()  # TaskResult[bool, TaskError]

if result.is_ok():
    print(f"Pipeline succeeded: {result.ok_value}")
else:
    print(f"Pipeline failed: {result.err_value}")
```

### 2.3 Inline Definition (Simple Cases Only)

For simple DAGs without conditions or subworkflows:

```python
node_a = TaskNode(fn=task_a, args=(1,))
node_b = TaskNode(fn=task_b, waits_for=[node_a])

spec = app.workflow(
    name="simple_workflow",
    tasks=[node_a, node_b],
    output=node_b,
)
handle = await start_workflow_async(spec, broker)
```

**Warning**: Inline workflows are not portable for `run_when`/`skip_when` or subworkflows.
Use class-based `WorkflowDefinition` for anything beyond trivial DAGs.

---

## 3. Node Dependencies: `waits_for`

`waits_for` defines execution order. A node waits until all its dependencies reach a terminal state (COMPLETED, FAILED, or SKIPPED).

```python
class DiamondDAG(WorkflowDefinition[str]):
    name = "diamond"

    #     ┌→ B ─┐
    # A ──┤     ├→ D
    #     └→ C ─┘

    a = TaskNode(fn=task_a)
    b = TaskNode(fn=task_b, waits_for=[a])
    c = TaskNode(fn=task_c, waits_for=[a])
    d = TaskNode(fn=task_d, waits_for=[b, c])  # Waits for BOTH b and c

    class Meta:
        output = d
```

**Important**: `waits_for` means "wait for completion", not "require success".
By default, if a dependency fails, the downstream task is SKIPPED.
Use `allow_failed_deps=True` to run anyway (see Section 6).

---

## 4. Data Flow: `args_from`

`args_from` injects upstream results as keyword arguments.

```python
@app.task()
def add(a: int, b: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=a + b)

@app.task()
def multiply(
    sum_result: TaskResult[int, TaskError],
    factor: int,
) -> TaskResult[int, TaskError]:
    if sum_result.is_err():
        return TaskResult(err=sum_result.err_value)
    return TaskResult(ok=sum_result.ok_value * factor)


class MathWorkflow(WorkflowDefinition[int]):
    name = "math"

    add_node = TaskNode(fn=add, args=(5, 3))
    multiply_node = TaskNode(
        fn=multiply,
        waits_for=[add_node],
        args_from={"sum_result": add_node},  # Receives TaskResult[int, TaskError]
        kwargs={"factor": 10},
    )

    class Meta:
        output = multiply_node
```

**The injected value is always `TaskResult[T, TaskError]`**, not the raw value.
This lets you handle upstream failures explicitly.

---

## 5. Workflow Context: `workflow_ctx_from`

When a task needs access to multiple upstream results or subworkflow summaries,
use `workflow_ctx_from` instead of multiple `args_from` entries.

```python
from horsies import WorkflowContext

@app.task()
def aggregate(
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[Summary, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX", message="Missing context"))

    # Access individual results
    result_a = workflow_ctx.result_for(node_a)  # TaskResult[int, TaskError]
    result_b = workflow_ctx.result_for(node_b)  # TaskResult[str, TaskError]

    # Build summary from both
    return TaskResult(ok=Summary(
        value_a=result_a.ok_value if result_a.is_ok() else None,
        value_b=result_b.ok_value if result_b.is_ok() else None,
    ))


class AggregationWorkflow(WorkflowDefinition[Summary]):
    name = "aggregation"

    node_a = TaskNode(fn=produce_int)
    node_b = TaskNode(fn=produce_str)
    agg = TaskNode(
        fn=aggregate,
        waits_for=[node_a, node_b],
        workflow_ctx_from=[node_a, node_b],  # Both results available in context
    )

    class Meta:
        output = agg
```

**Rules:**
- Every node in `workflow_ctx_from` must also be in `waits_for`
- The task function must have `workflow_ctx: WorkflowContext | None` parameter
- Context is built fresh per task; it does not propagate downstream

---

## 6. Subworkflows: Composing Workflows

A **SubWorkflowNode** runs an entire workflow as a single node in the parent DAG.

### 6.1 Why Use Subworkflows?

| Use Subworkflows When | Use a Single DAG When |
|-----------------------|-----------------------|
| Reusing workflow logic across multiple parents | All tasks belong to one logical unit |
| Child needs its own success policy | Uniform retry/pause across all tasks |
| You want isolated retry behavior | Tight data flow without boundaries |
| You need partial success visibility (SubWorkflowSummary) | No need for nested health checks |

### 6.2 Basic Subworkflow

```python
# Child workflow
class ChildPipeline(WorkflowDefinition[int]):
    name = "child_pipeline"

    step1 = TaskNode(fn=produce_int, args=(5,))
    step2 = TaskNode(fn=double, waits_for=[step1], args_from={"value": step1})

    class Meta:
        output = step2


# Parent workflow
class ParentWorkflow(WorkflowDefinition[int]):
    name = "parent_workflow"

    child = SubWorkflowNode(workflow_def=ChildPipeline)

    class Meta:
        output = child  # Parent result = child workflow result
```

**Lifecycle mapping:**
- Child RUNNING → Parent node RUNNING
- Child COMPLETED → Parent node COMPLETED (result = child output)
- Child FAILED → Parent node FAILED (result = SubWorkflowError)

### 6.3 Parameterized Subworkflows with `build_with()`

When the child workflow needs runtime parameters:

```python
class DataProcessor(WorkflowDefinition[ProcessedData]):
    name = "data_processor"

    # These will be set by build_with()
    fetch: TaskNode[RawData]
    clean: TaskNode[CleanedData]
    process: TaskNode[ProcessedData]

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        source_url: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec:
        """Build workflow with runtime parameters."""
        fetch = TaskNode(fn=fetch_raw_data, args=(source_url,))
        clean = TaskNode(fn=clean_data, waits_for=[fetch], args_from={"raw": fetch})
        process = TaskNode(fn=process_data, waits_for=[clean], args_from={"clean": clean})

        return WorkflowSpec(
            name=cls.name,
            tasks=[fetch, clean, process],
            output=process,
            workflow_def_module=cls.__module__,
            workflow_def_qualname=cls.__qualname__,
        )

    class Meta:
        output = None  # Set dynamically in build_with


# Use in parent with kwargs
class MultiSourceAggregation(WorkflowDefinition[Report]):
    name = "multi_source"

    source1 = SubWorkflowNode(
        workflow_def=DataProcessor,
        kwargs={"source_url": "https://api1.example.com"},
    )
    source2 = SubWorkflowNode(
        workflow_def=DataProcessor,
        kwargs={"source_url": "https://api2.example.com"},
    )
    aggregate = TaskNode(
        fn=aggregate_results,
        waits_for=[source1, source2],
        args_from={"result1": source1, "result2": source2},
    )

    class Meta:
        output = aggregate
```

### 6.4 Accessing Subworkflow Health: SubWorkflowSummary

When you need visibility into child workflow internals:

```python
from horsies import WorkflowContext, SubWorkflowSummary

@app.task()
def report_health(
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[HealthReport, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX", message="Missing context"))

    # Get summary for the child subworkflow node
    summary: SubWorkflowSummary = workflow_ctx.summary_for(child_node)

    return TaskResult(ok=HealthReport(
        status=summary.status,           # WorkflowStatus (COMPLETED, FAILED, etc.)
        output=summary.output,           # Child workflow output (if completed)
        total_tasks=summary.total,       # Total tasks in child
        completed=summary.completed,     # Successfully completed tasks
        failed=summary.failed,           # Failed tasks
        skipped=summary.skipped,         # Skipped tasks
        error_summary=summary.error,     # Error details (if failed)
    ))


class MonitoredPipeline(WorkflowDefinition[HealthReport]):
    name = "monitored"

    child = SubWorkflowNode(workflow_def=ChildPipeline)
    reporter = TaskNode(
        fn=report_health,
        waits_for=[child],
        workflow_ctx_from=[child],  # Required for summary_for()
        allow_failed_deps=True,     # Run even if child fails
    )

    class Meta:
        output = reporter
```

**SubWorkflowSummary fields:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | `WorkflowStatus` | Terminal status of child workflow |
| `output` | `Any \| None` | Child workflow output (if COMPLETED) |
| `total` | `int` | Total tasks in child workflow |
| `completed` | `int` | Tasks that completed successfully |
| `failed` | `int` | Tasks that failed |
| `skipped` | `int` | Tasks that were skipped |
| `error` | `str \| None` | Error summary (if FAILED) |

### 6.5 Receiving Subworkflow Results via `args_from`

SubWorkflowNode results flow like TaskNode results:

```python
@app.task()
def process_child_result(
    child_result: TaskResult[ChildOutput, TaskError],
) -> TaskResult[ParentOutput, TaskError]:
    if child_result.is_err():
        # Child workflow failed - error is SubWorkflowError
        return TaskResult(err=child_result.err_value)

    # Child succeeded - unwrap the output
    child_output: ChildOutput = child_result.ok_value
    return TaskResult(ok=ParentOutput(derived_from=child_output))


class ParentWithDataFlow(WorkflowDefinition[ParentOutput]):
    name = "parent_data_flow"

    child = SubWorkflowNode(workflow_def=ChildPipeline)
    processor = TaskNode(
        fn=process_child_result,
        waits_for=[child],
        args_from={"child_result": child},  # Receives TaskResult[ChildOutput, TaskError]
    )

    class Meta:
        output = processor
```

---

## 7. Error Handling

### 7.1 Default Behavior: SKIP on Upstream Failure

By default, if a dependency fails, the downstream task is SKIPPED:

```
A (fails) → B (SKIPPED) → C (SKIPPED)
```

This prevents cascading execution of tasks that depend on failed data.

### 7.2 `allow_failed_deps=True`: Run Anyway

Use this for recovery tasks or aggregators that handle failures:

```python
@app.task()
def recovery_handler(
    primary_result: TaskResult[Data, TaskError],
) -> TaskResult[Data, TaskError]:
    if primary_result.is_ok():
        return primary_result  # Pass through success

    # Handle the failure - use fallback, log, alert, etc.
    error = primary_result.err_value
    return TaskResult(ok=Data(fallback=True, original_error=str(error)))


class RecoveryWorkflow(WorkflowDefinition[Data]):
    name = "recovery"

    primary = TaskNode(fn=fetch_data)
    recovery = TaskNode(
        fn=recovery_handler,
        waits_for=[primary],
        args_from={"primary_result": primary},
        allow_failed_deps=True,  # Run even if primary fails
    )

    class Meta:
        output = recovery
```

### 7.3 Workflow-Level Error Policy: `on_error`

Control what happens when any task fails:

```python
from horsies import OnError

spec = MyWorkflow.build(app)
spec = spec.with_on_error(OnError.FAIL)  # or OnError.PAUSE
```

| Policy | Behavior |
|--------|----------|
| `OnError.FAIL` (default) | Mark workflow FAILED, propagate SKIP to dependents |
| `OnError.PAUSE` | Pause workflow, wait for manual intervention |

With `OnError.PAUSE`:
- Workflow enters PAUSED state
- No new tasks are enqueued
- Already-running tasks complete
- Call `handle.resume()` to continue after fixing the issue

### 7.4 Handling SKIPPED Dependencies

When a task runs with `allow_failed_deps=True` and its dependency was SKIPPED
(not just FAILED), the injected result has error code `UPSTREAM_SKIPPED`:

```python
@app.task()
def handle_any_failure(
    upstream: TaskResult[Data, TaskError],
) -> TaskResult[Report, TaskError]:
    if upstream.is_ok():
        return TaskResult(ok=Report(data=upstream.ok_value))

    error = upstream.err_value
    if error.error_code == "UPSTREAM_SKIPPED":
        # Upstream was skipped (its own dependency failed)
        return TaskResult(ok=Report(skipped=True))
    else:
        # Upstream actually failed
        return TaskResult(ok=Report(failed=True, error=str(error)))
```

---

## 8. Conditional Execution

### 8.1 `skip_when`: Skip If Condition Is True

```python
class ConditionalWorkflow(WorkflowDefinition[str]):
    name = "conditional"

    check = TaskNode(fn=check_condition)
    expensive = TaskNode(
        fn=expensive_operation,
        waits_for=[check],
        workflow_ctx_from=[check],
        skip_when=lambda ctx: ctx.result_for(check).ok_value == "skip",
    )
    fallback = TaskNode(
        fn=fallback_operation,
        waits_for=[check],
        workflow_ctx_from=[check],
        skip_when=lambda ctx: ctx.result_for(check).ok_value != "skip",
    )

    class Meta:
        output = expensive  # or use SuccessPolicy for alternatives
```

### 8.2 `run_when`: Run Only If Condition Is True

```python
process = TaskNode(
    fn=process_data,
    waits_for=[validate],
    workflow_ctx_from=[validate],
    run_when=lambda ctx: ctx.result_for(validate).ok_value.is_valid,
)
```

### 8.3 Condition Priority

When both are set: `skip_when` is evaluated first.
- If `skip_when` returns `True` → SKIPPED (regardless of `run_when`)
- If `skip_when` returns `False` → check `run_when`
- If `run_when` returns `False` → SKIPPED

**Requirement**: Conditions require `workflow_ctx_from` to access results.

---

## 9. Join Semantics

Control when a task with multiple dependencies becomes READY.

### 9.1 `join="all"` (Default)

Task runs when ALL dependencies reach terminal state:

```python
aggregate = TaskNode(
    fn=aggregate,
    waits_for=[branch_a, branch_b, branch_c],
    join="all",  # Default - wait for all
)
```

- If ANY dependency fails/skips → task is SKIPPED (unless `allow_failed_deps=True`)

### 9.2 `join="any"`

Task runs when ANY dependency completes successfully:

```python
first_success = TaskNode(
    fn=use_first_result,
    waits_for=[fast_source, slow_source, backup_source],
    join="any",  # Run as soon as one succeeds
)
```

- Task becomes READY when first dependency COMPLETES
- Task is SKIPPED only if ALL dependencies fail/skip

### 9.3 `join="quorum"`

Task runs when a minimum number of dependencies succeed:

```python
quorum_result = TaskNode(
    fn=quorum_handler,
    waits_for=[replica_a, replica_b, replica_c],
    join="quorum",
    min_success=2,  # Need at least 2 of 3
)
```

- Task becomes READY when `min_success` dependencies COMPLETE
- Task is SKIPPED if threshold becomes impossible to reach

---

## 10. Success Policy

Define what "workflow success" means beyond the default output node.

### 10.1 Basic Success Policy

```python
from horsies import SuccessPolicy, SuccessCase

class DeliveryWorkflow(WorkflowDefinition[str]):
    name = "delivery"

    pickup = TaskNode(fn=pickup_package)
    deliver_door = TaskNode(fn=deliver_to_door, waits_for=[pickup])
    deliver_neighbor = TaskNode(fn=deliver_to_neighbor, waits_for=[pickup])
    deliver_locker = TaskNode(fn=deliver_to_locker, waits_for=[pickup])

    class Meta:
        output = deliver_door
        # Workflow succeeds if ANY delivery method works
        success_policy = SuccessPolicy(cases=[
            SuccessCase(required=[deliver_door]),
            SuccessCase(required=[deliver_neighbor]),
            SuccessCase(required=[deliver_locker]),
        ])
```

**Evaluation**: Cases are checked in order. First satisfied case determines success.

### 10.2 Multiple Required Tasks

```python
# Both validation AND storage must succeed
success_policy = SuccessPolicy(cases=[
    SuccessCase(required=[validate, store]),
])
```

---

## 11. Pause and Resume

### 11.1 Manual Pause

```python
await handle.pause()
# Workflow enters PAUSED state
# Running tasks complete, no new tasks enqueue
```

### 11.2 Resume

```python
await handle.resume()
# Workflow resumes from where it paused
```

### 11.3 Pause with Subworkflows

When a parent workflow pauses:
- Parent enters PAUSED state
- Running child workflows receive pause signal
- No new child workflows start

When parent resumes:
- Parent and paused children resume
- Processing continues

---

## 12. Validation

At `WorkflowSpec` creation, these are validated:

| Validation | Error Code |
|------------|------------|
| No root tasks (all have dependencies) | `WORKFLOW_INVALID_STRUCTURE` |
| Cycle detected | `WORKFLOW_CYCLE_DETECTED` |
| `args_from` references task not in `waits_for` | `WORKFLOW_INVALID_ARGS_FROM` |
| `workflow_ctx_from` references task not in `waits_for` | `WORKFLOW_INVALID_CTX_FROM` |
| `workflow_ctx_from` set but function lacks param | `WORKFLOW_CTX_PARAM_MISSING` |
| Unsupported subworkflow retry mode | `WORKFLOW_INVALID_SUBWORKFLOW_RETRY_MODE` |
| Nested workflow cycle (A contains B contains A) | `WORKFLOW_SUBWORKFLOW_CYCLE` |

---

## 13. Best Practices

### DO

```python
# Use class-based definitions for reusability
class MyWorkflow(WorkflowDefinition[Output]):
    name = "my_workflow"
    ...

# Use args_from for direct data flow
node_b = TaskNode(fn=task_b, waits_for=[node_a], args_from={"data": node_a})

# Use workflow_ctx_from when accessing multiple results
node_c = TaskNode(fn=aggregate, waits_for=[a, b], workflow_ctx_from=[a, b])

# Use allow_failed_deps for recovery tasks
recovery = TaskNode(fn=recover, waits_for=[primary], allow_failed_deps=True)

# Use SubWorkflowNode for reusable workflow modules
child = SubWorkflowNode(workflow_def=ReusablePipeline)

# Keep build_with() deterministic
@classmethod
def build_with(cls, app, param, *_args, **_kwargs):
    # Always produces same structure for same inputs
    ...
```

### DON'T

```python
# Don't use inline workflows with conditions/subworkflows
spec = app.workflow(
    name="inline",
    tasks=[TaskNode(fn=task, run_when=lambda ctx: ...)],  # Won't work in workers!
)

# Don't forget workflow_ctx_from when using conditions
node = TaskNode(
    fn=task,
    waits_for=[dep],
    # workflow_ctx_from=[dep],  # Missing! Condition can't access dep's result
    run_when=lambda ctx: ctx.result_for(dep).is_ok(),  # Will fail
)

# Don't access results not in workflow_ctx_from
node = TaskNode(
    fn=task,
    waits_for=[a, b, c],
    workflow_ctx_from=[a],  # Only 'a' is available
)
# Inside task: ctx.result_for(b)  # Error! 'b' not in context

# Don't use SubWorkflowNode for trivial single-task logic
# Just use a TaskNode instead

# Don't make build_with() non-deterministic
@classmethod
def build_with(cls, app, *_args, **_kwargs):
    if random.random() > 0.5:  # Non-deterministic! Workers will rebuild differently
        ...
```

---

## 14. Troubleshooting

### Error: `WORKFLOW_SUBWORKFLOW_APP_MISSING`

**Cause**: Broker has no `app` attached.

**Fix**: Use `app.get_broker()` or attach manually:
```python
broker = PostgresBroker(...)
broker.app = app  # Attach before start_workflow
```

### Error: `WORKFLOW_CTX_PARAM_MISSING`

**Cause**: `workflow_ctx_from` is set but task function lacks the parameter.

**Fix**: Add the parameter:
```python
@app.task()
def my_task(
    workflow_ctx: WorkflowContext | None = None,  # Add this
) -> TaskResult[Output, TaskError]:
    ...
```

### Error: `WORKFLOW_INVALID_ARGS_FROM`

**Cause**: `args_from` references a node not in `waits_for`.

**Fix**: Add the node to `waits_for`:
```python
node_b = TaskNode(
    fn=task_b,
    waits_for=[node_a],  # Must include node_a
    args_from={"data": node_a},
)
```

### Error: `WORKFLOW_CYCLE_DETECTED`

**Cause**: Circular dependency in the DAG.

**Fix**: Review your `waits_for` relationships. Use a DAG visualization tool if needed.

### Subworkflow Not Found in Worker

**Cause**: Worker process doesn't have the workflow module imported.

**Fix**: Ensure the workflow definition module is imported in your worker entry point:
```python
# worker.py
import myapp.workflows  # Import all workflow definitions
from horsies import run_worker

run_worker(...)
```

### Task Stuck in PENDING

**Cause**: Dependencies never reach terminal state, or workflow is PAUSED.

**Fix**: Check:
1. Are all dependencies defined correctly?
2. Is the workflow PAUSED? Call `handle.resume()`
3. Are workers running and processing the queue?

---

## 15. Complete Example: Multi-Source Data Pipeline

Putting it all together:

```python
from typing import Any

from pydantic import BaseModel

from horsies import (
    Horsies,
    WorkflowDefinition,
    TaskNode,
    SubWorkflowNode,
    TaskResult,
    TaskError,
    WorkflowContext,
    WorkflowSpec,
    SuccessPolicy,
    SuccessCase,
)

app = Horsies()


# --- Data Types ---

class RawData(BaseModel):
    source: str
    content: str

class ProcessedData(BaseModel):
    source: str
    records: list[dict]

class AggregatedReport(BaseModel):
    total_records: int
    sources: list[str]
    errors: list[str]


# --- Task Functions ---

@app.task()
def fetch_from_api(url: str) -> TaskResult[RawData, TaskError]:
    # Simulate fetch
    return TaskResult(ok=RawData(source=url, content="..."))

@app.task()
def parse_and_validate(
    raw: TaskResult[RawData, TaskError],
) -> TaskResult[ProcessedData, TaskError]:
    if raw.is_err():
        return TaskResult(err=raw.err_value)
    data = raw.ok_value
    # Simulate parsing
    return TaskResult(ok=ProcessedData(source=data.source, records=[{"id": 1}]))

@app.task()
def aggregate_sources(
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[AggregatedReport, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX", message="Missing context"))

    total = 0
    sources = []
    errors = []

    # Access subworkflow summaries
    for node in [source1_node, source2_node]:
        summary = workflow_ctx.summary_for(node)
        if summary.status.value == "COMPLETED":
            sources.append(f"{node.name}: {summary.completed}/{summary.total} tasks")
            if summary.output:
                total += len(summary.output.records)
        else:
            errors.append(f"{node.name}: {summary.error or 'unknown error'}")

    return TaskResult(ok=AggregatedReport(
        total_records=total,
        sources=sources,
        errors=errors,
    ))


# --- Child Workflow: Reusable Data Processor ---

class DataSourcePipeline(WorkflowDefinition[ProcessedData]):
    """Reusable pipeline for fetching and processing a single data source."""
    name = "data_source_pipeline"

    fetch: TaskNode[RawData]
    process: TaskNode[ProcessedData]

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        source_url: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec:
        fetch = TaskNode(fn=fetch_from_api, args=(source_url,))
        process = TaskNode(
            fn=parse_and_validate,
            waits_for=[fetch],
            args_from={"raw": fetch},
        )
        return WorkflowSpec(
            name=cls.name,
            tasks=[fetch, process],
            output=process,
            workflow_def_module=cls.__module__,
            workflow_def_qualname=cls.__qualname__,
        )

    class Meta:
        output = None  # Dynamic


# --- Parent Workflow: Multi-Source Aggregation ---

# Define nodes at module level for workflow_ctx_from reference
source1_node = SubWorkflowNode(
    workflow_def=DataSourcePipeline,
    kwargs={"source_url": "https://api1.example.com/data"},
)
source2_node = SubWorkflowNode(
    workflow_def=DataSourcePipeline,
    kwargs={"source_url": "https://api2.example.com/data"},
)
aggregate_node = TaskNode(
    fn=aggregate_sources,
    waits_for=[source1_node, source2_node],
    workflow_ctx_from=[source1_node, source2_node],
    allow_failed_deps=True,  # Aggregate even if some sources fail
)


class MultiSourcePipeline(WorkflowDefinition[AggregatedReport]):
    """
    Orchestrates multiple data source pipelines and aggregates results.

    - Runs source pipelines in parallel (no waits_for between them)
    - Aggregates results even if some sources fail (allow_failed_deps)
    - Uses SubWorkflowSummary for partial success visibility
    """
    name = "multi_source_pipeline"

    source1 = source1_node
    source2 = source2_node
    aggregate = aggregate_node

    class Meta:
        output = aggregate
        # Success if aggregation completes (even with partial data)
        success_policy = SuccessPolicy(cases=[
            SuccessCase(required=[aggregate]),
        ])


# --- Usage ---

async def run_pipeline(broker):
    spec = MultiSourcePipeline.build(app)
    handle = await start_workflow_async(spec, broker)

    result = await handle.get()

    if result.is_ok():
        report = result.ok_value
        print(f"Processed {report.total_records} records from {len(report.sources)} sources")
        if report.errors:
            print(f"Errors: {report.errors}")
    else:
        print(f"Pipeline failed: {result.err_value}")
```

This example demonstrates:
- Parameterized subworkflows with `build_with()`
- Parallel subworkflow execution
- `SubWorkflowSummary` for partial success visibility
- `allow_failed_deps` for graceful degradation
- `SuccessPolicy` for custom success criteria
- Proper typing throughout
