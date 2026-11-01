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
- Error handling policies (`on_error`, `allow_failed_deps`)
- Success criteria (`SuccessPolicy`)

---

## 2. Defining a Workflow

### 2.1 Class-Based Definition (Recommended)

<!-- check: skip partial -- uses names that the page does not define -->
```python
from pydantic import SecretStr
from horsies import Horsies, AppConfig, PostgresConfig, WorkflowDefinition, WorkflowDefinitionConfig, TaskNode, TaskResult, TaskError, JsonValue

app = Horsies(AppConfig(
    broker=PostgresConfig(database_url=SecretStr("postgresql+psycopg://user:pass@localhost:5432/mydb")),
))

@app.task(task_name="fetch_data")
def fetch_data(*, url: str) -> TaskResult[dict[str, JsonValue], TaskError]:
    # fetch logic...
    return TaskResult(ok={"data": "..."})

@app.task(task_name="transform")
def transform(*, raw: TaskResult[dict[str, JsonValue], TaskError]) -> TaskResult[str, TaskError]:
    # With the defaults, a failed `fetch` makes this node SKIPPED, so this branch
    # runs only on a decode failure. An upstream err also arrives with
    # allow_failed_deps=True, join="any", or join="quorum".
    if raw.is_err():
        return TaskResult(err=raw.err_value)
    return TaskResult(ok=str(raw.ok_value))

@app.task(task_name="store")
def store(*, transformed: str) -> TaskResult[bool, TaskError]:
    # store logic...
    return TaskResult(ok=True)


class DataPipeline(WorkflowDefinition[bool]):
    """
    A simple ETL pipeline: fetch -> transform -> store.

    The generic parameter [bool] indicates the workflow output type.
    """
    name = "data_pipeline"
    definition_key = "myapp.data_pipeline.v1"

    fetch = TaskNode(fn=fetch_data, kwargs={'url': "https://api.example.com"})
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

    config = WorkflowDefinitionConfig(
        output=store,  # Workflow result comes from this node
    )
```

**Key points:**
- `name` is required and must be unique
- `definition_key` is required — a stable string key used for persistence and runtime lookup (e.g. `"myapp.data_pipeline.v1"`)
- Nodes are class attributes (order doesn't matter, DAG is built from `waits_for`)
- `config = WorkflowDefinitionConfig(output=...)` names the node whose result becomes the workflow result. The class body can name its own nodes there
- A `Meta` attribute (the earlier nested `class Meta` form), or a `config` that is not a `WorkflowDefinitionConfig`, raises `HRS-033` when the class is created

For type-safe node construction with static arguments, see [Typed Node Builder](typed-node-builder).

### 2.2 Starting a Workflow

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Ok

# Build and start
spec = DataPipeline.build(app)

# start_async() returns Ok(handle) or Err(WorkflowStartError)
match await spec.start_async():
    case Err(start_error):
        print(f"Start failed: {start_error.code} - {start_error.message}")
    case Ok(handle):
        # Wait for completion
        result = await handle.get_async()  # TaskResult[bool, TaskError]

        if result.is_ok():
            print(f"Pipeline succeeded: {result.ok_value}")
        else:
            print(f"Pipeline failed: {result.err_value}")
```

### 2.3 Inline Definition (Simple Cases Only)

For simple DAGs without conditions or subworkflows:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Ok

node_a = TaskNode(fn=task_a, kwargs={'value': 1})
node_b = TaskNode(fn=task_b, waits_for=[node_a])

spec = app.workflow(
    name="simple_workflow",
    tasks=[node_a, node_b],
    definition_key="myapp.simple_workflow.v1",
    output=node_b,
)
match await spec.start_async():
    case Err(start_error):
        raise RuntimeError(start_error.message)
    case Ok(handle):
        pass
```

**Warning**: Inline workflows are not portable for subworkflows.
Use class-based `WorkflowDefinition` for anything beyond trivial DAGs.

---

## 3. Node Dependencies: `waits_for`

`waits_for` defines execution order. A node waits until all its dependencies reach a terminal state (COMPLETED, FAILED, or SKIPPED).

<!-- check: skip partial -- uses names that the page does not define -->
```python
class DiamondDAG(WorkflowDefinition[str]):
    name = "diamond"
    definition_key = "myapp.diamond.v1"

    #     ┌→ B ─┐
    # A ──┤     ├→ D
    #     └→ C ─┘

    a = TaskNode(fn=task_a)
    b = TaskNode(fn=task_b, waits_for=[a])
    c = TaskNode(fn=task_c, waits_for=[a])
    d = TaskNode(fn=task_d, waits_for=[b, c])  # Waits for BOTH b and c

    config = WorkflowDefinitionConfig(output=d)
```

**Important**: `waits_for` means "wait for completion", not "require success".
By default, if a dependency fails, the downstream task is SKIPPED.
Use `allow_failed_deps=True` to run anyway (see Section 6).

---

## 4. Data Flow: `args_from`

`args_from` injects upstream results as keyword arguments.

<!-- check: skip partial -- uses a name before the page defines it -->
```python
@app.task(task_name="add")
def add(*, a: int, b: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=a + b)

@app.task(task_name="multiply")
def multiply(
    *,
    sum_result: TaskResult[int, TaskError],
    factor: int,
) -> TaskResult[int, TaskError]:
    if sum_result.is_err():
        return TaskResult(err=sum_result.err_value)
    return TaskResult(ok=sum_result.ok_value * factor)


class MathWorkflow(WorkflowDefinition[int]):
    name = "math"
    definition_key = "myapp.math.v1"

    add_node = TaskNode(fn=add, kwargs={'a': 5, 'b': 3})
    multiply_node = TaskNode(
        fn=multiply,
        waits_for=[add_node],
        args_from={"sum_result": add_node},  # Receives TaskResult[int, TaskError]
        kwargs={"factor": 10},
    )

    config = WorkflowDefinitionConfig(output=multiply_node)
```

**The injected value is always `TaskResult[T, TaskError]`**, not the raw value.
This lets you handle upstream failures explicitly.

---

## 5. Workflow Context: `workflow_ctx_from`

When a task needs access to multiple upstream results or subworkflow summaries,
use `workflow_ctx_from` instead of multiple `args_from` entries.

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import WorkflowContext

@app.task(task_name="aggregate")
def aggregate(
    *,
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
    definition_key = "myapp.aggregation.v1"

    node_a = TaskNode(fn=produce_int)
    node_b = TaskNode(fn=produce_str)
    agg = TaskNode(
        fn=aggregate,
        waits_for=[node_a, node_b],
        workflow_ctx_from=[node_a, node_b],  # Both results available in context
    )

    config = WorkflowDefinitionConfig(output=agg)
```

**Rules:**
- Every node in `workflow_ctx_from` must also be in `waits_for`
- The task function must have `workflow_ctx: WorkflowContext | None` parameter
- Context is built fresh per task; it does not propagate downstream
- `result_for()` and `summary_for()` raise `WorkflowContextMissingIdError` if the node has no `node_id`. Always assign `node_id` to nodes used with context

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

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Child workflow
class ChildPipeline(WorkflowDefinition[int]):
    name = "child_pipeline"
    definition_key = "myapp.child_pipeline.v1"

    step1 = TaskNode(fn=produce_int, kwargs={'value': 5})
    step2 = TaskNode(fn=double, waits_for=[step1], args_from={"value": step1})

    config = WorkflowDefinitionConfig(output=step2)


# Parent workflow
class ParentWorkflow(WorkflowDefinition[int]):
    name = "parent_workflow"
    definition_key = "myapp.parent_workflow.v1"

    child = SubWorkflowNode(workflow_def=ChildPipeline)

    config = WorkflowDefinitionConfig(
        output=child,  # Parent result = child workflow result
    )
```

**Lifecycle mapping:**
- Child RUNNING → Parent node RUNNING
- Child COMPLETED with an ok output → Parent node COMPLETED (result = child output)
- Child COMPLETED with an err output → Parent node FAILED (result = that err): see [Output Node Under a Success Policy](/horsies/concepts/workflows/workflow-semantics/#output-node-under-a-success-policy)
- Child FAILED → Parent node FAILED (result = SubWorkflowError)

### 6.3 Parameterized Subworkflows with `build_with()`

**Important:** Passing `kwargs` or `args_from` to a `SubWorkflowNode` requires overriding `build_with()`.
The default implementation ignores all parameters. If you pass params without overriding, validation
fails with `HRS-022` (`WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH`).

When the child workflow needs runtime parameters:

<!-- check: skip partial -- uses names that the page does not define -->
```python
class DataProcessor(WorkflowDefinition[ProcessedData]):
    name = "data_processor"
    definition_key = "myapp.data_processor.v1"

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
        fetch = TaskNode(fn=fetch_raw_data, kwargs={'source_url': source_url})
        clean = TaskNode(fn=clean_data, waits_for=[fetch], args_from={"raw": fetch})
        process = TaskNode(fn=process_data, waits_for=[clean], args_from={"clean": clean})

        return WorkflowSpec(
            name=cls.name,
            tasks=[fetch, clean, process],
            output=process,
        )
```

:::note[Queue and priority binding]
A `build_with()` that returns a `WorkflowSpec(...)` directly does not run the
queue/priority binding that `app.workflow()` applies. A `TaskNode` left without
an explicit `priority` inherits its queue's configured priority, and one
without an explicit `queue` inherits `fn.task_queue_name` — but that resolution
happens when the subworkflow is enqueued, not at construction time. So a node
built this way carries `queue=None, priority=None` until enqueue.

`horsies check` validates the queues of nodes in any spec a registered builder
returns, and **recursively expands statically-parameterized `SubWorkflowNode`s**
to validate their nodes too. A subworkflow is static when it has no `args_from`
mapping — its `build_with` kwargs are fully literal — so the child spec is
deterministic at check time. An invalid queue (unknown in CUSTOM mode,
non-`default` in DEFAULT mode) anywhere in that tree fails check instead of
surfacing at runtime.

A **runtime-parameterized** subworkflow — one that injects dependency results
into `build_with` via `args_from` — cannot be built at check time, so it is not
expanded. To get check coverage for one, register its `build_with` as a
workflow builder (`@app.workflow_builder`, with `cases` for required params).
:::

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Use in parent with kwargs
class MultiSourceAggregation(WorkflowDefinition[Report]):
    name = "multi_source"
    definition_key = "myapp.multi_source_aggregation.v1"

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

    config = WorkflowDefinitionConfig(output=aggregate)
```

#### Dynamic Fan-Out with `args_from`

`build_with()` runs when the `SubWorkflowNode` becomes ready, not when the parent starts.
A node becomes ready when its `waits_for` dependencies are terminal (see [Join Semantics](#8-join-semantics)).
The child graph can therefore depend on the result of an upstream node, for example one child node per element of a list.

Each `args_from` entry arrives in `build_with()` as the upstream `TaskResult[T, TaskError]`, not as the plain value.
The key of an `args_from` entry must be a parameter of `build_with()`.
A key that is also a static `kwargs` key fails the node.

The example builds one `count_shard_rows` node for each shard that `list_shards` returns.
A fan-in task reads the shard results by `NodeKey`, because the child nodes do not exist at import time.

```python
from typing import Any

from horsies import (
    Horsies,
    NodeKey,
    SubWorkflowNode,
    TaskError,
    TaskNode,
    TaskResult,
    WorkflowContext,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
    WorkflowSpec,
)
from instance import app


@app.task("list_shards")
def list_shards(*, tenant_id: str) -> TaskResult[list[str], TaskError]:
    return TaskResult(ok=[f"{tenant_id}-a", f"{tenant_id}-b", f"{tenant_id}-c"])


@app.task("count_shard_rows")
def count_shard_rows(*, shard: str) -> TaskResult[int, TaskError]:
    return TaskResult(ok=len(shard))


@app.task("sum_shard_rows")
def sum_shard_rows(
    *,
    shard_count: int,
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[int, TaskError]:
    if workflow_ctx is None:
        return TaskResult(
            err=TaskError(error_code="NO_CONTEXT", message="workflow_ctx is missing"),
        )
    total = 0
    for position in range(shard_count):
        shard_result = workflow_ctx.result_for(NodeKey[int](f"shard_{position}"))
        if shard_result.is_err():
            return TaskResult(err=shard_result.err_value)
        total += shard_result.ok_value
    return TaskResult(ok=total)


class ShardReport(WorkflowDefinition[int]):
    """Child workflow. Its shape depends on the result of list_shards."""

    name = "shard_report"
    definition_key = "myapp.shard_report.v1"

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        shards: TaskResult[list[str], TaskError],
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[int]:
        if shards.is_err():
            raise ValueError("shard_report needs the shard list")
        shard_names = shards.ok_value

        count_nodes: list[TaskNode[int]] = [
            count_shard_rows.node(node_id=f"shard_{position}")(shard=shard_name)
            for position, shard_name in enumerate(shard_names)
        ]
        total = sum_shard_rows.node(
            waits_for=list(count_nodes),
            workflow_ctx_from=list(count_nodes),
            node_id="total",
        )(shard_count=len(count_nodes))
        return app.workflow(
            name=cls.name,
            tasks=[*count_nodes, total],
            output=total,
        )


class TenantReport(WorkflowDefinition[int]):
    name = "tenant_report"
    definition_key = "myapp.tenant_report.v1"

    shards = list_shards.node(node_id="shards")(tenant_id="tenant-1")
    report = SubWorkflowNode(
        workflow_def=ShardReport,
        waits_for=[shards],
        args_from={"shards": shards},
        node_id="report",
    )

    config = WorkflowDefinitionConfig(output=report)
```

Rules for this pattern:

- Define the tasks at module level. A worker resolves a task by name from the modules it imported.
- Import the child module in the worker (`app.discover_tasks`). See [Subworkflow Not Found in Worker](#subworkflow-not-found-in-worker).
- With the default `allow_failed_deps=False`, a failed upstream node SKIPS the `SubWorkflowNode` and `build_with()` does not run.
  With `allow_failed_deps=True`, `build_with()` receives `TaskResult(err=...)` and must handle it.
- An exception in `build_with()` fails the `SubWorkflowNode` with `WORKFLOW_ENQUEUE_FAILED`. The message starts with `Subworkflow build failed`.
- The child start parses the task options of each child root and of each child task with `good_until`. Options that do not parse are a defect, not a node failure. They occur only when the `task_options_json` of a task function changed after registration. The child start raises `SerializationError`, and its transaction rolls back, as in the workflow start transaction.
- The child start also encodes and serializes the kwargs of each child node. Spec construction in `build_with()` already encoded and decoded them, and refuses a value that does not fit with `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (the node fails with `WORKFLOW_ENQUEUE_FAILED`). So a refusal at the child start is a defect: the child start raises `SerializationError` and its transaction rolls back.
- `horsies check` does not expand a `SubWorkflowNode` that has `args_from`. See the note above on `@app.workflow_builder`.
- A raise in the child start stops in the reaper's recovery, and the [recovery limit](/horsies/concepts/workflows/workflow-semantics/#recovery-limit) bounds it. At the limit, the `SubWorkflowNode` goes `FAILED` with `WORKFLOW_RECOVERY_LIMIT_REACHED`.
- A child workflow that reaches the recovery limit goes `FAILED`. Its parent node then goes `FAILED` with `SUBWORKFLOW_FAILED`, and `sub_workflow_summary.error_summary` holds the message of the child.

### 6.4 Accessing Subworkflow Health: SubWorkflowSummary

When you need visibility into child workflow internals:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import WorkflowContext, SubWorkflowSummary

@app.task(task_name="report_health")
def report_health(
    *,
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[HealthReport, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX", message="Missing context"))

    # Get summary for the child subworkflow node
    summary: SubWorkflowSummary = workflow_ctx.summary_for(child_node)

    return TaskResult(ok=HealthReport(
        status=summary.status,           # WorkflowStatus (COMPLETED, FAILED, etc.)
        output=summary.output,           # Child workflow output (if completed)
        total_tasks=summary.total_tasks,       # Total tasks in child
        completed=summary.completed_tasks,     # Successfully completed tasks
        failed=summary.failed_tasks,           # Failed tasks
        skipped=summary.skipped_tasks,         # Skipped tasks
        error_summary=summary.error_summary,   # Error details (if failed)
    ))


class MonitoredPipeline(WorkflowDefinition[HealthReport]):
    name = "monitored"
    definition_key = "myapp.monitored_pipeline.v1"

    child = SubWorkflowNode(workflow_def=ChildPipeline)
    reporter = TaskNode(
        fn=report_health,
        waits_for=[child],
        workflow_ctx_from=[child],  # Required for summary_for()
        allow_failed_deps=True,     # Run even if child fails
    )

    config = WorkflowDefinitionConfig(output=reporter)
```

**SubWorkflowSummary fields:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | `WorkflowStatus` | Terminal status of child workflow. `COMPLETED` on a FAILED node when the child's output is an err |
| `output` | `Any \| None` | Child workflow output (if COMPLETED). `None` when the child's output result is an err |
| `total_tasks` | `int` | Total tasks in child workflow |
| `completed_tasks` | `int` | Tasks that completed successfully |
| `failed_tasks` | `int` | Tasks that failed |
| `skipped_tasks` | `int` | Tasks that were skipped |
| `error_summary` | `str \| None` | Error summary (if FAILED) |

### 6.5 Receiving Subworkflow Results via `args_from`

SubWorkflowNode results flow like TaskNode results:

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task(task_name="process_child_result")
def process_child_result(
    *,
    child_result: TaskResult[ChildOutput, TaskError],
) -> TaskResult[ParentOutput, TaskError]:
    if child_result.is_err():
        # Child workflow failed - error is SubWorkflowError
        return TaskResult(err=child_result.err_value)

    # Child succeeded - access the output value
    child_output: ChildOutput = child_result.ok_value
    return TaskResult(ok=ParentOutput(derived_from=child_output))


class ParentWithDataFlow(WorkflowDefinition[ParentOutput]):
    name = "parent_data_flow"
    definition_key = "myapp.parent_data_flow.v1"

    child = SubWorkflowNode(workflow_def=ChildPipeline)
    processor = TaskNode(
        fn=process_child_result,
        waits_for=[child],
        args_from={"child_result": child},  # Receives TaskResult[ChildOutput, TaskError]
    )

    config = WorkflowDefinitionConfig(output=processor)
```

Outputless child workflows are orchestration-only when used through
`SubWorkflowNode`. If a `WorkflowDefinition` has no `config.output`, declare it
as `WorkflowDefinition[None]`; the parent node receives `TaskResult[None,
TaskError]` with `ok=None` when the child completes. The terminal-results map
returned by a top-level outputless `app.workflow(...).get()` is not exposed as
a nested subworkflow output.

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

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task(task_name="recovery_handler")
def recovery_handler(
    *,
    primary_result: TaskResult[Data, TaskError],
) -> TaskResult[Data, TaskError]:
    if primary_result.is_ok():
        return primary_result  # Pass through success

    # Handle the failure - use fallback, log, alert, etc.
    error = primary_result.err_value
    return TaskResult(ok=Data(fallback=True, original_error=str(error)))


class RecoveryWorkflow(WorkflowDefinition[Data]):
    name = "recovery"
    definition_key = "myapp.recovery.v1"

    primary = TaskNode(fn=fetch_data)
    recovery = TaskNode(
        fn=recovery_handler,
        waits_for=[primary],
        args_from={"primary_result": primary},
        allow_failed_deps=True,  # Run even if primary fails
    )

    config = WorkflowDefinitionConfig(output=recovery)
```

### 7.3 Workflow-Level Error Policy: `on_error`

Control what happens when any task fails:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import OnError, WorkflowDefinitionConfig

# Set on_error in the WorkflowDefinition's config
class MyWorkflow(WorkflowDefinition[OutputType]):
    name = "my_workflow"
    definition_key = "myapp.my_workflow.v1"
    # ... nodes ...

    config = WorkflowDefinitionConfig(
        output=some_node,
        on_error=OnError.PAUSE,  # default is OnError.FAIL
    )
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

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task(task_name="handle_any_failure")
def handle_any_failure(
    *,
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

## 8. Join Semantics

Control when a task with multiple dependencies becomes READY.

### 8.1 `join="all"` (Default)

Task runs when ALL dependencies reach terminal state:

<!-- check: skip partial -- uses names that the page does not define -->
```python
aggregate = TaskNode(
    fn=aggregate,
    waits_for=[branch_a, branch_b, branch_c],
    join="all",  # Default - wait for all
)
```

- If ANY dependency fails/skips → task is SKIPPED (unless `allow_failed_deps=True`)

### 8.2 `join="any"`

Task runs when ANY dependency completes successfully:

<!-- check: skip partial -- uses names that the page does not define -->
```python
first_success = TaskNode(
    fn=use_first_result,
    waits_for=[fast_source, slow_source, backup_source],
    join="any",  # Run as soon as one succeeds
)
```

- Task becomes READY when first dependency COMPLETES
- Task is SKIPPED only if ALL dependencies fail/skip

### 8.3 `join="quorum"`

Task runs when a minimum number of dependencies succeed:

<!-- check: skip partial -- uses names that the page does not define -->
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

## 9. Success Policy

Define what "workflow success" means beyond the default output node.

### 9.1 Basic Success Policy

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import SuccessPolicy, SuccessCase

class DeliveryWorkflow(WorkflowDefinition[str]):
    name = "delivery"
    definition_key = "myapp.delivery.v1"

    pickup = TaskNode(fn=pickup_package)
    deliver_door = TaskNode(fn=deliver_to_door, waits_for=[pickup])
    deliver_neighbor = TaskNode(fn=deliver_to_neighbor, waits_for=[pickup])
    deliver_locker = TaskNode(fn=deliver_to_locker, waits_for=[pickup])

    config = WorkflowDefinitionConfig(
        output=deliver_door,
        # Workflow succeeds if ANY delivery method works
        success_policy=SuccessPolicy(cases=[
            SuccessCase(required=[deliver_door]),
            SuccessCase(required=[deliver_neighbor]),
            SuccessCase(required=[deliver_locker]),
        ]),
    )
```

**Evaluation**: Cases are checked in order. First satisfied case determines success.

### 9.2 Multiple Required Tasks

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Both validation AND storage must succeed
success_policy = SuccessPolicy(cases=[
    SuccessCase(required=[validate, store]),
])
```

---

## 10. Pause and Resume

### 10.1 Manual Pause

```python
result = handle.pause()  # Returns HandleResult[bool]
# Workflow enters PAUSED state
# Running tasks complete, no new tasks enqueue

# Async variant:
result = await handle.pause_async()
```

### 10.2 Resume

```python
result = handle.resume()  # Returns HandleResult[bool]
# Workflow resumes from where it paused

# Async variant:
result = await handle.resume_async()
```

### 10.3 Pause with Subworkflows

When a parent workflow pauses:
- Parent enters PAUSED state
- Running child workflows receive pause signal
- No new child workflows start

When parent resumes:
- Parent and paused children resume
- Processing continues

---

## 11. Validation

At `WorkflowSpec` creation, these are validated:

| Validation | Error Code |
|------------|------------|
| No root tasks (all have dependencies) | `WORKFLOW_NO_ROOT_TASKS` |
| Cycle detected | `WORKFLOW_CYCLE_DETECTED` |
| `args_from` references task not in `waits_for` | `WORKFLOW_INVALID_ARGS_FROM` |
| `workflow_ctx_from` references task not in `waits_for` | `WORKFLOW_INVALID_CTX_FROM` |
| `workflow_ctx_from` set but function lacks param | `WORKFLOW_CTX_PARAM_MISSING` |
| Workflow spec or definition missing `definition_key` | `WORKFLOW_NO_DEFINITION_KEY` |
| Two workflow definitions share the same `definition_key` | `WORKFLOW_DUPLICATE_DEFINITION_KEY` |

---

## 12. Best Practices

### DO

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Use class-based definitions for reusability
class MyWorkflow(WorkflowDefinition[Output]):
    name = "my_workflow"
    definition_key = "myapp.my_workflow.v1"
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

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
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

## 13. Troubleshooting

### Error: `WORKFLOW_SUBWORKFLOW_APP_MISSING`

**Cause**: Broker has no `app` attached.

**Fix**: Use `app.get_broker()` or attach manually:
<!-- check: skip partial -- uses names that the page does not define -->
```python
broker = PostgresBroker(...)
broker.app = app  # Attach before start_workflow
```

**Runtime note (post-ENQUEUED path)**:
If this condition is hit while a workflow is already running (for example, dependency completion triggers
subworkflow enqueue and `broker.app` is `None`), Horsies does not re-raise `WorkflowValidationError` to the caller.
Instead, it marks the `SubWorkflowNode` as `FAILED` with `TaskError.error_code = WORKFLOW_ENQUEUE_FAILED` so the
workflow can continue deterministic failure propagation without leaving the node stranded in `ENQUEUED`.

### Error: `WORKFLOW_CTX_PARAM_MISSING`

**Cause**: `workflow_ctx_from` is set but task function lacks the parameter.

**Fix**: Add the parameter:
<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task(task_name="my_task")
def my_task(
    *,
    workflow_ctx: WorkflowContext | None = None,  # Add this
) -> TaskResult[Output, TaskError]:
    ...
```

### Error: `WORKFLOW_INVALID_ARGS_FROM`

**Cause**: `args_from` references a node not in `waits_for`.

**Fix**: Add the node to `waits_for`:
<!-- check: skip partial -- uses names that the page does not define -->
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

**Cause**: Worker process doesn't have the workflow definition module imported, so `definition_key` is not registered.

Every `WorkflowDefinition` subclass auto-registers its `definition_key` at import time via the metaclass. If the module isn't imported in the worker, the registry is empty and subworkflow resolution fails with `SUBWORKFLOW_LOAD_FAILED`.

**Fix**: Ensure the workflow definition module is discovered by the app:
```python
# In your app setup
app.discover_tasks(["myapp.tasks", "myapp.workflows"])
```
Then start the worker via CLI:
```bash
horsies worker myapp.instance:app
```

### Task Stuck in PENDING

**Cause**: Dependencies never reach terminal state, or workflow is PAUSED.

**Fix**: Check:
1. Are all dependencies defined correctly?
2. Is the workflow PAUSED? Call `handle.resume()`
3. Are workers running and processing the queue?

---

## 14. Complete Example: Multi-Source Data Pipeline

Putting it all together:

```python
from typing import Any

from pydantic import BaseModel

from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
    TaskNode,
    SubWorkflowNode,
    TaskResult,
    TaskError,
    WorkflowContext,
    WorkflowSpec,
    SuccessPolicy,
    SuccessCase,
    Err,
    Ok,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(database_url=SecretStr("postgresql+psycopg://user:pass@localhost:5432/mydb")),
))


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

@app.task(task_name="fetch_from_api")
def fetch_from_api(*, url: str) -> TaskResult[RawData, TaskError]:
    # Simulate fetch
    return TaskResult(ok=RawData(source=url, content="..."))

@app.task(task_name="parse_and_validate")
def parse_and_validate(
    *,
    raw: TaskResult[RawData, TaskError],
) -> TaskResult[ProcessedData, TaskError]:
    if raw.is_err():
        return TaskResult(err=raw.err_value)
    data = raw.ok_value
    # Simulate parsing
    return TaskResult(ok=ProcessedData(source=data.source, records=[{"id": 1}]))

@app.task(task_name="aggregate_sources")
def aggregate_sources(
    *,
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
            sources.append(f"{node.name}: {summary.completed_tasks}/{summary.total_tasks} tasks")
            if summary.output:
                total += len(summary.output.records)
        else:
            errors.append(f"{node.name}: {summary.error_summary or 'unknown error'}")

    return TaskResult(ok=AggregatedReport(
        total_records=total,
        sources=sources,
        errors=errors,
    ))


# --- Child Workflow: Reusable Data Processor ---

class DataSourcePipeline(WorkflowDefinition[ProcessedData]):
    """Reusable pipeline for fetching and processing a single data source."""
    name = "data_source_pipeline"
    definition_key = "myapp.data_source_pipeline.v1"

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
        fetch = TaskNode(fn=fetch_from_api, kwargs={'url': source_url})
        process = TaskNode(
            fn=parse_and_validate,
            waits_for=[fetch],
            args_from={"raw": fetch},
        )
        return WorkflowSpec(
            name=cls.name,
            tasks=[fetch, process],
            output=process,
        )


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
    definition_key = "myapp.multi_source_pipeline.v1"

    source1 = source1_node
    source2 = source2_node
    aggregate = aggregate_node

    config = WorkflowDefinitionConfig(
        output=aggregate,
        # Success if aggregation completes (even with partial data)
        success_policy=SuccessPolicy(cases=[
            SuccessCase(required=[aggregate]),
        ]),
    )


# --- Usage ---

async def run_pipeline():
    spec = MultiSourcePipeline.build(app)
    match await spec.start_async():
        case Err(start_error):
            print(f"Start failed: {start_error.code} - {start_error.message}")
            return
        case Ok(handle):
            pass

    result = await handle.get_async()

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
