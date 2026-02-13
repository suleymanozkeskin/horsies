---
title: Workflow Semantics
summary: DAG-based workflow behavior, failure handling, and dependency resolution.
related: [workflow-api, subworkflows, result-handling, ../../tasks/retry-policy]
tags: [concepts, workflows, DAG, semantics]
---

## Overview

Workflows in Horsies are Directed Acyclic Graphs (DAGs) where:
- **Nodes** are tasks to execute
- **Edges** are dependencies between tasks
- **Execution** proceeds as dependencies are satisfied

## Workflow Status Lifecycle

```
PENDING → RUNNING → COMPLETED
                  ↘ FAILED
                  ↘ PAUSED (if on_error="pause")
                  ↘ CANCELLED (manual)
```

**Terminal workflow statuses:** `COMPLETED`, `FAILED`, `CANCELLED`. (`PAUSED` is non-terminal.)

## Task Status Lifecycle

```
PENDING → READY → ENQUEUED → RUNNING → COMPLETED
                                     ↘ FAILED
       ↘ SKIPPED (if upstream failed/skipped)
```

**Terminal task statuses (within workflows):** `COMPLETED`, `FAILED`, `SKIPPED`.

## Status Enums

`WorkflowStatus` values:

| Enum | Value | Terminal |
|------|-------|----------|
| `PENDING` | `"PENDING"` | No |
| `RUNNING` | `"RUNNING"` | No |
| `COMPLETED` | `"COMPLETED"` | Yes |
| `FAILED` | `"FAILED"` | Yes |
| `PAUSED` | `"PAUSED"` | No |
| `CANCELLED` | `"CANCELLED"` | Yes |

`WorkflowTaskStatus` values:

| Enum | Value | Terminal |
|------|-------|----------|
| `PENDING` | `"PENDING"` | No |
| `READY` | `"READY"` | No |
| `ENQUEUED` | `"ENQUEUED"` | No |
| `RUNNING` | `"RUNNING"` | No |
| `COMPLETED` | `"COMPLETED"` | Yes |
| `FAILED` | `"FAILED"` | Yes |
| `SKIPPED` | `"SKIPPED"` | Yes |

Use `is_terminal` or the frozenset constants to check terminal status programmatically:

```python
from horsies import (
    WorkflowStatus,
    WorkflowTaskStatus,
    WORKFLOW_TERMINAL_STATES,
    WORKFLOW_TASK_TERMINAL_STATES,
)

WorkflowStatus.COMPLETED.is_terminal  # True
WorkflowStatus.PAUSED.is_terminal     # False

WorkflowTaskStatus.SKIPPED.is_terminal  # True
WorkflowTaskStatus.ENQUEUED.is_terminal # False

# Frozensets for use in queries or filters
WORKFLOW_TERMINAL_STATES       # frozenset({COMPLETED, FAILED, CANCELLED})
WORKFLOW_TASK_TERMINAL_STATES  # frozenset({COMPLETED, FAILED, SKIPPED})
```

## Retries and Crash Recovery

### Task retries inside workflows

Workflow tasks use the same retry mechanism as standalone tasks:

- A task retries only when it has a `retry_policy` **and** the failure matches `auto_retry_for` (configured on `RetryPolicy`).
- `auto_retry_for` is a required field on `RetryPolicy`, so retry triggers are always co-located with retry timing.
- While retrying, the workflow task remains `RUNNING` until the final attempt completes.

### Worker crash / restart

If a worker crashes mid-task, recovery behaves as follows:

- **CLAIMED task** (never started): requeued safely.
- **RUNNING task** (started): marked `FAILED` with `WORKER_CRASHED`.
- **Workflow reconciliation**: if `tasks.status` is terminal but the corresponding
  `workflow_tasks.status` is still non-terminal, recovery triggers the normal
  completion path to update `workflow_tasks`, propagate to dependents, and finalize
  the workflow.

When a task is terminal but its result is missing, recovery synthesizes errors:

- `TASK_CANCELLED` for cancelled tasks
- `RESULT_NOT_AVAILABLE` for completed tasks missing results
- `WORKER_CRASHED` for failed tasks missing results

## Failure Semantics

### When is a workflow marked FAILED?

A workflow is marked **FAILED** if **any task fails** (when `on_error="fail"`, the default).

The failure flow:
1. Task fails → workflow stores error, but status remains `RUNNING`
2. DAG continues resolving (dependents may be SKIPPED or run with `allow_failed_deps`)
3. Once all tasks reach terminal state → workflow marked `FAILED`

This design allows recovery handlers (`allow_failed_deps=True`) to execute before the workflow finalizes.

### on_error Policies

| Policy | Behavior |
|--------|----------|
| `fail` (default) | Store error, continue DAG resolution, mark FAILED when complete |
| `pause` | Immediately pause workflow, block new enqueues, await resume |

### Failure Propagation and Short-Circuiting

Horsies does **not** stop the entire workflow on first failure. It resolves the DAG to terminal state and applies failure rules **per dependency**. Short-circuiting happens **along the failed path**, not globally.

**Default behavior (`join="all"`, `allow_failed_deps=False`, `on_error="fail"`):**

- When a dependency fails or is skipped, any downstream task that requires all dependencies is **SKIPPED**.
- That skip propagates to its dependents.
- Other branches that do not depend on the failed task still run.
- The workflow is marked FAILED **after** all tasks are terminal.

**Example:**

```
A → B → C
A → D
```

- If **A fails**:
  - **B** is SKIPPED
  - **C** is SKIPPED
  - **D** still runs (independent branch)
  - Workflow ends as FAILED

**To run recovery logic downstream:**

Set `allow_failed_deps=True` on the downstream task. It will run and receive the failed `TaskResult`.

**Join rules change the short-circuit behavior:**

- `join="any"`: a task can run if **any** dependency succeeds, even if others fail.
- `join="quorum"`: a task can run if the success threshold is met, even with some failures.

**Pause behaves differently:**

- `on_error="pause"` does not kill running tasks.
- It blocks new enqueues and waits for `resume()` or `cancel()`.

**Result access is separate:**

- `WorkflowHandle.get()` returns the workflow-level status (it can be FAILED even if the output task succeeded).
- `WorkflowHandle.result_for()` is non-blocking and may return `RESULT_NOT_READY` if the task has not completed.

### Upstream Failure and args_from

When an upstream task fails and the downstream task uses `join="all"` (the default):

- **`allow_failed_deps=False` (default):** The downstream task is **SKIPPED**. It does not execute and does not receive the failed result.
- **`allow_failed_deps=True`:** The downstream task **runs** and receives the upstream `TaskResult` with `is_err() == True`.

`on_error="fail"` does **not** short-circuit the DAG. The workflow stores the error and continues resolving dependents — downstream tasks are SKIPPED (or run if `allow_failed_deps=True`) before the workflow is marked FAILED.

With `join="any"`, a downstream task runs when **any** dependency succeeds, so a single upstream failure does not cause a skip unless **all** dependencies fail. With `join="quorum"`, the task runs when `min_success` dependencies succeed, so failures only matter if they make the threshold unreachable.

```python
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    TaskNode,
    TaskResult,
    TaskError,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)
app = Horsies(config)

@app.task("produce")
def produce() -> TaskResult[int, TaskError]:
    return TaskResult(ok=1)

@app.task("process")
def process(data: TaskResult[int, TaskError]) -> TaskResult[str, TaskError]:
    if data.is_err():
        return TaskResult(err=TaskError(
            error_code="UPSTREAM_FAILED",
            message="Upstream task failed",
        ))
    return TaskResult(ok=str(data.ok_value))

node_a: TaskNode[int] = TaskNode(fn=produce)

# Default (join="all", allow_failed_deps=False): downstream is SKIPPED when upstream fails
node_b_skip: TaskNode[str] = TaskNode(
    fn=process,
    waits_for=[node_a],
    args_from={"data": node_a},
)

# With allow_failed_deps: downstream runs and receives the failed TaskResult
node_b_recover: TaskNode[str] = TaskNode(
    fn=process,
    waits_for=[node_a],
    args_from={"data": node_a},
    allow_failed_deps=True,
)
```

The manual `is_err()` check is the intended pattern when `allow_failed_deps=True`. Without `allow_failed_deps`, the task never executes on upstream failure — no error handling code is needed.

### PAUSE Semantics (Cooperative Stop)

When a task fails with `on_error="pause"`:

1. **Immediate status change**: Workflow status becomes `PAUSED`
2. **New enqueues blocked**: No new tasks will transition from PENDING → READY or READY → ENQUEUED
3. **Running tasks may complete**: Tasks already running will finish (cooperative, no force-kill)
4. **Already-enqueued tasks**: Tasks already in the queue may still be claimed and run
5. **Resume required**: Workflow remains PAUSED until an explicit resume operation

**What PAUSE blocks:**

- Tasks cannot become READY while workflow is PAUSED
- READY tasks cannot be ENQUEUED while workflow is PAUSED
- Retries are blocked (retries are new enqueues)
- Downstream task propagation is halted

**What PAUSE allows:**

- Already-running tasks complete normally
- Tasks already claimed/started before PAUSE will finish (cooperative stop)

### Resume

A paused workflow can be resumed via `WorkflowHandle.resume()` or `WorkflowHandle.resume_async()`.

```python
handle = await spec.start_async()

# ... workflow pauses due to task failure ...

# Resume the workflow
resumed = await handle.resume_async()  # Returns True if resumed, False if not PAUSED
```

**Resume behavior:**

1. **Guard**: Only works if workflow status is `PAUSED`. Returns `False` (no-op) for other states.
2. **Transition**: Sets workflow status from `PAUSED` → `RUNNING`
3. **Re-evaluate PENDING tasks**: Checks if dependencies are terminal, marks READY if so
4. **Enqueue READY tasks**: All READY tasks are enqueued with their dependency results

**What resume does NOT do:**

- Does not restart failed tasks (they remain FAILED)
- Does not affect already-running tasks
- Does not change COMPLETED/FAILED/CANCELLED workflows

## Dependency Semantics

### Join Modes

TaskNode supports three join modes via the `join` parameter:

| Join Mode | Behavior |
|-----------|----------|
| `all` (default) | Task runs when ALL dependencies are terminal |
| `any` | Task runs when ANY dependency succeeds (COMPLETED) |
| `quorum` | Task runs when at least `min_success` dependencies succeed |

**Note:** `COMPLETED` means the dependency returned `TaskResult.ok`. A dependency that finished execution but returned `TaskResult.err` is `FAILED`, not `COMPLETED`.

### AND-join (join="all")

```
Task runs when: ALL dependencies are in terminal state (COMPLETED/FAILED/SKIPPED)
```

**Behavior matrix:**

| Dependency State | allow_failed_deps=False | allow_failed_deps=True |
|------------------|-------------------------|------------------------|
| All COMPLETED | Task runs normally | Task runs normally |
| Any FAILED | Task is SKIPPED | Task runs with failed TaskResult |
| Any SKIPPED | Task is SKIPPED | Task runs with UPSTREAM_SKIPPED sentinel |

### OR-join (join="any")

```python
# Task runs when any dependency succeeds
aggregator = TaskNode(
    fn=aggregate,
    waits_for=[branch_a, branch_b, branch_c],
    join="any",
)
```

**Semantics:**
- Task becomes READY when **any** dependency is COMPLETED
- Task is SKIPPED if **all** dependencies fail/skip (none succeeded)
- Failed/skipped deps do NOT block the task (unlike join="all")

### Quorum Join (join="quorum")

```python
# Task runs when at least 2 of 3 dependencies succeed
quorum_task = TaskNode(
    fn=quorum_handler,
    waits_for=[replica_a, replica_b, replica_c],
    join="quorum",
    min_success=2,
)
```

**Semantics:**
- Task becomes READY when `min_success` dependencies are COMPLETED
- Task is SKIPPED if it becomes **impossible** to reach threshold
  - Example: 2 of 3 deps failed, but need 2 successes → impossible → SKIPPED
- `min_success` must be >= 1 and <= number of dependencies

### SKIPPED Propagation

When a task is SKIPPED, all its dependents are also SKIPPED (unless they have `allow_failed_deps=True`). This cascades through the DAG.

```
A (FAILED) → B (SKIPPED) → C (SKIPPED) → D (SKIPPED)
```

### allow_failed_deps=True

Tasks with `allow_failed_deps=True` receive the `TaskResult` from dependencies:

- **FAILED deps**: Receive the actual `TaskResult` with the original error
- **SKIPPED deps**: Receive a sentinel `TaskResult` with `error_code=UPSTREAM_SKIPPED`

```python
from horsies import LibraryErrorCode

@app.task()
def recovery_handler(input_result: TaskResult[Data, TaskError]) -> TaskResult[...]:
    if input_result.is_err():
        err = input_result.unwrap_err()
        if err.error_code == LibraryErrorCode.UPSTREAM_SKIPPED:
            # Dependency was skipped (upstream failure)
            return TaskResult(ok={"status": "skipped_upstream"})
        # Handle actual failure
        return TaskResult(ok={"recovered": True, "error": err})
    return TaskResult(ok={"passed_through": input_result.unwrap()})
```

### UPSTREAM_SKIPPED Sentinel

When a dependency is SKIPPED (due to upstream failure), tasks with `allow_failed_deps=True` receive a sentinel `TaskResult` instead of missing kwargs:

```python
TaskResult(err=TaskError(
    error_code=LibraryErrorCode.UPSTREAM_SKIPPED,
    message="Upstream dependency was SKIPPED",
    data={"dependency_index": <index>},
))
```

This applies to both `args_from` kwargs and `workflow_ctx_from` context data.

## Conditional Execution

Tasks can be conditionally skipped based on runtime conditions using `run_when` and `skip_when` parameters.

### skip_when

Skip the task if the condition returns `True`:

```python
node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

# Skip node_b if node_a's result is > 10
node_b: TaskNode[int] = TaskNode(
    fn=task_b,
    waits_for=[node_a],
    workflow_ctx_from=[node_a],  # Required for condition to access results
    skip_when=lambda ctx: ctx.result_for(node_a).unwrap() > 10,
)
```

### run_when

Run the task only if the condition returns `True`:

```python
node_a: TaskNode[int] = TaskNode(fn=task_a, kwargs={'value': 5})

# Run node_b only if node_a's result is < 100
node_b: TaskNode[int] = TaskNode(
    fn=task_b,
    waits_for=[node_a],
    workflow_ctx_from=[node_a],
    run_when=lambda ctx: ctx.result_for(node_a).unwrap() < 100,
)
```

### Condition Priority

When both `skip_when` and `run_when` are set on the same task:
1. `skip_when` is evaluated first
2. If `skip_when` returns `True` → task is SKIPPED
3. If `skip_when` returns `False` → evaluate `run_when`
4. If `run_when` returns `False` → task is SKIPPED

### Condition Evaluation Timing

Conditions are evaluated:
- **After** all dependencies are in terminal state
- **Before** the task is enqueued

This ensures the condition has access to all dependency results via `WorkflowContext`.

### Important Notes

1. **Workflow registration**: The workflow module must be imported in the worker process
   so the spec is registered. If it isn't, conditions are skipped.

2. **Context sources**: Any TaskNode (or NodeKey) used in the condition via
   `ctx.result_for(...)` must be included in `workflow_ctx_from`

3. **Error handling**: If condition evaluation raises an exception, the task is SKIPPED
   (safe default to prevent hanging workflows)

## DAG Examples

### Linear Chain

```
A → B → C → D
```

- If A fails: B, C, D are SKIPPED
- Workflow: FAILED

### Fan-out / Fan-in

```
    ┌→ B ─┐
A ──┼→ C ─┼→ E
    └→ D ─┘
```

- If B fails: E is SKIPPED (waiting on B)
- C and D still complete
- Workflow: FAILED

### Diamond with Partial Failure

```
        ┌→ B ─┐
    A ──┤     ├→ D
        └→ C ─┘
```

- If B fails: D is SKIPPED (waits for both B and C)
- Even though C succeeds, D doesn't run
- Workflow: FAILED

### Diamond with Recovery

```
        ┌→ B ─┐
    A ──┤     ├→ D (allow_failed_deps=True)
        └→ C ─┘
```

- If B fails: D still runs (receives B's error, C's success)
- D can implement recovery logic
- Workflow: FAILED (but D produced a result)
  > **Note:** `handle.get()` will return the workflow-level failure. To access D's result in this scenario, use `handle.results()["D#3"]` (where 3 is D's task index) or inspect the specific task.

### Multi-Branch with Nested Convergence

```
                    ┌── ca ── e₁
                    │
        ┌── c ──────┤
        │           └── cb ── e₂
a ── b ─┤
        │           ┌── da ── e₃
        └── d ──────┤
                    └── db ── e₄
```

**Current behavior if `c` fails:**
- `ca`, `cb` → SKIPPED
- `e₁`, `e₂` → SKIPPED
- `d`, `da`, `db`, `e₃`, `e₄` → may complete successfully
- Workflow: **FAILED** (because `c` failed)

## Limitations

### Not Supported: Dynamic Task Generation

**Scenario:** "Generate tasks at runtime based on input data"

```python
# Wanted (not implemented):
@workflow
def process_items(items: list[str]):
    for item in items:
        yield TaskNode(fn=process_item, kwargs={'item': item})
```

**Current:** DAG is static at submission time. All tasks must be defined before workflow starts.

## Support Matrix

| Pattern | Supported | Notes |
|---------|-----------|-------|
| Linear chain | Yes | Basic sequential execution |
| Fan-out (parallel branches) | Yes | Independent parallel execution |
| Fan-in (convergence) | Yes | AND-join: all branches must complete |
| Diamond pattern | Yes | Classic DAG pattern |
| AND-join with error handling | Yes | `allow_failed_deps=True` |
| OR-join (any branch succeeds) | Yes | `join="any"` parameter |
| Quorum join (N of M succeed) | Yes | `join="quorum"` with `min_success` |
| Conditional skip | Yes | `run_when` / `skip_when` parameters |
| Partial success workflow | Yes | Via `success_policy` with success cases |
| Dynamic task generation | No | DAG is static at submission time |

## Retry Policies in Workflows

Workflow tasks honor the same retry policies configured on the `@task` decorator. When a task with a `retry_policy` is used in a workflow, those settings (including `auto_retry_for`) are preserved and applied when the workflow engine enqueues the task.

```python
from horsies import RetryPolicy

@app.task(
    retry_policy=RetryPolicy.exponential(
        base_seconds=1,
        max_retries=3,
        auto_retry_for=["TASK_EXCEPTION", "NETWORK_ERROR"],
    ),
)
def fetch_data(url: str) -> TaskResult[dict, TaskError]:
    ...

# This task will retry up to 3 times when used in a workflow
node = TaskNode(fn=fetch_data, kwargs={'url': "https://api.example.com"})
```

**Behavior:**

- `max_retries` from the retry policy is applied to the underlying task
- `auto_retry_for` error codes (on `RetryPolicy`) trigger automatic retries on matching failures
- Retries happen at the task level (worker handles retry scheduling)
- A task that fails after exhausting retries propagates failure to the workflow

## Success Cases (Partial Success Policy)

By default, any task failure marks the workflow as **FAILED**. The `success_policy` option allows defining explicit success criteria, so a workflow can be **COMPLETED** even if some tasks fail.

### SuccessCase and SuccessPolicy

```python
from horsies import SuccessPolicy, SuccessCase

# Define success cases
policy = SuccessPolicy(
    cases=[
        SuccessCase(required=[task_a]),  # Succeed if A completes
        SuccessCase(required=[task_b]),  # OR if B completes
    ],
    optional=[cleanup_task],  # May fail without failing workflow
)

workflow = app.workflow(
    "my_workflow",
    tasks=[task_a, task_b, cleanup_task],
    success_policy=policy,
)
```

### Semantics

1. **Case evaluation**: A `SuccessCase` is **satisfied** if ALL its `required` tasks are COMPLETED.
2. **Workflow success**: Workflow is **COMPLETED** if **any** SuccessCase is satisfied.
3. **SKIPPED counts as not satisfied**: A SKIPPED task does not satisfy a required condition.
4. **Optional tasks**: Tasks in `optional` can fail without affecting success evaluation.
5. **Default behavior**: If `success_policy` is None, any task failure → FAILED (unchanged).

### Example: Shipping Workflow

A package delivery workflow where multiple delivery outcomes are acceptable:

```python
# Define tasks
pickup = TaskNode(fn=pickup_package)
deliver_recipient = TaskNode(fn=deliver_to_recipient, waits_for=[pickup])
deliver_neighbor = TaskNode(fn=deliver_to_neighbor, waits_for=[pickup])
deliver_locker = TaskNode(fn=deliver_to_locker, waits_for=[pickup])
notify = TaskNode(fn=send_notification, waits_for=[pickup])

# Success policy: any delivery method is acceptable
policy = SuccessPolicy(
    cases=[
        SuccessCase(required=[deliver_recipient]),
        SuccessCase(required=[deliver_neighbor]),
        SuccessCase(required=[deliver_locker]),
    ],
    optional=[notify],  # Notification can fail
)

spec = WorkflowSpec(
    name="ship_package",
    tasks=[pickup, deliver_recipient, deliver_neighbor, deliver_locker, notify],
    success_policy=policy,
    broker=broker,
)
```

**Behavior:**
- If `deliver_to_recipient` succeeds → workflow COMPLETED
- If `deliver_to_recipient` fails but `deliver_to_neighbor` succeeds → workflow COMPLETED
- If all delivery methods fail → workflow FAILED
- If `notify` fails but a delivery succeeds → workflow COMPLETED

### Error Handling with Success Policy

When no success case is satisfied:

1. If a **required** task from any case FAILED, the workflow error contains that task's error.
2. If no required task FAILED (all SKIPPED), the error is `WORKFLOW_SUCCESS_CASE_NOT_MET`.

## Best Practices

### 1. Use allow_failed_deps for Recovery

```python
# Primary task that can fail
primary = TaskNode("primary_fetch", fetch_data)

# Recovery handler receives the error
recovery = TaskNode(
    "handle_failure",
    recovery_handler,
    allow_failed_deps=True,
).waits_for(primary).with_args_from(primary=primary)
```

### 2. Design for Failure Visibility

Structure DAGs so failures are visible in the final result:

```python
# Final aggregator that reports all branch outcomes
final = TaskNode(
    "summarize",
    summarize_results,
    allow_failed_deps=True,
).waits_for([branch_a, branch_b, branch_c])
```

### 3. Use on_error="pause" for Critical Workflows

```python
workflow = app.workflow(
    "critical_pipeline",
    tasks=[...],
    on_error="pause",  # Stop for manual review on failure
)
```

### 4. Explicit Output Task

```python
# Designate which task's result is the workflow output
workflow = app.workflow(
    "pipeline",
    tasks=[a, b, c, final],
    output=final,  # workflow.get() returns final's result
)
```

## Result Keying

Results from `WorkflowHandle.results()` and `WorkflowHandle.get()` (when no output task is specified) use unique keys in the format `node_id`.

This ensures no result collisions when the same task function is used multiple times in a workflow.

```python
# Same task used twice
fetch_a = TaskNode(fn=fetch_data, kwargs={'url': "url_a"})  # index 0
fetch_b = TaskNode(fn=fetch_data, kwargs={'url': "url_b"})  # index 1

spec = WorkflowSpec(name="parallel_fetch", tasks=[fetch_a, fetch_b], broker=broker)
handle = await spec.start_async()

# Wait for workflow completion first
await handle.get_async(timeout_ms=30000)

# Now safe to access individual results (node_id keys)
results = handle.results()
result_a = results["parallel_fetch:0"]  # First fetch
result_b = results["parallel_fetch:1"]  # Second fetch

# Typed access using handle.result_for()
# NOTE: result_for() / result_for_async() are non-blocking single queries.
# Always wait for completion first.
typed_a = handle.result_for(fetch_a)
typed_b = handle.result_for(fetch_b)
```

## WorkflowContext (Type-Safe Result Access)

Tasks can access upstream results via `WorkflowContext` using the type-safe `result_for(...)` method.

### Enabling WorkflowContext

1. Set `workflow_ctx_from` on the TaskNode to specify which upstream results to include
2. Declare `workflow_ctx: WorkflowContext | None` parameter in the task function

**Important:** `workflow_ctx_from` is **per-task**. The context is built fresh for each task that opts in, and does **not** persist or propagate to downstream tasks.

**Scope:** Inside a task function, only results from nodes listed in `workflow_ctx_from` are accessible via `workflow_ctx.result_for()`. Accessing a node not in the list raises `KeyError` with `"TaskNode id '{id}' not in workflow context"`.

This is distinct from `WorkflowHandle.result_for()`, which queries the database for any node in the workflow and returns `RESULT_NOT_READY` if the task hasn't completed.

**Condition evaluation:** For `run_when` / `skip_when` lambdas, if `workflow_ctx_from` is set, conditions can only access those nodes. If `workflow_ctx_from` is **not** set, conditions receive a context with **all dependency** results.

```python
from horsies import WorkflowContext, TaskNode, TaskResult, TaskError

node_a: TaskNode[int] = TaskNode(fn=fetch_data, kwargs={'url': "url"})
node_b: TaskNode[str] = TaskNode(fn=transform, waits_for=[node_a], args_from={"data": node_a})
node_c: TaskNode[Summary] = TaskNode(
    fn=aggregate,
    waits_for=[node_a, node_b],
    workflow_ctx_from=[node_a, node_b],  # Include both in context
)

@app.task()
def aggregate(workflow_ctx: WorkflowContext | None = None) -> TaskResult[Summary, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX", message="Missing context"))

    # Type-safe access: result_for returns TaskResult[T, TaskError] matching the node's type
    a_result: TaskResult[int, TaskError] = workflow_ctx.result_for(node_a)
    b_result: TaskResult[str, TaskError] = workflow_ctx.result_for(node_b)

    if a_result.is_err():
        return TaskResult(err=a_result.unwrap_err())

    return TaskResult(ok=Summary(
        data=a_result.unwrap(),
        transformed=b_result.unwrap() if b_result.is_ok() else None,
    ))
```

### Type Safety

`result_for(node: TaskNode[T] | NodeKey[T])` returns `TaskResult[T, TaskError]` where `T` matches the node's generic parameter. This enables static type checking:

```python
# Type checker knows these types
a_result: TaskResult[int, TaskError] = ctx.result_for(node_a)      # node_a is TaskNode[int]
b_result: TaskResult[str, TaskError] = ctx.result_for(node_b)      # node_b is TaskNode[str]

value: int = a_result.unwrap()  # Type checker knows this is int
```

### NodeKey (Stable Lookup)

`WorkflowContext` looks up results by **node_id**, not object identity. This means you can safely use
`TaskNode` instances or typed `NodeKey` objects:

```python
key_a = node_a.key()  # NodeKey[int]
result = workflow_ctx.result_for(key_a)
```

For dynamic workflows where node references are not available inside the task function, construct a `NodeKey` from the string ID:

```python
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    WorkflowContext,
    NodeKey,
    TaskResult,
    TaskError,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)
app = Horsies(config)

@app.task("fan_in")
def fan_in(workflow_ctx: WorkflowContext | None = None) -> TaskResult[str, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX", message="Missing context"))

    i: int = 0
    result = workflow_ctx.result_for(NodeKey(f"save_{i}"))
    if result.is_err():
        return TaskResult(err=result.unwrap_err())
    return TaskResult(ok="ok")
```

The string-based `NodeKey` pattern is common for fan-in tasks that gather results from dynamically created nodes.

If a TaskNode lacks `node_id`, `result_for()` raises:
```
RuntimeError: TaskNode node_id is not set. Ensure WorkflowSpec assigns node_id
or provide an explicit node_id.
```

`node_id` is optional on TaskNode. If omitted, it is auto-assigned during
WorkflowSpec construction using the format `{slugified_workflow_name}:{task_index}`.

Workflow names can contain any characters (including spaces). The `slugify()`
function converts spaces to underscores and removes invalid characters:

```python
from horsies import slugify

# Workflow name with spaces
spec = app.workflow(name="My Data Pipeline", tasks=[a, b])
# Auto-generated node_ids: "My_Data_Pipeline:0", "My_Data_Pipeline:1"

# Manual slugify for custom use
safe_name = slugify("Hello World!")  # "Hello_World"
```

You can also provide an explicit `node_id` for stable external references.
Explicit node_ids must match the pattern `[A-Za-z0-9_\-:.]+`.

### WorkflowMeta (Metadata Only)

For tasks that only need workflow metadata without result access, use `WorkflowMeta`:

```python
from horsies import WorkflowMeta

@app.task()
def my_task(workflow_meta: WorkflowMeta | None = None) -> TaskResult[str, TaskError]:
    if workflow_meta:
        print(f"Running in workflow {workflow_meta.workflow_id}")
        print(f"Task index: {workflow_meta.task_index}")
        print(f"Task name: {workflow_meta.task_name}")
    return TaskResult(ok="done")
```

`WorkflowMeta` is auto-injected if the task declares the parameter. Unlike `WorkflowContext`, it doesn't require `workflow_ctx_from`.

### Comparison: args_from vs workflow_ctx_from

| Feature | `args_from` | `workflow_ctx_from` |
|---------|-------------|---------------------|
| Use case | Direct typed parameters | Access multiple upstream results |
| Type safety | Full (typed parameter) | Full (via `result_for(node)`) |
| Function signature | Explicit parameters | Single `WorkflowContext` param |
| Best for | 1-3 dependencies | Many dependencies, aggregators |

**Prefer `args_from`** for simple cases - it's more explicit and keeps data flow obvious.

**Use `workflow_ctx_from`** when you need to access many upstream results or want workflow metadata.

**Important:** When using `args_from` or `workflow_ctx_from`, positional `args` are not allowed.
Put static values in `kwargs` instead.

**Important:** `kwargs` and `args_from` keys must be disjoint. If the same key appears in both,
validation fails with `E021` (`WORKFLOW_KWARGS_ARGS_FROM_OVERLAP`).

For type-safe node construction with static arguments, see [Typed Node Builder](typed-node-builder).

### args_from: What the Receiving Function Gets

`args_from` delivers the upstream task's full `TaskResult[T, TaskError]` — not the unwrapped `T`.

```python
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    TaskNode,
    TaskResult,
    TaskError,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)
app = Horsies(config)

@app.task("produce_number")
def produce_number() -> TaskResult[int, TaskError]:
    return TaskResult(ok=42)

# transform receives TaskResult[int, TaskError], not int:
@app.task("transform")
def transform(data: TaskResult[int, TaskError]) -> TaskResult[str, TaskError]:
    if data.is_err():
        return TaskResult(err=data.unwrap_err())
    value: int = data.ok_value
    return TaskResult(ok=str(value))

node_a: TaskNode[int] = TaskNode(fn=produce_number)
node_b: TaskNode[str] = TaskNode(
    fn=transform,
    waits_for=[node_a],
    args_from={"data": node_a},
)
```

## Things to Avoid

### Fire-and-Forget with result_for()

`result_for()` is non-blocking. It queries the database once and returns immediately. If the task hasn't completed, it returns `TaskResult(err=TaskError(error_code=RESULT_NOT_READY))`.

**Don't do this:**

```python
# Fire-and-forget: start workflow without waiting
handle = spec.start()

# Immediately call result_for() - task likely hasn't completed yet
result = handle.result_for(node)  # Returns RESULT_NOT_READY error
```

**Do this instead:**

```python
handle = spec.start()

# Option 1: Wait for workflow completion first
handle.get(timeout_ms=30000)
result = handle.result_for(node)  # `RESULT_NOT_READY` is still possible depending on your workflow definition

# Option 2: Check if result is ready
result = handle.result_for(node)
if result.is_err() and result.err.error_code == LibraryErrorCode.RESULT_NOT_READY:
    # Handle not-ready case: poll, wait, or skip, decide for your use case
    do_something()
```

**Why this matters:**

- Workflows can be long-running (minutes to hours)
- `result_for()` does not block or poll - it's a single database query
- Use `handle.get(timeout_ms=...)` to wait for completion before accessing individual task results

For class and method signatures, see [Workflow API](../workflow-api/).
