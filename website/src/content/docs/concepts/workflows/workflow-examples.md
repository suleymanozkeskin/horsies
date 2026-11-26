---
title: Workflow Examples
section: python
summary: Python examples of the workflow rules - status helpers, failed and skipped dependencies, resume, joins, sentinels, retries, success policies, result keys, WorkflowContext, WorkflowMeta, and node result reads.
related: [workflow-semantics, workflow-api, typed-node-builder, subworkflows]
tags: [workflows, examples, TaskNode, WorkflowContext, SuccessPolicy, NodeKey, TaskOutcome, ReadResult]
---

# Workflow Examples

Each section shows the Python form of a rule on [Workflow Semantics](/horsies/concepts/workflows/workflow-semantics/). The signatures are in [Workflow API](/horsies/concepts/workflows/workflow-api/).

## Status Enums

Each status has `is_terminal`. The frozenset constants hold the terminal statuses:

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
WORKFLOW_TERMINAL_STATES       # frozenset({COMPLETED, FAILED, CANCELLED, EXPIRED})
WORKFLOW_TASK_TERMINAL_STATES  # frozenset({COMPLETED, FAILED, SKIPPED})
```

## Upstream Failure and args_from

`process` receives the `TaskOutcome` of `produce`: `Succeeded(value)` or `Failed(task_error)`. With the defaults, the node is SKIPPED when `produce` fails. With `allow_failed_dependencies=True`, it runs and receives `Failed(task_error)`. See [Upstream Failure and args_from](/horsies/concepts/workflows/workflow-semantics/#upstream-failure-and-args_from).

```python
from pydantic import SecretStr
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    TaskNode,
    Err,
    Failed,
    Ok,
    Result,
    Succeeded,
    TaskError,
    TaskOutcome,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
)
app = Horsies(config)

@app.task("produce")
def produce() -> Result[int, TaskError]:
    return Ok(1)

@app.task("process")
def process(*, data: TaskOutcome[int, TaskError]) -> Result[str, TaskError]:
    match data:
        case Failed():
            return Err(TaskError(
                error_code="UPSTREAM_FAILED",
                message="Upstream task failed",
            ))
        case Succeeded(value):
            return Ok(str(value))

node_a: TaskNode[int] = TaskNode(fn=produce)

# Default (join="all", allow_failed_dependencies=False): downstream is SKIPPED when upstream fails
node_b_skip: TaskNode[str] = TaskNode(
    fn=process,
    waits_for=[node_a],
    args_from={"data": node_a},
)

# With allow_failed_dependencies: downstream runs and receives Failed(task_error)
node_b_recover: TaskNode[str] = TaskNode(
    fn=process,
    waits_for=[node_a],
    args_from={"data": node_a},
    allow_failed_dependencies=True,
)
```

## Resume

<!-- check: skip partial -- uses a name before the page defines it -->
```python
from horsies import Err, Ok

match await spec.start_async():
    case Err(start_error):
        raise RuntimeError(start_error.message)
    case Ok(handle):
        pass

# ... workflow pauses due to task failure ...

# Resume the workflow
resumed = await handle.resume_async()  # Ok(bool) or Err(HandleOperationError): match on it
```

See [Resume](/horsies/concepts/workflows/workflow-semantics/#resume) for what a resume does and how it can fail.

## Join Modes

See [Join Modes](/horsies/concepts/workflows/workflow-semantics/#join-modes).

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Task runs when any dependency succeeds
aggregator = TaskNode(
    fn=aggregate,
    waits_for=[branch_a, branch_b, branch_c],
    join="any",
)
```

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Task runs when at least 2 of 3 dependencies succeed
quorum_task = TaskNode(
    fn=quorum_handler,
    waits_for=[replica_a, replica_b, replica_c],
    join="quorum",
    min_success=2,
)
```

## Recovery Handlers

A node with `allow_failed_dependencies=True` receives `Failed(task_error)` for a failed dependency, or the `UPSTREAM_SKIPPED` sentinel for a skipped dependency. See [allow_failed_dependencies=True](/horsies/concepts/workflows/workflow-semantics/#allow_failed_dependenciestrue).

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Failed, Ok, OutcomeCode, Result, Succeeded, TaskError, TaskOutcome

@app.task(task_name="recovery_handler")
def recovery_handler(*, input_result: TaskOutcome[Data, TaskError]) -> Result[...]:
    match input_result:
        case Failed(err) if err.error_code == OutcomeCode.UPSTREAM_SKIPPED:
            # Dependency was skipped (upstream failure)
            return Ok({"status": "skipped_upstream"})
        case Failed(err):
            # Handle actual failure
            return Ok({"recovered": True, "error": err})
        case Succeeded(data):
            return Ok({"passed_through": data})
```

## Sentinels

The entry that a node with `allow_failed_dependencies=True` receives for a SKIPPED dependency:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
Failed(TaskError(
    error_code=OutcomeCode.UPSTREAM_SKIPPED,
    message="Upstream dependency was SKIPPED",
    data={"dependency_index": <index>},
))
```

The entry for a dependency with no stored result:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
Failed(TaskError(
    error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
    message="Dependency node <node_id> (index <index>, task <task_name>) is <status> with no stored result",
    data={"dependency_index": <index>},
))
```

## Partial Inputs

With `join="any"` or `join="quorum"`, each `args_from` parameter declares a default. See [args_from with join='any' / 'quorum'](/horsies/concepts/workflows/workflow-semantics/#args_from-with-joinany--quorum).

```python
@app.task("partial_consumer")
def partial_consumer(
    *,
    b_result: TaskOutcome[int, TaskError] | None = None,
    c_result: TaskOutcome[int, TaskError] | None = None,
) -> Result[str, TaskError]:
    ...
```

## Retry Policies in Workflows

```python
from horsies import RetryPolicy, Result, TaskError, JsonValue

@app.task(
    "fetch_data",
    retry_policy=RetryPolicy.exponential(
        base_seconds=1,
        max_retries=3,
        auto_retry_for=["UNHANDLED_EXCEPTION", "NETWORK_ERROR"],
    ),
)
def fetch_data(*, url: str) -> Result[dict[str, JsonValue], TaskError]:
    ...

# This task will retry up to 3 times when used in a workflow
node = TaskNode(fn=fetch_data, kwargs={'url': "https://api.example.com"})
```

## Success Policy

See [Success Cases](/horsies/concepts/workflows/workflow-semantics/#success-cases-partial-success-policy) for the rules.

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import SuccessPolicy, SuccessCase

# Define success cases
policy = SuccessPolicy(
    cases=[
        SuccessCase(required=[task_a]),  # Succeed if A completes
        SuccessCase(required=[task_b]),  # OR if B completes
    ],
    optional=[cleanup_task],  # Declarative: no runtime effect
)

workflow = app.workflow(
    "my_workflow",
    tasks=[task_a, task_b, cleanup_task],
    success_policy=policy,
    definition_key="myapp.my_workflow.v1",
)
```

### Example: Shipping Workflow

A package delivery workflow where multiple delivery outcomes are acceptable:

<!-- check: skip partial -- uses names that the page does not define -->
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
    optional=[notify],  # Declarative: no case requires notify
)

spec = app.workflow(
    name="ship_package",
    tasks=[pickup, deliver_recipient, deliver_neighbor, deliver_locker, notify],
    success_policy=policy,
    definition_key="myapp.ship_package.v1",
)
```

**Behavior:**
- If `deliver_to_recipient` succeeds → workflow COMPLETED
- If `deliver_to_recipient` fails but `deliver_to_neighbor` succeeds → workflow COMPLETED
- If all delivery methods fail → workflow FAILED
- If `notify` fails but a delivery succeeds → workflow COMPLETED

## Output Node Under a Success Policy

A COMPLETED workflow whose output node is SKIPPED returns `WORKFLOW_OUTPUT_SKIPPED`. See [Output Node Under a Success Policy](/horsies/concepts/workflows/workflow-semantics/#output-node-under-a-success-policy).

<!-- check: alone -->
```python
from horsies import Err, Failed, Ok, OutcomeCode, Succeeded, WorkflowHandle


def read_label(handle: WorkflowHandle[str]) -> str | None:
    match handle.get(timeout_ms=30000):
        case Ok(Succeeded(label)):
            return label
        case Ok(Failed(error)) if error.error_code == OutcomeCode.WORKFLOW_OUTPUT_SKIPPED:
            return None  # The workflow succeeded and has no label.
        case Ok(Failed(error)):
            raise RuntimeError(error.message)
        case Err(read_error):
            raise RuntimeError(read_error.message)
```

## Best Practices

### 1. Use allow_failed_dependencies for Recovery

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Primary task that can fail
primary = TaskNode(fn=fetch_data)

# Recovery handler receives the error
recovery = TaskNode(
    fn=recovery_handler,
    allow_failed_dependencies=True,
    waits_for=[primary],
    args_from={"primary": primary},
)
```

### 2. Design for Failure Visibility

Structure DAGs so failures are visible in one node's result. The workflow is still FAILED, so read the aggregator with `result_for(final)`:

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Final aggregator that reports all branch outcomes
final = TaskNode(
    fn=summarize_results,
    allow_failed_dependencies=True,
    waits_for=[branch_a, branch_b, branch_c],
)
```

### 3. Use OnError.PAUSE for Critical Workflows

```python
from horsies import OnError, Ok, Result, TaskError, TaskNode

@app.task("review_step")
def review_step() -> Result[str, TaskError]:
    return Ok("done")

workflow = app.workflow(
    "critical_pipeline",
    tasks=[TaskNode(fn=review_step, node_id="review")],
    on_error=OnError.PAUSE,  # Stop for manual review on failure
    definition_key="myapp.critical_pipeline.v1",
)
```

### 4. Explicit Output Task

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Designate which task's result is the workflow output
workflow = app.workflow(
    "pipeline",
    tasks=[a, b, c, final],
    output=final,  # workflow.get() returns final's result
    definition_key="myapp.pipeline.v1",
)
```

## Result Keying

```python
# Same task used twice
fetch_a = TaskNode(fn=fetch_data, kwargs={'url': "url_a"})  # index 0
fetch_b = TaskNode(fn=fetch_data, kwargs={'url': "url_b"})  # index 1

spec = WorkflowSpec(
    name="parallel_fetch",
    tasks=[fetch_a, fetch_b],
    broker=broker,
    definition_key="myapp.parallel_fetch.v1",
)

# start_async() returns Ok(handle) or Err(WorkflowStartError)
match await spec.start_async():
    case Err(start_error):
        raise RuntimeError(start_error.message)
    case Ok(handle):
        pass

# Wait for workflow completion first
await handle.get_async(timeout_ms=30000)

# Now safe to access individual results (node_id keys)
# results() returns Ok(dict) or Err(HandleOperationError)
match handle.results():
    case Err(results_error):
        raise RuntimeError(results_error.message)
    case Ok(results):
        pass
result_a = results["parallel_fetch:0"]  # First fetch: a TaskOutcome
result_b = results["parallel_fetch:1"]  # Second fetch: a TaskOutcome

# Typed access using handle.result_for(): a ReadResult
# NOTE: result_for() / result_for_async() are non-blocking single queries.
# Always wait for completion first.
typed_a = handle.result_for(fetch_a)
typed_b = handle.result_for(fetch_b)
```

## WorkflowContext

Set `workflow_context_from` on the node, and declare `workflow_ctx: WorkflowContext | None` in the task. See [WorkflowContext](/horsies/concepts/workflows/workflow-semantics/#workflowcontext-type-safe-result-access) for its scope.

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Failed, Ok, Result, Succeeded, TaskError, TaskNode, TaskOutcome, WorkflowContext

node_a: TaskNode[int] = TaskNode(fn=fetch_data, kwargs={'url': "url"})
node_b: TaskNode[str] = TaskNode(fn=transform, waits_for=[node_a], args_from={"data": node_a})
node_c: TaskNode[Summary] = TaskNode(
    fn=aggregate,
    waits_for=[node_a, node_b],
    workflow_context_from=[node_a, node_b],  # Include both in context
)

@app.task(task_name="aggregate")
def aggregate(*, workflow_ctx: WorkflowContext | None = None) -> Result[Summary, TaskError]:
    if workflow_ctx is None:
        return Err(TaskError(error_code="NO_CTX", message="Missing context"))

    # Type-safe access: result_for returns TaskOutcome[T, TaskError] matching the node's type
    a_result: TaskOutcome[int, TaskError] = workflow_ctx.result_for(node_a)
    b_result: TaskOutcome[str, TaskError] = workflow_ctx.result_for(node_b)

    match (a_result, b_result):
        case (Failed(error), _):
            return Err(error)
        case (Succeeded(data), Succeeded(transformed)):
            return Ok(Summary(data=data, transformed=transformed))
        case (Succeeded(data), Failed()):
            return Ok(Summary(data=data, transformed=None))
```

### Type Safety

`result_for(node: TaskNode[T] | NodeKey[T])` returns `TaskOutcome[T, TaskError]` where `T` matches the node's generic parameter. This enables static type checking:

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Type checker knows these types
a_result: TaskOutcome[int, TaskError] = ctx.result_for(node_a)      # node_a is TaskNode[int]
b_result: TaskOutcome[str, TaskError] = ctx.result_for(node_b)      # node_b is TaskNode[str]

match a_result:
    case Succeeded(value):
        reveal_type(value)  # int
    case Failed(error):
        reveal_type(error)  # TaskError
```

### NodeKey (Stable Lookup)

`result_for` takes a `TaskNode` or a typed `NodeKey`. The lookup is by node id: see [Node IDs](/horsies/concepts/workflows/workflow-semantics/#node-ids).

<!-- check: skip partial -- uses names that the page does not define -->
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
    Err,
    Failed,
    Ok,
    Result,
    Succeeded,
    TaskError,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
)
app = Horsies(config)

@app.task("fan_in")
def fan_in(*, workflow_ctx: WorkflowContext | None = None) -> Result[str, TaskError]:
    if workflow_ctx is None:
        return Err(TaskError(error_code="NO_CTX", message="Missing context"))

    i: int = 0
    match workflow_ctx.result_for(NodeKey(f"save_{i}")):
        case Failed(error):
            return Err(error)
        case Succeeded():
            return Ok("ok")
```

The string-based `NodeKey` pattern is common for fan-in tasks that gather results from dynamically created nodes.

If a TaskNode lacks `node_id`, `result_for()` raises:
```
RuntimeError: TaskNode node_id is not set. Ensure WorkflowSpec assigns node_id
or provide an explicit node_id.
```

`slugify()` gives the slug that an auto-assigned node id starts with:

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import slugify

# Workflow name with spaces
spec = app.workflow(
    name="My Data Pipeline",
    tasks=[a, b],
    definition_key="myapp.my_data_pipeline.v1",
)
# Auto-generated node_ids: "My_Data_Pipeline:0", "My_Data_Pipeline:1"

# Manual slugify for custom use
safe_name = slugify("Hello World!")  # "Hello_World"
```

## WorkflowMeta

A task that needs only the workflow metadata declares a `WorkflowMeta` parameter:

```python
from horsies import WorkflowMeta

@app.task(task_name="my_task")
def my_task(*, workflow_meta: WorkflowMeta | None = None) -> Result[str, TaskError]:
    if workflow_meta:
        print(f"Running in workflow {workflow_meta.workflow_id}")
        print(f"Task index: {workflow_meta.task_index}")
        print(f"Task name: {workflow_meta.task_name}")
    return Ok("done")
```

`WorkflowMeta` is auto-injected if the task declares the parameter. Unlike `WorkflowContext`, it doesn't require `workflow_context_from`.

## What the Receiving Function Gets

`args_from` delivers the upstream task's `TaskOutcome[T, TaskError]`, not the raw `T`:

```python
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    TaskNode,
    Err,
    Failed,
    Ok,
    Result,
    Succeeded,
    TaskError,
    TaskOutcome,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
)
app = Horsies(config)

@app.task("produce_number")
def produce_number() -> Result[int, TaskError]:
    return Ok(42)

# transform receives TaskOutcome[int, TaskError], not int:
@app.task("transform")
def transform(*, data: TaskOutcome[int, TaskError]) -> Result[str, TaskError]:
    match data:
        case Failed(error):
            return Err(error)
        case Succeeded(value):
            return Ok(str(value))

node_a: TaskNode[int] = TaskNode(fn=produce_number)
node_b: TaskNode[str] = TaskNode(
    fn=transform,
    waits_for=[node_a],
    args_from={"data": node_a},
)
```

## Things to Avoid

### Fire-and-Forget with result_for()

`result_for()` is non-blocking. It queries the database once and returns immediately. If the task hasn't completed, it returns `Err(ResultNotReady(...))`.

**Don't do this:**

```python
# Fire-and-forget: start workflow without waiting
match spec.start():
    case Err(start_error):
        raise RuntimeError(start_error.message)
    case Ok(handle):
        # Immediately call result_for() - task likely hasn't completed yet
        result = handle.result_for(node)  # Returns Err(ResultNotReady(...))
```

**Do this instead:**

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Err, Ok, ResultNotReady

match spec.start():
    case Err(start_error):
        raise RuntimeError(start_error.message)
    case Ok(handle):
        pass

# Option 1: Wait for workflow completion first
handle.get(timeout_ms=30000)
result = handle.result_for(node)  # `ResultNotReady` is still possible depending on your workflow definition

# Option 2: Check if result is ready
match handle.result_for(node):
    case Err(ResultNotReady()):
        # Handle not-ready case: poll, wait, or skip, decide for your use case
        do_something()
    case _:
        pass  # an outcome or another read error: see Retrieving Results
```

**Why this matters:**

- Workflows can be long-running (minutes to hours)
- `result_for()` does not block or poll - it's a single database query
- Use `handle.get(timeout_ms=...)` to wait for completion before accessing individual task results
