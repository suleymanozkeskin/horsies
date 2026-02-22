---
title: Workflow API
summary: Signatures and return types for WorkflowSpec and WorkflowHandle.
related: [workflow-semantics, subworkflows]
tags: [concepts, workflows, api, reference]
---

# Workflow API

Use this page for the exact method signatures and return types used by workflows.

## API Reference

### WorkflowSpec

| Attribute / Method | Type / Signature | Description |
|---|---|---|
| `.name` | `str` | Workflow name |
| `.tasks` | `list[TaskNode[Any] \| SubWorkflowNode[Any]]` | All nodes in the DAG |
| `.on_error` | `OnError` | Error policy (`FAIL` or `PAUSE`) |
| `.output` | `TaskNode[Any] \| SubWorkflowNode[Any] \| None` | Output node for `handle.get()` |
| `.success_policy` | `SuccessPolicy \| None` | Custom success criteria |
| `.start(workflow_id=None)` | `(str \| None) -> WorkflowStartResult[WorkflowHandle[T]]` | Start workflow (sync) |
| `.start_async(workflow_id=None)` | `(str \| None) -> WorkflowStartResult[WorkflowHandle[T]]` | Start workflow (async) |

### WorkflowStartResult

`WorkflowStartResult[T] = Result[T, WorkflowStartError]`

Returned by `.start()`, `.start_async()`, `start_workflow()`, and `start_workflow_async()`.

| Outcome | Type | Description |
|---|---|---|
| Success | `Ok(WorkflowHandle[T])` | Workflow created and root tasks enqueued |
| Failure | `Err(WorkflowStartError)` | Start failed with categorized error |

**WorkflowStartError fields:**

| Field | Type | Description |
|---|---|---|
| `code` | `WorkflowStartErrorCode` | Failure category |
| `message` | `str` | Human-readable description |
| `retryable` | `bool` | Whether caller can safely retry |
| `stage` | `WorkflowStartStage` | Pipeline stage where failure occurred |
| `workflow_name` | `str` | Workflow spec name |
| `workflow_id` | `str` | Generated workflow ID (always populated) |
| `exception` | `BaseException \| None` | Original cause |
| `details` | `dict[str, Any] \| None` | Structured metadata |

**WorkflowStartErrorCode values:**

| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | DAG validation failed (unresolved queue/priority) |
| `SERIALIZATION_FAILED` | No | Args/kwargs serialization failed |
| `SCHEMA_INIT_FAILED` | Maybe | Database schema initialization failed |
| `DB_OPERATION_FAILED` | Maybe | Database transaction failed |
| `LOOP_RUNNER_FAILED` | No | Sync bridge infrastructure failure |
| `INTERNAL_FAILED` | No | Unexpected sync-path exception |

**WorkflowStartStage values:**

| Stage | Description |
|---|---|
| `PREVALIDATE` | Pre-submission validation |
| `ENSURE_SCHEMA` | Schema initialization |
| `DB_TRANSACTION` | Database insert/enqueue |
| `SYNC_BRIDGE` | Sync wrapper infrastructure |

**Usage:**

```python
from horsies import is_err

result = spec.start()
if is_err(result):
    err = result.err_value
    print(f"[{err.code}] {err.message} (retryable={err.retryable})")
    return

handle = result.ok_value  # WorkflowHandle[T]
```

### WorkflowHandle

| Attribute / Method | Type / Signature | Description |
|---|---|---|
| `.workflow_id` | `str` | Workflow UUID |
| `.status()` / `.status_async()` | `-> WorkflowStatus` | Current workflow status |
| `.get()` / `.get_async()` | `(timeout_ms: int \| None) -> TaskResult[Any, TaskError]` | Block until completion or timeout |
| `.results()` / `.results_async()` | `-> dict[str, TaskResult[Any, TaskError]]` | All results keyed by node_id |
| `.result_for()` / `.result_for_async()` | `(TaskNode[T] \| NodeKey[T]) -> TaskResult[T, TaskError]` | Single node result (non-blocking) |
| `.tasks()` / `.tasks_async()` | `-> list[WorkflowTaskInfo]` | Status of all workflow tasks |
| `.cancel()` / `.cancel_async()` | `-> None` | Cancel workflow |
| `.pause()` / `.pause_async()` | `-> bool` | Pause running workflow |
| `.resume()` / `.resume_async()` | `-> bool` | Resume paused workflow |

### WorkflowTaskInfo

| Attribute | Type | Description |
|---|---|---|
| `.node_id` | `str \| None` | Node identifier (may be None for legacy rows) |
| `.index` | `int` | Task position in the DAG |
| `.name` | `str` | Task name |
| `.status` | `WorkflowTaskStatus` | Current task status |
| `.result` | `TaskResult[Any, TaskError] \| None` | Task result if stored (COMPLETED/FAILED; SKIPPED often None) |
| `.started_at` | `datetime \| None` | When execution started |
| `.completed_at` | `datetime \| None` | When execution completed |

### TaskFunction.node()

The `.node()` method on task functions returns a `NodeFactory` for type-safe `TaskNode` creation.

| Method | Signature | Description |
|---|---|---|
| `.node(...)` | `(**workflow_opts) -> NodeFactory[P, T]` | Create a factory with workflow options |
| `NodeFactory(...)` | `(*args, **kwargs) -> TaskNode[T]` | Call factory with typed task arguments |

**Workflow options** (all keyword-only, all optional):

| Parameter | Type | Default | Description |
|---|---|---|---|
| `waits_for` | `Sequence[TaskNode \| SubWorkflowNode]` | `None` | Dependencies |
| `args_from` | `dict[str, TaskNode \| SubWorkflowNode]` | `None` | Result injection mapping |
| `workflow_ctx_from` | `Sequence[TaskNode \| SubWorkflowNode]` | `None` | Context sources |
| `node_id` | `str` | `None` | Stable identifier |
| `queue` | `str` | `None` | Queue override |
| `priority` | `int` | `None` | Priority override |
| `allow_failed_deps` | `bool` | `False` | Run despite failed deps |
| `run_when` | `Callable[[WorkflowContext], bool]` | `None` | Conditional execution |
| `skip_when` | `Callable[[WorkflowContext], bool]` | `None` | Conditional skip |
| `join` | `Literal['all', 'any', 'quorum']` | `'all'` | Dependency join semantics |
| `min_success` | `int` | `None` | Required for `join='quorum'` |
| `good_until` | `datetime` | `None` | Task expiry deadline |

See [Typed Node Builder](typed-node-builder) for usage examples.

### `@app.workflow_builder`

Register a function that builds a `WorkflowSpec` for validation during `horsies check`. Registered builders are executed under send suppression (no tasks are enqueued) so the returned `WorkflowSpec` can be fully validated — DAG structure, kwargs against function signatures, `args_from` type compatibility, etc.

```python
from horsies import Horsies, WorkflowSpec

app = Horsies(config)

# Zero-parameter builder — called automatically during check
@app.workflow_builder()
def build_etl_pipeline() -> WorkflowSpec:
    return app.workflow("etl", tasks=[...])

# Parameterized builder — provide cases= for check to exercise
@app.workflow_builder(cases=[
    {"region": "us-east"},
    {"region": "eu-west"},
])
def build_regional_pipeline(region: str) -> WorkflowSpec:
    return app.workflow(f"pipeline-{region}", tasks=[...])
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cases` | `list[dict[str, Any]] \| None` | No | Kwarg dicts to invoke the builder with during check. Required when the builder has parameters without defaults. |

**Errors:**

| Code | When |
|------|------|
| E027 | Parameterized builder missing `cases=` |
| E029 | Builder raises an exception or does not return a `WorkflowSpec` |
| E030 | Function returns `WorkflowSpec` but lacks `@app.workflow_builder` |

For the guarantee model and CI usage, see [Startup Validation](../../configuration/app-config#startup-validation-appcheck).
