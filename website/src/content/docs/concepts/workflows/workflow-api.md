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
| `.tasks` | `Sequence[TaskNode[Any] \| SubWorkflowNode[Any]]` | All nodes in the DAG |
| `.on_error` | `OnError` | Error policy (`FAIL` or `PAUSE`) |
| `.output` | `TaskNode[Any] \| SubWorkflowNode[Any] \| None` | Output node for `handle.get()` |
| `.success_policy` | `SuccessPolicy \| None` | Custom success criteria |
| `.resend_on_transient_err` | `bool` | Auto-retry transient start errors (default `False`) |
| `.start(workflow_id=None)` | `(str \| None) -> WorkflowStartResult[WorkflowHandle[T]]` | Start workflow (sync) |
| `.start_async(workflow_id=None)` | `(str \| None) -> WorkflowStartResult[WorkflowHandle[T]]` | Start workflow (async) |
| `.retry_start(error)` | `(WorkflowStartRetryable) -> WorkflowStartResult[WorkflowHandle[T]]` | Retry a failed start (sync) |
| `.retry_start_async(error)` | `(WorkflowStartRetryable) -> WorkflowStartResult[WorkflowHandle[T]]` | Retry a failed start (async) |

### Module-Level Start Functions

For cases where you have a `WorkflowSpec` and a `PostgresBroker` separately (e.g., in tests or custom orchestration), use the module-level functions:

```python
import uuid

from horsies import start_workflow, start_workflow_async

# Sync. A supplied workflow_id must be a UUID string; other values fail with HRS-032.
result = start_workflow(spec, broker, workflow_id=str(uuid.uuid4()))

# Async
result = await start_workflow_async(spec, broker)
```

| Function | Signature |
|---|---|
| `start_workflow(spec, broker, workflow_id=None, sent_at=None, *, resend_on_transient_err=False)` | `-> WorkflowStartResult[WorkflowHandle[T]]` |
| `start_workflow_async(spec, broker, workflow_id=None, sent_at=None, *, resend_on_transient_err=False)` | `-> WorkflowStartResult[WorkflowHandle[T]]` |

These are equivalent to `spec.start()` / `spec.start_async()` but allow passing a broker explicitly instead of relying on the one attached to the spec.

### WorkflowStartResult

`WorkflowStartResult[T] = Result[T, WorkflowStartError]`

Returned by `.start()`, `.start_async()`, `start_workflow()`, and `start_workflow_async()`.

| Outcome | Type | Description |
|---|---|---|
| Success | `Ok(WorkflowHandle[T])` | Workflow created and root tasks enqueued |
| Failure | `Err(WorkflowStartError)` | Start failed; match on the variant |

**WorkflowStartError variants:**

`WorkflowStartError` is a union with one frozen variant per `WorkflowStartErrorCode` member. Every variant has `code`, `message`, `retryable`, `workflow_name` and `workflow_id`. `workflow_id` is the id that this start used: the caller's id, or the id minted for the start. `message` and the cause do not show in `repr()`.

| Variant | `code` | Other fields | `retryable` | After the failure |
|---|---|---|---|---|
| `WorkflowStartBrokerNotConfigured` | `BROKER_NOT_CONFIGURED` | – | `False` | Nothing ran |
| `WorkflowStartValidationFailed` | `VALIDATION_FAILED` | `exception: BaseException \| NoCause` | `False` | Nothing written |
| `WorkflowStartIdConflict` | `WORKFLOW_ID_CONFLICT` | `existing_workflow_name: str` | `False` | Nothing written |
| `WorkflowStartEnqueueFailed` | `ENQUEUE_FAILED` | `exception: BaseException` | Driver classification of the cause | Rolled back, or the commit outcome is unknown |
| `WorkflowStartLoopRunnerFailed` | `LOOP_RUNNER_FAILED` | `exception: BaseException` (the `LoopRunnerError`) | `False` | Nothing ran |
| `WorkflowStartEventLoopLimit` | `EVENT_LOOP_LIMIT` | `loop_limit: EventLoopLimit` | `False` | Nothing written |
| `WorkflowStartAsyncContext` | `ASYNC_CONTEXT` | – | `False` | Nothing written |

`NoCause()` means that a check refused the input without a Python exception. Match on `exception` before `raise ... from`.

**WorkflowStartErrorCode values:**

<!-- members: WorkflowStartErrorCode -->
| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | A start check (`WorkflowValidationError`) refused the start, or a retry of another workflow's error. Any other exception in the start checks is a defect and propagates. In the start transaction, an exception that is not a database or driver error is a defect and propagates, also a `SerializationError` |
| `WORKFLOW_ID_CONFLICT` | No | The workflow id exists for another workflow name; nothing was written. `existing_workflow_name` names it. Use another id |
| `ENQUEUE_FAILED` | Maybe | Schema init or the start transaction failed with a database or driver error. Any other exception in the start transaction is a defect and propagates |
| `LOOP_RUNNER_FAILED` | No | Sync `spec.start()` could not run the call on the broker's sync runner (stopped, thread did not start, or scheduling failed); nothing was written. A stopped runner does not start again: call `await spec.retry_start_async(err)` or use a new broker |
| `EVENT_LOOP_LIMIT` | No | The broker serves its limit of open event loops and this call ran on a new one; nothing was written |
| `ASYNC_CONTEXT` | No | Sync `spec.start()` / `retry_start()` was called on a thread that runs an event loop; nothing was written. Call `await spec.retry_start_async(err)` to complete the start with the error's id |

`WorkflowStartRetryable` is `WorkflowStartEnqueueFailed | WorkflowStartAsyncContext | WorkflowStartLoopRunnerFailed`. It is the parameter type of `.retry_start()` and `.retry_start_async()`; another variant is a type error, and at run time it fails an assertion that names the variant. The retry methods start the workflow again with the error's `workflow_id`. An error of another workflow returns `WorkflowStartValidationFailed`.

**Usage:**

```python
from horsies import Ok, Err, WorkflowStartEnqueueFailed

match spec.start():
    case Ok(handle):
        result = handle.get(timeout_ms=30000)  # Wait for completion
        if result.is_ok():
            print(f"Success: {result.ok_value}")
        else:
            print(f"Task failed: {result.err_value.error_code}")

    case Err(WorkflowStartEnqueueFailed(retryable=True) as err):
        # Transient infra failure — retry with stored workflow_id
        match spec.retry_start(err):
            case Ok(handle):
                result = handle.get(timeout_ms=30000)
                if result.is_err():
                    print(f"Task failed: {result.err_value.error_code}")
            case Err(retry_err):
                print(f"Retry failed: {retry_err.code} - {retry_err.message}")

    case Err(err):
        print(f"[{err.code}] {err.message}")
```

> **Idempotency caveat:** `retry_start` is best-effort idempotent by `workflow_id`.
> Unlike task send, workflow start does not verify payload identity on collision.
> Safe for transient DB retries within the same deploy. Cross-version spec drift
> on the same `workflow_id` will silently return the existing handle.

### WorkflowHandle

| Attribute / Method | Type / Signature | Description |
|---|---|---|
| `.workflow_id` | `str` | Workflow UUID |
| `.status()` / `.status_async()` | `-> HandleResult[WorkflowStatus]` | Current workflow status |
| `.get()` / `.get_async()` | `(timeout_ms: int \| None) -> TaskResult[OutT, TaskError]` | Block until completion or timeout |
| `.results()` / `.results_async()` | `-> HandleResult[dict[str, TaskResult[Any, TaskError]]]` | All results keyed by node_id; `Ok({})` before any node has a result, `HandleWorkflowNotFound` when no workflow has the id |
| `.result_for()` / `.result_for_async()` | `(TaskNode[T] \| NodeKey[T]) -> TaskResult[T, TaskError]` | Single node result (non-blocking) |
| `.tasks()` / `.tasks_async()` | `-> HandleResult[list[WorkflowTaskInfo]]` | Status of all workflow tasks; `HandleWorkflowNotFound` when no workflow has the id |
| `.cancel()` / `.cancel_async()` | `-> HandleResult[None]` | Cancel workflow |
| `.pause()` / `.pause_async()` | `-> HandleResult[bool]` | Pause running workflow |
| `.resume()` / `.resume_async()` | `-> HandleResult[bool]` | Resume paused workflow |

### HandleResult

`HandleResult[T] = Result[T, HandleOperationError]`

Returned by the `WorkflowHandle` methods `.status()`, `.results()`, `.tasks()`, `.cancel()`, `.pause()`, `.resume()` and their `*_async` forms, and by `pause_workflow()`, `resume_workflow()`, `pause_workflow_sync()` and `resume_workflow_sync()`.

**HandleOperationError variants:**

`HandleOperationError` is a union with one frozen variant per `HandleErrorCode` member. Every variant has `code`, `message`, `retryable` and `workflow_id`. `message` and the cause do not show in `repr()`. Any other exception, for example a defect in pause or resume, propagates.

| Variant | `code` | Other fields | `retryable` | After the failure |
|---|---|---|---|---|
| `HandleWorkflowNotFound` | `WORKFLOW_NOT_FOUND` | – | `False` | Nothing changed |
| `HandleDbOperationFailed` | `DB_OPERATION_FAILED` | `exception: BaseException` | Driver classification of the cause | A read changed nothing; a write rolled back, or the commit outcome is unknown |
| `HandleStoredDataInvalid` | `STORED_DATA_INVALID` | `exception: BaseException` | `False` | Nothing changed; a stored workflow status or subworkflow summary does not parse |
| `HandleResumeRecoveryFailed` | `RESUME_RECOVERY_FAILED` | `exception: BaseException` | `False` | The resume committed and the workflow is `RUNNING`; the recovery pass after the commit failed |
| `HandleLoopRunnerFailed` | `LOOP_RUNNER_FAILED` | `exception: BaseException` (the `LoopRunnerError`) | `False` | Nothing ran |
| `HandleEventLoopLimit` | `EVENT_LOOP_LIMIT` | `loop_limit: EventLoopLimit` | `False` | Nothing ran |
| `HandleAsyncContext` | `ASYNC_CONTEXT` | – | `False` | Nothing ran |

See [Errors Reference](../../tasks/errors#workflow-handle-errors-handleresult) for when each code occurs.

**Usage:**

<!-- check: skip defect -- does not type-check: reportPossiblyUnboundVariable -->
```python
from horsies import Err, HandleResumeRecoveryFailed, Ok

match handle.resume():
    case Ok(True):
        print("resumed")
    case Ok(False):
        print("not PAUSED; nothing changed")
    case Err(HandleResumeRecoveryFailed()):
        # The resume committed. The worker reaper runs the recovery pass again.
        print("resumed; the recovery pass failed")
    case Err(err):
        print(f"[{err.code}] {err.message}")
```

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
| `priority` | `int` | `None` | Priority override, from 1 (highest) to 100 (lowest). A value outside 1–100 raises `HRS-034` when the spec is built |
| `allow_failed_deps` | `bool` | `False` | Run despite failed deps |
| `join` | `Literal['all', 'any', 'quorum']` | `'all'` | Dependency join semantics |
| `min_success` | `int` | `None` | Required for `join='quorum'` |
| `good_until` | `datetime` | `None` | Task expiry deadline. Must be timezone-aware: a naive `datetime` raises `HRS-035` when the spec is built |

See [Typed Node Builder](typed-node-builder) for usage examples.

### `@app.workflow_builder`

Register a function that builds a `WorkflowSpec` for validation during `horsies check`. Registered builders are executed under send suppression (no tasks are enqueued) so the returned `WorkflowSpec` can be fully validated — DAG structure, kwargs against function signatures, `args_from` type compatibility, etc.

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Horsies, WorkflowSpec

app = Horsies(config)

# Zero-parameter builder — called automatically during check
@app.workflow_builder()
def build_etl_pipeline() -> WorkflowSpec:
    return app.workflow(
        name="etl",
        tasks=[...],
        definition_key="myapp.etl.v1",
    )

# Parameterized builder — provide cases= for check to exercise
@app.workflow_builder(cases=[
    {"region": "us-east"},
    {"region": "eu-west"},
])
def build_regional_pipeline(region: str) -> WorkflowSpec:
    return app.workflow(
        name=f"pipeline-{region}",
        tasks=[...],
        definition_key=f"myapp.pipeline.{region}.v1",
    )
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cases` | `list[dict[str, Any]] \| None` | No | Kwarg dicts to invoke the builder with during check. Required when the builder has parameters without defaults. |

**Errors:**

<!-- members: ErrorCode subset -->
| Code | When |
|------|------|
| HRS-027 | Parameterized builder missing `cases=` |
| HRS-029 | Builder raises an exception, returns non-`WorkflowSpec`, or returns a spec missing required validation like `definition_key` |
| HRS-030 | Function returns `WorkflowSpec` but lacks `@app.workflow_builder` |

For the guarantee model and CI usage, see [Startup Validation](../../configuration/app-config#startup-validation-appcheck).
