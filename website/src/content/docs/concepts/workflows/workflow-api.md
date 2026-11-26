---
title: Workflow API
section: python
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
| `WorkflowStartCommandConflict` | `WORKFLOW_COMMAND_CONFLICT` | – | `False` | Nothing written |
| `WorkflowStartEnqueueFailed` | `ENQUEUE_FAILED` | `exception: BaseException` | The core's classification of the cause | Rolled back, or the commit outcome is unknown |
| `WorkflowStartAsyncContext` | `ASYNC_CONTEXT` | – | `False` | Nothing written |

`NoCause()` means that a check refused the input without a Python exception. Match on `exception` before `raise ... from`.

**WorkflowStartErrorCode values:**

<!-- members: WorkflowStartErrorCode -->
| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | A start check (`WorkflowValidationError`) refused the start, or a retry of another workflow's error. Any other exception in the start checks is a defect and propagates. In the start transaction, an exception that is not a database or driver error is a defect and propagates, also a `SerializationError` |
| `WORKFLOW_ID_CONFLICT` | No | The workflow id exists for another workflow name; nothing was written. `existing_workflow_name` names it. Use another id |
| `WORKFLOW_COMMAND_CONFLICT` | No | The workflow id holds a workflow of the same name that another start command started, or a sub-workflow; nothing was written. Send the same start command, or use another id. See [Start a Workflow Once per Entity](/horsies/guides/start-workflow-once/) |
| `ENQUEUE_FAILED` | Maybe | Schema init or the start transaction failed with a database or driver error. Any other exception in the start transaction is a defect and propagates |
| `ASYNC_CONTEXT` | No | Sync `spec.start()` / `retry_start()` was called on a thread that runs an event loop; nothing was written. Call `await spec.retry_start_async(err)` to complete the start with the error's id |

`WorkflowStartRetryable` is `WorkflowStartEnqueueFailed | WorkflowStartAsyncContext`. It is the parameter type of `.retry_start()` and `.retry_start_async()`; another variant is a type error, and at run time it fails an assertion that names the variant. The retry methods start the workflow again with the error's `workflow_id`. An error of another workflow returns `WorkflowStartValidationFailed`.

**Usage:**

```python
from horsies import Err, Failed, Ok, Succeeded, WorkflowStartEnqueueFailed

match spec.start():
    case Ok(handle):
        match handle.get(timeout_ms=30000):  # Wait for completion
            case Ok(Succeeded(value)):
                print(f"Success: {value}")
            case Ok(Failed(task_error)):
                print(f"Workflow failed: {task_error.code}")
            case Err(read_error):
                print(f"Read failed: {read_error.code}")

    case Err(WorkflowStartEnqueueFailed(retryable=True) as err):
        # Transient infra failure — retry with stored workflow_id
        match spec.retry_start(err):
            case Ok(handle):
                match handle.get(timeout_ms=30000):
                    case Ok(Failed(task_error)):
                        print(f"Workflow failed: {task_error.code}")
                    case _:
                        pass
            case Err(retry_err):
                print(f"Retry failed: {retry_err.code} - {retry_err.message}")

    case Err(err):
        print(f"[{err.code}] {err.message}")
```

> **Idempotency:** a start with an existing `workflow_id` compares its start command
> with the stored one. The same command returns the existing handle. Another command
> returns `WorkflowStartCommandConflict`, and nothing is written. `retry_start` builds
> the same spec again, so a first attempt that committed comes back as the existing
> handle. A spec that changed between the two starts (for example after a deploy) is
> another command. A run started before this release has no stored start command:
> the start returns its handle with no compare. See
> [Start a Workflow Once per Entity](/horsies/guides/start-workflow-once/).

### WorkflowHandle

| Attribute / Method | Type / Signature | Description |
|---|---|---|
| `WorkflowHandle.for_id(text, broker)` | `(str, PostgresBroker) -> Result[WorkflowHandle[Any], HandleInvalidWorkflowId]` | The handle of a workflow id text, or `HandleInvalidWorkflowId` when the text is not a UUID. Reads nothing |
| `.workflow_id` | `uuid.UUID` | Workflow id; `str(handle.workflow_id)` is its canonical text |
| `.status()` / `.status_async()` | `-> HandleResult[WorkflowStatus]` | Current workflow status |
| `.get()` / `.get_async()` | `(timeout_ms: int \| None) -> ReadResult[OutT, TaskError]` | Block until completion or timeout. The outcome is in `Ok(...)`. A read failure is in `Err(ReadError)`, for example `Err(WaitTimeout(...))`. A COMPLETED workflow whose output node is SKIPPED returns `Ok(Failed(task_error))` with `WORKFLOW_OUTPUT_SKIPPED`: see [Output Node Under a Success Policy](/horsies/concepts/workflows/workflow-semantics/#output-node-under-a-success-policy) |
| `.results()` / `.results_async()` | `-> HandleResult[dict[str, TaskOutcome[Any, TaskError]]]` | All results keyed by node_id; `Ok({})` before any node has a result, `HandleWorkflowNotFound` when no workflow has the id |
| `.result_for()` / `.result_for_async()` | `(TaskNode[T] \| NodeKey[T]) -> ReadResult[T, TaskError]` | Single node result (non-blocking). No stored result yet is `Err(ResultNotReady(...))` |
| `.tasks()` / `.tasks_async()` | `-> HandleResult[list[WorkflowTaskInfo]]` | Status of all workflow tasks; `HandleWorkflowNotFound` when no workflow has the id |
| `.cancel()` / `.cancel_async()` | `-> HandleResult[None]` | Cancel workflow |
| `.pause()` / `.pause_async()` | `-> HandleResult[bool]` | Pause running workflow |
| `.resume()` / `.resume_async()` | `-> HandleResult[bool]` | Resume paused workflow |

`WorkflowHandle(workflow_id=..., broker=...)` takes a `uuid.UUID`. `for_id` parses a text once: every spelling that `uuid.UUID` accepts (upper case, braces, `urn:uuid:`, no hyphens) gives the same UUID. Text that is not a UUID is `HandleInvalidWorkflowId` with `message`, `workflow_id_text` (the text as given) and `exception` (the `ValueError` of `uuid.UUID`); the same text gives the same error. It is not a `HandleOperationError` variant.

### HandleResult

`HandleResult[T] = Result[T, HandleOperationError]`

Returned by the `WorkflowHandle` methods `.status()`, `.results()`, `.tasks()`, `.cancel()`, `.pause()`, `.resume()` and their `*_async` forms, and by `pause_workflow()`, `resume_workflow()`, `pause_workflow_sync()` and `resume_workflow_sync()`. These four functions take the workflow id as a `uuid.UUID`. `.get()` and `.result_for()` do not return a `HandleResult`: they return a `ReadResult`.

**HandleOperationError variants:**

`HandleOperationError` is a union with one frozen variant per `HandleErrorCode` member. Every variant has `code`, `message`, `retryable` and `workflow_id`. `message` and the cause do not show in `repr()`. Any other exception, for example a defect in pause or resume, propagates.

| Variant | `code` | Other fields | `retryable` | After the failure |
|---|---|---|---|---|
| `HandleWorkflowNotFound` | `WORKFLOW_NOT_FOUND` | – | `False` | Nothing changed |
| `HandleDbOperationFailed` | `DB_OPERATION_FAILED` | `exception: BaseException` | The core's classification of the cause | A read changed nothing; a write rolled back, or the commit outcome is unknown |
| `HandleStoredDataInvalid` | `STORED_DATA_INVALID` | `exception: BaseException` | `False` | Nothing changed; a stored workflow status or subworkflow summary does not parse |
| `HandleResumeRecoveryFailed` | `RESUME_RECOVERY_FAILED` | `exception: BaseException` | `False` | The resume committed and the workflow is `RUNNING`; the recovery pass after the commit failed |
| `HandleAsyncContext` | `ASYNC_CONTEXT` | – | `False` | Nothing ran |

See [Errors Reference](/horsies/tasks/errors/#workflow-handle-error-codes) for when each code occurs.

**Usage:**

<!-- check: skip partial -- handle comes from a block that is not checked -->
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
| `.result` | `TaskOutcome[Any, TaskError] \| None` | Task outcome if stored: `Succeeded(value)` or `Failed(task_error)` (COMPLETED/FAILED; SKIPPED often None) |
| `.started_at` | `datetime \| None` | When execution started |
| `.completed_at` | `datetime \| None` | When execution completed |
| `.sub_workflow_id` | `str \| None` | Child workflow id when the node is a subworkflow node; `None` otherwise |
| `.sub_workflow_summary` | `SubWorkflowSummary[Any] \| None` | Summary of the child workflow when the node is a subworkflow node; `None` otherwise |

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
| `workflow_context_from` | `Sequence[TaskNode \| SubWorkflowNode]` | `None` | Context sources |
| `node_id` | `str` | `None` | Stable identifier |
| `queue` | `str` | `None` | Queue override |
| `priority` | `int` | `None` | Priority override, from 1 (highest) to 100 (lowest). A value outside 1–100 raises `HRS-034` when the spec is built |
| `allow_failed_dependencies` | `bool` | `False` | Run despite failed deps |
| `join` | `Literal['all', 'any', 'quorum']` | `'all'` | Dependency join semantics |
| `min_success` | `int` | `None` | Required for `join='quorum'` |
| `good_until` | `datetime` | `None` | Task expiry deadline. Must be timezone-aware: a naive `datetime` raises `HRS-035` when the spec is built |

See [Typed Node Builder](/horsies/concepts/workflows/typed-node-builder/) for usage examples.

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
| HRS-029 | Builder raises an exception, returns non-`WorkflowSpec`, or returns a spec missing required validation like `definition_key`, or `build_with()` returned a spec that this call did not build |
| HRS-030 | Function returns `WorkflowSpec` but lacks `@app.workflow_builder` |

For the guarantee model and CI usage, see [Startup Validation](/horsies/configuration/app-config/#startup-validation-appcheck).
