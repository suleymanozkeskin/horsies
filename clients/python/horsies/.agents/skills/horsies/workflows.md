# horsies — Workflows

Detailed reference for building, starting, and managing workflow DAGs.
Covers spec construction, node types, handle operations, failure semantics, and validation.

## WorkflowSpec Construction

Two approaches: functional (inline) and class-based.

### Functional — `app.workflow()`

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
spec = app.workflow(
    name: str,
    tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
    on_error: OnError = OnError.FAIL,
    output: TaskNode[OutT] | SubWorkflowNode[OutT] | None = None,
    success_policy: SuccessPolicy | None = None,
    *,
    definition_key: str | None = None,  # required; None raises HRS-016
) -> WorkflowSpec[OutT] | WorkflowSpec[WorkflowTerminalResults]
```

- When `output` is provided: returns `WorkflowSpec[OutT]` typed to that node's ok-type.
- When `output=None`: returns `WorkflowSpec[WorkflowTerminalResults]`. `WorkflowTerminalResults` is `dict[str, TaskOutcome[Any, TaskError] | None]`, keyed by `node_id`. On success `handle.get()` returns `Ok(Succeeded(terminal_results))`.
- Resolves queue/priority for each `TaskNode` using app config. A queue that is not in app config raises HRS-103 (`CUSTOM` mode) or HRS-200 (`DEFAULT` mode).
- Attaches broker and `resend_on_transient_err` from app config automatically.
- `definition_key` is required for top-level workflows built with `app.workflow()` and is used for persistence/runtime lookup.

Best for dynamic workflows where node kwargs depend on runtime inputs.

### Class-based — `WorkflowDefinition`

<!-- check: skip partial -- uses names that the page does not define -->
```python
class ETLPipeline(WorkflowDefinition[SaveResult]):
    name = "etl_pipeline"
    definition_key = "myapp.etl_pipeline.v1"

    fetch = TaskNode(fn=fetch_data)
    process = TaskNode(fn=process_data, waits_for=[fetch], args_from={"data": fetch})
    save = TaskNode(fn=save_result, waits_for=[process], args_from={"result": process})

    config = WorkflowDefinitionConfig(
        output=save,
        on_error=OnError.FAIL,
        success_policy=None,  # optional
    )

spec = ETLPipeline.build(app)
```

- `node_id` auto-assigned from attribute name (`fetch`, `process`, `save`).
- `definition_key` is required — a stable string key for persistence and runtime lookup. Auto-registered by the metaclass at import time.
- `config` is a frozen `WorkflowDefinitionConfig(output=None, on_error=OnError.FAIL, success_policy=None)`. The class body can name its own nodes in it. A subclass without its own `config` uses the parent's `config`.
- A `Meta` attribute (any object, also from a base class) or a `config` that is not a `WorkflowDefinitionConfig` raises HRS-033 when the class is created, before `build()`.
- Generic parameter `WorkflowDefinition[T]` types the output node result. With no `config.output`, `T` must be `None` (`WorkflowDefinition[None]`) or `Any`; a concrete `T` without `config.output` raises HRS-025.
- Nodes are collected in definition order by the metaclass.
- Spec and all nodes are **frozen** (immutable) after construction.

Best for static, reusable DAGs.

### Parameterized builds — `build_with()`

Override `build_with()` to accept runtime parameters. This is the child-workflow form. The engine calls `build_with()` when it enqueues a `SubWorkflowNode`, and binds queue and priority for the child nodes at that time:

<!-- check: skip partial -- uses names that the page does not define -->
```python
class ChildPipeline(WorkflowDefinition[ProcessedData]):
    name = "child_pipeline"
    definition_key = "myapp.child_pipeline.v1"

    @classmethod
    def build_with(cls, app: Horsies, source_url: str, *_a: Any, **_kw: Any) -> WorkflowSpec:
        fetch = TaskNode(fn=fetch_raw, kwargs={"source_url": source_url})
        process = TaskNode(fn=process_data, waits_for=[fetch], args_from={"raw": fetch})
        return WorkflowSpec(
            name=cls.name, tasks=[fetch, process], output=process,
        )

# In a parent workflow. The engine calls build_with() when it enqueues this node.
child = SubWorkflowNode(workflow_def=ChildPipeline, kwargs={"source_url": "https://..."})
```

A `WorkflowSpec(...)` built directly has `broker=None` and no bound queue or priority on its `TaskNode` items.

- `start()` returns `BROKER_NOT_CONFIGURED` only when `broker` is `None`.
- With `broker=` passed, the start runs the checks. Each top-level `TaskNode` must have `queue` and `priority` set. If one is `None`, `start()` returns `VALIDATION_FAILED` with `HRS-014` (queue) or `HRS-015` (priority).
- For a top-level workflow, use `app.workflow()` or `build(app)`. They set the broker and bind the queue and priority.

Requirements:
- Must return a **fresh** `WorkflowSpec` per call (not cached).
- Must build the `WorkflowSpec` from its arguments only: no network, database or file-system calls, and no sleep. Put that work in an upstream `TaskNode`, and pass its result to `build_with()` through `args_from`.
- `definition_key` is auto-populated via ContextVar when returning a `WorkflowSpec` from `build_with()`; manual setting is unnecessary.

### `@app.workflow_builder` decorator

Registers a function for `horsies check` validation:

```python
@app.workflow_builder(cases=[{"region": "us-east"}, {"region": "eu-west"}])
def build_regional_workflow(region: str) -> WorkflowSpec:
    ...
```

- Executed under send suppression during check (no tasks enqueued).
- `cases` provides kwarg dicts for parameterized builders. Missing `cases` with required params raises HRS-027.

## TaskNode

<!-- check: skip partial -- uses names that the page does not define -->
```python
@dataclass
class TaskNode(Generic[OkT]):
    fn: TaskFunction[Any, OkT] | AsyncTaskFunction[Any, OkT]       # required
    kwargs: Mapping[str, Any] = {}                                 # static task kwargs (JSON-serializable)
    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] = [] # dependencies
    args_from: Mapping[str, TaskNode[Any] | SubWorkflowNode[Any]] = {}  # inject upstream TaskOutcome
    workflow_context_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None
    queue: str | None = None                  # override @task decorator queue
    priority: int | None = None               # 1 (highest)..100 (lowest); outside raises HRS-034 at build
    allow_failed_dependencies: bool = False
    join: Literal['all', 'any', 'quorum'] = 'all'
    min_success: int | None = None            # required when join='quorum'
    good_until: datetime | None = None        # task expiry deadline; naive raises HRS-035 at build
    node_id: str | None = None                # auto-assigned if None
    # Assigned by WorkflowSpec:
    index: int | None = None                  # position in DAG (0-based)
```

Properties: `.name` → `fn.task_name`, `.key()` → `NodeKey[OkT]`.

**Auto-assigned `node_id` format:**
- In `WorkflowDefinition`: attribute name (e.g., `"fetch"`)
- In `app.workflow()`: `"{slugify(workflow_name)}:{index}"` (e.g., `"etl_pipeline:0"`)
- Explicit: must match `[A-Za-z0-9_\-:.]+`, max 128 chars.

### Set `node_id` explicitly

Set `node_id` on every node. The docs site recommends it too.

- An auto-assigned id in `app.workflow()` is `{slugify(name)}:{index}`. It changes when you reorder or insert tasks.
- In a `WorkflowDefinition`, the auto id is the attribute name. A renamed attribute changes the id.
- The id is the key of `handle.results()`, the lookup key of `result_for()`, and `WorkflowTaskInfo.node_id`.
- An explicit id stays the same through reorders and renames.

<!-- check: skip partial -- uses names that the page does not define -->
```python
fetch = fetch_rows.node(node_id="fetch")(source="warehouse")
```

**Import cycle with `result_for(Workflow.node)`.** A task that calls `ctx.result_for(MyWorkflow.fetch)` must import `MyWorkflow`. The workflow module imports the task module for `fn=`. The two modules import each other.

`result_for()` accepts a `NodeKey` and looks up the result by `node_id`. Build the key from the explicit id string. The task module then does not import the workflow module.

```python
from horsies import (
    Err,
    Failed,
    NodeKey,
    Ok,
    Result,
    Succeeded,
    TaskError,
    WorkflowContext,
)

FETCH_NODE_ID = "fetch"  # shared constant; the workflow passes it to node_id=


@app.task("summarize_rows")
def summarize_rows(
    *,
    workflow_ctx: WorkflowContext | None = None,
) -> Result[int, TaskError]:
    if workflow_ctx is None:
        return Err(TaskError(error_code="NO_CONTEXT", message="workflow_ctx is missing"))
    match workflow_ctx.result_for(NodeKey[list[int]](FETCH_NODE_ID)):
        case Succeeded(rows):
            return Ok(sum(rows))
        case Failed(error):
            return Err(error)
```

- The key's type argument (`list[int]`) is not checked against the node. Keep it equal to the node's `TaskNode[T]`.
- The node must be in `workflow_context_from`. A missing result raises `KeyError`.
- `NodeKey("fetch")` for an id that no node has also raises `KeyError`. Keep the id in one shared constant.

## SubWorkflowNode

<!-- check: skip partial -- uses names that the page does not define -->
```python
@dataclass
class SubWorkflowNode(Generic[OkT]):
    workflow_def: type[WorkflowDefinition[OkT]]   # child class
    kwargs: Mapping[str, Any] = {}                 # forwarded to build_with()
    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] = []
    args_from: Mapping[str, TaskNode[Any] | SubWorkflowNode[Any]] = {}
    workflow_context_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None
    join: Literal['all', 'any', 'quorum'] = 'all'
    min_success: int | None = None
    allow_failed_dependencies: bool = False
    node_id: str | None = None
    index: int | None = None
```

Differences from `TaskNode`:
- No `fn`, `queue`, `priority`, `good_until` fields.
- `kwargs` forwarded to `workflow_def.build_with(app, **kwargs)` at execution time.
- `args_from` injects the upstream `TaskOutcome` into `build_with()`.
- The node follows the child's output: child COMPLETED with a `Succeeded` output → node COMPLETED with that output; child COMPLETED with a `Failed` output → node FAILED with that error (see SuccessPolicy / SuccessCase); child FAILED → node FAILED with `SubWorkflowFailure`.
- Outputless child (`WorkflowDefinition[None]`, no `config.output`): the parent node completes with `Succeeded(None)`. The child's terminal-results map is top-level `handle.get()` behavior only and is never exposed as a nested subworkflow output.
- A raise in the child start stops in the reaper's recovery, and the recovery limit bounds it (see Failure Semantics).
- An exception in `build_with()` fails the node with `WORKFLOW_ENQUEUE_FAILED`. The child start parses the task options of each child root and of each child task with `good_until`. Options that do not parse (the `task_options_json` of the task function changed after registration) are a defect: the child start raises `SerializationError` and its transaction rolls back, as in the workflow start transaction.
- The child start encodes and serializes the kwargs of each child node. Spec construction in `build_with()` already encoded and decoded them (a value that does not fit is `WORKFLOW_KWARGS_NOT_SERIALIZABLE`, and the node fails with `WORKFLOW_ENQUEUE_FAILED`), so a refusal at the child start is a defect: it raises `SerializationError` and the transaction rolls back.

## NodeFactory / `.node()` — Two-Step Builder

<!-- check: skip partial -- uses names that the page does not define -->
```python
# Step 1: workflow options → NodeFactory
factory = my_task.node(
    waits_for=[dep_a],
    args_from={"data": dep_a},
    queue="critical",
    # ... all workflow options
)

# Step 2: task kwargs → TaskNode (type-checked against task signature)
node = factory(extra_arg="value")
```

Step 1 sets workflow options (keyword-only). Step 2 passes task kwargs with type checking.

## Starting a Workflow

`start()` / `start_async()` return `WorkflowStartResult[WorkflowHandle[T]]`:

```python
match spec.start():
    case Ok(handle):
        result = handle.get(timeout_ms=30_000)
    case Err(WorkflowStartEnqueueFailed(retryable=True) as err):
        match spec.retry_start(err):
            case Ok(handle): ...
            case Err(retry_err): ...  # give up
    case Err(err):
        raise RuntimeError(f"[{err.code}] {err.message}")
```

### WorkflowStartError

`WorkflowStartError` is a union with one frozen variant per `WorkflowStartErrorCode` member. Every variant has `code`, `message`, `retryable`, `workflow_name` and `workflow_id` (the id that this start used, minted when the caller gave none). Match on the variant.

| Variant | `code` | Other fields | `retryable` |
|---|---|---|---|
| `WorkflowStartBrokerNotConfigured` | `BROKER_NOT_CONFIGURED` | – | `False` |
| `WorkflowStartValidationFailed` | `VALIDATION_FAILED` | `cause: BaseException \| NoCause` | `False` |
| `WorkflowStartIdConflict` | `WORKFLOW_ID_CONFLICT` | `existing_workflow_name: str` | `False` |
| `WorkflowStartCommandConflict` | `WORKFLOW_COMMAND_CONFLICT` | – | `False` |
| `WorkflowStartEnqueueFailed` | `ENQUEUE_FAILED` | `cause: BaseException` | the core's classification |
| `WorkflowStartAsyncContext` | `ASYNC_CONTEXT` | – | `False` |

### WorkflowStartErrorCode

<!-- members: WorkflowStartErrorCode -->
| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | A start check (`WorkflowValidationError`) refused the start, or a retry of another workflow's error. Any other exception in the start checks is a defect and propagates. In the start transaction, an exception that is not a database or driver error is a defect and propagates, also a `SerializationError` |
| `WORKFLOW_ID_CONFLICT` | No | The workflow id exists for another workflow name; nothing was written — use another id |
| `WORKFLOW_COMMAND_CONFLICT` | No | The workflow id holds a workflow of the same name that another start command started, or a sub-workflow; nothing was written — send the same start command, or use another id |
| `ENQUEUE_FAILED` | Maybe | Schema init or the start transaction failed with a database or driver error; any other exception in the start transaction propagates |
| `ASYNC_CONTEXT` | No | Sync `spec.start()` / `retry_start()` called on a thread that runs an event loop; nothing was written — call `await spec.retry_start_async(err)` |

### Retry

`retry_start()` / `retry_start_async()` take `WorkflowStartRetryable` = `WorkflowStartEnqueueFailed | WorkflowStartAsyncContext` and start again with the error's `workflow_id`. Another variant is a type error and fails an assertion at run time. An error of another workflow returns `Err(WorkflowStartValidationFailed)`. The retry methods refuse `WorkflowStartCommandConflict` and `WorkflowStartIdConflict`.

### Start with an existing `workflow_id`

The core compares the start command of the start with the stored one:

- Same name, same start command: `Ok(handle)` of the existing workflow; nothing written.
- Same name, another start command: `Err(WorkflowStartCommandConflict)`; nothing written.
- Another name: `Err(WorkflowStartIdConflict)`; nothing written.
- The id holds a sub-workflow: `Err(WorkflowStartCommandConflict)`.
- A run started before this release (no stored start command): `Ok(handle)`, no compare.
- A row that retention deleted: a new workflow starts.

The start command is the workflow name, `definition_key`, `on_error`, output, success policy, and each top-level node: index, `node_id`, task name, kwargs (byte for byte), queue, priority, dependencies, `args_from`, `workflow_context_from`, `allow_failed_dependencies`, join, `min_success`, task options (with `good_until`), retention class, sub-workflow facts. Not in it: `workflow_id`, `sent_at`, trace, `resend_on_transient_err`, pre-built child workflows. A value that changes per call (a `good_until` from now, a timestamp kwarg, a changed spec after a deploy) makes each repeat a conflict. `retry_start` rebuilds the same spec, so a committed first attempt comes back as the existing handle. See `/horsies/guides/start-workflow-once/`.

### Auto-retry

`resend_on_transient_err=True` on `AppConfig` retries transient `ENQUEUE_FAILED` up to 3 times with exponential backoff (200ms–2s cap). This is infra retry, not execution retry.

## WorkflowHandle

Obtained from `spec.start()`, or from `WorkflowHandle.for_id(text, broker)` to reconnect to an existing workflow by ID.

```python
handle.workflow_id  # uuid.UUID — the workflow's id; str(handle.workflow_id) is its canonical text
```

### Reconnecting to an Existing Workflow

`WorkflowHandle` is a `@dataclass` with `workflow_id: uuid.UUID` and `broker: PostgresBroker`; pyright refuses a `str` id. To reconnect to a workflow whose id you have as text (from a database, an HTTP request, or a previous session), parse it once with `WorkflowHandle.for_id(text, broker)`:

- `Ok(handle)`: the text is a UUID in any spelling that `uuid.UUID` accepts (upper case, braces, `urn:uuid:`, no hyphens); `handle.workflow_id` is the UUID. Nothing is read.
- `Err(WorkflowHandleInvalidWorkflowId)`: the text is not a UUID. Fields: `message`, `workflow_id_text` (the text as given), `cause` (the `ValueError` of `uuid.UUID`). No handle is built. It is not a `WorkflowHandleError` variant.

```python
from horsies import Err, Ok, WorkflowHandle

match WorkflowHandle.for_id("3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f", app.get_broker()):
    case Ok(ok_value=handle):
        status = handle.status()       # queries DB by workflow_id
        result = handle.get()          # waits for completion
    case Err(err_value=invalid):
        print(invalid.message)         # the text is not a UUID
```

All handle methods (`status`, `get`, `result_for`, `cancel`, `pause`, `resume`, `results`, `tasks`) work identically whether the handle came from `spec.start()` or `for_id`. If the `workflow_id` does not exist, `get` and `result_for` return `Err(WorkflowNotFound(...))`. The other methods return `Err(WorkflowHandleNotFound)`. `pause_workflow()`, `resume_workflow()` and their sync forms take the `uuid.UUID` too. The `workflow_id` of every handle error class is the canonical text of the UUID.

### Return types

| Methods | Return type | A failure |
|---|---|---|
| `status`, `cancel`, `pause`, `resume`, `results`, `tasks` | `HandleResult[T]` = `Result[T, WorkflowHandleError]` | `Err(WorkflowHandleError)` |
| `get`, `result_for` | `ReadResult[T, TaskError]` = `Ok[Succeeded[T]] \| Ok[Failed[TaskError]] \| Err[ReadError]` | `Err(ReadError)`: the read gave no outcome |

The `*_async` forms have the same return types. Read `get` and `result_for` with one `match`. `Ok(Failed(task_error))` holds the error of the workflow or of the node: `task_error.error_code` names it (for example `WORKFLOW_FAILED`, `WORKFLOW_PAUSED`). `Err(read_error)` is a read that gave no outcome. `ReadError` is a closed union of frozen classes, one per catalog code, each with `code` and `message`.

```python
from horsies import Err, Failed, Ok, Succeeded, WorkflowHandle


def report_total(workflow: WorkflowHandle[int]) -> None:
    match workflow.get(timeout_ms=30_000):
        case Ok(Succeeded(total)):
            print(f"total: {total}")
        case Ok(Failed(task_error)):
            print(f"failed: {task_error.error_code} {task_error.message}")
        case Err(read_error):
            print(f"not read: {read_error.code} {read_error.message}")
```

### All methods

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
# Status
def status(self) -> HandleResult[WorkflowStatus]
async def status_async(self) -> HandleResult[WorkflowStatus]

# Get — blocking poll until terminal or paused
def get(self, timeout_ms: int | None = None) -> ReadResult[OutT, TaskError]
async def get_async(self, timeout_ms: int | None = None) -> ReadResult[OutT, TaskError]

# Single-node result — non-blocking, single DB query
def result_for(self, node: TaskNode[OkT] | NodeKey[OkT]) -> ReadResult[OkT, TaskError]
async def result_for_async(self, node: TaskNode[OkT] | NodeKey[OkT]) -> ReadResult[OkT, TaskError]

# All outcomes keyed by node_id. Ok({}) before any node has a result;
# Err(WorkflowHandleNotFound) when no workflow has the id.
def results(self) -> HandleResult[dict[str, TaskOutcome[Any, TaskError]]]
async def results_async(self) -> HandleResult[dict[str, TaskOutcome[Any, TaskError]]]

# Task info list. Err(WorkflowHandleNotFound) when no workflow has the id.
def tasks(self) -> HandleResult[list[WorkflowTaskInfo]]
async def tasks_async(self) -> HandleResult[list[WorkflowTaskInfo]]

# Cancel — idempotent, no-op if already terminal
def cancel(self) -> HandleResult[None]
async def cancel_async(self) -> HandleResult[None]

# Pause — RUNNING → PAUSED
def pause(self) -> HandleResult[bool]    # Ok(True)=paused, Ok(False)=no-op
async def pause_async(self) -> HandleResult[bool]

# Resume — PAUSED → RUNNING
def resume(self) -> HandleResult[bool]   # Ok(True)=resumed, Ok(False)=no-op
async def resume_async(self) -> HandleResult[bool]
```

**Pause has two paths per node.** A node whose task is still `PENDING` moves
nothing. A node whose task is already `CLAIMED` has that claim abandoned: the
task row terminalizes as `CANCELLED` with a message naming the pause and, like
every terminal task, its record moves to the task-history archive; the node
returns to `READY` with its task id cleared, and resume enqueues a **fresh**
task row. Nodes already executing are never interrupted. History records
appearing during a pause are expected, not a fault.

### `get()` semantics

- Polls via PostgreSQL `LISTEN` on `workflow_done` channel, with 1-second polling fallback.
- `COMPLETED` → returns `Ok(Succeeded(value))` with the output node's value. With no `output` it returns `Ok(Succeeded(terminal_results))`, a `{node_id: TaskOutcome | None, ...}` map; the entry of a `SKIPPED` terminal node is `Failed(TaskError(error_code=OutcomeCode.WORKFLOW_OUTPUT_SKIPPED))`. With a success case that does not require the output node: a `SKIPPED` output node → `Ok(Failed(task_error))` with `OutcomeCode.WORKFLOW_OUTPUT_SKIPPED`, and a `FAILED` output node → `Ok(Failed(task_error))` with that node's error. A `COMPLETED` row whose result is NULL or empty → `Err(ResultDeserializationError(...))`: the library always stores a result with `COMPLETED`.
- `FAILED` / `CANCELLED` → returns `Ok(Failed(task_error))`: the stored error, or `OutcomeCode.WORKFLOW_FAILED` / `WORKFLOW_CANCELLED` when the error is NULL (no error is stored). An empty stored error → `Err(ResultDeserializationError(...))`.
- `EXPIRED` → returns `Ok(Failed(task_error))` with `OutcomeCode.WORKFLOW_EXPIRED`. The error `data` holds the policy name and the age.
- A stored error that is not JSON, is JSON but not an object, or does not decode as a `TaskError` → `Err(ResultDeserializationError(...))`, with the cause in the message. Any other exception in the decode is a defect and propagates.
- The workflow row is removed after `get()` read its terminal status, also `COMPLETED` (retention) → `Err(WorkflowNotFound(...))`.
- A status read that fails: no workflow has the id → `Err(WorkflowNotFound(...))`; a database failure or a stored status that does not parse → `Err(ReadDatabaseError(...))`.
- `PAUSED` → returns immediately with `Ok(Failed(task_error))` with `OutcomeCode.WORKFLOW_PAUSED`.
- Timeout → returns `Err(WaitTimeout(...))`. The workflow can still end; a later `get()` can read its outcome.

### `result_for()` semantics

Non-blocking single DB query. Returns `Err(ResultNotReady(...))` if the node has no result yet. Always call `handle.get()` first to wait for completion. A node with no `node_id` returns `Err(WorkflowCtxMissingId(...))`.

### WorkflowHandleError / WorkflowHandleErrorCode

`WorkflowHandleError` is a union with one frozen variant per `WorkflowHandleErrorCode` member other than `INVALID_WORKFLOW_ID`. Every variant has `code`, `message`, `retryable` and `workflow_id`. Match on the variant; a read of `cause` needs the match first. Any other exception, for example a defect in `pause_workflow()` or `resume_workflow()`, propagates.

<!-- members: WorkflowHandleErrorCode -->
| Code | Variant | Other fields | Retryable | After the failure |
|---|---|---|---|---|
| `WORKFLOW_NOT_FOUND` | `WorkflowHandleNotFound` | – | No | No workflow has this id; nothing changed |
| `DATABASE_OPERATION_FAILED` | `WorkflowHandleDatabaseOperationFailed` | `cause: BaseException` | the core's classification | A read changed nothing; `cancel`, `pause` and `resume` before its commit rolled back, or the commit outcome is unknown |
| `STORED_DATA_INVALID` | `WorkflowHandleStoredDataInvalid` | `cause: BaseException` | No | A stored workflow status or subworkflow summary does not parse; nothing changed, a repeat returns the same error |
| `RESUME_RECOVERY_FAILED` | `WorkflowHandleResumeRecoveryFailed` | `cause: BaseException` | No | `resume` committed and the recovery pass after the commit failed; the workflow is `RUNNING`, the worker reaper runs the same pass, a repeated `resume` returns `Ok(False)` |
| `ASYNC_CONTEXT` | `WorkflowHandleAsyncContext` | – | No | A sync handle method or `pause_workflow_sync` / `resume_workflow_sync` called on a thread that runs an event loop; nothing ran — call the `*_async` form (sync `get()` / `result_for()` return `Err(AsyncContext(...))` instead) |
| `INVALID_WORKFLOW_ID` | `WorkflowHandleInvalidWorkflowId` | `workflow_id_text: str`, `cause: BaseException` | No | The workflow id text of `WorkflowHandle.for_id` is not a UUID; no handle was built, the same text is refused again; not in the `WorkflowHandleError` union |

```python
from horsies import Err, WorkflowHandleResumeRecoveryFailed, Ok

match handle.resume():
    case Ok(True):
        ...  # resumed
    case Ok(False):
        ...  # not PAUSED; nothing changed
    case Err(WorkflowHandleResumeRecoveryFailed()):
        ...  # resumed; the recovery pass failed and the reaper runs it again
    case Err(err):
        print(f"[{err.code}] {err.message}")
```

## Enums

### WorkflowStatus

```
PENDING → RUNNING → COMPLETED | FAILED | PAUSED | CANCELLED
                             PAUSED → EXPIRED (age policy)
```

`is_terminal` is `True` for `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`.
**`PAUSED` is NOT terminal** — workflow can resume.

`CANCELLED` means someone decided; `EXPIRED` means time ran out. A workflow
PAUSED longer than `AppConfig.retention.paused_workflow_auto_cancel_after`
(a `timedelta`, default `None` = disabled) is expired by policy, with the
policy name and age recorded on the run's `error`. An expired child
propagates to its parent exactly as a cancelled one. **Exhaustive matches
over `WorkflowStatus` must handle `EXPIRED`.**

### WorkflowTaskStatus

```
PENDING → READY → ENQUEUED → RUNNING → COMPLETED | FAILED | SKIPPED
```

`is_terminal` is `True` for `COMPLETED`, `FAILED`, `SKIPPED`.

### WorkflowTaskInfo

<!-- check: skip elided -- shows the library definition, not user code -->
```python
@dataclass
class WorkflowTaskInfo:
    node_id: str | None
    index: int
    name: str
    status: WorkflowTaskStatus
    result: TaskOutcome[Any, TaskError] | None
    started_at: datetime | None
    completed_at: datetime | None
```

## OnError — FAIL vs PAUSE

### `OnError.FAIL` (default)

1. Task fails → error stored, workflow stays `RUNNING`.
2. DAG continues: dependents without `allow_failed_dependencies` are SKIPPED; other branches run.
3. All tasks terminal → workflow becomes `FAILED`.
4. Recovery handlers with `allow_failed_dependencies=True` can run before finalization.

### `OnError.PAUSE`

1. Task fails → workflow immediately becomes `PAUSED`.
2. No new `PENDING → READY` or `READY → ENQUEUED` transitions.
3. Already-running tasks may complete (cooperative stop, no force-kill).
4. `handle.get()` returns immediately with `Ok(Failed(task_error))`, with `error_code` `WORKFLOW_PAUSED`.
5. Only `handle.resume()` or `handle.cancel()` can un-pause.
6. Cascades: pause propagates to all running child workflows (BFS). Resume propagates similarly.

`OnError.PAUSE` does not apply at the recovery limit: a limit must end the workflow.

## SuccessPolicy / SuccessCase

<!-- check: skip partial -- uses names that the page does not define -->
```python
policy = SuccessPolicy(
    cases=[
        SuccessCase(required=[deliver_door]),
        SuccessCase(required=[deliver_neighbor]),
        SuccessCase(required=[deliver_locker]),
    ],
    optional=[send_notification],  # declarative: no runtime effect
)
```

- Workflow `COMPLETED` if **any** `SuccessCase` has all `required` nodes `COMPLETED`.
- `required` and `optional` take any node: `TaskNode` or `SubWorkflowNode`.
- `optional` is declarative: it records which nodes may fail, and it has no runtime effect. A node that no case requires does not change the result, with or without `optional`.
- Without policy: any task failure → `FAILED`.
- No case satisfied: `WORKFLOW_SUCCESS_CASE_NOT_MET` error.
- A case does not have to require the output node. The workflow is then `COMPLETED`, and its output node can end `SKIPPED` or `FAILED`.
  - For a `SKIPPED` output node, `get()` returns `Ok(Failed(task_error))` with `WORKFLOW_OUTPUT_SKIPPED`. The workflow succeeded and has no output; no stored data is corrupt. `data` holds `workflow_id`, `output_node_id`, `output_task_index` and `output_task_name`.
  - For a `FAILED` output node, `get()` returns `Ok(Failed(task_error))` with that node's error.
- A node is `COMPLETED` only when its result is a success. A `SubWorkflowNode` of a child that is `COMPLETED` with a `Failed` output is `FAILED` and holds the child's error.
  - `on_error` runs. A dependent with `allow_failed_dependencies=False` is `SKIPPED`; with `allow_failed_dependencies=True` it receives `Failed(error)`. `join="any"` and `join="quorum"` do not count the node. A `SuccessCase` that requires the node is not met.
  - `result_for()` returns `Ok(Failed(error))`. `results()` and `tasks()` hold `Failed(error)`. Without a success policy the parent is `FAILED`, and its `get()` returns `Ok(Failed(error))`.
  - `SubWorkflowSummary.status` is `COMPLETED` on this `FAILED` node. `SubWorkflowSummary.output` is `None`.
  - A parent can complete when the node is in `optional` or in no case. `OnError.FAIL` stores the node's error on the workflow, and the workflow stays `RUNNING`; the success case then decides. A `COMPLETED` parent keeps that error in its error column, as for a failed `TaskNode`. `OnError.PAUSE` pauses the parent.
  - The next level sees a parent that is `FAILED` as a failed child, with `SUB_WORKFLOW_FAILED`. A parent that is `COMPLETED` with this node as its output stores the error, so the next level is `FAILED` with the same error.
- A `COMPLETED` child whose stored result is not a well-formed result envelope, or that stores no result, makes a `FAILED` node. The node holds an error envelope with `RESULT_DESERIALIZATION_ERROR` that names the child and the cause, so each reader and the parent's `get()` report that code in `Failed(task_error)`. The stored text stays on the child workflow row.

## WorkflowContext

Injected when a task declares `workflow_ctx: WorkflowContext | None = None`:

<!-- check: skip elided -- pseudo-code or a signature, not valid Python -->
```python
class WorkflowContext(BaseModel):
    workflow_id: str
    task_index: int
    task_name: str

    def result_for(self, node: TaskNode[T] | SubWorkflowNode[T] | NodeKey[T]) -> TaskOutcome[T, TaskError]
    def has_result(self, node: TaskNode | SubWorkflowNode) -> bool
    def summary_for(self, node: SubWorkflowNode[T]) -> SubWorkflowSummary[T]
    def has_summary(self, node: SubWorkflowNode) -> bool
```

**Requirements:**
1. `TaskNode.workflow_context_from` must list the upstream nodes to include.
2. Every node in `workflow_context_from` must also be in `waits_for` (HRS-009).
3. If `workflow_context_from` is set but param is missing: HRS-010.

**Scope:** Only nodes in `workflow_context_from` are accessible. Others raise `KeyError`.

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("aggregate")
def aggregate(*, workflow_ctx: WorkflowContext | None = None) -> Result[Report, TaskError]:
    if workflow_ctx is None:
        return Err(TaskError(
            error_code="NO_CTX",
            message="aggregate ran without a workflow context",
        ))
    ra = workflow_ctx.result_for(ParallelPipeline.a)  # TaskOutcome[int, TaskError]
    rb = workflow_ctx.result_for(ParallelPipeline.b)
    ...  # match each outcome: case Succeeded(value) / case Failed(error)
```

## WorkflowMeta

Lighter alternative — metadata only, no result access:

<!-- check: skip elided -- shows the library definition, not user code -->
```python
@dataclass
class WorkflowMeta:
    workflow_id: str
    task_index: int
    task_name: str
```

Auto-injected when task declares `workflow_meta: WorkflowMeta | None = None`.
Does **not** require `workflow_context_from`.

## NodeKey

<!-- check: skip partial -- uses names that the page does not define -->
```python
@dataclass(frozen=True)
class NodeKey(Generic[OkT]):
    node_id: str
```

- Obtained via `node.key()` (raises if `node_id` is None).
- Accepted by `WorkflowContext.result_for()` and `WorkflowHandle.result_for()`.
- Useful for string-based lookup of a node with an explicit `node_id`: `TaskNode(fn=fetch_data, node_id="fetch")` → `NodeKey("fetch")`.

## SubWorkflowSummary

<!-- check: skip partial -- uses names that the page does not define -->
```python
@dataclass
class SubWorkflowSummary(Generic[OkT]):
    status: WorkflowStatus
    output: OkT | None
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    skipped_tasks: int
    error_summary: str | None = None
```

Accessed via `workflow_ctx.summary_for(subworkflow_node)`.

## Failure Semantics

### Recovery limit

A recovery repair that raises rolls back, and the next reaper pass runs it again. A workflow fails closed after `RecoveryConfig.workflow_recovery_failure_limit` passes (default 25, 3–1000) with a persistent raise, with `WORKFLOW_RECOVERY_LIMIT_REACHED`.

- Transient: `OperationalError`, `InterfaceError`, a pool timeout, a disconnect, and the error after a lost connection. It does not count, so a database outage cannot fail a workflow.
- Persistent: a defect, stored data that the engine cannot use, and a SQL error outside `OperationalError`. It counts once per pass, for the workflow.
- The count is stored in `horsies_workflow_recovery_failures`, so a restart keeps it. The count has no reset, so a later unrelated raise after a repair reaches the limit sooner.
- At the limit: the node that raised goes `FAILED`, other `PENDING` and `READY` nodes go `SKIPPED`, and a stored first error stays. Live nodes finish, and the workflow goes `FAILED` at the last one. A `success_policy` that nodes met can then end it `COMPLETED`. With no live node it goes `FAILED` at once, with no `success_policy` evaluation.
- A child at the limit fails its parent node with `SUB_WORKFLOW_FAILED`.
- A phase-2 row whose progression raises counts toward `phase2_quarantine_after_attempts`. At that bound the row moves to quarantine and the workflow fails the same way.

### SKIPPED cascade

When a task becomes SKIPPED, all dependents are SKIPPED too (unless `allow_failed_dependencies=True`). Cascades transitively: `A→B→C→D` where A fails → B, C, D all SKIPPED. Independent branches unaffected.

### `allow_failed_dependencies`

<!-- members: WorkflowTaskStatus subset -->
| Upstream state | `allow_failed_dependencies=False` | `allow_failed_dependencies=True` |
|---|---|---|
| COMPLETED | Runs normally | Runs normally |
| FAILED | SKIPPED | Runs; receives `Failed(original_error)` |
| SKIPPED | SKIPPED | Runs; receives `Failed(TaskError(error_code=UPSTREAM_SKIPPED))` |

### Join modes

| Mode | Triggers when | Skips when |
|---|---|---|
| `"all"` (default) | ALL deps terminal | ANY dep failed/skipped (unless `allow_failed_dependencies`) |
| `"any"` | ANY dep COMPLETED | ALL deps failed/skipped |
| `"quorum"` | `min_success` deps COMPLETED | Threshold mathematically unreachable |

`join="quorum"` requires `min_success >= 1` and `<= len(waits_for)`.

With `join='any'`/`'quorum'`, an `args_from` source may not be terminal
when the node fires — its kwarg is then **not injected**. Each such
parameter must declare a default (e.g.
`TaskOutcome[T, TaskError] | None = None`); spec validation rejects
no-default targets (`WORKFLOW_INVALID_JOIN`).

### `args_from` — data flow

- Maps kwarg names to upstream nodes.
- Injects the upstream outcome `TaskOutcome[T, TaskError]` (`Succeeded(value)` or `Failed(error)`) — **not just the raw value**.
- Receiving function parameter must be typed `TaskOutcome[T, TaskError]`.
- Upstream must be in `waits_for` (HRS-008). Keys must not overlap with `kwargs` (HRS-021).
- Positional args not supported on workflow nodes (HRS-026).
- With the defaults (`join="all"`, `allow_failed_dependencies=False`), a failed or skipped upstream makes the node SKIPPED. The node does not run and does not receive a `Failed`. A `Failed` arrives only with `allow_failed_dependencies=True`, with `join="any"` / `join="quorum"`, or when the stored upstream result fails to decode (`RESULT_DESERIALIZATION_ERROR`).

<!-- check: skip partial -- uses names that the page does not define -->
```python
@app.task("process")
def process(*, data: TaskOutcome[int, TaskError]) -> Result[str, TaskError]:
    match data:
        case Succeeded(value):
            return Ok(str(value))
        case Failed(error):
            # With the defaults, this branch runs only on a decode failure.
            return Err(error)

node_b = TaskNode(fn=process, waits_for=[node_a], args_from={"data": node_a})
```

## Validation Errors (HRS-001–HRS-038)

<!-- members: HorsiesErrorCode subset -->
| Code | Name | Trigger |
|---|---|---|
| HRS-001 | `WORKFLOW_NO_NAME` | Missing `name` |
| HRS-002 | `WORKFLOW_NO_NODES` | WorkflowSpec has no tasks |
| HRS-003 | `WORKFLOW_INVALID_NODE_ID` | Empty, >128 chars, or invalid chars, or `key()` on a node with no `node_id` |
| HRS-004 | `WORKFLOW_DUPLICATE_NODE_ID` | Duplicate `node_id` |
| HRS-005 | `WORKFLOW_NO_ROOT_TASKS` | All tasks have deps |
| HRS-006 | `WORKFLOW_INVALID_DEPENDENCY` | `waits_for` references node not in task list |
| HRS-007 | `WORKFLOW_CYCLE_DETECTED` | Cycle in dependency graph |
| HRS-008 | `WORKFLOW_INVALID_ARGS_FROM` | `args_from` node not in `waits_for` |
| HRS-009 | `WORKFLOW_INVALID_CONTEXT_FROM` | `workflow_context_from` node not in `waits_for` |
| HRS-010 | `WORKFLOW_CONTEXT_PARAMETER_MISSING` | `workflow_context_from` set but fn lacks param |
| HRS-011 | `WORKFLOW_INVALID_OUTPUT` | `output` node not in task list |
| HRS-012 | `WORKFLOW_INVALID_SUCCESS_POLICY` | Policy references unknown nodes |
| HRS-013 | `WORKFLOW_INVALID_JOIN` | `quorum` without valid `min_success` |
| HRS-014 | `WORKFLOW_UNRESOLVED_QUEUE` | A `TaskNode` has `queue=None` at start (spec not built by `app.workflow()`) |
| HRS-015 | `WORKFLOW_UNRESOLVED_PRIORITY` | A `TaskNode` has `priority=None` at start (spec not built by `app.workflow()`) |
| HRS-016 | `WORKFLOW_NO_DEFINITION_KEY` | Workflow definition/spec missing `definition_key` |
| HRS-017 | `WORKFLOW_DUPLICATE_DEFINITION_KEY` | Two definitions share the same `definition_key` |
| HRS-018 | `WORKFLOW_SUBWORKFLOW_APP_MISSING` | No app when subworkflow enqueues |
| HRS-019 | `WORKFLOW_INVALID_KWARG_KEY` | `kwargs` key not in function signature |
| HRS-020 | `WORKFLOW_MISSING_REQUIRED_PARAMS` | Required param not in kwargs or args_from |
| HRS-021 | `WORKFLOW_KWARGS_ARGS_FROM_OVERLAP` | Same key in kwargs and args_from |
| HRS-022 | `WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH` | Child uses default build_with() |
| HRS-024 | `WORKFLOW_ARGS_FROM_TYPE_MISMATCH` | Incompatible `TaskOutcome` type |
| HRS-025 | `WORKFLOW_OUTPUT_TYPE_MISMATCH` | Generic param doesn't match output, or concrete `WorkflowDefinition[T]` has no `config.output` |
| HRS-026 | `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` | Non-empty positional args |
| HRS-027 | `WORKFLOW_CHECK_CASES_REQUIRED` | Builder needs cases= |
| HRS-028 | `WORKFLOW_CHECK_CASE_INVALID` | Invalid case kwarg dict |
| HRS-029 | `WORKFLOW_CHECK_BUILDER_EXCEPTION` | Builder raised, returned wrong type, or signature not introspectable, or `build_with()` returned a spec that this call did not build |
| HRS-030 | `WORKFLOW_CHECK_UNDECORATED_BUILDER` | Returns WorkflowSpec without decorator |
| HRS-031 | `WORKFLOW_KWARGS_NOT_SERIALIZABLE` | kwargs value fails serialization, also a lone surrogate in a string or a dict key |
| HRS-032 | `WORKFLOW_INVALID_ID` | supplied `workflow_id` is not a UUID |
| HRS-033 | `WORKFLOW_INVALID_DEFINITION_CONFIG` | `WorkflowDefinition` class defines `Meta`, or its `config` is not a `WorkflowDefinitionConfig` (raised at class creation) |
| HRS-034 | `WORKFLOW_INVALID_PRIORITY` | A `TaskNode` `priority` is outside 1–100 |
| HRS-035 | `WORKFLOW_NAIVE_GOOD_UNTIL` | A `TaskNode` `good_until` is a naive `datetime` (no `tzinfo`) |
| HRS-036 | `WORKFLOW_SUBWORKFLOW_DEPTH_LIMIT` | `horsies check`: a subworkflow nesting with no `waits_for` on its path would start a child workflow deeper than 32 |
| HRS-037 | `WORKFLOW_SUBWORKFLOW_SELF_NESTING` | `horsies check`: a subworkflow includes itself with the same kwargs and with no `waits_for` between, so its nesting never ends |
| HRS-038 | `WORKFLOW_TOO_MANY_NODES` | A `WorkflowSpec` has more than 10,000 nodes (raised when the spec is built) |

## Known Constraints

- The DAG of a workflow is fixed when it starts. A `SubWorkflowNode` builds its child DAG from upstream results when the node becomes ready (`build_with()` with `args_from`).
- A child workflow has at most depth 32 (a top-level workflow has depth 0). A deeper child is not started: its `SubWorkflowNode` fails with `SUBWORKFLOW_DEPTH_LIMIT_REACHED`, and `on_error` applies.
- A `WorkflowSpec` holds at most 10,000 nodes; more raises HRS-038 when the spec is built. Split larger work into `SubWorkflowNode`s.
- `workflow_context_from` scope is per-task, does not propagate downstream.
- A start with an existing `workflow_id` must send the same start command; another command is `WorkflowStartCommandConflict`. Runs started before this release are not checked. The protection ends when retention deletes the workflow row.
- WorkflowSpec and all nodes are frozen after construction. Reusing node objects across specs is safe (they are copied internally).
- `PAUSED` is not terminal. `handle.get()` returns immediately with `Ok(Failed(task_error))` with `WORKFLOW_PAUSED`; does not wait for resume.

## All Public Imports

```python
from horsies import (
    # Spec construction
    WorkflowSpec, WorkflowDefinition, OnError,
    # Nodes
    TaskNode, SubWorkflowNode, NodeKey, AnyNode,
    # Success policy
    SuccessPolicy, SuccessCase,
    # Context
    WorkflowContext, WorkflowMeta, SubWorkflowSummary,
    # Enums
    WorkflowStatus, WorkflowTaskStatus,
    WORKFLOW_TERMINAL_STATES, WORKFLOW_TASK_TERMINAL_STATES,
    # Handle
    WorkflowHandle, WorkflowTaskInfo,
    # Handle result types
    HandleResult, WorkflowHandleError, WorkflowHandleErrorCode,
    WorkflowHandleNotFound, WorkflowHandleDatabaseOperationFailed, WorkflowHandleStoredDataInvalid,
    WorkflowHandleResumeRecoveryFailed, WorkflowHandleAsyncContext,
    # Start result types
    WorkflowStartResult, WorkflowStartError, WorkflowStartErrorCode,
    WorkflowStartRetryable, WorkflowStartBrokerNotConfigured,
    WorkflowStartValidationFailed, WorkflowStartIdConflict, WorkflowStartCommandConflict,
    WorkflowStartEnqueueFailed, WorkflowStartAsyncContext,
    # Result primitives
    Result, Ok, Err, is_ok, is_err,
    # Outcomes and reads
    TaskOutcome, Succeeded, Failed, ReadResult, ReadError,
    WaitTimeout, WorkflowNotFound, ResultNotReady, ResultDeserializationError,
    ReadDatabaseError, NoTypeAvailable, AsyncContext, WaitInsideTask, InvalidInput,
    WorkflowCtxMissingId,
)
```
