---
name: horsies-workflows
description: Workflow DAG guidance for horsies, including WorkflowSpec, TaskNode and SubWorkflowNode, WorkflowDefinition, WorkflowHandle, failure semantics, and validation errors E001-E031. Use when building, validating, or troubleshooting workflows.
---

# horsies — Workflows

Detailed reference for building, starting, and managing workflow DAGs.
Covers spec construction, node types, handle operations, failure semantics, and validation.

## WorkflowSpec Construction

Two approaches: functional (inline) and class-based.

### Functional — `app.workflow()`

```python
spec = app.workflow(
    name: str,
    tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
    on_error: OnError = OnError.FAIL,
    output: TaskNode[OutT] | SubWorkflowNode[OutT] | None = None,
    success_policy: SuccessPolicy | None = None,
) -> WorkflowSpec[OutT] | WorkflowSpec[WorkflowTerminalResults]
```

- When `output` is provided: returns `WorkflowSpec[OutT]` typed to that node's ok-type.
- When `output=None`: returns `WorkflowSpec[WorkflowTerminalResults]` — `handle.get()` returns `TaskResult[dict[str, TaskResult | None], TaskError]` where `.ok` is `{node_id: TaskResult | None, ...}`.
- Resolves queue/priority for each `TaskNode` using app config; raises E014/E015 if unregistered.
- Attaches broker and `resend_on_transient_err` from app config automatically.

Best for dynamic workflows where node kwargs depend on runtime inputs.

### Class-based — `WorkflowDefinition`

```python
class ETLPipeline(WorkflowDefinition[SaveResult]):
    name = "etl_pipeline"

    fetch = TaskNode(fn=fetch_data)
    process = TaskNode(fn=process_data, waits_for=[fetch], args_from={"data": fetch})
    save = TaskNode(fn=save_result, waits_for=[process], args_from={"result": process})

    class Meta:
        output = save
        on_error = OnError.FAIL
        success_policy = None  # optional

spec = ETLPipeline.build(app)
```

- `node_id` auto-assigned from attribute name (`fetch`, `process`, `save`).
- Generic parameter `WorkflowDefinition[T]` types the output node result.
- Nodes are collected in definition order by the metaclass.
- Spec and all nodes are **frozen** (immutable) after construction.

Best for static, reusable DAGs.

### Parameterized builds — `build_with()`

Override `build_with()` to accept runtime parameters:

```python
class ChildPipeline(WorkflowDefinition[ProcessedData]):
    name = "child_pipeline"

    @classmethod
    def build_with(cls, app: Horsies, source_url: str, *_a: Any, **_kw: Any) -> WorkflowSpec:
        fetch = TaskNode(fn=fetch_raw, kwargs={"source_url": source_url})
        process = TaskNode(fn=process_data, waits_for=[fetch], args_from={"raw": fetch})
        return WorkflowSpec(
            name=cls.name, tasks=[fetch, process], output=process,
        )

spec = ChildPipeline.build_with(app, source_url="https://...")
```

Requirements:
- Must return a **fresh** `WorkflowSpec` per call (not cached).
- `workflow_def_module` and `workflow_def_qualname` are auto-populated via ContextVar; manual setting is unnecessary.
- Signature must accept `*_args` and `**_kwargs` beyond declared params (E023 otherwise).

### `@app.workflow_builder` decorator

Registers a function for `horsies check` validation:

```python
@app.workflow_builder(cases=[{"region": "us-east"}, {"region": "eu-west"}])
def build_regional_workflow(region: str) -> WorkflowSpec:
    ...
```

- Executed under send suppression during check (no tasks enqueued).
- `cases` provides kwarg dicts for parameterized builders. Missing `cases` with required params raises E027.

## TaskNode

```python
@dataclass
class TaskNode(Generic[OkT]):
    fn: TaskFunction[Any, OkT]                                     # required
    kwargs: Mapping[str, Any] = {}                                 # static task kwargs (JSON-serializable)
    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] = [] # dependencies
    args_from: Mapping[str, TaskNode[Any] | SubWorkflowNode[Any]] = {}  # inject upstream TaskResult
    workflow_ctx_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None
    queue: str | None = None                  # override @task decorator queue
    priority: int | None = None               # override @task decorator priority
    allow_failed_deps: bool = False
    join: Literal['all', 'any', 'quorum'] = 'all'
    min_success: int | None = None            # required when join='quorum'
    good_until: datetime | None = None        # task expiry deadline
    node_id: str | None = None                # auto-assigned if None
    # Assigned by WorkflowSpec:
    index: int | None = None                  # position in DAG (0-based)
```

Properties: `.name` → `fn.task_name`, `.key()` → `NodeKey[OkT]`.

**Auto-assigned `node_id` format:**
- In `WorkflowDefinition`: attribute name (e.g., `"fetch"`)
- In `app.workflow()`: `"{slugify(workflow_name)}:{index}"` (e.g., `"etl_pipeline:0"`)
- Explicit: must match `[A-Za-z0-9_\-:.]+`, max 128 chars.

## SubWorkflowNode

```python
@dataclass
class SubWorkflowNode(Generic[OkT]):
    workflow_def: type[WorkflowDefinition[OkT]]   # child class
    kwargs: Mapping[str, Any] = {}                 # forwarded to build_with()
    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] = []
    args_from: Mapping[str, TaskNode[Any] | SubWorkflowNode[Any]] = {}
    workflow_ctx_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None
    join: Literal['all', 'any', 'quorum'] = 'all'
    min_success: int | None = None
    allow_failed_deps: bool = False
    node_id: str | None = None
    index: int | None = None
```

Differences from `TaskNode`:
- No `fn`, `queue`, `priority`, `good_until` fields.
- `kwargs` forwarded to `workflow_def.build_with(app, **kwargs)` at execution time.
- `args_from` injects upstream `TaskResult` into `build_with()`.
- Child status mirrors parent node: child COMPLETED → node COMPLETED with output; child FAILED → node FAILED with `SubWorkflowError`.

## NodeFactory / `.node()` — Two-Step Builder

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
    case Err(err) if err.retryable:
        match spec.retry_start(err):
            case Ok(handle): ...
            case Err(retry_err): ...  # give up
    case Err(err):
        raise RuntimeError(f"[{err.code}] {err.message}")
```

### WorkflowStartError

```python
@dataclass(slots=True, frozen=True)
class WorkflowStartError:
    code: WorkflowStartErrorCode
    message: str
    retryable: bool
    workflow_name: str
    workflow_id: str         # always populated
    exception: BaseException | None = None
```

### WorkflowStartErrorCode

| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | DAG validation, serialization, or args error |
| `ENQUEUE_FAILED` | Maybe | Schema init or DB transaction failed |
| `INTERNAL_FAILED` | No | Sync bridge or unexpected exception |

### Retry

`retry_start()` / `retry_start_async()` accept only `ENQUEUE_FAILED` errors. Other codes return `Err(VALIDATION_FAILED)`. Best-effort idempotent by `workflow_id` (not payload-verified).

### Auto-retry

`resend_on_transient_err=True` on `AppConfig` retries transient `ENQUEUE_FAILED` up to 3 times with exponential backoff (200ms–2s cap). This is infra retry, not execution retry.

## WorkflowHandle

Obtained from `spec.start()`, or constructed directly to reconnect to an existing workflow by ID.

```python
handle.workflow_id  # str — the workflow's UUID
```

### Reconnecting to an Existing Workflow

`WorkflowHandle` is a `@dataclass` with two fields — `workflow_id: str` and `broker: PostgresBroker`. You can construct it directly to reconnect to a workflow you already know the ID of (e.g. from a database, an HTTP request, or a previous session):

```python
from horsies import WorkflowHandle

handle = WorkflowHandle(workflow_id="existing-wf-uuid", broker=app.get_broker())
status = handle.status()       # works — queries DB by workflow_id
result = handle.get()          # works — polls for completion
```

All handle methods (`status`, `get`, `result_for`, `cancel`, `pause`, `resume`, `results`, `tasks`) work identically whether the handle came from `spec.start()` or direct construction. If the `workflow_id` does not exist, methods return `Err(HandleOperationError(WORKFLOW_NOT_FOUND))` or `TaskResult(err=TaskError(WORKFLOW_NOT_FOUND))`.

### Two error strategies

| Strategy | Methods | Return type |
|---|---|---|
| Wrap | `status`, `cancel`, `pause`, `resume`, `results`, `tasks` | `HandleResult[T]` = `Result[T, HandleOperationError]` |
| Fold | `get`, `result_for` | `TaskResult[T, TaskError]` (infra errors fold into `TaskError(BROKER_ERROR)`) |

### All methods

```python
# Status
def status(self) -> HandleResult[WorkflowStatus]
async def status_async(self) -> HandleResult[WorkflowStatus]

# Get — blocking poll until terminal or paused
def get(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]
async def get_async(self, timeout_ms: int | None = None) -> TaskResult[OutT, TaskError]

# Single-node result — non-blocking, single DB query
def result_for(self, node: TaskNode[OkT] | NodeKey[OkT]) -> TaskResult[OkT, TaskError]
async def result_for_async(self, node: TaskNode[OkT] | NodeKey[OkT]) -> TaskResult[OkT, TaskError]

# All results keyed by node_id
def results(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]
async def results_async(self) -> HandleResult[dict[str, TaskResult[Any, TaskError]]]

# Task info list
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

### `get()` semantics

- Polls via PostgreSQL `LISTEN` on `workflow_done` channel, with 1-second polling fallback.
- `COMPLETED` → returns output task's `TaskResult` (or `TaskResult(ok={node_id: TaskResult, ...})` if no `output`).
- `FAILED` / `CANCELLED` → returns `TaskResult(err=TaskError(...))`.
- `PAUSED` → returns immediately with `TaskResult(err=TaskError(error_code='WORKFLOW_PAUSED'))`.
- Timeout → returns `TaskResult(err=TaskError(error_code=RetrievalCode.WAIT_TIMEOUT))`.

### `result_for()` semantics

Non-blocking single DB query. Returns `RESULT_NOT_READY` if the task hasn't completed. Always call `handle.get()` first to wait for completion.

### HandleOperationError / HandleErrorCode

```python
@dataclass(slots=True, frozen=True)
class HandleOperationError:
    code: HandleErrorCode
    message: str
    retryable: bool
    workflow_id: str
    exception: BaseException | None = None
```

| Code | Retryable | When |
|---|---|---|
| `WORKFLOW_NOT_FOUND` | No | Workflow ID doesn't exist |
| `DB_OPERATION_FAILED` | Maybe | DB query/commit failure |
| `LOOP_RUNNER_FAILED` | No | Sync bridge failure |
| `INTERNAL_FAILED` | No | Unexpected exception |

## Enums

### WorkflowStatus

```
PENDING → RUNNING → COMPLETED | FAILED | PAUSED | CANCELLED
```

`is_terminal` is `True` for `COMPLETED`, `FAILED`, `CANCELLED`.
**`PAUSED` is NOT terminal** — workflow can resume.

### WorkflowTaskStatus

```
PENDING → READY → ENQUEUED → RUNNING → COMPLETED | FAILED | SKIPPED
```

`is_terminal` is `True` for `COMPLETED`, `FAILED`, `SKIPPED`.

### WorkflowTaskInfo

```python
@dataclass
class WorkflowTaskInfo:
    node_id: str | None
    index: int
    name: str
    status: WorkflowTaskStatus
    result: TaskResult[Any, TaskError] | None
    started_at: datetime | None
    completed_at: datetime | None
```

## OnError — FAIL vs PAUSE

### `OnError.FAIL` (default)

1. Task fails → error stored, workflow stays `RUNNING`.
2. DAG continues: dependents without `allow_failed_deps` are SKIPPED; other branches run.
3. All tasks terminal → workflow becomes `FAILED`.
4. Recovery handlers with `allow_failed_deps=True` can run before finalization.

### `OnError.PAUSE`

1. Task fails → workflow immediately becomes `PAUSED`.
2. No new `PENDING → READY` or `READY → ENQUEUED` transitions.
3. Already-running tasks may complete (cooperative stop, no force-kill).
4. `handle.get()` returns immediately with `WORKFLOW_PAUSED` error.
5. Only `handle.resume()` or `handle.cancel()` can un-pause.
6. Cascades: pause propagates to all running child workflows (BFS). Resume propagates similarly.

## SuccessPolicy / SuccessCase

```python
policy = SuccessPolicy(
    cases=[
        SuccessCase(required=[deliver_door]),
        SuccessCase(required=[deliver_neighbor]),
        SuccessCase(required=[deliver_locker]),
    ],
    optional=[send_notification],  # can fail without affecting success
)
```

- Workflow `COMPLETED` if **any** `SuccessCase` has all `required` tasks `COMPLETED`.
- `optional` tasks excluded from failure accounting.
- Without policy: any task failure → `FAILED`.
- No case satisfied: `WORKFLOW_SUCCESS_CASE_NOT_MET` error.

## WorkflowContext

Injected when a task declares `workflow_ctx: WorkflowContext | None = None`:

```python
class WorkflowContext(BaseModel):
    workflow_id: str
    task_index: int
    task_name: str

    def result_for(self, node: TaskNode[T] | NodeKey[T]) -> TaskResult[T, TaskError]
    def has_result(self, node: TaskNode | SubWorkflowNode) -> bool
    def summary_for(self, node: SubWorkflowNode[T]) -> SubWorkflowSummary[T]
    def has_summary(self, node: SubWorkflowNode) -> bool
```

**Requirements:**
1. `TaskNode.workflow_ctx_from` must list the upstream nodes to include.
2. Every node in `workflow_ctx_from` must also be in `waits_for` (E009).
3. If `workflow_ctx_from` is set but param is missing: E010.

**Scope:** Only nodes in `workflow_ctx_from` are accessible. Others raise `KeyError`.

```python
@app.task("aggregate")
def aggregate(workflow_ctx: WorkflowContext | None = None) -> TaskResult[Report, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code="NO_CTX"))
    ra = workflow_ctx.result_for(ParallelPipeline.a)  # TaskResult[int, TaskError]
    rb = workflow_ctx.result_for(ParallelPipeline.b)
    ...
```

## WorkflowMeta

Lighter alternative — metadata only, no result access:

```python
@dataclass
class WorkflowMeta:
    workflow_id: str
    task_index: int
    task_name: str
```

Auto-injected when task declares `workflow_meta: WorkflowMeta | None = None`.
Does **not** require `workflow_ctx_from`.

## NodeKey

```python
@dataclass(frozen=True)
class NodeKey(Generic[OkT]):
    node_id: str
```

- Obtained via `node.key()` (raises if `node_id` is None).
- Accepted by `WorkflowContext.result_for()` and `WorkflowHandle.result_for()`.
- Useful for string-based lookup: `NodeKey("my_workflow:0")`.

## SubWorkflowSummary

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

### SKIPPED cascade

When a task becomes SKIPPED, all dependents are SKIPPED too (unless `allow_failed_deps=True`). Cascades transitively: `A→B→C→D` where A fails → B, C, D all SKIPPED. Independent branches unaffected.

### `allow_failed_deps`

| Upstream state | `allow_failed_deps=False` | `allow_failed_deps=True` |
|---|---|---|
| COMPLETED | Runs normally | Runs normally |
| FAILED | SKIPPED | Runs; receives `TaskResult(err=original_error)` |
| SKIPPED | SKIPPED | Runs; receives `TaskResult(err=TaskError(UPSTREAM_SKIPPED))` |

### Join modes

| Mode | Triggers when | Skips when |
|---|---|---|
| `"all"` (default) | ALL deps terminal | ANY dep failed/skipped (unless `allow_failed_deps`) |
| `"any"` | ANY dep COMPLETED | ALL deps failed/skipped |
| `"quorum"` | `min_success` deps COMPLETED | Threshold mathematically unreachable |

`join="quorum"` requires `min_success >= 1` and `<= len(waits_for)`.

### `args_from` — data flow

- Maps kwarg names to upstream nodes.
- Injects the full `TaskResult[T, TaskError]` — **not just the raw value**.
- Receiving function parameter must be typed `TaskResult[T, TaskError]`.
- Upstream must be in `waits_for` (E008). Keys must not overlap with `kwargs` (E021).
- Positional args not supported on workflow nodes (E026).

```python
@app.task("process")
def process(data: TaskResult[int, TaskError]) -> TaskResult[str, TaskError]:
    if data.is_err():
        return TaskResult(err=data.err_value)
    return TaskResult(ok=str(data.ok_value))

node_b = TaskNode(fn=process, waits_for=[node_a], args_from={"data": node_a})
```

## Validation Errors (E001–E031)

| Code | Name | Trigger |
|---|---|---|
| E001 | `WORKFLOW_NO_NAME` | Missing `name` |
| E002 | `WORKFLOW_NO_NODES` | WorkflowSpec has no tasks |
| E003 | `WORKFLOW_INVALID_NODE_ID` | Empty, >128 chars, or invalid chars |
| E004 | `WORKFLOW_DUPLICATE_NODE_ID` | Duplicate `node_id` |
| E005 | `WORKFLOW_NO_ROOT_TASKS` | All tasks have deps |
| E006 | `WORKFLOW_INVALID_DEPENDENCY` | `waits_for` references node not in task list |
| E007 | `WORKFLOW_CYCLE_DETECTED` | Cycle in dependency graph |
| E008 | `WORKFLOW_INVALID_ARGS_FROM` | `args_from` node not in `waits_for` |
| E009 | `WORKFLOW_INVALID_CTX_FROM` | `workflow_ctx_from` node not in `waits_for` |
| E010 | `WORKFLOW_CTX_PARAM_MISSING` | `workflow_ctx_from` set but fn lacks param |
| E011 | `WORKFLOW_INVALID_OUTPUT` | `output` node not in task list |
| E012 | `WORKFLOW_INVALID_SUCCESS_POLICY` | Policy references unknown nodes |
| E013 | `WORKFLOW_INVALID_JOIN` | `quorum` without valid `min_success` |
| E014 | `WORKFLOW_UNRESOLVED_QUEUE` | Queue not registered |
| E015 | `WORKFLOW_UNRESOLVED_PRIORITY` | Priority unresolvable |
| E018 | `WORKFLOW_SUBWORKFLOW_APP_MISSING` | No app when subworkflow enqueues |
| E019 | `WORKFLOW_INVALID_KWARG_KEY` | `kwargs` key not in function signature |
| E020 | `WORKFLOW_MISSING_REQUIRED_PARAMS` | Required param not in kwargs or args_from |
| E021 | `WORKFLOW_KWARGS_ARGS_FROM_OVERLAP` | Same key in kwargs and args_from |
| E022 | `WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH` | Child uses default build_with() |
| E023 | `WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING` | Bad build_with() signature |
| E024 | `WORKFLOW_ARGS_FROM_TYPE_MISMATCH` | Incompatible TaskResult type |
| E025 | `WORKFLOW_OUTPUT_TYPE_MISMATCH` | Generic param doesn't match output |
| E026 | `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` | Non-empty positional args |
| E027 | `WORKFLOW_CHECK_CASES_REQUIRED` | Builder needs cases= |
| E028 | `WORKFLOW_CHECK_CASE_INVALID` | Invalid case kwarg dict |
| E029 | `WORKFLOW_CHECK_BUILDER_EXCEPTION` | Builder raised or returned wrong type |
| E030 | `WORKFLOW_CHECK_UNDECORATED_BUILDER` | Returns WorkflowSpec without decorator |
| E031 | `WORKFLOW_KWARGS_NOT_SERIALIZABLE` | kwargs value fails serialization |

## Known Constraints

- DAG is **static at submission time**. No runtime-defined nodes.
- `workflow_ctx_from` scope is per-task, does not propagate downstream.
- `retry_start()` is best-effort idempotent by `workflow_id` only (no payload verification).
- WorkflowSpec and all nodes are frozen after construction. Reusing node objects across specs is safe (they are copied internally).
- `PAUSED` is not terminal. `handle.get()` returns immediately with `WORKFLOW_PAUSED`; does not wait for resume.

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
    HandleResult, HandleOperationError, HandleErrorCode,
    # Start result types
    WorkflowStartResult, WorkflowStartError, WorkflowStartErrorCode,
    # Result primitives
    Result, Ok, Err, is_ok, is_err,
)
```
