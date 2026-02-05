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
| `.start(workflow_id=None)` | `(str \| None) -> WorkflowHandle` | Start workflow (sync) |
| `.start_async(workflow_id=None)` | `(str \| None) -> WorkflowHandle` | Start workflow (async) |

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

See [Typed Node Builder](/concepts/workflows/typed-node-builder) for usage examples.
