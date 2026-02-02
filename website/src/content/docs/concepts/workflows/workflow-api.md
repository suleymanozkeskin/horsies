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
| `.index` | `int` | Task position in the DAG |
| `.name` | `str` | Task name |
| `.status` | `WorkflowTaskStatus` | Current task status |
| `.result` | `TaskResult[Any, TaskError] \| None` | Task result if completed |
| `.started_at` | `datetime \| None` | When execution started |
| `.completed_at` | `datetime \| None` | When execution completed |
