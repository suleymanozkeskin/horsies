---
title: Testing
summary: How to test tasks and workflow tasks without a database, and how HORSIES_SUPPRESS_SENDS stops sends in code that calls .send().
related: [../tasks/defining-tasks, ../tasks/sending-tasks, ../tasks/retrieving-results, ../concepts/workflows/workflow-semantics, ../configuration/broker-config]
tags: [testing, pytest, HORSIES_SUPPRESS_SENDS, WorkflowContext, from_serialized, TaskSendSuppressed, direct call]
---

# Testing

A task is a plain Python function with a typed signature. A test calls it directly. The call runs in the test process and needs no database, no worker and no broker.

## Why and When

Use a direct call to test the body of a task. Use `HORSIES_SUPPRESS_SENDS=1` to test code that calls `.send()` without a database. Use a real database for tests of queueing, retries and workflow progress.

A direct call does not test these:

- retries (`retry_policy`)
- persistence
- worker distribution
- scheduling
- serialization of kwargs and results

## How To

The examples use these tasks:

```python
from horsies import NodeKey, TaskError, TaskResult, WorkflowContext

from myapp.instance import app

FIRST_KEY: NodeKey[int] = NodeKey('first')


@app.task('double')
def double(*, value: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value * 2)


@app.task('describe')
def describe(*, data: TaskResult[int, TaskError]) -> TaskResult[str, TaskError]:
    if data.is_err():
        return TaskResult(err=data.unwrap_err())
    return TaskResult(ok=f'value={data.unwrap()}')


@app.task('summarize')
def summarize(
    *,
    workflow_ctx: WorkflowContext | None = None,
) -> TaskResult[int, TaskError]:
    if workflow_ctx is None:
        return TaskResult(err=TaskError(error_code='NO_CTX', message='Missing context'))
    first = workflow_ctx.result_for(FIRST_KEY)
    return TaskResult(ok=first.unwrap())
```

### Call a Task Directly

Pass keyword arguments. The call returns the `TaskResult` of the task.

```python
from myapp.tasks import double


def test_direct_call() -> None:
    result = double(value=21)
    assert result.is_ok()
    assert result.unwrap() == 42
```

Task parameters are keyword-only. A call with a positional argument, or without a required kwarg, does not raise. It returns a `TaskResult` with the error code `UNHANDLED_EXCEPTION`, and the message names the `TypeError`.

### Pass an Upstream Result

A parameter that a workflow fills through `args_from` has the type `TaskResult[T, TaskError]`. In a test, pass the value yourself.

```python
from horsies import TaskResult

from myapp.tasks import describe


def test_injected_result() -> None:
    result = describe(data=TaskResult(ok=4))
    assert result.unwrap() == 'value=4'
```

To test the failure path, pass `TaskResult(err=TaskError(...))`.

### Build a Workflow Context

`WorkflowContext.from_serialized(...)` builds the context that a worker gives to a task with a `workflow_ctx` parameter.

<!-- check: skip -- does not type-check: reportAssignmentType -->
```python
from horsies import TaskResult, WorkflowContext

from myapp.tasks import FIRST_KEY, summarize


def test_workflow_context() -> None:
    ctx = WorkflowContext.from_serialized(
        workflow_id='wf-1',
        task_index=1,
        task_name='summarize',
        results_by_id={FIRST_KEY.node_id: TaskResult(ok=7)},
    )
    assert summarize(workflow_ctx=ctx).unwrap() == 7
```

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `workflow_id` | `str` | Id of the workflow |
| `task_index` | `int` | Index of the node |
| `task_name` | `str` | Name of the task |
| `results_by_id` | `dict[str, TaskResult[Any, TaskError]]` | Upstream results, keyed by `node_id` |
| `summaries_by_id` | `dict[str, SubWorkflowSummary[Any]] \| None` | Subworkflow summaries, keyed by `node_id`. Default `None` |

`result_for(node)` raises `KeyError` for a node whose id is not in `results_by_id`.

### Refuse a TaskResult Kwarg on `.send()`

The workflow engine fills `TaskResult` parameters. A producer must not send them. `.send()` returns `Err(TaskSendValidationFailed)` and writes nothing.

```python
from horsies import Err, TaskResult, TaskSendValidationFailed

from myapp.tasks import describe


def test_send_refuses_task_result() -> None:
    sent = describe.send(data=TaskResult(ok=4))
    assert isinstance(sent, Err)
    assert isinstance(sent.err_value, TaskSendValidationFailed)
```

The same check applies to `workflow_ctx` and `workflow_meta`. The engine fills those parameters, and a producer must not send them.

## Suppress Sends

`HORSIES_SUPPRESS_SENDS=1` turns every send and schedule into a no-op. Each call returns `Err(TaskSendSuppressed)` with the code `SEND_SUPPRESSED`. Nothing is written, and the call does not connect to a database.

```python
import pytest

from horsies import Err, TaskSendSuppressed

from myapp.tasks import double


def test_suppressed_send(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('HORSIES_SUPPRESS_SENDS', '1')
    sent = double.send(value=1)
    assert isinstance(sent, Err)
    assert isinstance(sent.err_value, TaskSendSuppressed)
```

Rules:

- The variable is read on each send. The value must be exactly `1` after trimming spaces. Any other value does not suppress.
- The effect is local to the process. It does not affect other producers or workers.
- It applies to `.send()`, `.send_async()`, `.schedule()` and `.schedule_async()`.
- The retry methods refuse a `TaskSendSuppressed` error.
- Horsies turns on the same suppression by itself when it imports modules in a worker or scheduler, and during `app.check()`. A `.send()` at module level is a no-op in those phases.

Code under test must treat `Err(TaskSendSuppressed)` as a normal return. A test that needs `Ok(handle)` needs a database.

## Tests with a Database

A broker keeps one set of connections for each open event loop. A broker serves at most 5 open loops, not counting its sync runner loop. A call from one more loop returns `EVENT_LOOP_LIMIT`. `pytest-asyncio` keeps one loop open per fixture scope. See [Broker Config](../configuration/broker-config#event-loops).

## Things to Avoid

**Don't test retries, persistence or scheduling with a direct call.** A direct call skips all of them. Use a database and a worker for those tests.

**Don't set `HORSIES_SUPPRESS_SENDS` in production.** The variable turns every send into `Err(TaskSendSuppressed)`.
