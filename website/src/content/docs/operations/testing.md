---
title: Testing
section: python
summary: How to test tasks and workflow tasks without a database, and how HORSIES_SUPPRESS_SENDS stops sends in code that calls .send().
related: [../tasks/defining-tasks, ../tasks/sending-tasks, ../tasks/retrieving-results, ../concepts/workflows/workflow-semantics, ../configuration/broker-config]
tags: [testing, pytest, HORSIES_SUPPRESS_SENDS, WorkflowContext, from_serialized, TaskRuntime, for_test, AttemptNumber, TaskSendSuppressed, TaskOutcome, direct call]
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
from horsies import (
    Err,
    Failed,
    NodeKey,
    Ok,
    Result,
    Succeeded,
    TaskError,
    TaskOutcome,
    WorkflowContext,
)

from myapp.instance import app

FIRST_KEY: NodeKey[int] = NodeKey('first')


@app.task('double')
def double(*, value: int) -> Result[int, TaskError]:
    return Ok(value * 2)


@app.task('describe')
def describe(*, data: TaskOutcome[int, TaskError]) -> Result[str, TaskError]:
    match data:
        case Succeeded(value):
            return Ok(f'value={value}')
        case Failed(error):
            return Err(error)


@app.task('summarize')
def summarize(
    *,
    workflow_context: WorkflowContext | None = None,
) -> Result[int, TaskError]:
    if workflow_context is None:
        return Err(TaskError(code='NO_CTX', message='Missing context'))
    match workflow_context.result_for(FIRST_KEY):
        case Succeeded(first):
            return Ok(first)
        case Failed(error):
            return Err(error)
```

### Call a Task Directly

Pass keyword arguments. The call returns the `Result` of the task: `Ok(value)` or `Err(task_error)`.

```python
from myapp.tasks import double


def test_direct_call() -> None:
    assert double(value=21) == Ok(42)
```

Task parameters are keyword-only. A call with a positional argument, or without a required kwarg, does not raise. It returns `Err(TaskError(...))` with the error code `UNHANDLED_EXCEPTION`, and the message names the `TypeError`.

### Pass an Upstream Result

A parameter that a workflow fills through `args_from` has the type `TaskOutcome[T, TaskError]`. In a test, pass the outcome yourself.

```python
from horsies import Succeeded

from myapp.tasks import describe


def test_injected_result() -> None:
    assert describe(data=Succeeded(4)) == Ok('value=4')
```

To test the failure path, pass `Failed(TaskError(...))`.

### Build a Workflow Context

`WorkflowContext.from_serialized(...)` builds the context that a worker gives to a task with a `workflow_context` parameter.

```python
from horsies import Succeeded, WorkflowContext

from myapp.tasks import FIRST_KEY, summarize


def test_workflow_context() -> None:
    ctx = WorkflowContext.from_serialized(
        workflow_id='wf-1',
        node_index=1,
        task_name='summarize',
        results_by_node_id={FIRST_KEY.node_id: Succeeded(7)},
    )
    assert summarize(workflow_context=ctx) == Ok(7)
```

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `workflow_id` | `str` | Id of the workflow |
| `node_index` | `int` | Index of the node |
| `task_name` | `str` | Name of the task |
| `results_by_node_id` | `dict[str, TaskOutcome[Any, TaskError]]` | Upstream outcomes (`Succeeded(value)` or `Failed(task_error)`), keyed by `node_id` |
| `summaries_by_node_id` | `dict[str, SubWorkflowSummary[Any]] \| None` | Subworkflow summaries, keyed by `node_id`. Default `None` |

`result_for(node)` raises `NodeNotInContext` (code `ContractCode.NODE_NOT_IN_CONTEXT`) for a node whose id is not in `results_by_node_id`, and `WorkflowContextMissingId` (code `ContractCode.WORKFLOW_CONTEXT_MISSING_ID`) for a node with no `node_id`.

### Pass a Task Runtime

A task whose first parameter is `runtime: TaskRuntime` takes the runtime first in a direct call. `TaskRuntime.for_test(...)` builds the runtime that a worker gives to an attempt of a task.

```python
import uuid

from horsies import AttemptNumber, Err, FencingToken, MaxRetries, Ok, Result, TaskError, TaskRuntime


@app.task('stamp')
def stamp(runtime: TaskRuntime, *, label: str) -> Result[str, TaskError]:
    token = runtime.fencing_token
    return Ok(f'{label}:{token.value}')


def test_task_runtime() -> None:
    match AttemptNumber.parse(2):
        case Ok(ok_value=second_attempt):
            pass
        case Err(err_value=refused):
            raise AssertionError(refused.reason)
    match MaxRetries.parse(3):
        case Ok(ok_value=budget):
            pass
        case Err(err_value=refused):
            raise AssertionError(refused.reason)
    task_id = uuid.uuid4()
    runtime = TaskRuntime.for_test(task_id=task_id, attempt=second_attempt, max_retries=budget)
    assert runtime.fencing_token == FencingToken(task_id=task_id, value=2)
    assert runtime.retries_left.value == 2
    assert stamp(runtime, label='order') == Ok('order:2')
```

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `task_id` | `uuid.UUID` | The task id of the token |
| `attempt` | `AttemptNumber` | The attempt number. Default: the first attempt (1). A test of a retry passes 2 |
| `max_retries` | `MaxRetries` | The retry budget of the task. Default: `MaxRetries(value=0)`. `retries_left` follows from `attempt` and `max_retries` |
| `cancel_state` | `RunCancelState` | The cancel state of the run for the whole call. Default: `CancelNotRequested()`. A test of the stop path passes `CancelRequested(cause=CancelCause.OPERATOR)`. See [Cancellation](/horsies/tasks/defining-tasks/#cancellation) |

`AttemptNumber.parse(number)` returns `Ok(AttemptNumber)` for a number from 1 to `2**31`, and `Err(AttemptNumberRefused)` for another number. `AttemptNumberRefused.reason` names the number and the bounds. `MaxRetries.parse(number)` returns `Ok(MaxRetries)` for a number from 0 to 20, and `Err(MaxRetriesRefused)` for another number. `for_test` cannot fail. `.send()` does not take the runtime; the worker passes it on each run.

### Refuse a TaskOutcome Kwarg on `.send()`

The workflow engine fills `TaskOutcome` parameters. A producer must not send them. `.send()` returns `Err(TaskSendValidationFailed)` and writes nothing.

```python
from horsies import Err, Succeeded, TaskSendValidationFailed

from myapp.tasks import describe


def test_send_refuses_task_outcome() -> None:
    sent = describe.send(data=Succeeded(4))
    assert isinstance(sent, Err)
    assert isinstance(sent.err_value, TaskSendValidationFailed)
```

The same check applies to `workflow_context` and `workflow_meta`, and to the parameter that takes the `TaskRuntime`. The engine fills `workflow_context` and `workflow_meta`, and the worker passes the `TaskRuntime`. A producer must not send them.

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
- It applies to `.send()` and `.send_async()`, with or without `with_options(...)`.
- The resend methods refuse a `TaskSendSuppressed` error.
- Horsies turns on the same suppression by itself when it imports modules in a worker or scheduler, and during `app.check()`. A `.send()` at module level is a no-op in those phases.

Code under test must treat `Err(TaskSendSuppressed)` as a normal return. A test that needs `Ok(handle)` needs a database.

## Tests with a Database

A broker has one core pool for every event loop and thread of the test process. `pytest-asyncio` keeps one loop open per fixture scope; fixtures of every scope can share one broker. See [Broker Config](/horsies/configuration/broker-connections/#event-loops).

## Things to Avoid

**Don't test retries, persistence or scheduling with a direct call.** A direct call skips all of them. Use a database and a worker for those tests.

**Don't set `HORSIES_SUPPRESS_SENDS` in production.** The variable turns every send into `Err(TaskSendSuppressed)`.
