---
title: Retrieving Results
section: python
summary: Reading the outcome of a task with TaskHandle.get() - the ReadResult, its match, the wait limit, the handle cache and the task info.
related: [sending-tasks, ../../concepts/result-handling]
tags: [tasks, results, TaskHandle, ReadResult, TaskOutcome, ReadError, errors]
---

# Retrieving Results

Read the outcome of a task with `TaskHandle.get()` or `get_async()`. A read returns a `ReadResult`: `Ok(Succeeded(value))`, `Ok(Failed(task_error))` or `Err(read_error)`. The types and the `ReadError` classes are on [Result Handling](/horsies/concepts/result-handling/#readresult). For error handling patterns, see [Error Handling](/horsies/tasks/error-handling/).

## How To

### Basic Retrieval

```python
from horsies import Err, Failed, Ok, ReadWaitTimeout, Succeeded
from instance import my_task

send_result = my_task.send(x=10, y=20)
if send_result.is_err():
    raise RuntimeError(f"Send failed: {send_result.err_value.code}")
handle = send_result.ok_value

# Block until complete (or timeout/error)
match handle.get():
    case Ok(Succeeded(value)):
        print(f"Success: {value}")
    case Ok(Failed(task_error)):
        print(f"Task failed: {task_error.code} - {task_error.message}")
    case Err(ReadWaitTimeout()):
        print(f"Timeout waiting for {handle.task_id}")
    case Err(read_error):
        print(f"Read failed: {read_error.code} - {read_error.message}")
```

### Async Retrieval

```python
send_result = await my_task.send_async(x=10, y=20)
if send_result.is_err():
    raise RuntimeError(f"Send failed: {send_result.err_value.code}")
handle = send_result.ok_value
read = await handle.get_async()  # a ReadResult: match it as in Basic Retrieval
```
`get_async()` waits for broker notifications (LISTEN/NOTIFY) with a polling fallback if notifications are lost.

On a thread that runs an event loop, sync `get()` returns `Err(ReadAsyncContext(...))`, and sync `info()` returns `Err(DatabaseAsyncContext(...))`. Both do nothing. Use `get_async()` and `info_async()` there.

### Timeouts

Specify maximum wait time in milliseconds:

```python
# Wait up to 5 seconds
match handle.get(wait_ms=5000):
    case Err(ReadWaitTimeout()):
        # Task didn't complete in time - may still be running
        print("Timed out, task may still complete later")
    case _:
        pass  # an outcome or another read error: match them as in Basic Retrieval
```

A `wait_ms` that is negative, not an int, or above `MAX_TASK_TIMEOUT_MS` gives `Err(ReadInvalidInput(...))`, and nothing is read.

### Result Caching

The handle keeps a final answer: an outcome, `ReadResultNotAvailable`, `ReadResultDeserializationError` or `ReadNoTypeAvailable`. A later `get()` returns it with no database query. The handle does not keep the other read errors, for example `ReadWaitTimeout` or `ReadDatabaseError`. After one of them, a later `get()` reads again.

```python
send_result = my_task.send(...)
if send_result.is_err():
    raise RuntimeError(f"Send failed: {send_result.err_value.code}")
handle = send_result.ok_value

# First call fetches from database
read1 = handle.get()

# A later call returns the kept answer
read2 = handle.get()  # No database query when read1 is a final answer
```

### Checking Result State

```python
match handle.get():
    case Ok(Succeeded(value)):
        print(value)       # the success value
    case Ok(Failed(task_error)):
        print(task_error)  # the TaskError of the task
    case Err(read_error):
        print(read_error)  # why the read gave no outcome
```

### Task Metadata and Attempt History

Use `handle.info()` to fetch task metadata without waiting for completion:

```python
from horsies import is_ok

info_result = handle.info(include_result=True, include_failed_reason=True)
if is_ok(info_result) and info_result.ok_value is not None:
    task_info = info_result.ok_value
    print(f"Status: {task_info.status}, Retries: {task_info.retry_count}")
    print(f"Error code: {task_info.error_code}")  # final error_code (None if not failed)
```

`TaskInfo.result` is one of three states:

| `TaskInfo.result` | Meaning |
|---|---|
| `ResultSlotNotRequested()` | The read did not pass `include_result=True` |
| `ResultSlotNotStored()` | The read passed `include_result=True`, and the task stored no result. A live task has none |
| `ResultSlotStored(outcome)` | The read passed `include_result=True`, and the task stored a result. `outcome` is a `TaskOutcome`: `Succeeded(value)` or `Failed(error)` |

```python
from horsies import ResultSlotStored, Succeeded, is_ok

stored_info_result = handle.info(include_result=True)
if is_ok(stored_info_result) and stored_info_result.ok_value is not None:
    match stored_info_result.ok_value.result:
        case ResultSlotStored(value=Succeeded(value)):
            print(f"Returned: {value}")
        case _:
            print("No stored value")
```

`handle.info` and `app.get_task_info` decode an ok value with the task's type when the task is registered in this process. They leave it plain JSON when it is not.

To inspect per-attempt execution history, pass `include_attempts=True`:

```python
info_result = handle.info(include_attempts=True)
if is_ok(info_result) and info_result.ok_value is not None:
    task_info = info_result.ok_value
    for attempt in task_info.attempts or []:
        print(f"Attempt {attempt.attempt}: {attempt.outcome} "
              f"(retry={attempt.will_retry}, error={attempt.error_code})")
```

`TaskInfo.attempts` is `None` when `include_attempts=False` (default) and `[]` when requested but no history exists. Attempts are ordered by attempt number descending (most recent first).

Attempt history and `error_code` are only available from version `0.1.0a26` onward.

### Terminal Statuses

`TaskInfo.status` is a `TaskStatus`. Each status has `is_terminal`, and `TASK_TERMINAL_STATES` holds the terminal statuses. The statuses and their transitions are on [Task Lifecycle](/horsies/concepts/task-lifecycle/).

```python
from horsies import TaskStatus, TASK_TERMINAL_STATES

status = TaskStatus.FAILED
status.is_terminal  # True

TaskStatus.RUNNING.is_terminal  # False

# Frozenset for use in queries or filters
TASK_TERMINAL_STATES  # frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.EXPIRED})
```

### Testing

For unit tests, call tasks directly:

```python
def test_my_task():
    result = my_task(x=10, y=20)  # Direct call, no queue

    assert result == Ok("expected_output")
```

## Things to Avoid

**Don't silently ignore errors by only logging them.**

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong - logs error but takes no action
match handle.get():
    case Ok(Failed(task_error)):
        print(task_error.message)
    case _:
        pass
# Code continues as if nothing happened...

# Correct - handle or propagate errors
match handle.get():
    case Ok(Succeeded(value)):
        print(f"Success: {value}")
    case Ok(Failed(task_error)):
        # depending on your use case, either pass to a error handler ( prefer this when possible )
        your_error_handler(task_error)
        # or return/raise to propagate
        return task_error.code
    case Err(read_error):
        your_read_error_handler(read_error)
        return read_error.code
```

For detailed error handling patterns, see [error-handling](/horsies/tasks/error-handling/).
