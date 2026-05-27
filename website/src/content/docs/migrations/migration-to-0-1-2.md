---
title: Migrating to 0.1.2
summary: Breaking changes from 0.1.1 to 0.1.2 (strict-serde) and how to update existing code.
related: [../internals/serialization, ../tasks/sending-tasks, ../tasks/defining-tasks]
tags: [migration, strict-serde, 0.1.2, breaking-changes]
---

## What Changed

0.1.2 lands the strict-serde redesign. The wire stops carrying class identity. Every parameter and return type must classify into a concrete shape; the decoder uses the declared type — not metadata embedded on the value — to materialize objects.

This page lists every user-facing change against 0.1.1 and the mechanical fix for each.

## Task Signatures

### Positional task arguments are rejected at registration

`*args` (`VAR_POSITIONAL`), `**kwargs` (`VAR_KEYWORD`), and positional-only parameters (`def f(x, /):`) raise `SignatureValidationError` at `@app.task` time.

```python
# Wrong
@app.task("bad")
def bad(*args, **kwargs) -> TaskResult[int, TaskError]:
    ...

# Correct
@app.task("good")
def good(x: int, y: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=x + y)
```

### Banned annotation types

These are rejected by the signature validator:

| Banned | Fix |
|--------|-----|
| `Any` | Use a concrete type, or `JsonValue` for raw JSON data |
| `object` | Same as above |
| bare `dict`, `list`, `tuple` | Parameterize: `dict[str, JsonValue]`, `list[int]`, `tuple[str, int]` |
| `set`, `frozenset` | JSON arrays don't preserve set semantics; use `list[T]` |
| `bytes` | Wrap in a model with explicit base64 fields, or pass `str` |
| `TypeVar` | Define a wrapper task per concrete instantiation |
| bare `BaseModel` | Use a concrete subclass |
| `TypedDict` | Replace with `BaseModel` or `@dataclass` |
| `pathlib.PurePath` (and subclasses) | Pass `str` and convert at the boundary |
| Tasks without return annotation | Declare `-> TaskResult[T, TaskError]` |

`JsonValue` (re-exported as `horsies.JsonValue`) is the only untyped fence. Use it only at task boundary positions or inside `BaseModel` / `@dataclass` fields:

```python
from horsies import JsonValue

@app.task("validate_input")
def validate_input(
    data: dict[str, JsonValue],
) -> TaskResult[dict[str, JsonValue], TaskError]:
    ...
```

## Sending Tasks

### `.send()` / `.send_async()` / `.schedule()` are keyword-only

Positional arguments to send/schedule are rejected with `Err(TaskSendError(code=VALIDATION_FAILED))`.

```python
# Wrong
match my_task.send(5, 3):
    ...

# Correct
match my_task.send(a=5, b=3):
    ...
```

The signatures themselves changed in the API reference: `.send(*args, **kwargs)` is now `.send(**kwargs)`; same for `.send_async` and `.schedule`.

## Workflows

### `TaskNode(args=...)` removed

`TaskNode` no longer accepts an `args` field. Pass all data through `kwargs`.

```python
# Wrong
node = TaskNode(fn=step_task, args=("hello",))

# Correct
node = TaskNode(fn=step_task, kwargs={"step": "hello"})
```

### Subworkflow `build_with` receives typed `TaskResult`

In-process subworkflow handoff no longer round-trips through a JSON envelope. `build_with(parent_result)` receives the typed `TaskResult[T, TaskError]` directly. Existing user code that already declared the typed parameter needs no change; code that declared `Any` or relied on a wire envelope must declare the concrete `TaskResult[ParentOk, TaskError]`.

## Scheduling

### `TaskSchedule(args=...)` rejected at enqueue

The scheduler service rejects schedules carrying positional `args` with `Err(BrokerOperationError(code=ENQUEUE_FAILED))`. Use `kwargs` only.

```python
# Wrong
TaskSchedule(
    name="sync-us-east",
    task_name="process_region",
    pattern=IntervalSchedule(hours=1),
    args=("us-east",),
)

# Correct
TaskSchedule(
    name="sync-us-east",
    task_name="process_region",
    pattern=IntervalSchedule(hours=1),
    kwargs={"region": "us-east"},
)
```

## Result Retrieval

### `broker.get_result(...)` / `broker.get_result_async(...)` removed

The broker no longer exposes a typed result fetch. Use `app.get_result(...)` / `app.get_result_async(...)` for typed decode, or `broker.get_raw_result_record_async(...)` for the raw envelope.

```python
# Wrong
result = await broker.get_result_async(task_id, timeout_ms=5000)
if result.is_ok():
    ...

# Correct (typed)
outer = await app.get_result_async(task_id, timeout_ms=5000)
if is_err(outer):
    # infrastructure failure (INVALID_JSON_PAYLOAD, NO_TYPE_AVAILABLE, BROKER_ERROR)
    handle_broker_error(outer.err_value)
else:
    task_result = outer.ok_value  # TaskResult[Any, TaskError]
    if task_result.is_ok():
        ...
```

The return shape is now `BrokerResult[TaskResult[Any, TaskError]]` — the outer `BrokerResult` carries infrastructure errors; the inner `TaskResult` carries the domain result.

### New error codes

| Code | Enum | When |
|------|------|------|
| `INVALID_JSON_PAYLOAD` | `BrokerErrorCode` | Raw `result` column does not parse as JSON |
| `NO_TYPE_AVAILABLE` | `BrokerErrorCode` | `app.get_result_async` ok-slot decode needs `ok_type` but `task_name` is not in the local registry |
| `NO_TYPE_AVAILABLE` | `ContractCode` | Outputless workflow per-node decode failed for a terminal task name not registered locally |
| `RESULT_DESERIALIZATION_ERROR` | `OperationalErrorCode` | Envelope shape invalid or `ok` slot does not match `ok_type` (existed pre-strict-serde; semantics narrowed) |

Failed-task results decode without an `ok_type`, so `BrokerErrorCode.NO_TYPE_AVAILABLE` only fires on the success path. Reading failed tasks across processes that don't import the user code still works.

### Removed error code

`PYDANTIC_HYDRATION_ERROR` is retained as a legacy enum member on `ContractCode` but is no longer emitted by any production path. New code should not match on it. The strict-serde equivalents are `RETURN_TYPE_MISMATCH` (encode-side return type mismatch) and `RESULT_DESERIALIZATION_ERROR` (decode-side envelope/type failure).

## Wire Envelope

The stored `result` column shape changed from the legacy `__task_result__` / `__pydantic_model__` / `__dataclass__` / `__datetime__` per-value class tags to a single envelope:

```json
{
  "__h_task_result__": true,
  "ok":  <typed JSON value>,
  "err": null
}
```

See [Serialization](../internals/serialization) for the full envelope shape (including outputless workflow terminals).

### Implications

- **In-flight rows from a pre-strict-serde worker are not consumable by a 0.1.2 worker.** A 0.1.2 worker rejects rows carrying positional args with an explicit error. Drop or drain pre-strict-serde rows before upgrading.
- **The `__horsies_*` smuggle path is closed.** Any payload carrying reserved `__h_*` or `__builtin_task_code__` keys at user-controlled positions is rejected at decode time.

## TaskError

### `exception` field shape tightened

```python
TaskError.exception: BaseException | FlattenedException | None
```

In-process the value is a live `BaseException`. On the wire it flattens to a `FlattenedException` TypedDict (`module`, `qualname`, `str`, optional traceback). Code accessing `.exception` should switch on the concrete type rather than assuming `dict[str, Any]`.

### Reserved built-in codes are rejected as strings

`TaskError(error_code="WORKER_RESOLUTION_ERROR")` raises `ValueError` at construction time. Built-in codes must be passed as enum members:

```python
# Wrong
TaskError(error_code="WORKER_RESOLUTION_ERROR")

# Correct
from horsies import OperationalErrorCode
TaskError(error_code=OperationalErrorCode.WORKER_RESOLUTION_ERROR)
```

User-defined codes remain plain `str` (must not collide with any reserved built-in code; `horsies check` catches statically visible collisions via `HRS-212`).

## Test Fixtures (Library Contributors)

The canonical `app.get_broker()` sets both `app._broker = broker` AND `broker.app = app`. Direct construction (`PostgresBroker(config.broker)` + manual `app._broker = broker`) leaves `broker.app` as `None`, which breaks consumer-side outputless workflow decode (terminal node lookup needs `broker.app.tasks`).

If you maintain test fixtures that bypass `app.get_broker()`:

```python
# Wrong
app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker

# Correct
app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker
broker.app = app   # required for outputless workflow per-node decode

# Or simply:
app = Horsies(config)
broker = app.get_broker()
```

## Upgrade Checklist

- [ ] Audit `@app.task` signatures: replace `Any` / `object` / bare containers / `bytes` / `TypeVar` with concrete types or `JsonValue`.
- [ ] Search for positional `.send(` / `.send_async(` / `.schedule(` calls and rewrite as kwargs.
- [ ] Search for `TaskNode(args=` / `TaskSchedule(args=` and migrate to `kwargs=`.
- [ ] Replace `broker.get_result(...)` / `broker.get_result_async(...)` with `app.get_result(...)` / `app.get_result_async(...)`; update callers to unwrap the outer `BrokerResult`.
- [ ] Drop or drain in-flight pre-strict-serde rows before upgrading workers.
- [ ] Replace string-form built-in error codes (e.g. `"BROKER_ERROR"`, `"WORKER_RESOLUTION_ERROR"`) with the corresponding enum member.
- [ ] If you match on `PYDANTIC_HYDRATION_ERROR`, add `RESULT_DESERIALIZATION_ERROR` and `RETURN_TYPE_MISMATCH` to the match arms.
