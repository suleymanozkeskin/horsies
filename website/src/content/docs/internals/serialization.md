---
title: Serialization
summary: Wire envelope and type-driven decoding for task arguments and results.
related: [../../tasks/defining-tasks, ../../concepts/result-handling]
tags: [internals, serialization, codec, JsonValue, envelope]
---

## Model

Strict-serde. The wire carries values plus one envelope marker. It does **not** carry class identity. The receiver's declared type drives every decode through `pydantic.TypeAdapter`.

Two consequences:

- Tasks must declare every parameter and return type. Banned types (`Any`, `object`, bare `dict`/`list`/`tuple`, `TypeVar`, bare `BaseModel`, `TypedDict`, `bytes`, `set`/`frozenset`, `Callable`, and `pathlib.PurePath` subclasses) are rejected at `@app.task` registration.
- `JsonValue` is the only untyped fence. Use it at task boundary positions or inside `BaseModel`/`@dataclass` fields when the payload is genuinely raw JSON.

## Codec Layout

| Module | Purpose |
|--------|---------|
| `horsies.core.codec.json_value` | `JsonValue` PEP 695 alias; `StrictJsonError`; strict JSON-native validators |
| `horsies.core.codec.serde` | `dumps_json` / `loads_json` stringification helpers and library error-payload serialization |
| `horsies.core.codec.signature_check` | `validate_task_signature` — rejects banned types at decorator time |
| `horsies.core.codec.kwargs` | `encode_kwargs(task_fn, kwargs)` / `decode_kwargs(...)` for producer/consumer kwarg binding |
| `horsies.core.codec.typed` | `encode_value` / `decode_value` / `encode_task_result` / `decode_task_result` / `decode_task_error` / `validate_task_result_envelope` |

Re-exports: `from horsies import JsonValue, StrictJsonError`.

## Wire Envelope

Every stored task result is one shape:

```json
{
  "__h_task_result__": true,
  "ok":  ...payload...,
  "err": null
}
```

- Marker `__h_task_result__: true` distinguishes a Horsies envelope from any other JSON in the row.
- Exactly one of `ok` / `err` is non-null.
- The `ok` slot carries a JSON value matching the task's declared `ok_type`. `BaseModel`/`@dataclass` instances are stored as their plain Pydantic JSON form (no per-value type tag) — the receiver reconstructs them by calling `decode_task_result(envelope, ok_type)`.
- The `err` slot, when populated, carries the JSON form of `TaskError`, including `error_code`, `message`, `data`, and a flattened `exception` (a `FlattenedException` TypedDict, never a live `BaseException` on the wire).

Outputless-workflow envelopes wrap an additional marker:

```json
{
  "__h_task_result__": true,
  "__h_outputless_terminals__": true,
  "ok": {
    "results_by_id":   {"<node_id>": <per-node envelope>, ...},
    "task_name_by_id": {"<node_id>": "<task_name>", ...}
  },
  "err": null
}
```

Each `results_by_id` entry is itself a `__h_task_result__` envelope; the consumer dispatches per-node decode using the task name to look up `task_ok_type` in the local registry.

## Encoding (worker side)

The worker calls `encode_task_result(result, ok_type)` after the user function returns. The codec:

1. Validates `result.ok` against `ok_type` via `TypeAdapter` (raises `ValidationError` / `PydanticSerializationError` on shape mismatch).
2. Emits the envelope.
3. Flattens any live `BaseException` carried on `TaskError.exception` into a `FlattenedException` (`type`, `module`, `message`, `repr`, `traceback`).

Encode failures surface as `WORKER_SERIALIZATION_ERROR`.

```python
from horsies.core.codec.typed import encode_task_result
from horsies import TaskResult, TaskError

envelope = encode_task_result(
    TaskResult[int, TaskError](ok=42),
    ok_type=int,
)
# {"__h_task_result__": True, "ok": 42, "err": None}
```

## Decoding (consumer side)

The consumer holds the declared `ok_type` (from the task registry) and calls `decode_task_result(envelope, ok_type)`:

1. `validate_task_result_envelope` checks the marker and structural shape.
2. If `err` is populated, `decode_task_error` runs first — no `ok_type` is required to surface failed tasks, including those whose task name is unknown locally.
3. Otherwise `TypeAdapter[ok_type].validate_python(envelope["ok"])` materializes the typed value.

Distinct decode-error surfaces:

| Code | Source | When |
|------|--------|------|
| `BrokerErrorCode.INVALID_JSON_PAYLOAD` | broker/app result read | the stored result does not parse as JSON, is not a JSON object, has a malformed envelope, or fails typed slot decode through an app-level API |
| `BrokerErrorCode.NO_TYPE_AVAILABLE` | `app.get_result_async` | `task_name` is not in the local task registry, so no `ok_type` available to decode the ok slot |
| `OperationalErrorCode.RESULT_DESERIALIZATION_ERROR` | handle / workflow / engine wrappers around typed decode | a wrapper catches a strict decode failure and folds it into a `TaskResult(err=...)` |
| `ContractCode.NO_TYPE_AVAILABLE` | outputless workflow handle decode | per-node lookup failed inside `WorkflowHandle.get()` for a terminal task name not in the local registry |

## Type Surface at the Boundary

Values flowing through a task signature must classify as **one** of:

- A JSON primitive (`None`, `bool`, `int`, `float`, `str`).
- `datetime.datetime` / `date` / `time`, `uuid.UUID`, `decimal.Decimal` (Pydantic coerces these to/from JSON strings via the declared type).
- `JsonValue` (at boundary positions only).
- An `Enum` subclass.
- A concrete `BaseModel` subclass (not bare `BaseModel`).
- A `@dataclass`.
- `list[T]`, `tuple[T, ...]`, `dict[str, T]`, `Literal[...]`, `Annotated[T, ...]`, `T | None`, or a discriminated union — recursively classified.
- `TaskResult[T, TaskError]` (anywhere `T` is one of the above; `err_t` must be `TaskError`).

Anything outside this surface raises `SignatureValidationError` at `@app.task` time. The error names the banned type and prints the documented fix.

```python
from horsies import Horsies, TaskResult, TaskError, JsonValue
from pydantic import BaseModel

app = Horsies(config)

class Order(BaseModel):
    id: int
    items: list[str]

@app.task("process_order")
def process_order(order: Order) -> TaskResult[Order, TaskError]:
    return TaskResult(ok=order)
```

The wire stores the Pydantic JSON form (`{"id": 1, "items": ["widget"]}`), with no per-value class tag. The worker decodes it back to `Order` because the registered task declared `Order` as the parameter type.

For raw JSON payloads where the inner shape is genuinely dynamic:

```python
@app.task("validate_input")
def validate_input(
    data: dict[str, JsonValue],
) -> TaskResult[dict[str, JsonValue], TaskError]:
    return TaskResult(ok=data)
```

## Pydantic Models in Workflows

Models flowing through `args_from` ride through the same envelope. The downstream task declares `TaskResult[Model, TaskError]` (or an `Optional` / `Union` containing it) as the parameter type; the engine decodes the upstream envelope and binds a typed `TaskResult` into the consumer's kwargs.

```python
from datetime import datetime, timezone
from pydantic import BaseModel
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    TaskResult,
    TaskError,
    TaskNode,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url="postgresql+psycopg://user:password@localhost:5432/mydb",
    ),
)
app = Horsies(config)

class Order(BaseModel):
    item: str
    total: float
    created_at: datetime

@app.task("create_order")
def create_order() -> TaskResult[Order, TaskError]:
    return TaskResult(ok=Order(
        item="widget",
        total=9.99,
        created_at=datetime.now(timezone.utc),
    ))

@app.task("process_order")
def process_order(
    order_result: TaskResult[Order, TaskError],
) -> TaskResult[str, TaskError]:
    if order_result.is_err():
        return TaskResult(err=order_result.err_value)
    order: Order = order_result.ok_value  # typed Order, not a dict
    return TaskResult(ok=f"Processed {order.item}")

node_create: TaskNode[Order] = TaskNode(fn=create_order)
node_process: TaskNode[str] = TaskNode(
    fn=process_order,
    waits_for=[node_create],
    args_from={"order_result": node_create},
)
```

## API Reference

### `encode_task_result(result, ok_type) -> dict[str, Json]`

Encode a `TaskResult[T, TaskError]` to the wire envelope.

| Parameter | Type | Description |
|-----------|------|-------------|
| `result` | `TaskResult[T, TaskError]` | Domain result returned by the task |
| `ok_type` | `TypeAnnotation` | Declared `T` from the task signature |

**Raises:** `pydantic.ValidationError` or `PydanticSerializationError` if `result.ok` does not match `ok_type`; `StrictJsonError` for non-JSON-serializable structures.

### `decode_task_result(envelope, ok_type) -> TaskResult[T, TaskError]`

Decode a wire envelope into a typed `TaskResult`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `envelope` | `dict[str, Json]` | The `__h_task_result__` envelope |
| `ok_type` | `TypeAnnotation` | Declared `T` from the task signature (only used when the `ok` slot is populated) |

### `decode_task_error(err_slot) -> TaskError`

Decode the `err` slot independently of `ok_type`. Used by consumers reading failed tasks whose task name is unknown locally.

### `validate_task_result_envelope(envelope) -> dict[str, Json]`

Cheap shape check before per-slot decode. Returns the envelope as `dict[str, Json]` on success; raises `StrictJsonError` if the marker is missing, the key set is wrong, or both slots are populated.

### `encode_value(value, type) / decode_value(payload, type)`

Primitive entry points for non-`TaskResult` values (workflow kwargs, success policies, args_from blocks). Same `TypeAdapter`-driven semantics.

### `JsonValue`

```python
type JsonValue = (
    None | bool | int | float | str
    | list[JsonValue]
    | dict[str, JsonValue]
)
```

PEP 695 type alias. Re-exported as `horsies.JsonValue`.

### `StrictJsonError`

Raised by strict codec primitives and the JSON parse-constant guard for strict JSON violations. `dumps_json` / `loads_json` return `SerdeResult` and wrap stringification or parse failures as `SerializationError`. Re-exported as `horsies.StrictJsonError`.

## Things to Avoid

**Don't use `Any`, `object`, or bare containers in task signatures.** They are rejected at registration time.

```python
# Wrong — raises SignatureValidationError
@app.task("bad")
def bad(data: dict) -> TaskResult[dict, TaskError]:
    return TaskResult(ok=data)

# Correct — declare the contents
@app.task("good")
def good(data: dict[str, JsonValue]) -> TaskResult[dict[str, JsonValue], TaskError]:
    return TaskResult(ok=data)
```

**Don't define result types in `__main__` or inside functions.** Workers import types by module path; classes without an importable qualname cannot participate in typed decode.

**Don't smuggle reserved keys into `TaskError.error_code` or `TaskError.data`.** `TaskError.error_code` accepts the built-in `OperationalErrorCode` / `RetrievalCode` / `ContractCode` enums or any non-reserved string. Reserved built-in codes passed as strings (e.g. `"WORKER_RESOLUTION_ERROR"`) are rejected — pass the enum member directly.

**Don't expect class identity to survive the wire.** Two `BaseModel` subclasses with structurally identical fields decode interchangeably when the consumer's declared type matches either. The wire stores values, not classes.
