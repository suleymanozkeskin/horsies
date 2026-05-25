---
title: Serialization
summary: JSON serialization and rehydration for task arguments and results.
related: [../../tasks/defining-tasks, ../../concepts/result-handling]
tags: [internals, serialization, JSON, Pydantic, dataclass, datetime]
---

## Codec Module

Located at `horsies/core/codec/serde.py`. Handles serialization (`to_jsonable`) and rehydration (`rehydrate_value`) of task arguments, keyword arguments, and results.

## Serialization Functions

All serialization and deserialization functions return `SerdeResult[T]` (an alias for `Result[T, SerializationError]`) instead of raising exceptions. Callers check the result with `is_err()` and handle failures explicitly.

| Function | Returns | Purpose |
| -------- | ------- | ------- |
| `to_jsonable(value)` | `SerdeResult[Json]` | Convert a value to a JSON-serializable structure |
| `rehydrate_value(value)` | `SerdeResult[Any]` | Restore typed objects from JSON structures |
| `args_to_json(args)` | `SerdeResult[str]` | Serialize positional arguments |
| `kwargs_to_json(kwargs)` | `SerdeResult[str]` | Serialize keyword arguments |
| `dumps_json(value)` | `SerdeResult[str]` | Serialize a value to a JSON string |
| `loads_json(json_str)` | `SerdeResult[Json]` | Deserialize a JSON string |
| `task_result_from_json(j)` | `SerdeResult[TaskResult[Any, TaskError]]` | Deserialize a `TaskResult` |
| `serialize_error_payload(tr)` | `str` | Serialize a `TaskResult` error with hardcoded fallback (never fails) |

## Supported Types

### Native JSON Types

- `str`, `int`, `float`, `bool`, `None`
- `list`, `dict`
- Nested combinations of the above

### Pydantic BaseModel

Pydantic models serialize with type metadata for automatic rehydration. The codec stores the module path and class name so workers can reconstruct the exact type.

```python
from pydantic import BaseModel
from horsies import Horsies, TaskResult, TaskError

app = Horsies(config)

class Order(BaseModel):
    id: int
    items: list[str]

@app.task('process_order')
def process_order(order: Order) -> TaskResult[Order, TaskError]:
    return TaskResult(ok=order)
```

Serialized form:

```json
{
  "__h_pydantic__": true,
  "module": "myapp.models",
  "qualname": "Order",
  "data": {"id": 1, "items": ["widget"]}
}
```

Rehydration looks the `(module, qualname)` pair up in the [serde class registry](#serde-class-registry) and calls `model_validate()` on the resolved class — Pydantic handles type coercion (including ISO strings back to `datetime` for model fields). There is no fallback `import_module` path; an unregistered type returns `Err(SerializationError(code=UNREGISTERED_REHYDRATION_TYPE))`.

### Pydantic Models in Workflows

Pydantic models flowing through `args_from` are serialized with `model_dump(mode="json")` and rehydrated with `model_validate()`. The downstream task receives the reconstructed model instance, not a dict.

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
def process_order(order_result: TaskResult[Order, TaskError]) -> TaskResult[str, TaskError]:
    if order_result.is_err():
        return TaskResult(err=order_result.err_value)
    order: Order = order_result.ok_value  # Rehydrated Order instance, not a dict
    print(order.created_at)               # datetime object, not a string
    return TaskResult(ok=f"Processed {order.item}")

# Wiring
node_create: TaskNode[Order] = TaskNode(fn=create_order)
node_process: TaskNode[str] = TaskNode(
    fn=process_order,
    waits_for=[node_create],
    args_from={"order_result": node_create},
)
```

### Dataclasses

Dataclasses serialize with the same metadata approach. Each field is recursively converted via `to_jsonable`, preserving nested Pydantic and dataclass types.

```python
from dataclasses import dataclass

@dataclass
class Metrics:
    page_count: int
    total_words: int
```

Serialized form:

```json
{
  "__h_dataclass__": true,
  "module": "myapp.models",
  "qualname": "Metrics",
  "data": {"page_count": 5, "total_words": 1200}
}
```

Rehydration reconstructs the dataclass via its constructor. Fields with `init=False` are set directly on the instance after construction.

### Datetime Types

`datetime.datetime`, `datetime.date`, and `datetime.time` serialize as tagged dicts with ISO 8601 strings. This enables lossless round-trip rehydration — values come back as the correct Python type, not as plain strings.

```python
import datetime as dt

@app.task('record_event')
def record_event() -> TaskResult[dict, TaskError]:
    return TaskResult(ok={
        'occurred_at': dt.datetime(2025, 6, 15, 10, 30, 0, tzinfo=dt.timezone.utc),
        'event_date': dt.date(2025, 6, 15),
    })
```

Serialized forms:

```json
{"__h_datetime__": true, "value": "2025-06-15T10:30:00+00:00"}
{"__h_date__": true, "value": "2025-06-15"}
{"__h_time__": true, "value": "14:30:00"}
```

Timezone offsets are preserved. `isoformat()` produces the offset (e.g. `+00:00`, `+05:30`), and `fromisoformat()` restores it. Naive datetimes (no timezone) round-trip as naive.

Datetime types also work as fields inside dataclasses and dicts — the recursive serialization handles them automatically.

### Sequences and Mappings

`Sequence` types (e.g. `tuple`, `list`) and `Mapping` types (e.g. `dict`, `OrderedDict`) are recursively serialized. `str`, `bytes`, and `bytearray` are excluded from sequence handling.

### Unsupported

- Custom classes without Pydantic or dataclass decoration
- Classes defined in `__main__` (not importable by workers)
- Local classes defined inside functions
- File handles, connections
- Functions, lambdas

Attempting to serialize an unsupported type returns `Err(SerializationError)`.

## TaskResult Serialization

```python
# Success
TaskResult(ok=value)
# → {"__h_task_result__": true, "ok": <serialized_value>, "err": null}

# Error
TaskResult(err=TaskError(...))
# → {"__h_task_result__": true, "ok": null, "err": {"__h_task_error__": true, ...}}
```

## Reserved Key Namespace

All internal serde envelopes and engine transport keys live under the `__h_*` namespace. The serializer (`to_jsonable`) rejects any user-supplied dict key matching `^__h_` with `SerializationError(code=RESERVED_KEY_IN_USER_DATA)`. `__builtin_task_code__` (a Pydantic discriminator on `TaskError.error_code`) is also reserved.

The recursive rejection applies to model field data too — `BaseModel.model_dump()` output is scanned for `__h_*` keys before being embedded in the envelope. This closes the smuggling vector where a `dict[str, Any]` model field would carry a forged tag through Pydantic's serializer.

Reserved tags (do not use as dict keys in user data):

| Tag | Purpose |
| --- | ------- |
| `__h_pydantic__` | BaseModel envelope |
| `__h_dataclass__` | Dataclass envelope |
| `__h_task_result__` | TaskResult envelope |
| `__h_task_error__` | TaskError envelope |
| `__h_datetime__` / `__h_date__` / `__h_time__` | Datetime envelopes |
| `__h_workflow_ctx__` / `__h_workflow_meta__` | Workflow transport keys |
| `__h_taskresult_envelope__` | Engine `args_from` envelope |
| `__builtin_task_code__` | TaskError.error_code discriminator |

## Serde Class Registry

Rehydration looks types up in a process-local registry keyed by `f"{module}:{qualname}"`. The registry replaces dynamic `import_module` / `getattr` so payloads can only construct types the application has opted in to.

Two registration paths populate the registry:

1. **Signature walker** at `@app.task` registration. A conservative recursive walk over parameter and return annotations: BaseModel and dataclass subclasses reachable from the signature are auto-registered, including types nested in `Optional` / `Union` / `list` / `dict` / `tuple` / `Annotated` / Pydantic `model_fields` / dataclass field annotations. `TaskResult[OkT, TaskError]` is unwrapped to walk `OkT`.

2. **Explicit registration** via `@horsies_serdetype` (decorator) or `app.register_serde_type(cls)` (call). Use when the walker can't see the type — for example, types only carried inside `dict[str, Any]` fields, or types used by code that doesn't appear in any task signature.

   ```python
   from horsies import horsies_serdetype

   @horsies_serdetype
   class SharedConfig(BaseModel):
       feature_flag: str
   ```

The registry is append-only. Re-registering the same class is a no-op; a different class under the same `(module, qualname)` key raises `ValueError`. `TaskError` and `TaskResult` are baseline-registered at module import so user code never has to touch them.

Pure-consumer processes (monitoring services, ops tools) that don't import the result types can use [`raw_result`](#raw-result-escape-hatch) instead of registering.

## Raw Result Escape Hatch

`TaskHandle.raw_result()` / `raw_result_async()` and `WorkflowHandle.raw_result()` / `raw_result_async()` return the underlying stored JSON dict, skipping `rehydrate_value` entirely. The returned dict is the un-rehydrated `__h_task_result__` envelope — nested `__h_pydantic__` / `__h_dataclass__` payloads inside `ok` come back as plain dicts.

Use this when the consumer process doesn't import the task return types and therefore can't populate the registry.

## Error Codes

| Code | Cause |
| ---- | ----- |
| `WORKER_SERIALIZATION_ERROR` | Task result could not be serialized to JSON |
| `PYDANTIC_HYDRATION_ERROR` | Task succeeded but return value could not be rehydrated to declared type |
| `RESULT_DESERIALIZATION_ERROR` | Stored result JSON is corrupt or could not be deserialized |
| `RESERVED_KEY_IN_USER_DATA` | A user dict carried a `__h_*` or `__builtin_task_code__` key |
| `UNREGISTERED_REHYDRATION_TYPE` | A payload referenced a type not in the serde class registry |
| `LEGACY_SERDE_TAG_UNSUPPORTED` | A payload carried a pre-namespace tag (`__pydantic_model__`, etc.) |
| `UNKNOWN_SERDE_TAG` | A payload carried an unrecognised `__h_*` tag (newer producer) |

## Return Type Validation

Return values are validated against declared types using Pydantic's `TypeAdapter` (in `horsies/core/task_decorator.py`):

```python
@app.task('typed')
def typed() -> TaskResult[int, TaskError]:
    return TaskResult(ok='not an int')  # RETURN_TYPE_MISMATCH
```

## Things to Avoid

**Don't return bare custom classes.** Use Pydantic `BaseModel` or `@dataclass` for task arguments and results. The codec needs type metadata for rehydration.

**Don't define result types in `__main__`.** The registry key includes the module name. Classes defined in the entrypoint script can't be rehydrated by workers because their `__module__` is `__main__`. Move them to a separate module.

**Don't define result types inside functions.** Local classes have `<locals>` in their qualname and can't be registered by the signature walker.

**Don't use `__h_*` or `__builtin_task_code__` as dict keys in user data.** They are reserved for horsies internals. The serializer fails closed with `RESERVED_KEY_IN_USER_DATA` when it sees them.

**Don't put Pydantic models inside `dict[str, Any]` task arguments expecting them to round-trip.** The signature walker can't see types reached only through `Any`. Either widen the signature to a typed `Union[...]` so the walker registers each variant, or use `@horsies_serdetype` to register the type explicitly.

## Migration from Pre-Namespace Releases

The internal tag names changed in this release. Pre-namespace payloads (with `__pydantic_model__`, `__dataclass__`, `__task_result__`, `__datetime__`, `__date__`, `__time__`, `__task_error__`) fail closed with `LEGACY_SERDE_TAG_UNSUPPORTED` on rehydration. Same for the old engine transport keys (`__horsies_workflow_ctx__`, `__horsies_workflow_meta__`, `__horsies_taskresult__`).

To upgrade:

1. Drain enqueued tasks and finish in-flight workflows before deploying the new release.
2. After deployment, register any types not reachable from a task signature using `@horsies_serdetype` or `app.register_serde_type()`.
3. Pure-consumer services that don't import task definitions can use `handle.raw_result()` instead of `handle.get()` for read-only access.
