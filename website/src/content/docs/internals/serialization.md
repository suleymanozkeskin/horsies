---
title: Serialization
summary: JSON serialization and rehydration for task arguments and results.
related: [../../tasks/defining-tasks, ../../concepts/result-handling]
tags: [internals, serialization, JSON, Pydantic, dataclass, datetime]
---

## Codec Module

Located at `horsies/core/codec/serde.py`. Handles serialization (`to_jsonable`) and rehydration (`rehydrate_value`) of task arguments, keyword arguments, and results.

## Serialization Functions

| Function | Purpose |
| -------- | ------- |
| `to_jsonable(value)` | Convert a value to a JSON-serializable structure |
| `rehydrate_value(value)` | Restore typed objects from JSON structures |
| `args_to_json(args)` | Serialize positional arguments |
| `kwargs_to_json(kwargs)` | Serialize keyword arguments |
| `dumps_json(value)` | Serialize a value to a JSON string |
| `loads_json(json_str)` | Deserialize a JSON string |
| `task_result_from_json(data)` | Deserialize a `TaskResult` |

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
  "__pydantic_model__": true,
  "module": "myapp.models",
  "qualname": "Order",
  "data": {"id": 1, "items": ["widget"]}
}
```

Rehydration uses `model_validate()` on the resolved class — Pydantic handles type coercion (including ISO strings back to `datetime` for model fields).

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
  "__dataclass__": true,
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
{"__datetime__": true, "value": "2025-06-15T10:30:00+00:00"}
{"__date__": true, "value": "2025-06-15"}
{"__time__": true, "value": "14:30:00"}
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

Attempting to serialize an unsupported type raises `SerializationError`.

## TaskResult Serialization

```python
# Success
TaskResult(ok=value)
# → {"__task_result__": true, "ok": <serialized_value>, "err": null}

# Error
TaskResult(err=TaskError(...))
# → {"__task_result__": true, "ok": null, "err": {"__task_error__": true, ...}}
```

## Error Codes

| Code | Cause |
| ---- | ----- |
| `WORKER_SERIALIZATION_ERROR` | Task result could not be serialized to JSON |
| `PYDANTIC_HYDRATION_ERROR` | Rehydration of a Pydantic model failed (missing module, validation error) |

## Return Type Validation

Return values are validated against declared types using Pydantic's `TypeAdapter` (in `horsies/core/task_decorator.py`):

```python
@app.task('typed')
def typed() -> TaskResult[int, TaskError]:
    return TaskResult(ok='not an int')  # RETURN_TYPE_MISMATCH
```

## Things to Avoid

**Don't return bare custom classes.** Use Pydantic `BaseModel` or `@dataclass` for task arguments and results. The codec needs type metadata for rehydration.

**Don't define result types in `__main__`.** Workers import types by module path. Classes defined in the entrypoint script cannot be resolved. Move them to a separate module.

**Don't define result types inside functions.** Local classes have `<locals>` in their qualname and cannot be imported by workers.
