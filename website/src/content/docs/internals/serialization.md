---
title: Serialization
summary: JSON serialization for task arguments and results.
related: [../../tasks/defining-tasks, ../../concepts/result-handling]
tags: [internals, serialization, JSON, Pydantic]
---

## Codec Module

Located at `horsies/core/codec/serde.py`.

## Serialization Functions

| Function | Purpose |
| -------- | ------- |
| `args_to_json(args)` | Serialize positional arguments |
| `kwargs_to_json(kwargs)` | Serialize keyword arguments |
| `dumps_json(value)` | General JSON serialization |
| `loads_json(json_str)` | JSON deserialization |
| `task_result_from_json(data)` | Deserialize TaskResult |

## Pydantic Integration

Pydantic models are automatically handled:

```python
# Serialization uses model_dump()
data = model.model_dump()
json_str = dumps_json(data)

# Deserialization via TypeAdapter
adapter = TypeAdapter(ResultType)
result = adapter.validate_python(data)
```

## Supported Types

### Native JSON Types

- `str`, `int`, `float`, `bool`, `None`
- `list`, `dict`
- Nested combinations

### Extended Types

- `datetime` → ISO format string
- `uuid.UUID` → string
- `Pydantic BaseModel` → dict

### Unsupported

- Custom classes (without serialization method)
- File handles, connections
- Functions, lambdas

## TaskResult Serialization

```python
# Success
TaskResult(ok=value) → {"ok": serialized_value}

# Error
TaskResult(err=TaskError(...)) → {"err": {...}}
```

## Error Codes for Serialization Issues

| Code | Cause |
| ---- | ----- |
| `WORKER_SERIALIZATION_ERROR` | Result couldn't be serialized |
| `PYDANTIC_HYDRATION_ERROR` | Arguments couldn't be deserialized |

## Type Validation

Return values are validated against declared types:

```python
@app.task("typed")
def typed() -> TaskResult[int, TaskError]:
    return TaskResult(ok="not an int")  # RETURN_TYPE_MISMATCH
```

Uses Pydantic's `TypeAdapter.validate_python()`.

## Custom Serialization

For custom types, implement `__json__()` or use Pydantic:

```python
from pydantic import BaseModel

class Order(BaseModel):
    id: int
    items: list[str]

@app.task("process")
def process(order: dict) -> TaskResult[Order, TaskError]:
    # Pass as dict, reconstruct in task
    order_obj = Order(**order)
    return TaskResult(ok=order_obj)  # Serialized via model_dump()
```

## Performance

- Uses `orjson` if available (faster)
- Falls back to standard `json` module
- Pydantic v2 for efficient validation
