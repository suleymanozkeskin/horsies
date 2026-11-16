---
title: Serialization
summary: JSON serialization for task arguments and results via serde.
related: [../../tasks/defining-tasks, ../../concepts/result-handling]
tags: [internals, serialization, JSON, serde]
---

## Codec Module

Located at `horsies/src/core/codec/mod.rs`. Handles serialization and deserialization of task arguments, keyword arguments, and results using `serde` and `serde_json`.

## Serialization Functions

| Function | Returns | Purpose |
| -------- | ------- | ------- |
| `to_json_bytes(value)` | `Result<Vec<u8>, serde_json::Error>` | Serialize a value to JSON bytes |
| `from_json_bytes(bytes)` | `Result<T, serde_json::Error>` | Deserialize a value from JSON bytes |
| `to_json_string(value)` | `Result<String, serde_json::Error>` | Serialize a value to a JSON string |
| `from_json_str(s)` | `Result<T, serde_json::Error>` | Deserialize a value from a JSON string |
| `encode_task_result(result)` | `Result<Vec<u8>, serde_json::Error>` | Serialize a `TaskResult<T>` to JSON bytes |
| `decode_task_result(bytes)` | `Result<TaskResult<T>, serde_json::Error>` | Deserialize a `TaskResult<T>` from JSON bytes |
| `task_result_to_json_string(result)` | `Result<String, serde_json::Error>` | Serialize a `TaskResult<T>` to a JSON string |
| `task_result_from_json_str(s)` | `Result<TaskResult<T>, serde_json::Error>` | Deserialize a `TaskResult<T>` from a JSON string |

All functions return `Result` -- errors are handled explicitly, not via panics.

## Supported Types

### Native JSON Types

Any type implementing `serde::Serialize` and `serde::Deserialize`:

- `String`, `i32`, `i64`, `f64`, `bool`, `()`
- `Vec<T>`, `HashMap<K, V>`
- `Option<T>`
- Nested combinations of the above

### Structs with `#[derive(Serialize, Deserialize)]`

All task input and output types must derive `Serialize` and `Deserialize`:

```rust
use serde::{Serialize, Deserialize};
use horsies::{task, TaskError};

#[derive(Serialize, Deserialize)]
struct Order {
    id: i64,
    items: Vec<String>,
}

#[task("process_order")]
async fn process_order(input: Order) -> Result<Order, TaskError> {
    Ok(input)
}
```

Stored form for an object-shaped input:

```text
args   = NULL
kwargs = {"id":1,"items":["widget"]}
```

### Enums

Tagged enums serialize naturally via serde:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum PaymentMethod {
    CreditCard { number: String, expiry: String },
    BankTransfer { iban: String },
}
```

### DateTime Types

Use `chrono` types with serde support:

```rust
use chrono::{DateTime, Utc, NaiveDate};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct Event {
    occurred_at: DateTime<Utc>,
    event_date: NaiveDate,
}
```

Serialized as ISO 8601 strings:

```json
{"occurred_at": "2025-06-15T10:30:00Z", "event_date": "2025-06-15"}
```

Timezone offsets are preserved through serialization round-trips.

### Nested Types in Workflows

Types flowing through `arg_from(...)` in workflows are serialized and deserialized transparently. The downstream task receives a `TaskResult<T>`, not a raw JSON blob:

```rust
#[derive(Serialize, Deserialize)]
struct Order {
    item: String,
    total: f64,
    created_at: DateTime<Utc>,
}

#[task("create_order")]
async fn create_order() -> Result<Order, TaskError> {
    Ok(Order {
        item: "widget".into(),
        total: 9.99,
        created_at: Utc::now(),
    })
}

#[task("process_order")]
async fn process_order(order_result: TaskResult<Order>) -> Result<String, TaskError> {
    match order_result {
        TaskResult::Ok(order) => {
            // order.created_at is a DateTime<Utc>, not a string
            Ok(format!("Processed {}", order.item))
        }
        TaskResult::Err(err) => Err(err),
    }
}
```

### Unsupported

- Types without `Serialize`/`Deserialize` implementations
- File handles, connections, raw pointers
- Functions, closures
- Types containing non-serializable fields (unless skipped with `#[serde(skip)]`)

Attempting to serialize an unsupported type produces a `serde_json::Error`.

## Wire Format

### Task Arguments

Task inputs are stored in separate columns:

- `args` / `args_json` for scalar, string, array, or other non-object input
- `kwargs` / `kwargs_json` for object-shaped input

Examples:

```text
i32                -> args = "42", kwargs = NULL
Vec<i32>           -> args = "[1,2,3]", kwargs = NULL
MyStruct { ... }   -> args = NULL, kwargs = "{\"field\":...}"
()                 -> args = NULL, kwargs = NULL
```

This same split is used by:

- standalone task sends
- `TaskNode::set_input(...)`
- workflow engine enqueue for task nodes
- scheduler task payloads

### TaskResult Serialization

```rust
// Success
Ok(value)
// Stored as: {"__type": "ok", "value": <serialized_value>}

// Error
Err(TaskError { .. })
// Stored as: {"__type": "err", "value": {"error_code": "...", "message": "..."}}
```

## Error Codes

| Code | Cause |
| ---- | ----- |
| `WORKER_SERIALIZATION_ERROR` | Task result could not be serialized to JSON |
| `RESULT_DESERIALIZATION_ERROR` | Stored result JSON is corrupt or could not be deserialized |
| `UNHANDLED_ERROR` | Task panicked; panic message is captured as error message |

## Things to Avoid

**Derive `Serialize` and `Deserialize` on all task types.** Without these derives, the task cannot be sent or its results stored. The compiler will catch this at build time.

**Avoid non-deterministic serialization.** Types like `HashMap` may serialize fields in different orders across runs. This does not affect correctness but can complicate debugging. Use `BTreeMap` if stable ordering matters.

**Avoid large payloads.** Task arguments and results are stored as TEXT in PostgreSQL. Excessively large payloads (multi-MB) impact database performance. For large data, store it externally (S3, filesystem) and pass a reference/URL as the task argument.
