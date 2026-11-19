---
title: Task Errors
summary: TaskError<E> with the library codes and the task's own code enum, the UserErrorCode derive, and the data and exception of a stored error.
related: [defining-tasks, retrieving-results, ../../tasks/errors]
tags: [rust, errors, TaskError, UserErrorCode, BuiltInTaskCode]
---

# Task Errors

A task body returns `Result<T, TaskError<E>>`. `E` is the code type of the task's own errors. The library records its own codes, for example `TASK_TIMEOUT` or `WORKER_CRASHED`, with the same `TaskError<E>`. The codes and their meaning are on the shared [Errors Reference](/horsies/tasks/errors/).

## The Code Type

Derive `horsies::UserErrorCode` on an enum of unit variants. Each variant's code is its name in UPPER_SNAKE_CASE.

```rust
use horsies::UserErrorCode;

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    CardDeclined,   // CARD_DECLINED
    RateLimited,    // RATE_LIMITED
}
```

These are compile errors:

- a variant with fields;
- two variants with one code;
- a code with the name of a built-in code or of a reserved code (`WORKFLOW_STOPPED`).

`std::convert::Infallible` is the code type of a task with no codes of its own. A hand-written `UserErrorCode` impl keeps the same contract.

`CODES` are the task's [declared error codes](/horsies/tasks/errors/#declared-error-codes). The task macros (`#[task]`, `#[blocking_task]`, `async_task_fn!`, `blocking_task_fn!`) fill them from the code type, with no other user work. `register` checks them:

| Refusal | Code |
|---------|------|
| A built-in or reserved name | `HRS-212` |
| An empty code, a code that is not `UPPER_SNAKE_CASE`, or an exception class name | `HRS-112` |
| A code given twice | `HRS-113` |

The derive refuses a built-in or reserved name and a repeated code at compile time. A variant name whose code is not `UPPER_SNAKE_CASE` (a leading `_`, a letter outside ASCII) and a hand-written impl reach the check of `register`.

## TaskError

```rust
use horsies::{task, OperationalErrorCode, TaskError, UserErrorCode};

#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum ChargeCode {
    CardDeclined,
}

#[task("charge_card")]
async fn charge_card(amount_cents: u64) -> Result<String, TaskError<ChargeCode>> {
    if amount_cents == 0 {
        return Err(TaskError::library(OperationalErrorCode::TaskException, "nothing to charge"));
    }
    Err(TaskError::user(ChargeCode::CardDeclined, "the issuer declined the card")
        .with_data(serde_json::json!({ "amount_cents": amount_cents })))
}
```

| Item | Returns |
|------|---------|
| `TaskError::user(code, message)` | An error with a code of `E` |
| `TaskError::library(code, message)` | An error with a library code (`BuiltInTaskCode` or one of its four families) |
| `.with_data(value)` | This error with `data`. Never fails; see [Data](#data) |
| `.code()` | `&TaskErrorCode<E>`: `Library(BuiltInTaskCode)` or `User(E)` |
| `.message()` | `Option<&str>` |
| `.data()` | `Option<&JsonText>`: the stored text; `decode::<T>()` reads it, an integer of any width with all its digits |
| `.exception()` | `Option<&ExceptionRecord>`: set by the library for a panic of the task body |
| `.sub_workflow()` | `Option<&SubWorkflowFailure>`: the failed child workflow, when the error is a sub-workflow failure |

`TaskErrorCode::text()` returns the stored code text. The fields of `TaskError` are private. The exception record and the sub-workflow failure are set only by the library.

The library code catalogs are `BuiltInTaskCode` and its four families, `OperationalErrorCode`, `ContractCode`, `RetrievalCode` and `OutcomeCode`, with the names and the wire values of the shared catalog.

## Data

`with_data(value)` takes any `serde::Serialize` value and never fails. A value that cannot be stored is replaced with `{"data_refused": "<cause>"}`; the code and the message are kept, so retry matching and outcome routing still work. The replacement is logged at ERROR with the code and the cause.

A value cannot be stored when:

- it is not JSON-native: a non-finite float, a map key that is not a string, or a `Serialize` impl that fails;
- it holds a key that only the engine writes (`__h_...`, `__horsies_...`, `__builtin_task_code__`);
- it is nested deeper than the error bound.

## What the Library Records

| Event | Stored error |
|-------|--------------|
| The input does not decode with the task's input type | `WORKER_SERIALIZATION_ERROR`. The stored error names the type, the field path and the kind of failure; a part of the message that comes from the input is written as `<input>` |
| The returned value does not read back with the output type | `WORKER_SERIALIZATION_ERROR`. The child encodes the value, decodes the text with `T`, encodes it again, and stores the result only when the two texts are equal |

The other library codes (timeouts, crashes, cancels, expiry) are on the [Errors Reference](/horsies/tasks/errors/).

## Reading a Stored Error

A read returns the task's error inside `TaskOutcome::Failed`; see [Retrieving Results](/horsies/rust/tasks/retrieving-results/). A stored error whose user code `E` does not name is the read error `ReadError::UnknownUserCode`, with the code text and the code type that the read asked for: the task that stored it has another code type, or a version of the enum without that code.
