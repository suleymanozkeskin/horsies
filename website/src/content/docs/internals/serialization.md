---
title: Serialization
section: shared
summary: The wire envelope of task results, how the declared type drives each decode, the strict JSON rules of the core, and the error path.
related: [../tasks/task-types, ../rust/tasks/defining-tasks, ../concepts/result-handling]
tags: [internals, serialization, codec, envelope, strict JSON]
---

## Model

Strict serde. The wire carries values plus one envelope marker. It does **not** carry class identity. The receiver's declared type drives every decode.

Consequences:

- A task declares the type of each parameter and of its result. A client refuses, at task definition, a type that cannot cross the wire.
- Two types with the same fields decode interchangeably when the reader's declared type matches either. The wire stores values, not classes.

The types that each client accepts, and its codec API, are on the client pages: [Task Types](/horsies/tasks/task-types/) (Python) and [Defining Tasks](/horsies/rust/tasks/defining-tasks/) (Rust: the input and output types are serde types).

A `SignatureValidationError` of the Python client names the refused type and links this page. The allowed types are in [Type Surface at the Boundary](/horsies/tasks/task-types/#type-surface-at-the-boundary), and the refused shapes in [Shapes Refused at Definition](/horsies/tasks/task-types/#shapes-refused-at-definition).

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
- The `ok` slot carries a JSON value matching the task's declared output type. A structured value (a model, a dataclass, a struct) is stored as its plain JSON form, with no type tag. The reader reconstructs it from its declared type.
- The `err` slot, when populated, carries the JSON form of the task error: `error_code` (the field `TaskError.code`), `message`, `data`, and the exception in flattened form (`type`, `module`, `message`, `repr`, `traceback`), never a live exception.

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

Each `results_by_id` entry is itself a `__h_task_result__` envelope. The reader decodes each node by its task name: the task name gives the output type in the local registry.

## Encoding (worker side)

After the task function returns, the worker:

1. Validates the ok value against the declared output type, strictly.
2. Reads the value back with the decode that a reader uses, and refuses a value that a reader cannot read or that loses data.
3. Emits the envelope.
4. Flattens a live exception of the error into its flattened form.

An ok value that fails surfaces as `WORKER_SERIALIZATION_ERROR`. An error never fails here: see [The Error Path](#the-error-path).

## Decoding (consumer side)

The reader holds the declared output type (from the task registry):

1. The envelope check reads the marker and the structural shape.
2. If `err` is populated, the error decodes first. No output type is required to surface a failed task, also when its task name is unknown locally. The decode refuses a key that the error type does not declare, and an error without `error_code`. The refusal names the cause.
3. Otherwise the ok slot is read through its JSON text, strictly, against the output type.

A read that cannot decode returns an error with a code. The codes of the Python client are in [Decode Errors](/horsies/tasks/task-types/#decode-errors); the read errors of the Rust client are in [ReadError](/horsies/rust/tasks/retrieving-results/#readerror).

## Strict Values

A client checks each value strictly against its declared type. A wrong value is refused, not converted. A stored value is read through its JSON text, also strictly.

A send, a schedule, a workflow definition and a task result decode what they encoded, with the decode that the worker uses. A value that a reader cannot read is refused before it is stored.

## Strict JSON

The core reads and writes JSON text with these rules:

- `NaN`, `Infinity` and `-Infinity` are refused.
- A string with a lone UTF-16 surrogate, such as `\ud800`, is refused. A surrogate pair is one valid character and is accepted.
- A value that user code builds (a task input, a result value, the `data` of a task error) has at most 200 levels of arrays and objects. A stored text has at most 300 levels, so the engine has room for the envelopes that it puts around a value.
- A value that user code builds holds no key that only the engine writes: a key that starts with `__h_` or `__horsies_`, and the built-in code tag `__builtin_task_code__`.

## The Error Path

An error is the report of a failure, and `retry_for` and outcome routing read its code. So an error always encodes, and its code and message stay.

- A lone surrogate in the code, the message or the exception text is escaped.
- The `data` of an error must be JSON-native. A value that is not, or that a reader cannot read, is replaced with `{'data_refused': <cause>}`. The cause names a type, never the value.

The Python rules of `TaskError` subclasses and `SubWorkflowError` are in [The Error Path](/horsies/tasks/task-types/#the-error-path) of Task Types.
