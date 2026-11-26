---
title: Task Types
section: python
summary: The types that a Python task can take and return, the shapes that the client refuses, the strict value rules, JsonValue and JsonInput, and the codec functions.
related: [../internals/serialization, defining-tasks, error-types]
tags: [serialization, codec, pydantic, JsonValue, JsonInput, types]
---

# Task Types

A Python task declares the type of each parameter and of its result, and `pydantic.TypeAdapter` encodes and decodes each value from that type. The wire envelope, the strict JSON rules and the error path are on [Serialization](/horsies/internals/serialization/).

`@app.task` refuses a banned type at registration: `Any`, `object`, bare `dict`/`list`/`tuple`, `TypeVar`, bare `BaseModel`, `TypedDict`, `bytes`, `set`/`frozenset`, `Callable`, and `pathlib.PurePath` subclasses. `JsonValue` is the only untyped fence. Use it at task boundary positions or inside `BaseModel`/`@dataclass` fields when the payload is genuinely raw JSON. `JsonInput` holds the same values, for a position that only accepts them. See [`JsonInput`](#jsoninput).

## Type Surface at the Boundary

Values flowing through a task signature must classify as **one** of:

- A JSON primitive (`None`, `bool`, `int`, `float`, `str`).
- `datetime.datetime` / `date` / `time`, `uuid.UUID`, `decimal.Decimal`. These are written as JSON strings and read from them. A str at input is refused.
  Datetime caveats: a `ZoneInfo`-aware datetime decodes with a **fixed-offset** timezone
  (same instant, zone semantics lost — DST arithmetic on the consumer differs), and
  **naive** datetimes round-trip naive (ambiguous across workers in different timezones —
  prefer UTC-aware values).
- `JsonValue` or `JsonInput` (at boundary positions only).
- An `Enum` subclass.
- A concrete `BaseModel` subclass (not bare `BaseModel`).
- A `@dataclass`.
- `list[T]`, `tuple[T, ...]`, `dict[str, T]`, `Literal[...]`, `Annotated[T, ...]`, `T | None`, or a discriminated union — recursively classified.
- `TaskOutcome[T, TaskError]` or `TaskOutcome[T, TaskError] | None`, as a whole parameter only. The engine fills it through `args_from` with `Succeeded(value)` or `Failed(task_error)`. The second type argument must be `TaskError`, and a default must be `None`.

Anything outside this surface raises `SignatureValidationError` at `@app.task` time. The error names the banned type and prints the documented fix.

The same rules apply to each parameter of a `build_with` that a `SubWorkflowNode` targets, because its kwargs are stored and read back. See [Subworkflows](/horsies/concepts/workflows/subworkflows/).

<!-- check: skip partial -- uses names that the page does not define -->
```python
from horsies import Horsies, Ok, Result, TaskError, JsonValue
from pydantic import BaseModel

app = Horsies(config)

class Order(BaseModel):
    id: int
    items: list[str]

@app.task("process_order")
def process_order(*, order: Order) -> Result[Order, TaskError]:
    return Ok(order)
```

The wire stores the Pydantic JSON form (`{"id": 1, "items": ["widget"]}`), with no per-value class tag. The worker decodes it back to `Order` because the registered task declared `Order` as the parameter type.

For raw JSON payloads where the inner shape is genuinely dynamic:

```python
@app.task("validate_input")
def validate_input(
    *,
    data: dict[str, JsonValue],
) -> Result[dict[str, JsonValue], TaskError]:
    return Ok(data)
```

## Strict Values

The codec validates in strict mode at every boundary. A wrong value is refused, not converted.

| Value | Declared type | Result |
| ----- | ------------- | ------ |
| `"7"`, `7.0`, `True` | `int` | Refused |
| `"yes"`, `1` | `bool` | Refused |
| `1700000000`, `"2026-01-01"` | `datetime` | Refused |
| `"red"` | an `Enum` | Refused. Pass the member |
| `(1, 2)`, `{1, 2}` | `list[int]` | Refused |
| `1` | `float` | Accepted, as `1.0` |
| `{"n": 1}` | a `BaseModel` | Accepted, checked field by field |
| `{"n": 1}` | a dataclass | Refused. Pass an instance |

A stored value is read through its JSON text, also in strict mode. A datetime reads from its string, and `"7"` does not read as an int.

**Read-back.** `.send()`, a schedule, a workflow definition and a task result decode what they encoded, with the decode that the worker uses. A value that a reader cannot read is refused before it is stored. This finds a value that validation does not see, such as a mutated instance or `model_construct`.

**Lost data.** The value and its read-back are compared. These are refused:

- An unknown key in a dict, at any depth.
- A subclass instance with fields that the declared type does not have. A subclass with no new field is accepted.
- A model field that a serializer drops, when its value differs from the default.

**Unknown keys on read.** A stored key that the model no longer declares is ignored, so old rows stay readable after a model change. See [Change a Model](/horsies/operations/deploying-code-changes/#change-a-model).

**Omitted parameters.** An omitted parameter crosses no wire, so its default is validated before the task body runs, and the function gets the returned value. A list or a dict default is a fresh copy for each run. A model default stays the same object.

**Validators.** A validator can run more than once on one value, because the wrapper, the encode and the read each run it. Write validators that give the same result when they run again.

## Shapes Refused at Definition

`@app.task` and each `SubWorkflowNode` refuse a shape that cannot cross the wire, or that cancels the check. Each spelling gets the same answer: `Annotated`, a `Field` default, a decorator, a stdlib or a pydantic dataclass, nested or generic.

| Shape | Why | Instead |
| ----- | --- | ------- |
| `Field(exclude=True)`, `exclude_if` | The field is not written, so the reader gets the default | Keep the field out of this type |
| `alias`, `alias_generator` or `AliasPath` with a key that the read does not accept | The written key does not read back | Set `populate_by_name=True` or `serialize_by_alias=True`, or add the field name to `AliasChoices` |
| `extra='allow'` | Extra fields have no type | Declare each field, or add a `dict[str, T]` field |
| `PlainValidator`, `WrapValidator`, `mode='plain'`, `mode='wrap'` | The validator replaces or can skip the check | `AfterValidator` or `BeforeValidator` |
| `SkipValidation`, any position that accepts any value | No check runs | A concrete type, or `JsonValue` |
| A class or metadata with its own `__get_pydantic_core_schema__`, `GetPydanticSchema` | The hook replaces the check | A type from the allow list |
| `computed_field` with `extra='forbid'` | The field is written, and the read refuses it | Remove one of the two |
| `Json[T]` | The value is written as JSON, not as a JSON string | Declare `T` |
| `Literal` with a plain `Enum` member | The member is written as its value, and `Literal` refuses that | The `Enum` type, or a `str` or `int` Enum |
| A dataclass with `revalidate_instances` other than `never` | Revalidation drops the fields of a subclass | Remove the setting |
| A default that does not hold its declared type | The reader or the function would get a wrong type | A default that is a value of the type |

A default is checked against the type only. No validator runs at import, because a validator can need a model, a context or a service.

A self-check runs each rule once per process. The rules read core schema keys that pydantic owns, so a pydantic that renames a key crashes loud, with the rule, the version and the fix. It does not switch a rule off.

## What the Contract Does Not Cover

- `PrivateAttr` and an attribute set outside the declared fields are not part of the data, so they do not cross the wire.
- A validator that replaces an instance on purpose, such as `AfterValidator` that builds a base class, keeps its result.
- A serializer on a dataclass that drops a field with a default is not found. A required field is refused at read-back.
- `default_factory` is not called at definition, because it may have side effects. A wrong type is refused on each write. A read of an old row with the key missing is not checked.
- A field default is not validated at read, as in pydantic. Write it in its final form, or set `validate_default=True`.
- A constraint on a union, such as `Annotated[int | float, Field(gt=0)] = -1`, is not checked on a default.

## The Error Path

The rules of an error that crosses the wire are on [The Error Path](/horsies/internals/serialization/#the-error-path). In Python:

- `TaskError.data` takes `JsonInput` and gives `JsonValue`, so pyright flags a value that is not JSON-native at the call, and takes a `list[str]`.
- A `SubWorkflowError` keeps its class, its id, its status and its counts. If its output cannot cross, the output is `None`.
- A `TaskError` subclass with extra fields does not cross the wire, and comes back as a plain `TaskError`. Put the values in `data`, and match on `error_code`.
- A `str` or `dict` subclass in `data` passes and reads as a plain `str` or `dict`. A `StrEnum` member reads as its value.

## Pydantic Models in Workflows

Models flowing through `args_from` ride through the same envelope. The downstream task declares `TaskOutcome[Model, TaskError]` (or `TaskOutcome[Model, TaskError] | None`) as the parameter type. The engine decodes the upstream envelope and passes a typed `Succeeded(model)` or `Failed(task_error)` to the parameter.

```python
from pydantic import SecretStr
from datetime import datetime, timezone
from pydantic import BaseModel
from horsies import (
    Horsies,
    AppConfig,
    PostgresConfig,
    Err,
    Failed,
    Ok,
    Result,
    Succeeded,
    TaskError,
    TaskNode,
    TaskOutcome,
)

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr("postgresql://user:password@localhost:5432/mydb"),
    ),
)
app = Horsies(config)

class Order(BaseModel):
    item: str
    total: float
    created_at: datetime

@app.task("create_order")
def create_order() -> Result[Order, TaskError]:
    return Ok(Order(
        item="widget",
        total=9.99,
        created_at=datetime.now(timezone.utc),
    ))

@app.task("process_order")
def process_order(
    *,
    order_result: TaskOutcome[Order, TaskError],
) -> Result[str, TaskError]:
    match order_result:
        case Failed(error):
            return Err(error)
        case Succeeded(order):  # typed Order, not a dict
            return Ok(f"Processed {order.item}")

node_create: TaskNode[Order] = TaskNode(fn=create_order)
node_process: TaskNode[str] = TaskNode(
    fn=process_order,
    waits_for=[node_create],
    args_from={"order_result": node_create},
)
```

## Codec Layout

| Module | Purpose |
|--------|---------|
| `horsies.core.types.json_value` | `JsonValue` PEP 695 alias; `JsonInput`, `JsonList`, `JsonDict` |
| `horsies.core.codec.json_value` | `StrictJsonError`; strict JSON-native validators; re-exports `JsonValue` |
| `horsies.core.codec.json_io` | `dumps_json` / `loads_json` — the strict JSON-native stringification boundary (no class tags). `loads_json` takes a `str`, never `None`; an empty text is not JSON and is `Err(EmptyJsonTextError)`, a `SerializationError`. A unit test keeps every other JSON parse out of the library |
| `horsies.core.codec.error_payload` | `flatten_exception`; `serialize_error_payload` for library-emitted error payloads |
| `horsies.core.codec.task_options` | `serialize_task_options` — `TaskOptions` → persisted JSON |
| `horsies.core.codec.signature_check` | `check_task_signature` — rejects banned types at decorator time |
| `horsies.core.codec.kwargs` | `encode_kwargs_read_back(task_fn, kwargs)` / `decode_kwargs(...)` for producer/consumer kwarg binding |
| `horsies.core.codec.typed` | `encode_value` / `decode_value` / `encode_task_result` / `decode_task_result` / `decode_task_error` / `validate_task_result_envelope` |

Re-exports: `from horsies import JsonDict, JsonInput, JsonList, JsonValue, StrictJsonError`.

## Encode and Decode

The worker calls `encode_task_result(result, ok_type)` after the task function returns. A wrong value or a serializer failure raises `ValueValidationError`; the text holds no input value.

```python
from horsies.core.codec.typed import encode_task_result
from horsies import Ok

envelope = encode_task_result(
    Ok(42),
    ok_type=int,
)
# {"__h_task_result__": True, "ok": 42, "err": None}
```

The consumer holds the declared `ok_type` (from the task registry) and calls `decode_task_result(envelope, ok_type)`.

### Decode Errors

| Code | Source | When |
|------|--------|------|
| `BrokerErrorCode.INVALID_JSON_PAYLOAD` | raw result read (`app.get_raw_result`, `broker.get_raw_result_record`), task info read (`handle.info(include_result=True)`) | the stored result does not parse as JSON, or is not a JSON object. For the task info read, also a malformed envelope or a failed typed slot decode |
| `OperationalErrorCode.RESULT_DESERIALIZATION_ERROR` | typed read (`TaskHandle.get`, `app.get_result`, `WorkflowHandle.get`, `WorkflowHandle.result_for`) | the stored result does not parse, has a malformed envelope, or fails the typed slot decode. The read returns `Err(ResultDeserializationError(...))` |
| `ContractCode.NO_TYPE_AVAILABLE` | typed read | the task succeeded, and its `task_name` is not in the local task registry, so no `ok_type` is available to decode the ok slot. The read returns `Err(NoTypeAvailable(...))` |
| `OperationalErrorCode.RESULT_DESERIALIZATION_ERROR` or `ContractCode.NO_TYPE_AVAILABLE` | node map (`WorkflowHandle.results()`, the terminal results of an outputless workflow's `get()`) | a node whose stored result does not decode stays in the map as `Failed(TaskError(error_code=...))` with one of these codes |

## API Reference

### `encode_task_result(result, ok_type) -> dict[str, Json]`

Encode a `Result[T, TaskError]` to the wire envelope.

| Parameter | Type | Description |
|-----------|------|-------------|
| `result` | `Result[T, TaskError]` | Domain result returned by the task: `Ok(value)` or `Err(task_error)` |
| `ok_type` | `TypeAnnotation` | Declared `T` from the task signature |

**Raises:** `ValueValidationError` if the value of `Ok(value)` does not match `ok_type` or pydantic cannot serialize it; `StrictJsonError` for non-JSON-serializable structures. `ValueValidationError` (`horsies.core.codec`) is a `SerializationError` and a `ValueError`; its text names each failing field, the message and the error type, not the value.

### `decode_task_result(envelope, ok_type) -> TaskOutcome[T, TaskError]`

Decode a wire envelope into a typed `TaskOutcome`: `Succeeded(value)` or `Failed(task_error)`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `envelope` | `dict[str, Json]` | The `__h_task_result__` envelope |
| `ok_type` | `TypeAnnotation` | Declared `T` from the task signature (only used when the `ok` slot is populated) |

### `decode_task_error(err_slot) -> TaskError`

Decode the `err` slot independently of `ok_type`. Used by consumers reading failed tasks whose task name is unknown locally.

### `validate_task_result_envelope(envelope) -> dict[str, Json]`

Cheap shape check before per-slot decode. Returns the envelope as `dict[str, Json]` on success; raises `StrictJsonError` if the marker is missing, the key set is wrong, or both slots are populated.

### `encode_value(value, type) / decode_value(payload, type)`

Primitive entry points for values that are not a task result (workflow kwargs, success policies, args_from blocks). Same `TypeAdapter`-driven semantics.

**Raises:** `encode_value` raises `ValueValidationError` for a dict key without a JSON form (a `frozenset`, or an Enum member whose value is a list, a dict or a set), for a value that holds itself, for a value that is nested too deep, and for `bytes` that are not UTF-8. Any other `TypeError` or `ValueError` from the dump propagates, for example from a computed field.

**Validators:** a validator refuses a value with `ValueError` or `AssertionError`. Any other exception from a validator is a bug: it propagates, and `encode_value` and `decode_value` do not catch it.

### `JsonValue`

<!-- check: skip elided -- shows the library definition, not user code -->
```python
type JsonValue = (
    None | bool | int | float | str
    | list[JsonValue]
    | dict[str, JsonValue]
)
```

PEP 695 type alias. Re-exported as `horsies.JsonValue`. This is what a reader gets: an exact `list` or `dict`, so a match over them is exhaustive.

### `JsonInput`

A type checker does not accept a `list[str]` for a `list[JsonValue]`, because a `list` is mutable and the receiver could add an `int` to it. `JsonInput` is what a position accepts. It holds the same values as `JsonValue`, and a `list[str]` or a `dict[str, int]` fits it. It exists only for a type checker: at run time `JsonInput` is `JsonValue`, and every check of `JsonValue` applies.

| Value | Type checker | Run time |
|---|---|---|
| A `list` or `dict` of JSON values, at any depth, with `str` keys | Accepted | Accepted |
| A subclass of `dict`, such as `OrderedDict` or `Counter` | Accepted | Accepted, read as a `dict` |
| A tuple, `bytes`, a deque or a set | Flagged | Refused |
| A `Mapping` that is not a `dict`, such as `mappingproxy`, `UserDict` or `ChainMap` | Flagged | Refused |
| `dict[int, str]`, `list[Decimal]`, or any other bad value inside a container | Flagged | Refused |

`JsonList` and `JsonDict` are the protocols behind it, and they appear in hover text and in errors. `JsonList` has `pop` with an index, because only a `list` has that form. `JsonDict` has `__reversed__` and `popitem`, because only a `dict` has both.

`TaskError.data` takes `JsonInput` and gives `JsonValue`, and so does a subclass of `TaskError`.

A task parameter has one annotation, which serves `.send()` and the task body. A return type has one too. Choose by what the task does with the value:

| Annotation | Use it when | Result |
|---|---|---|
| A concrete type | The shape is known | The best choice: `.send()` and the body both get the exact type |
| `JsonValue` | The task reads the value | An exact read. Pyright flags a `list[str]` variable at `.send()` |
| `JsonInput` | The task passes the value on | `.send()` takes a `list[str]`. A read in the body gives partly unknown types in strict mode |

Pyright flags an open `TypedDict` as `JsonInput`, because it can hold keys that it does not declare. Pass a `dict`, or use a model. Pyright also flags a `TaskError` that is passed to a user's read-only `Protocol` with a `data` property. mypy accepts that form.

### `StrictJsonError`

Raised by strict codec primitives and the JSON parse guards for strict JSON violations. The parse guards reject the constants `NaN`, `Infinity` and `-Infinity`, and a numeric literal outside the finite binary64 range, such as `1e9999`. A literal that underflows, such as `1e-9999`, parses to `0.0`. They also reject a string with a lone surrogate, such as `\ud800`; a surrogate pair in JSON text is one valid character and stays accepted. `encode_value` refuses each string that `dumps_json` refuses: a lone surrogate in a string or in a dict key raises `StrictJsonError` at any depth. In a Python `str`, a surrogate pair is two code points, so it is refused too. `encode_value` and `decode_value` also raise `StrictJsonError` when the error text of a validator holds a lone surrogate, because pydantic cannot build that text. The text names the kwarg and holds no input value. `dumps_json` / `loads_json` return `SerdeResult` and wrap stringification or parse failures as `SerializationError`. Re-exported as `horsies.StrictJsonError`.

`MAX_TYPED_JSON_DEPTH` (200) is the deepest nesting of a value under a declared type, the limit of the strict JSON read. A deeper value is refused at read-back. `MAX_JSON_DEPTH` (300) is the deepest nesting of lists and dicts at the JSON boundary, which also holds the envelopes around a value. `dumps_json` and the fence of `encode_value` at a `JsonValue` position refuse a deeper value and a value that holds itself. `loads_json` refuses a deeper text, so each walker that gets a parsed value stays inside the recursion limit of Python. The bound is above the limit of a pydantic dump, which refuses at 255 levels: a value that `encode_value` accepts also passes the bound inside its envelope.

## Things to Avoid

**Don't use `Any`, `object`, or bare containers in task signatures.** They are rejected at registration time.

<!-- check: skip elided -- shows a wrong form next to the right one -->
```python
# Wrong — raises SignatureValidationError
@app.task("bad")
def bad(*, data: dict) -> Result[dict, TaskError]:
    return Ok(data)

# Correct — declare the contents
@app.task("good")
def good(*, data: dict[str, JsonValue]) -> Result[dict[str, JsonValue], TaskError]:
    return Ok(data)
```

**Don't define result types in `__main__` or inside functions.** Workers import types by module path; classes without an importable qualname cannot participate in typed decode.

**Don't smuggle reserved keys into `TaskError.error_code` or `TaskError.data`.** `TaskError.error_code` accepts the built-in `OperationalErrorCode` / `RetrievalCode` / `ContractCode` enums or any non-reserved string. Reserved built-in codes passed as strings (e.g. `"WORKER_RESOLUTION_ERROR"`) are rejected — pass the enum member directly.
