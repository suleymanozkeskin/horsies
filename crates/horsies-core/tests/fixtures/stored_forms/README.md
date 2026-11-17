# Stored JSON forms: shared golden vectors

Both clients write and read the same stored JSON forms through the one core.
Each file of this folder pins one form: a vector is a value and the exact text
that Python stores for it. The Python client, the core and the Rust client
each encode every value to the text and decode the text to the value, so a
client that drifts fails its own test.

The folder is in the core's test fixtures because the core owns the stored
forms (one schema and one set of SQL functions for both clients), next to the
core's other cross-client fixtures (`task_history/`). Each side reads it by
path:

| Side | Tests | Path to this folder |
|---|---|---|
| Python | `tests/unit/test_stored_form_*.py`, helpers in `tests/stored_form_vectors.py` | `VECTORS_DIR` |
| Core | `crates/horsies-core/src/stored_form_vectors/` | `CARGO_MANIFEST_DIR/tests/fixtures/stored_forms` |
| Rust client | `crates/horsies/src/stored_form_vectors/` | `CARGO_MANIFEST_DIR/../horsies-core/tests/fixtures/stored_forms` |

Python's text is the reference: every vector text comes from Python's own
encoder, and the Python tests check that it still does.

## Files

| File | Form |
|---|---|
| `task_input.json` | Task input (`input_json`) |

## Rows

- `vectors`: `name`, the value, `text` (Python's exact text).
  - `rust_cannot_express`: the Rust client has no type for the value; the text
    gives the reason. The Rust tests skip the row.
  - `rust_drift`: `{"text", "finding"}`: the text that the Rust client writes
    today in place of Python's. The Rust tests assert this text, so a change of
    either client fails a test; the finding is in [Findings](#findings). The
    Rust tests still read Python's text back to the value.
- `refused_values`: values that every encoder refuses.
- `refused_texts`: texts that every reader refuses. `core_refusal` names the
  core's `StrictJsonError` variant.
- `typed_refused_texts`: texts that read as JSON and do not decode under the
  given parameter types.

A vector file holds no lone UTF-16 surrogate: the Rust readers refuse the file
itself, and a Rust `String` cannot hold one. Python refuses one at its encoder
(`tests/unit/test_typed_codec.py`).

## Value notation

A typed value is `{"type": <name>, "value": <notation>}` (a task input
parameter adds `"name"`). Integers are decimal text, so that no reader of the
file rounds one.

| Type | Notation | Python | Rust |
|---|---|---|---|
| `bool` | JSON boolean | `bool` | `bool` |
| `i64`, `u64`, `i128`, `u128` | decimal text | `int` | the named type |
| `big_int` | decimal text | `int` | none (above `u128::MAX`) |
| `f64` | float text, `nan`, `inf`, `-inf` | `float` | `f64` |
| `str` | JSON string | `str` | `String` |
| `datetime_utc` | ISO 8601 text | aware `datetime` | `DateTime<Utc>` |
| `datetime_offset` | ISO 8601 text | aware `datetime` | `DateTime<FixedOffset>` |
| `datetime_naive` | ISO 8601 text | naive `datetime` | `NaiveDateTime` |
| `date` | ISO 8601 date | `date` | `NaiveDate` |
| `uuid` | hyphenated text | `UUID` | `Uuid` |
| `decimal` | decimal text | `Decimal` | none |
| `bytes` | UTF-8 text | `bytes` | none |
| `str_enum` | `red` or `green` | `Color(str, Enum)` | `Color`, `rename_all = "lowercase"` |
| `int_enum` | decimal text | `Level(IntEnum)` | none |
| `optional_i64` | `null` or decimal text | `int \| None` | `Option<i64>` |
| `list_i64` | list of decimal texts | `list[int]` | `Vec<i64>` |
| `map_str_i64` | object of decimal texts, in order | `dict[str, int]` | `BTreeMap<String, i64>` (sorted keys only) |
| `json` | the JSON value | `JsonValue` | `serde_json::Value` |
| `record_order` | `{"id": <decimal>, "amount": <float>}` | `Order(BaseModel)` | `Order` struct, same field order |

## Task input (`task_input.json`)

One JSON object of keyword arguments: one member per parameter, keyed by the
parameter's name, in declaration order. Both clients store every parameter
under its name, a single struct parameter too (`fn f(order: Order)` stores
`{"order": {...}}`, #1406). Python writes the members in the order of the
call's keyword arguments; the vectors give them in declaration order, the
order of the Rust struct.

Encoders:

- Python: `encode_kwargs_read_back` (`python/horsies/core/codec/kwargs.py:299`),
  then `dumps_json` (`python/horsies/core/codec/json_io.py:43`); the send path
  is `python/horsies/core/task_decorator.py:2426-2432`.
- Rust client: the `Input` struct of the task attribute
  (`crates/horsies-macros/src/codegen.rs:482`, `input_struct`), encoded by
  `encode_read_back` (`crates/horsies/src/task/body.rs:186`) through the core's
  `to_json_bytes_strict` (`crates/horsies-core/src/core/codec/strict.rs:26`);
  the send path is `crates/horsies/src/send/function.rs:343`.
- Core: none. The send checks the text with the strict read rule
  (`crates/horsies-core/src/core/history/enqueue.rs:198`, `check_json_text` at
  `crates/horsies-core/src/core/codec/strict_read.rs:131`) and stores it as
  written.

Decoders:

- Python: `loads_json` (`python/horsies/core/codec/json_io.py:97`), then
  `decode_kwargs` (`python/horsies/core/codec/kwargs.py:496`), in the child at
  `python/horsies/core/worker/child_runner.py:1300-1329`.
- Core: the child host passes an input without engine members byte for byte
  (`crates/horsies-core/src/worker/execution.rs:186`, `build_task_input`).
- Rust client: `decode_input` (`crates/horsies/src/task/body.rs:48`);
  `decode_unit_input` for a task with no parameters.

## Findings

Differences that the vectors found. A fixed one names its PR; an open one is a
`rust_drift` or a `rust_cannot_express` row, and is described on #1603.

| Finding | Form | Facts | State |
|---|---|---|---|
| member order | every form | serde_json without `preserve_order` wrote each object with sorted keys; Python writes insertion order (`TASK_TIMEOUT` data: `task_id`, `timeout_ms`, `cancel_end`) | fixed, #1610 |
| float notation | every form | serde_json wrote `0.00001` and `1.5e-7`; Python writes `1e-05` and `1.5e-07` | fixed in the codec encoders, #1613 |
| `no-parameter-input` | task input | a task with no parameters: Python stores `{}`, the Rust client stores `null` (its input is `()`) | open |
| `datetime-milliseconds` | task input, result | chrono writes a fraction of whole milliseconds with 3 digits (`.500Z`); Python writes 6 (`.500000Z`). Microseconds and whole seconds agree | open |
| not expressible in Rust | task input, result | `big_int`, `decimal`, `bytes`, `int_enum`, a typed map in insertion order: the client depends on no type that writes Python's text | listed per row |
