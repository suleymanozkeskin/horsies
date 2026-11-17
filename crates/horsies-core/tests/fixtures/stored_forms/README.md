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
| `result_envelope.json` | Result envelope (`{"__h_task_result__": true, "ok": ..., "err": ...}`) |
| `error_data.json` | Library errors with `data`, as stored |

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
  core's `StrictJsonError` variant. In `result_envelope.json`, `refusal` is
  `envelope` (the shape is refused) or `ok value` (the decode of the `ok` slot
  is refused; the core's `read_task_result_text` keeps that slot as text, as
  Python's `validate_task_result_envelope` does).
- `stored_data`: the `data` that a writer stores in place of a value that it
  refuses (`{"data_refused": "<cause>"}`); the read gives it back.
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

## Result envelope (`result_envelope.json`)

`{"__h_task_result__":true,"ok":<value>,"err":null}` or
`{"__h_task_result__":true,"ok":null,"err":<error>}`. An error object has the
keys `exception`, `error_code`, `data`, `message`, in this order, nulls kept;
a sub-workflow error adds `sub_workflow_id` and `sub_workflow_summary`. A
built-in code is `{"__builtin_task_code__": "<NAME>"}`, a user code its text.
A vector holds an `ok` typed value (the notation above) or an `err` task error
(`{"error_code": {"built_in" | "user": ...}, "message", "data", "exception",
"sub_workflow"?}`).

The history storage envelope of a result (`result_envelope_version` 1, codec
`json-utf8`, a SHA-256 digest) wraps this text byte for byte. Its vectors are
`task_history/python-v052-codec-vectors.json` (`result_envelopes`), tested by
`crates/horsies-core/src/core/history/tests.rs`
(`result_vectors_match_python_bytes`).

Encoders:

- Python: `encode_returned_result` for a task body's return
  (`python/horsies/core/codec/typed.py:862`), `encode_task_result` for a
  library error (`:798`), the error object by `encode_task_error` (`:1053`),
  then `dumps_json`.
- Core: `TaskResult`'s `Serialize` (`crates/horsies-core/src/core/task/result.rs:42`)
  and `TaskError`'s (`crates/horsies-core/src/core/task/error.rs:781`), written
  through `task_result_to_json_string`
  (`crates/horsies-core/src/core/codec/mod.rs:99`). The child host puts the
  text of a task body's ok value in the envelope as it is (`result_end`,
  `crates/horsies-core/src/worker/executor/child_host/run.rs:380`) and writes a
  task error with `error_envelope` (`:325`).
- Rust client: `encode_output` (`crates/horsies/src/task/body.rs:169`) for an
  ok value; `body_result` (`:152`) and `TaskError::into_stored`
  (`crates/horsies/src/task/error.rs:155`) for an error.

Decoders:

- Python: `loads_json`, `validate_task_result_envelope`
  (`python/horsies/core/codec/typed.py:1133`), `decode_task_result` (`:1183`),
  `decode_task_error` (`:1078`).
- Core: `TaskResult`'s `Deserialize` (`crates/horsies-core/src/core/task/result.rs:63`),
  `read_task_result_text` (`:158`), `TaskError::from_stored_value`
  (`crates/horsies-core/src/core/task/error.rs:665`). A result read keeps the
  stored text (`StoredEnvelopeText::parse`,
  `crates/horsies-core/src/broker/result_types.rs:146`).
- Rust client: `outcome_of_record` (`crates/horsies/src/handle.rs:327`) and
  `stored_result` (`crates/horsies/src/reads/by_task_id.rs:114`), on the
  core's typed read.

The Rust client builds an error from a code, a message and `data`. The
exception record and the sub-workflow failure are written by the library, and
`TaskError::user` and `TaskError::library` take a message: the Rust tests only
read those vectors.

## Error data (`error_data.json`)

One vector per library error with `data` that the core writes, in the form
that it is stored: `stored_as` is `envelope` (the task or node result) or
`error` (the error object of the workflow error column). `python_writer` and
`core_writer` name the two writers; `inputs` holds the inputs of a writer that
the tests call. The texts are Python's encoder output for the data of
Python's writer; for `TASK_TIMEOUT`, `WORKER_CRASHED` (stale),
`WORKFLOW_START_INCOMPLETE` and `WORKFLOW_RECOVERY_LIMIT_REACHED` the Python
test calls the writer itself.

| Code | `data` keys, in order | Python writer | Core writer |
|---|---|---|---|
| `TASK_TIMEOUT` (4 vectors: `cancel_end` `RETURNED_AFTER_SIGNAL`, `DROPPED_AFTER_GRACE`, `KILLED_WITH_CHILD`, and none) | `task_id`, `timeout_ms`, `cancel_end` | `python/horsies/core/worker/shared_stop.py:148` (`timeout_error_data`), `python/horsies/core/worker/dispatch.py:1101` | `crates/horsies-core/src/worker/executor/slots/outcome.rs:243` (`timeout_write`) |
| `WORKER_CRASHED`, killed child | `task_id`, `worker_id` | `python/horsies/core/worker/dispatch.py:333-339` | `crates/horsies-core/src/worker/executor/slots/outcome.rs:212` (`killed_child_write`) |
| `WORKER_CRASHED`, stale | `stale_threshold_ms`, `stale_threshold_seconds`, `worker_pid`, `worker_hostname`, `worker_id`, `started_at`, `detected_at` | `python/horsies/core/brokers/stale_running.py:178` (`crash_result_json`) | `crates/horsies-core/src/worker/recovery.rs:1450` (`stale_attempt_command`) |
| `UPSTREAM_SKIPPED` | `dependency_index` | `python/horsies/core/workflows/engine.py:2496-2499` | `crates/horsies-core/src/workflow_engine/args_merge.rs:330`; SQL `horsies_task_error_envelope` |
| `RESULT_DESERIALIZATION_ERROR`, dependency without a result | `dependency_index` | `python/horsies/core/workflows/engine.py:186-193` | `crates/horsies-core/src/workflow_engine/args_merge.rs:345` |
| `WORKFLOW_OUTPUT_SKIPPED` | `workflow_id`, `output_node_id`, `output_task_index`, `output_task_name` | `python/horsies/core/workflows/engine.py:3398-3420` | SQL `horsies_workflow_output_error_data` (`crates/horsies-core/baseline/0005_workflow_functions.sql:1309`) |
| `WORKFLOW_ENQUEUE_FAILED`, `args_from` conflict | `workflow_id`, `task_index` | `python/horsies/core/workflows/engine.py:545-551` | `crates/horsies-core/src/workflow_engine/args_merge.rs:111` (`node_error`); SQL |
| `SUBWORKFLOW_DEPTH_LIMIT_REACHED` | `workflow_id`, `task_index` | `python/horsies/core/workflows/engine.py:1068-1078` | `crates/horsies-core/src/workflow_engine/start.rs:193`; SQL |
| `SUBWORKFLOW_LOAD_FAILED` | `definition_key` | `python/horsies/core/workflows/engine.py:616-619` | `crates/horsies-core/src/workflow_engine/start_statement.rs:707` |
| `WORKFLOW_RECOVERY_LIMIT_REACHED` | `workflow_id`, `case`, `task_index`, `failed_passes`, `exception_type` | `python/horsies/core/workflows/recovery_limit.py:525` (`recovery_limit_error`) | `crates/horsies-core/src/workflow_engine/recovery_limit.rs:526`; SQL (`0005_workflow_functions.sql:4099`) |
| `WORKFLOW_START_INCOMPLETE` (error object) | `workflow_id`, `recovery` | `python/horsies/core/workflows/recovery.py:580` (`orphaned_workflow_error`) | `crates/horsies-core/src/workflow_engine/recovery.rs:1748` |
| `WORKFLOW_EXPIRED` (error object) | `policy`, `older_than_seconds` | `python/horsies/core/workflows/lifecycle.py:845-857` | `crates/horsies-core/src/workflow_engine/lifecycle.rs:300` |
| `UNHANDLED_EXCEPTION`, with the exception record | `exception_type` | `python/horsies/core/task_decorator.py:2031-2036` | `crates/horsies-core/src/worker/executor/child_host/panic.rs:175` (a Rust panic) |

The SQL writer is `horsies_task_error_envelope`
(`crates/horsies-core/baseline/0005_workflow_functions.sql:99`); a DB test
calls it with the code, the message and the data text of each engine vector.
The core's tests call `timeout_write`, `killed_child_write`,
`stale_attempt_command`, `upstream_skipped_error`, `missing_result_error` and
`node_error`; the other core writers build their data with `json!` in the key
order of the table, and the generic test pins the encoding of each vector.
The Rust client writes none of these errors; its test reads each envelope
through `outcome_of_record`.

## Findings

Differences that the vectors found. A fixed one names its PR; an open one is a
`rust_drift` or a `rust_cannot_express` row, and is described on #1603.

| Finding | Form | Facts | State |
|---|---|---|---|
| member order | every form | serde_json without `preserve_order` wrote each object with sorted keys; Python writes insertion order (`TASK_TIMEOUT` data: `task_id`, `timeout_ms`, `cancel_end`) | fixed, #1610 |
| float notation | every form | serde_json wrote `0.00001` and `1.5e-7`; Python writes `1e-05` and `1.5e-07` | fixed in the codec encoders, #1613 |
| SQL error key order | error data | `horsies_task_error_envelope` wrote `error_code`, `message`, `data`, `exception`; Python writes `exception`, `error_code`, `data`, `message` | fixed, #1623 |
| instants in stored data | error data, task options | the stale-crash `started_at` and `detected_at` and a node's merged `good_until` used `to_rfc3339()` (`.500`); Python's `isoformat()` writes `.500000` | fixed, #1623 |
| result read of a big integer | result envelope | the core's result read parsed the envelope into a `serde_json::Map`: `2**64` came back as `1.8446744073709552e+19`, which Python's and the Rust client's typed reads refuse | fixed, #1617 |
| `no-parameter-input` | task input | a task with no parameters: Python stores `{}`, the Rust client stores `null` (its input is `()`) | open |
| `datetime-milliseconds` | task input, result | chrono writes a fraction of whole milliseconds with 3 digits (`.500Z`); Python writes 6 (`.500000Z`). Microseconds and whole seconds agree | open |
| not expressible in Rust | task input, result | `big_int`, `decimal`, `bytes`, `int_enum`, a typed map in insertion order: the client depends on no type that writes Python's text | listed per row |
