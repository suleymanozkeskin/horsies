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
| Python | `crossclient/test_stored_form_*.py`, helpers in `crossclient/stored_form_vectors.py` | `VECTORS_DIR` |
| Core | `core/src/stored_form_vectors/` | `CARGO_MANIFEST_DIR/tests/fixtures/stored_forms` |
| Rust client | `clients/rust/horsies/src/stored_form_vectors/` | `CARGO_MANIFEST_DIR/../../../core/tests/fixtures/stored_forms` |

Python's text is the reference: every vector text comes from Python's own
encoder, and the Python tests check that it still does.

## Files

| File | Form |
|---|---|
| `task_input.json` | Task input (`input_json`) |
| `result_envelope.json` | Result envelope (`{"__h_task_result__": true, "ok": ..., "err": ...}`) |
| `error_data.json` | Library errors with `data`, as stored |
| `task_options.json` | Task options text (`task_options` column) |
| `workflow_spec_data.json` | Workflow spec data, the text a client sends the core at a workflow start |
| `trace_carrier.json` | Trace carrier text (`trace_carrier` column of `horsies_tasks` and `horsies_workflows`) |

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
  core's `StrictJsonError` variant. `refusal_text`, for a refusal that
  Python's reader words itself (a non-standard constant, a number outside
  finite binary64, a lone surrogate), is Python's text of the refusal; the
  core's `StrictJsonError` writes the same text, and Python's `loads_json`
  writes it after `JSON parse failed: `. In `result_envelope.json`, `refusal` is
  `envelope` (the shape is refused) or `ok value` (the decode of the `ok` slot
  is refused; the core's `read_task_result_text` keeps that slot as text, as
  Python's `validate_task_result_envelope` does).
- `stored_data`: the `data` that a writer stores in place of a value that it
  refuses (`{"data_refused": "<cause>"}`); the read gives it back.
- `typed_refused_texts`: texts that read as JSON and do not decode under the
  given parameter types.

A vector file holds no lone UTF-16 surrogate: the Rust readers refuse the file
itself, and a Rust `String` cannot hold one. Python refuses one at its encoder
(`clients/python/tests/unit/test_typed_codec.py`).

Every form that holds user text has a vector with text outside ASCII, which
every writer stores as the characters, not as `\u` escapes:

| Form | Vectors |
|---|---|
| Task input | `str_unicode` |
| Result envelope | `ok_str_unicode`, `err_user_code_with_data` |
| Error data | `result_deserialization_error_non_ascii_task_name` (the message), `workflow_output_skipped_non_ascii_task_name` (the data) |
| Workflow spec data | `non_ascii_text` |

The task options hold no user text: an error code of `auto_retry_for` is
UPPER_SNAKE_CASE ASCII in both clients.

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
| `datetime_utc` | ISO 8601 text | aware `datetime` | `StoredDateTime<DateTime<Utc>>` |
| `datetime_offset` | ISO 8601 text | aware `datetime` | `StoredDateTime<DateTime<FixedOffset>>` |
| `datetime_naive` | ISO 8601 text | naive `datetime` | `StoredDateTime<NaiveDateTime>` |
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

A date-time's Rust type is `horsies::StoredDateTime`
(`clients/rust/horsies/src/task/stored_datetime.rs`), which writes Python's
fraction: 6 digits when the microsecond is not 0, else none. chrono's own
serde form of `DateTime` and `NaiveDateTime` writes a whole millisecond with
3 digits (`.500Z`); both clients read either text to the same value.

## Task input (`task_input.json`)

One JSON object of keyword arguments: one member per parameter, keyed by the
parameter's name, in declaration order. Both clients store every parameter
under its name, a single struct parameter too (`fn f(order: Order)` stores
`{"order": {...}}`, #1406). Python writes the members in the order of the
call's keyword arguments; the vectors give them in declaration order, the
order of the Rust struct.

Encoders:

- Python: `encode_kwargs_read_back` (`clients/python/horsies/core/codec/kwargs.py:299`),
  then `dumps_json` (`clients/python/horsies/core/codec/json_io.py:43`); the send path
  is `clients/python/horsies/core/task_decorator.py:2426-2432`.
- Rust client: the `Input` struct of the task attribute
  (`clients/rust/macros/src/codegen.rs`, `input_struct`), encoded by
  `encode_send_input` (`clients/rust/horsies/src/task/body.rs`): `{}` for a task
  with no parameters (the input `()`), else `encode_read_back` through the
  core's `to_json_bytes_strict` (`core/src/core/codec/strict.rs`);
  the send path is `clients/rust/horsies/src/send/function.rs` (`encoded_input`). A
  schedule without input stores `{}` (`schedule_input_text`,
  `clients/rust/horsies/src/config/scheduler_manifest.rs`).
- Core: none. The send checks the text with the strict read rule
  (`core/src/core/history/enqueue.rs:198`, `check_json_text` at
  `core/src/core/codec/strict_read.rs:131`) and stores it as
  written.

Decoders:

- Python: `loads_json` (`clients/python/horsies/core/codec/json_io.py:97`), then
  `decode_kwargs` (`clients/python/horsies/core/codec/kwargs.py:496`), in the child at
  `clients/python/horsies/core/worker/child_runner.py:1300-1329`.
- Core: the child host passes an input without engine members byte for byte
  (`core/src/worker/execution.rs:192`, `build_task_input`).
  It writes an input with engine members from its text: each other member
  and each result envelope keeps its stored text.
- Rust client: `decode_input` (`clients/rust/horsies/src/task/body.rs:48`);
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
`core/src/core/history/tests.rs`
(`result_vectors_match_python_bytes`).

Encoders:

- Python: `encode_returned_result` for a task body's return
  (`clients/python/horsies/core/codec/typed.py:862`), `encode_task_result` for a
  library error (`:798`), the error object by `encode_task_error` (`:1053`),
  then `dumps_json`.
- Core: `TaskResult`'s `Serialize` (`core/src/core/task/result.rs:42`)
  and `TaskError`'s (`core/src/core/task/error.rs:781`), written
  through `task_result_to_json_string`
  (`core/src/core/codec/mod.rs:99`). The child host puts the
  text of a task body's ok value in the envelope as it is (`result_end`,
  `core/src/worker/executor/child_host/run.rs:380`) and writes a
  task error with `error_envelope` (`:325`).
- Rust client: `encode_output` (`clients/rust/horsies/src/task/body.rs:169`) for an
  ok value; `body_result` (`:152`) and `TaskError::into_stored`
  (`clients/rust/horsies/src/task/error.rs:155`) for an error.

Decoders:

- Python: `loads_json`, `validate_task_result_envelope`
  (`clients/python/horsies/core/codec/typed.py:1133`), `decode_task_result` (`:1183`),
  `decode_task_error` (`:1078`).
- Core: `TaskResult`'s `Deserialize` (`core/src/core/task/result.rs:67`),
  `read_task_result_text` (`:123`), `TaskError::from_stored_text`
  (`core/src/core/task/error.rs:678`); the `ok` value and the
  error `data` decode from their own text. A result read keeps the
  stored text (`StoredEnvelopeText::parse`,
  `core/src/broker/result_types.rs:146`). The child host
  (`build_task_input`, an `args_from` member and a result of the workflow
  context) and the sub-workflow child build (`merged_child_input`,
  `core/src/workflow_engine/start_statement.rs:458`) give a
  dependent each envelope as its stored text; the core test passes each
  vector through both.
- Rust client: `outcome_of_record` (`clients/rust/horsies/src/handle.rs:360`) and
  `stored_result` (`clients/rust/horsies/src/reads/by_task_id.rs:115`), on the
  core's typed read. A dependency result (`TaskOutcome`'s `Deserialize`,
  `clients/rust/horsies/src/workflow/dependency.rs`) and
  `WorkflowContext::result_for` (`clients/rust/horsies/src/workflow/context.rs`)
  read the envelope's text with `read_stored_result`
  (`clients/rust/horsies/src/handle.rs:422`), as `outcome_of_record` does; the
  Rust test reads each `ok` vector through all three.

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
Python's writer. The Python writers of `TASK_TIMEOUT`, `WORKER_CRASHED`
(killed child and stale), `WORKFLOW_START_INCOMPLETE` and
`WORKFLOW_RECOVERY_LIMIT_REACHED` were removed in 0.8.0: their
`python_writer` names the file that held the writer when the vector was
made, and the core writer is the only writer.

| Code | `data` keys, in order | Python writer | Core writer |
|---|---|---|---|
| `TASK_TIMEOUT` (4 vectors: `cancel_end` `RETURNED_AFTER_SIGNAL`, `DROPPED_AFTER_GRACE`, `KILLED_WITH_CHILD`, and none) | `task_id`, `timeout_ms`, `cancel_end` | removed in 0.8.0 | `core/src/worker/executor/slots/outcome.rs:243` (`timeout_write`) |
| `WORKER_CRASHED`, killed child | `task_id`, `worker_id` | removed in 0.8.0 | `core/src/worker/executor/slots/outcome.rs:212` (`killed_child_write`) |
| `WORKER_CRASHED`, stale | `stale_threshold_ms`, `stale_threshold_seconds`, `worker_pid`, `worker_hostname`, `worker_id`, `started_at`, `detected_at` | removed in 0.8.0 | `core/src/worker/recovery.rs:1450` (`stale_attempt_command`) |
| `UPSTREAM_SKIPPED` | `dependency_index` | `clients/python/horsies/core/workflows/engine.py:2496-2499` | `core/src/workflow_engine/args_merge.rs:330`; SQL `horsies_task_error_envelope` |
| `RESULT_DESERIALIZATION_ERROR`, dependency without a result (2 vectors: an ASCII and a non-ASCII task name in the message) | `dependency_index` | `clients/python/horsies/core/workflows/engine.py:186-193` | `core/src/workflow_engine/args_merge.rs:345` |
| `WORKFLOW_OUTPUT_SKIPPED` (2 vectors: an ASCII and a non-ASCII `output_task_name`) | `workflow_id`, `output_node_id`, `output_task_index`, `output_task_name` | `clients/python/horsies/core/workflows/engine.py:3398-3420` | SQL `horsies_workflow_output_error_data` (`core/baseline/0005_workflow_functions.sql:1309`) |
| `WORKFLOW_ENQUEUE_FAILED`, `args_from` conflict | `workflow_id`, `task_index` | `clients/python/horsies/core/workflows/engine.py:545-551` | `core/src/workflow_engine/args_merge.rs:111` (`node_error`); SQL |
| `SUBWORKFLOW_DEPTH_LIMIT_REACHED` | `workflow_id`, `task_index` | `clients/python/horsies/core/workflows/engine.py:1068-1078` | `core/src/workflow_engine/start.rs:193`; SQL |
| `SUBWORKFLOW_LOAD_FAILED` | `definition_key` | `clients/python/horsies/core/workflows/engine.py:616-619` | `core/src/workflow_engine/start_statement.rs:707` |
| `WORKFLOW_RECOVERY_LIMIT_REACHED` | `workflow_id`, `case`, `task_index`, `failed_passes`, `exception_type` | removed in 0.8.0 | `core/src/workflow_engine/recovery_limit.rs:526`; SQL (`0005_workflow_functions.sql:4099`) |
| `WORKFLOW_START_INCOMPLETE` (error object) | `workflow_id`, `recovery` | removed in 0.8.0 | `core/src/workflow_engine/recovery.rs:1748` |
| `WORKFLOW_EXPIRED` (error object) | `policy`, `older_than_seconds` | `clients/python/horsies/core/workflows/lifecycle.py:845-857` | `core/src/workflow_engine/lifecycle.rs:300` |
| `UNHANDLED_EXCEPTION`, with the exception record | `exception_type` | `clients/python/horsies/core/task_decorator.py:2031-2036` | `core/src/worker/executor/child_host/panic.rs:175` (a Rust panic) |

The SQL writer is `horsies_task_error_envelope`
(`core/baseline/0005_workflow_functions.sql:99`); a DB test
calls it with the code, the message and the data text of each engine vector.
The core's tests call `timeout_write`, `killed_child_write`,
`stale_attempt_command`, `upstream_skipped_error`, `missing_result_error` and
`node_error`; the other core writers build their data with `json!` in the key
order of the table, and the generic test pins the encoding of each vector.
The Rust client writes none of these errors; its test reads each envelope
through `outcome_of_record`.

## Task options (`task_options.json`)

`{"retry_policy": <policy or null>, "good_until": <instant or null>,
"timeout_ms": <ms or null>, "execution": <class>, "cancel_grace_ms": <ms or
null>}`, in this order; the policy holds `max_retries`, `intervals`,
`backoff_strategy`, `jitter`, `max_delay_seconds` (only when set) and
`auto_retry_for`, codes by name. Every task stores the text, nulls included.
A vector's `kind` is `task` (the options of a task), `send` (a send with a
deadline: the task's text with the deadline in `good_until`, in
`isoformat()`) or `node` (a workflow
node: the task's text with the node's deadline merged as `good_until`, in
`isoformat()`). `options` holds the stored facts (a `SHARED_ASYNC` task's
grace as resolved at definition).

Encoders:

- Python: `serialize_task_options` (`clients/python/horsies/core/codec/task_options.py:27`);
  a send's options with its deadline at
  `clients/python/horsies/core/task_decorator.py:2067-2087`; a node's merge at
  `clients/python/horsies/core/workflows/child_build.py:574` and
  `clients/python/horsies/core/workflows/engine.py:464`.
- Core: `TaskOptions::stored_options_text`
  (`core/src/core/task/options.rs`); a send with a deadline,
  `TaskOptions::stored_send_options_text`; `merge_good_until_into_options`
  for a node (`core/src/workflow_engine/mod.rs`).
- Rust client: its send stores `send_options_text`
  (`clients/rust/horsies/src/send/function.rs`), the core's text with the
  deadline in `good_until`, and the deadline also in the row's `good_until`
  column; a node merges its deadline in
  `task_options_text` (`clients/rust/horsies/src/workflow/spec_data.rs`), written
  with the core's `python_isoformat_utc`.

Decoders:

- Python: `stored_retry_policy` (`clients/python/horsies/core/codec/task_options.py:85`),
  `stored_max_retries` (`:112`), `stored_execution` (`:147`).
- Core: `prepare_enqueue_facts` (`core/src/core/history/enqueue.rs:101`,
  the execution class), `stored_max_retries`
  (`core/src/worker/retry.rs:481`), `read_stored_options`
  (`:222`, the timeout and the policy at dispatch).

## Workflow spec data (`workflow_spec_data.json`)

The facts of a built spec that a client sends the core, as JSON text: the
core stores its workflow row and node rows from it. A vector's `document` is
the spec data; its `text` is Python's. Each object's members are in the order
of the generated dataclasses (`kind` first in a tagged union). The vector
`non_ascii_text` holds text outside ASCII in the name, the definition key,
the task name and the node input.

- Python: `workflow_spec_data` and `workflow_spec_json`
  (`clients/python/horsies/core/workflows/spec_data.py`), which call
  `dump_workflow_spec_data` (`clients/python/horsies/_catalog/workflow_spec.py`,
  generated by `clients/python/codegen/src/manifest.rs` with
  `DumpEncoder::StrictJsonIo`): `dumps_json`, the encoder of the other
  stored forms, on the dataclasses with each tuple as a list.
- Core: reads only, `parse_workflow_spec_data`
  (`core/src/workflow_engine/spec_data.rs:282`).
- Rust client: `spec_data_text` (`clients/rust/horsies/src/workflow/spec_data.rs:49`),
  read back by `client_spec` (`:36`).

The Python test builds the dataclasses from each document; the core test
reads each text into its spec and each refused text at its field path
(`core_field_path`); the Rust client test builds every vector but
`members_of_every_kind` with its DSL. That one (`args_from`, workflow context,
encoded options, a sub-workflow node) is read by the core test; the Rust DSL
writes those members with the same keys, in the same order.

## Trace carrier (`trace_carrier.json`)

The map that a client's OpenTelemetry propagator wrote at a send or a
workflow start, stored as one JSON object: the keys in byte order, no
whitespace, each value a JSON string in which only `"` and `\` are escaped.
The keys keep their case (the X-Ray propagator writes and reads
`X-Amzn-Trace-Id`). For every carrier that the check accepts, the text equals
Python's `json.dumps(carrier, sort_keys=True, separators=(",", ":"),
ensure_ascii=False)`: the check leaves only printable ASCII.

Rows: `vectors` (`name`, `carrier`, `trace_id` as hex, `text`) and `refused`
(`name`, `carrier`, `trace_id`, `refusal`: the `CarrierRefusal` variant). A
refused carrier is not an error of the send: the row stores no carrier.

- Core: the one encoder. `check_carrier` checks the bounds and
  `carrier_text` writes the text (`core/src/telemetry/trace_carrier.rs`).
- Python and the Rust client: no encoder. Each hands the core the map and
  the trace id of the producer span, so neither has a test of this file.

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
| options of a default task | task options | the core's encoder wrote no text for an isolated task with no policy, timeout or grace; Python stores the text with nulls | fixed, #1629 |
| `send-deadline-in-options` | task options | a send with a deadline: Python writes it in the options' `good_until`; the Rust client wrote `null` there (the deadline was only in the row's column) | fixed: the client's send writes `TaskOptions::stored_send_options_text` |
| `spec-data-queue-class-order` | workflow spec data | `queue_classes`: Python writes the queues in the order of the nodes; the Rust client wrote them sorted (`BTreeMap`). The core reads both | fixed: the client collects them in a `serde_json::Map`, in the order of the nodes that first name them |
| `spec-data-non-ascii-escapes` | workflow spec data | Python's `dump_workflow_spec_data` kept `json.dumps`' default `ensure_ascii=True` and wrote `\u00dc` for `Ü`; every other Python stored-form encoder (`dumps_json`) and the Rust client write the character. The core reads both | fixed: the generated dumper calls `dumps_json`; pinned by `non_ascii_text` |
| `no-parameter-input` | task input | a task with no parameters: Python stores `{}`, the Rust client stored `null` (its input is `()`) | fixed, the client's send stores `{}` |
| `datetime-milliseconds` | task input, result, task options | chrono writes a fraction of whole milliseconds with 3 digits (`.500Z`); Python writes 6 (`.500000Z`). Microseconds and whole seconds agree. The Rust client's node deadline (`to_rfc3339`) too | fixed: `StoredDateTime` writes Python's text for a user value, and the node deadline uses `python_isoformat_utc`; chrono's own serde form is unchanged |
| not expressible in Rust | task input, result | `big_int`, `decimal`, `bytes`, `int_enum`, a typed map in insertion order: the client depends on no type that writes Python's text | listed per row |
