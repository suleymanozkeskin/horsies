---
title: Migrating to 0.6.0
summary: What an operator does to upgrade from 0.5.x to 0.6.0 - the stop-all upgrade rule, schema v52, new settings and flags, worker behaviour changes, and a checklist of required code changes.
related: [../changelog, ../workers/worker-architecture, ../configuration/recovery-config, ../internals/database-schema, migration-to-0-5-0]
tags: [migration, 0.6.0, schema-v52, breaking-changes, worker, upgrade]
---

## What 0.6.0 changes

For an operator, 0.6.0 changes four things:

- Workers run each task in a child process that the worker owns. There is no process pool.
- SIGTERM drain has a bound: `--drain-timeout` (default 300 seconds).
- A workflow whose recovery raises on every reaper pass now fails after `workflow_recovery_failure_limit` passes.
- Schema v52 adds one empty table.

For a developer, the type contract is strict at every boundary. See topic 13.

Many error types, return values and codes changed. See [Required code changes](#required-code-changes).

## Upgrade rule

Do not run a rolling upgrade from 0.5.x to 0.6.0. Stop every process, upgrade all of them, then start them.

1. Stop every worker, scheduler, producer and `horsies web` process.
2. Upgrade the `horsies` package in all of them.
3. Start the processes.

The reasons:

- A 0.5.5 broker checks the schema version once, at its first use. A 0.5.5 process that already runs does not stop on a v52 database, so it continues to write beside 0.6.0.
- A 0.5.5 process cannot decode a result that 0.6.0 stored with `WORKFLOW_OUTPUT_SKIPPED` or `WORKFLOW_RECOVERY_LIMIT_REACHED`. The read fails with `Unknown built-in task code`.
- 0.6.0 cannot decode an error that a 0.5.5 process stores without an error code.

## Schema step

The schema goes from v51 to v52. The change is one new table, `horsies_workflow_recovery_failures`. The table starts empty. There is no data step.

Any process that uses a `PostgresBroker` applies the migration the first time it calls an entry point that initializes the schema:

- `horsies worker` at start.
- `horsies scheduler` at start.
- The first workflow start (`spec.start()`, `start_workflow()`).
- The first send or schedule (`enqueue_async`).
- The first `get_task_info` or `get_raw_result` read.
- `ensure_schema_initialized()` on the broker.

`horsies web` does not apply the migration. Until another process applies it, `/api/meta` reports `schema_state: MISMATCH` and the web actions are off. A broker built with `run_schema_migrations=False` or `assume_initialized=True` does not apply it either.

The migration is safe to run from many processes at once. A PostgreSQL advisory lock guards the DDL. See [Database Schema](/horsies/internals/database-schema/).

After v52 is applied, a 0.5.5 `horsies worker` or `horsies scheduler` refuses to start on that database, with `database schema v52 is newer than this build supports (v51)`. Decide your rollback before you upgrade.

## New settings and flags

| Name | Default | Where | Page |
|------|---------|-------|------|
| `workflow_recovery_failure_limit` | `25` (3-1,000) | `RecoveryConfig` | [Recovery Config](/horsies/configuration/recovery-config/) |
| `--drain-timeout SECONDS` | `300` (finite, `> 0`) | `horsies worker` | [Graceful Shutdown](/horsies/workers/worker-architecture/#graceful-shutdown) |
| `--child-preload MODE` | `library` (`library`, `app`, `none`) | `horsies worker` | [Preload Parent](/horsies/workers/worker-architecture/#preload-parent) |

At the default `check_interval_ms` and the default limit, the recovery limit is reached 24 intervals after the first pass, which is about 720 seconds. A workflow that is in a failing state when you upgrade reaches the limit and fails. Set a higher `workflow_recovery_failure_limit` to get more time for a fix.

Set `--drain-timeout` below the stop window of your supervisor. See [CLI Reference](/horsies/cli/).

## What an operator sees

- **SIGTERM drain.** The worker stops claims and returns tasks that wait for a child to `PENDING`. It waits up to `--drain-timeout` seconds for running tasks. Then it kills the children that still run a task. Each of these tasks takes the child crash path and gets `WORKER_CRASHED`, or a retry when `WORKER_CRASHED` is in `auto_retry_for`.
- **Child loss.** When a child dies, the worker starts a new child in that slot. Other children continue. A task in `CLAIMED` returns to `PENDING`. A task in `RUNNING` takes the retry policy for `WORKER_CRASHED`.
- **Refusals at worker start.** The worker exits non-zero in these cases:
  - The app locator is `__main__:app`.
  - `--child-preload app` is set and the import leaves a Python thread or a socket.
  - `--max-memory-per-child-mb` is at or below the warmed child baseline.
  - A child fails to start three times, or the operating system refuses to start a process.
  - A child hook fails.
- **New code on a running worker.** A recycled child is a new fork of the preload parent, so it keeps the library code that the parent imported. With `--child-preload=app` it also keeps your app code. To load new code, restart the worker.
- **Process names.** On Linux, a forked child shows the command line of the preload parent in `ps`. A monitor that counts `multiprocessing-fork` processes needs a change.
- **Memory.** `children_memory_mb` counts shared pages once for each child. It reads above real use. Read the container or cgroup total.

See [Worker Architecture](/horsies/workers/worker-architecture/) for the process model.

## Required code changes

Each topic below lists what to change and how to find it. The [changelog](/horsies/changelog/) has the full text of each change.

Run `horsies check` first. The type contract refuses more shapes at definition, so a task or a model can stop registering. See topic 13.

1. **Worker process model.**
   - Rename `ExecutorRestartFailedError` to `ChildStartFailedError`. There is no alias. `WarmupIncompleteError` and `MemoryRecycleUnsupportedError` are removed, and the worker no longer raises them.
   - Set libraries that read a setting at import, such as `PSYCOPG_IMPL`, in the process environment, not in a `.env` file that your app loads.
   - Find uses of `multiprocessing.parent_process()` in task code. It returns `None` in a child.
2. **Workflow recovery limit.**
   - Add a `WORKFLOW_RECOVERY_LIMIT_REACHED` case to each exhaustive `match` on `OperationalErrorCode`.
   - The code string is reserved. An `exception_mapper` or `default_unhandled_error_code` with that value fails `horsies check` with `HRS-212`.
3. **Schedules.**
   - Run `horsies check`. A schedule whose kwargs exceed `payload.reject_bytes` fails at scheduler start with `HRS-205`.
   - A kwargs field with a `default_factory`, or a validator that reads the clock, runs once for each scheduler start and not once for each fire.
4. **Sync calls and the event loop.**
   - Replace each sync call from a coroutine or a Jupyter cell with the `*_async` form. A sync call in a running event loop returns `ASYNC_CONTEXT`.
   - Add an `ASYNC_CONTEXT` case to each exhaustive `match` on `BrokerErrorCode`, `WorkflowStartErrorCode`, `HandleErrorCode` and `RetrievalCode`.
5. **Send, enqueue and broker errors.**
   - `TaskSendError`, `BrokerOperationError` and `BrokerEnqueueError` are unions of frozen variants. Match on the variant before you read `exception`, `task_id` or `payload`. Pyright reports each site.
   - Replace `isinstance(err, TaskSendError)` with a `match` on the variants. Replace `TaskSendError(code=...)` in tests with a variant constructor.
   - Add cases for `LOOP_RUNNER_FAILED`, `INVALID_INPUT` and `LISTENER_DISABLED` where you match on codes.
   - `TaskSendValidationFailed` is not retryable. Do not pass it to the retry methods.
   - Rename the environment variable `TASKLIB_SUPPRESS_SENDS` to `HORSIES_SUPPRESS_SENDS`.
   - A failed broker construction raises `HRS-211` from each send method. A defect in the enqueue write propagates.
6. **Workflow start.**
   - `WorkflowStartError` is a union of frozen variants. Match on the variant before you read `exception`.
   - Add cases for `WORKFLOW_ID_CONFLICT` and `LOOP_RUNNER_FAILED`. Remove `WorkflowStartErrorCode.INTERNAL_FAILED`.
   - `retry_start()` takes `WorkflowStartRetryable` only.
   - A task node with a `priority` outside 1-100 raises `HRS-034`. A naive `good_until` raises `HRS-035`. Run `horsies check`.
   - Add `WORKFLOW_INVALID_PRIORITY`, `WORKFLOW_NAIVE_GOOD_UNTIL`, `WORKFLOW_INVALID_DEFINITION_CONFIG`, `TASK_ASYNC_FUNCTION_NOT_SUPPORTED` and `CHECK_LIVE_ASYNC_CONTEXT` to each exhaustive `match` on `ErrorCode`.
7. **Workflow handle and result reads.**
   - `HandleOperationError` is a union of frozen variants. Remove `HandleErrorCode.INTERNAL_FAILED`. Add cases for `STORED_DATA_INVALID` and `RESUME_RECOVERY_FAILED`.
   - `results()` and `tasks()` return `HandleWorkflowNotFound` for an unknown workflow id.
   - `get()` returns `WORKFLOW_NOT_FOUND` when retention removed the row, and `RESULT_DESERIALIZATION_ERROR` for stored data that does not parse.
   - `get()` returns `WORKFLOW_OUTPUT_SKIPPED` for a completed workflow whose output node is skipped. Code that read this state through `RESULT_DESERIALIZATION_ERROR` must match the new code.
   - Add a `WORKFLOW_OUTPUT_SKIPPED` case to each exhaustive `match` on `OutcomeCode`. The code string is reserved: an `exception_mapper` or `default_unhandled_error_code` with that value fails `horsies check` with `HRS-212`.
8. **Subworkflow node outcome.**
   - A `SubWorkflowNode` is `FAILED` when its child is `COMPLETED` with an err output, or with a stored result that does not parse.
   - A dependent with `allow_failed_deps=False` is now `SKIPPED`. Keep the node out of the success cases of the parent, or set `allow_failed_deps=True` on the dependent.
9. **Codec and JSON boundary.**
   - `encode_value` and `decode_value` raise `ValueValidationError`, not pydantic `ValidationError`. Change `except ValidationError` around these calls.
   - Strings with a lone surrogate, self-referencing values, dict keys without a JSON form, non-UTF-8 `bytes` and nesting above 300 levels are refused. The refusal is a `TaskSendValidationFailed` from a send, `CONFIG_INVALID_SCHEDULE` from a schedule, and `HRS-031` from a workflow spec.
   - `RETURN_TYPE_MISMATCH` no longer stores the returned value in `actual_value`.
10. **`TaskError` contract and retired codes.**
    - `TaskError` requires `error_code` and refuses a key that it does not declare. Find `TaskError(message=...)` calls without a code.
    - A stored error without a code does not decode. No migration rewrites old rows.
    - `ContractCode.PYDANTIC_HYDRATION_ERROR` and `ErrorCode.WORKFLOW_EXCESS_POSITIONAL_ARGS` are removed. Use `RESULT_DESERIALIZATION_ERROR`, `INVALID_JSON_PAYLOAD` or `RETURN_TYPE_MISMATCH`, and `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED`. `ErrorCode.WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING` is removed too: no code raised it, so remove its case.
11. **Monitoring, task actions and rerun.**
    - `MonitoringQueryError` and `TaskActionError` are unions of frozen variants. Add cases for `HISTORY_CONTRACT_VIOLATION` and `STORED_DATA_INVALID`.
    - `get_task_detail` returns `TaskDetailNotFound`, not `Ok(None)`, for an absent task. Replace `detail is None` checks.
    - `list_tasks`, `task_breakdown` and `list_workflow_runs` take `TaskPage`, `BreakdownLimit` and `WorkflowRunsLimit`. Build each with `parse`.
    - `RerunSourceAbsent.predates_retained_floor` is now `reason`, a `TaskAbsenceReason`. Add a `RerunSourceCorrupt` case to each exhaustive `match` on `RerunOutcome`.
12. **Definition-time refusals, config and tooling.**
    - `CustomQueueConfig` refuses an empty queue name, a name with whitespace at the start or the end, and a name over 100 characters (`HRS-200`).
    - Every config model refuses unknown keys. Remove misspelled keys.
    - `@app.task()` refuses `async def` functions (`HRS-106`) and an empty task name (`HRS-102`).
    - A `WorkflowDefinition` sets options with `config = WorkflowDefinitionConfig(...)`. A nested `class Meta` raises `HRS-033`.
    - The agent skill files are in `horsies/.agents/skills/horsies/`. Install them with `horsies skills install --target DIR`.
    - `horsies get-docs` downloads the docs of the release tag. Pass `--ref main` for the `main` branch.
13. **Strict type contract.**
    - Run `horsies check`. A task or a model that uses a refused shape no longer registers, so a worker does not start until it changes. The list is in [Shapes Refused at Definition](/horsies/internals/serialization/#shapes-refused-at-definition): `Field(exclude=True)`, `exclude_if`, `extra='allow'`, `PlainValidator`, `WrapValidator`, `SkipValidation`, a schema hook, `Json[T]`, an alias whose written key does not read back, `Literal` with a plain `Enum`, a default that does not hold its type (`n: int = None`).
    - A conversion is refused at `.send()`, in a schedule and on a return: `"7"` for an `int`, a str for a `datetime`, `Enum`, `UUID` or `Decimal`. Pass the value of the declared type. Schedule kwargs hold Python values, so write the real value there too.
    - `.send()` refuses a missing required argument and an unknown key at any depth. A subclass with fields that the declared type lacks is refused.
    - Each `build_with` that a `SubWorkflowNode` targets follows the task rules: `Any` and bare `dict` parameters are refused, and its hints must resolve. `WorkflowDefinition[Any]` cannot feed `args_from` or be an output.
    - A return that is not a `TaskResult`, including `None`, gives `RETURN_TYPE_MISMATCH`, not `UNHANDLED_EXCEPTION` or `TASK_EXCEPTION`. A policy with `auto_retry_for=['TASK_EXCEPTION']` no longer retries a forgotten `return`.
    - Each key of `kwargs` and `args_from` on a node binds to one named, typed parameter. A key that reached `**params` of a `build_with` is refused when the spec is built. Name the parameter, or declare `params: dict[str, JsonValue]`. A row from an earlier version with such a key fails when the parent reaches the node.
    - `TaskError.data` must be JSON-native. A `Decimal`, a `datetime` or a tuple becomes `{'data_refused': <cause>}`. Convert with `str()`, `.isoformat()` or `list()`.
    - Stored rows are read strictly. A result that an earlier version stored with a value outside a return constraint, with a serializer that changes the JSON type, or deeper than 200 levels, reads as `RESULT_DESERIALIZATION_ERROR` until retention removes it (30 days by default). A queued task of that shape fails with `WORKER_SERIALIZATION_ERROR`. Before the upgrade, drain the queues of a task with such a type.
