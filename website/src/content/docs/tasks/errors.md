---
title: Errors Reference
section: shared
summary: The error codes of horsies - the codes a task can end with, the codes of the client calls, and the HRS codes of config and definition errors.
related: [error-handling, error-types, ../rust/tasks/errors, ../concepts/result-handling]
tags: [errors, BuiltInTaskCode, ErrorCode, reference]
---

# Errors Reference

Horsies has two kinds of error codes: the **task error codes** that a task ends with, and the **HRS codes** of config, task definition and workflow definition errors, which stop an app before it runs. The client calls (send, workflow start, workflow handle) have their own code catalogs. The two clients use the same codes and names.

The API of the error types is on the client pages: [Error Types](/horsies/tasks/error-types/) (Python) and [Task Errors](/horsies/rust/tasks/errors/) (Rust).

## Task Error Codes

A task ends with its own code or with a library code. A task that ends `FAILED` stores its error code with its record, so a task info read returns it. Each execution attempt is recorded with its own error code, message and outcome. See [Retrieving Results](/horsies/tasks/retrieving-results/#task-metadata-and-attempt-history).

### BuiltInTaskCode (4-Family Split)

All library-defined error codes are grouped into four families. The umbrella type alias `BuiltInTaskCode` is the union of all four enums. User code should handle these explicitly.

#### OperationalErrorCode

Errors from task execution, broker, and worker internals.

<!-- members: OperationalErrorCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `UNHANDLED_EXCEPTION` | Uncaught exception in task code, wrapped to `UNHANDLED_EXCEPTION` by library  | Yes, if in `auto_retry_for` |
| `TASK_EXCEPTION` | The worker caught an exception outside the task wrapper, or a validator raised on a default | Yes, if in `auto_retry_for` |
| `WORKER_CRASHED` | Worker process died during execution | Yes, if in `auto_retry_for` |
| `BROKER_ERROR` | Database or broker failure during operation | No |
| `WORKER_RESOLUTION_ERROR` | Failed to resolve task by name | No |
| `WORKER_SERIALIZATION_ERROR` | Failed to serialize/deserialize task data: the stored arguments, a stored timeout (or the stored cancel grace of a `SHARED_ASYNC` row) that the worker cannot use, or a stored `retry_policy` that does not validate as `RetryPolicy` (this failure does not retry). No user code ran. A stored-data cause does not clear on a retry | Yes, if in `auto_retry_for` |
| `RESULT_DESERIALIZATION_ERROR` | A stored result or a stored workflow error is corrupt or could not be deserialized | No |
| `WORKFLOW_ENQUEUE_FAILED` | Workflow node failed after it went to ENQUEUED, while its task was built or enqueued | No |
| `SUBWORKFLOW_LOAD_FAILED` | Subworkflow definition could not be loaded | No |
| `WORKFLOW_RECOVERY_LIMIT_REACHED` | Workflow recovery raised a persistent error in `workflow_recovery_failure_limit` reaper passes, so the workflow failed closed | No |
| `SUBWORKFLOW_DEPTH_LIMIT_REACHED` | The engine did not start a child workflow, because its depth would pass 32 | No |
| `TASK_BLOCKED_LOOP` | The task held the event loop of its shared async child for the block limit (5 s) or longer, before its `timeout_ms`. The worker killed the child. The retry runs in an isolated child. See [Shared Async Tasks](/horsies/workers/shared-async-tasks/#a-blocked-event-loop) | Yes, if in `auto_retry_for` |
| `NEIGHBOUR_BLOCKED_LOOP` | Another task held the event loop of this task's shared async child for the block limit (5 s) or longer, before this task's `timeout_ms`. The worker killed the child, with this task in it. See [Shared Async Tasks](/horsies/workers/shared-async-tasks/#a-blocked-event-loop) | Yes, if in `auto_retry_for` |
| `WORKFLOW_START_INCOMPLETE` | A RUNNING workflow has no node rows: its start did not finish. Recovery fails the workflow with this code | No |

#### ContractCode

Errors from type/schema validation and structural contracts.

<!-- members: ContractCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `RETURN_TYPE_MISMATCH` | The task returned a value that does not fit its declared type, or something that is not a `TaskResult`, including `None` | Yes, if in `auto_retry_for` |
| `NO_TYPE_AVAILABLE` | A successful result could not be decoded because the caller does not have the task's or workflow's declared success type locally registered | No |
| `WORKFLOW_CTX_MISSING_ID` | Workflow context is missing required ID | No |

A return that is not a `TaskResult` has `variant` `'not_a_task_result'`, an empty `validation_errors` and a count of 0. It no longer goes through `exception_mapper` or `default_unhandled_error_code`, so it is not retried unless a policy lists `RETURN_TYPE_MISMATCH`.

`RETURN_TYPE_MISMATCH` does not store the returned value. `message` names the variant, the actual type, the expected type and the failing fields. `data` holds `variant`, `expected_type`, `actual_type`, `validation_errors` (at most 10 entries, each with `loc`, `msg` and `type`) and `validation_error_count`. `exception` is `None`. A validator whose error text holds a lone surrogate gives one `validation_errors` entry of type `lone_surrogate`, because pydantic cannot build that text.

#### RetrievalCode

Errors from `handle.get()` / `get_async()`. These indicate issues retrieving the result, not task execution failures.

<!-- members: RetrievalCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `WAIT_TIMEOUT` | `get()` timed out; task may still be running | No |
| `TASK_NOT_FOUND` | Task ID doesn't exist in database | No |
| `WORKFLOW_NOT_FOUND` | Workflow ID doesn't exist in database, or retention removed the workflow after `get()` read its terminal status | No |
| `RESULT_NOT_AVAILABLE` | Result cache was never set | No |
| `RESULT_NOT_READY` | Result not yet available; task is still running | No |
| `ASYNC_CONTEXT` | Sync `get()` / `result_for()` was called on a thread that runs an event loop; nothing ran; not cached. Call `await handle.get_async()` / `result_for_async()` | No |
| `INVALID_INPUT` | An argument of `get()` / `get_async()` was refused before any read, for example a `timeout_ms` that is negative, not an int, or above `MAX_TASK_TIMEOUT_MS` (4,294,967,295 ms); not cached | No |
| `WAIT_INSIDE_TASK` | A result wait was called inside a worker child, where task bodies run; it waited for nothing; not cached. Compose the tasks with a workflow: see [Waiting for a Result Inside a Task](/horsies/concepts/result-handling/#waiting-for-a-result-inside-a-task) | No |

#### OutcomeCode

Terminal outcome codes for tasks and workflows.

<!-- members: OutcomeCode -->
| Code | Description | Auto-Retry? |
| ---- | ----------- | ----------- |
| `TASK_CANCELLED` | Task was cancelled before completion | No |
| `TASK_EXPIRED` | Task's `good_until` deadline passed before execution started (PENDING or CLAIMED) | No |
| `TASK_TIMEOUT` | Task exceeded its `timeout_ms` execution limit: the worker killed its child process, or, for a `SHARED_ASYNC` task on a shared async child, cancelled it and ended its work. `data` has `task_id` and `timeout_ms`; a `SHARED_ASYNC` task on a shared async child also has `cancel_end`: `RETURNED_AFTER_SIGNAL`, `DROPPED_AFTER_GRACE` or `KILLED_WITH_CHILD` ([The Timeout](/horsies/workers/shared-async-tasks/#the-timeout)) | Yes, if in `auto_retry_for` |
| `WORKFLOW_PAUSED` | Workflow was paused | No |
| `WORKFLOW_EXPIRED` | A paused workflow passed `paused_workflow_auto_cancel_after` and expired; `data` holds the policy name and the age | No |
| `WORKFLOW_FAILED` | Workflow failed | No |
| `WORKFLOW_CANCELLED` | Workflow was cancelled | No |
| `UPSTREAM_SKIPPED` | Upstream task in workflow was skipped | No |
| `SUBWORKFLOW_FAILED` | Subworkflow failed | No |
| `WORKFLOW_SUCCESS_CASE_NOT_MET` | Workflow success condition was not satisfied | No |
| `WORKFLOW_OUTPUT_SKIPPED` | A `COMPLETED` workflow has no output: its output node, or a terminal node of an outputless workflow, is SKIPPED, and a success case does not require that node; `data` names the workflow and the node | No |

#### Workflow Errors

Workflow-specific error codes are distributed across the four families above:

- **OperationalErrorCode**: `WORKFLOW_ENQUEUE_FAILED`, `SUBWORKFLOW_LOAD_FAILED`, `WORKFLOW_RECOVERY_LIMIT_REACHED`, `SUBWORKFLOW_DEPTH_LIMIT_REACHED`
- **ContractCode**: `WORKFLOW_CTX_MISSING_ID`
- **RetrievalCode**: `WORKFLOW_NOT_FOUND`
- **OutcomeCode**: `WORKFLOW_PAUSED`, `WORKFLOW_EXPIRED`, `WORKFLOW_FAILED`, `WORKFLOW_CANCELLED`, `UPSTREAM_SKIPPED`, `SUBWORKFLOW_FAILED`, `WORKFLOW_SUCCESS_CASE_NOT_MET`, `WORKFLOW_OUTPUT_SKIPPED`

### Reserved Codes

Every code of the four families is reserved: a task's own code must not have the name of a library code. `WORKFLOW_STOPPED` is reserved too; it is not a task code. A client refuses a reserved name as a task's own code, as an `auto_retry_for` entry of a user code, and in the config. A library code is stored in a tagged form, `{"__builtin_task_code__": "BROKER_ERROR"}`, and a task's own code as plain text, so a stored code reads back as the same kind. The client rules: [Python](/horsies/tasks/error-types/#reserved-built-in-code-enforcement), [Rust](/horsies/rust/tasks/errors/#the-code-type).

### Declared Error Codes

A task can declare its own error codes. The declared codes close the set of values of the `error_code` label of the task's worker metrics:

- each built-in code, by its value (`BROKER_ERROR`);
- each code that the task declares (`CARD_DECLINED`);
- `undeclared`, for every other user code of the task.

A declared set has no size limit. Only the label folds a code into `undeclared`: the task's error, its attempt record, its events, traces and logs keep the exact code. `horsies check` lists the declared codes of each task that declares some.

Each client refuses a bad set when the task is defined:

| Refusal | Code |
| ------- | ---- |
| A built-in code or a reserved name (`WORKFLOW_STOPPED`) | `HRS-212` |
| An empty code, a code that is not `UPPER_SNAKE_CASE`, or an exception class name (`TimeoutError`) | `HRS-112` |
| A code given twice | `HRS-113` |

Declaring is optional. The client APIs: [Python](/horsies/tasks/defining-tasks/#declare-error-codes) (`error_codes=` of `@app.task`, plus the values of the task's and the app's exception mappers, with no second declaration), [Rust](/horsies/rust/tasks/errors/#the-code-type) (`UserErrorCode::CODES` of the task's code type, with no other user work).

## Call Error Codes

The codes of the client calls. The error types that carry them are on the client pages.

### Send Error Codes

<!-- members: TaskSendErrorCode -->
| Code | Description | Retry methods |
| ---- | ----------- | ------------- |
| `SEND_SUPPRESSED` | Send suppressed during worker import/discovery to prevent side effects | Refused |
| `ASYNC_CONTEXT` | Sync send/schedule (or sync retry variant) called inside a running event loop -- the blocking enqueue would stall the loop; `retry_*_async(err)` completes the same dispatch (or call the `*_async` entry point with the original args) | Accepted |
| `VALIDATION_FAILED` | Argument serialization or validation failed before enqueue, the broker refused the input before the write (`INVALID_INPUT`, `INVALID_TASK_OPTIONS`), or a retry method refused the payload (another task, or a schedule payload sent to a send retry) | Refused |
| `ENQUEUE_FAILED` | Broker/database failure during enqueue, or the core of the process could not run the send ([Python: Calls That Run on the Core](/horsies/tasks/error-types/#calls-that-run-on-the-core)); the row may or may not exist | Accepted |
| `PAYLOAD_MISMATCH` | The task id exists, and the command or the idempotency key differs from the original send -- the payload was altered between send and retry | Refused |
| `PAYLOAD_TOO_LARGE` | The serialized input exceeds `payload.reject_bytes` bytes of UTF-8 text; nothing was enqueued | Refused |
| `IDEMPOTENCY_KEY_CONFLICT` | The idempotency key is reserved by another task with a different command; nothing was enqueued. `owner_task_id` names the owner task. A retry after the reservation ends is accepted | Accepted |

### Workflow Start Error Codes

<!-- members: WorkflowStartErrorCode -->
| Code | Retryable | When |
|---|---|---|
| `BROKER_NOT_CONFIGURED` | No | `spec.start()` called without broker |
| `VALIDATION_FAILED` | No | A start check (`WorkflowValidationError`) refused the start, or a retry of another workflow's error. Any other exception in the start checks is a defect and propagates. In the start transaction, an exception that is not a database or driver error is a defect and propagates, also a `SerializationError` |
| `WORKFLOW_ID_CONFLICT` | No | The workflow id exists for another workflow name; nothing was written. Use another id |
| `ENQUEUE_FAILED` | Maybe | Schema init or the start transaction failed with a database or driver error. Any other exception in the start transaction propagates |
| `ASYNC_CONTEXT` | No | Sync `spec.start()` / `retry_start()` was called on a thread that runs an event loop; nothing was written. Call `await spec.retry_start_async(err)` to complete the start with the error's id |

### Workflow Handle Error Codes

<!-- members: HandleErrorCode -->
| Code | Variant | Other fields | Retryable | When |
|---|---|---|---|---|
| `WORKFLOW_NOT_FOUND` | `HandleWorkflowNotFound` | – | No | No workflow has this id; nothing changed |
| `DB_OPERATION_FAILED` | `HandleDbOperationFailed` | `exception: BaseException` | Driver classification of the cause | A database or driver error. A read changed nothing; `cancel()`, `pause()` and `resume()` before its commit rolled back, or the commit outcome is unknown |
| `STORED_DATA_INVALID` | `HandleStoredDataInvalid` | `exception: BaseException` | No | A stored workflow status that is not a `WorkflowStatus` (`status()`, `cancel()`), or a subworkflow summary that is not valid (`tasks()`); nothing changed, and a repeat returns the same error |
| `RESUME_RECOVERY_FAILED` | `HandleResumeRecoveryFailed` | `exception: BaseException` | No | `resume()` committed, and the recovery pass for the workflow tree, which runs after the commit, failed with a database error. The workflow is `RUNNING`; the worker reaper runs the same pass over all workflows. A repeated `resume()` returns `Ok(False)` |
| `ASYNC_CONTEXT` | `HandleAsyncContext` | – | No | A sync handle method or `pause_workflow_sync` / `resume_workflow_sync` called on a thread that runs an event loop; nothing ran. Call the `*_async` form (sync `get()` / `result_for()` return `RetrievalCode.ASYNC_CONTEXT` instead) |

`get()` folds a status read failure into its `TaskResult`: `WORKFLOW_NOT_FOUND` becomes `RetrievalCode.WORKFLOW_NOT_FOUND`, and `DB_OPERATION_FAILED` and `STORED_DATA_INVALID` become `BROKER_ERROR`.

## Config Error Codes (HorsiesError)

A config, task definition or workflow definition that breaks a rule stops the app before it runs: the worker or the scheduler does not start and takes no task. Each refusal carries an `ErrorCode` (`HRS-xxx`), its category, a message, notes and help text.

### PgBouncer Configuration Errors

These startup and operator messages usually mean the deployment is using
transaction-pooled PgBouncer without a direct/session-capable URL.

```text
session_database_url required when pgbouncer_transaction_mode=True
```

Set `PostgresConfig.session_database_url` to the direct/session-capable
Postgres URL. Managed providers, such as PlanetScale Postgres, may expose
separate pooled and direct connection strings; verify the current ports and URLs
in your provider dashboard or docs.

```text
Postgres LISTEN failed. If database_url points to PgBouncer transaction pooling
```

The worker listener could not establish a persistent PostgreSQL session. Workers
do not support PgBouncer-only transaction-pool deployment.

```text
LISTEN unavailable; falling back to polling
```

Producer-side result waits can poll when notifications are unavailable. This is
not the supported worker dispatch mode.

```text
session_database_url appears to be transaction-pooled; LISTEN notification was not delivered
```

`horsies check --live` accepted the session URL at the SQL level, but a
`LISTEN` + `NOTIFY` behavior probe did not deliver a notification. Use a direct
Postgres URL or a session-pooling URL for `session_database_url`.

See [PgBouncer Troubleshooting](/horsies/troubleshooting/pgbouncer/) for the full
deployment pattern.

### ErrorCode Categories

| Range | Category | Description |
| ----- | -------- | ----------- |
| HRS-001-HRS-099 | Workflow Validation | Invalid workflow specification |
| HRS-100-HRS-199 | Task Definition | Invalid task decorator usage |
| HRS-200-HRS-299 | Configuration | Invalid app/broker configuration |
| HRS-300-HRS-399 | Registry | Task registration failures |

### Workflow Validation (HRS-001-HRS-099)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-001 | `WORKFLOW_NO_NAME` | Workflow has no name |
| HRS-002 | `WORKFLOW_NO_NODES` | Workflow has no nodes |
| HRS-003 | `WORKFLOW_INVALID_NODE_ID` | Invalid node ID reference, or `key()` on a node with no `node_id` |
| HRS-004 | `WORKFLOW_DUPLICATE_NODE_ID` | Duplicate node ID |
| HRS-005 | `WORKFLOW_NO_ROOT_TASKS` | No root tasks in workflow |
| HRS-006 | `WORKFLOW_INVALID_DEPENDENCY` | Invalid dependency reference |
| HRS-007 | `WORKFLOW_CYCLE_DETECTED` | Cycle detected in workflow DAG |
| HRS-008 | `WORKFLOW_INVALID_ARGS_FROM` | Invalid args_from reference |
| HRS-009 | `WORKFLOW_INVALID_CTX_FROM` | Invalid ctx_from reference |
| HRS-010 | `WORKFLOW_CTX_PARAM_MISSING` | Context parameter missing |
| HRS-011 | `WORKFLOW_INVALID_OUTPUT` | Invalid output specification |
| HRS-012 | `WORKFLOW_INVALID_SUCCESS_POLICY` | Invalid success policy |
| HRS-013 | `WORKFLOW_INVALID_JOIN` | Invalid join configuration |
| HRS-014 | `WORKFLOW_UNRESOLVED_QUEUE` | Queue name not resolved |
| HRS-015 | `WORKFLOW_UNRESOLVED_PRIORITY` | Priority not resolved |
| HRS-016 | `WORKFLOW_NO_DEFINITION_KEY` | Workflow definition/spec missing `definition_key` |
| HRS-017 | `WORKFLOW_DUPLICATE_DEFINITION_KEY` | Two definitions share the same `definition_key` |
| HRS-018 | `WORKFLOW_SUBWORKFLOW_APP_MISSING` | Subworkflow app reference missing |
| HRS-019 | `WORKFLOW_INVALID_KWARG_KEY` | Unknown kwargs or args_from key for callable |
| HRS-020 | `WORKFLOW_MISSING_REQUIRED_PARAMS` | Missing required parameters for task or subworkflow |
| HRS-021 | `WORKFLOW_KWARGS_ARGS_FROM_OVERLAP` | kwargs and args_from share one or more keys |
| HRS-022 | `WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH` | Subworkflow params passed but build_with is not overridden |
| HRS-024 | `WORKFLOW_ARGS_FROM_TYPE_MISMATCH` | args_from source result type doesn't match target parameter |
| HRS-025 | `WORKFLOW_OUTPUT_TYPE_MISMATCH` | Output node type doesn't match WorkflowSpec generic |
| HRS-026 | `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` | Positional args are not supported for workflow nodes |
| HRS-027 | `WORKFLOW_CHECK_CASES_REQUIRED` | Parameterized workflow builder missing test cases |
| HRS-028 | `WORKFLOW_CHECK_CASE_INVALID` | Workflow builder test case is invalid |
| HRS-029 | `WORKFLOW_CHECK_BUILDER_EXCEPTION` | Workflow builder raised an exception or returned non-WorkflowSpec, or `build_with()` returned a spec that this call did not build |
| HRS-030 | `WORKFLOW_CHECK_UNDECORATED_BUILDER` | Function returns WorkflowSpec but lacks @app.workflow_builder |
| HRS-031 | `WORKFLOW_KWARGS_NOT_SERIALIZABLE` | kwargs value fails JSON serialization, also a lone surrogate in a string or a dict key |
| HRS-032 | `WORKFLOW_INVALID_ID` | supplied `workflow_id` is not a UUID |
| HRS-033 | `WORKFLOW_INVALID_DEFINITION_CONFIG` | A `WorkflowDefinition` class defines `Meta`, or sets `config` to a value that is not a `WorkflowDefinitionConfig`; raised when the class is created |
| HRS-034 | `WORKFLOW_INVALID_PRIORITY` | A `TaskNode` sets a `priority` outside 1–100; raised when the spec is built |
| HRS-035 | `WORKFLOW_NAIVE_GOOD_UNTIL` | A `TaskNode` sets a naive `good_until` (no `tzinfo`); raised when the spec is built |
| HRS-036 | `WORKFLOW_SUBWORKFLOW_DEPTH_LIMIT` | `horsies check`: a subworkflow nesting with no `waits_for` on its path would start a child workflow deeper than 32 |
| HRS-037 | `WORKFLOW_SUBWORKFLOW_SELF_NESTING` | `horsies check`: a subworkflow includes itself with the same kwargs and with no `waits_for` between, so its nesting never ends |
| HRS-038 | `WORKFLOW_TOO_MANY_NODES` | A `WorkflowSpec` has more than 10,000 nodes; raised when the spec is built |

### Task Definition (HRS-100-HRS-199)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-100 | `TASK_NO_RETURN_TYPE` | Task function missing return type |
| HRS-101 | `TASK_INVALID_RETURN_TYPE` | Return type is not TaskResult |
| HRS-102 | `TASK_INVALID_OPTIONS` | Invalid task options |
| HRS-103 | `TASK_INVALID_QUEUE` | Invalid queue specification |
| HRS-104 | `TASK_PREDECORATED_NOT_SUPPORTED` | Task function is pre-decorated: it has a `__wrapped__` chain. Apply `@app.task()` to the raw function |
| HRS-105 | `TASK_STRICT_SIGNATURE_VIOLATION` | Task signature fails the strict check: a parameter that is not keyword-only (`*args`, `**kwargs`, positional), a type that is not allowed (for example `Any`, a bare container, `set`, `bytes`), or a return type that is not `TaskResult[OkT, TaskError]` |
| HRS-107 | `TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF` | `execution=Execution.SHARED_ASYNC` on a function defined with `def`. Define it with `async def`, or use `SHARED_THREAD` |
| HRS-108 | `TASK_SHARED_THREAD_REQUIRES_DEF` | `execution=Execution.SHARED_THREAD` on a function defined with `async def`. Define it with `def`, or use `SHARED_ASYNC` |
| HRS-109 | `TASK_SHARED_EXECUTION_REQUIRES_TIMEOUT` | `SHARED_ASYNC` or `SHARED_THREAD` without `timeout_ms` |
| HRS-110 | `TASK_CANCEL_GRACE_REQUIRES_SHARED_ASYNC` | `cancel_grace_ms` with an execution class other than `SHARED_ASYNC` |
| HRS-111 | `TASK_CANCEL_GRACE_OUT_OF_RANGE` | `cancel_grace_ms` outside `MIN_CANCEL_GRACE_MS` (100) to `MAX_CANCEL_GRACE_MS` (10,000) |
| HRS-112 | `TASK_INVALID_ERROR_CODE` | A declared error code is empty, not `UPPER_SNAKE_CASE`, or an exception class name. See [Declared Error Codes](#declared-error-codes) |
| HRS-113 | `TASK_DUPLICATE_ERROR_CODE` | A task declares one error code more than once |

### Configuration (HRS-200-HRS-299)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-200 | `CONFIG_INVALID_QUEUE_MODE` | Invalid queue mode or custom queue list: `custom_queues` in `DEFAULT` mode, missing or duplicate `custom_queues` in `CUSTOM` mode, or a `CustomQueueConfig.name` that is empty, has whitespace at the start or the end, is longer than 52 bytes of UTF-8 text (`QUEUE_NAME_MAX_BYTES`), or has no UTF-8 text |
| HRS-201 | `CONFIG_INVALID_CLUSTER_CAP` | Invalid cluster capacity |
| HRS-202 | `CONFIG_INVALID_PREFETCH` | Invalid prefetch setting |
| HRS-203 | `BROKER_INVALID_URL` | Invalid broker URL |
| HRS-204 | `CONFIG_INVALID_RECOVERY` | Invalid recovery configuration |
| HRS-205 | `CONFIG_INVALID_SCHEDULE` | Invalid schedule configuration |
| HRS-206 | `CLI_INVALID_ARGS` | Invalid CLI arguments |
| HRS-207 | `WORKER_INVALID_LOCATOR` | Invalid worker locator |
| HRS-208 | `CONFIG_INVALID_RESILIENCE` | Invalid resilience configuration |
| HRS-209 | `CONFIG_INVALID_EXCEPTION_MAPPER` | Invalid exception mapper |
| HRS-210 | `MODULE_EXEC_ERROR` | Module raised an error during import |
| HRS-211 | `BROKER_INIT_FAILED` | Broker failed to initialize |
| HRS-212 | `CHECK_RESERVED_CODE_COLLISION` | User config value collides with a reserved built-in error code, or a task declares a built-in code or a reserved name as its own error code |
| HRS-213 | `CONFIG_INVALID_BROKER_POOL` | A `PostgresConfig` pool value outside its range: `pool_size` outside 1 to 10,000, `max_overflow` outside 0 to 10,000, `pool_timeout` outside 0 to 4,294,967,295 s, or `pool_recycle` that is not -1 or 0 to 4,294,967,295 s. `pool_size=0` and `max_overflow=-1` (no limit in SQLAlchemy) are refused, and the help says to give a number. Also a `PostgresConfig` that sets a pool field removed in 0.7.0: `worker_pool_size` or `worker_max_overflow` (the help names `WorkerProfile.pool`), or `worker_child_pool_min_size`, `worker_child_pool_max_size` or `worker_child_pool_check` (the help names the `pool` field of each child kind that replaces it) |
| HRS-214 | `CONFIG_INVALID_CHILD_HOOK` | `on_child_process_start` received a value that is not callable, or an async function |
| HRS-215 | `CONFIG_INVALID_KEEPALIVE` | A TCP value outside its range: while `tcp_keepalives` is on, `tcp_keepalives_idle` or `tcp_keepalives_interval` outside 1 to 32,767 s, or `tcp_keepalives_count` outside 1 to 127; or a `tcp_user_timeout_ms` that is not 0 or 1 to 2,147,483,647 ms. The first value out of range is refused, in field order |
| HRS-216 | `CONFIG_INVALID_RETENTION` | Invalid retention configuration |
| HRS-217 | `CHECK_LIVE_ASYNC_CONTEXT` | `check(live=True)` was called on a thread that runs an event loop; the live check did not run |
| HRS-218 | `CONFIG_INVALID_WORKER_PROFILE` | A worker profile in `AppConfig.worker_profiles` is not valid: a name a `--profile` argument cannot carry, a profile with no children, a claim limit above the capacity of the worker, a child pool whose `min_size` is above its resolved `max_size`, or a `PoolSize` below 3 (`PARENT_POOL_MIN_CONNECTIONS`) for the pool of the worker process (`ParentPool.max_size`). The message names the profile and the field path |
| HRS-219 | `CHECK_WORKER_CONNECTIONS` | `check(live=True)`: one worker of a profile opens more Postgres connections itself than the server has usable slots (the error also states what task code that sends adds: the `task_send_pool` of each child kind), or the server limit could not be read |
| HRS-220 | `CONFIG_OTEL_API_MISSING` | `AppConfig(tracing=OtelTracing())` and the OpenTelemetry API is not installed. The help names the extra: `pip install 'horsies[otel]'` |
| HRS-221 | `CONFIG_INVALID_TRACE_LINK` | A `TraceLinkTemplate` of `MonitoringUIConfig` that the core refuses: not an `http` or `https` URL with a host, not exactly one `{trace_id}`, a brace outside it, a byte outside printable ASCII, or more than 2,048 bytes. The note names the rule |

### Registry (HRS-300-HRS-399)

<!-- members: ErrorCode part -->
| Code | Name | Description |
| ---- | ---- | ----------- |
| HRS-300 | `TASK_NOT_REGISTERED` | Task not found in registry |
| HRS-301 | `TASK_DUPLICATE_NAME` | Duplicate task name |
