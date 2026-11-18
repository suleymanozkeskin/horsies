---
title: Migrating to 0.8.0
section: site
summary: What to change to upgrade from 0.7.x to 0.8.0, in upgrade order - the database URL without a driver, the connection settings that the core reads, the native extension, a new database in place of the 0.7 database, task ids as uuid.UUID, the class of a database failure, the errors after a fork or a core panic, removed and changed members, new config bounds, web API clients, and the worker liveness lock.
related: [../changelog, ../configuration/broker-config, ../configuration/broker-connections, ../tasks/error-types, ../workers/heartbeats-recovery, ../rust/migrations/switching-to-0-8-0, migration-to-0-7-0]
tags: [migration, 0.8.0, breaking-changes, database-url, native-extension, schema, liveness-lock, fork]
---

0.8.0 runs the Python client on a core that is written in Rust, with a new baseline schema. The core does not upgrade a 0.7 database. Drain the 0.7 deployment, then start 0.8.0 on a new database. This page lists each change that a 0.7.x deployment must make. The full list of changes is in the [Changelog](/horsies/changelog/).

## Upgrade order

1. Remove `+psycopg` from `database_url` and `session_database_url`. See [The database URL names no driver](#the-database-url-names-no-driver).
2. Remove the URL parameters and the `PG*` environment variables that the core refuses. See [Connection settings that the core reads](#connection-settings-that-the-core-reads).
3. Install horsies 0.8.0. See [The native extension](#the-native-extension).
4. Change the code: [task ids](#task-ids-are-uuids), [the class of a database failure](#the-class-of-a-database-failure), [forks](#errors-after-a-fork-or-a-core-panic), [removed and changed members](#removed-and-changed-members), [config values](#config-values-with-new-bounds) and [web API clients](#web-api-clients).
5. Create a new PostgreSQL database for 0.8.0.
6. Run `horsies check <app>`. Run `horsies check <app> --live` against the new database, in the environment of each process.
7. Update the supervisor and the connection budget for the [worker liveness lock](#the-worker-liveness-lock).
8. Drain the 0.7 deployment, then start 0.8.0 on the new database. See [Start on a new database](#start-on-a-new-database).

## The database URL names no driver

0.7 accepted only the scheme `postgresql+psycopg://`. 0.8.0 accepts `postgresql://` and `postgres://`. Remove `+psycopg` from `PostgresConfig.database_url` and from `PostgresConfig.session_database_url`.

0.7:

<!-- check: skip elided -- the 0.7 URLs, which 0.8.0 refuses -->
```python
from pydantic import SecretStr

from horsies import PostgresConfig

broker_config = PostgresConfig(
    database_url=SecretStr('postgresql+psycopg://user:pass@pooler:6432/mydb'),
    session_database_url=SecretStr('postgresql+psycopg://user:pass@db:5432/mydb'),
    pgbouncer_transaction_mode=True,
)
```

0.8.0:

```python
from pydantic import SecretStr

from horsies import PostgresConfig

broker_config = PostgresConfig(
    database_url=SecretStr('postgresql://user:pass@pooler:6432/mydb'),
    session_database_url=SecretStr('postgresql://user:pass@db:5432/mydb'),
    pgbouncer_transaction_mode=True,
)
```

A URL that names a driver fails when `PostgresConfig` is built. It raises `ConfigurationError` with `HRS-203` (`BROKER_INVALID_URL`). The error names the scheme, not the rest of the URL. Its help names the fix: `replace 'postgresql+psycopg://' with 'postgresql://'`. A URL with another scheme, or with no scheme, also gets `HRS-203`.

Change the URL where the deployment keeps it: in the code, in an environment variable or in a secret store. `horsies web --database-url` and `--session-database-url` take the same schemes.

The URL of your own SQLAlchemy engine does not change. An engine that you give to [`rerun_task`](/horsies/operations/rerun-in-your-transaction/) still names its driver, for example `postgresql+psycopg://`. Only the two URLs of `PostgresConfig` change.

## Connection settings that the core reads

The core does not use libpq. It reads only these settings: the URL and its supported query parameters, the TLS files that the URL names, `PGPASSWORD` and `~/.pgpass`. It refuses each other setting by name. 0.7 opened its connections through libpq, which applied every libpq setting.

| Setting | 0.8.0 | What to do |
| ------- | ----- | ---------- |
| The query parameters `host`, `port`, `user`, `password`, `dbname`, `application_name`, `options`, `connect_timeout`, `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `sslnegotiation`, `channel_binding`, `target_session_attrs` and `load_balance_hosts` | Accepted | Nothing |
| `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count` or `tcp_user_timeout` in the URL | Refused | Set `tcp_keepalives`, `tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count` and `tcp_user_timeout_ms` in `PostgresConfig` |
| Another libpq parameter, for example `service`, `passfile`, `sslpassword`, `hostaddr` or `gssencmode`. An unknown parameter. A parameter that the URL gives two times | Refused | Remove it |
| `connect_timeout=0`, or a negative `connect_timeout` | Refused: it waits with no limit | Give 1 to 3,600 seconds. With no `connect_timeout`, one connection attempt to one host waits at most 10 seconds |
| A URL with no host, for example `postgresql:///mydb` | Refused | Give the host. For a Unix socket, give `host=/path/to/socket/directory` |
| A libpq environment variable for a parameter that the URL does not set, for example `PGSSLMODE` | Refused | Set the parameter in the URL, or unset the variable |
| A libpq environment variable for a parameter that the URL sets | Ignored, as libpq ignores it | Nothing |
| A libpq environment variable that horsies does not support, for example `PGSERVICE`, `PGTZ` or `PGGSSENCMODE` | Refused | Unset it |
| `PGPASSFILE`, when neither the URL nor `PGPASSWORD` gives the password | Refused | Unset it. horsies reads only `~/.pgpass` |

The core refuses these settings when it opens its first pool, not when the config is built. A send then returns `TaskSendEnqueueFailed`, and `horsies worker` does not start. Run `horsies check <app> --live` in the environment of each process. It reports a refused setting as `HRS-203` (`broker connectivity check failed`), with the cause in a note. See [Connection URL Format](/horsies/configuration/broker-config/#connection-url-format).

## The native extension

0.8.0 contains a compiled extension module, `horsies._core`, that maturin builds from Rust. 0.7 was pure Python. The Python version (3.13 or later) and the Python dependencies do not change.

- A wheel contains the compiled module. The module uses the stable ABI of CPython 3.13 (`abi3`), so one wheel of a platform serves the standard builds of CPython 3.13 and later.
- When no wheel fits the platform, pip and uv build the module from the source distribution.
- A build from the source distribution or from a clone needs a Rust toolchain and a C compiler. The repository pins Rust 1.91.1 in `rust-toolchain.toml`. The C compiler builds the TLS library `ring`. The installer gets the build backend, maturin, itself.
- `import horsies` fails with `ImportError` when `horsies._core` is missing.

## Start on a new database

The core installs a new baseline schema. It does not upgrade a 0.7 database, and it changes nothing in a 0.7 database:

- A 0.8.0 process refuses a 0.7 database at its first operation. A send returns `TaskSendEnqueueFailed` with `retryable=False`. `horsies worker` logs `Worker start failed (SCHEMA_REFUSED)` and exits with code 1. The cause says: `the database holds a horsies 0.x schema (...); this build does not upgrade a 0.x database: drain it and point the fleet at a new database`.
- A 0.7 process refuses a 0.8.0 database: `database schema v1000 is newer than this build supports (v61)`.
- The 0.8.0 web UI shows a 0.7 database as `SchemaState.ZERO_X`.

Nothing moves from the 0.7 database. Tasks in flight, workflows, task history, schedule state and idempotency key reservations stay there. See [Schema Creation](/horsies/internals/database-schema/#schema-creation).

1. Create a new PostgreSQL database for 0.8.0.
2. Stop the 0.7 producers and `horsies scheduler`, so that no new task enters the 0.7 database.
3. Let the 0.7 workers run the queued tasks to their end. The 0.7 table `horsies_tasks` holds only PENDING, CLAIMED and RUNNING tasks. The drain is complete when both queries return 0:

   ```sql
   SELECT count(*) FROM horsies_tasks;
   SELECT count(*) FROM horsies_workflows WHERE status IN ('PENDING', 'RUNNING', 'PAUSED');
   ```

   A task that was sent with a delay stays PENDING until its time. A PAUSED workflow does not end by itself: resume it or cancel it with 0.7.
4. Stop the 0.7 workers and `horsies web`.
5. Point each 0.8.0 process at the new database. Start the workers, the scheduler, the producers and `horsies web`. The first operation installs the schema.

## Task ids are UUIDs

Each task id of the public API is a `uuid.UUID`, not a `str`. Workflow ids stay `str`, except the id of a `WorkflowHandle` ([below](#workflow-handle-ids-are-uuids)).

| Where | What to do |
| ----- | ---------- |
| `TaskHandle.task_id`, the task ids of the send errors and of `BrokerIdempotencyKeyConflict`, `TaskInfo`, `TaskAttemptInfo`, `RawResultRecord` and the rerun outcomes | Read a `uuid.UUID`. Write `str(task_id)` where you keep or send the id as text, for example in JSON |
| The `task_id` of `Horsies.get_result`, `get_raw_result`, `get_task_info` and their async forms, of the broker reads and of `PostgresBroker.enqueue`; `RerunTask.source_task_id`; `horsies.monitoring.get_task_detail` and `cancel_task` | Give a `uuid.UUID`. Parse a stored text with `uuid.UUID(text)` |
| The `id` of each row of `get_stale_tasks` and `get_expired_tasks` | Read a `uuid.UUID` |

```python
from uuid import UUID

stored_task_id = '0193a1f2-4c5d-7e6f-8a9b-0c1d2e3f4a5b'
result = app.get_result(UUID(stored_task_id))
```

## Workflow handle ids are UUIDs

`WorkflowHandle.workflow_id` is a `uuid.UUID`, not a `str`, and pyright refuses a `str` id. A handle that a start returns needs no change.

| Where | What to do |
| ----- | ---------- |
| `WorkflowHandle(workflow_id=text, broker=broker)` | Parse the text once with `WorkflowHandle.for_id(text, broker)`: `Ok(handle)`, or `Err(HandleInvalidWorkflowId)` for text that is not a UUID. In 0.7 each call of such a handle returned `DB_OPERATION_FAILED` |
| `handle.workflow_id` | Read a `uuid.UUID`. Write `str(handle.workflow_id)` where you keep, send or compare the id as text, for example against the `workflow_id` of a handle error |
| The `workflow_id` of `pause_workflow`, `resume_workflow`, `pause_workflow_sync` and `resume_workflow_sync` | Give a `uuid.UUID` |
| The `workflow_id` of the handle error classes | Still a `str`: the canonical text of the UUID (lower case, with hyphens). In 0.7 it was the text that the handle was made from |

```python
from horsies import Err, Ok, WorkflowHandle

stored_workflow_id = '0193a1f2-4c5d-7e6f-8a9b-0c1d2e3f4a5c'
match WorkflowHandle.for_id(stored_workflow_id, app.get_broker()):
    case Ok(ok_value=handle):
        status = handle.status()
    case Err(err_value=invalid):
        print(invalid.message)  # the text is not a UUID; no handle was built
```

## The class of a database failure

In 0.7, the `exception` field of a database failure held the exception of the driver (SQLAlchemy or psycopg). In 0.8.0, it holds one class of the core: `CoreServerError`, `CoreConnectionError`, `CoreContractViolation` or `CorePreconditionFailed`. Examples are `BrokerOperationFailed.exception`, `TaskSendEnqueueFailed.exception`, `TaskActionDbOperationFailed.exception` and `HandleDbOperationFailed.exception`. The declared types do not change.

Code that tests the class of `exception` against a SQLAlchemy or psycopg class must test the core classes. The SQLSTATE of a server error is `CoreServerError.sqlstate`:

```python
from horsies import BrokerOperationFailed, CoreServerError


def server_sqlstate(error: BrokerOperationFailed) -> str | None:
    match error.exception:
        case CoreServerError(sqlstate=sqlstate):
            return sqlstate
        case _:
            return None
```

The fields of each class are in [Calls That Run on the Core](/horsies/tasks/error-types/#calls-that-run-on-the-core). These classes, and the errors of the next section, are defined in the extension `horsies._core`, and its type stubs are generated from the core. Import them from `horsies`.

The code enums `BrokerErrorCode`, `TaskSendErrorCode`, `WorkflowStartErrorCode`, `HandleErrorCode`, `OperationalErrorCode`, `ContractCode`, `RetrievalCode`, `OutcomeCode`, `TaskStatus`, `TaskAttemptOutcome`, `WorkflowStatus`, `WorkflowTaskStatus`, `TaskAbsenceReason`, `MonitoringQueryErrorCode` and `TaskActionErrorCode` are also generated, into `horsies._catalog`. `horsies` and `horsies.monitoring` export them with the same names, and their 0.7 module paths still import them. Their members change only as [Removed and changed members](#removed-and-changed-members) states.

## Errors after a fork or a core panic

`ForkedAfterCoreStart` and `CoreStopped` are states of the process. The core refuses each later call in that process, so a retry in the same process gets the same refusal. Import both from `horsies`.

- Each call that runs on the core raises them, in its sync form and in its async form. No call returns them as a result, and no result type has a variant for them.
- This covers the sends and their retry methods, `PostgresBroker.enqueue()`, the task, result and health reads, `ensure_schema_initialized()`, `check(live=True)`, the workflow start, the methods of `WorkflowHandle`, `pause_workflow()`, `resume_workflow()`, the reads and `cancel_task()` of `horsies.monitoring`, and the schema probe of `horsies web`.
- Code that handles only the `Err` of a call does not see them.

See [Process-State Refusals](/horsies/tasks/error-types/#process-state-refusals).

### `ForkedAfterCoreStart`

The process was forked without exec after the core started in its parent. `args` are `(core_pid, this_pid, advice)`.

- The core of a process starts at the first call that runs on it, for example the first send or read. An import of horsies starts nothing. A new app or a new broker starts nothing.
- A worker child starts its core when it starts a run. Thus a fork in a task body always comes after the start.
- A fork without exec is, for example, `os.fork()`, or `multiprocessing` with the `fork` start method. `fork` is the default start method on Linux before CPython 3.14.

To fix it, do one of these:

- Start the child processes that call horsies with the `spawn` or `forkserver` start method.
- Fork before the first call of the parent that runs on the core.

```python
import multiprocessing
from typing import assert_never

from horsies import Err, Ok


def send_report(report_id: int) -> bool:
    match my_task.send(report_id=report_id):
        case Ok():
            return True
        case Err():
            return False
        case _ as unreachable:
            assert_never(unreachable)


if __name__ == '__main__':
    context = multiprocessing.get_context('spawn')
    with context.Pool(processes=4) as pool:
        sent = pool.map(send_report, [101, 102, 103])
```

### `CoreStopped`

A core background task panicked. `args` are the `(subject, cause)` of the first panic. The panic is a defect of horsies.

- In a `horsies worker` process, the panic aborts the process. Its supervisor restarts it.
- In each other process, for example a producer, `horsies scheduler` or a web app, the core stops. Each later call raises `CoreStopped`.

To fix it, restart the process. The core does not start again in the same process. Report the subject and the cause.

## Removed and changed members

| 0.7 | 0.8.0 | What to do |
| --- | ----- | ---------- |
| `SlotHealthNotRecorded`; `SlotHealth` was `AllSlotsServing \| SlotHealthNotRecorded \| SlotsDown` | Removed. `SlotHealth` is `AllSlotsServing \| SlotsDown` | Remove the match arm |
| `ClassCapacityNotRecorded`; `ClassCapacity` was `ClassBudgets \| ClassCapacityNotRecorded` | Removed. `ClassCapacity` is `ClassBudgets` | Remove the match arm |
| `ClassesNotRecorded`, `horsies.monitoring.ClassesNotRecordedInfo`, and the `not_recorded` fields of `ClassCoverage`, `ClassReport` and `ClassReportInfo` | Removed. Each worker snapshot of the baseline schema records these facts | Remove the reads |
| `SchemaState.CUTOVER_REQUIRED` | Removed. `SchemaState.ZERO_X` names a 0.x database | Match `ZERO_X` |
| `GroupRow.group` of `horsies.monitoring.task_breakdown` was a `str`; the unclaimed tasks were `'unknown'`. `Breakdown.total` was a `GroupRow` with the group `'TOTAL'` | `GroupRow.group` is `BreakdownValueGroup(value=...)`, or `BreakdownUnclaimedGroup()` for the tasks that no worker claimed (`group_by='worker'` only). A worker with the id `unknown` is a value group of its own. `GroupRow` derives from `BreakdownCounts`. `Breakdown.total` is a `BreakdownCounts`, with no `group` | Match `row.group` on the two classes. Read `total` as counts |
| `queue_priorities`, `queue_max_concurrency` and `recovery_config` of `WorkerStateSnapshot`, and `WorkerStateInfo.queue_max_concurrency`, were `dict \| None` | `dict`. `{}` when the worker had none set | Test for `{}` where the code tested `None` |
| `OperationalErrorCode` | One more member: `WORKFLOW_START_INCOMPLETE`. Recovery fails a RUNNING workflow that has no node rows with this code. `TaskError` refuses `'WORKFLOW_START_INCOMPLETE'` as a user code | Add the case to an exhaustive `match`. Rename a user code with this name |
| `TaskSendPayload.args_json`, `kwargs_json` and `enqueue_sha` | Removed. `TaskSendPayload.input_json` holds the keyword arguments as one JSON object, or `None` for none | Read `input_json` |
| `PostgresBroker.enqueue()` and `enqueue_async()` with `args_json`, `kwargs_json` and `enqueue_sha` | `input_json` replaces the three. `sent_at` and `send_kind` are required. An `input_json` that is not a JSON object or null, or that repeats a key, gets `INVALID_INPUT` | Give `input_json`, `sent_at` and `send_kind`. Import `SendKind` from `horsies`: `SendKind.FIRST` for the first send of a task id, `SendKind.REPEAT` for a resend |
| `RerunInputUnavailable.reference_locator` | Removed. `task_id` and `disposition` stay | Remove the read |
| `horsies.core.utils.fingerprint.enqueue_fingerprint` | Removed | Remove the call |
| `get_stale_tasks(stale_threshold_minutes)` with any value | An `int` from 0 to 525,600, else `INVALID_INPUT`. `get_stale_tasks` and `get_expired_tasks` return at most 10,000 rows | Give a value in the range |
| `loads_json('')` and `loads_json(None)` in `horsies.core.codec.json_io` read as `null` | `loads_json` and `loads_json_bytes` refuse an empty text with `EmptyJsonTextError`, and take no `None`. They refuse an object that repeats a key | Check for `None` before the call |
| `horsies.web.routes`, `horsies.web.events`, `horsies.web.read_cache`, `horsies.web.auth.view_guard`, `horsies.web.auth.act_guard`, `horsies.web.schema.schema_guard` | Removed. The core serves each route. The exports of `horsies.web` do not change | Mount `create_monitoring_app` |
| `horsies/web/static/` in the wheel | Removed. `horsies._core` contains the web UI | Nothing to serve |
| Syce, the terminal monitoring tool | Removed | Use the web UI |
| `PostgresBroker.mark_stale_tasks_as_failed()` recovered every stale RUNNING task in one call | One call reads at most 1,000 stale tasks. When it reads 1,000, it logs a warning, and the next call or the next reaper pass recovers the rest. A threshold outside 0 to 2,147,483,647 ms gets `CLEANUP_FAILED`, not retryable | Give thresholds in the range. With `auto_fail_stale_running=False`, call the method again after the warning |
| `PostgresBroker.create_single_connection_engine()` and `BrokerResources.session_async_engine()` | Removed | Remove the calls |

The error types keep their `EVENT_LOOP_LIMIT` and `LOOP_RUNNER_FAILED` variants, so an exhaustive `match` on them stays valid. The calls that run on the core do not return them.

## Config values with new bounds

0.7 accepted these values. 0.8.0 refuses them when the config is built, unless the row says otherwise.

| Field | 0.8.0 accepts | Error |
| ----- | ------------- | ----- |
| `PostgresConfig.pool_size` | 1 to 10,000. `0` (no limit in SQLAlchemy) is refused | `HRS-213` |
| `PostgresConfig.max_overflow` | 0 to 10,000. `-1` (no limit in SQLAlchemy) is refused | `HRS-213` |
| `PostgresConfig.pool_timeout` | 0 to 4,294,967,295 seconds | `HRS-213` |
| `PostgresConfig.pool_recycle` | `-1`, or 0 to 4,294,967,295 seconds | `HRS-213` |
| `tcp_keepalives_idle`, `tcp_keepalives_interval`, with `tcp_keepalives=True` | 1 to 32,767 seconds | `HRS-215` |
| `tcp_keepalives_count`, with `tcp_keepalives=True` | 1 to 127 | `HRS-215` |
| `tcp_user_timeout_ms` | 0, or 1 to 2,147,483,647 ms | `HRS-215` |
| The name of a `CustomQueueConfig` | At most 52 bytes of UTF-8 text. In 0.7, each send to a queue with a name of 53 to 100 bytes failed | `HRS-200` |
| The `time` of `DailySchedule`, `WeeklySchedule` and `MonthlySchedule` | No `tzinfo` and no microseconds. Set the zone in the `timezone` field of the schedule | `HRS-205` |
| `paused_workflow_auto_cancel_after` | At most 365,000 days | `HRS-216` |
| `prefetch_buffer`, `cluster_wide_cap`, and the `max_concurrency` of a custom queue | At most 2,147,483,647 | `HRS-202`, `HRS-201`, `HRS-200` |
| `claim_lease_ms`, `max_claim_renew_age_ms` | At most 4,294,967,295 | `HRS-202` |
| `PayloadPolicy.warn_bytes`, `reject_bytes` | At most 18,446,744,073,709,551,615. Both count bytes of the UTF-8 text, not characters | `ValidationError` |
| `RetryPolicy.max_delay_seconds` | Below 2<sup>64</sup>. A larger value is accepted when the config is built, and each send of the task gets `VALIDATION_FAILED` | `VALIDATION_FAILED` at the send |

See [Refusals](/horsies/configuration/broker-connections/#refusals).

## Web API clients

| Request | 0.7 | 0.8.0 |
| ------- | --- | ----- |
| `GET /api/tasks/{task_id}` and `POST /api/tasks/{task_id}/cancel`, with an id that is not a UUID | 404 | 400 `{"code": "INVALID_INPUT", "detail": "Path parameter task_id: ..."}` |
| `POST /api/workflows/{workflow_id}/pause`, `/resume` and `/cancel`, with an id that is not a UUID | 503 | 400 `{"code": "INVALID_INPUT", "detail": "Path parameter workflow_id: ..."}` |
| A request body over 1 MiB | – | 413 |
| `GET /api/workers/classes` | Has the key `not_recorded` | Has no key `not_recorded` |
| `slot_health` of `GET /api/workers` | Can be `not_recorded` | Never `not_recorded` |
| `queue_max_concurrency` of `GET /api/workers` | Can be `null` | `{}` when none is set |
| `GET /api/tasks/breakdown` | `group` is text, `'unknown'` for the unclaimed tasks. `total` has the group `'TOTAL'` | `group` is `{"kind": "value", "value": ...}` or `{"kind": "unclaimed"}`. `total` has no `group` key |
| The times of each answer | In the zone of the database session | In UTC, with `Z` |

## The worker liveness lock

Each `horsies worker` process holds a session advisory lock on one connection of its own to the session URL (`session_database_url`, else `database_url`). It takes the lock before its first claim, and holds it until its stop ends. The lock has no setting of its own. See [Worker Liveness Lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock).

### Connections

- The lock takes 1 connection on the session URL.
- The listener takes 1 connection. In 0.7 it took 2.
- At the defaults, with no `session_database_url`, a worker thus opens as many connections as in 0.7. A profile of 8 isolated children needs `5 + 1 + 1 + 1 + 8 × 1 = 16`.
- When `session_database_url` differs from `database_url`, the count also holds a session pool on the session URL, with the size of the pool of the worker process.
- `horsies check <app> --live`, `HRS-219` and the boot log count the lock as `liveness lock 1`. Run the live check against the new database.

See [Connections](/horsies/workers/worker-profiles/#connections).

### Exit code 79

With `auto_fail_stale_running=True` (the default), a worker that holds a task and whose lock is not held for 60 seconds kills each child that holds a task. Then it exits with code 79, with no drain. Configure the supervisor to restart the worker after code 79. See [Worker Exits With Code 79](/horsies/workers/heartbeats-recovery/#worker-exits-with-code-79).

### Recovery of an ended worker

When a worker process ends, the server releases its lock. The reaper of a live worker recovers the CLAIMED and RUNNING tasks of the ended worker when two of its passes, at least 75 seconds apart, find the lock free. At the default `check_interval_ms` of 30 seconds, this is 1.5 to 2 minutes after the worker ended. 0.7 waited for `running_stale_threshold_ms` (default 300 seconds) after the last runner heartbeat of a RUNNING task, and for `claimed_stale_threshold_ms` for a CLAIMED task.

When the server does not see the session end, for example after a power loss of the worker host, the heartbeat thresholds apply. See [Recovery Time After a Worker Stops](/horsies/workers/heartbeats-recovery/#recovery-time-after-a-worker-stops).

## After the upgrade

The changes in behaviour that need no change of code are in the Changed and Fixed lists of 0.8.0 in the [Changelog](/horsies/changelog/). Examples:

- The times of the `horsies.monitoring` reads and of the web API are in UTC.
- The scheduler takes its time from the database clock.
- `TaskInfo.attempts` is newest first also for a task in history.
- `echo=True` logs each statement of the core on the logger `horsies.sql`.

## Things to Avoid

**Do not point 0.8.0 at the 0.7 database.** The core refuses it and changes nothing in it.

**Do not stop the 0.7 workers before the queues drain.** The tasks that wait in the 0.7 database do not move to the new database.

**Do not call horsies in a child that its parent forked after a call that runs on the core.** Each core call in the child raises `ForkedAfterCoreStart`. Use the `spawn` or `forkserver` start method.
