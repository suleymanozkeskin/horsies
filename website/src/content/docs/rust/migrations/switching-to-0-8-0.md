---
title: Switching to 0.8.0
summary: Move a horsies-rust 0.1.0-alpha app to the horsies 0.8.0 crate - drain the old database, start 0.8.0 on a new one, and change the code by the list of API changes.
related: [../quick-start/getting-started, ../cli, ../configuration/app-config, ../tasks/defining-tasks, ../../changelog]
tags: [rust, migration, 0.8.0, 0.1.0-alpha, breaking-changes]
---

# Switching to 0.8.0

The 0.8.0 crate runs on `horsies-core`, the core that the Python client runs on, with a new baseline schema. It does not upgrade a database of `horsies-rust` 0.1.0-alpha. Drain the old database, then start 0.8.0 on a new one. This page lists the steps and the changes of the API. The full list is in the [Changelog](/horsies/changelog/#rust-client-crate-horsies).

## Switch Order

1. Change the code of the app by the tables below, and build it.
2. Create a new PostgreSQL database for 0.8.0.
3. Stop the producers and the scheduler of the 0.1.0-alpha app, so that no new task enters the old database.
4. Let the 0.1.0-alpha workers run the queued tasks to their end, then stop them.
5. Point the 0.8.0 app at the new database, and start its workers, its scheduler and its producers. The first operation installs the schema.

Nothing moves from the old database: tasks in flight, task history, schedule state and key reservations stay there.

## The Old Database Is Refused

The core refuses a database that 0.1.0-alpha installed (migrations 1 to 65) and changes nothing in it. The refusal is `SchemaRefusal::ForeignMigrations`: "the schema bookkeeping holds migration versions this build does not ship".

## Build

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| No declared Rust version | `rust-version = "1.91"` |
| The feature `arbitrary-precision` | Removed: the core stores the input text as the client wrote it |
| clap, tracing-subscriber, toml, owo-colors, tempfile always built | Built with the feature `cli`, on by default |
| sqlx always built | Built with the feature `sqlx`. The feature `tokio-postgres` adds the tokio-postgres rerun adapter |

## The App Binary

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| `horsies worker`, `horsies scheduler`, `horsies check` of the shipped binary | The commands of the app's own binary through `horsies::cli::run(app)`. The shipped binary refuses them with exit code 2 |
| Task bodies in the worker process | Task bodies in child processes: the worker starts the app's binary again in child mode. Every line of `main` before `run` runs in each child |
| `WorkerConfig`, `run_worker_with`, `Worker`, `WorkerError` | `app.run_worker(WorkerStart::profile(name))`, `WorkerRunError` |
| Worker flags `-q`/`--queues`, `-c`/`--concurrency`, `--max-claim-batch`, `--max-claim-per-worker`, `--coalesce-notifies` | A worker profile of `AppConfig.worker_profiles` and `--profile <name>`. The flags exit with code 2 and name what replaces each |
| Scheduler flags `--check-interval`, `--dry-run` | `AppConfig.schedule.check_interval_seconds`; the `check` command |
| `spawn_scheduler` | `app.scheduler()` (`PreparedScheduler`) and `app.run_scheduler()` |
| `horsies cutover`, `horsies transcode` | Removed |
| – | Added: `app.on_child_start(hook)`; `horsies skills install` |

See [CLI Reference](/horsies/rust/cli/), [Worker Setup](/horsies/rust/workers/worker-setup/) and [Schedule Config](/horsies/rust/scheduling/schedule-config/).

## The App

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| The app owns a broker: `with_broker`, `bind_broker`, `broker_if_initialized`, `get_broker`, `from_core`, `into_core`, `into_parts` | `Horsies::new` does no I/O; the app connects at its first operation. `Horsies::with_handle(config, Arc<CoreHandle>)` shares one handle |
| `new_observe_only` | `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)` |
| `role` and `set_role` with a string | `AppRole::Producer`, `Worker`, `Scheduler` |
| `AppError`, `AppResult` | Removed. `HorsiesError` for the config, a registration and `provide`; each other call returns its own closed error type |
| `PostgresBroker`, `Broker`, `pub mod core`, the row and listener types, `run_horsies_migrations`, `registry()`, `workflow_registry()` | No longer public |
| – | `broker.pool_size` outside 1 to 10,000 or `broker.max_overflow` above 10,000 is refused. A worker profile whose `preload` is `ChildPreload::Library` or `ChildPreload::App` is refused (`HRS-218`). A profile that does not set `preload` has `ChildPreload::NoPreload`, the default of `ChildPreload` |
| – | Added: `suppress_sends`, `are_sends_suppressed`, `HORSIES_SUPPRESS_SENDS` |

In `Default` queue mode a task that names a queue is refused, and in `Custom` mode a task that names no queue is refused (`HRS-200`). See [App Config](/horsies/rust/configuration/app-config/).

## Tasks and Errors

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| `TaskError` with a string code; `TaskError::new`, `TaskError::builtin`, `From<&str>`, `From<String>`, `cause` | `TaskError<E>` with `TaskErrorCode<E>`: `TaskError::user(code, message)` and `TaskError::library(code, message)`; read with `code()`, `message()`, `data()`, `exception()`, `sub_workflow()` |
| Free-text user codes | An enum with `#[derive(horsies::UserErrorCode)]`; `std::convert::Infallible` for a task with no codes of its own |
| `UNHANDLED_ERROR`, `TASK_ERROR` | `UNHANDLED_EXCEPTION`, `TASK_EXCEPTION` |
| `ARGUMENT_TYPE_MISMATCH`, `RETURN_TYPE_MISMATCH` | `WORKER_SERIALIZATION_ERROR` |
| `SubWorkflowError` | `SubWorkflowFailure`, and `ExceptionRecord` for a panic |
| `with_data` stored a non-finite float as `null` | `with_data` never fails; a value that cannot be stored is replaced with `{"data_refused": "<cause>"}` |
| A single parameter stored as its value: `fn double(x: i64)` stored `2` | Every parameter stored under its name: `{"x": 2}`; `fn f(order: Order)` stores `{"order": {...}}` |
| `auto_retry_for` entries stored as user codes; codes without a policy dropped | Checked at compile time: a built-in code or a code of `E`; `retry_policy` and `auto_retry_for` go together |
| – | `execution = isolated \| shared_async \| shared_thread`, and `cancel_grace_ms` on a `shared_async` task |
| `async_task_fn_kwargs!`, `blocking_task_fn_kwargs!` | `async_task_fn!`, `blocking_task_fn!` |
| `TaskRuntime::state` returned a `TaskError` | `Result<Arc<T>, StateNotProvided>`; `task_handle` returns `TaskFunctionNotFound` |
| – | Added: `TaskRuntime::cancelled`, `is_cancelled`, `spawn` |

Rename no parameter of a task that a stored input names: see [Defining Tasks](/horsies/rust/tasks/defining-tasks/) and [Task Errors](/horsies/rust/tasks/errors/).

## Sends and Reads

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| The generated `register`, `send`, `schedule`, `with_options` and `handle` of a task | `register` returns `TaskFunction<A, T, E>`; the sends return `Result<TaskHandle<T, E>, TaskSendError>`, a closed catalog; `handle(&runtime)` returns `Result<TaskFunction<A, T, E>, TaskFunctionNotFound>` |
| `schedule` took a `Duration` | `schedule` takes `WholeSeconds` |
| `retention_class` | `TaskSendOptions::retention(SendRetention)` |
| `TaskSendPayload` with `args_json`, `kwargs_json`, `enqueue_sha` | One `input_json` |
| `TaskHandle::get` returned `TaskResult<T>` | `get(WaitLimit)` returns `Result<TaskOutcome<T, E>, ReadError>` |
| – | Reads by task id on the app: `get_result`, `get_raw_result`, `get_task_info`, `task_handle_of` |
| – | The health reads on the app; `get_stale_tasks` and `get_expired_tasks` return `BoundedTaskList` |
| – | Added: the blocking views of `horsies::blocking` |

See [Sending Tasks](/horsies/rust/tasks/sending-tasks/), [Retrieving Results](/horsies/rust/tasks/retrieving-results/), [Health Reads](/horsies/rust/monitoring/health-reads/) and [Blocking API](/horsies/rust/tasks/blocking-api/).

## Workflows

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| `register_workflow`, `register_workflow_spec`, `register_parameterized_workflow`, `register_parameterized_workflow_with_children`, `workflow::<T>(name)`, `WorkflowRegistrationBuilder` | `register_workflow_definition::<D>()` and `workflow_template::<D>()` |
| `check_workflow_builder0` | `check_workflow_builder` |
| `TaskNode::args_json`, `kwargs_json`, `raw`, `task_options` | `set`, `set_input`, `input_json`; `TaskFunction::node()` and `task_name::node()` |
| A dependency's result as `TaskResult<T>` | A parameter of type `TaskOutcome<S, E>` with `arg_from` |
| `SuccessPolicy` and `SuccessCase` with node indexes | With node references |
| `SubWorkflowNode::new`, `typed`, `from_definition`, `input_json`, `good_until`, `workflow_ctx_from` | `D::node(SubWorkflowParams)` or `SubWorkflowNode::of::<D>(params)`; `set`, `set_params`, `arg_from` |
| `on_workflow_task_complete`, `on_subworkflow_complete`, `recover_stuck_workflows`, `cancel_workflow`, `pause_workflow`, `resume_workflow`, `get_workflow_result` on a `PgPool` | The core runs the engine; `WorkflowHandle` has `cancel`, `pause`, `resume`, `get` |
| `WorkflowHandle::result_for_key` | `result_for::<V, E>(&key)`. `get(WaitLimit)` returns `Result<TaskOutcome<T, UserCode>, WorkflowReadError>` |

See [Workflow API](/horsies/rust/concepts/workflows/workflow-api/), [Typed Node Builder](/horsies/rust/concepts/workflows/typed-node-builder/) and [Subworkflows](/horsies/rust/concepts/workflows/subworkflows/).

## Monitoring and Rerun

| 0.1.0-alpha | 0.8.0 |
|-------------|-------|
| The monitoring functions took a `PostgresBroker` | They take `&Horsies`: see [Monitoring Queries](/horsies/rust/monitoring/monitoring-queries/) |
| `web::create_monitoring_router` | `web::router(&app, MonitoringConfig, policy)`: see [Web UI Mount](/horsies/rust/monitoring/web-ui-mount/) |
| `rerun_task(broker, ...)` | `app.rerun_task(command, policy)`; added: `rerun::rerun_task_in` in the caller's transaction. See [Rerun in Your Transaction](/horsies/rust/operations/rerun-in-your-transaction/) |
| The CLI types and `fetch_docs` in the library | No longer public |

## Stored Forms

The 0.8.0 client writes each stored form as the Python client writes it:

- A send of a task with no parameters, and a schedule without input, store the input `{}`.
- The task options of a send with a deadline hold the deadline in `good_until`.
- A date-time of type `horsies::StoredDateTime<T>` is stored in Python's text (`…12.500000Z`). A plain chrono value keeps chrono's serde text (`…12.500Z`); both clients read either text to the same value.

## Things to Avoid

**Do not point 0.8.0 at the old database.** The core refuses it and changes nothing in it.

**Do not stop the 0.1.0-alpha workers before the queues drain.** The tasks that wait in the old database do not move to the new one.
