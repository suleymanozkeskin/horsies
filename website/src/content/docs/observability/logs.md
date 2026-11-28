---
title: Logs
section: shared
summary: The fields of the core's log records - task_id, attempt and trace_id on each record of an attempt - the log context of a task body, and the JSON format of each client.
related: [tracing, log-setup, ../rust/cli, ../workers/heartbeats-recovery]
tags: [observability, logging, json, correlation]
---

The core writes its log records as `tracing` events with named fields. Each client hands them to its own log system: the Python client to Python `logging`, the Rust client to its `tracing` subscriber. The fields of a record of an attempt join it to the task, the attempt and the trace.

## Fields of the Core's Records

Each record of the worker that names a claimed task carries:

| Field | Value |
|-------|-------|
| `task_id` | The task id |
| `attempt` | The attempt of the claim: the retry count of the row plus 1 |
| `trace_id` | The trace id that the send stored on the row, 32 lowercase hex digits. A row with no trace id gives no value, and the record has no such field |

This holds for the records of the claim, the placement on a child, the run in a slot, the outcome write, the retry wake, and the steps after the outcome of a workflow task. The records of the reaper and of the scheduler name a `task_id` only.

The records of a retry inside one of these steps name the retry in fields of their own, so `attempt` always means the attempt of the task:

| Field | Value |
|-------|-------|
| `phase2_retry`, `phase2_retries` | The retry number, and the bound, of a write of the workflow step after a task outcome |
| `advance_retry`, `advance_retries` | The retry number, and the bound, of the advance of a parent workflow |

In Python each field is an attribute of the `LogRecord`. In Rust each field is a field of the `tracing` event.

## The Context of a Task Body

The client puts the task on the records that the task body writes:

| Client | Mechanism | Fields |
|--------|-----------|--------|
| Python | `TaskContextFilter` on the horsies handler; add it to a handler of your own | `task_id`, `task_name`, `attempt`, and `trace_id`, `span_id` when the attempt has a span |
| Rust | Each task body runs inside the `tracing` span `run` (target `horsies::attempt`) | `horsies.task.id`, `horsies.task.name`, `horsies.task.attempt` on each record of the body |
| TypeScript | `currentTask()` in a log library hook (a mixin or a format) | `taskId`, `taskName`, `attempt`, and `traceId`, `spanId` when the attempt has a span |

Each works with tracing on or off. With tracing on, the `trace_id` of a record of the body is the trace id of the attempt span: the trace of the send, or a new trace with `LinkedToSend()` or with no carrier.

## JSON Lines

| Client | Turn on | Keys |
|--------|---------|------|
| Python | `AppConfig(log_format=JsonLogs())` | `ts`, `level`, `logger`, `message`, `process`, `task_id`, `task_name`, `attempt`, `trace_id`, `span_id`, `exc`; each line has every key |
| TypeScript | `logFormat: { kind: "jsonLogs" }` in `Horsies.create` | The keys of the Python client; a key with no value is `null` |
| Rust | `horsies::cli::run_with_log_format(app, LogFormat::JsonLogs)` | `timestamp`, `level`, `target`, the fields of the event at the top level (`message` among them), `span` (the innermost span, with its name and fields) and `spans` (every span of the record, outermost first) |

The worker, the scheduler, the check and each worker child write in the format. The setup of each client: [Log Setup (Python)](/horsies/observability/log-setup/), [Logs (Rust)](/horsies/rust/cli/#logs), [Log Format (TypeScript)](/horsies/typescript/configuration/app-config/#log-format).

## Records of a Failing Recovery Step

A step of the reaper that keeps failing still runs at each pass. Its log is:

- the first failure: one line with the cause;
- then one line per 10 failures in a row, with the count and the last cause;
- the first pass without a failure after failures: one INFO line with the count.

Each line has the field `step`. When the worker records metrics, `horsies.recovery.pass.failures{step}` counts every failure. See the [Metric Reference](/horsies/observability/metric-reference/#horsiesrecoverypassfailures).

## Things to Avoid

**Don't parse `attempt` out of a message text.** Read the field: the message of a record does not always name the attempt.
