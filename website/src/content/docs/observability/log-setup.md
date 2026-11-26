---
title: Log Setup
section: python
summary: The log format of the horsies processes (AppConfig.log_format, JsonLogs), and the task context on log records (TaskContextFilter, JsonLogsFormatter).
related: [logs, tracing-setup, ../configuration/app-config]
tags: [observability, logging, json, python]
---

`AppConfig.log_format` sets the format of the records of the `horsies` loggers. `TaskContextFilter` puts the task id, the task name, the attempt and the span ids of the running task on each record. The fields of the core's own records are on [Logs](/horsies/observability/logs/).

## How To

### Write JSON Lines

```python
from pydantic import SecretStr

from horsies import AppConfig, Horsies, JsonLogs, PostgresConfig

config = AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    log_format=JsonLogs(),
)
app = Horsies(config)
```

`horsies worker`, `horsies scheduler`, `horsies check` and each worker child write the records of the `horsies` loggers to stderr in this format, once they have loaded the app. One record is one line:

```json
{"ts":"2026-11-20T10:15:30.123456+00:00","level":"INFO","logger":"myapp.orders","message":"charging card","process":4242,"task_id":"0193a5e2-7c41-7d2e-9f10-3b8c2a6d4e51","task_name":"charge_card","attempt":1,"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","exc":null}
```

| Key | Value |
|-----|-------|
| `ts` | The time of the record, ISO 8601 in UTC, in microseconds |
| `level` | The level name |
| `logger` | The logger name |
| `message` | The message |
| `process` | The process id |
| `task_id`, `task_name`, `attempt`, `trace_id`, `span_id` | The record attributes of these names, or `null` |
| `exc` | The traceback text of the exception of the record, or `null` |

Each line has every key. The line is ASCII: other characters are escaped. `TextLogs()` (the default) writes one line of text per record.

### Put the Task Context on Your Own Records

The horsies handler has `TaskContextFilter`. Add the filter to a handler of your own to get the same attributes there, and `JsonLogsFormatter` for the same JSON lines:

```python
import logging

from horsies import JsonLogsFormatter, TaskContextFilter

order_handler = logging.StreamHandler()
order_handler.setFormatter(JsonLogsFormatter())
order_handler.addFilter(TaskContextFilter())
order_log = logging.getLogger('myapp.orders')
order_log.addHandler(order_handler)


@app.task('charge_card')
def charge_card(*, amount_cents: int) -> Result[str, TaskError]:
    order_log.info('charging card')
    return Ok('charged')
```

Inside the body of a task in a worker child, each record of `order_log` gets:

| Attribute | Value |
|-----------|-------|
| `task_id` | The task id |
| `task_name` | The task name |
| `attempt` | The number of the attempt, from 1 |
| `trace_id` | The trace id of the attempt span, 32 lowercase hex digits, when the attempt has a span |
| `span_id` | The span id of the attempt span, 16 lowercase hex digits, when the attempt has a span |

Outside a task body each attribute is `None`, so a format string can name every one. A record that has an attribute of one of these names keeps it: the records of the core carry their own `task_id`, `attempt` and `trace_id`. The filter never drops a record.

The context holds in an isolated child, on each seat of a shared async child and on each seat of a shared thread child: two tasks on two seats of one child each get their own values.

## API Reference

### `JsonLogs()`

One JSON object per line, with the keys of the table above. A value of `AppConfig.log_format`.

### `TextLogs()`

One line of text per record: time, component, level, message. The default of `AppConfig.log_format`.

### `TaskContextFilter()`

A `logging.Filter`. Sets `task_id`, `task_name`, `attempt`, `trace_id` and `span_id` on each record that does not have them. Returns `True` for every record.

### `JsonLogsFormatter()`

A `logging.Formatter`. Formats a record as one JSON line with the keys `ts`, `level`, `logger`, `message`, `process`, `task_id`, `task_name`, `attempt`, `trace_id`, `span_id` and `exc`. A context value that is not a text or an integer is written as its text.
