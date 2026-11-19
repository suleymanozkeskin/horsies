"""App instance with `OtelTracing()`: the worker makes the span of each
attempt that it ended, in its handler child.

The app registers no event handler: the switch `MAKE_SPANS` (the default of
`OtelTracing`) starts the handler child alone. Its start hook sets a tracer
provider whose exporter writes one JSON line per ended span to the file that
the environment variable `SPAN_OUT_ENV` names.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Sequence
from typing import Final

from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.tracing import OtelTracing
from horsies.core.models.workflow import TaskNode

from tests.e2e.helpers.env import e2e_database_url
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES

SPAN_OUT_ENV: Final = 'HORSIES_E2E_SPAN_OUT'
"""The environment variable that names the file of the span lines."""

SLEEPER_TIMEOUT_MS: Final = 1_000
"""The timeout of the sleeper: the worker kills its child at it."""

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')

config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
    worker_profiles=E2E_WORKER_PROFILES,
    tracing=OtelTracing(),
)

app = Horsies(config)
broker = PostgresBroker(config.broker)
app._broker = broker  # pyright: ignore[reportPrivateUsage]
broker.app = app


class LineExporter(SpanExporter):
    """Writes one JSON line per ended span: its name, ids, parent, times,
    status and attributes."""

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        with open(os.environ[SPAN_OUT_ENV], 'a', encoding='utf-8') as out:
            for span in spans:
                context = span.context
                parent = span.parent
                line = {
                    'name': span.name,
                    'trace_id': format(context.trace_id, '032x') if context else None,
                    'parent_span_id': format(parent.span_id, '016x') if parent else None,
                    'start': span.start_time,
                    'end': span.end_time,
                    'status': span.status.status_code.name,
                    'attributes': dict(span.attributes or {}),
                }
                _ = out.write(json.dumps(line) + '\n')
        return SpanExportResult.SUCCESS


@app.on_handler_child_start
def set_tracer_provider() -> None:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(LineExporter()))
    trace.set_tracer_provider(provider)


@app.task(task_name='e2e_worker_ended_spans_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_worker_ended_spans_sleeper', timeout_ms=SLEEPER_TIMEOUT_MS)
def sleeper() -> TaskResult[str, TaskError]:
    time.sleep(60)
    return TaskResult(ok='woke')


# One node of the sleeper: the worker kills it at its timeout, and the
# handler child reads its node for the span.
node_sleeper = TaskNode(fn=sleeper)

spec_sleeper = app.workflow(
    name='e2e_worker_ended_spans_flow',
    tasks=[node_sleeper],
    definition_key='tests.e2e.tasks.instance_worker_ended_spans.flow.v1',
)


app.discover_tasks(['tests.e2e.tasks.instance_worker_ended_spans'])
