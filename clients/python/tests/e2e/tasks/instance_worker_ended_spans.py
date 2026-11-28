"""App instance with `OtelTracing()`: the worker makes the span of each
attempt that it ended, in its handler child.

The app registers no event handler: the switch `MAKE_SPANS` (the default of
`OtelTracing`) starts the handler child alone. Its start hook, and the start
hook of each task child, set a tracer provider whose exporter writes one
JSON line per ended span to the file that the environment variable
`SPAN_OUT_ENV` names: the spans of the children and the spans that the
handler child makes go to one file.
"""

from __future__ import annotations

import asyncio
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
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.types.result import Ok, Result
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
    database=PostgresConfig(
        database_url=SecretStr(DB_URL),
        pool_size=5,
        max_overflow=5,
    ),
    worker_profiles=E2E_WORKER_PROFILES,
    tracing=OtelTracing(),
)

app = Horsies(config)
broker = PostgresBroker(config.database)
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


@app.on_child_start
def set_child_tracer_provider() -> None:
    """The task children export their attempt spans to the same file."""
    set_tracer_provider()


@app.task(name='e2e_worker_ended_spans_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(name='e2e_worker_ended_spans_sleeper', timeout_ms=SLEEPER_TIMEOUT_MS)
def sleeper() -> Result[str, TaskError]:
    time.sleep(60)
    return Ok('woke')


@app.task(
    name='e2e_worker_ended_spans_shared_sleeper',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=SLEEPER_TIMEOUT_MS,
)
async def shared_sleeper() -> Result[str, TaskError]:
    """The shared async child cancels it at its deadline and reports the
    timeout: the child lives, and its span is the span of the attempt."""
    await asyncio.sleep(60)
    return Ok('woke')


# One node of the sleeper: the worker kills it at its timeout, and the
# handler child reads its node for the span.
node_sleeper = TaskNode(function=sleeper)

spec_sleeper = app.workflow(
    name='e2e_worker_ended_spans_flow',
    nodes=[node_sleeper],
    definition_key='tests.e2e.tasks.instance_worker_ended_spans.flow.v1',
)


app.discover_tasks(['tests.e2e.tasks.instance_worker_ended_spans'])
