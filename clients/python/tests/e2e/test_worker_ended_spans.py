"""E2E: the span of an attempt that the worker ended, through a real worker.

An isolated task outruns its timeout; the worker kills its child, records
`TASK_TIMEOUT`, and sends the `AttemptEnded` event to its handler child,
which makes the span with the tracer provider of its start hook. The task is
sent inside a span of the test process, so the stored carrier puts the
worker-made span in that trace.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, cast

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskFunction, TaskHandle
from horsies.core.types.result import is_err
from tests.e2e.helpers.assertions import assert_err, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_worker_ended_spans as instance
from tests.e2e.tasks.instance_worker_ended_spans import SPAN_OUT_ENV

INSTANCE: Final = 'tests.e2e.tasks.instance_worker_ended_spans:app'
RESULT_TIMEOUT_MS: Final = 30_000
SPAN_LINE_TIMEOUT_S: Final = 30.0
"""The longest wait for the span line: the worker writes an events frame
within its flush interval, and the handler child makes the span at once."""
POLL_INTERVAL_S: Final = 0.1


def _ready_check(healthcheck: TaskFunction[[], str]) -> Callable[[], bool]:
    """Ready once a `def` healthcheck task ran in a child."""
    handle: TaskHandle[str] | None = None

    def check() -> bool:
        nonlocal handle
        if handle is None:
            sent = healthcheck.send()
            if is_err(sent):
                return False
            handle = sent.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return check


def _span_lines(path: Path) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding='utf-8')
    except FileNotFoundError:
        return []
    return [cast('dict[str, Any]', json.loads(line)) for line in text.splitlines()]


async def _wait_for_span(path: Path, task_id: str) -> list[dict[str, Any]]:
    """The span lines of `path` of the task `task_id`, once there is one."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SPAN_LINE_TIMEOUT_S
    while loop.time() < deadline:
        of_task = [
            line
            for line in _span_lines(path)
            if line['attributes'].get('horsies.task.id') == task_id
        ]
        if of_task:
            return of_task
        await asyncio.sleep(POLL_INTERVAL_S)
    pytest.fail(
        f'no span of task {task_id} after {SPAN_LINE_TIMEOUT_S}s; '
        f'lines: {_span_lines(path)}',
    )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_an_isolated_task_killed_at_its_timeout_gets_a_worker_made_span(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out = tmp_path / 'span_lines.txt'
    monkeypatch.setenv(SPAN_OUT_ENV, str(out))
    test_provider = TracerProvider()
    parent = test_provider.get_tracer('e2e').start_span('e2e parent')
    parent_trace_id = format(parent.get_span_context().trace_id, '032x')

    with run_worker(
        INSTANCE,
        profile='isolated-1',
        timeout=60.0,
        ready_check=_ready_check(instance.healthcheck),
    ):
        token = otel_context.attach(trace.set_span_in_context(parent))
        try:
            handle = unwrap_send(await instance.sleeper.send_async())
        finally:
            otel_context.detach(token)
        assert_err(await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS), 'TASK_TIMEOUT')
        spans = await _wait_for_span(out, str(handle.task_id))
    parent.end()
    test_provider.shutdown()

    assert len(spans) == 1, spans
    span = spans[0]
    assert span['name'] == 'run e2e_worker_ended_spans_sleeper'
    assert span['status'] == 'ERROR'
    assert span['trace_id'] == parent_trace_id
    assert span['parent_span_id'] is not None
    assert span['start'] <= span['end']
    attributes = cast('dict[str, Any]', span['attributes'])
    assert attributes['error.type'] == 'TASK_TIMEOUT'
    assert attributes['horsies.span.source'] == 'worker'
    assert attributes['horsies.task.execution'] == 'ISOLATED'
    assert attributes['horsies.task.attempt'] == 1
