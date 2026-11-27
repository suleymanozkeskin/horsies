"""E2E: the span of an attempt that the worker ended, through a real worker.

An isolated task outruns its timeout; the worker kills its child, records
`TASK_TIMEOUT`, and sends the `AttemptEnded` event to its handler child,
which makes the span with the tracer provider of its start hook. The task is
sent inside a span of the test process, so the stored carrier puts the
worker-made span in that trace. A shared async task that its child ends at
the deadline has one span, the span of its child, with the timeout.
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
from tests.e2e.helpers.assertions import (
    assert_failed,
    read_succeeded,
    start_ok_in_thread,
    unwrap_send,
)
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_worker_ended_spans as instance
from tests.e2e.tasks.instance_worker_ended_spans import SPAN_OUT_ENV

INSTANCE: Final = 'tests.e2e.tasks.instance_worker_ended_spans:app'
RESULT_TIMEOUT_MS: Final = 30_000
SPAN_LINE_TIMEOUT_S: Final = 30.0
"""The longest wait for the span line: the worker writes an events frame
within its flush interval, and the handler child makes the span at once."""
SECOND_SPAN_WAIT_S: Final = 3.0
"""How long a test waits after the first span line of an attempt for a
second one: past the flush interval of the events (25 ms) and the dispatch
of the handler child, which makes a span at once."""
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
        return read_succeeded(handle.get(wait_ms=2000))

    return check


def _span_lines(path: Path) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding='utf-8')
    except FileNotFoundError:
        return []
    return [cast('dict[str, Any]', json.loads(line)) for line in text.splitlines()]


async def _wait_for_span(path: Path, key: str, value: str) -> list[dict[str, Any]]:
    """The span lines of `path` whose attribute `key` is `value`, once there
    is one."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SPAN_LINE_TIMEOUT_S
    while loop.time() < deadline:
        matching = [
            line for line in _span_lines(path) if line['attributes'].get(key) == value
        ]
        if matching:
            return matching
        await asyncio.sleep(POLL_INTERVAL_S)
    pytest.fail(
        f'no span with {key} = {value} after {SPAN_LINE_TIMEOUT_S}s; '
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
        assert_failed(await handle.get_async(wait_ms=RESULT_TIMEOUT_MS), 'TASK_TIMEOUT')
        spans = await _wait_for_span(out, 'horsies.task.id', str(handle.task_id))
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


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_workflow_task_killed_at_its_timeout_gets_a_span_with_its_node(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The event of the attempt carries WORKFLOW_NOT_KNOWN; the handler child
    reads the node by the task id, so the span has the workflow attributes
    of the span of a child."""
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
            handle = await start_ok_in_thread(instance.spec_sleeper)
        finally:
            otel_context.detach(token)
        workflow_id = str(handle.workflow_id)
        spans = await _wait_for_span(out, 'horsies.workflow.id', workflow_id)
    parent.end()
    test_provider.shutdown()

    assert len(spans) == 1, spans
    span = spans[0]
    assert span['name'] == 'run e2e_worker_ended_spans_sleeper'
    assert span['status'] == 'ERROR'
    assert span['trace_id'] == parent_trace_id
    attributes = cast('dict[str, Any]', span['attributes'])
    assert attributes['error.type'] == 'TASK_TIMEOUT'
    assert attributes['horsies.span.source'] == 'worker'
    assert attributes['horsies.workflow.node'] == 0
    assert attributes['horsies.workflow.root_id'] == workflow_id


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_shared_task_that_its_child_ended_at_its_deadline_has_one_span(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The child of the shared run reports the timeout in its result frame
    and lives: the span of the child is the span of the attempt, with the
    timeout, and the handler child makes none."""
    out = tmp_path / 'span_lines.txt'
    monkeypatch.setenv(SPAN_OUT_ENV, str(out))
    test_provider = TracerProvider()
    parent = test_provider.get_tracer('e2e').start_span('e2e parent')
    parent_trace_id = format(parent.get_span_context().trace_id, '032x')

    with run_worker(
        INSTANCE,
        profile='split-isolated-1-shared-1x4',
        timeout=60.0,
        ready_check=_ready_check(instance.healthcheck),
    ):
        token = otel_context.attach(trace.set_span_in_context(parent))
        try:
            handle = unwrap_send(await instance.shared_sleeper.send_async())
        finally:
            otel_context.detach(token)
        assert_failed(await handle.get_async(wait_ms=RESULT_TIMEOUT_MS), 'TASK_TIMEOUT')
        _ = await _wait_for_span(out, 'horsies.task.id', str(handle.task_id))
        await asyncio.sleep(SECOND_SPAN_WAIT_S)
        spans = [
            line
            for line in _span_lines(out)
            if line['attributes'].get('horsies.task.id') == str(handle.task_id)
        ]
    parent.end()
    test_provider.shutdown()

    assert len(spans) == 1, spans
    span = spans[0]
    assert span['name'] == 'run e2e_worker_ended_spans_shared_sleeper'
    assert span['status'] == 'ERROR'
    assert span['trace_id'] == parent_trace_id
    attributes = cast('dict[str, Any]', span['attributes'])
    assert attributes['error.type'] == 'TASK_TIMEOUT'
    assert 'horsies.span.source' not in attributes
    assert attributes['horsies.task.execution'] == 'SHARED_ASYNC'
