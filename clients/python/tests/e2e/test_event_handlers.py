"""E2E: `@app.on_event` through a real worker.

The worker of the instance starts its handler child, which runs the
`on_handler_child_start` hook once, then gets the `AttemptEnded` event of
each attempt of the worker and calls the handler.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from pathlib import Path
from typing import Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskFunction, TaskHandle
from horsies.core.types.result import is_err
from tests.e2e.helpers.assertions import assert_ok, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_event_handlers as instance
from tests.e2e.tasks.instance_event_handlers import HANDLER_OUT_ENV

INSTANCE: Final = 'tests.e2e.tasks.instance_event_handlers:app'
RESULT_TIMEOUT_MS: Final = 30_000
HANDLER_LINE_TIMEOUT_S: Final = 30.0
"""The longest wait for the line of the handler: the worker writes an
events frame within its flush interval, and the handler child takes it at
once."""
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


def _lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding='utf-8').splitlines()
    except FileNotFoundError:
        return []


async def _wait_for_line(path: Path, prefix: str) -> list[str]:
    """The lines of `path` once one starts with `prefix`."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + HANDLER_LINE_TIMEOUT_S
    while loop.time() < deadline:
        lines = _lines(path)
        if any(line.startswith(prefix) for line in lines):
            return lines
        await asyncio.sleep(POLL_INTERVAL_S)
    pytest.fail(
        f'no handler line starts with {prefix!r} after {HANDLER_LINE_TIMEOUT_S}s; '
        f'lines: {_lines(path)}',
    )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_a_handler_gets_the_attempt_ended_event_of_a_task(
    broker: PostgresBroker,  # noqa: ARG001 - binds the e2e database first
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out = tmp_path / 'handler_lines.txt'
    monkeypatch.setenv(HANDLER_OUT_ENV, str(out))

    with run_worker(
        INSTANCE,
        profile='isolated-1',
        timeout=60.0,
        ready_check=_ready_check(instance.healthcheck),
    ):
        handle = unwrap_send(await instance.add.send_async(left=2, right=3))
        assert_ok(await handle.get_async(timeout_ms=RESULT_TIMEOUT_MS), 5)
        lines = await _wait_for_line(out, str(handle.task_id))

    started = [line for line in lines if line.startswith('started ')]
    assert len(started) == 1, lines
    assert lines.index(started[0]) == 0, lines
    assert int(started[0].split()[1]) != os.getpid()
    assert f'{handle.task_id} 1 AttemptEndReportedByChild' in lines
