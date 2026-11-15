"""E2E: the finalizing marker of an isolated async task that leaves work.

The marker is the last write of a run: the child writes it only when the
body and the work that it left on the task loop have ended. Until then the
worker beats the row, so the reaper does not fail it, also with the
smallest stale thresholds.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from collections.abc import Callable
from datetime import timedelta

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err

from tests.broker_resources_support import current_resources
from tests.e2e.helpers.assertions import unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import instance_left_work_marker as instance

LEFT_WORK_INSTANCE = 'tests.e2e.tasks.instance_left_work_marker:app'
LEFT_WORK_SECONDS = 8.0
"""How long the left work holds the loop: past running_stale_threshold_ms
(6 s) and finalizing_stale_threshold_ms (2 s) of the instance, and within
the parent backstop after a return."""
POLL_INTERVAL_S = 0.1
OUTCOME_TIMEOUT_S = 60.0
TERMINAL = ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')


def _make_ready_check() -> Callable[[], bool]:
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            sent = instance.healthcheck.send()
            if is_err(sent):
                return False
            handle = sent.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return _check


def _work_marker_state(path: str) -> str:
    try:
        with open(path, encoding='ascii') as marker:
            return marker.read()
    except FileNotFoundError:
        return 'absent'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_left_work_past_the_stale_thresholds_keeps_its_row_beaten(
    left_work_marker_broker: PostgresBroker,
) -> None:
    """The work that the task left holds the loop for 8 s. The row gets no
    finalizing marker before that work ended, the worker beats it the whole
    time, and the reaper leaves it: it completes with what the task
    returned, with no retry."""
    work_marker = os.path.join(
        tempfile.gettempdir(), 'horsies-e2e-left-work-beats.marker'
    )
    try:
        os.remove(work_marker)
    except FileNotFoundError:
        pass
    session_factory = current_resources(left_work_marker_broker).session_factory
    marked_before_the_work_ended: list[str] = []

    with run_worker(
        LEFT_WORK_INSTANCE,
        profile='isolated-1',
        ready_check=_make_ready_check(),
    ):
        handle = unwrap_send(
            await instance.left_work_holds_the_loop.send_async(
                marker_path=work_marker,
                seconds=LEFT_WORK_SECONDS,
            ),
        )
        deadline = time.monotonic() + OUTCOME_TIMEOUT_S
        while True:
            assert time.monotonic() < deadline, 'the task did not end'
            async with session_factory() as session:
                row = (
                    await session.execute(
                        text("""
                            SELECT status, finalizing_at
                            FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                        """),
                        {'id': handle.task_id},
                    )
                ).fetchone()
            if row is not None and row.finalizing_at is not None:
                state = _work_marker_state(work_marker)
                if state != 'ended':
                    marked_before_the_work_ended.append(state)
            if row is not None and row.status in TERMINAL:
                break
            await asyncio.sleep(POLL_INTERVAL_S)

    async with session_factory() as session:
        final = (
            await session.execute(
                text("""
                    SELECT t.status, t.error_code, t.retry_count, t.result,
                           t.started_at,
                           (SELECT max(h.sent_at) FROM horsies_heartbeats h
                            WHERE h.task_id = t.id AND h.role = 'runner')
                               AS last_beat
                    FROM itest_task_rows t WHERE t.id = CAST(:id AS uuid)
                """),
                {'id': handle.task_id},
            )
        ).fetchone()

    assert marked_before_the_work_ended == []
    assert _work_marker_state(work_marker) == 'ended'
    assert final is not None
    assert (final.status, final.error_code, final.retry_count) == ('COMPLETED', None, 0)
    assert final.result is not None and 'returned:' in final.result
    assert final.last_beat is not None
    assert final.last_beat - final.started_at >= timedelta(
        seconds=LEFT_WORK_SECONDS - 2
    )
