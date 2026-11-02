"""Slot handles for tests that drive the finalize path without a child."""

from __future__ import annotations

import asyncio

from horsies.core.worker.child_protocol import RssUnreadable, RunFrame
from horsies.core.worker.slots import SlotHandle, SlotOutcome, TaskFinished


def _run_frame(task_id: str) -> RunFrame:
    return RunFrame(
        task_id=task_id,
        task_name='slot_handle_test',
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        runner_heartbeat_interval_ms=30_000,
        is_workflow_task=False,
        timeout_ms=None,
        claimed_at=None,
    )


def unbound_handle(task_id: str = 'task-1') -> SlotHandle:
    """Return a handle that holds no child, so its kill sends nothing."""
    return SlotHandle(_run_frame(task_id), asyncio.get_running_loop())


def finished_handle(outcome: SlotOutcome) -> SlotHandle:
    """Return a handle whose slot work is already over."""
    handle = unbound_handle()
    handle.finish(outcome)
    return handle


def task_finished_handle(
    ok: bool,
    result_json: str,
    failed_reason: str | None,
) -> SlotHandle:
    """Return a handle for a task that ran and sent its result."""
    return finished_handle(
        TaskFinished(ok, result_json, failed_reason, RssUnreadable()),
    )
