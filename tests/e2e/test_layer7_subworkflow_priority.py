"""Layer 7 e2e: subworkflow child priority binding drives real claim order.

The priority-binding fix makes a subworkflow child inherit its queue's
configured priority instead of a hardcoded 100. This exercises the behavioral
consequence end-to-end: a subworkflow child (built via a direct WorkflowSpec)
and a direct-send task share the single-slot 'serial' queue (priority 30). With
both queued behind a blocker, a real worker claims them in (priority, FIFO)
order.

Discriminators vs the pre-fix bug:
- persisted priority: the child binds to 30, not 100 (the robust, timing-free
  assertion — pre-fix the child row would carry 100);
- claim order: head-to-head behind the blocker, the child (enqueued first at
  priority 30) is claimed before the direct task (also 30) by FIFO tiebreak;
  pre-fix the child at 100 would lose the slot to the direct task at 30.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err

from tests.e2e.helpers.assertions import start_ok_sync, unwrap_send
from tests.e2e.helpers.db import wait_for_all_terminal, wait_for_status
from tests.e2e.tasks import priority_binding as pb
from tests.e2e.tasks.instance_priority_binding import app  # noqa: F401

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope='function'),
]

PB_INSTANCE = 'tests.e2e.tasks.instance_priority_binding:app'
SERIAL_PRIORITY = 30


def _ready_check():
    from tests.e2e.helpers.worker import ReadyCheck  # noqa: F401

    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            result = pb.ready_task.send()
            if is_err(result):
                return False
            handle = result.ok_value
        return handle.get(timeout_ms=2000).is_ok()

    return _check


async def _wait_for_task_row(
    broker: PostgresBroker,
    task_name: str,
    *,
    timeout_s: float = 15.0,
) -> str:
    """Poll until a task row with the given name exists; return its id."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        async with broker.session_factory() as session:
            row = (
                await session.execute(
                    text('SELECT id FROM horsies_tasks WHERE task_name = :n LIMIT 1'),
                    {'n': task_name},
                )
            ).fetchone()
            if row is not None:
                return str(row[0])
        await asyncio.sleep(0.1)
    raise TimeoutError(f'task row {task_name!r} did not appear within {timeout_s}s')


async def test_subworkflow_child_binds_queue_priority_and_claim_order(
    priority_binding_broker: PostgresBroker,
) -> None:
    broker = priority_binding_broker
    from tests.e2e.helpers.worker import run_worker

    with run_worker(
        PB_INSTANCE,
        processes=4,
        ready_check=_ready_check(),
    ):
        # 1. Occupy the single serial slot so scrape + direct queue behind it.
        blocker_id = broker.enqueue(
            task_name='pb_blocker',
            queue_name='serial',
            task_id=str(uuid4()),
            enqueue_sha=f'pb-blocker-{uuid4().hex[:8]}',
            kwargs_json=dumps_json({'duration_ms': 6000}).unwrap(),
        ).unwrap()
        await wait_for_status(
            broker.session_factory, blocker_id, 'RUNNING', timeout_s=15.0,
        )

        # 2. Start the parent subworkflow; the worker expands it via the child's
        #    direct build_with, enqueuing pb_scrape on serial (behind blocker).
        start_ok_sync(pb.spec_priority_parent)
        scrape_id = await _wait_for_task_row(broker, 'pb_scrape')

        # 3. Direct-send a task on serial AFTER scrape exists (later enqueued_at).
        #    .send() resolves priority through the queue config (→ 30).
        direct_handle = unwrap_send(await pb.direct_task.send_async())
        direct_id = direct_handle.task_id

        # 4. Drain: blocker finishes, worker claims scrape then direct.
        await wait_for_all_terminal(
            broker.session_factory,
            [blocker_id, scrape_id, direct_id],
            timeout_s=30.0,
        )

    async with broker.session_factory() as session:
        rows = (
            await session.execute(
                text("""
                    SELECT task_name, priority, queue_name, status, claimed_at
                    FROM horsies_tasks
                    WHERE task_name IN ('pb_scrape', 'pb_direct')
                """),
            )
        ).fetchall()

    by_name = {r.task_name: r for r in rows}
    scrape = by_name['pb_scrape']
    direct = by_name['pb_direct']

    # (a) Both bound to the serial queue's configured priority — pre-fix the
    #     subworkflow child would carry 100. This is timing-independent.
    assert scrape.queue_name == 'serial'
    assert scrape.priority == SERIAL_PRIORITY, scrape.priority
    assert direct.priority == SERIAL_PRIORITY, direct.priority

    # (b) Both completed, and the child (enqueued first, same priority) was
    #     claimed before the direct task — FIFO preserved within the queue.
    assert scrape.status == 'COMPLETED'
    assert direct.status == 'COMPLETED'
    assert scrape.claimed_at is not None and direct.claimed_at is not None
    assert scrape.claimed_at <= direct.claimed_at, (
        f'scrape claimed {scrape.claimed_at}, direct claimed {direct.claimed_at}'
    )
