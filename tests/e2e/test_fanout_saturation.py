"""
Demonstrate the get_result_async() fan-out on the task_done channel.

For each task completion NOTIFY, the listener dispatcher distributes
to ALL subscriber queues — not just the one waiting for that task_id.

Tests exercise the raw listener layer to prove the O(N) fan-out per
notification, bypassing the session pool bottleneck in get_result_async.

No worker process needed — tasks are completed via direct SQL UPDATE
which fires the horsies_notify_task_changes() trigger.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import Callable
from typing import Any

import pytest
from psycopg import Notify
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.listener import PostgresListener


async def _listen_unwrap(
    listener: PostgresListener,
    channel: str,
) -> asyncio.Queue[Notify]:
    """Call listener.listen() and unwrap, failing the test on Err."""
    result = await listener.listen(channel)
    assert result.is_ok(), f'listener.listen({channel!r}) failed: {result.err_value}'
    return result.ok_value


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 5.0,
    interval_s: float = 0.02,
) -> bool:
    """Poll a predicate until it becomes true or timeout expires."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval_s)
    return predicate()


@pytest.mark.e2e
class TestTaskDoneFanout:

    @pytest.mark.asyncio(loop_scope='session')
    async def test_subscriber_count_matches_listen_calls(
        self,
        broker: PostgresBroker,
    ) -> None:
        """Each listener.listen() call adds an independent subscriber queue."""
        N = 100

        queues: list[asyncio.Queue[Notify]] = []
        for _ in range(N):
            q = await _listen_unwrap(broker.listener, 'task_done')
            queues.append(q)

        try:
            unique_count = len({id(q) for q in queues})
            print(f'\n[fanout] {N} listen() calls -> {unique_count} unique queue objects')
            assert unique_count == N, (
                f'Expected {N} unique queue objects, got {unique_count}'
            )
        finally:
            for q in queues:
                await broker.listener.unsubscribe('task_done', q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_single_notify_fans_out_to_all_subscriber_queues(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        1 NOTIFY on task_done is delivered to ALL N subscriber queues.

        Uses raw listener queues (no consumer draining them) so we can
        inspect qsize() after the notification propagates.
        """
        N = 30

        # Subscribe N raw queues — no consumer reads from them
        queues: list[asyncio.Queue[Notify]] = []
        for _ in range(N):
            q = await _listen_unwrap(broker.listener, 'task_done')
            queues.append(q)

        # Enqueue one task so the UPDATE trigger has a row to fire on
        task_id = (await broker.enqueue_async(
            task_name='e2e_simple',
            args=(0,),
            kwargs={},
            queue_name='default',
        )).unwrap()

        # Complete it via direct SQL (fires NOTIFY task_done, <task_id>)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'COMPLETED',
                    result = '{"ok": 0}',
                    updated_at = NOW()
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.commit()

        try:
            delivered_to_all = await _wait_until(
                lambda: all(q.qsize() > 0 for q in queues),
                timeout_s=5.0,
            )
            sizes = [q.qsize() for q in queues]
            total_delivered = sum(sizes)
            queues_that_received = sum(1 for s in sizes if s > 0)

            print(f'\n[fanout] 1 NOTIFY -> delivered to {queues_that_received}/{N} queues')
            print(f'[fanout] Total items across all queues: {total_delivered}')

            assert delivered_to_all and queues_that_received == N, (
                f'Expected all {N} queues to receive the notification, '
                f'but only {queues_that_received} did'
            )
        finally:
            for q in queues:
                await broker.listener.unsubscribe('task_done', q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_wasted_dispatches_per_completion(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        With N subscriber queues, completing 1 task delivers N notifications
        but only 1 is useful. Count total put_nowait calls to prove O(N).
        """
        N = 50

        queues: list[asyncio.Queue[Notify]] = []
        for _ in range(N):
            q = await _listen_unwrap(broker.listener, 'task_done')
            queues.append(q)

        task_id = (await broker.enqueue_async(
            task_name='e2e_simple',
            args=(0,),
            kwargs={},
            queue_name='default',
        )).unwrap()

        # Patch put_nowait on every queue to count deliveries
        delivery_count = 0
        originals: dict[int, Any] = {}

        for q in queues:
            originals[id(q)] = q.put_nowait

            def _make_counter(
                orig: Any,
            ) -> Any:
                def counted_put_nowait(item: Notify) -> None:
                    nonlocal delivery_count
                    delivery_count += 1
                    orig(item)

                return counted_put_nowait

            q.put_nowait = _make_counter(q.put_nowait)  # type: ignore[assignment]

        # Fire 1 completion
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'COMPLETED',
                    result = '{"ok": 0}',
                    updated_at = NOW()
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.commit()
        got_all_dispatches = await _wait_until(
            lambda: delivery_count >= N,
            timeout_s=5.0,
        )

        try:
            print(f'\n[fanout] 1 completion with {N} subscribers -> {delivery_count} put_nowait calls')
            print(f'[fanout] Useful: 1, Wasted: {delivery_count - 1}')

            assert got_all_dispatches and delivery_count == N, (
                f'Expected {N} put_nowait calls (1 per subscriber), got {delivery_count}'
            )
        finally:
            for q in queues:
                q.put_nowait = originals[id(q)]  # type: ignore[assignment]
                await broker.listener.unsubscribe('task_done', q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_unconsumed_notifications_accumulate_memory(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        Subscriber queues are unbounded. Notifications that no consumer
        reads accumulate indefinitely, growing memory proportional to
        N_subscribers * M_completions.

        Simulates idle subscribers (e.g. slow get_result callers) that
        never drain their queues while completions keep firing.
        """
        N_SUBSCRIBERS = 200
        M_COMPLETIONS = 100

        # Subscribe N queues — none will be consumed
        queues: list[asyncio.Queue[Notify]] = []
        for _ in range(N_SUBSCRIBERS):
            q = await _listen_unwrap(broker.listener, 'task_done')
            queues.append(q)

        # Measure baseline: memory of the empty queues
        baseline_bytes = sum(sys.getsizeof(q) for q in queues)
        # Also measure a single Notify object so we can estimate
        sample_notify_size = 0

        # Enqueue M tasks
        task_ids: list[str] = []
        for i in range(M_COMPLETIONS):
            tid = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(i,),
                kwargs={},
                queue_name='default',
            )).unwrap()
            task_ids.append(tid)

        # Complete all M tasks (each fires NOTIFY to all N queues)
        for tid in task_ids:
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 1}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': tid},
            )
            await session.commit()

        # Wait for all notifications to propagate
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            min_qsize = min(q.qsize() for q in queues)
            if min_qsize >= M_COMPLETIONS:
                break
            await asyncio.sleep(0.05)

        # Measure accumulated items
        total_items = sum(q.qsize() for q in queues)
        expected_items = N_SUBSCRIBERS * M_COMPLETIONS

        # Measure memory of the queued Notify objects
        # Sample one item to get its size
        sample_q = queues[0]
        if sample_q.qsize() > 0:
            sample_item = await sample_q.get()
            sample_notify_size = sys.getsizeof(sample_item)
            sample_q.put_nowait(sample_item)  # put it back

        # Estimated total memory held by unconsumed notifications
        estimated_bytes = total_items * sample_notify_size if sample_notify_size else 0

        print(f'\n[memory] {N_SUBSCRIBERS} subscribers x {M_COMPLETIONS} completions')
        print(f'[memory] Total queued items: {total_items:,} (expected {expected_items:,})')
        print(f'[memory] Notify object size: {sample_notify_size} bytes')
        print(f'[memory] Estimated memory held in queues: {estimated_bytes:,} bytes ({estimated_bytes / 1024:.1f} KB)')
        print(f'[memory] Queue container baseline: {baseline_bytes:,} bytes')
        print(f'[memory] At 1000 subscribers x 1000 completions: ~{1000 * 1000 * sample_notify_size / (1024 * 1024):.1f} MB')

        try:
            # Use >= because stale task_done notifications from earlier tests
            # in the same session can leak into these queues, which itself
            # demonstrates the accumulation problem.
            assert total_items >= expected_items, (
                f'Expected at least {expected_items} total queued items, got {total_items}'
            )
        finally:
            for q in queues:
                await broker.listener.unsubscribe('task_done', q)
            await session.execute(
                text("DELETE FROM horsies_tasks WHERE id = ANY(:ids)"),
                {'ids': task_ids},
            )
            await session.commit()

    @pytest.mark.asyncio(loop_scope='session')
    async def test_fanout_overhead_scales_quadratically(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        Measure dispatcher throughput as subscriber count grows.

        With N subscribers and M completions, the dispatcher does N*M
        put_nowait calls. This test fires M completions at different N
        values and measures wall-clock time.
        """
        M = 20  # completions per round
        results_by_n: list[tuple[int, float, int]] = []

        for n in (10, 50, 200):
            queues: list[asyncio.Queue[Notify]] = []
            for _ in range(n):
                q = await _listen_unwrap(broker.listener, 'task_done')
                queues.append(q)

            # Enqueue M tasks
            task_ids: list[str] = []
            for i in range(M):
                tid = (await broker.enqueue_async(
                    task_name='e2e_simple',
                    args=(i,),
                    kwargs={},
                    queue_name='default',
                )).unwrap()
                task_ids.append(tid)

            # Complete all M tasks, measure time for all notifications to arrive
            t0 = time.monotonic()
            for tid in task_ids:
                await session.execute(
                    text("""
                        UPDATE horsies_tasks
                        SET status = 'COMPLETED',
                            result = '{"ok": 1}',
                            updated_at = NOW()
                        WHERE id = :tid
                    """),
                    {'tid': tid},
                )
                await session.commit()

            # Wait for all queues to have received all M notifications
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                min_qsize = min(q.qsize() for q in queues)
                if min_qsize >= M:
                    break
                await asyncio.sleep(0.05)

            elapsed = time.monotonic() - t0
            total_dispatches = n * M

            results_by_n.append((n, elapsed, total_dispatches))
            print(
                f'\n[fanout] N={n} subscribers, M={M} completions: '
                f'{elapsed:.3f}s, {total_dispatches} total dispatches'
            )

            try:
                # Verify every queue got every notification
                for idx, q in enumerate(queues):
                    assert q.qsize() >= M, (
                        f'Queue {idx} expected {M} notifications, got {q.qsize()}'
                    )
            finally:
                for q in queues:
                    await broker.listener.unsubscribe('task_done', q)
                await session.execute(
                    text("DELETE FROM horsies_tasks WHERE id = ANY(:ids)"),
                    {'ids': task_ids},
                )
                await session.commit()

        # Summary
        print('\n[fanout] Scaling summary (M={} completions per round):'.format(M))
        for n, elapsed, total in results_by_n:
            rate = total / elapsed if elapsed > 0 else 0
            print(
                f'  N={n:>3} subscribers: {elapsed:.3f}s, '
                f'{total:>5} dispatches, {rate:,.0f} dispatches/s'
            )


@pytest.mark.e2e
class TestFanoutBoundary:
    """Boundary and edge-case tests for fan-out dispatcher."""

    @pytest.mark.asyncio(loop_scope='session')
    async def test_zero_subscribers_notify_does_not_crash(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """NOTIFY with 0 subscribers does not break later delivery on that channel."""
        # Ensure task_done channel has active LISTEN but no subscriber queues.
        # listen() then immediately unsubscribe to set up the LISTEN without subs.
        q = await _listen_unwrap(broker.listener, 'task_done')
        await broker.listener.unsubscribe('task_done', q)

        # Fire a completion with zero subscribers; should be a safe no-op.
        task_id = (await broker.enqueue_async(
            task_name='e2e_simple',
            args=(0,),
            kwargs={},
            queue_name='default',
        )).unwrap()
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET status = 'COMPLETED',
                    result = '{"ok": 0}',
                    updated_at = NOW()
                WHERE id = :tid
            """),
            {'tid': task_id},
        )
        await session.commit()

        # Verify delivery still works by subscribing and firing a new completion.
        probe_q = await _listen_unwrap(broker.listener, 'task_done')
        try:
            task_id_2 = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(1,),
                kwargs={},
                queue_name='default',
            )).unwrap()
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 1}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': task_id_2},
            )
            await session.commit()
            got_probe = await _wait_until(lambda: probe_q.qsize() >= 1, timeout_s=3.0)
            assert got_probe, 'Expected delivery after zero-subscriber completion'
        finally:
            await broker.listener.unsubscribe('task_done', probe_q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_single_subscriber_receives_notification(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """Degenerate case: 1 subscriber, 1 NOTIFY, queue receives exactly 1 item."""
        q = await _listen_unwrap(broker.listener, 'task_done')
        try:
            task_id = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(0,),
                kwargs={},
                queue_name='default',
            )).unwrap()
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 0}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': task_id},
            )
            await session.commit()
            got_notification = await _wait_until(
                lambda: q.qsize() >= 1,
                timeout_s=3.0,
            )

            assert got_notification and q.qsize() >= 1, (
                f'Single subscriber expected at least 1 notification, got {q.qsize()}'
            )
        finally:
            await broker.listener.unsubscribe('task_done', q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_unsubscribe_during_active_notifications(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        Unsubscribe some queues while notifications are in-flight.

        The list() snapshot in _dispatcher prevents iteration errors.
        Queues unsubscribed mid-burst stop receiving new notifications.
        """
        N = 10
        queues: list[asyncio.Queue[Notify]] = []
        for _ in range(N):
            q = await _listen_unwrap(broker.listener, 'task_done')
            queues.append(q)

        removed = queues[:N // 2]
        remaining = queues[N // 2:]

        pre_ids: list[str] = []
        for i in range(20):
            tid = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(i,),
                kwargs={},
                queue_name='default',
            )).unwrap()
            pre_ids.append(tid)

        post_id = (await broker.enqueue_async(
            task_name='e2e_simple',
            args=(99,),
            kwargs={},
            queue_name='default',
        )).unwrap()

        async def _complete_inflight(ids: list[str]) -> None:
            for tid in ids:
                await session.execute(
                    text("""
                        UPDATE horsies_tasks
                        SET status = 'COMPLETED',
                            result = '{"ok": 0}',
                            updated_at = NOW()
                        WHERE id = :tid
                    """),
                    {'tid': tid},
                )
                await session.commit()
                # Keep the stream active so unsubscribe happens mid-burst.
                await asyncio.sleep(0.01)

        try:
            inflight = asyncio.create_task(_complete_inflight(pre_ids))

            started = await _wait_until(
                lambda: any(q.qsize() > 0 for q in queues),
                timeout_s=3.0,
            )
            assert started, 'Expected notifications to start before unsubscribe'

            for q in removed:
                await broker.listener.unsubscribe('task_done', q)

            await inflight
            removed_sizes_after_unsub = [q.qsize() for q in removed]
            remaining_sizes_before_post = [q.qsize() for q in remaining]

            # Fire one more completion after unsubscribe and verify only remaining queues grow.
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 0}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': post_id},
            )
            await session.commit()

            remaining_grew = await _wait_until(
                lambda: all(
                    q.qsize() >= before + 1
                    for q, before in zip(remaining, remaining_sizes_before_post)
                ),
                timeout_s=3.0,
            )
            assert remaining_grew, 'Remaining queues did not receive post-unsubscribe completion'

            removed_sizes_after_post = [q.qsize() for q in removed]
            assert removed_sizes_after_post == removed_sizes_after_unsub, (
                'Removed queues received new notifications after unsubscribe: '
                f'before={removed_sizes_after_unsub}, after={removed_sizes_after_post}'
            )

            for idx, q in enumerate(remaining):
                assert q.qsize() >= 1, (
                    f'Remaining queue {idx} expected at least 1 notification, got {q.qsize()}'
                )
        finally:
            for q in queues:
                await broker.listener.unsubscribe('task_done', q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_double_unsubscribe_is_safe_noop(
        self,
        broker: PostgresBroker,
    ) -> None:
        """Calling unsubscribe() twice with the same queue does not raise."""
        q = await _listen_unwrap(broker.listener, 'task_done')

        # First unsubscribe — removes the queue
        await broker.listener.unsubscribe('task_done', q)

        # Second unsubscribe — should be a safe no-op (uses set.discard internally)
        await broker.listener.unsubscribe('task_done', q)

        # No exception means success


@pytest.mark.e2e
class TestFanoutErrorPaths:
    """Error and fault-path tests for fan-out dispatcher."""

    @pytest.mark.asyncio(loop_scope='session')
    async def test_unsubscribe_unknown_queue_is_safe(
        self,
        broker: PostgresBroker,
    ) -> None:
        """Calling unsubscribe with a queue never returned by listen() does not raise."""
        random_queue: asyncio.Queue[Notify] = asyncio.Queue()

        # Should not raise — discard on a set is a no-op for missing items
        await broker.listener.unsubscribe('task_done', random_queue)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_unlisten_removes_server_side_listen(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        unlisten() removes the local subscription and channel can be cleanly re-listened.
        """
        channel = 'test_unlisten_channel'
        q = await _listen_unwrap(broker.listener, channel)

        try:
            await broker.listener.unlisten(channel, q)

            # Re-listen and verify notifications flow again.
            q2 = await _listen_unwrap(broker.listener, channel)
            try:
                await session.execute(
                    text("SELECT pg_notify(:channel, 'ping')"),
                    {'channel': channel},
                )
                await session.commit()
                got_ping = await _wait_until(lambda: q2.qsize() >= 1, timeout_s=3.0)
                assert got_ping, f'Expected notification on re-listened channel {channel!r}'
            finally:
                await broker.listener.unlisten(channel, q2)
        finally:
            await broker.listener.unlisten(channel, q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_bounded_queue_drops_on_full(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """
        Dispatcher's QueueFull handler silently drops excess notifications.

        A bounded queue (maxsize=1) inserted into _subs should hold at most
        1 item after multiple notifications fire.
        """
        bounded_q: asyncio.Queue[Notify] = asyncio.Queue(maxsize=1)
        control_q = await _listen_unwrap(broker.listener, 'task_done')

        # Manually insert the bounded queue into _subs.
        # This is intentional: listen() always returns unbounded queues.
        async with broker.listener._lock:
            broker.listener._subs['task_done'].add(bounded_q)

        try:
            # Fire 3 notifications to exceed maxsize
            task_ids: list[str] = []
            for i in range(3):
                tid = (await broker.enqueue_async(
                    task_name='e2e_simple',
                    args=(i,),
                    kwargs={},
                    queue_name='default',
                )).unwrap()
                task_ids.append(tid)

            for tid in task_ids:
                await session.execute(
                    text("""
                        UPDATE horsies_tasks
                        SET status = 'COMPLETED',
                            result = '{"ok": 1}',
                            updated_at = NOW()
                        WHERE id = :tid
                    """),
                    {'tid': tid},
                )
                await session.commit()

            control_received = await _wait_until(
                lambda: control_q.qsize() >= 3,
                timeout_s=3.0,
            )
            assert control_received, (
                f'Control queue expected 3 notifications, got {control_q.qsize()}'
            )

            # Bounded queue should hold exactly maxsize items (extras dropped)
            assert bounded_q.qsize() == 1, (
                f'Bounded queue expected exactly 1 item, got {bounded_q.qsize()}'
            )
        finally:
            async with broker.listener._lock:
                broker.listener._subs['task_done'].discard(bounded_q)
            await broker.listener.unsubscribe('task_done', control_q)


@pytest.mark.e2e
class TestFanoutContracts:
    """Contract and invariant tests for fan-out dispatcher."""

    @pytest.mark.asyncio(loop_scope='session')
    async def test_notification_payload_contains_task_id(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """The Notify.payload on subscriber queues contains the completed task_id."""
        q = await _listen_unwrap(broker.listener, 'task_done')
        try:
            task_id = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(0,),
                kwargs={},
                queue_name='default',
            )).unwrap()
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 0}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': task_id},
            )
            await session.commit()
            got_notification = await _wait_until(
                lambda: q.qsize() >= 1,
                timeout_s=3.0,
            )

            assert got_notification and q.qsize() >= 1, 'Expected at least 1 notification'

            notification = await q.get()
            assert notification.payload == task_id, (
                f'Expected payload {task_id!r}, got {notification.payload!r}'
            )
        finally:
            await broker.listener.unsubscribe('task_done', q)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_channel_isolation_no_cross_delivery(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """NOTIFY on task_done must NOT deliver to task_new subscribers."""
        q_done = await _listen_unwrap(broker.listener, 'task_done')
        q_new = await _listen_unwrap(broker.listener, 'task_new')

        try:
            task_id = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(0,),
                kwargs={},
                queue_name='default',
            )).unwrap()

            # Drain any task_new notifications from the enqueue itself
            await _wait_until(lambda: q_new.qsize() >= 1, timeout_s=2.0)
            while not q_new.empty():
                q_new.get_nowait()

            # Complete the task — fires NOTIFY on task_done only
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 0}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': task_id},
            )
            await session.commit()

            got_done = await _wait_until(lambda: q_done.qsize() >= 1, timeout_s=3.0)

            # task_done queue should have received the notification
            assert got_done and q_done.qsize() >= 1, (
                f'task_done queue expected at least 1 notification, got {q_done.qsize()}'
            )

            # task_new queue should NOT have received any completion notification
            assert q_new.qsize() == 0, (
                f'task_new queue should be empty after completion, got {q_new.qsize()}'
            )
        finally:
            await broker.listener.unsubscribe('task_done', q_done)
            await broker.listener.unsubscribe('task_new', q_new)

    @pytest.mark.asyncio(loop_scope='session')
    async def test_consuming_one_queue_does_not_affect_others(
        self,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        """After fan-out, get() from one queue does not drain or alter items in others."""
        N = 3
        queues: list[asyncio.Queue[Notify]] = []
        for _ in range(N):
            q = await _listen_unwrap(broker.listener, 'task_done')
            queues.append(q)

        try:
            task_id = (await broker.enqueue_async(
                task_name='e2e_simple',
                args=(0,),
                kwargs={},
                queue_name='default',
            )).unwrap()
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'COMPLETED',
                        result = '{"ok": 0}',
                        updated_at = NOW()
                    WHERE id = :tid
                """),
                {'tid': task_id},
            )
            await session.commit()
            got_all = await _wait_until(
                lambda: all(q.qsize() >= 1 for q in queues),
                timeout_s=3.0,
            )
            assert got_all, 'Expected all queues to receive at least one notification'

            # Consume from the first queue
            assert queues[0].qsize() >= 1, 'Queue 0 expected at least 1 notification'
            _ = await queues[0].get()

            # Other queues should still have their items intact
            for idx in range(1, N):
                assert queues[idx].qsize() >= 1, (
                    f'Queue {idx} expected at least 1 notification after consuming '
                    f'from queue 0, got {queues[idx].qsize()}'
                )
        finally:
            for q in queues:
                await broker.listener.unsubscribe('task_done', q)
