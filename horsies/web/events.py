"""LISTEN-to-SSE event layer.

The monitoring channels carry bare row ids: an event says *something about
this entity changed*, never what it changed to. Clients treat every event as
a cache invalidation and re-read through the normal API.

Raw notifies are batched per topic on a debounce window so a burst of a
thousand status changes costs each connected browser one invalidation rather
than a thousand. Past a per-window id cap the batch degrades to "invalidate
the whole topic", which is cheaper than shipping an unbounded id list.

This app owns its listener rather than borrowing the broker's: the broker's
listener carries result-waiter subscriptions, and a monitoring stream must
not be able to disturb them.
"""

from __future__ import annotations

import asyncio
from asyncio import Queue
from dataclasses import dataclass, field

from psycopg import Notify

from horsies.core.brokers.listener import PostgresListener
from horsies.core.logging import get_logger
from horsies.core.types.result import is_err

logger = get_logger('web')

# Monitoring channel -> the client-facing topic it invalidates. Node-level and
# attempt changes have no trigger of their own, but every one of them
# coincides with a backing-task insert or status change, so the task channel
# already covers them.
CHANNEL_TOPICS: dict[str, str] = {
    'horsies_task_status': 'tasks',
    'horsies_workflow_status': 'workflows',
    'horsies_worker_state': 'workers',
}

TOPIC_DEGRADED = 'degraded'

# Ids carried per topic per window. Beyond this the event carries no ids at
# all, which clients read as "invalidate everything under this topic".
MAX_IDS_PER_EVENT = 100

# Batching window. Long enough to collapse a burst, short enough that a UI
# reacting to an operator's own action still feels immediate.
DEBOUNCE_SECONDS = 0.25

# Comment frames keep proxies from dropping an idle stream.
HEARTBEAT_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class TopicEvent:
    """One coalesced invalidation for a topic.

    An empty ``ids`` means the window overflowed: invalidate the whole topic.
    """

    topic: str
    ids: list[str]


@dataclass
class _TopicWindow:
    """Ids accumulated for one topic in the current window."""

    ids: list[str] = field(default_factory=list[str])
    seen: set[str] = field(default_factory=set[str])
    overflowed: bool = False


class EventCoalescer:
    """Collapses raw notifies into at most one event per topic per window."""

    def __init__(self, *, max_ids: int = MAX_IDS_PER_EVENT) -> None:
        self._max_ids = max_ids
        self._windows: dict[str, _TopicWindow] = {}

    def record(self, topic: str, entity_id: str) -> None:
        """Note that ``entity_id`` changed under ``topic``.

        Duplicates within a window are dropped. Once a window overflows its
        id cap the accumulated ids are released: the event will carry none.
        """
        window = self._windows.setdefault(topic, _TopicWindow())
        if window.overflowed or entity_id in window.seen:
            return
        window.seen.add(entity_id)
        window.ids.append(entity_id)
        if len(window.ids) > self._max_ids:
            window.overflowed = True
            window.ids.clear()
            window.seen.clear()

    def drain(self) -> list[TopicEvent]:
        """Take the pending events and start fresh windows."""
        events = [
            TopicEvent(topic=topic, ids=list(window.ids))
            for topic, window in self._windows.items()
        ]
        self._windows.clear()
        return events


class EventBroadcaster:
    """Owns the listener and fans coalesced events out to SSE subscribers.

    Started lazily on the first subscriber, so an app nobody is watching holds
    no extra connections.
    """

    def __init__(
        self,
        database_url: str,
        *,
        debounce_seconds: float = DEBOUNCE_SECONDS,
    ) -> None:
        self._database_url = database_url
        self._debounce_seconds = debounce_seconds
        self._listener: PostgresListener | None = None
        self._coalescer = EventCoalescer()
        self._subscribers: set[Queue[TopicEvent | None]] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._start_lock = asyncio.Lock()
        self._degraded = False

    @property
    def degraded(self) -> bool:
        """Whether the listener failed and the stream can no longer be trusted."""
        return self._degraded

    def _build_listener(self) -> PostgresListener:
        """Construct the listener. Seam for tests that inject a failing one."""
        return PostgresListener(self._database_url)

    async def _start(self) -> bool:
        """Bring up the listener and pumps once. Returns False if degraded."""
        async with self._start_lock:
            if self._degraded:
                return False
            if self._listener is not None:
                return True

            listener = self._build_listener()
            started = await listener.start()
            if is_err(started):
                logger.warning(
                    f'Monitoring event listener failed to start: '
                    f'{started.err_value.message}'
                )
                self._degraded = True
                return False

            channels = list(CHANNEL_TOPICS)
            subscribed = await listener.listen_many(channels)
            if is_err(subscribed):
                logger.warning(
                    f'Monitoring event listener failed to subscribe: '
                    f'{subscribed.err_value.message}'
                )
                await listener.close()
                self._degraded = True
                return False

            self._listener = listener
            for channel, queue in zip(channels, subscribed.ok_value):
                self._tasks.append(
                    asyncio.create_task(
                        self._pump(CHANNEL_TOPICS[channel], queue),
                        name=f'horsies-web-pump-{channel}',
                    )
                )
            self._tasks.append(
                asyncio.create_task(self._flush_loop(), name='horsies-web-flush')
            )
            return True

    async def _pump(self, topic: str, queue: Queue[Notify]) -> None:
        """Feed one channel's notifications into the coalescer."""
        while True:
            notification = await queue.get()
            self._coalescer.record(topic, notification.payload)

    async def _flush_loop(self) -> None:
        """Emit one batch per window for as long as the app is running."""
        while True:
            await asyncio.sleep(self._debounce_seconds)
            for event in self._coalescer.drain():
                self._publish(event)

    def _publish(self, event: TopicEvent) -> None:
        """Hand an event to every subscriber, dropping none silently."""
        for queue in list(self._subscribers):
            queue.put_nowait(event)

    async def subscribe(self) -> Queue[TopicEvent | None]:
        """Register an SSE stream.

        The queue receives ``None`` when the stream is degraded and should be
        closed; the caller reports that to its client.
        """
        queue: Queue[TopicEvent | None] = Queue()
        self._subscribers.add(queue)
        if not await self._start():
            queue.put_nowait(None)
        return queue

    def unsubscribe(self, queue: Queue[TopicEvent | None]) -> None:
        """Drop a stream that has gone away."""
        self._subscribers.discard(queue)

    async def close(self) -> None:
        """Stop the pumps and release the listener's connections."""
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        for queue in list(self._subscribers):
            queue.put_nowait(None)
        self._subscribers.clear()
        if self._listener is not None:
            await self._listener.close()
            self._listener = None
