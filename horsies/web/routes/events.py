# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""Server-sent invalidation events.

One stream per browser tab. The server never fabricates an event: if the
listener cannot be trusted, the stream says so and closes, and the client
falls back to interval polling until it can reconnect.
"""

from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from horsies.web.events import (
    HEARTBEAT_SECONDS,
    TOPIC_DEGRADED,
    EventBroadcaster,
)

# A comment frame: valid SSE, ignored by EventSource, enough to keep an idle
# connection from being reaped by an intermediary proxy.
_HEARTBEAT_FRAME = ': heartbeat\n\n'


def build_router(broadcaster: EventBroadcaster) -> APIRouter:
    """Build the ``/events`` router bound to one broadcaster."""
    router = APIRouter(tags=['events'])

    async def stream() -> AsyncIterator[str]:
        queue = await broadcaster.subscribe()
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        queue.get(), timeout=HEARTBEAT_SECONDS
                    )
                except TimeoutError:
                    yield _HEARTBEAT_FRAME
                    continue
                if event is None:
                    yield f'data: {json.dumps({"topic": TOPIC_DEGRADED})}\n\n'
                    return
                yield (
                    'data: '
                    f'{json.dumps({"topic": event.topic, "ids": event.ids})}'
                    '\n\n'
                )
        finally:
            broadcaster.unsubscribe(queue)

    @router.get('/events')
    async def read_events() -> StreamingResponse:
        """Subscribe to coalesced invalidation events."""
        return StreamingResponse(
            stream(),
            media_type='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',
            },
        )

    return router
