"""An event stream of the monitoring app, read over ASGI while it is open."""

from __future__ import annotations

import asyncio
import json
from typing import Any


class AsgiEventStream:
    """Reads a streaming ASGI response while it is still open.

    httpx's ASGI transport accumulates the whole body before returning a
    response, so an endless event stream deadlocks when read through it.
    Speaking ASGI directly is what makes the stream observable — and it still
    exercises the real app: routing, dependencies, and SSE framing.
    """

    def __init__(
        self, app: Any, path: str, *, headers: dict[str, str] | None = None
    ) -> None:
        self._app = app
        self._path = path
        self._headers = headers or {}
        self._chunks: asyncio.Queue[bytes] = asyncio.Queue()
        self._disconnect = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._start: asyncio.Future[dict[str, Any]] | None = None
        self._buffer = ''

    async def __aenter__(self) -> AsgiEventStream:
        self._start = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._run())
        await asyncio.wait_for(asyncio.shield(self._start), timeout=20)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self._disconnect.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def status_code(self) -> int:
        assert self._start is not None
        return int(self._start.result()['status'])

    @property
    def headers(self) -> dict[str, str]:
        assert self._start is not None
        return {
            key.decode().lower(): value.decode()
            for key, value in self._start.result()['headers']
        }

    async def _run(self) -> None:
        scope: dict[str, Any] = {
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': '2.3'},
            'http_version': '1.1',
            'method': 'GET',
            'scheme': 'http',
            'path': self._path,
            'raw_path': self._path.encode(),
            'query_string': b'',
            'root_path': '',
            'headers': [
                (key.lower().encode(), value.encode())
                for key, value in self._headers.items()
            ],
            'client': ('127.0.0.1', 12345),
            'server': ('testserver', 80),
        }

        async def receive() -> dict[str, Any]:
            await self._disconnect.wait()
            return {'type': 'http.disconnect'}

        async def send(message: dict[str, Any]) -> None:
            if message['type'] == 'http.response.start':
                assert self._start is not None
                if not self._start.done():
                    self._start.set_result(message)
            elif message['type'] == 'http.response.body':
                await self._chunks.put(bytes(message.get('body', b'')))

        await self._app(scope, receive, send)

    async def next_line(self, *, timeout_s: float) -> str:
        while '\n' not in self._buffer:
            chunk = await asyncio.wait_for(self._chunks.get(), timeout=timeout_s)
            self._buffer += chunk.decode()
        line, _, rest = self._buffer.partition('\n')
        self._buffer = rest
        return line

    async def next_data(self, *, timeout_s: float) -> dict[str, Any]:
        while True:
            line = await self.next_line(timeout_s=timeout_s)
            if line.startswith('data: '):
                parsed: dict[str, Any] = json.loads(line.removeprefix('data: '))
                return parsed

    async def next_comment(self, *, timeout_s: float) -> str:
        while True:
            line = await self.next_line(timeout_s=timeout_s)
            if line.startswith(':'):
                return line

    async def next_frame(self, *, timeout_s: float) -> str:
        """The next whole frame, with the blank line that ends it."""
        while '\n\n' not in self._buffer:
            chunk = await asyncio.wait_for(self._chunks.get(), timeout=timeout_s)
            self._buffer += chunk.decode()
        frame, _, rest = self._buffer.partition('\n\n')
        self._buffer = rest
        return f'{frame}\n\n'
