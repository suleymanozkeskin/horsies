"""Type pin: ``@app.on_event(EventType)`` checks the handler against its
event type.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. A ``def``
handler takes the event and returns ``None``; an ``async def`` handler takes
the event and its coroutine returns ``None``; the decorator gives the
function back with that type. Each wrong use below has an ignore comment,
and the config makes an unneeded ignore comment an error. So a decorator
type that accepts a handler of another event type, a handler that returns a
value, or a value that is not an event type, fails the check.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import assert_type

from horsies import Horsies
from horsies.telemetry import AnyEvent, AttemptEnded, TaskClaimed


def register(app: Horsies) -> None:
    @app.on_event(AttemptEnded)
    def on_end(event: AttemptEnded) -> None:
        del event

    @app.on_event(AttemptEnded)
    async def on_end_async(event: AttemptEnded) -> None:
        del event

    @app.on_event(TaskClaimed)
    def on_anything(event: object) -> None:
        del event

    assert_type(on_end, Callable[[AttemptEnded], None])
    assert_type(on_end_async, Callable[[AttemptEnded], Coroutine[object, object, None]])
    assert_type(on_anything, Callable[[TaskClaimed], None])

    def on_claim(event: TaskClaimed) -> None:
        del event

    def counts(event: AttemptEnded) -> int:
        del event
        return 1

    app.on_event(AttemptEnded)(on_claim)  # pyright: ignore[reportCallIssue, reportArgumentType]
    app.on_event(AttemptEnded)(counts)  # pyright: ignore[reportCallIssue, reportArgumentType]
    app.on_event(AnyEvent)  # pyright: ignore[reportArgumentType]
    app.on_event(int)  # pyright: ignore[reportArgumentType]
