# showcase/hemline/simulate.py
"""Deterministic simulation of everything outside the database.

Nothing here is random. Every draw is a stable hash of domain identifiers,
so one order id always draws the same outcome — in any process, on any
machine, across restarts. That is what makes "retry the declined payment
from the dashboard" decline again, and what makes a demo run reproducible.

`time.sleep` is the only side effect: it stands in for the payment provider,
the courier API, the label printer, and the invoice renderer.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

_DIGEST_BYTES: Final[int] = 8
_DIGEST_SPACE: Final[float] = float(1 << (8 * _DIGEST_BYTES))


@dataclass(frozen=True, slots=True)
class WorkEnvelope:
    """Inclusive millisecond range one simulated unit of work may take."""

    low_ms: int
    high_ms: int

    def __post_init__(self) -> None:
        if self.low_ms < 0:
            raise ValueError(f'low_ms must be >= 0, got {self.low_ms}')
        if self.high_ms < self.low_ms:
            raise ValueError(
                f'high_ms ({self.high_ms}) must be >= low_ms ({self.low_ms})',
            )


def unit(*parts: str) -> float:
    """Stable draw in ``[0.0, 1.0)`` derived from the joined parts."""
    if not parts:
        raise ValueError('unit() needs at least one part to hash')
    digest = hashlib.sha256('|'.join(parts).encode('utf-8')).digest()
    return int.from_bytes(digest[:_DIGEST_BYTES], 'big') / _DIGEST_SPACE


def draw(rate: float, *parts: str) -> bool:
    """Stable coin flip: ``True`` for ``rate`` of the possible part values."""
    if not 0.0 <= rate <= 1.0:
        raise ValueError(f'rate must be within [0.0, 1.0], got {rate}')
    return unit(*parts) < rate


def integer(low: int, high: int, *parts: str) -> int:
    """Stable integer draw in the inclusive range ``[low, high]``."""
    if high < low:
        raise ValueError(f'high ({high}) must be >= low ({low})')
    return low + int(unit(*parts) * (high - low + 1))


def duration_ms(envelope: WorkEnvelope, *parts: str) -> int:
    """Stable duration inside ``envelope``, without sleeping."""
    return integer(envelope.low_ms, envelope.high_ms, *parts)


def perform(envelope: WorkEnvelope, *parts: str) -> int:
    """Sleep for a stable duration inside ``envelope``; return the ms slept."""
    slept_ms = duration_ms(envelope, *parts)
    time.sleep(slept_ms / 1000.0)
    return slept_ms


def stall(stall_ms: int) -> int:
    """Sleep for a fixed duration — the stuck-render / hung-API path."""
    if stall_ms < 0:
        raise ValueError(f'stall_ms must be >= 0, got {stall_ms}')
    time.sleep(stall_ms / 1000.0)
    return stall_ms


def choice[T](options: Sequence[T], *parts: str) -> T:
    """Stable pick of one entry."""
    if not options:
        raise ValueError('choice() needs at least one option')
    return options[int(unit(*parts) * len(options))]


def sample[T](options: Sequence[T], count: int, *parts: str) -> list[T]:
    """Stable pick of ``count`` distinct entries, in draw order."""
    if count < 0:
        raise ValueError(f'count must be >= 0, got {count}')
    if count > len(options):
        raise ValueError(f'cannot pick {count} of {len(options)} options')
    remaining = list(options)
    picked: list[T] = []
    for index in range(count):
        position = int(unit(*parts, str(index)) * len(remaining))
        picked.append(remaining.pop(position))
    return picked
