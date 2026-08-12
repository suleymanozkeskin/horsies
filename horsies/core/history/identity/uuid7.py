"""Client-side UUIDv7 minting: monotonic within the millisecond.

One process-global generator mints every task ID. Within a millisecond,
ordering comes from a 12-bit counter in `rand_a` — a random value there
measurably bloats the primary-key btree by triggering non-rightmost page
splits, which is the recorded rejection of within-ms randomness. When the
counter is exhausted the generator waits for the clock to advance; it never
falls back to random order. A clock that steps backwards is absorbed by
continuing on the last observed millisecond, so minted IDs remain monotonic
even then. Staged history readers treat the embedded time only as a probe-order
hint and still search every retained leaf before reporting absence.

Monotonicity holds across threads and async tasks sharing the process
generator. No cross-process total order is promised.

The public surface is the canonical lowercase hyphenated string; `uuid.UUID`
never crosses a public boundary. The birth decoder is the same arithmetic
the generated staged-lookup function performs in SQL.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Final
from uuid import UUID


_MAX_UNIX_MILLISECONDS: Final = (1 << 48) - 1
_MAX_COUNTER: Final = (1 << 12) - 1
_RAND_B_BITS: Final = 62


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _system_entropy_62_bits() -> int:
    return int.from_bytes(os.urandom(8), 'big') >> (64 - _RAND_B_BITS)


class MonotonicUuid7Generator:
    """Mints v7 IDs whose (millisecond, counter) pair strictly increases.

    The clock and entropy sources are injectable for deterministic tests;
    production uses the module-level process generator with system sources.
    """

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _system_clock_ms,
        entropy_62_bits: Callable[[], int] = _system_entropy_62_bits,
    ) -> None:
        self._clock_ms = clock_ms
        self._entropy_62_bits = entropy_62_bits
        self._lock = threading.Lock()
        self._last_ms = -1
        self._counter = 0

    def mint(self) -> str:
        """Mint one canonical lowercase hyphenated UUIDv7 string."""
        with self._lock:
            milliseconds, counter = self._advance()
            entropy = self._entropy_62_bits()
        if not 0 <= entropy < 1 << _RAND_B_BITS:
            raise ValueError('entropy source exceeded 62 bits')
        value = (
            (milliseconds << 80)
            | (0x7 << 76)
            | (counter << 64)
            | (0b10 << 62)
            | entropy
        )
        return str(UUID(int=value))

    def _advance(self) -> tuple[int, int]:
        now = self._clock_ms()
        if not 0 <= now <= _MAX_UNIX_MILLISECONDS:
            raise ValueError('millisecond clock is outside the 48-bit range')
        if now > self._last_ms:
            self._last_ms = now
            self._counter = 0
            return now, 0
        if self._counter < _MAX_COUNTER:
            self._counter += 1
            return self._last_ms, self._counter
        while now <= self._last_ms:
            now = self._clock_ms()
        if not now <= _MAX_UNIX_MILLISECONDS:
            raise ValueError('millisecond clock is outside the 48-bit range')
        self._last_ms = now
        self._counter = 0
        return now, 0


_PROCESS_GENERATOR = MonotonicUuid7Generator()


def mint_task_id() -> str:
    """Mint a task ID from the process-global generator.

    Called once per prepared-send construction; the minted ID travels with
    the payload and fingerprint and is resubmitted verbatim by automatic
    resend and retry paths, so a lost commit response cannot create a
    second unkeyed request.
    """
    return _PROCESS_GENERATOR.mint()


def uuid7_birth_at(value: str | UUID) -> datetime | None:
    """Decode a v7 identifier's embedded UTC birth time.

    Returns None for any other UUID version — the caller falls back to the
    complete staged walk, never to a guessed birth time.
    """
    parsed = value if isinstance(value, UUID) else UUID(value)
    if parsed.version != 7:
        return None
    unix_milliseconds = parsed.int >> 80
    return datetime.fromtimestamp(unix_milliseconds / 1_000, tz=timezone.utc)
