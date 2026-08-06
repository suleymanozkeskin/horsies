"""Scoped idempotency keys: exact opaque bytes under task-name scope.

The caller's key is opaque — no trimming, no case folding, no
normalization. Aliasing two distinct caller values without a typed
namespace contract is the recorded rejection of every normalization
candidate. Scope is the canonical task name: unrelated task types may
safely reuse natural business keys, and routing changes cannot silently
change idempotency identity the way queue scope would.

The digest is length-framed SHA-256 over the version-1 domain string, the
task name, and the exact key bytes; framing makes concatenation collisions
impossible and the domain string pins scope version 1 into every stored
digest.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Final


IDEMPOTENCY_KEY_MAX_BYTES: Final = 255
IDEMPOTENCY_SCOPE_VERSION: Final = 1
IDEMPOTENCY_WINDOW_DEFAULT: Final = timedelta(hours=24)
IDEMPOTENCY_WINDOW_MAX: Final = timedelta(days=30)

_SCOPE_DOMAIN_V1: Final = b'horsies.enqueue-key.v1'


@dataclass(frozen=True, slots=True)
class ScopedIdempotencyKey:
    """One caller key under one task-name scope, validated at construction."""

    task_name: str
    key: str

    def __post_init__(self) -> None:
        _validate_opaque_value('task_name', self.task_name)
        _validate_opaque_value('idempotency key', self.key)

    @property
    def digest(self) -> bytes:
        """The 32-byte scoped digest stored by the reservation registry."""
        return _digest_framed(
            _SCOPE_DOMAIN_V1,
            self.task_name.encode(),
            self.key.encode(),
        )


def validate_reservation_window(window: timedelta) -> timedelta:
    """Validate a configured reservation duration before any mutation.

    The accepted range is greater than zero through the inclusive 30-day
    maximum; the check runs before any task or registry write so an invalid
    configuration cannot leave partial state.
    """
    if window <= timedelta(0):
        raise ValueError('idempotency reservation window must be positive')
    if window > IDEMPOTENCY_WINDOW_MAX:
        raise ValueError(
            f'idempotency reservation window exceeds the inclusive maximum '
            f'of {IDEMPOTENCY_WINDOW_MAX.days} days'
        )
    return window


def _validate_opaque_value(label: str, value: str) -> None:
    encoded = value.encode()
    if not encoded:
        raise ValueError(f'{label} must be non-empty')
    if len(encoded) > IDEMPOTENCY_KEY_MAX_BYTES:
        raise ValueError(
            f'{label} must be at most {IDEMPOTENCY_KEY_MAX_BYTES} UTF-8 bytes'
        )


def _digest_framed(domain: bytes, *parts: bytes) -> bytes:
    digest = hashlib.sha256()
    digest.update(len(domain).to_bytes(4, 'big'))
    digest.update(domain)
    for part in parts:
        digest.update(len(part).to_bytes(4, 'big'))
        digest.update(part)
    return digest.digest()
