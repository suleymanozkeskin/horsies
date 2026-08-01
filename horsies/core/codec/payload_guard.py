# horsies/core/codec/payload_guard.py
"""Payload-size guardrail applied at the encode boundaries.

Producers call :func:`enforce_payload_policy` with the length of the
already-serialized JSON string; the check is one integer comparison. The
warning is rate-limited to once per (task_name, kind) per process so a
task that persistently ships oversized payloads warns once instead of on
every enqueue. Rejection semantics belong to the caller: this module
reports the violation, the producer decides how to fail.
"""

from __future__ import annotations

from typing import Literal, Optional

from horsies.core.logging import get_logger
from horsies.core.models.payload import PayloadPolicy

logger = get_logger('payload')

PayloadKind = Literal['kwargs', 'result']

# Process-wide warn rate-limit registry. CPython set.add is atomic under
# the GIL; a rare concurrent duplicate warning is acceptable.
_warned: set[tuple[str, str]] = set()


def reset_payload_warnings() -> None:
    """Clear the warn rate-limit registry (test isolation)."""
    _warned.clear()


def enforce_payload_policy(
    policy: PayloadPolicy,
    *,
    task_name: str,
    kind: PayloadKind,
    encoded_len: int,
) -> Optional[int]:
    """Apply the payload policy to one serialized payload.

    Emits the rate-limited warning when ``encoded_len`` exceeds
    ``policy.warn_bytes``. Returns ``encoded_len`` when it exceeds
    ``policy.reject_bytes`` (the caller fails closed), ``None`` otherwise.
    """
    if policy.warn_bytes is not None and encoded_len > policy.warn_bytes:
        key = (task_name, kind)
        if key not in _warned:
            _warned.add(key)
            logger.warning(
                'Payload size guardrail: %s payload for task %r is %d bytes '
                '(warn threshold %d). Every claim and poll ships this '
                'payload; consider passing a reference instead, or raise '
                'payload.warn_bytes if this size is intended. '
                'Further warnings for this task/kind are suppressed '
                'for this process.',
                kind,
                task_name,
                encoded_len,
                policy.warn_bytes,
            )
    if policy.reject_bytes is not None and encoded_len > policy.reject_bytes:
        return encoded_len
    return None
