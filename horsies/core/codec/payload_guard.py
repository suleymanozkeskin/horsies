# horsies/core/codec/payload_guard.py
"""Payload-size guardrail applied at the encode boundaries.

Producers pass the length of the already-serialized JSON string; each
check is one integer comparison.

- :func:`warn_if_payload_large` logs the warning for ``warn_bytes``. The
  warning is rate-limited to once per (task_name, kind) per process, so a
  task that persistently ships oversized payloads warns once instead of on
  every enqueue.
- :func:`check_payload_size` compares the length with ``reject_bytes`` and
  returns a :data:`PayloadSizeCheck`. It does not log. The producer decides
  how to fail on :class:`PayloadOverLimit`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

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


@dataclass(slots=True, frozen=True)
class PayloadWithinLimit:
    """The payload is not larger than ``reject_bytes``, or no limit is set."""


@dataclass(slots=True, frozen=True)
class PayloadOverLimit:
    """The payload is larger than ``reject_bytes``."""

    size_bytes: int
    limit_bytes: int


type PayloadSizeCheck = PayloadWithinLimit | PayloadOverLimit


def warn_if_payload_large(
    policy: PayloadPolicy,
    *,
    task_name: str,
    kind: PayloadKind,
    encoded_len: int,
) -> None:
    """Log the payload-size warning when ``encoded_len`` exceeds ``policy.warn_bytes``.

    Logs once per (task_name, kind) per process. ``warn_bytes=None`` disables
    the warning. Does not read ``reject_bytes``.
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


def check_payload_size(
    policy: PayloadPolicy,
    *,
    encoded_len: int,
) -> PayloadSizeCheck:
    """Compare one serialized payload length with ``policy.reject_bytes``.

    The limit is exclusive: a length equal to ``reject_bytes`` is within the
    limit. ``reject_bytes=None`` gives ``PayloadWithinLimit``. Does not log.
    """
    match policy.reject_bytes:
        case None:
            return PayloadWithinLimit()
        case int() as limit_bytes if encoded_len > limit_bytes:
            return PayloadOverLimit(size_bytes=encoded_len, limit_bytes=limit_bytes)
        case int():
            return PayloadWithinLimit()
