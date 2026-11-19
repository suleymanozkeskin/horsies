# clients/python/horsies/core/codec/payload_guard.py
"""Payload-size guardrail applied at the encode boundaries.

Producers pass the already-serialized JSON text. The size is the number of
bytes of its UTF-8 text (:func:`utf8_byte_length`), the bytes that are
stored and sent; the serializer keeps non-ASCII characters as they are
(``ensure_ascii=False``), so a character can take up to 4 bytes.

- :func:`warn_if_payload_large` logs the warning for ``warn_bytes``. The
  warning is rate-limited to once per (task_name, kind) per process, so a
  task that persistently ships oversized payloads warns once instead of on
  every enqueue.
- :func:`check_payload_size` compares the size with ``reject_bytes`` and
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


def utf8_byte_length(payload_text: str) -> int:
    """The number of bytes of the UTF-8 text of ``payload_text``.

    The strict codec refuses a lone surrogate before a payload reaches a
    guard (``dumps_json``). The count still never raises: it encodes with
    ``surrogatepass``, which counts a lone surrogate as its 3-byte form.
    """
    return len(payload_text.encode('utf-8', 'surrogatepass'))


@dataclass(slots=True, frozen=True)
class PayloadWithinLimit:
    """The UTF-8 text of the payload is not larger than ``reject_bytes``, or
    no limit is set."""


@dataclass(slots=True, frozen=True)
class PayloadOverLimit:
    """The UTF-8 text of the payload is larger than ``reject_bytes``."""

    size_bytes: int
    limit_bytes: int


type PayloadSizeCheck = PayloadWithinLimit | PayloadOverLimit


def warn_if_payload_large(
    policy: PayloadPolicy,
    *,
    task_name: str,
    kind: PayloadKind,
    payload_text: str,
) -> None:
    """Log the payload-size warning when the UTF-8 text of ``payload_text``
    is longer than ``policy.warn_bytes`` bytes.

    Logs once per (task_name, kind) per process. ``warn_bytes=None`` disables
    the warning and measures nothing. Does not read ``reject_bytes``.
    """
    warn_bytes = policy.warn_bytes
    if warn_bytes is None:
        return
    size_bytes = utf8_byte_length(payload_text)
    if size_bytes <= warn_bytes:
        return
    key = (task_name, kind)
    if key in _warned:
        return
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
        size_bytes,
        warn_bytes,
    )


def check_payload_size(
    policy: PayloadPolicy,
    *,
    payload_text: str,
) -> PayloadSizeCheck:
    """Compare the UTF-8 byte size of one serialized payload with
    ``policy.reject_bytes``.

    The limit is exclusive: a size equal to ``reject_bytes`` is within the
    limit. ``reject_bytes=None`` gives ``PayloadWithinLimit`` and measures
    nothing. Does not log.
    """
    match policy.reject_bytes:
        case None:
            return PayloadWithinLimit()
        case int() as limit_bytes:
            size_bytes = utf8_byte_length(payload_text)
            if size_bytes > limit_bytes:
                return PayloadOverLimit(size_bytes=size_bytes, limit_bytes=limit_bytes)
            return PayloadWithinLimit()
