"""Typed details that some error codes carry.

A detail is a plain value type. ``BrokerOperationError`` and
``TaskSendError`` carry the same detail value, so the type lives here and
neither error catalog depends on the other.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class IdempotencyKeyOwner:
    """The task that holds the reservation of an idempotency key.

    Carried by an ``IDEMPOTENCY_KEY_CONFLICT`` error: the key is reserved
    by ``owner_task_id`` for a different command. While the owner task is
    live, the reservation stays. After the owner task finishes, the
    reservation ends when the reservation window has passed (24 h by
    default, at most 30 days); a send with the same key is then accepted.
    """

    owner_task_id: str
