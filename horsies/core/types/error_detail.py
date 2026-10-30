"""Typed values that several error catalogs share.

Each value is a plain frozen type. The type lives here so that no error
catalog depends on another one.
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


@dataclass(slots=True, frozen=True)
class NoCause:
    """No Python exception caused this failure: a check or a comparison found it.

    It has no fields, so every instance is equal and ``case NoCause():``
    matches it. Match on the cause field before a ``raise ... from``, an
    ``exc_info=`` or an error tracker call.
    """
