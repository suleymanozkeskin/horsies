"""The send kind of an enqueue: the first send of a task id or a repeat of it.

The core runs the send in one statement (``horsies_enqueue_task``,
``core/src/broker/enqueue_task.rs``): an unkeyed send inserts
the row or answers what the row of its task id holds, and a keyed send
claims the key reservation first. ``SendKind`` tells the function whether
to read history first.
"""

from __future__ import annotations

from enum import Enum


class SendKind(Enum):
    """Whether a send is the first send of its task id or a repeat of it."""

    FIRST = 'FIRST'
    """The first send of a task id: the function does not read history."""

    REPEAT = 'REPEAT'
    """A send retry: the same task id, command and ``sent_at`` again. The
    function reads history first, so a task that finished answers with its
    id and does not run again."""
