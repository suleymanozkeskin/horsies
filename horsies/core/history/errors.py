"""Errors that mean the history subsystem and the database disagree.

These are never task or maintenance outcomes. A refused detach, a blocked
leaf, or an unexpired leaf is an outcome variant in `outcomes.py` — the
operation learned something and reports it. An error here means one side is
running against a contract the other does not implement: a parent relation
that should exist does not, a lock this transaction must own was not held,
a catalog row and its relation tell different stories that no outcome
variant is allowed to paper over.
"""

from __future__ import annotations


class HistoryContractError(Exception):
    """The database state violates an invariant this subsystem relies on."""


class LeafLockNotHeld(HistoryContractError):
    """A maintenance path released or lost an advisory lock it never owned."""


class HistoryParentAbsent(HistoryContractError):
    """A cataloged finite parent relation does not exist in the database."""
