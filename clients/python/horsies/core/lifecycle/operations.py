"""The terminalization kind that the database stores on a task row.

The kind is persisted on the row and carried into history, so it is a frozen
vocabulary: a value in a row from last month has to keep meaning what it
meant when it was written. Kinds are written by the database, never supplied
by a caller: each function hardcodes its own.
"""

from __future__ import annotations

from enum import Enum


class TerminalizationKind(Enum):
    """The committed semantic operation, as stored on the task row.

    Frozen vocabulary: a value here is a promise to every row that already
    carries it. Add members; do not rename or repurpose them.
    """

    COMPLETE_LOCKED = 'COMPLETE_LOCKED'
    COMPLETE_FUSED = 'COMPLETE_FUSED'
    FAIL_RUNNING = 'FAIL_RUNNING'
    FAIL_STALE = 'FAIL_STALE'
    EXPIRE_CLAIMED = 'EXPIRE_CLAIMED'
    EXPIRE_PENDING = 'EXPIRE_PENDING'
    CANCEL_ADMIN = 'CANCEL_ADMIN'
    CANCEL_ORPHAN = 'CANCEL_ORPHAN'
    CANCEL_ORPHAN_SWEEP = 'CANCEL_ORPHAN_SWEEP'
    PAUSE_ABANDON_CLAIM = 'PAUSE_ABANDON_CLAIM'
    PAUSE_ABANDON_CLAIM_BATCH = 'PAUSE_ABANDON_CLAIM_BATCH'
    PAUSE_ABANDON_WORKFLOW = 'PAUSE_ABANDON_WORKFLOW'
    WORKFLOW_CANCEL_CLAIM = 'WORKFLOW_CANCEL_CLAIM'
    WORKFLOW_CANCEL_CLAIM_BATCH = 'WORKFLOW_CANCEL_CLAIM_BATCH'
    WORKFLOW_CANCEL_WORKFLOW = 'WORKFLOW_CANCEL_WORKFLOW'


