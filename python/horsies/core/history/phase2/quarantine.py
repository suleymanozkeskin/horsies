"""Phase-2 quarantine: copy, verify, repoint one pending row.

Phase-2 recovery quarantines a pending row whose attempt count reached the
bound: the database function copies the minimum recovery projection out of
the history leaf into the quarantine table, verifies the copy, and repoints
the pending row at it. A failed copy or verification keeps the history
locator and returns a typed refusal; nothing deletes unresolved recovery
evidence.

The function locks only the pending row, a single-tier acquisition that can
take part in no lock cycle with consumption's workflow -> node -> pending
order. It reads one pruned history leaf, the node row, and the quarantine
table.
"""

from __future__ import annotations

from typing import Final


PHASE2_QUARANTINE_VERDICT_TYPE: Final = 'horsies_phase2_quarantine_verdict'
PHASE2_QUARANTINE_FUNCTION: Final = 'horsies_phase2_quarantine_one'


# ---------------------------------------------------------------------------
# Verdict vocabulary
# ---------------------------------------------------------------------------


REPOINTED: Final = 'REPOINTED'
DRAINED_VERDICTS: Final = frozenset({'PENDING_GONE', 'ALREADY_QUARANTINED'})
REFUSAL_VERDICTS: Final = frozenset(
    {
        'NODE_ROW_ABSENT',
        'SOURCE_ABSENT',
        'COPY_VERIFICATION_FAILED',
    }
)
KNOWN_QUARANTINE_VERDICTS: Final = (
    frozenset({REPOINTED}) | DRAINED_VERDICTS | REFUSAL_VERDICTS
)
