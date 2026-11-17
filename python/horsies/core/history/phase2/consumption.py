"""Phase-2 consumption: one transaction from pending to durable disposition.

The database-owned function acquires locks in the engine's N6 order —
workflow row, node row, then the pending row — loads the authoritative
recovery material from the source the locator names, validates identity
and integrity, classifies into the exhaustive disposition set, applies
the node write when progression remains valid, and deletes recovery
evidence only on a durable disposition. Every integrity or state problem
is a typed disposition that RETAINS pending; no branch deletes recovery
evidence on a miss.

The caller supplies the terminal NODE status to apply — the task-status
to node-status mapping is engine policy, and keeping it out of this
function keeps the function mechanical. The returned row carries the
progression context the engine needs to drive dependent promotion and
workflow-completion effects inside the same caller-owned transaction,
the same shape its completion statement returns today.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import HistoryContractError

PHASE2_DISPOSITION_TYPE: Final = 'horsies_phase2_disposition'
PHASE2_CONSUME_FUNCTION: Final = 'horsies_phase2_consume'


# ---------------------------------------------------------------------------
# Typed outcomes and client wrapper
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Phase2Disposition:
    """One consumption verdict with the engine's progression context.

    Evidence-retaining dispositions (`SOURCE_*`, `SOURCE_STATE_CONFLICT`,
    `PENDING_ABSENT`) leave the pending row in place; the durable three
    (`APPLIED_TO_NODE`, `ALREADY_APPLIED`,
    `SUPERSEDED_BY_WORKFLOW_TERMINAL`) commit with the evidence deleted.
    """

    disposition: str
    workflow_id: str | None
    node_row_id: str | None
    task_index: int | None
    workflow_status: str | None
    workflow_depth: int | None
    root_workflow_id: str | None
    on_error: str | None
    node_status: str | None
    detail: str | None


KNOWN_DISPOSITIONS: Final = frozenset(
    {
        'APPLIED_TO_NODE',
        'ALREADY_APPLIED',
        'SUPERSEDED_BY_WORKFLOW_TERMINAL',
        'SOURCE_STATE_CONFLICT',
        'PENDING_ABSENT',
        'SOURCE_ABSENT',
        'SOURCE_VERSION_CONFLICT',
        'SOURCE_DIGEST_MISMATCH',
    }
)

EVIDENCE_RETAINING_DISPOSITIONS: Final = frozenset(
    {
        'SOURCE_STATE_CONFLICT',
        'PENDING_ABSENT',
        'SOURCE_ABSENT',
        'SOURCE_VERSION_CONFLICT',
        'SOURCE_DIGEST_MISMATCH',
    }
)


async def consume_phase2(
    connection: AsyncConnection,
    *,
    task_id: str,
    terminal_node_status: str,
) -> Phase2Disposition:
    """Execute one consumption inside the caller-owned transaction."""
    row = (
        await connection.execute(
            text(
                f'SELECT * FROM {PHASE2_CONSUME_FUNCTION}('
                'CAST(:task_id AS uuid), :node_status)'
            ),
            {'task_id': task_id, 'node_status': terminal_node_status},
        )
    ).one()
    return decode_phase2_row(row)


def decode_phase2_row(row: Any) -> Phase2Disposition:
    """Decode one disposition row, failing closed on contract breaks."""
    disposition = row.disposition
    if disposition not in KNOWN_DISPOSITIONS:
        raise HistoryContractError(
            f'unknown phase-2 disposition {disposition!r}'
        )
    return Phase2Disposition(
        disposition=disposition,
        workflow_id=(
            str(row.workflow_id) if row.workflow_id is not None else None
        ),
        node_row_id=(
            str(row.node_row_id) if row.node_row_id is not None else None
        ),
        task_index=row.task_index,
        workflow_status=row.workflow_status,
        workflow_depth=row.workflow_depth,
        root_workflow_id=(
            str(row.root_workflow_id)
            if row.root_workflow_id is not None
            else None
        ),
        on_error=row.on_error,
        node_status=row.node_status,
        detail=row.detail,
    )
