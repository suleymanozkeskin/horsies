"""Detach-horizon quarantine: copy, verify, repoint one stale locator.

A pending locator pins its history leaf against detach. When a locator
outlives the caller's horizon, maintenance moves the recovery material out
of the leaf: copy the minimum recovery projection into the quarantine
table, verify the copy, and repoint pending at the copy — after which the
leaf no longer carries recovery evidence and detach can proceed. A failed
copy or verification keeps the history locator and blocks detach with a
typed refusal; maintenance never deletes unresolved recovery evidence to
make retention progress.

Each per-task copy/verify/repoint is one database function call, so one
statement is one transaction even on the autocommit connection that holds
the session-scoped leaf advisory lock — every repoint is committed before
the leaf is declared unpinned. The caller must hold the leaf advisory
lock; the operation itself locks only the pending row, a single-tier
acquisition that can participate in no lock cycle with consumption's
workflow -> node -> pending order.

Bounded relations per statement: the pending table, one pruned history
leaf, the node row, and the quarantine table — inherently per-leaf, since
a locator names exactly one leaf.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import LeafRef
from ..errors import HistoryContractError
from ..names import (
    TASK_HISTORY_PARENT,
    WORKFLOW_PHASE2_PENDING,
    WORKFLOW_PHASE2_QUARANTINE,
)

PHASE2_QUARANTINE_VERDICT_TYPE: Final = 'horsies_phase2_quarantine_verdict'
PHASE2_QUARANTINE_FUNCTION: Final = 'horsies_phase2_quarantine_one'

PHASE2_QUARANTINE_VERDICT_TYPE_DDL = f"""
CREATE TYPE {PHASE2_QUARANTINE_VERDICT_TYPE} AS (
    verdict text,
    detail text
)
"""

PHASE2_QUARANTINE_FUNCTION_DDL = f"""
CREATE FUNCTION {PHASE2_QUARANTINE_FUNCTION}(
    p_task_id uuid,
    p_reason text
) RETURNS {PHASE2_QUARANTINE_VERDICT_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_pending {WORKFLOW_PHASE2_PENDING}%ROWTYPE;
    v_node_id text;
    v_hist record;
    v_copy record;
BEGIN
    -- The only lock this function takes: the pending row. Single-tier;
    -- consumption acquiring workflow -> node -> pending can wait on it
    -- but no cycle can form because nothing else is held here.
    SELECT * INTO v_pending
    FROM {WORKFLOW_PHASE2_PENDING}
    WHERE task_id = p_task_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('PENDING_GONE', 'no pending row; locator drained')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;
    IF v_pending.recovery_source <> 'HISTORY' THEN
        RETURN ROW('ALREADY_QUARANTINED',
                   'pending already repointed at quarantine')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;

    SELECT wt.node_id INTO v_node_id
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id;
    IF NOT FOUND THEN
        RETURN ROW('NODE_ROW_ABSENT',
                   'node row absent while pending exists')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;
    IF v_node_id IS NULL THEN
        -- The quarantine projection requires a node identity and the
        -- node row carries none; refusing retains the history locator.
        RETURN ROW('NODE_IDENTITY_ABSENT',
                   'node row carries no node_id')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;

    -- One-leaf parent probe, NOT the rejected fan-out: the locator
    -- supplies both partition keys, so LIST (class) and RANGE (anchor)
    -- prune to exactly one leaf at plan time.
    SELECT h.task_id, h.task_name, h.status, h.terminalization_kind,
           h.terminal_at, h.history_schema_version,
           h.result_envelope_version, h.result_codec,
           h.result_content_type, h.result_payload, h.result_digest
    INTO v_hist
    FROM {TASK_HISTORY_PARENT} h
    WHERE h.retention_class_key = v_pending.history_class
      AND h.retention_anchor_at = v_pending.history_anchor
      AND h.task_id = p_task_id;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_ABSENT', 'history row absent at locator')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;

    BEGIN
        INSERT INTO {WORKFLOW_PHASE2_QUARANTINE} (
            task_id, workflow_id, workflow_node_row_id, node_id,
            task_name, terminal_status, terminalization_kind, terminal_at,
            history_schema_version, result_envelope_version,
            result_codec, result_content_type,
            result_payload, result_digest,
            source_history_class, source_history_anchor,
            quarantine_reason, quarantined_at
        ) VALUES (
            v_hist.task_id, v_pending.workflow_id,
            v_pending.workflow_node_row_id, v_node_id,
            v_hist.task_name, v_hist.status, v_hist.terminalization_kind,
            v_hist.terminal_at,
            v_hist.history_schema_version, v_hist.result_envelope_version,
            v_hist.result_codec, v_hist.result_content_type,
            v_hist.result_payload, v_hist.result_digest,
            v_pending.history_class, v_pending.history_anchor,
            p_reason, statement_timestamp()
        );

        -- Verification on the copy itself: read the row back and hold
        -- it against the pending locator, so a projection defect is a
        -- refusal, not a corrupt quarantine row.
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version
        INTO STRICT v_copy
        FROM {WORKFLOW_PHASE2_QUARANTINE} q
        WHERE q.task_id = p_task_id;
        IF v_copy.task_id IS DISTINCT FROM p_task_id
           OR v_copy.result_digest IS DISTINCT FROM v_pending.result_digest
           OR v_copy.result_payload IS NULL
           OR sha256(v_copy.result_payload) <> v_copy.result_digest
           OR v_copy.history_schema_version
              IS DISTINCT FROM v_pending.history_schema_version
        THEN
            RAISE EXCEPTION 'quarantine copy disagrees with locator'
                USING ERRCODE = 'HQ001';
        END IF;
    EXCEPTION
        WHEN SQLSTATE 'HQ001' OR unique_violation OR not_null_violation
             OR check_violation THEN
            -- The sub-transaction rolls the copy back; pending keeps its
            -- history locator and the leaf stays pinned.
            RETURN ROW('COPY_VERIFICATION_FAILED', SQLERRM)
                ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END;

    UPDATE {WORKFLOW_PHASE2_PENDING}
    SET recovery_source = 'QUARANTINE',
        quarantine_task_id = p_task_id,
        history_class = NULL,
        history_anchor = NULL
    WHERE task_id = p_task_id;

    RETURN ROW('REPOINTED', NULL)::{PHASE2_QUARANTINE_VERDICT_TYPE};
END
$function$
"""


def quarantine_fragments() -> tuple[str, ...]:
    """The verdict type and quarantine function, in install order."""
    return (
        PHASE2_QUARANTINE_VERDICT_TYPE_DDL,
        PHASE2_QUARANTINE_FUNCTION_DDL,
    )


# ---------------------------------------------------------------------------
# Typed command, outcomes, and the per-leaf operation
# ---------------------------------------------------------------------------


REPOINTED: Final = 'REPOINTED'
DRAINED_VERDICTS: Final = frozenset({'PENDING_GONE', 'ALREADY_QUARANTINED'})
REFUSAL_VERDICTS: Final = frozenset(
    {
        'NODE_ROW_ABSENT',
        'NODE_IDENTITY_ABSENT',
        'SOURCE_ABSENT',
        'COPY_VERIFICATION_FAILED',
    }
)
KNOWN_QUARANTINE_VERDICTS: Final = (
    frozenset({REPOINTED}) | DRAINED_VERDICTS | REFUSAL_VERDICTS
)


@dataclass(frozen=True, slots=True)
class QuarantineLeafBlockers:
    """Quarantine every locator on one leaf older than the horizon.

    The caller must hold the leaf's advisory lock; the horizon is the age
    (by the pending row's `created_at`) past which a locator is treated as
    stalled recovery evidence rather than in-flight drain traffic.
    """

    leaf: LeafRef
    horizon: timedelta

    def __post_init__(self) -> None:
        if self.horizon <= timedelta(0):
            raise ValueError('quarantine horizon must be positive')


@dataclass(frozen=True, slots=True)
class TaskQuarantineRefusal:
    """One locator that could not be quarantined; pending is retained."""

    task_id: str
    verdict: str
    detail: str | None


@dataclass(frozen=True, slots=True)
class NoOverHorizonBlockers:
    """No locator on the leaf has outlived the horizon."""

    leaf_name: str


@dataclass(frozen=True, slots=True)
class BlockersQuarantined:
    """Every over-horizon locator was repointed or found already drained."""

    leaf_name: str
    repointed: int
    drained: int


@dataclass(frozen=True, slots=True)
class QuarantineRefused:
    """At least one locator refused; the leaf remains pinned.

    Repoints that committed before a refusal stand — each is individually
    consistent — but the refusing locators keep their history rows pinned
    and the alert/repair path owns them.
    """

    leaf_name: str
    repointed: int
    refusals: tuple[TaskQuarantineRefusal, ...]


type LeafQuarantine = (
    NoOverHorizonBlockers | BlockersQuarantined | QuarantineRefused
)


async def quarantine_over_horizon_blockers(
    connection: AsyncConnection,
    command: QuarantineLeafBlockers,
) -> LeafQuarantine:
    """Copy, verify, and repoint every over-horizon locator on one leaf.

    Statement-at-a-time by design: discovery is one read, and each
    copy/verify/repoint is one call to the database function, so the
    operation is correct on an autocommit maintenance connection and every
    repoint is durable before the caller re-inspects the leaf. Discovery
    orders by task id; each call locks only its own pending row.
    """
    leaf = command.leaf
    task_ids = (
        (
            await connection.execute(
                text(
                    f"""
                    SELECT task_id
                    FROM {WORKFLOW_PHASE2_PENDING}
                    WHERE recovery_source = 'HISTORY'
                      AND history_class = :class_key
                      AND history_anchor >= :lower
                      AND history_anchor < :upper
                      AND created_at <= statement_timestamp() - :horizon
                    ORDER BY task_id
                    """
                ),
                {
                    'class_key': leaf.class_key,
                    'lower': leaf.bounds.lower,
                    'upper': leaf.bounds.upper,
                    'horizon': command.horizon,
                },
            )
        )
        .scalars()
        .all()
    )
    if not task_ids:
        return NoOverHorizonBlockers(leaf_name=leaf.leaf_name)

    reason = f'over-horizon phase-2 locator on {leaf.leaf_name}'
    repointed = 0
    drained = 0
    refusals: list[TaskQuarantineRefusal] = []
    for task_id in task_ids:
        row = (
            await connection.execute(
                text(
                    f'SELECT * FROM {PHASE2_QUARANTINE_FUNCTION}('
                    'CAST(:task_id AS uuid), :reason)'
                ),
                {'task_id': task_id, 'reason': reason},
            )
        ).one()
        verdict = _decode_quarantine_verdict(row)
        if verdict == REPOINTED:
            repointed += 1
        elif verdict in DRAINED_VERDICTS:
            drained += 1
        else:
            refusals.append(
                TaskQuarantineRefusal(
                    task_id=str(task_id),
                    verdict=verdict,
                    detail=row.detail,
                )
            )
    if refusals:
        return QuarantineRefused(
            leaf_name=leaf.leaf_name,
            repointed=repointed,
            refusals=tuple(refusals),
        )
    return BlockersQuarantined(
        leaf_name=leaf.leaf_name, repointed=repointed, drained=drained
    )


def _decode_quarantine_verdict(row: Any) -> str:
    verdict = row.verdict
    if verdict not in KNOWN_QUARANTINE_VERDICTS:
        raise HistoryContractError(
            f'unknown phase-2 quarantine verdict {verdict!r}'
        )
    return verdict
