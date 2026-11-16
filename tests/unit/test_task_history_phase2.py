"""Phase-2 consumption: structural pins over the generated SQL.

The N6 lock order is a 0.2.9 deadlock scar inherited deliberately; the
pruned-parent probe is the distinction between a one-leaf read and the
rejected fan-out; and the disposition vocabulary is closed between the
SQL, the decoder, and the evidence-retention subset.
"""

from __future__ import annotations

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.phase2.consumption import (
    EVIDENCE_RETAINING_DISPOSITIONS,
    decode_phase2_row,
)
from horsies.core.history.phase2.quarantine import (
    DRAINED_VERDICTS,
    KNOWN_QUARANTINE_VERDICTS,
    REFUSAL_VERDICTS,
    REPOINTED,
)
from tests.baseline_sql import baseline_function, baseline_sql, baseline_type

pytestmark = [pytest.mark.unit]

PHASE2_CONSUME_FUNCTION_DDL = baseline_function('horsies_phase2_consume')
PHASE2_DISPOSITION_TYPE_DDL = baseline_type('horsies_phase2_disposition')
PHASE2_QUARANTINE_FUNCTION_DDL = baseline_function('horsies_phase2_quarantine_one')


def _created_before(type_name: str, function_name: str) -> bool:
    sql = baseline_sql()
    return sql.index(f'CREATE TYPE {type_name} AS') < sql.index(
        f'CREATE FUNCTION {function_name}('
    )


class TestLockOrder:
    def test_n6_order_workflow_then_node_then_pending(self) -> None:
        body = PHASE2_CONSUME_FUNCTION_DDL
        # The replay path (no pending) takes no locks; the main path's
        # acquisitions must follow the engine's documented invariant.
        workflow_lock = body.index('FROM horsies_workflows w', body.index('Lock order:'))
        node_lock = body.index(
            'FROM horsies_workflow_tasks wt', workflow_lock
        )
        pending_lock = body.index(
            f'FROM horsies_workflow_phase2_pending', node_lock
        )
        assert workflow_lock < node_lock < pending_lock
        for span_start in (workflow_lock, node_lock, pending_lock):
            assert 'FOR UPDATE' in body[span_start:span_start + 400]


class TestPrunedProbe:
    def test_history_read_carries_both_partition_keys(self) -> None:
        body = PHASE2_CONSUME_FUNCTION_DDL
        probe = body.index('h.retention_class_key = v_pending.history_class')
        assert 'h.terminal_at = v_pending.history_anchor' in body
        assert 'h.task_id = p_task_id' in body
        comment = body.index('NOT the rejected fan-out')
        assert comment < probe

    def test_composite_node_lock_uses_structural_pinning(self) -> None:
        body = PHASE2_CONSUME_FUNCTION_DDL
        assert 'wt.id = v_pending.workflow_node_row_id' in body
        assert 'wt.workflow_id = v_pending.workflow_id' in body


class TestDispositionVocabulary:
    def test_sql_and_decoder_vocabularies_are_closed_together(self) -> None:
        from horsies.core.history.phase2.consumption import (
            KNOWN_DISPOSITIONS,
        )

        for disposition in KNOWN_DISPOSITIONS:
            assert f"'{disposition}'" in PHASE2_CONSUME_FUNCTION_DDL
        assert EVIDENCE_RETAINING_DISPOSITIONS < KNOWN_DISPOSITIONS

    def test_pending_deletes_exist_only_on_durable_paths(self) -> None:
        body = PHASE2_CONSUME_FUNCTION_DDL
        assert body.count('DELETE FROM horsies_workflow_phase2_pending') == 2

    def test_the_type_is_created_before_the_function(self) -> None:
        assert _created_before('horsies_phase2_disposition', 'horsies_phase2_consume')

    def test_type_carries_the_progression_context(self) -> None:
        for column in (
            'workflow_status text',
            'workflow_depth integer',
            'root_workflow_id uuid',
            'on_error text',
        ):
            assert column in PHASE2_DISPOSITION_TYPE_DDL


class TestDecoder:
    def test_unknown_disposition_raises(self) -> None:
        class Row:
            disposition = 'EXPLODED'

        with pytest.raises(HistoryContractError, match='unknown phase-2'):
            decode_phase2_row(Row())


class TestQuarantineLockPosture:
    def test_pending_row_lock_is_the_only_lock(self) -> None:
        body = PHASE2_QUARANTINE_FUNCTION_DDL
        assert body.count('FOR UPDATE') == 1
        pending_read = body.index('FROM horsies_workflow_phase2_pending')
        assert 'FOR UPDATE' in body[pending_read:pending_read + 200]
        # Single-tier: neither the workflow nor the node row is locked,
        # so no cycle with consumption's workflow -> node -> pending.
        node_read = body.index('FROM horsies_workflow_tasks wt')
        assert 'FOR UPDATE' not in body[node_read:node_read + 300]

    def test_history_probe_carries_both_partition_keys(self) -> None:
        body = PHASE2_QUARANTINE_FUNCTION_DDL
        probe = body.index('h.retention_class_key = v_pending.history_class')
        assert 'h.terminal_at = v_pending.history_anchor' in body
        assert 'h.task_id = p_task_id' in body
        assert body.index('NOT the rejected fan-out') < probe


class TestQuarantineEvidenceRetention:
    def test_verification_failure_is_a_caught_subtransaction(self) -> None:
        body = PHASE2_QUARANTINE_FUNCTION_DDL
        insert_at = body.index('INSERT INTO horsies_workflow_phase2_quarantine')
        raise_at = body.index("USING ERRCODE = 'HQ001'")
        catch_at = body.index("WHEN SQLSTATE 'HQ001'")
        assert insert_at < raise_at < catch_at
        assert 'COPY_VERIFICATION_FAILED' in body[catch_at:]

    def test_repoint_clears_the_history_locator(self) -> None:
        body = PHASE2_QUARANTINE_FUNCTION_DDL
        repoint = body.index("SET recovery_source = 'QUARANTINE'")
        span = body[repoint:repoint + 300]
        assert 'quarantine_task_id = p_task_id' in span
        assert 'history_class = NULL' in span
        assert 'history_anchor = NULL' in span

    def test_no_branch_deletes_pending(self) -> None:
        assert (
            'DELETE FROM horsies_workflow_phase2_pending'
            not in PHASE2_QUARANTINE_FUNCTION_DDL
        )


class TestQuarantineVocabulary:
    def test_sql_and_decoder_vocabularies_are_closed_together(self) -> None:
        for verdict in KNOWN_QUARANTINE_VERDICTS:
            assert f"'{verdict}'" in PHASE2_QUARANTINE_FUNCTION_DDL
        assert REPOINTED in KNOWN_QUARANTINE_VERDICTS
        assert DRAINED_VERDICTS < KNOWN_QUARANTINE_VERDICTS
        assert REFUSAL_VERDICTS < KNOWN_QUARANTINE_VERDICTS
        assert not DRAINED_VERDICTS & REFUSAL_VERDICTS

    def test_the_type_is_created_before_the_function(self) -> None:
        assert _created_before(
            'horsies_phase2_quarantine_verdict', 'horsies_phase2_quarantine_one'
        )

