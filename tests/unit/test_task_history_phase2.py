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
    PHASE2_CONSUME_FUNCTION_DDL,
    PHASE2_DISPOSITION_TYPE_DDL,
    consumption_fragments,
    decode_phase2_row,
)

pytestmark = [pytest.mark.unit]


class TestLockOrder:
    def test_n6_order_workflow_then_node_then_pending(self) -> None:
        body = PHASE2_CONSUME_FUNCTION_DDL
        # The replay path (no pending) takes no locks; the main path's
        # acquisitions must follow the engine's documented invariant.
        workflow_lock = body.index('FROM horsies_workflows w', body.index('N6'))
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
        assert 'h.retention_anchor_at = v_pending.history_anchor' in body
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

    def test_fragment_order(self) -> None:
        first, second = consumption_fragments()
        assert 'CREATE TYPE' in first
        assert 'CREATE FUNCTION' in second

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
