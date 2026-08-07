"""The completion-family move: structural pins over the generated SQL.

The reconciled ten-step order, the isolated envelope and attempt blocks,
the copied-never-recomputed digest, the STRICT node lookup, and the
staged uniqueness guard are contract facts the integration suite proves
behaviorally; these pins make each one fail fast and name itself when a
refactor touches the body text.
"""

from __future__ import annotations

import pytest

from horsies.core.history.ddl.fragments import frozen_fragments
from horsies.core.history.maintenance.gate import (
    ARCHIVE_AVAILABILITY_FUNCTION_DDL,
    gate_fragments,
)
from horsies.core.history.terminalization.live_cutover import (
    LIVE_CUTOVER_COLUMNS_DDL,
)
from horsies.core.history.terminalization.move import (
    ATTEMPT_ENCODER_DDL,
    completion_family_fragments,
)
from horsies.core.history.terminalization.outcome import (
    MISS_CLASSIFIER_DDL,
    OUTCOME_TYPE_DDL,
    outcome_fragments,
)
from horsies.core.lifecycle.operations import TerminalizationKind

pytestmark = [pytest.mark.unit]


MOVE_BODY = completion_family_fragments()[1]


class TestGate:
    def test_gate_joins_the_frozen_fragments(self) -> None:
        combined = '\n'.join(frozen_fragments())
        assert 'horsies_archive_access_gate' in combined
        assert 'horsies_assert_archive_available' in combined

    def test_assert_takes_shared_lock_and_types_the_refusal(self) -> None:
        assert 'FOR SHARE' in ARCHIVE_AVAILABILITY_FUNCTION_DDL
        assert "ERRCODE = 'object_in_use'" in ARCHIVE_AVAILABILITY_FUNCTION_DDL

    def test_gate_installs_before_use(self) -> None:
        fragments = gate_fragments()
        assert 'CREATE TABLE' in fragments[0]
        assert 'INSERT INTO' in fragments[1]
        assert 'CREATE FUNCTION' in fragments[3]


class TestOutcomeLayer:
    def test_outcome_type_task_id_is_uuid(self) -> None:
        assert 'task_id uuid' in OUTCOME_TYPE_DDL

    def test_miss_classifier_consults_provenance_without_live(self) -> None:
        assert (
            'horsies_task_provenance_staged(p_task_id, FALSE)'
            in MISS_CLASSIFIER_DDL
        )

    def test_miss_order_absent_then_already_applied_then_foreign(self) -> None:
        absent = MISS_CLASSIFIER_DDL.index("'TASK_ABSENT'")
        already = MISS_CLASSIFIER_DDL.index("'ALREADY_APPLIED'")
        foreign = MISS_CLASSIFIER_DDL.index("'FOREIGN_TERMINALIZATION'")
        lost = MISS_CLASSIFIER_DDL.index("'LOST_CLAIM'")
        assert absent < already < foreign < lost

    def test_live_hit_from_provenance_after_live_miss_raises(self) -> None:
        assert "ERRCODE = 'data_corrupted'" in MISS_CLASSIFIER_DDL

    def test_fragment_order(self) -> None:
        first, second = outcome_fragments()
        assert 'CREATE TYPE' in first
        assert 'CREATE FUNCTION' in second


class TestMoveStructure:
    def test_reconciled_step_order(self) -> None:
        availability = MOVE_BODY.index('horsies_assert_archive_available')
        advisory = MOVE_BODY.index('pg_advisory_xact_lock')
        snapshot = MOVE_BODY.index('SELECT * INTO STRICT v_task')
        uniqueness = MOVE_BODY.index('horsies_task_provenance_staged')
        insert = MOVE_BODY.index('INSERT INTO horsies_task_history')
        deletes = MOVE_BODY.index('DELETE FROM horsies_task_attempts')
        notify = MOVE_BODY.index("pg_notify('task_done'")
        assert (
            availability < advisory < snapshot < uniqueness < insert
            < deletes < notify
        )

    def test_uniqueness_guard_excludes_live(self) -> None:
        assert (
            'FROM horsies_task_provenance_staged(p_task_id, FALSE)'
            in MOVE_BODY
        )

    def test_node_lookup_is_strict_with_ownership_predicate(self) -> None:
        assert 'INTO STRICT v_workflow_node_row_id' in MOVE_BODY
        assert 'FROM horsies_workflow_tasks' in MOVE_BODY

    def test_envelope_ladder_eligibility_before_policy(self) -> None:
        never = MOVE_BODY.index("v_rerun_disposition := 'NEVER_ELIGIBLE'")
        declined = MOVE_BODY.index(
            "v_rerun_disposition := 'DECLINED_BY_POLICY'"
        )
        carried = MOVE_BODY.index(
            'v_rerun_disposition := v_task.prepared_rerun_input_disposition'
        )
        assert never < declined < carried

    def test_completed_status_is_never_eligible(self) -> None:
        assert (
            "v_task.is_workflow_task OR p_terminal_status = 'COMPLETED'"
            in MOVE_BODY
        )

    def test_envelope_digest_is_copied_never_recomputed(self) -> None:
        assert 'v_rerun_digest := v_task.prepared_rerun_input_digest' in (
            MOVE_BODY
        )
        assert 'sha256(v_rerun' not in MOVE_BODY
        assert 'sha256(v_task.prepared' not in MOVE_BODY

    def test_row_count_assertions_guard_insert_and_delete(self) -> None:
        assert MOVE_BODY.count('GET DIAGNOSTICS') == 2

    def test_pending_written_only_for_deferred_variants(self) -> None:
        assert (
            'IF v_requires_deferred_phase2 AND v_task.is_workflow_task'
            in MOVE_BODY
        )

    def test_reservation_step_passes_the_live_rows_digest(self) -> None:
        assert 'IF v_task.idempotency_key_digest IS NOT NULL' in MOVE_BODY
        assert (
            'horsies_key_reservation_terminalize(\n'
            '            v_task.idempotency_key_digest, p_task_id, '
            'p_terminal_at\n'
            '        )'
        ) in MOVE_BODY

    def test_unsupported_kinds_raise_until_their_family_lands(self) -> None:
        assert 'has no move family yet' in MOVE_BODY
        for kind in (
            TerminalizationKind.COMPLETE_LOCKED,
            TerminalizationKind.COMPLETE_FUSED,
        ):
            assert f"'{kind.value}'" in MOVE_BODY


class TestAttemptEncoder:
    def test_never_renders_a_jsonb_array_to_text(self) -> None:
        assert 'jsonb_agg' not in ATTEMPT_ENCODER_DDL
        assert 'json_agg' not in ATTEMPT_ENCODER_DDL
        assert 'string_agg' in ATTEMPT_ENCODER_DDL

    def test_timestamps_are_floor_epoch_microsecond_bigints(self) -> None:
        assert (
            'floor(\n                        extract(epoch FROM a.started_at)'
            ' * 1000000\n                    )::bigint'
        ) in ATTEMPT_ENCODER_DDL

    def test_orders_by_attempt(self) -> None:
        assert "',' ORDER BY a.attempt" in ATTEMPT_ENCODER_DDL

    def test_empty_sequence_is_the_empty_array(self) -> None:
        assert "COALESCE(" in ATTEMPT_ENCODER_DDL
        assert "'' || ']'" not in ATTEMPT_ENCODER_DDL


class TestLiveCutover:
    def test_prepared_disposition_is_not_null_with_the_ratified_set(
        self,
    ) -> None:
        combined = '\n'.join(LIVE_CUTOVER_COLUMNS_DDL)
        assert 'prepared_rerun_input_disposition varchar(32) NOT NULL' in (
            combined
        )
        assert 'retain_rerun_input boolean NOT NULL' in combined
        assert 'idempotency_key_digest bytea' in combined

    def test_no_defaults_on_classification_columns(self) -> None:
        combined = '\n'.join(LIVE_CUTOVER_COLUMNS_DDL)
        assert 'DEFAULT' not in combined
