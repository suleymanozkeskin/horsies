"""The completion-family move: structural pins over the generated SQL.

The reconciled ten-step order, the isolated envelope and attempt blocks,
the copied-never-recomputed digest, the STRICT node lookup, and the
staged uniqueness guard are contract facts the integration suite proves
behaviorally; these pins make each one fail fast and name itself when a
refactor touches the body text.
"""

from __future__ import annotations

import re

import pytest

from horsies.core.lifecycle.operations import TerminalizationKind
from tests.baseline_sql import (
    baseline_sql,
    baseline_table,
    baseline_type,
    installed_function,
    installed_function_position,
)

pytestmark = [pytest.mark.unit]


# The functions under test, as the migration chain installs them, grouped as
# the families they were written in.
MOVE_BODY = installed_function('horsies_move_task_to_history')
"""The single move that the writers call: it takes the row the writer locked."""
BATCH_MOVE_BODY = installed_function('horsies_move_tasks_to_history')
"""The batch move stage that every batch writer calls with the ids it locked."""
_BATCH_MOVE_CALL = 'horsies_move_tasks_to_history('
ATTEMPT_ENCODER_FUNCTION = 'horsies_encode_attempt_archive'
COMPLETE_FUSED_FUNCTION = 'horsies_complete_task_fused'
COMPLETE_FUSED_BATCH_FUNCTION = 'horsies_complete_tasks_fused'
FINISH_FAILED_ATTEMPT_FUNCTION = 'horsies_finish_failed_attempt'
FINISH_STALE_ATTEMPT_FUNCTION = 'horsies_finish_stale_attempt'
FINISH_WORKFLOW_SUCCESS_FUNCTION = 'horsies_finish_workflow_success'


def _functions(*names: str) -> list[str]:
    return [installed_function(name) for name in names]


def completion_family_fragments() -> list[str]:
    return [
        installed_function(ATTEMPT_ENCODER_FUNCTION),
        MOVE_BODY,
        installed_function(FINISH_WORKFLOW_SUCCESS_FUNCTION),
        installed_function(COMPLETE_FUSED_FUNCTION),
    ]


def failure_family_fragments() -> list[str]:
    return _functions(FINISH_FAILED_ATTEMPT_FUNCTION, FINISH_STALE_ATTEMPT_FUNCTION)


def expiry_family_fragments() -> list[str]:
    return _functions('horsies_expire_owned_claim', 'horsies_expire_pending_tasks')


def cancellation_family_fragments() -> list[str]:
    return _functions(
        'horsies_cancel_task',
        'horsies_cancel_owned_orphan',
        'horsies_cancel_orphaned_tasks',
    )


def workflow_node_family_fragments() -> list[str]:
    return _functions(
        'horsies_abandon_owned_node',
        'horsies_cancel_owned_node',
        'horsies_abandon_owned_nodes',
        'horsies_cancel_owned_nodes',
        'horsies_abandon_nodes_of_paused_workflows',
        'horsies_cancel_nodes_of_cancelled_workflow',
    )


def completion_batch_fragments() -> list[str]:
    return _functions(COMPLETE_FUSED_BATCH_FUNCTION)


def outcome_fragments() -> list[str]:
    return [
        baseline_type('horsies_terminalization_outcome'),
        installed_function('horsies_terminalization_miss'),
    ]


OUTCOME_TYPE_DDL = baseline_type('horsies_terminalization_outcome')
MISS_CLASSIFIER_DDL = installed_function('horsies_terminalization_miss')
ATTEMPT_ENCODER_DDL = installed_function(ATTEMPT_ENCODER_FUNCTION)


class TestOutcomeLayer:
    def test_outcome_type_task_id_is_uuid(self) -> None:
        assert 'task_id uuid' in OUTCOME_TYPE_DDL

    def test_miss_classifier_consults_provenance_without_live(self) -> None:
        # The one-argument provenance reader reads history only.
        assert (
            'horsies_task_provenance_staged(p_task_id) AS p'
            in MISS_CLASSIFIER_DDL
        )

    def test_miss_order_absent_then_already_applied_then_foreign(self) -> None:
        absent = MISS_CLASSIFIER_DDL.index("'TASK_ABSENT'")
        already = MISS_CLASSIFIER_DDL.index("'ALREADY_APPLIED'")
        foreign = MISS_CLASSIFIER_DDL.index("'FOREIGN_TERMINALIZATION'")
        lost = MISS_CLASSIFIER_DDL.index("'LOST_CLAIM'")
        assert absent < already < foreign < lost

    def test_provenance_cannot_answer_live_after_a_live_miss(self) -> None:
        # No live flag is passed, so the reader cannot report the live table.
        assert 'horsies_task_provenance_staged(p_task_id, ' not in MISS_CLASSIFIER_DDL

    def test_fragment_order(self) -> None:
        first, second = outcome_fragments()
        assert 'CREATE TYPE' in first
        assert 'CREATE FUNCTION' in second


class TestMoveStructure:
    def test_reconciled_step_order(self) -> None:
        # The writer takes the advisory lock and reads the row it passes; the
        # move starts at the shared leaf lock.
        leaf_lock = MOVE_BODY.index('pg_advisory_xact_lock_shared')
        uniqueness = MOVE_BODY.index('horsies_task_history_duplicate_staged')
        insert = MOVE_BODY.index('INSERT INTO horsies_task_history')
        deletes = MOVE_BODY.index('DELETE FROM horsies_task_attempts')
        notify = MOVE_BODY.index("pg_notify('task_done'")
        assert leaf_lock < uniqueness < insert < deletes < notify

    def test_uniqueness_guard_uses_the_duplicate_guard(self) -> None:
        # The guard never probes the live table; the miss classifier keeps
        # the full provenance probe.
        assert 'horsies_task_history_duplicate_staged(p_task.id)' in MOVE_BODY
        assert 'horsies_task_provenance_staged' not in MOVE_BODY

    def test_node_lookup_is_strict_with_ownership_predicate(self) -> None:
        assert 'INTO STRICT v_workflow_node_row_id' in MOVE_BODY
        assert 'FROM horsies_workflow_tasks' in MOVE_BODY

    def test_envelope_ladder_eligibility_before_policy(self) -> None:
        never = MOVE_BODY.index("v_rerun_disposition := 'NEVER_ELIGIBLE'")
        declined = MOVE_BODY.index(
            "v_rerun_disposition := 'DECLINED_BY_POLICY'"
        )
        built = MOVE_BODY.index(
            'v_rerun_envelope := horsies_encode_rerun_input('
        )
        assert never < declined < built

    def test_completed_status_is_never_eligible(self) -> None:
        assert (
            "p_task.is_workflow_task OR p_terminal_status = 'COMPLETED'"
            in MOVE_BODY
        )

    def test_envelope_digest_is_of_the_built_envelope_once(self) -> None:
        assert MOVE_BODY.count('sha256(v_rerun_envelope)') == 1
        assert 'v_rerun_digest := sha256(v_rerun_envelope)' in MOVE_BODY
        assert 'prepared_rerun_input' not in MOVE_BODY

    def test_row_count_assertions_guard_insert_and_delete(self) -> None:
        assert MOVE_BODY.count('GET DIAGNOSTICS') == 2

    def test_pending_written_only_for_deferred_variants(self) -> None:
        assert (
            'IF v_requires_deferred_phase2 AND p_task.is_workflow_task'
            in MOVE_BODY
        )

    def test_reservation_step_passes_the_live_rows_digest(self) -> None:
        assert 'IF p_task.idempotency_key_digest IS NOT NULL' in MOVE_BODY
        assert (
            'horsies_key_reservation_terminalize('
            'p_task.idempotency_key_digest, p_task.id, p_terminal_at)'
        ) in MOVE_BODY

    def test_unsupported_kinds_raise_until_their_family_lands(self) -> None:
        assert 'has no single-row move' in MOVE_BODY
        for kind in (
            TerminalizationKind.COMPLETE_LOCKED,
            TerminalizationKind.COMPLETE_FUSED,
        ):
            assert f"'{kind.value}'" in MOVE_BODY


class TestLockOrderInvariant:
    """Advisory before any row lock on the task, in every wire function.

    Two sessions taking the two orders on the same task deadlock; the
    invariant is global and every future family inherits it, so each wire
    body must acquire the advisory lock ahead of its guard select.
    """

    @pytest.mark.parametrize(
        'body',
        [
            *completion_family_fragments()[2:],
            *failure_family_fragments(),
            expiry_family_fragments()[0],
            *cancellation_family_fragments()[:2],
            *workflow_node_family_fragments()[:2],
        ],
    )
    def test_wire_functions_take_the_advisory_lock_first(
        self, body: str
    ) -> None:
        advisory = body.index('pg_advisory_xact_lock')
        guard_select = body.index('FROM horsies_tasks')
        assert advisory < guard_select

    def test_batch_takes_no_advisory_lock_and_never_waits(self) -> None:
        batch = expiry_family_fragments()[1]
        assert 'pg_advisory_xact_lock' not in batch
        assert 'FOR UPDATE SKIP LOCKED' in batch


class TestExpiryFamily:
    """The expiry family's wire shape against the production contract."""

    def test_move_case_covers_claimed_expiry(self) -> None:
        assert "'claimed-expiry projection disagrees'" in MOVE_BODY

    def test_owned_expiry_judges_the_deadline_from_the_capture(self) -> None:
        owned = ' '.join(expiry_family_fragments()[0].split())
        assert (
            'v_task.good_until IS NOT NULL'
            ' AND v_task.good_until <= v_capture.evaluated_at'
        ) in owned
        assert "'DEADLINE'" in owned
        assert "'good_until', v_task.good_until" in owned
        assert 'LOOP' not in owned

    def test_batch_validates_before_any_mutation(self) -> None:
        batch = expiry_family_fragments()[1]
        size_check = batch.index('p_batch_size must be a positive integer')
        stage_call = batch.index(_BATCH_MOVE_CALL)
        assert size_check < stage_call
        fence = BATCH_MOVE_BODY.index(
            'deferred workflow terminalization requires a result payload'
        )
        node_check = BATCH_MOVE_BODY.index('lacks exactly one node row')
        uniqueness = BATCH_MOVE_BODY.index('exists in multiple locations')
        insert = BATCH_MOVE_BODY.index('INSERT INTO horsies_task_history')
        assert fence < node_check < uniqueness < insert

    def test_batch_discovery_is_oldest_first_pending_only(self) -> None:
        batch = expiry_family_fragments()[1]
        assert "status = 'PENDING'" in batch
        assert 'ORDER BY good_until ASC' in batch
        assert 'LIMIT p_batch_size' in batch

    def test_batch_outcomes_stream_from_live_before_the_deletes(self) -> None:
        batch = expiry_family_fragments()[1]
        outcome = batch.index("'APPLIED'::text")
        stage_call = batch.index(_BATCH_MOVE_CALL)
        assert outcome < stage_call
        assert 'FROM horsies_task_history h' not in batch

    def test_batch_counts_are_asserted_both_ways(self) -> None:
        assert 'batch history insert moved' in BATCH_MOVE_BODY
        assert 'batch live delete removed' in BATCH_MOVE_BODY

    def test_batch_notifies_per_moved_row(self) -> None:
        assert "pg_notify('task_done', u.id::text)" in BATCH_MOVE_BODY

    def test_reservation_transition_has_one_owner(self) -> None:
        batch = expiry_family_fragments()[1]
        assert 'horsies_key_reservation_terminalize_batch' in BATCH_MOVE_BODY
        for body in (batch, BATCH_MOVE_BODY, MOVE_BODY):
            assert 'UPDATE horsies_key_reservations' not in body

    def test_batch_workflow_linkage_agrees_with_the_single_row_path(
        self,
    ) -> None:
        assert (
            'CASE WHEN t.is_workflow_task THEN n.workflow_id END'
            in BATCH_MOVE_BODY
        )


class TestFailureFamily:
    """The failure family's wire shape against the production contract."""

    def test_move_case_covers_both_failure_kinds(self) -> None:
        assert "'running-failure projection disagrees'" in MOVE_BODY
        assert "'stale-failure projection disagrees'" in MOVE_BODY

    def test_stale_capture_reads_the_runner_heartbeat_in_one_snapshot(
        self,
    ) -> None:
        stale = failure_family_fragments()[1]
        capture = stale.index("h.role = 'runner'")
        judged = stale.index('NOW()')
        refusal = stale.index('jsonb_build_object')
        assert capture < judged < refusal
        for field in (
            'last_heartbeat_at',
            'started_at',
            'finalizing_at',
            'stale_after_ms',
            'finalizing_stale_after_ms',
            'evaluated_at',
        ):
            assert f"'{field}'" in stale

    def test_stale_miss_path_uses_null_claim_parameters(self) -> None:
        """The miss passes the owner parameters with the fence of the
        evidence. BEAT_AGE refuses an owner, so its miss gets NULL claim
        parameters and no fence; OWNER_GONE fences on the owner and its
        claim generation."""
        stale = failure_family_fragments()[1]
        beat_age = stale.index("WHEN 'BEAT_AGE' THEN")
        no_owner = stale.index('num_nulls(p_owner_worker_id, p_owner_claimed_at) <> 2')
        owner_gone = stale.index("WHEN 'OWNER_GONE' THEN", beat_age)
        assert beat_age < no_owner < owner_gone
        assert (
            "CASE p_evidence WHEN 'OWNER_GONE' THEN 'OWNER_AND_GENERATION' ELSE 'NONE' END,\n"
            '            p_owner_worker_id, p_owner_claimed_at'
        ) in stale

    def test_the_failed_attempt_passes_reason_and_code_to_the_move(self) -> None:
        failed = failure_family_fragments()[0]
        assert 'p_result, p_error_code, p_failed_reason' in failed


class TestNoCodecLiterals:
    """The history format version implies the codec and content type."""

    def test_the_move_stores_no_codec_literal(self) -> None:
        assert "'json-utf8'" not in MOVE_BODY
        assert "'application/json'" not in MOVE_BODY


class TestBatchBuilder:
    """The set-wise skeleton's invariants, pinned once at the builder.

    Both discovery batches are builder output; the stage-order pin runs
    against each rendering but is written here once, because the builder
    is load-bearing infrastructure that owns its own invariants.
    """

    @pytest.mark.parametrize(
        'batch',
        [expiry_family_fragments()[1], cancellation_family_fragments()[2]],
    )
    def test_stage_order_is_the_builders(self, batch: str) -> None:
        size_check = batch.index('p_batch_size must be a positive integer')
        discovery = batch.index('FOR UPDATE')
        outcome = batch.index("'APPLIED'::text")
        stage_call = batch.index(_BATCH_MOVE_CALL)
        assert size_check < discovery < outcome < stage_call

    def test_batch_move_stage_order(self) -> None:
        uniqueness = BATCH_MOVE_BODY.index('exists in multiple locations')
        insert = BATCH_MOVE_BODY.index('INSERT INTO horsies_task_history')
        reservation = BATCH_MOVE_BODY.index('horsies_key_reservation_terminalize_batch')
        deletes = BATCH_MOVE_BODY.index('DELETE FROM horsies_task_attempts')
        notify = BATCH_MOVE_BODY.index("pg_notify('task_done'")
        assert uniqueness < insert < reservation < deletes < notify

    @pytest.mark.parametrize(
        'batch',
        [expiry_family_fragments()[1], cancellation_family_fragments()[2]],
    )
    def test_no_batch_takes_advisory_locks(self, batch: str) -> None:
        assert 'pg_advisory_xact_lock' not in batch
        assert 'FOR UPDATE SKIP LOCKED' in batch or (
            'FOR UPDATE OF t2 SKIP LOCKED' in batch
        ) or (
            'FOR UPDATE OF t SKIP LOCKED' in batch
        )

    def test_deferred_batch_has_fence_and_pending(self) -> None:
        expiry = expiry_family_fragments()[1]
        assert "'EXPIRED', 'EXPIRE_PENDING'" in expiry
        assert 'requires a result payload' in BATCH_MOVE_BODY
        assert 'INSERT INTO horsies_workflow_phase2_pending' in BATCH_MOVE_BODY

    def test_non_deferred_batch_has_neither(self) -> None:
        sweep = cancellation_family_fragments()[2]
        assert 'requires a result payload' not in sweep
        assert 'INSERT INTO horsies_workflow_phase2_pending' not in sweep

    def test_linkage_is_the_unique_task_link(self) -> None:
        # ix_horsies_workflow_tasks_task_id is unique: a task has at most one
        # node row, so the stage joins it by task id.
        assert (
            'LEFT JOIN horsies_workflow_tasks AS n ON n.task_id = t.id'
            in BATCH_MOVE_BODY
        )

    def test_sweep_physical_scan_is_cursor_ordered(self) -> None:
        sweep = cancellation_family_fragments()[2]
        first_page = sweep.index('IF v_cursor_id IS NULL THEN')
        classification = sweep.index('FROM unnest(COALESCE(v_scan_ids')
        bounded_scan = sweep[first_page:classification]
        assert 'ORDER BY t.created_at, t.id' in bounded_scan
        assert 'LIMIT p_batch_size' in bounded_scan
        assert 'FOR UPDATE OF t SKIP LOCKED' in sweep[classification:]


_BATCH_WRITERS = (
    expiry_family_fragments()[1],
    cancellation_family_fragments()[2],
    *workflow_node_family_fragments()[2:],
)
_BATCH_WRITER_IDS = (
    'expire_pending',
    'cancel_orphan_sweep',
    'abandon_owned_nodes',
    'cancel_owned_nodes',
    'abandon_paused_sweep',
    'cancel_cancelled_sweep',
)


class TestLadderOrder:
    """The single-row move and the batch move rank the rerun dispositions
    the same way: never eligible, declined by policy, over the bound,
    inline."""

    @pytest.mark.parametrize(
        'body',
        [MOVE_BODY, BATCH_MOVE_BODY],
        ids=['move', 'batch_move'],
    )
    def test_rungs_appear_in_ladder_order(self, body: str) -> None:
        position = body.index("'NEVER_ELIGIBLE'")
        for rung in ("'DECLINED_BY_POLICY'", "'OVER_BOUND'", "'INLINE'"):
            following = body.index(rung, position)
            assert following > position, rung
            position = following
        assert 'retain_rerun_input' in body
        assert '> 65536' in body
        assert 'horsies_encode_rerun_input(' in body


class TestSnapshotAndDigestReuse:
    """Each move encodes an attempt snapshot once and hashes a result once."""

    @pytest.mark.parametrize('writer', _BATCH_WRITERS, ids=_BATCH_WRITER_IDS)
    def test_every_batch_writer_moves_through_the_stage_once(
        self, writer: str
    ) -> None:
        assert writer.count(_BATCH_MOVE_CALL) == 1
        assert f'{ATTEMPT_ENCODER_FUNCTION}(' not in writer
        assert 'INSERT INTO horsies_task_history' not in writer

    def test_batch_encodes_each_snapshot_once(self) -> None:
        batch = BATCH_MOVE_BODY
        assert batch.count(f'{ATTEMPT_ENCODER_FUNCTION}(') == 1
        assert 'WITH attempt_data AS MATERIALIZED' in batch
        assert 'JOIN attempt_data ON attempt_data.id = t.id' in batch
        assert 'sha256(attempt_data.snapshot)' in batch

    def test_batch_hashes_the_batch_result_once(self) -> None:
        """The payload is one value per batch, so its digest is computed
        once, not once per moved row or per deferred record."""
        batch = BATCH_MOVE_BODY
        assert batch.count('sha256(v_result_payload)') == 1
        assert 'v_result_digest bytea;' in batch
        assert 'v_result_digest := sha256(v_result_payload)' in batch
        pending = batch[
            batch.index('INSERT INTO horsies_workflow_phase2_pending'):
        ]
        assert '1, v_result_digest,' in pending

    def test_move_hashes_the_result_once(self) -> None:
        assert MOVE_BODY.count('sha256(v_result_payload)') == 1
        assert 'prior_result_payload' not in MOVE_BODY
        pending = MOVE_BODY[
            MOVE_BODY.index('INSERT INTO horsies_workflow_phase2_pending'):
        ]
        assert '1, v_result_digest,' in pending

class TestCancellationFamily:
    """The cancellation family's wire shape against the production contract."""

    def test_move_case_covers_the_family(self) -> None:
        assert "'administrative-cancel projection disagrees'" in MOVE_BODY
        assert "'orphan-cancel projection disagrees'" in MOVE_BODY

    def test_admin_cancel_rejects_workflow_tasks(self) -> None:
        assert "'COMPLETE_FUSED', 'CANCEL_ADMIN'" in MOVE_BODY

    def test_admin_wire_owns_the_literals(self) -> None:
        """A workflow task is refused on the locked row, before the move."""
        admin = cancellation_family_fragments()[0]
        assert "'TASK_CANCELLED', 'Cancelled via monitoring API'" in admin
        refusal = admin.index('IF v_task.is_workflow_task THEN')
        assert "'WORKFLOW_TASK'::text" in admin[refusal:]
        assert refusal < admin.index('horsies_move_task_to_history(')

    def test_node_lookup_shape_derives_from_deferral(self) -> None:
        strict = MOVE_BODY.index('IF v_requires_deferred_phase2 THEN')
        strict_lookup = MOVE_BODY.index('INTO STRICT v_workflow_node_row_id')
        optional_lookup = MOVE_BODY.index('INTO v_workflow_node_row_id, v_workflow_id')
        assert strict < strict_lookup < optional_lookup
        # Both lookups probe the unique task_id link.
        assert MOVE_BODY.count('WHERE n.task_id = p_task.id') == 2

    def test_orphan_refusal_carries_the_link_state(self) -> None:
        orphan = cancellation_family_fragments()[1]
        assert "'WORKFLOW_LINK_STATE'" in orphan
        assert "jsonb_build_object('node_status', v_capture.node_status)" in orphan


class TestWorkflowNodeFamily:
    """The final six operations: postures, literals, and full coverage."""

    def test_every_terminalization_kind_is_accounted_for(self) -> None:
        """The single-row move handles every kind but the two batch-only ones."""
        classified = {
            kind for kind in TerminalizationKind if f"'{kind.value}'" in MOVE_BODY
        }
        batch_only = {
            TerminalizationKind.EXPIRE_PENDING,
            TerminalizationKind.CANCEL_ORPHAN_SWEEP,
        }
        assert classified | batch_only == set(TerminalizationKind)
        assert not classified & batch_only

    @pytest.mark.parametrize(
        'batch', [*workflow_node_family_fragments()[2:4]]
    )
    def test_id_keyed_batches_wait_in_global_order(self, batch: str) -> None:
        assert 'pg_advisory_xact_lock' not in batch
        assert 'ORDER BY t.id' in batch
        lock_span_start = batch.index('ORDER BY t.id')
        lock_span_end = batch.index(') locked;')
        assert 'FOR UPDATE' in batch[lock_span_start:lock_span_end]
        assert 'SKIP LOCKED' not in batch[lock_span_start:lock_span_end]

    @pytest.mark.parametrize(
        'batch', [*workflow_node_family_fragments()[2:4]]
    )
    def test_id_keyed_preconditions_precede_any_work(self, batch: str) -> None:
        distinct = batch.index('batch task ids must be distinct')
        lock = batch.index('ORDER BY t.id')
        assert distinct < lock

    @pytest.mark.parametrize(
        'batch', [*workflow_node_family_fragments()[2:4]]
    )
    def test_id_keyed_misses_route_through_the_one_classifier(
        self, batch: str
    ) -> None:
        assert 'CROSS JOIN LATERAL horsies_terminalization_miss' in batch
        assert 'input.ordinality' in batch

    def test_all_batches_share_the_one_stage(self) -> None:
        for batch in _BATCH_WRITERS:
            assert batch.count(_BATCH_MOVE_CALL) == 1
            assert 'horsies_key_reservation_terminalize_batch' not in batch
        assert 'horsies_key_reservation_terminalize_batch' in BATCH_MOVE_BODY

    @pytest.mark.parametrize(
        'sweep', [*workflow_node_family_fragments()[4:6]]
    )
    def test_sweeps_are_scope_bounded_not_size_bounded(
        self, sweep: str
    ) -> None:
        assert 'p_batch_size' not in sweep
        assert 'p_workflow_ids uuid[]' in sweep
        assert 'SKIP LOCKED' in sweep

    def test_sweep_predicates_match_production(self) -> None:
        paused = workflow_node_family_fragments()[4]
        cancelled = workflow_node_family_fragments()[5]
        assert "w.status = 'PAUSED'" in paused
        assert "wt.status IN ('ENQUEUED', 'RUNNING')" in paused
        assert "t2.status = 'CLAIMED'" in paused
        # EXPIRED propagates exactly as CANCELLED: one sweep serves both.
        assert "w.status IN ('CANCELLED', 'EXPIRED')" in cancelled
        assert "wt.status = 'ENQUEUED'" in cancelled
        assert "t2.status IN ('PENDING', 'CLAIMED', 'RUNNING')" in cancelled

    def test_workflow_cancel_kinds_archive_null_summaries(self) -> None:
        import re

        cancel_single = workflow_node_family_fragments()[1]
        assert 'NULL, NULL, NULL' in cancel_single
        # The batch writers pass a NULL result, error_code and
        # final_failed_reason to the batch move stage.
        for fragment in (
            workflow_node_family_fragments()[3],
            workflow_node_family_fragments()[5],
        ):
            assert re.search(
                r"horsies_move_tasks_to_history\(\s*\w+,\s*'CANCELLED',\s*'\w+',"
                r'\s*v_terminal_at,\s*NULL,\s*NULL,\s*NULL\s*\)',
                fragment,
            )

    def test_requeued_pending_carve_out_is_single_only(self) -> None:
        single = workflow_node_family_fragments()[1]
        assert 'p_accepts_requeued_pending' in single
        for batch in workflow_node_family_fragments()[2:4]:
            assert 'p_accepts_requeued_pending' not in batch


class TestAttemptEncoder:
    """Attempt archive version 2: ``[attempts, open]`` from the live row."""

    def test_never_renders_a_jsonb_array_to_text(self) -> None:
        assert 'jsonb_agg' not in ATTEMPT_ENCODER_DDL
        assert 'json_agg' not in ATTEMPT_ENCODER_DDL
        assert 'string_agg' in ATTEMPT_ENCODER_DDL

    def test_timestamps_are_floor_epoch_microsecond_bigints(self) -> None:
        for column in (
            'a.started_at',
            'a.finished_at',
            'a.enqueued_at',
            'a.claimed_at',
            'a.first_claimed_at',
            'a.placed_at',
            'a.finalizing_at',
            'a.last_seen_at',
            'p_task.first_claimed_at',
            'p_task.finalizing_at',
        ):
            assert f'floor(extract(epoch FROM {column}) * 1000000)::bigint' in ATTEMPT_ENCODER_DDL

    def test_orders_by_attempt(self) -> None:
        assert "',' ORDER BY a.attempt" in ATTEMPT_ENCODER_DDL

    def test_empty_sequence_is_the_empty_array(self) -> None:
        assert "COALESCE(" in ATTEMPT_ENCODER_DDL
        assert "'' || ']'" not in ATTEMPT_ENCODER_DDL

    def test_the_encoder_takes_the_live_row(self) -> None:
        """The open attempt comes from the row the caller read, so the
        encoder takes that row, not an id it would read again."""
        assert 'horsies_encode_attempt_archive(p_task horsies_tasks) RETURNS bytea' in (
            ATTEMPT_ENCODER_DDL
        )
        assert 'WHERE a.task_id = p_task.id' in ATTEMPT_ENCODER_DDL

    def test_the_open_record_is_null_when_the_current_attempt_has_its_row(self) -> None:
        open_rule = ATTEMPT_ENCODER_DDL.index('bool_or(a.attempt = p_task.retry_count + 1)')
        assert ATTEMPT_ENCODER_DDL.index("THEN 'null'", open_rule) > open_rule
        assert 'p_task.attempt_claims' in ATTEMPT_ENCODER_DDL

    def test_the_moves_store_version_two(self) -> None:
        assert 'horsies_encode_attempt_archive(p_task)' in MOVE_BODY
        assert '1, 2, v_attempt_snapshot, sha256(v_attempt_snapshot),' in MOVE_BODY
        assert 'horsies_encode_attempt_archive(t) AS snapshot' in BATCH_MOVE_BODY
        assert '1, 2, attempt_data.snapshot, sha256(attempt_data.snapshot),' in BATCH_MOVE_BODY


class TestAttemptPhaseRecord:
    """Each attempt writer fills the phase record in the statement that
    writes the attempt row; the retries start the claim count again."""

    _PHASE_COLUMNS = (
        'phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at, '
        'finalizing_at, recorded_by, last_seen_at'
    )

    @pytest.mark.parametrize(
        'name',
        [
            COMPLETE_FUSED_FUNCTION,
            FINISH_WORKFLOW_SUCCESS_FUNCTION,
            FINISH_FAILED_ATTEMPT_FUNCTION,
            FINISH_STALE_ATTEMPT_FUNCTION,
        ],
    )
    def test_every_attempt_writer_names_the_phase_columns(self, name: str) -> None:
        body = ' '.join(installed_function(name).split())
        assert self._PHASE_COLUMNS in body
        assert 'last_seen_at = EXCLUDED.last_seen_at;' in body

    @pytest.mark.parametrize(
        'name',
        [COMPLETE_FUSED_FUNCTION, FINISH_WORKFLOW_SUCCESS_FUNCTION, FINISH_FAILED_ATTEMPT_FUNCTION],
    )
    def test_a_worker_writer_records_its_placement(self, name: str) -> None:
        body = installed_function(name)
        assert 'p_placement_kind text,\n    p_slot_wait_us bigint\n)' in body
        assert f"horsies_require_attempt_placement('{name}', p_placement_kind, p_slot_wait_us)" in body
        assert "v_ctx.finalizing_at, 'WORKER', NULL" in body
        assert body.index('horsies_require_attempt_placement') < body.index('pg_advisory_xact_lock')

    def test_the_cancel_records_a_running_attempt_before_the_move_and_the_notice(self) -> None:
        """The cancel of a RUNNING task writes the CANCELLED attempt with the
        writer CANCEL, no placement and no last sign of life, then moves the
        row, then sends the cancel notice."""
        body = ' '.join(installed_function('horsies_cancel_task').split())
        assert self._PHASE_COLUMNS in body
        assert "v_row.finalizing_at, 'CANCEL', NULL" in body
        insert = body.index("'CANCELLED', FALSE")
        move = body.index('horsies_move_task_to_history(')
        notice = body.index("pg_notify( 'horsies_task_cancel'")
        assert insert < move < notice

    def test_recovery_records_the_last_sign_of_life_and_no_placement(self) -> None:
        body = installed_function(FINISH_STALE_ATTEMPT_FUNCTION)
        assert "'RECOVERY', GREATEST(v_ctx.started_at, v_ctx.finalizing_at, v_ctx.newest_runner_beat)" in body
        assert 'p_placement_kind' not in body

    @pytest.mark.parametrize('name', [FINISH_FAILED_ATTEMPT_FUNCTION, FINISH_STALE_ATTEMPT_FUNCTION])
    def test_a_retry_starts_the_claim_count_again(self, name: str) -> None:
        body = installed_function(name)
        retry = body.index("'RETRY_SCHEDULED'::text")
        reset = body.rindex('attempt_claims = 0,', 0, retry)
        assert body.index('first_claimed_at = NULL,', reset) < retry

    def test_the_claim_counts_on_the_current_attempt(self) -> None:
        body = installed_function('horsies_claim_candidates')
        assert 'attempt_claims = t.attempt_claims + 1,' in body
        assert (
            'first_claimed_at = CASE WHEN t.attempt_claims = 0 THEN now() ELSE t.first_claimed_at END,'
        ) in body

    def test_a_new_row_starts_with_no_claim(self) -> None:
        body = ' '.join(installed_function('horsies_insert_task_rows').split())
        assert 'trace_id, attempt_claims, first_claimed_at )' in body
        assert 'r.trace_carrier, r.trace_id, 0, NULL FROM unnest(' in body


class TestLiveStatusDomain:
    """The classifier's premise is a declared constraint, not fixture folklore."""

    def test_live_only_status_domain_is_a_baseline_check(self) -> None:
        sql = ' '.join(baseline_sql().split())
        assert 'CONSTRAINT horsies_tasks_status_live CHECK (' in sql
        assert "status IN ('PENDING', 'CLAIMED', 'RUNNING')" in sql


class TestNodeRowIdentityType:
    """uuid end to end: frozen fragments, the move, never a stand-in guess."""

    def test_the_baseline_tables_declare_uuid_node_identity(self) -> None:
        sql = baseline_sql()
        assert sql.count('workflow_node_row_id uuid NOT NULL') == 2
        assert 'workflow_node_row_id bigint' not in sql

    def test_move_declares_uuid_node_identity(self) -> None:
        assert 'v_workflow_node_row_id uuid;' in MOVE_BODY
        assert 'v_workflow_node_row_id bigint' not in MOVE_BODY


class TestLiveRerunColumns:
    """The live columns the rerun path classifies by: typed, no defaults."""

    def test_classification_columns_are_typed(self) -> None:
        table = _live_tasks_table()
        assert 'retain_rerun_input boolean NOT NULL' in table
        assert 'idempotency_key_digest bytea' in table

    def test_no_defaults_on_the_live_table(self) -> None:
        assert 'DEFAULT' not in _live_tasks_table()


def _live_tasks_table() -> str:
    return baseline_table('horsies_tasks')


class TestBatchedFusedCompletion:
    """The batched plain-success completion is a loop over the fused
    completion: each row keeps that function's lock, fence, move and kind."""

    BODY = completion_batch_fragments()[0]

    def test_each_row_calls_the_fused_completion_with_its_own_arguments(
        self,
    ) -> None:
        """Each row passes its own id, claim generation and result; the
        worker and the wake are those of the batch."""
        assert (
            f'FROM {COMPLETE_FUSED_FUNCTION}(\n'
            '            v_item.id, p_worker_id, v_item.claimed_at, v_item.result, p_wake, '
            'v_item.placement_kind,\n'
            '            v_item.slot_wait_us\n'
            '        ) AS c;'
        ) in self.BODY

    def test_rows_run_in_sorted_task_id_order_at_their_input_ordinality(
        self,
    ) -> None:
        assert 'WITH ORDINALITY' in self.BODY
        loop = self.BODY.index('FOR v_item IN')
        assert self.BODY.index('ORDER BY u.id', loop) < self.BODY.index(
            'LOOP', loop
        )
        assert 'SELECT c.task_id, v_item.position,' in self.BODY

    def test_the_function_takes_no_lock_and_no_subtransaction_of_its_own(
        self,
    ) -> None:
        assert 'pg_advisory_xact_lock' not in self.BODY
        assert 'FOR UPDATE' not in self.BODY
        assert 'EXCEPTION\n' not in self.BODY.split('BEGIN', 1)[1].replace(
            'RAISE EXCEPTION',
            '',
        )

    def test_the_arrays_are_checked_before_any_row_runs(self) -> None:
        loop = self.BODY.index('FOR v_item IN')
        for check in (
            'batch arrays, worker and wake must be non-NULL',
            'a batch holds 1 to 64 rows',
            'batch array lengths differ',
            'batch task ids must be distinct and non-NULL',
            'every row needs its claim generation',
            'horsies_require_attempt_placement(',
        ):
            assert self.BODY.index(check) < loop

    def test_the_install_creates_it_after_the_fused_completion(self) -> None:
        fused = installed_function_position(COMPLETE_FUSED_FUNCTION)
        batched = installed_function_position(COMPLETE_FUSED_BATCH_FUNCTION)
        assert fused < batched

_LOCKING_READ = re.compile(
    r'FROM horsies_tasks(?: (?P<alias>\w+))?\n    WHERE (?:(?P=alias)\.)?(?P<where>.*?)\n    FOR UPDATE;',
    re.DOTALL,
)
"""A locking read of one live row; ``where`` is its predicate without the
table alias."""


def _function_ddl(name: str) -> str:
    return installed_function(name)


class TestLockingReads:
    """A completion or failure function reads its row by id alone and checks
    the fence on the locked row, so only the primary key serves the read,
    whatever the statistics say."""

    @pytest.mark.parametrize(
        'name',
        [
            COMPLETE_FUSED_FUNCTION,
            FINISH_FAILED_ATTEMPT_FUNCTION,
            FINISH_WORKFLOW_SUCCESS_FUNCTION,
        ],
    )
    def test_the_locking_read_names_only_the_id(self, name: str) -> None:
        reads = [match.group('where') for match in _LOCKING_READ.finditer(_function_ddl(name))]

        assert reads == ['id = p_task_id']

