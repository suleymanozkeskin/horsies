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
    cancellation_family_fragments,
    completion_family_fragments,
    disposition_case_expression,
    disposition_if_chain,
    expiry_family_fragments,
    failure_family_fragments,
    workflow_node_family_fragments,
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


class TestLadderSingleSource:
    """Both ladder renderings derive from one source; drift is impossible."""

    def test_single_row_move_embeds_the_generated_if_chain(self) -> None:
        assert disposition_if_chain('v_task', 'p_terminal_status') in MOVE_BODY

    def test_batch_embeds_the_generated_case_expression(self) -> None:
        batch = expiry_family_fragments()[1]
        assert disposition_case_expression('t', "'EXPIRED'") in batch

    def test_the_two_renderings_agree_rung_for_rung(self) -> None:
        chain = disposition_if_chain('r', 's')
        case = disposition_case_expression('r', 's')
        for fact in (
            "r.is_workflow_task OR s = 'COMPLETED'",
            'NOT r.retain_rerun_input',
            "'NEVER_ELIGIBLE'",
            "'DECLINED_BY_POLICY'",
            'r.prepared_rerun_input_disposition',
        ):
            assert fact in chain
            assert fact in case


class TestExpiryFamily:
    """The expiry family's wire shape against the production contract."""

    def test_move_case_covers_claimed_expiry(self) -> None:
        assert "'claimed-expiry projection disagrees'" in MOVE_BODY

    def test_owned_expiry_judges_the_deadline_from_the_capture(self) -> None:
        owned = expiry_family_fragments()[0]
        assert 'v_good_until IS NOT NULL AND v_good_until <= v_evaluated_at' in (
            owned
        )
        assert "'DEADLINE'" in owned
        assert "'good_until', v_good_until" in owned
        assert 'LOOP' not in owned

    def test_batch_validates_before_any_mutation(self) -> None:
        batch = expiry_family_fragments()[1]
        size_check = batch.index('p_batch_size must be a positive integer')
        fence = batch.index(
            'deferred workflow terminalization requires a result payload'
        )
        node_check = batch.index('lacks exactly one node row')
        uniqueness = batch.index('exists in multiple locations')
        insert = batch.index('INSERT INTO horsies_task_history')
        assert size_check < fence < node_check < uniqueness < insert

    def test_batch_discovery_is_oldest_first_pending_only(self) -> None:
        batch = expiry_family_fragments()[1]
        assert "status = 'PENDING'" in batch
        assert 'ORDER BY good_until ASC' in batch
        assert 'LIMIT p_batch_size' in batch

    def test_batch_outcomes_stream_from_live_before_the_deletes(self) -> None:
        batch = expiry_family_fragments()[1]
        outcome = batch.index("'APPLIED'::text")
        deletes = batch.index('DELETE FROM horsies_task_attempts')
        assert outcome < deletes
        assert 'FROM horsies_task_history h' not in batch

    def test_batch_counts_are_asserted_both_ways(self) -> None:
        batch = expiry_family_fragments()[1]
        assert 'batch history insert moved' in batch
        assert 'batch live delete removed' in batch

    def test_batch_notifies_per_moved_row(self) -> None:
        batch = expiry_family_fragments()[1]
        assert "pg_notify('task_done', u.tid::text)" in batch

    def test_reservation_transition_has_one_owner(self) -> None:
        batch = expiry_family_fragments()[1]
        assert 'horsies_key_reservation_terminalize_batch' in batch
        assert 'UPDATE horsies_key_reservations' not in batch
        assert 'UPDATE horsies_key_reservations' not in MOVE_BODY

    def test_batch_workflow_linkage_agrees_with_the_single_row_path(
        self,
    ) -> None:
        batch = expiry_family_fragments()[1]
        assert 'CASE WHEN t.is_workflow_task THEN n.workflow_id END' in batch


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
        stale = failure_family_fragments()[1]
        assert 'NULL::text, NULL::timestamptz' in stale

    def test_locked_failure_passes_reason_and_code_to_the_move(self) -> None:
        locked = failure_family_fragments()[0]
        assert 'p_result, p_error_code, p_failed_reason' in locked


class TestCodecCrossPins:
    """The DDL's codec literals are M3's constants, not free spellings."""

    def test_move_codec_literals_match_the_archive_constants(self) -> None:
        from horsies.core.history.archive.versions import (
            JSON_CONTENT_TYPE,
            JSON_UTF8_CODEC,
        )

        assert f"'{JSON_UTF8_CODEC}'" in MOVE_BODY
        assert f"'{JSON_CONTENT_TYPE}'" in MOVE_BODY
        assert "'json-utf8'" in MOVE_BODY
        assert MOVE_BODY.count("'json-utf8'") == 2
        assert MOVE_BODY.count("'application/json'") == 2


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
        availability = batch.index('horsies_assert_archive_available')
        discovery = batch.index('FOR UPDATE')
        uniqueness = batch.index('exists in multiple locations')
        insert = batch.index('INSERT INTO horsies_task_history')
        reservation = batch.index('horsies_key_reservation_terminalize_batch')
        outcome = batch.index("'APPLIED'::text")
        deletes = batch.index('DELETE FROM horsies_task_attempts')
        notify = batch.index("pg_notify('task_done'")
        assert (
            size_check < availability < discovery < uniqueness < insert
            < reservation < outcome < deletes < notify
        )

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
        assert 'requires a result payload' in expiry
        assert 'INSERT INTO horsies_workflow_phase2_pending' in expiry

    def test_non_deferred_batch_has_neither(self) -> None:
        sweep = cancellation_family_fragments()[2]
        assert 'requires a result payload' not in sweep
        assert 'INSERT INTO horsies_workflow_phase2_pending' not in sweep

    def test_non_deferred_linkage_is_deterministic_first_link(self) -> None:
        sweep = cancellation_family_fragments()[2]
        assert 'ORDER BY wt.id' in sweep
        assert 'task links to multiple workflows' in sweep

    def test_sweep_physical_scan_is_cursor_ordered(self) -> None:
        sweep = cancellation_family_fragments()[2]
        first_page = sweep.index('IF v_cursor_id IS NULL THEN')
        classification = sweep.index('FROM unnest(COALESCE(v_scan_ids')
        bounded_scan = sweep[first_page:classification]
        assert 'ORDER BY t.created_at, t.id' in bounded_scan
        assert 'LIMIT p_batch_size' in bounded_scan
        assert 'FOR UPDATE OF t SKIP LOCKED' in sweep[classification:]


class TestCancellationFamily:
    """The cancellation family's wire shape against the production contract."""

    def test_move_case_covers_the_family(self) -> None:
        assert "'administrative-cancel projection disagrees'" in MOVE_BODY
        assert "'orphan-cancel projection disagrees'" in MOVE_BODY

    def test_admin_cancel_rejects_workflow_tasks(self) -> None:
        assert "'COMPLETE_FUSED', 'CANCEL_ADMIN'" in MOVE_BODY

    def test_gate_8_swap_copies_prior_bytes_and_digests_them(self) -> None:
        assert 'v_prior_result_payload := CASE' in MOVE_BODY
        assert 'convert_to(v_task.result' in MOVE_BODY
        assert 'sha256(v_prior_result_payload)' in MOVE_BODY

    def test_admin_wire_owns_the_literals(self) -> None:
        admin = cancellation_family_fragments()[0]
        assert "'TASK_CANCELLED', 'Cancelled via monitoring API'" in admin
        assert 't.is_workflow_task = FALSE' in admin

    def test_node_lookup_shape_derives_from_deferral(self) -> None:
        strict = MOVE_BODY.index('IF v_requires_deferred_phase2 THEN')
        strict_lookup = MOVE_BODY.index('INTO STRICT v_workflow_node_row_id')
        optional_lookup = MOVE_BODY.index('ORDER BY n.id')
        assert strict < strict_lookup < optional_lookup
        assert 'task links to multiple workflows' in MOVE_BODY

    def test_orphan_refusal_carries_the_link_state(self) -> None:
        orphan = cancellation_family_fragments()[1]
        assert "'WORKFLOW_LINK_STATE'" in orphan
        assert "jsonb_build_object('node_status', v_node_status)" in orphan


class TestWorkflowNodeFamily:
    """The final six operations: postures, literals, and full coverage."""

    def test_every_terminalization_kind_is_accounted_for(self) -> None:
        from horsies.core.history.terminalization.move import (
            KIND_PROJECTIONS,
        )

        classified = {kind for kind, _, _, _ in KIND_PROJECTIONS}
        batch_only = {
            TerminalizationKind.EXPIRE_PENDING,
            TerminalizationKind.CANCEL_ORPHAN_SWEEP,
        }
        # Written only by the cutover relocation; no wire family, and
        # the single-row move's ELSE arm rejects it.
        relocation_only = {TerminalizationKind.LEGACY_TERMINAL}
        assert (
            classified | batch_only | relocation_only
            == set(TerminalizationKind)
        )
        assert not classified & (batch_only | relocation_only)

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
        availability = batch.index('horsies_assert_archive_available')
        lock = batch.index('ORDER BY t.id')
        assert distinct < availability < lock

    @pytest.mark.parametrize(
        'batch', [*workflow_node_family_fragments()[2:4]]
    )
    def test_id_keyed_misses_route_through_the_one_classifier(
        self, batch: str
    ) -> None:
        assert 'CROSS JOIN LATERAL horsies_terminalization_miss' in batch
        assert 'input.ordinality' in batch

    def test_all_four_batches_share_the_core_verbatim(self) -> None:
        marker = 'Per-row uniqueness guard through the staged mechanism'
        for batch in (
            expiry_family_fragments()[1],
            cancellation_family_fragments()[2],
            *workflow_node_family_fragments()[2:4],
            *workflow_node_family_fragments()[4:6],
        ):
            assert marker in batch
            assert 'horsies_key_reservation_terminalize_batch' in batch

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
        # Batch projection order pins error_code and final_failed_reason
        # (the two expressions after result_digest's CASE) to NULL;
        # whitespace-independent because the projection renders from the
        # shared column authority, one expression per line.
        for fragment in (
            workflow_node_family_fragments()[3],
            workflow_node_family_fragments()[5],
        ):
            assert re.search(
                r'sha256\(v_result_payload\) END,\s*NULL,\s*NULL,',
                fragment,
            )

    def test_requeued_pending_carve_out_is_single_only(self) -> None:
        single = workflow_node_family_fragments()[1]
        assert 'p_accepts_requeued_pending' in single
        for batch in workflow_node_family_fragments()[2:4]:
            assert 'p_accepts_requeued_pending' not in batch


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


class TestLiveStatusDomain:
    """The classifier's premise is a declared fragment, not fixture folklore."""

    def test_live_only_status_domain_is_production_ddl(self) -> None:
        from horsies.core.history.terminalization.live_cutover import (
            LIVE_STATUS_DOMAIN_DDL,
        )

        assert 'ADD CONSTRAINT horsies_tasks_live_status_only' in (
            LIVE_STATUS_DOMAIN_DDL
        )
        assert (
            "CHECK (status IN ('PENDING', 'CLAIMED', 'RUNNING'))"
            in LIVE_STATUS_DOMAIN_DDL
        )


class TestNodeRowIdentityType:
    """uuid end to end: frozen fragments, the move, never a stand-in guess."""

    def test_frozen_fragments_declare_uuid_node_identity(self) -> None:
        combined = '\n'.join(frozen_fragments())
        assert combined.count('workflow_node_row_id uuid NOT NULL') == 2
        assert 'workflow_node_row_id bigint' not in combined

    def test_move_declares_uuid_node_identity(self) -> None:
        assert 'v_workflow_node_row_id uuid;' in MOVE_BODY
        assert 'v_workflow_node_row_id bigint' not in MOVE_BODY


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


class TestTighteningRendering:
    """Three renderings, one structured authority, no text parsing."""

    def test_set_not_null_covers_exactly_the_declared_columns(self) -> None:
        from horsies.core.history.terminalization.live_cutover import (
            CUTOVER_COLUMNS,
            tightening_cutover_ddl,
        )

        rendered = '\n'.join(tightening_cutover_ddl())
        for column in CUTOVER_COLUMNS:
            clause = f'ALTER COLUMN {column.name} SET NOT NULL'
            assert (clause in rendered) is column.not_null, column.name

    def test_every_declared_check_lands_once(self) -> None:
        from horsies.core.history.terminalization.live_cutover import (
            CUTOVER_COLUMNS,
            tightening_cutover_ddl,
        )

        rendered = tightening_cutover_ddl()
        checked = [c for c in CUTOVER_COLUMNS if c.check is not None]
        constraint_statements = [
            s for s in rendered if 'ADD CONSTRAINT' in s
        ]
        # One per declared column check, plus the lineage pair.
        assert len(constraint_statements) == len(checked) + 1
        assert any(
            'rerun_lineage_pair' in s for s in constraint_statements
        )

    def test_final_and_transitional_share_the_column_set(self) -> None:
        from horsies.core.history.terminalization.live_cutover import (
            LIVE_CUTOVER_COLUMNS_DDL,
            cutover_column_definitions,
            transitional_cutover_columns_ddl,
        )

        final = '\n'.join(LIVE_CUTOVER_COLUMNS_DDL)
        transitional = transitional_cutover_columns_ddl()
        for name, _ in cutover_column_definitions():
            assert f'ADD COLUMN {name} ' in final
            assert f'ADD COLUMN IF NOT EXISTS {name} ' in transitional
