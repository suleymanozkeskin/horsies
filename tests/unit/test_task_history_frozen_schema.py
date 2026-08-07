"""The frozen schema program: column pinning, gating, and fragment order.

The history projection's exact columns are a ratified fact; these tests pin
the frozen 37 present and the 12 gated absent, so neither an accidental
column nor a premature gate emission can pass review silently. The
no-defaults rule and the enum-rendered terminalization kinds are asserted
against the DDL text itself — the same text the installer executes.
"""

from __future__ import annotations

import re

import pytest

from horsies.core.history.ddl.classes import finite_class_parent_name
from horsies.core.history.ddl.conditional import (
    GatedFragment,
    gated_fragment,
)
from horsies.core.history.ddl.fragments import (
    cutover_fragments,
    frozen_fragments,
)
from horsies.core.history.ddl.tables import (
    KEY_RESERVATIONS_DDL,
    TASK_HISTORY_PARENT_DDL,
    WORKFLOW_PHASE2_PENDING_DDL,
    WORKFLOW_PHASE2_QUARANTINE_DDL,
)
from horsies.core.lifecycle.operations import TerminalizationKind

pytestmark = [pytest.mark.unit]


FROZEN_HISTORY_COLUMNS = {
    'task_id',
    'task_name',
    'queue_name',
    'priority',
    'command_fingerprint_version',
    'command_fingerprint',
    'status',
    'terminalization_kind',
    'terminal_at',
    'retention_anchor_at',
    'retention_class_key',
    'sent_at',
    'enqueued_at',
    'claimed_at',
    'started_at',
    'created_at',
    'good_until',
    'retry_count',
    'max_retries',
    'last_claimed_worker_id',
    'last_worker_hostname',
    'last_worker_pid',
    'last_worker_process_name',
    'result_envelope_version',
    'result_codec',
    'result_content_type',
    'result_payload',
    'prior_result_payload',
    'result_digest',
    'error_code',
    'final_failed_reason',
    'input_digest',
    'rerun_of_task_id',
    'rerun_root_task_id',
    'workflow_id',
    'is_workflow_task',
    'history_schema_version',
}

GATED_ATTEMPT_COLUMNS = {
    'attempt_archive_version',
    'attempt_snapshot_codec',
    'attempt_snapshot_content_type',
    'attempt_snapshot',
    'attempt_snapshot_digest',
}

GATED_RERUN_INPUT_COLUMNS = {
    'rerun_input_disposition',
    'rerun_input_version',
    'rerun_input_codec',
    'rerun_input_content_type',
    'rerun_input_digest',
    'rerun_input_inline',
    'rerun_input_reference',
}


def defined_columns(ddl: str) -> set[str]:
    """Column names introduced by a CREATE TABLE or ADD COLUMN statement."""
    names: set[str] = set()
    for line in ddl.splitlines():
        match = re.match(
            r'\s*(?:ADD COLUMN\s+)?([a-z_][a-z0-9_]*)\s+'
            r'(?:uuid|text|varchar|integer|smallint|bigint|boolean|bytea|'
            r'timestamptz|interval)',
            line,
        )
        if match is not None:
            names.add(match.group(1))
    return names


class TestFrozenHistoryProjection:
    def test_exactly_the_frozen_columns(self) -> None:
        assert defined_columns(TASK_HISTORY_PARENT_DDL) == FROZEN_HISTORY_COLUMNS
        assert len(FROZEN_HISTORY_COLUMNS) == 37

    def test_gated_columns_are_absent(self) -> None:
        for column in GATED_ATTEMPT_COLUMNS | GATED_RERUN_INPUT_COLUMNS:
            assert column not in TASK_HISTORY_PARENT_DDL

    def test_no_column_defaults_on_authoritative_history(self) -> None:
        assert 'DEFAULT' not in TASK_HISTORY_PARENT_DDL

    def test_terminalization_kinds_render_from_the_enum(self) -> None:
        for kind in TerminalizationKind:
            assert f"'{kind.value}'" in TASK_HISTORY_PARENT_DDL

    def test_ratified_structural_checks_are_present(self) -> None:
        assert 'CHECK (retention_anchor_at = terminal_at)' in (
            TASK_HISTORY_PARENT_DDL
        )
        assert 'PARTITION BY LIST (retention_class_key)' in (
            TASK_HISTORY_PARENT_DDL
        )
        assert "status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')" in (
            TASK_HISTORY_PARENT_DDL
        )

    def test_administrative_cancel_result_exclusivity(self) -> None:
        assert 'CHECK (result_payload IS NULL OR prior_result_payload IS NULL)' in (
            TASK_HISTORY_PARENT_DDL
        )
        assert (
            "terminalization_kind <> 'CANCEL_ADMIN'" in TASK_HISTORY_PARENT_DDL
        )

    def test_identifier_columns_are_native_uuid(self) -> None:
        for column in ('task_id', 'rerun_of_task_id', 'rerun_root_task_id',
                       'workflow_id'):
            assert re.search(rf'{column} uuid', TASK_HISTORY_PARENT_DDL)


class TestGatedFragments:
    def test_every_gate_returns_its_columns(self) -> None:
        attempt = '\n'.join(
            gated_fragment(GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS)
        )
        assert defined_columns(attempt) == GATED_ATTEMPT_COLUMNS
        rerun = '\n'.join(gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS))
        assert defined_columns(rerun) == GATED_RERUN_INPUT_COLUMNS
        indexes = '\n'.join(
            gated_fragment(GatedFragment.RESERVATION_REGISTRY_INDEXES)
        )
        assert 'CREATE INDEX' in indexes

    def test_rerun_disposition_is_the_ratified_exhaustive_set(self) -> None:
        rerun = '\n'.join(gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS))
        for value in (
            'INLINE',
            'REFERENCE',
            'DECLINED_BY_POLICY',
            'OVER_BOUND',
            'NEVER_ELIGIBLE',
        ):
            assert f"'{value}'" in rerun
        assert 'MISSING_OBJECT' not in rerun

    def test_rerun_inline_bound_is_inclusive_65536(self) -> None:
        rerun = '\n'.join(gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS))
        assert 'octet_length(rerun_input_inline) <= 65536' in rerun

    def test_eligibility_states_never_eligible_for_completed_and_workflow(
        self,
    ) -> None:
        rerun = '\n'.join(gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS))
        assert (
            "(status <> 'COMPLETED' AND NOT is_workflow_task)"
            in rerun
        )


class TestFrozenFragmentList:
    def test_frozen_list_contains_no_gated_material(self) -> None:
        combined = '\n'.join(frozen_fragments())
        for column in GATED_ATTEMPT_COLUMNS | GATED_RERUN_INPUT_COLUMNS:
            assert column not in combined
        assert '_expiry_idx' not in combined

    def test_dependency_order(self) -> None:
        fragments = frozen_fragments()

        def position(marker: str) -> int:
            for index, fragment in enumerate(fragments):
                if marker in fragment:
                    return index
            raise AssertionError(f'no fragment contains {marker!r}')

        classes = position('CREATE TABLE horsies_retention_classes')
        parent = position('CREATE TABLE horsies_task_history (')
        forever = position('CREATE TABLE horsies_task_history_forever')
        catalog = position('CREATE TABLE horsies_task_history_leaf_catalog')
        quarantine = position(
            'CREATE TABLE horsies_workflow_phase2_quarantine'
        )
        pending = position('CREATE TABLE horsies_workflow_phase2_pending')
        registry = position('CREATE TABLE horsies_key_reservations')
        assert classes < parent < forever
        assert classes < catalog
        assert quarantine < pending
        assert pending < registry

    def test_no_default_history_partition_anywhere(self) -> None:
        combined = '\n'.join(frozen_fragments())
        assert 'DEFAULT PARTITION' not in combined
        assert 'PARTITION OF' in combined

    def test_cutover_fragments_carry_the_composite_pending_fkey(self) -> None:
        combined = '\n'.join(cutover_fragments())
        assert 'UNIQUE (id, workflow_id)' in combined
        assert 'FOREIGN KEY (workflow_node_row_id, workflow_id)' in combined


class TestSupportRelations:
    def test_pending_recovery_source_is_constrained_text(self) -> None:
        assert "recovery_source IN ('HISTORY', 'QUARANTINE')" in (
            WORKFLOW_PHASE2_PENDING_DDL
        )
        assert 'CREATE TYPE' not in WORKFLOW_PHASE2_PENDING_DDL

    def test_pending_history_locator_shape_check(self) -> None:
        assert 'history_class IS NOT NULL' in WORKFLOW_PHASE2_PENDING_DDL
        assert 'quarantine_task_id IS NULL' in WORKFLOW_PHASE2_PENDING_DDL

    def test_pending_has_no_row_defaults(self) -> None:
        assert 'DEFAULT' not in WORKFLOW_PHASE2_PENDING_DDL

    def test_quarantine_requires_the_result_envelope(self) -> None:
        assert 'result_payload bytea NOT NULL' in WORKFLOW_PHASE2_QUARANTINE_DDL
        assert 'octet_length(result_digest) = 32' in (
            WORKFLOW_PHASE2_QUARANTINE_DDL
        )

    def test_registry_window_bound_is_inclusive_30_days(self) -> None:
        assert "reservation_window <= interval '30 days'" in KEY_RESERVATIONS_DDL

    def test_registry_live_reservations_have_no_expiry(self) -> None:
        assert "(disposition = 'LIVE' AND expires_at IS NULL)" in (
            KEY_RESERVATIONS_DDL
        )
        assert "(disposition = 'TERMINAL' AND expires_at IS NOT NULL)" in (
            KEY_RESERVATIONS_DDL
        )


class TestClassRegistrationValidation:
    def test_parent_name_derivation(self) -> None:
        assert (
            finite_class_parent_name('finite_30d_v1')
            == 'horsies_task_history_finite_30d_v1'
        )

    def test_rejects_keys_that_break_relation_naming(self) -> None:
        with pytest.raises(ValueError, match='safe relation name'):
            finite_class_parent_name('30-days!')

    def test_rejects_overlong_derived_names(self) -> None:
        with pytest.raises(ValueError, match='safe relation name'):
            finite_class_parent_name('k' * 60)


class TestHistoryFoundationList:
    """The migration-facing subset: frozen minus the v28-owned entries.

    The subtraction is by imported identity, so these pins prove the
    boundary rather than re-deriving it: exactly the registry table and
    the reservation function program drop out, everything else keeps
    its frozen order.
    """

    def test_subtracts_exactly_the_v28_owned_entries(self) -> None:
        from horsies.core.history.ddl.fragments import (
            history_foundation_fragments,
        )
        from horsies.core.history.ddl.tables import KEY_RESERVATIONS_DDL
        from horsies.core.history.identity.reservations import (
            reservation_function_fragments,
        )

        foundation = history_foundation_fragments()
        v28_owned = (KEY_RESERVATIONS_DDL, *reservation_function_fragments())
        for owned in v28_owned:
            assert owned in frozen_fragments()
            assert owned not in foundation
        assert len(foundation) == len(frozen_fragments()) - len(v28_owned)

    def test_preserves_frozen_order(self) -> None:
        from horsies.core.history.ddl.fragments import (
            history_foundation_fragments,
        )

        frozen = frozen_fragments()
        indices = [frozen.index(f) for f in history_foundation_fragments()]
        assert indices == sorted(indices)

    def test_carries_the_history_parent_and_the_gate(self) -> None:
        from horsies.core.history.ddl.fragments import (
            history_foundation_fragments,
        )

        combined = '\n'.join(history_foundation_fragments())
        assert 'CREATE TABLE horsies_task_history (' in combined
        assert 'horsies_archive_access_gate' in combined
        assert 'CREATE TABLE horsies_key_reservations' not in combined
