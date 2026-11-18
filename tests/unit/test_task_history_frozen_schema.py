"""The history relations of the baseline: column pinning and their rules.

The history projection's exact columns are a ratified fact; these tests pin
the columns of `horsies_task_history` as the baseline migration creates them,
so neither an accidental column nor a column the baseline removed can pass
review silently. The no-defaults rule and the cross-column rules are
asserted against the SQL text that the install runs.
"""

from __future__ import annotations

import re

import pytest

from tests.baseline_sql import baseline_function_statements, baseline_sql, baseline_table
from tests.unit.test_terminal_writer_inventory import history_insert_columns

pytestmark = [pytest.mark.unit]


HISTORY_COLUMNS = {
    'task_id',
    'task_name',
    'queue_name',
    'priority',
    'command_fingerprint_version',
    'command_fingerprint',
    'retention_class_key',
    'is_workflow_task',
    'workflow_id',
    'rerun_of_task_id',
    'rerun_root_task_id',
    'sent_at',
    'created_at',
    'enqueued_at',
    'claimed_at',
    'started_at',
    'good_until',
    'terminal_at',
    'status',
    'terminalization_kind',
    'retry_count',
    'max_retries',
    'last_claimed_worker_id',
    'last_worker_hostname',
    'last_worker_pid',
    'last_worker_process_name',
    'error_code',
    'final_failed_reason',
    'result_envelope_version',
    'result_payload',
    'result_digest',
    'history_schema_version',
    'attempt_archive_version',
    'attempt_snapshot',
    'attempt_snapshot_digest',
    'rerun_input_disposition',
    'rerun_input_version',
    'rerun_input_digest',
    'rerun_input_inline',
}

REMOVED_COLUMNS = {
    'retention_anchor_at',
    'result_codec',
    'result_content_type',
    'prior_result_payload',
    'input_digest',
    'attempt_snapshot_codec',
    'attempt_snapshot_content_type',
    'rerun_input_codec',
    'rerun_input_content_type',
    'rerun_input_reference',
}
"""v61 history columns the baseline does not have: the format version implies
the codecs, the RANGE key is terminal_at, an administrative cancel stores no
result, nothing read the input digest, and no envelope is stored by
reference."""

_COLUMN_LINE = re.compile(r'^    ([a-z_][a-z0-9_]*) ', re.MULTILINE)

HISTORY_DDL = baseline_table('horsies_task_history')
PENDING_DDL = baseline_table('horsies_workflow_phase2_pending')
QUARANTINE_DDL = baseline_table('horsies_workflow_phase2_quarantine')
KEY_RESERVATIONS_DDL = baseline_table('horsies_key_reservations')


def defined_columns(table_ddl: str) -> set[str]:
    """Column names of a baseline CREATE TABLE statement."""
    return {
        name for name in _COLUMN_LINE.findall(table_ddl) if name != 'CONSTRAINT'
    }


def _normalised(sql: str) -> str:
    return ' '.join(sql.split())


class TestHistoryProjection:
    def test_exactly_the_baseline_columns(self) -> None:
        assert defined_columns(HISTORY_DDL) == HISTORY_COLUMNS
        assert len(HISTORY_COLUMNS) == 39

    def test_removed_columns_are_absent(self) -> None:
        assert not HISTORY_COLUMNS & REMOVED_COLUMNS
        for column in REMOVED_COLUMNS:
            assert re.search(rf'\b{column}\b', HISTORY_DDL) is None, column

    def test_no_column_defaults_on_authoritative_history(self) -> None:
        assert 'DEFAULT' not in HISTORY_DDL

    def test_partitioned_by_class_with_no_default_partition(self) -> None:
        assert HISTORY_DDL.rstrip().endswith(
            'PARTITION BY LIST (retention_class_key);'
        )
        assert 'DEFAULT PARTITION' not in baseline_sql()

    def test_administrative_cancel_stores_no_result(self) -> None:
        assert (
            "terminalization_kind <> 'CANCEL_ADMIN' OR "
            '(result_payload IS NULL AND result_digest IS NULL)'
        ) in _normalised(HISTORY_DDL)

    def test_identifier_columns_are_native_uuid(self) -> None:
        for column in ('task_id', 'rerun_of_task_id', 'rerun_root_task_id',
                       'workflow_id'):
            assert re.search(rf'\b{column} uuid', HISTORY_DDL), column


class TestRerunInputColumns:
    def test_rerun_disposition_is_the_ratified_exhaustive_set(self) -> None:
        shape = HISTORY_DDL[HISTORY_DDL.index('horsies_task_history_rerun_input_shape'):]
        for value in (
            'INLINE',
            'DECLINED_BY_POLICY',
            'OVER_BOUND',
            'NEVER_ELIGIBLE',
        ):
            assert f"'{value}'" in shape
        assert "'REFERENCE'" not in shape
        assert 'MISSING_OBJECT' not in shape

    def test_eligibility_states_never_eligible_for_completed_and_workflow(
        self,
    ) -> None:
        shape = _normalised(
            HISTORY_DDL[HISTORY_DDL.index('horsies_task_history_rerun_input_shape'):]
        )
        assert shape.count("AND status <> 'COMPLETED' AND NOT is_workflow_task") == 2


class TestHistoryWriters:
    """Every history insert names every history column: no column takes a
    database default, so a column left out fails the insert."""

    def test_every_history_insert_names_every_column(self) -> None:
        inserts = [
            (name, columns)
            for name, body in baseline_function_statements()
            for columns in history_insert_columns(body)
        ]
        assert inserts
        for name, columns in inserts:
            assert len(columns) == len(set(columns)), name
            assert set(columns) == HISTORY_COLUMNS, name


class TestSupportRelations:
    def test_pending_recovery_source_is_constrained_text(self) -> None:
        pending = _normalised(PENDING_DDL)
        assert "recovery_source = 'HISTORY'" in pending
        assert "recovery_source = 'QUARANTINE'" in pending
        assert 'CREATE TYPE' not in PENDING_DDL

    def test_pending_history_locator_shape_check(self) -> None:
        assert 'history_class IS NOT NULL' in PENDING_DDL
        assert 'quarantine_task_id IS NULL' in PENDING_DDL

    def test_pending_has_no_row_defaults(self) -> None:
        assert 'DEFAULT' not in PENDING_DDL

    def test_pending_node_key_is_the_composite_node_identity(self) -> None:
        assert 'FOREIGN KEY (workflow_node_row_id, workflow_id)' in PENDING_DDL
        assert 'REFERENCES horsies_workflow_tasks (id, workflow_id)' in (
            PENDING_DDL
        )

    def test_quarantine_requires_the_result_envelope(self) -> None:
        assert 'result_payload bytea NOT NULL' in QUARANTINE_DDL
        assert 'result_digest = sha256(result_payload)' in QUARANTINE_DDL

    def test_registry_window_bound_is_inclusive_30_days(self) -> None:
        assert "reservation_window <= interval '30 days'" in KEY_RESERVATIONS_DDL

    def test_registry_live_reservations_have_no_expiry(self) -> None:
        assert "(disposition = 'LIVE' AND expires_at IS NULL)" in (
            KEY_RESERVATIONS_DDL
        )
        assert "(disposition = 'TERMINAL' AND expires_at IS NOT NULL)" in (
            KEY_RESERVATIONS_DDL
        )


