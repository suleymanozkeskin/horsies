"""The copied baseline statement must not drift from the statement it cites.

The rerun-input terminalization collector compares the direct live-to-history
transition against the pre-consolidation same-row terminal update. That update
lives in a frozen control module whose header states it must not be imported by
production code or correctness tests, so the collector copies its text and
rebinds the relation name instead of importing and mutating it.

A copy with no test is a copy that silently ages. This module imports the frozen
original for one purpose only — comparing it with the copy — and asserts they
are identical once the single relation substitution is undone. Nothing here
asserts anything about product behaviour, and no runtime path reaches the
frozen statement through this module.
"""

from __future__ import annotations

import pytest

from tests.perf.legacy_terminalization_sql import MARK_TASK_FAILED_SQL
from tests.task_history_prototypes.rerun_terminalization_evidence import (
    BASELINE_TERMINAL_FAILURE_TEMPLATE,
    INLINE_BOUND_BYTES,
    BaselineRelationError,
    PairedSide,
    PreparedEnvelopeError,
    SideRelations,
    baseline_statement_text,
    source_baseline_relation,
    validate_prepared_envelope,
)
from tests.task_history_prototypes.schema import PrototypeSchema


class TestBaselineCopyMatchesFrozenControl:
    def test_copy_renders_to_the_frozen_statement_exactly(self) -> None:
        rendered = baseline_statement_text(relation=source_baseline_relation())

        assert rendered == MARK_TASK_FAILED_SQL.text

    def test_template_carries_exactly_one_substitution_point(self) -> None:
        assert BASELINE_TERMINAL_FAILURE_TEMPLATE.count('{relation}') == 1

    def test_frozen_statement_names_its_relation_once(self) -> None:
        # If the frozen statement ever named the table twice, a single
        # substitution would leave the copy half-bound to the deployed table
        # and the baseline side would measure the wrong relation.
        assert MARK_TASK_FAILED_SQL.text.count(source_baseline_relation()) == 1

    def test_rebinding_changes_only_the_relation(self) -> None:
        rendered = baseline_statement_text(relation='other_relation')

        assert rendered == MARK_TASK_FAILED_SQL.text.replace(
            source_baseline_relation(),
            'other_relation',
        )

    def test_empty_relation_is_rejected(self) -> None:
        with pytest.raises(BaselineRelationError):
            baseline_statement_text(relation='   ')


class TestPreparedEnvelopeBound:
    def test_envelope_at_the_inclusive_bound_is_accepted(self) -> None:
        validate_prepared_envelope(b'x' * INLINE_BOUND_BYTES)

    def test_envelope_above_the_bound_is_rejected(self) -> None:
        with pytest.raises(PreparedEnvelopeError):
            validate_prepared_envelope(b'x' * (INLINE_BOUND_BYTES + 1))

    def test_empty_envelope_is_rejected(self) -> None:
        with pytest.raises(PreparedEnvelopeError):
            validate_prepared_envelope(b'')


class TestSideRelationsAreDisjoint:
    def test_each_side_owns_distinct_relation_names(self) -> None:
        schema = PrototypeSchema('rerun_gate_probe')
        baseline = SideRelations(schema=schema, side=PairedSide.BASELINE)
        candidate = SideRelations(schema=schema, side=PairedSide.CANDIDATE)

        assert baseline.live_tasks != candidate.live_tasks
        assert baseline.live_attempts != candidate.live_attempts
