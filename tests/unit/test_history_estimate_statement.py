"""The planner-estimate companion to the history pagination count.

The estimate must run over EXACTLY the predicate the exact count runs —
a drifted condition list would estimate a different population than the
one the total describes — and its payload decode must fail closed: a
shape the decoder does not recognize becomes ``HistoryContractError``,
never a number.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.aggregates import (
    history_count_statement,
    history_estimate_statement,
    plan_rows_from_explain,
)
from horsies.core.history.reads.pages import HistoryScope, HistoryWindow

pytestmark = pytest.mark.unit

WINDOW = HistoryWindow(
    lower=datetime(2026, 1, 1, tzinfo=timezone.utc),
    upper=datetime(2026, 2, 1, tzinfo=timezone.utc),
)

SCOPED = HistoryScope(
    statuses=('COMPLETED', 'FAILED'),
    task_names=('alpha',),
    retried_only=True,
)


class TestEstimateStatement:
    """EXPLAIN over the exact count's own predicate."""

    @pytest.mark.parametrize('scope', [HistoryScope(), SCOPED])
    def test_predicate_and_parameters_match_the_exact_count(
        self, scope: HistoryScope
    ) -> None:
        count_sql, count_params = history_count_statement(WINDOW, scope)
        estimate_sql, estimate_params = history_estimate_statement(
            WINDOW, scope
        )

        count_predicate = count_sql.split(' WHERE ', 1)[1]
        estimate_predicate = estimate_sql.split(' WHERE ', 1)[1]
        assert estimate_predicate == count_predicate
        assert estimate_params == count_params

    def test_statement_is_explain_json_and_never_executes_the_scan(
        self,
    ) -> None:
        estimate_sql, _ = history_estimate_statement(WINDOW, HistoryScope())
        assert estimate_sql.startswith('EXPLAIN (FORMAT JSON) ')
        # EXPLAIN without ANALYZE: planning only. An accidental ANALYZE
        # would execute the very scan the estimate exists to avoid.
        assert 'ANALYZE' not in estimate_sql


class TestPlanRowsDecode:
    """Fail-closed decode of the EXPLAIN (FORMAT JSON) payload."""

    def test_parsed_integer_rows(self) -> None:
        assert plan_rows_from_explain([{'Plan': {'Plan Rows': 42}}]) == 42

    def test_parsed_float_rows_truncate(self) -> None:
        assert (
            plan_rows_from_explain([{'Plan': {'Plan Rows': 109_670.0}}])
            == 109_670
        )

    def test_rendered_text_payload(self) -> None:
        rendered = '[{"Plan": {"Plan Rows": 7, "Node Type": "Append"}}]'
        assert plan_rows_from_explain(rendered) == 7

    def test_extra_plan_keys_are_ignored(self) -> None:
        payload = [
            {
                'Plan': {
                    'Node Type': 'Gather',
                    'Plan Rows': 3,
                    'Plans': [{'Node Type': 'Seq Scan', 'Plan Rows': 3}],
                },
                'Planning Time': 0.2,
            }
        ]
        assert plan_rows_from_explain(payload) == 3

    @pytest.mark.parametrize(
        'payload',
        [
            None,
            [],
            {},
            [{'Plan': {}}],
            [{'Plan': {'Plan Rows': 'many'}}],
            [{'Plan': {'Plan Rows': True}}],
            [{'NotAPlan': {'Plan Rows': 1}}],
            'not json at all',
            '{"Plan": {"Plan Rows": 1}}',
        ],
        ids=[
            'none',
            'empty-list',
            'bare-object',
            'plan-without-rows',
            'non-numeric-rows',
            'boolean-rows',
            'wrong-top-key',
            'unparseable-text',
            'object-not-array',
        ],
    )
    def test_unrecognized_shapes_fail_closed(self, payload: object) -> None:
        with pytest.raises(HistoryContractError):
            plan_rows_from_explain(payload)
