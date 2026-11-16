"""History read primitives: column discipline and validations.

The listing surfaces must never carry an envelope column. Every exclusion
pin proves its presence half first: the excluded name is shown to exist in
the frozen DDL, so an exclusion assertion can actually fail.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.history.ddl.conditional import (
    GatedFragment,
    gated_fragment,
)
from horsies.core.history.ddl.tables import TASK_HISTORY_PARENT_DDL
from horsies.core.history.reads.aggregates import (
    HistoryStatusAggregate,
    history_status_aggregate_statement,
)
from horsies.core.history.reads.pages import (
    HISTORY_SUMMARY_COLUMNS,
    HistoryFacet,
    HistoryFacetQuery,
    HistoryPageQuery,
    HistoryScope,
    HistoryWindow,
    history_facet_statement,
    history_page_statement,
    history_sort_expression,
    history_scope_conditions,
)

pytestmark = [pytest.mark.unit]

UTC = timezone.utc
LOWER = datetime(2026, 8, 1, tzinfo=UTC)


def make_window() -> HistoryWindow:
    return HistoryWindow(lower=LOWER, upper=LOWER + timedelta(days=1))


ENVELOPE_COLUMNS = (
    'result_payload',
    'attempt_snapshot',
    'rerun_input_inline',
)


class TestColumnDiscipline:
    def test_presence_half_the_envelope_columns_are_real(self) -> None:
        frozen = TASK_HISTORY_PARENT_DDL
        gated = '\n'.join(
            fragment
            for kind in (
                GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS,
                GatedFragment.RERUN_INPUT_COLUMNS,
            )
            for fragment in gated_fragment(kind)
        )
        installed = frozen + gated
        for column in ENVELOPE_COLUMNS:
            assert column in installed, column

    def test_summary_columns_carry_no_envelope(self) -> None:
        for column in ENVELOPE_COLUMNS:
            assert column not in HISTORY_SUMMARY_COLUMNS

    def test_page_facet_and_aggregate_statements_carry_no_envelope(
        self,
    ) -> None:
        page_sql, _ = history_page_statement(
            HistoryPageQuery(window=make_window(), limit=100)
        )
        facet_sql, _ = history_facet_statement(
            HistoryFacetQuery(window=make_window(), facet=HistoryFacet.STATUS)
        )
        aggregate_sql, _ = history_status_aggregate_statement(
            HistoryStatusAggregate(window=make_window())
        )
        for sql in (page_sql, facet_sql, aggregate_sql):
            for column in ENVELOPE_COLUMNS:
                assert column not in sql, (column, sql)

    def test_every_statement_is_window_scoped(self) -> None:
        page_sql, page_params = history_page_statement(
            HistoryPageQuery(window=make_window(), limit=100)
        )
        facet_sql, _ = history_facet_statement(
            HistoryFacetQuery(window=make_window(), facet=HistoryFacet.STATUS)
        )
        aggregate_sql, _ = history_status_aggregate_statement(
            HistoryStatusAggregate(window=make_window())
        )
        for sql in (page_sql, facet_sql, aggregate_sql):
            assert 'terminal_at >= :window_lower' in sql
            assert 'terminal_at < :window_upper' in sql
        assert page_params['window_lower'] == LOWER


class TestBuilderValidation:
    def test_window_rejects_naive_and_inverted_bounds(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            HistoryWindow(lower=datetime(2026, 8, 1), upper=LOWER)
        with pytest.raises(ValueError, match='increasing'):
            HistoryWindow(lower=LOWER, upper=LOWER)

    def test_page_limit_bounds(self) -> None:
        with pytest.raises(ValueError, match='between 1 and 500'):
            HistoryPageQuery(window=make_window(), limit=0)
        with pytest.raises(ValueError, match='between 1 and 500'):
            HistoryPageQuery(window=make_window(), limit=501)
        with pytest.raises(ValueError, match='non-negative'):
            HistoryPageQuery(window=make_window(), limit=1, offset=-1)

    @pytest.mark.parametrize(
        'field',
        ['started_at', 'completed_at', 'failed_at', 'queue_s', 'exec_s'],
    )
    def test_nullable_descending_sorts_render_nulls_last(self, field: str) -> None:
        assert history_sort_expression(field, descending=True).endswith(
            'DESC NULLS LAST'
        )

    @pytest.mark.parametrize(
        'field',
        ['enqueued_at', 'status', 'task_name', 'queue_name', 'priority'],
    )
    def test_nonnullable_sorts_do_not_change_the_index_path(self, field: str) -> None:
        expression = history_sort_expression(field, descending=True)
        assert expression.endswith(' DESC')
        assert 'NULLS LAST' not in expression

    def test_page_statement_carries_the_taxonomy_dimension(self) -> None:
        """The page WHERE renders every scope dimension the counts do.

        Regression pin: the page query once took its own five filter
        fields and silently dropped `category_families` and
        `domain_complement` — a category-filtered list returned
        unfiltered history rows while the scoped count disagreed. The
        page statement must render from the same `HistoryScope`
        conditions as the aggregates, category arms included.
        """
        scope = HistoryScope(
            category_families=(('TASK_EXCEPTION', 'TASK_TIMEOUT'),),
            domain_complement=('TASK_EXCEPTION',),
        )
        page_sql, page_params = history_page_statement(
            HistoryPageQuery(window=make_window(), limit=10, scope=scope)
        )
        count_conditions, _ = history_scope_conditions(make_window(), scope)
        for condition in count_conditions:
            assert condition in page_sql, condition
        assert page_params['family_0_filter'] == [
            'TASK_EXCEPTION',
            'TASK_TIMEOUT',
        ]
        assert page_params['builtin_code_filter'] == ['TASK_EXCEPTION']

    def test_filters_bind_rather_than_interpolate(self) -> None:
        sql, parameters = history_page_statement(
            HistoryPageQuery(
                window=make_window(),
                limit=10,
                scope=HistoryScope(
                    statuses=('COMPLETED',),
                    task_names=('acme.report',),
                ),
            )
        )
        assert 'status = ANY(CAST(:status_filter AS text[]))' in sql
        assert (
            'task_name = ANY(CAST(:task_name_filter AS text[]))' in sql
        )
        assert "'COMPLETED'" not in sql
        assert parameters['status_filter'] == ['COMPLETED']
        assert parameters['task_name_filter'] == ['acme.report']
