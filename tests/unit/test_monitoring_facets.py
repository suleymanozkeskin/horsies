"""The Python side of ``task_facets``: the rank is SQL's, the filter is here."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.history.reads.pages import HistoryWindow
from horsies.core.types.result import Ok, is_ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import ErrorCategory, task_facets
from horsies.monitoring.queries import facets_statement

pytestmark = pytest.mark.unit

_WINDOW = HistoryWindow(
    datetime(2026, 1, 1, tzinfo=UTC),
    datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=1),
)


def _row(facet: str, value: str, count: int) -> SimpleNamespace:
    return SimpleNamespace(
        facet=facet,
        facet_value=value,
        facet_count=count,
        category=None,
        category_total=None,
    )


def _code(value: str, count: int, category: ErrorCategory, total: int) -> SimpleNamespace:
    return SimpleNamespace(
        facet='error_code',
        facet_value=value,
        facet_count=count,
        category=category.value,
        category_total=total,
    )


def _broker(rows: Sequence[SimpleNamespace]) -> MagicMock:
    result = MagicMock()
    result.all.return_value = list(rows)
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result)
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    broker.select_resources = AsyncMock(
        return_value=Ok(SimpleNamespace(session_factory=factory)),
    )
    return broker


async def _facets(
    rows: Sequence[SimpleNamespace],
    categories: list[ErrorCategory],
) -> Any:
    result = await task_facets(
        _broker(rows),
        window=_WINDOW,
        statuses=[],
        error_categories=categories,
        retried_only=False,
    )
    assert is_ok(result)
    return result.ok_value


@pytest.mark.asyncio
async def test_the_rank_of_the_statement_is_kept() -> None:
    """Ties follow the database collation, so Python must not sort again."""
    facets = await _facets(
        [
            _row('task_name', 'a', 1),
            _row('task_name', 'B', 1),
            _row('task_name', 'c', 1),
            _code('DOMAIN_b', 1, ErrorCategory.DOMAIN, 2),
            _code('DOMAIN_A', 1, ErrorCategory.DOMAIN, 2),
        ],
        [],
    )

    assert [f.value for f in facets.task_names] == ['a', 'B', 'c']
    assert [f.value for f in facets.error_codes] == ['DOMAIN_b', 'DOMAIN_A']


@pytest.mark.asyncio
async def test_a_total_is_the_statement_total_and_not_the_rows_it_returned() -> None:
    """A category can hold more codes than the statement returns."""
    rows = [_code(f'DOMAIN_{index}', 1, ErrorCategory.DOMAIN, 40) for index in range(30)]

    facets = await _facets(rows, [])

    assert len(facets.error_codes) == 30
    assert facets.error_category_totals == {ErrorCategory.DOMAIN.value: 40}


@pytest.mark.asyncio
async def test_the_selection_narrows_the_list_and_not_the_totals() -> None:
    rows = [
        _code('WORKER_CRASHED', 9, ErrorCategory.OPERATIONAL, 9),
        _code('DOMAIN_A', 2, ErrorCategory.DOMAIN, 2),
    ]

    facets = await _facets(rows, [ErrorCategory.DOMAIN])

    assert [f.value for f in facets.error_codes] == ['DOMAIN_A']
    assert facets.error_codes[0].category == ErrorCategory.DOMAIN.value
    assert facets.error_category_totals == {
        ErrorCategory.OPERATIONAL.value: 9,
        ErrorCategory.DOMAIN.value: 2,
    }


@pytest.mark.asyncio
async def test_the_cap_applies_after_the_selection() -> None:
    """30 selected codes show, even when other families rank above them."""
    rows = [
        _code(f'OPS_{index}', 100, ErrorCategory.OPERATIONAL, 3000) for index in range(30)
    ]
    rows += [_code(f'DOMAIN_{index}', 1, ErrorCategory.DOMAIN, 30) for index in range(30)]

    facets = await _facets(rows, [ErrorCategory.DOMAIN])

    assert len(facets.error_codes) == 30
    assert all(f.value.startswith('DOMAIN_') for f in facets.error_codes)


@pytest.mark.asyncio
async def test_two_selected_families_merge_by_the_overall_rank() -> None:
    rows = [
        _code('DOMAIN_high', 9, ErrorCategory.DOMAIN, 10),
        _code('WORKER_CRASHED', 5, ErrorCategory.OPERATIONAL, 5),
        _code('DOMAIN_low', 1, ErrorCategory.DOMAIN, 10),
    ]

    facets = await _facets(rows, [ErrorCategory.DOMAIN, ErrorCategory.OPERATIONAL])

    assert [f.value for f in facets.error_codes] == [
        'DOMAIN_high',
        'WORKER_CRASHED',
        'DOMAIN_low',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'selected',
    [[], [ErrorCategory.OPERATIONAL, ErrorCategory.DOMAIN]],
    ids=['no_selection', 'two_families'],
)
async def test_the_merged_families_stay_under_the_cap(
    selected: list[ErrorCategory],
) -> None:
    """The statement bounds each family, so two of them can return 60 codes."""
    rows = [
        _code(f'OPS_{index}', 100, ErrorCategory.OPERATIONAL, 3000) for index in range(30)
    ]
    rows += [_code(f'DOMAIN_{index}', 1, ErrorCategory.DOMAIN, 30) for index in range(30)]

    facets = await _facets(rows, selected)

    assert [f.value for f in facets.error_codes] == [f'OPS_{index}' for index in range(30)]


@pytest.mark.asyncio
async def test_no_rows_give_empty_facets() -> None:
    facets = await _facets([], [])

    assert facets.workers == []
    assert facets.error_category_totals == {}


def test_the_statement_bounds_the_code_list_per_category() -> None:
    """An error code is a free string, so the rows must not follow the codes."""
    sql, params = facets_statement(
        window=_WINDOW,
        statuses=[TaskStatus.FAILED],
        retried_only=True,
    )

    assert sql.count('GROUPING SETS') == 2
    assert 'category_rank <= :error_limit' in sql
    assert params['error_limit'] == 30
    assert params['status_filter'] == ['FAILED']
    assert sql.count('retry_count > 0') == 2


def test_the_statement_names_each_built_in_family() -> None:
    sql, params = facets_statement(window=_WINDOW, statuses=[], retried_only=False)

    for category in ErrorCategory:
        assert f"'{category.value}'" in sql
    assert 'WORKER_CRASHED' in params['category_operational_codes']
    assert "ELSE 'DOMAIN'" in sql


def test_the_statement_scopes_only_the_history_side_by_the_window() -> None:
    sql, _ = facets_statement(window=_WINDOW, statuses=[], retried_only=False)

    assert sql.count('retention_anchor_at') == 2
    assert 'WHERE TRUE' in sql
