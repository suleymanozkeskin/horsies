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
    return SimpleNamespace(facet=facet, facet_value=value, facet_count=count)


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
            _row('error_code', 'DOMAIN_b', 1),
            _row('error_code', 'DOMAIN_A', 1),
        ],
        [],
    )

    assert [f.value for f in facets.task_names] == ['a', 'B', 'c']
    assert [f.value for f in facets.error_codes] == ['DOMAIN_b', 'DOMAIN_A']


@pytest.mark.asyncio
async def test_the_code_list_is_capped_and_the_totals_are_not() -> None:
    rows = [_row('error_code', f'DOMAIN_{index}', 1) for index in range(40)]

    facets = await _facets(rows, [])

    assert len(facets.error_codes) == 30
    assert facets.error_category_totals == {ErrorCategory.DOMAIN.value: 40}


@pytest.mark.asyncio
async def test_the_selection_narrows_the_list_and_not_the_totals() -> None:
    rows = [
        _row('error_code', 'TASK_TIMEOUT', 9),
        _row('error_code', 'DOMAIN_A', 2),
    ]

    facets = await _facets(rows, [ErrorCategory.DOMAIN])

    assert [f.value for f in facets.error_codes] == ['DOMAIN_A']
    assert facets.error_category_totals[ErrorCategory.DOMAIN.value] == 2
    assert sum(facets.error_category_totals.values()) == 11


@pytest.mark.asyncio
async def test_the_cap_applies_after_the_selection() -> None:
    """30 selected codes show, even when other families rank above them."""
    rows = [_row('error_code', 'TASK_TIMEOUT', 100)]
    rows += [_row('error_code', f'DOMAIN_{index}', 1) for index in range(40)]

    facets = await _facets(rows, [ErrorCategory.DOMAIN])

    assert len(facets.error_codes) == 30
    assert all(f.value.startswith('DOMAIN_') for f in facets.error_codes)


@pytest.mark.asyncio
async def test_no_rows_give_empty_facets() -> None:
    facets = await _facets([], [])

    assert facets.workers == []
    assert facets.error_category_totals == {}


def test_the_statement_keeps_the_scope_out_of_the_code_list() -> None:
    """The category selection is not in the SQL: the totals need every code."""
    sql, params = facets_statement(
        window=_WINDOW,
        statuses=[TaskStatus.FAILED],
        retried_only=True,
    )

    assert sql.count('GROUPING SETS') == 2
    assert 'category' not in sql
    assert params['status_filter'] == ['FAILED']
    assert sql.count('retry_count > 0') == 2


def test_the_statement_scopes_only_the_history_side_by_the_window() -> None:
    sql, _ = facets_statement(window=_WINDOW, statuses=[], retried_only=False)

    assert sql.count('retention_anchor_at') == 2
    assert 'WHERE TRUE' in sql
