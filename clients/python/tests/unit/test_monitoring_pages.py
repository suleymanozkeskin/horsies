"""A monitoring page value is parsed once and cannot be illegal.

``parse`` is the one way that reports a refusal, as a value. Direct
construction of an illegal value is a code defect, so it crashes loud. A read
takes these values, so it cannot receive a bad page.
"""

from __future__ import annotations

import dataclasses

import pytest

from horsies.core.types.result import is_err
from horsies.monitoring import (
    MAX_BREAKDOWN_LIMIT,
    MAX_TASK_PAGE_LIMIT,
    MAX_TASK_PAGE_REACH,
    MAX_WORKFLOW_RUNS_LIMIT,
    BreakdownLimit,
    PageRefused,
    TaskPage,
    WorkflowRunsLimit,
)

pytestmark = [pytest.mark.unit]


class TestBounds:
    def test_bounds_are_the_published_numbers(self) -> None:
        assert MAX_TASK_PAGE_REACH == 500
        assert MAX_TASK_PAGE_LIMIT == 200
        assert MAX_BREAKDOWN_LIMIT == 500
        assert MAX_WORKFLOW_RUNS_LIMIT == 200


class TestTaskPageParse:
    @pytest.mark.parametrize(
        ('offset', 'limit'),
        [(0, 1), (0, 200), (300, 200), (499, 1)],
    )
    def test_accepts_a_page_inside_its_bounds(
        self, offset: int, limit: int,
    ) -> None:
        result = TaskPage.parse(offset, limit)
        assert not is_err(result)
        assert result.ok_value == TaskPage(offset=offset, limit=limit)
        assert result.ok_value.reach == offset + limit

    @pytest.mark.parametrize(
        ('offset', 'limit', 'reason'),
        [
            (-1, 50, 'offset must be at least 0; got -1'),
            (0, 0, 'limit must be between 1 and 200; got 0'),
            (0, -1, 'limit must be between 1 and 200; got -1'),
            (0, 201, 'limit must be between 1 and 200; got 201'),
            (301, 200, 'offset + limit must be <= 500; got 501'),
            (500, 1, 'offset + limit must be <= 500; got 501'),
        ],
    )
    def test_refuses_a_page_outside_its_bounds(
        self, offset: int, limit: int, reason: str,
    ) -> None:
        result = TaskPage.parse(offset, limit)
        assert is_err(result)
        assert result.err_value == PageRefused(reason=reason)


class TestBreakdownLimitParse:
    @pytest.mark.parametrize('limit', [1, 500])
    def test_accepts_a_limit_inside_its_bounds(self, limit: int) -> None:
        result = BreakdownLimit.parse(limit)
        assert not is_err(result)
        assert result.ok_value == BreakdownLimit(value=limit)

    @pytest.mark.parametrize('limit', [-1, 0, 501])
    def test_refuses_a_limit_outside_its_bounds(self, limit: int) -> None:
        result = BreakdownLimit.parse(limit)
        assert is_err(result)
        assert result.err_value == PageRefused(
            reason=f'limit must be between 1 and 500; got {limit}'
        )


class TestWorkflowRunsLimitParse:
    @pytest.mark.parametrize('limit', [1, 200])
    def test_accepts_a_limit_inside_its_bounds(self, limit: int) -> None:
        result = WorkflowRunsLimit.parse(limit)
        assert not is_err(result)
        assert result.ok_value == WorkflowRunsLimit(value=limit)

    @pytest.mark.parametrize('limit', [-1, 0, 201])
    def test_refuses_a_limit_outside_its_bounds(self, limit: int) -> None:
        result = WorkflowRunsLimit.parse(limit)
        assert is_err(result)
        assert result.err_value == PageRefused(
            reason=f'limit must be between 1 and 200; got {limit}'
        )


class TestIllegalConstruction:
    @pytest.mark.parametrize(
        ('offset', 'limit'),
        [(-1, 50), (0, 0), (0, 201), (301, 200)],
    )
    def test_task_page_crashes_loud(self, offset: int, limit: int) -> None:
        with pytest.raises(AssertionError):
            TaskPage(offset=offset, limit=limit)

    @pytest.mark.parametrize('limit', [0, 501])
    def test_breakdown_limit_crashes_loud(self, limit: int) -> None:
        with pytest.raises(AssertionError):
            BreakdownLimit(value=limit)

    @pytest.mark.parametrize('limit', [0, 201])
    def test_workflow_runs_limit_crashes_loud(self, limit: int) -> None:
        with pytest.raises(AssertionError):
            WorkflowRunsLimit(value=limit)


class TestFrozenFacts:
    def test_task_page_cannot_change_after_parse(self) -> None:
        page = TaskPage(offset=0, limit=1)
        with pytest.raises(dataclasses.FrozenInstanceError):
            page.limit = 500  # type: ignore[misc]

    def test_limits_cannot_change_after_parse(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            BreakdownLimit(value=1).value = 501  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            WorkflowRunsLimit(value=1).value = 201  # type: ignore[misc]
