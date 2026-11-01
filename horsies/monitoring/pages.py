"""Bounded page values for the monitoring reads.

A read takes one of these values and not plain integers, so it cannot receive
an illegal page. ``parse`` is the one way that reports a refusal, as a
``PageRefused`` value. Each type also asserts its bounds at construction, so
building an illegal value directly is a code defect and crashes loud.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Self

from horsies.core.types.result import Err, Ok, Result

MAX_TASK_PAGE_REACH: Final = 500
"""Maximum ``offset + limit`` accepted by the bounded two-side merge."""

MAX_TASK_PAGE_LIMIT: Final = 200
"""Maximum rows on one ``list_tasks`` page."""

MAX_BREAKDOWN_LIMIT: Final = 500
"""Maximum groups in one ``task_breakdown`` answer."""

MAX_WORKFLOW_RUNS_LIMIT: Final = 200
"""Maximum runs in one ``list_workflow_runs`` answer."""


@dataclass(frozen=True, slots=True)
class PageRefused:
    """A page value outside its bounds. No query ran."""

    reason: str


def _limit_refused(limit: int, maximum: int) -> PageRefused:
    return PageRefused(reason=f'limit must be between 1 and {maximum}; got {limit}')


@dataclass(frozen=True, slots=True)
class TaskPage:
    """One ``list_tasks`` page: its offset, limit and reach are in bounds."""

    offset: int
    limit: int

    def __post_init__(self) -> None:
        assert self.offset >= 0, f'illegal page offset {self.offset}'
        assert 1 <= self.limit <= MAX_TASK_PAGE_LIMIT, (
            f'illegal page limit {self.limit}'
        )
        assert self.reach <= MAX_TASK_PAGE_REACH, (
            f'illegal page reach {self.reach}'
        )

    @property
    def reach(self) -> int:
        """How far past the first row this page reads."""
        return self.offset + self.limit

    @classmethod
    def parse(cls, offset: int, limit: int) -> Result[Self, PageRefused]:
        """Build a page, or name the first bound that it breaks."""
        if offset < 0:
            return Err(PageRefused(reason=f'offset must be at least 0; got {offset}'))
        if not 1 <= limit <= MAX_TASK_PAGE_LIMIT:
            return Err(_limit_refused(limit, MAX_TASK_PAGE_LIMIT))
        reach = offset + limit
        if reach > MAX_TASK_PAGE_REACH:
            return Err(PageRefused(
                reason=f'offset + limit must be <= {MAX_TASK_PAGE_REACH}; got {reach}'
            ))
        return Ok(cls(offset=offset, limit=limit))


@dataclass(frozen=True, slots=True)
class BreakdownLimit:
    """How many groups ``task_breakdown`` returns."""

    value: int

    def __post_init__(self) -> None:
        assert 1 <= self.value <= MAX_BREAKDOWN_LIMIT, (
            f'illegal breakdown limit {self.value}'
        )

    @classmethod
    def parse(cls, limit: int) -> Result[Self, PageRefused]:
        """Build a limit, or refuse one outside its bounds."""
        if not 1 <= limit <= MAX_BREAKDOWN_LIMIT:
            return Err(_limit_refused(limit, MAX_BREAKDOWN_LIMIT))
        return Ok(cls(value=limit))


@dataclass(frozen=True, slots=True)
class WorkflowRunsLimit:
    """How many runs ``list_workflow_runs`` returns."""

    value: int

    def __post_init__(self) -> None:
        assert 1 <= self.value <= MAX_WORKFLOW_RUNS_LIMIT, (
            f'illegal workflow runs limit {self.value}'
        )

    @classmethod
    def parse(cls, limit: int) -> Result[Self, PageRefused]:
        """Build a limit, or refuse one outside its bounds."""
        if not 1 <= limit <= MAX_WORKFLOW_RUNS_LIMIT:
            return Err(_limit_refused(limit, MAX_WORKFLOW_RUNS_LIMIT))
        return Ok(cls(value=limit))
