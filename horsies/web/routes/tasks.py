# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""Task read endpoints.

Static paths are declared before the parameterized one so ``/tasks/stats``
is never captured as a task id.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, status

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_err
from horsies.monitoring import (
    Breakdown,
    Facets,
    StatusCount,
    TaskDetail,
    TaskListPage,
    get_task_detail,
    list_tasks,
    task_breakdown,
    task_facets,
    task_stats,
)
from horsies.web.routes._common import (
    parse_group_by,
    parse_sort_by,
    parse_statuses,
    query_failed,
)


def build_router(broker: PostgresBroker) -> APIRouter:
    """Build the ``/tasks`` router bound to one broker."""
    router = APIRouter(prefix='/tasks', tags=['tasks'])

    @router.get('/stats')
    async def read_stats(
        task_name: list[str] = Query(default=[]),
        queue: list[str] = Query(default=[]),
        worker: list[str] = Query(default=[]),
        error_code: list[str] = Query(default=[]),
        retried_only: bool = Query(default=False),
    ) -> list[StatusCount]:
        """Task counts by status. Deliberately unscoped by status: the cards
        this feeds are the status selector."""
        result = await task_stats(
            broker,
            task_names=task_name,
            queues=queue,
            workers=worker,
            error_codes=error_code,
            retried_only=retried_only,
        )
        if is_err(result):
            raise query_failed('Task stats', result.err_value)
        return result.ok_value

    @router.get('/facets')
    async def read_facets(
        task_status: list[str] = Query(default=[], alias='status'),
        retried_only: bool = Query(default=False),
    ) -> Facets:
        """Distinct filter values with counts, scoped only by the coarse filters."""
        result = await task_facets(
            broker,
            statuses=parse_statuses(task_status),
            retried_only=retried_only,
        )
        if is_err(result):
            raise query_failed('Task facets', result.err_value)
        return result.ok_value

    @router.get('/breakdown')
    async def read_breakdown(
        group_by: str = Query(default='worker'),
        task_status: list[str] = Query(default=[], alias='status'),
        task_name: list[str] = Query(default=[]),
        queue: list[str] = Query(default=[]),
        worker: list[str] = Query(default=[]),
        error_code: list[str] = Query(default=[]),
        retried_only: bool = Query(default=False),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> Breakdown:
        """Per-group status pivot with a rollup TOTAL row."""
        result = await task_breakdown(
            broker,
            group_by=parse_group_by(group_by),
            statuses=parse_statuses(task_status),
            task_names=task_name,
            queues=queue,
            workers=worker,
            error_codes=error_code,
            retried_only=retried_only,
            limit=limit,
        )
        if is_err(result):
            raise query_failed('Task breakdown', result.err_value)
        return result.ok_value

    @router.get('')
    async def read_tasks(
        task_status: list[str] = Query(default=[], alias='status'),
        task_name: list[str] = Query(default=[]),
        queue: list[str] = Query(default=[]),
        worker: list[str] = Query(default=[]),
        error_code: list[str] = Query(default=[]),
        retried_only: bool = Query(default=False),
        sort_by: str = Query(default='enqueued_at'),
        sort_dir: Literal['asc', 'desc'] = Query(default='desc'),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> TaskListPage:
        """A paginated, server-sorted, server-filtered slice of tasks."""
        result = await list_tasks(
            broker,
            statuses=parse_statuses(task_status),
            task_names=task_name,
            queues=queue,
            workers=worker,
            error_codes=error_code,
            retried_only=retried_only,
            sort_by=parse_sort_by(sort_by),
            sort_dir=sort_dir,
            offset=offset,
            limit=limit,
        )
        if is_err(result):
            raise query_failed('Task list', result.err_value)
        return result.ok_value

    @router.get('/{task_id}')
    async def read_task(task_id: str) -> TaskDetail:
        """One task with its full attempt history."""
        result = await get_task_detail(broker, task_id)
        if is_err(result):
            raise query_failed('Task detail', result.err_value)
        detail = result.ok_value
        if detail is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='Task not found.',
            )
        return detail

    return router
