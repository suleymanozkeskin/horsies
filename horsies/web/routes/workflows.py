# pyright: reportUnusedFunction=false
# Route handlers are registered by their decorator, not called by name.
"""Workflow read endpoints.

``/workflows/names`` is declared before ``/workflows/{workflow_id}`` so the
literal path is not captured as a run id.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import is_err
from horsies.monitoring import (
    WorkflowRunDetail,
    WorkflowRunSummary,
    WorkflowTaskDetail,
    get_workflow_node,
    get_workflow_run,
    list_workflow_names,
    list_workflow_runs,
)
from horsies.web.routes._common import query_failed


def build_router(broker: PostgresBroker) -> APIRouter:
    """Build the ``/workflows`` router bound to one broker."""
    router = APIRouter(prefix='/workflows', tags=['workflows'])

    @router.get('/names')
    async def read_names() -> list[str]:
        """Distinct names of root workflow runs."""
        result = await list_workflow_names(broker)
        if is_err(result):
            raise query_failed('Workflow names', result.err_value)
        return result.ok_value

    @router.get('')
    async def read_runs(
        name: str | None = Query(default=None),
        run_status: str | None = Query(default=None, alias='status'),
        limit: int = Query(default=30, ge=1, le=200),
    ) -> list[WorkflowRunSummary]:
        """Recent root runs, newest first. A status no run carries yields []."""
        result = await list_workflow_runs(
            broker, name=name, status=run_status, limit=limit
        )
        if is_err(result):
            raise query_failed('Workflow runs', result.err_value)
        return result.ok_value

    @router.get('/{workflow_id}')
    async def read_run(workflow_id: str) -> WorkflowRunDetail:
        """One run's DAG. Works for root and subworkflow ids alike."""
        result = await get_workflow_run(broker, workflow_id)
        if is_err(result):
            raise query_failed('Workflow run', result.err_value)
        detail = result.ok_value
        if detail is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='Workflow run not found.',
            )
        return detail

    @router.get('/{workflow_id}/tasks/{task_index}')
    async def read_node(workflow_id: str, task_index: int) -> WorkflowTaskDetail:
        """One node's detail. A node whose backing task row is gone still
        resolves, with a null ``leaf``."""
        result = await get_workflow_node(broker, workflow_id, task_index)
        if is_err(result):
            raise query_failed('Workflow task', result.err_value)
        detail = result.ok_value
        if detail is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='Workflow task not found.',
            )
        return detail

    return router
