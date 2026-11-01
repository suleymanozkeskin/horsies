"""``get()`` of a workflow whose terminal status reads as an error.

``get_async`` reads the status, and for ``FAILED``, ``CANCELLED`` or
``EXPIRED`` it reads the stored error in a second statement. The second read
uses the status of the first read: a terminal status does not change.
Retention can delete the row between the two reads; that is
``WORKFLOW_NOT_FOUND``, not a guessed status.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.resources import ListenerDisabled
from horsies.core.models.tasks import OutcomeCode, RetrievalCode
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import (
    GET_WORKFLOW_ERROR_SQL,
    GET_WORKFLOW_STATUS_SQL,
    WorkflowHandle,
)
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '0b6f3c1e-9d2a-4e7b-8c5f-1a2b3c4d5e6f'
LEGACY_EXPIRY_TEXT = 'paused_workflow_auto_cancel_after: 1 day, 0:00:00'

TERMINAL_ERROR_STATUSES = [
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
    WorkflowStatus.EXPIRED,
]


def _handle(status: WorkflowStatus, error_row: Any) -> WorkflowHandle[Any]:
    """A handle whose status read gives ``status`` and whose error read gives
    ``error_row`` (``None`` is a missing row)."""
    status_result = MagicMock()
    status_result.fetchone.return_value = SimpleNamespace(status=status.value)
    error_result = MagicMock()
    error_result.fetchone.return_value = error_row

    def execute(statement: Any, *_args: Any, **_kwargs: Any) -> MagicMock:
        if statement is GET_WORKFLOW_STATUS_SQL:
            return status_result
        if statement is GET_WORKFLOW_ERROR_SQL:
            return error_result
        raise AssertionError(f'unexpected statement: {statement}')

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=execute)
    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker,
        fake_resources(
            session_factory=MagicMock(return_value=context),
            listener=ListenerDisabled(),
        ),
    )
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


class TestRowRemovedAfterTheStatusRead:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', TERMINAL_ERROR_STATUSES, ids=str)
    async def test_it_is_workflow_not_found(self, status: WorkflowStatus) -> None:
        handle = _handle(status, error_row=None)

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is RetrievalCode.WORKFLOW_NOT_FOUND
        assert WORKFLOW_ID in (error.message or '')
        assert status.value in (error.message or '')


class TestNoStoredError:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status', 'code'),
        [
            (WorkflowStatus.FAILED, OutcomeCode.WORKFLOW_FAILED),
            (WorkflowStatus.CANCELLED, OutcomeCode.WORKFLOW_CANCELLED),
            (WorkflowStatus.EXPIRED, OutcomeCode.WORKFLOW_EXPIRED),
        ],
        ids=['FAILED', 'CANCELLED', 'EXPIRED'],
    )
    async def test_the_code_is_the_outcome_code_of_the_read_status(
        self,
        status: WorkflowStatus,
        code: OutcomeCode,
    ) -> None:
        # The error row has no status field: the error read does not read
        # the status again.
        handle = _handle(status, error_row=SimpleNamespace(error=None))

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is code
        assert error.message == f'Workflow {status.value.lower()}'


class TestLegacyExpiryText:
    @pytest.mark.asyncio
    async def test_the_read_status_decides_the_legacy_expiry_text(self) -> None:
        handle = _handle(
            WorkflowStatus.EXPIRED,
            error_row=SimpleNamespace(error=LEGACY_EXPIRY_TEXT),
        )

        result = await handle.get_async(timeout_ms=0)

        error = result.unwrap_err()
        assert error.error_code is OutcomeCode.WORKFLOW_EXPIRED
        assert error.message == LEGACY_EXPIRY_TEXT
