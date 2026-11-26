"""Workflow start: what prevalidation catches.

Prevalidation returns ``WorkflowStartValidationFailed`` only for a
``WorkflowValidationError``. Any other exception from a check is a defect:
it propagates, and the start makes no core call.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from horsies.core.codec.json_io import SerializationError
from horsies.core.errors import (
    ErrorCode,
    MultipleValidationErrors,
    ValidationReport,
    WorkflowValidationError,
)
from horsies.core.workflows.lifecycle import start_workflow_async

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '3c7e9a10-5b2d-4f8e-a1c3-7d9e0f1a2b3c'
DEFECT_MARKER = 'defect-marker-7d41'


def _prevalidation_defects() -> list[BaseException]:
    report = ValidationReport('workflow')
    report.add(WorkflowValidationError('first', code=ErrorCode.WORKFLOW_NO_NAME))
    report.add(WorkflowValidationError('second', code=ErrorCode.WORKFLOW_NO_NODES))
    defects: list[BaseException] = [
        ValueError(DEFECT_MARKER),
        MultipleValidationErrors(message=DEFECT_MARKER, report=report),
        SerializationError(DEFECT_MARKER),
    ]
    return defects


class TestPrevalidationCatch:
    """Prevalidation catches only ``WorkflowValidationError``."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'defect', _prevalidation_defects(), ids=lambda exc: type(exc).__name__,
    )
    async def test_another_exception_from_a_prevalidation_check_propagates(
        self, defect: BaseException,
    ) -> None:
        spec = MagicMock()
        spec.name = 'zone-one-wf'
        spec.nodes = []
        spec.workflow_definition = object()
        broker = MagicMock()
        broker.app = None

        with patch(
            'horsies.core.workflows.lifecycle.validate_workflow_generic_output_match',
            side_effect=defect,
        ):
            with pytest.raises(type(defect)) as exc_info:
                await start_workflow_async(spec, broker, WORKFLOW_ID)

        assert exc_info.value is defect
        broker._core_handle.start_workflow_async.assert_not_called()
