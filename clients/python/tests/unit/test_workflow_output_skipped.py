"""A COMPLETED workflow with an explicit output node always stores an envelope.

A success case that does not require the output node lets a workflow be
COMPLETED while that node is SKIPPED or FAILED. A SKIPPED output node stores an
error envelope with ``WORKFLOW_OUTPUT_SKIPPED``. A FAILED output node stores its
own error.

A ``SubWorkflowNode`` is COMPLETED only when the child's output is ok, as a task
node is. An err output makes a FAILED node that holds the child's text. A stored
child result that does not parse, or no result, makes a FAILED node that holds an
envelope with ``RESULT_DESERIALIZATION_ERROR``, so each reader names the cause.

An outputless workflow has no output node. It stores the plain envelope with
``ok`` null, and a SKIPPED terminal node gives no ``WORKFLOW_OUTPUT_SKIPPED``.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false


import pytest

from horsies.core.models.tasks import (
    BUILTIN_CODE_REGISTRY,
    OutcomeCode,
)
from horsies.monitoring.models import ErrorCategory
from horsies.monitoring.queries import categorize_error_code

pytestmark = [pytest.mark.unit]


class TestOutcomeCode:
    def test_code_is_registered_as_a_built_in_outcome(self) -> None:
        name = 'WORKFLOW_OUTPUT_SKIPPED'

        assert BUILTIN_CODE_REGISTRY[name] is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert categorize_error_code(name) is ErrorCategory.OUTCOME
