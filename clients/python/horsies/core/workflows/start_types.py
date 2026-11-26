"""Typed errors of workflow start operations.

``start_workflow_async`` / ``start_workflow`` and ``WorkflowSpec.start`` /
``start_async`` return ``WorkflowStartResult[WorkflowHandle[T]]``. The error
side is ``WorkflowStartError``: a union with one frozen variant per
``WorkflowStartErrorCode`` member. Every variant has ``code``, ``message``,
``retryable``, ``workflow_name`` and ``workflow_id``. Callers match on the
variant.

``workflow_id`` is the id that this start used: the caller's id, or the id
minted for the start when the caller gave none.

The resend methods ``WorkflowSpec.resend_start`` / ``resend_start_async`` take a
``WorkflowStartRetryable`` and start the workflow again with its id.

The classes of the core's start catalog are generated from horsies-core into
``horsies._catalog.workflow_start_errors`` and re-exported here.
``WorkflowStartError`` is the generated union and
``WorkflowStartBrokerNotConfigured``, which only this client produces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from horsies._catalog.workflow_start import (
    WorkflowStartErrorCode as WorkflowStartErrorCode,
)
from horsies._catalog.workflow_start_errors import (
    WorkflowStartAsyncContext as WorkflowStartAsyncContext,
    WorkflowStartCommandConflict as WorkflowStartCommandConflict,
    WorkflowStartCoreError as WorkflowStartCoreError,
    WorkflowStartEnqueueFailed as WorkflowStartEnqueueFailed,
    WorkflowStartIdConflict as WorkflowStartIdConflict,
    WorkflowStartValidationFailed as WorkflowStartValidationFailed,
)
from horsies.core.types.result import Result


@dataclass(slots=True, frozen=True)
class WorkflowStartBrokerNotConfigured:
    """The spec has no broker, so the start cannot run. Nothing ran.

    Build the spec with ``app.workflow()`` or set its broker. The retry
    methods refuse this error.
    """

    code: ClassVar[WorkflowStartErrorCode] = WorkflowStartErrorCode.BROKER_NOT_CONFIGURED
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    workflow_name: str
    workflow_id: str


type WorkflowStartError = WorkflowStartCoreError | WorkflowStartBrokerNotConfigured
"""Every failure of a workflow start: one variant per ``WorkflowStartErrorCode``.
The classes of the core's catalog, and ``WorkflowStartBrokerNotConfigured``,
which only this client produces."""

type WorkflowStartRetryable = WorkflowStartEnqueueFailed | WorkflowStartAsyncContext
"""The variants that the retry methods accept. Each carries the id of its start."""

type WorkflowStartResult[T] = Result[T, WorkflowStartError]
