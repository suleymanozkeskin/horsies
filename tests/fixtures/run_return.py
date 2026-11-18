"""The end of a run of the version-2 child, for tests that call
``_run_task_entry``.

``envelope_of`` and ``stop_reason_of`` read the end of a run. ``wire_of``
gives the version-1 wire tuple of that end, for tests that hand it to the
finalize of the Python worker parent (``task_finished_handle``); the core
worker reads the version-2 end itself.
"""

from __future__ import annotations

from typing import assert_never

from horsies.core.worker.run_return import (
    ResultEnvelope,
    RunReturn,
    SetRunning,
    StopReason,
    StoppedBeforeRunning,
)

type WireResult = tuple[bool, str, str | None]
"""The version-1 wire tuple: whether a result envelope was produced, the
envelope, and the reason of a stop."""


def envelope_of(returned: RunReturn) -> str:
    """The result envelope of a run that set its row RUNNING. A stop before
    running fails the test."""
    match returned:
        case SetRunning(result=ResultEnvelope(text=text)):
            return text
        case StoppedBeforeRunning(reason=reason):
            raise AssertionError(f'the run stopped before running: {reason}')
        case _ as unreachable:
            assert_never(unreachable)


def stop_reason_of(returned: RunReturn) -> StopReason:
    """The reason of a run that stopped before running. A run that set its
    row RUNNING fails the test."""
    match returned:
        case StoppedBeforeRunning(reason=reason):
            return reason
        case SetRunning():
            raise AssertionError('the run set its row RUNNING')
        case _ as unreachable:
            assert_never(unreachable)


def _version_one_reason(reason: StopReason) -> str:
    match reason:
        case 'NODE_NOT_LINKED':
            return 'WORKFLOW_CHECK_FAILED'
        case (
            'CLAIM_LOST'
            | 'OWNERSHIP_UNCONFIRMED'
            | 'WORKFLOW_STOPPED'
            | 'TASK_EXPIRED'
            | 'ORPHAN_CANCELLED'
        ):
            return reason
        case _ as unreachable:
            assert_never(unreachable)


def wire_of(returned: RunReturn) -> WireResult:
    """The version-1 wire tuple of the end of a run, as the Python parent's
    finalize reads it: ``(True, envelope, None)`` after the RUNNING update,
    ``(False, '', reason)`` for a stop before it."""
    match returned:
        case SetRunning(result=ResultEnvelope(text=text)):
            return (True, text, None)
        case StoppedBeforeRunning(reason=reason):
            return (False, '', _version_one_reason(reason))
        case _ as unreachable:
            assert_never(unreachable)
