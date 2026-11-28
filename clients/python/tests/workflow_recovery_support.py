"""The reaper's workflow recovery passes, run once through the core handle of a broker.

The reaper of a worker runs them in this order: the phase-2 records
(``CoreHandle.recover_workflow_phase2_async``), then the stuck workflows
(``CoreHandle.recover_workflows_async``). Each pass uses the defaults of
``RecoveryConfig``. A failed pass fails the test.
"""

from __future__ import annotations

from typing import Final

import pytest

import horsies._core as core
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.recovery import RecoveryConfig

PHASE2_RECOVERY_ROWS: Final = 200
"""The phase-2 records that one test pass takes: more than any test seeds."""

_DEFAULTS: Final = RecoveryConfig()


async def phase2_recovery_pass(
    broker: PostgresBroker,
    *,
    grace_ms: int = 0,
) -> core.Phase2RecoveryRecord:
    """One phase-2 recovery pass over the records older than ``grace_ms``."""
    handle = broker._core_handle  # pyright: ignore[reportPrivateUsage]
    outcome = await handle.recover_workflow_phase2_async(
        grace_ms=grace_ms,
        max_rows=PHASE2_RECOVERY_ROWS,
        quarantine_after_attempts=_DEFAULTS.finalization_quarantine_after_attempts,
    )
    match outcome:
        case core.Phase2RecoveryRecord():
            return outcome
        case core.Phase2RecoveryFailed(message=message):
            pytest.fail(f'phase-2 recovery pass failed: {message}')


async def workflow_recovery_pass(broker: PostgresBroker) -> core.RecoveryPassRecord:
    """One global recovery pass of the stuck workflows."""
    handle = broker._core_handle  # pyright: ignore[reportPrivateUsage]
    outcome = await handle.recover_workflows_async(
        failure_limit=_DEFAULTS.workflow_recovery_failure_limit,
    )
    match outcome:
        case core.RecoveryPassRecord():
            return outcome
        case core.RecoveryPassFailed(message=message):
            pytest.fail(f'workflow recovery pass failed: {message}')
