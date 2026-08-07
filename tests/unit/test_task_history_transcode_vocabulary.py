"""Transcode vocabulary pins: the qualified ceilings and ruled bounds.

The ceilings are the qualified ones and may not drift silently; the
blocker query bound is declared, never unbounded; and the exhaustion
outcome's capture-failed marker exists with the safe default, because
diagnostics never mask the outcome.
"""

from __future__ import annotations

import pytest

from horsies.core.history.transcode.outcomes import (
    BLOCKER_QUERY_TRUNCATION_CHARS,
    MAINTENANCE_SECONDS_MAXIMUM,
    SWAP_LOCK_ATTEMPTS_MAXIMUM,
    SWAP_LOCK_SECONDS_MAXIMUM,
    SWAP_RETRY_BACKOFF_SECONDS,
    ArchiveComponent,
    SwapLockMode,
    TranscodeSwapExhausted,
)

pytestmark = [pytest.mark.unit]


class TestQualifiedCeilings:
    def test_the_four_ceilings_are_the_qualified_values(self) -> None:
        assert SWAP_LOCK_ATTEMPTS_MAXIMUM == 120
        assert SWAP_RETRY_BACKOFF_SECONDS == 0.25
        assert SWAP_LOCK_SECONDS_MAXIMUM == 2.0
        assert MAINTENANCE_SECONDS_MAXIMUM == 600.0

    def test_lock_modes_are_the_qualified_pair(self) -> None:
        assert SwapLockMode.PARENT.value == 'ACCESS_EXCLUSIVE'
        assert SwapLockMode.LEAVES.value == 'SHARE'

    def test_the_four_components_are_closed(self) -> None:
        assert {component.value for component in ArchiveComponent} == {
            'HISTORY_ROW',
            'RESULT',
            'ATTEMPTS',
            'RERUN_INPUT',
        }


class TestRuledDiagnosticBounds:
    def test_query_truncation_bound_is_declared(self) -> None:
        assert BLOCKER_QUERY_TRUNCATION_CHARS == 1024

    def test_exhaustion_carries_the_capture_failed_marker(self) -> None:
        exhausted = TranscodeSwapExhausted(
            job_id='j',
            lock_mode=SwapLockMode.PARENT,
            relation_names=('r',),
            attempts=120,
            retry_sleep_seconds=29.75,
        )
        # The safe default: no blockers claimed, no failure claimed —
        # an empty capture is distinguishable from a failed one.
        assert exhausted.blockers == ()
        assert exhausted.blocker_capture_failed is False
