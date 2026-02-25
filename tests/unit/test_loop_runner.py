"""Tests for LoopRunner (horsies/core/utils/loop_runner.py)."""

from __future__ import annotations

import gc
import warnings
from unittest.mock import patch

import pytest

from horsies.core.utils.loop_runner import LoopRunner, LoopRunnerError


@pytest.mark.unit
class TestLoopRunnerCall:
    """Behavioral tests for LoopRunner.call()."""

    def test_call_closes_coroutine_when_scheduling_fails(self) -> None:
        """Scheduling failure should not leak an un-awaited coroutine warning."""
        runner = LoopRunner()
        runner.start()

        async def sample() -> int:
            return 1

        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter('always', RuntimeWarning)
                with (
                    patch(
                        'asyncio.run_coroutine_threadsafe',
                        side_effect=RuntimeError('boom'),
                    ),
                    pytest.raises(LoopRunnerError, match='Failed to schedule coroutine'),
                ):
                    runner.call(sample)
                gc.collect()

            warning_texts = [str(w.message) for w in caught]
            assert not any('was never awaited' in text for text in warning_texts)
        finally:
            runner.stop()
