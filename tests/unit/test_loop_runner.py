"""Tests for LoopRunner (horsies/core/utils/loop_runner.py)."""

from __future__ import annotations

import gc
import threading
import time
import warnings
from unittest.mock import MagicMock, patch

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

    def test_call_after_stop_raises_instead_of_restarting(self) -> None:
        """Calling after a successful stop should fail closed, not restart."""
        runner = LoopRunner()
        runner.start()
        runner.stop()
        assert runner._started is False
        assert runner._loop is None

        async def sample() -> int:
            return 1

        with pytest.raises(LoopRunnerError, match='cannot be restarted'):
            runner.call(sample)

        assert runner._started is False
        assert runner._loop is None
        assert runner._thread is None


@pytest.mark.unit
class TestLoopRunnerStop:
    """Behavioral tests for LoopRunner.stop()."""

    def test_stop_does_not_close_loop_when_thread_still_alive(self) -> None:
        """If join(timeout) returns with alive thread, keep loop/thread state intact."""
        runner = LoopRunner()
        runner._started = True
        runner._loop = MagicMock()
        runner._thread = MagicMock()
        runner._thread.is_alive.return_value = True

        with patch.object(runner.logger, 'warning') as mock_warn:
            runner.stop()

        runner._loop.call_soon_threadsafe.assert_called_once()
        runner._thread.join.assert_called_once_with(timeout=2)
        runner._loop.close.assert_not_called()
        assert runner._started is True
        assert runner._loop is not None
        assert runner._thread is not None
        mock_warn.assert_called_once()

    def test_stop_closes_loop_when_thread_stops(self) -> None:
        """When thread stops within timeout, loop closes and runner resets."""
        runner = LoopRunner()
        loop = MagicMock()
        thread = MagicMock()
        thread.is_alive.return_value = False
        runner._started = True
        runner._loop = loop
        runner._thread = thread

        runner.stop()

        loop.call_soon_threadsafe.assert_called_once()
        thread.join.assert_called_once_with(timeout=2)
        loop.close.assert_called_once()
        assert runner._started is False
        assert runner._loop is None
        assert runner._thread is None


@pytest.mark.unit
class TestLoopRunnerThreadSafety:
    """Thread-safety tests for loop construction."""

    def test_ensure_loop_is_thread_safe_under_concurrency(self) -> None:
        """Concurrent _ensure_loop() calls should create a single loop/thread pair."""
        runner = LoopRunner()
        barrier = threading.Barrier(8)
        results: list[object] = []
        errors: list[BaseException] = []
        results_lock = threading.Lock()

        def _slow_new_event_loop() -> MagicMock:
            # Enlarge race window so unsynchronized code would likely create >1 loop.
            time.sleep(0.01)
            return MagicMock()

        def _worker() -> None:
            try:
                barrier.wait()
                loop = runner._ensure_loop()
                with results_lock:
                    results.append(loop)
            except BaseException as exc:
                with results_lock:
                    errors.append(exc)

        with patch('asyncio.new_event_loop', side_effect=_slow_new_event_loop) as mock_new_loop:
            workers = [threading.Thread(target=_worker) for _ in range(8)]
            for t in workers:
                t.start()
            for t in workers:
                t.join()

        assert errors == []
        assert len(results) == 8
        assert len({id(loop) for loop in results}) == 1
        assert mock_new_loop.call_count == 1
        assert runner._thread is not None
