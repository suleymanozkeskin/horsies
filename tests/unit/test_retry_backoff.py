"""Unit tests for _RetryBackoff in horsies.core.worker.worker."""

from __future__ import annotations

import random

import pytest

from horsies.core.worker.worker import _RetryBackoff


@pytest.mark.unit
class TestRetryBackoffCanRetry:
    """Tests for can_retry with finite and infinite max_attempts."""

    def test_infinite_retries_always_allows(self) -> None:
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=0)
        for _ in range(100):
            assert backoff.can_retry() is True
            backoff.next_delay_seconds()

    def test_finite_retries_exhausts(self) -> None:
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=3)
        assert backoff.can_retry() is True
        backoff.next_delay_seconds()  # attempts=1
        assert backoff.can_retry() is True
        backoff.next_delay_seconds()  # attempts=2
        assert backoff.can_retry() is True
        backoff.next_delay_seconds()  # attempts=3
        assert backoff.can_retry() is False

    def test_single_attempt_limit(self) -> None:
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=1)
        assert backoff.can_retry() is True
        backoff.next_delay_seconds()  # attempts=1
        assert backoff.can_retry() is False

    def test_can_retry_true_before_any_delay_call_infinite_mode(self) -> None:
        """max_attempts=0 returns True even at attempts=0 without any delay calls."""
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=0)
        assert backoff.attempts == 0
        assert backoff.can_retry() is True

    def test_finite_retries_boundary_two(self) -> None:
        """Off-by-one boundary: max_attempts=2 allows exactly 2 retries."""
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=2)
        assert backoff.can_retry() is True
        backoff.next_delay_seconds()  # attempts=1
        assert backoff.can_retry() is True
        backoff.next_delay_seconds()  # attempts=2
        assert backoff.can_retry() is False


@pytest.mark.unit
class TestRetryBackoffReset:
    """Tests for reset behavior."""

    def test_reset_clears_attempts(self) -> None:
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=2)
        backoff.next_delay_seconds()
        backoff.next_delay_seconds()
        assert backoff.can_retry() is False

        backoff.reset()
        assert backoff.attempts == 0
        assert backoff.can_retry() is True

    def test_reset_restores_initial_delay_range(self) -> None:
        """After exhaustion and reset, first delay returns to initial_ms jitter range."""
        initial_ms = 1000
        backoff = _RetryBackoff(initial_ms=initial_ms, max_ms=100_000, max_attempts=5)
        # Grow delay through several attempts
        for _ in range(4):
            backoff.next_delay_seconds()

        backoff.reset()
        delay = backoff.next_delay_seconds()

        # First delay after reset: base_ms=1000, jitter ±250 → [0.75s, 1.25s]
        assert 0.75 <= delay <= 1.25, (
            f'delay {delay:.3f}s not in initial range after reset'
        )


@pytest.mark.unit
class TestRetryBackoffExponentialGrowth:
    """Tests for exponential backoff calculation."""

    def test_delays_grow_exponentially(self) -> None:
        backoff = _RetryBackoff(initial_ms=1000, max_ms=100_000, max_attempts=0)
        delays: list[float] = []
        for _ in range(5):
            delays.append(backoff.next_delay_seconds())

        # Each delay should be roughly 2x the previous (within jitter tolerance).
        # attempt 1: base=1000ms, attempt 2: base=2000ms, attempt 3: base=4000ms, ...
        for i in range(1, len(delays)):
            # The ratio should be ~2x, but jitter adds ±25%, so allow 1.2x–3.5x.
            ratio = delays[i] / delays[i - 1]
            assert 1.2 <= ratio <= 3.5, (
                f'delay[{i}]={delays[i]:.3f}s / delay[{i-1}]={delays[i-1]:.3f}s = {ratio:.2f}'
            )

    def test_delay_capped_at_max_ms(self) -> None:
        backoff = _RetryBackoff(initial_ms=1000, max_ms=2000, max_attempts=0)
        # After a few attempts the base should be capped at 2000ms.
        delay = 0.0
        for _ in range(10):
            delay = backoff.next_delay_seconds()
        # max base = 2000ms, jitter ±25% → max delay = 2500ms = 2.5s
        assert delay <= 2.5 + 0.01

    def test_delay_never_below_minimum(self) -> None:
        backoff = _RetryBackoff(initial_ms=100, max_ms=500, max_attempts=0)
        for _ in range(20):
            delay = backoff.next_delay_seconds()
            assert delay >= 0.1

    def test_floor_clamps_with_tiny_initial_ms(self) -> None:
        """When initial_ms is small enough that jitter pushes below 0.1s, floor activates."""
        backoff = _RetryBackoff(initial_ms=10, max_ms=50, max_attempts=0)
        # base_ms=10 → jitter_range=2.5 → delay_ms in [7.5, 12.5] → delay_s in [0.0075, 0.0125]
        # Without floor, delay would be ~0.01s. Floor forces exactly 0.1s.
        delay = backoff.next_delay_seconds()
        assert delay == 0.1


@pytest.mark.unit
class TestRetryBackoffJitter:
    """Tests for jitter range (±25% of base)."""

    def test_first_delay_within_jitter_range(self) -> None:
        """First attempt: base=initial_ms, jitter=±25%."""
        initial_ms = 1000
        backoff = _RetryBackoff(initial_ms=initial_ms, max_ms=100_000, max_attempts=0)

        delays: list[float] = []
        for _ in range(50):
            b = _RetryBackoff(initial_ms=initial_ms, max_ms=100_000, max_attempts=0)
            delays.append(b.next_delay_seconds())

        # base_ms = 1000, jitter ±250 → delay ∈ [750ms, 1250ms] → [0.75s, 1.25s]
        for d in delays:
            assert 0.75 <= d <= 1.25, f'delay {d:.3f}s outside expected jitter range'

    def test_jitter_deterministic_with_seed(self) -> None:
        """Seeded RNG produces reproducible jitter values across runs."""
        initial_ms = 1000
        random.seed(42)
        backoff = _RetryBackoff(initial_ms=initial_ms, max_ms=100_000, max_attempts=0)
        first_run = backoff.next_delay_seconds()

        backoff.reset()
        random.seed(42)
        second_run = backoff.next_delay_seconds()

        assert first_run == second_run


@pytest.mark.unit
class TestRetryBackoffConstruction:
    """Tests for dataclass construction defaults."""

    def test_fresh_instance_has_zero_attempts(self) -> None:
        """Newly constructed _RetryBackoff starts with attempts=0."""
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=3)
        assert backoff.attempts == 0


@pytest.mark.unit
class TestRetryBackoffAttemptTracking:
    """Tests for the attempt-incrementing side effect of next_delay_seconds."""

    def test_next_delay_increments_attempts_by_one(self) -> None:
        """Each next_delay_seconds call increments attempts by exactly 1."""
        backoff = _RetryBackoff(initial_ms=500, max_ms=30_000, max_attempts=0)
        for expected in range(1, 6):
            backoff.next_delay_seconds()
            assert backoff.attempts == expected, (
                f'after {expected} calls, attempts={backoff.attempts}'
            )
