"""Unit tests for retry decision logic.

Tests _should_retry_task() directly and the full chain:
resolve_exception_error_code() → TaskError → _should_retry_task().
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.exception_mapper import ExceptionMapper, resolve_exception_error_code
from horsies.core.models.tasks import LibraryErrorCode, TaskError
from horsies.core.worker.worker import Worker


def _mock_session(row: SimpleNamespace | None) -> AsyncMock:
    """Return an AsyncMock session whose execute().fetchone() returns row."""
    mock_result = MagicMock()
    mock_result.fetchone.return_value = row

    session = AsyncMock()
    session.execute.return_value = mock_result
    return session


def _make_row(
    retry_count: int,
    max_retries: int,
    task_options: str | None,
    *,
    good_until: datetime | None = None,
    db_now: datetime | None = None,
) -> SimpleNamespace:
    """Build a fake DB row matching GET_TASK_RETRY_INFO_SQL columns."""
    if db_now is None:
        db_now = datetime.now(timezone.utc)
    return SimpleNamespace(
        retry_count=retry_count,
        max_retries=max_retries,
        task_options=task_options,
        good_until=good_until,
        db_now=db_now,
    )


def _task_options_json(
    auto_retry_for: list[str],
    *,
    intervals: list[int] | None = None,
    backoff_strategy: str | None = None,
    jitter: bool | None = None,
) -> str:
    """Build a valid task_options JSON with retry_policy.auto_retry_for."""
    retry_policy: dict[str, object] = {
        "auto_retry_for": auto_retry_for,
    }
    if intervals is not None:
        retry_policy["intervals"] = intervals
    if backoff_strategy is not None:
        retry_policy["backoff_strategy"] = backoff_strategy
    if jitter is not None:
        retry_policy["jitter"] = jitter
    return json.dumps({
        "retry_policy": retry_policy,
    })


@pytest.mark.unit
class TestShouldRetryTask:
    """Direct tests for Worker._should_retry_task()."""

    @pytest.mark.asyncio
    async def test_matching_error_code_returns_true(self) -> None:
        """Error code present in auto_retry_for with retries left returns True."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is True

    @pytest.mark.asyncio
    async def test_non_matching_error_code_returns_false(self) -> None:
        """Error code NOT in auto_retry_for returns False."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code="TIMEOUT")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_task_not_found_returns_false(self) -> None:
        """fetchone() returning None means task not found, returns False."""
        session = _mock_session(row=None)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_retries_exhausted_returns_false(self) -> None:
        """retry_count >= max_retries means no retries left."""
        row = _make_row(
            retry_count=3,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_max_retries_zero_returns_false(self) -> None:
        """max_retries=0 means retries are disabled."""
        row = _make_row(
            retry_count=0,
            max_retries=0,
            task_options=_task_options_json(["RATE_LIMITED"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_library_error_code_enum_matches(self) -> None:
        """LibraryErrorCode enum .value matches string in auto_retry_for."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["UNHANDLED_EXCEPTION"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code=LibraryErrorCode.UNHANDLED_EXCEPTION)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is True

    @pytest.mark.asyncio
    async def test_malformed_task_options_json_returns_false(self) -> None:
        """Invalid JSON in task_options returns False."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options='NOT_JSON{',
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_task_options_not_dict_returns_false(self) -> None:
        """task_options that deserializes to a non-dict returns False."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options='"just_a_string"',
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_retry_policy_not_dict_returns_false(self) -> None:
        """retry_policy value that is not a dict returns False."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=json.dumps({"retry_policy": "string"}),
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_empty_auto_retry_for_returns_false(self) -> None:
        """Empty auto_retry_for list returns False."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json([]),
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_auto_retry_for_missing_returns_false(self) -> None:
        """No auto_retry_for key in retry_policy returns False."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=json.dumps({"retry_policy": {"backoff_strategy": "fixed"}}),
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_none_error_code_returns_false(self) -> None:
        """TaskError with error_code=None returns False (code is falsy)."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code=None)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_multiple_codes_matches_any(self) -> None:
        """auto_retry_for with multiple codes matches if error code is any of them."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["A_CODE", "B_CODE", "C_CODE"]),
        )
        session = _mock_session(row)
        error = TaskError(error_code="B_CODE")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is True

    @pytest.mark.asyncio
    async def test_good_until_already_expired_blocks_retry(self) -> None:
        """Already-expired good_until blocks retry even when code matches."""
        db_now = datetime.now(timezone.utc)
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
            good_until=db_now - timedelta(seconds=10),
            db_now=db_now,
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is False

    @pytest.mark.asyncio
    async def test_good_until_none_does_not_affect_retry(self) -> None:
        """None good_until keeps existing retry eligibility behavior."""
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
            good_until=None,
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is True

    @pytest.mark.asyncio
    async def test_good_until_in_future_allows_retry(self) -> None:
        """Future good_until allows retry to proceed to scheduling step."""
        db_now = datetime.now(timezone.utc)
        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["RATE_LIMITED"]),
            good_until=db_now + timedelta(seconds=120),
            db_now=db_now,
        )
        session = _mock_session(row)
        error = TaskError(error_code="RATE_LIMITED")

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert result is True


@pytest.mark.unit
class TestScheduleRetryExpiry:
    """Tests for Worker._schedule_retry() good_until-aware outcomes."""

    @staticmethod
    def _make_schedule_session(
        config_row: SimpleNamespace,
        update_row: SimpleNamespace | None,
        *,
        postcheck_row: SimpleNamespace | None = None,
    ) -> AsyncMock:
        config_result = MagicMock()
        config_result.fetchone.return_value = config_row
        update_result = MagicMock()
        update_result.fetchone.return_value = update_row
        postcheck_result = MagicMock()
        postcheck_result.fetchone.return_value = postcheck_row

        session = AsyncMock()
        if update_row is None:
            session.execute = AsyncMock(
                side_effect=[config_result, update_result, postcheck_result]
            )
        else:
            session.execute = AsyncMock(side_effect=[config_result, update_result])
        return session

    @pytest.mark.asyncio
    async def test_returns_expired_when_next_retry_exceeds_good_until(self) -> None:
        db_now = datetime.now(timezone.utc)
        config_row = SimpleNamespace(
            retry_count=0,
            task_options=_task_options_json(
                ["TRANSIENT"], intervals=[60], backoff_strategy="fixed", jitter=False
            ),
            good_until=db_now + timedelta(seconds=5),
            db_now=db_now,
        )
        session = self._make_schedule_session(config_row, SimpleNamespace(id="task-1"))

        worker = MagicMock(spec=Worker)
        worker._calculate_retry_delay.return_value = 60.0
        worker._get_task_queue_name = AsyncMock(return_value="default")
        worker._spawn_background = MagicMock()
        worker._schedule_delayed_notification = MagicMock(return_value=AsyncMock())

        outcome = await Worker._schedule_retry(worker, "task-1", session)

        assert outcome == 'expired'
        assert session.execute.await_count == 1
        worker._spawn_background.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_scheduled_when_within_good_until(self) -> None:
        db_now = datetime.now(timezone.utc)
        config_row = SimpleNamespace(
            retry_count=0,
            task_options=_task_options_json(
                ["TRANSIENT"], intervals=[5], backoff_strategy="fixed", jitter=False
            ),
            good_until=db_now + timedelta(seconds=120),
            db_now=db_now,
        )
        session = self._make_schedule_session(config_row, SimpleNamespace(id="task-1"))

        worker = MagicMock(spec=Worker)
        worker._calculate_retry_delay.return_value = 5.0
        worker._get_task_queue_name = AsyncMock(return_value="default")
        worker._spawn_background = MagicMock()
        worker._schedule_delayed_notification = MagicMock(return_value=AsyncMock())

        outcome = await Worker._schedule_retry(worker, "task-1", session)

        assert outcome == 'scheduled'
        assert session.execute.await_count == 2
        worker._spawn_background.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_scheduled_when_good_until_none(self) -> None:
        db_now = datetime.now(timezone.utc)
        config_row = SimpleNamespace(
            retry_count=0,
            task_options=_task_options_json(
                ["TRANSIENT"], intervals=[5], backoff_strategy="fixed", jitter=False
            ),
            good_until=None,
            db_now=db_now,
        )
        session = self._make_schedule_session(config_row, SimpleNamespace(id="task-1"))

        worker = MagicMock(spec=Worker)
        worker._calculate_retry_delay.return_value = 5.0
        worker._get_task_queue_name = AsyncMock(return_value="default")
        worker._spawn_background = MagicMock()
        worker._schedule_delayed_notification = MagicMock(return_value=AsyncMock())

        outcome = await Worker._schedule_retry(worker, "task-1", session)

        assert outcome == 'scheduled'
        assert session.execute.await_count == 2
        worker._spawn_background.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_reaper_reclaimed_when_update_returns_none(self) -> None:
        db_now = datetime.now(timezone.utc)
        config_row = SimpleNamespace(
            retry_count=0,
            task_options=_task_options_json(
                ["TRANSIENT"], intervals=[5], backoff_strategy="fixed", jitter=False
            ),
            good_until=None,
            db_now=db_now,
        )
        session = self._make_schedule_session(
            config_row,
            update_row=None,
            postcheck_row=SimpleNamespace(status='FAILED', good_until=None),
        )

        worker = MagicMock(spec=Worker)
        worker._calculate_retry_delay.return_value = 5.0
        worker._get_task_queue_name = AsyncMock(return_value="default")
        worker._spawn_background = MagicMock()
        worker._schedule_delayed_notification = MagicMock(return_value=AsyncMock())

        outcome = await Worker._schedule_retry(worker, "task-1", session)

        assert outcome == 'reaper_reclaimed'
        assert session.execute.await_count == 3
        worker._spawn_background.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_expired_when_update_guard_rejects_due_to_good_until(self) -> None:
        db_now = datetime.now(timezone.utc)
        initial_good_until = db_now + timedelta(seconds=60)
        postcheck_good_until = db_now + timedelta(seconds=4)
        config_row = SimpleNamespace(
            retry_count=0,
            task_options=_task_options_json(
                ["TRANSIENT"], intervals=[5], backoff_strategy="fixed", jitter=False
            ),
            good_until=initial_good_until,
            db_now=db_now,
        )
        session = self._make_schedule_session(
            config_row,
            update_row=None,
            postcheck_row=SimpleNamespace(status='RUNNING', good_until=postcheck_good_until),
        )

        worker = MagicMock(spec=Worker)
        worker._calculate_retry_delay.return_value = 5.0
        worker._get_task_queue_name = AsyncMock(return_value="default")
        worker._spawn_background = MagicMock()
        worker._schedule_delayed_notification = MagicMock(return_value=AsyncMock())

        outcome = await Worker._schedule_retry(worker, "task-1", session)

        assert outcome == 'expired'
        assert session.execute.await_count == 3
        worker._spawn_background.assert_not_called()


@pytest.mark.unit
class TestRetryDecisionChain:
    """Full pipeline: resolve_exception_error_code() → TaskError → _should_retry_task()."""

    @pytest.mark.asyncio
    async def test_task_mapper_code_triggers_retry(self) -> None:
        """Task mapper maps exception to code that is in auto_retry_for."""
        task_mapper: ExceptionMapper = {ValueError: "VAL_ERROR"}
        exc = ValueError("bad value")

        code = resolve_exception_error_code(
            exc,
            task_mapper=task_mapper,
            global_mapper=None,
            task_default=None,
            global_default="UNHANDLED",
        )
        error = TaskError(error_code=code)

        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["VAL_ERROR"]),
        )
        session = _mock_session(row)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert code == "VAL_ERROR"
        assert result is True

    @pytest.mark.asyncio
    async def test_global_mapper_code_triggers_retry(self) -> None:
        """Global mapper maps exception to code that is in auto_retry_for."""
        global_mapper: ExceptionMapper = {ValueError: "VAL_ERROR"}
        exc = ValueError("bad value")

        code = resolve_exception_error_code(
            exc,
            task_mapper=None,
            global_mapper=global_mapper,
            task_default=None,
            global_default="UNHANDLED",
        )
        error = TaskError(error_code=code)

        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["VAL_ERROR"]),
        )
        session = _mock_session(row)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert code == "VAL_ERROR"
        assert result is True

    @pytest.mark.asyncio
    async def test_task_mapper_wins_over_global_for_retry(self) -> None:
        """Task mapper code wins; since it's not in auto_retry_for, no retry."""
        task_mapper: ExceptionMapper = {ValueError: "TASK_CODE"}
        global_mapper: ExceptionMapper = {ValueError: "GLOBAL_CODE"}
        exc = ValueError("bad value")

        code = resolve_exception_error_code(
            exc,
            task_mapper=task_mapper,
            global_mapper=global_mapper,
            task_default=None,
            global_default="UNHANDLED",
        )
        error = TaskError(error_code=code)

        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["GLOBAL_CODE"]),
        )
        session = _mock_session(row)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert code == "TASK_CODE"
        assert result is False

    @pytest.mark.asyncio
    async def test_task_mapper_wins_over_global_positive(self) -> None:
        """Task mapper code wins and IS in auto_retry_for, so retry triggers."""
        task_mapper: ExceptionMapper = {ValueError: "TASK_CODE"}
        global_mapper: ExceptionMapper = {ValueError: "GLOBAL_CODE"}
        exc = ValueError("bad value")

        code = resolve_exception_error_code(
            exc,
            task_mapper=task_mapper,
            global_mapper=global_mapper,
            task_default=None,
            global_default="UNHANDLED",
        )
        error = TaskError(error_code=code)

        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["TASK_CODE"]),
        )
        session = _mock_session(row)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert code == "TASK_CODE"
        assert result is True

    @pytest.mark.asyncio
    async def test_unmapped_exception_uses_global_default(self) -> None:
        """No mappers match, global default used, and it's in auto_retry_for."""
        exc = RuntimeError("unexpected")

        code = resolve_exception_error_code(
            exc,
            task_mapper=None,
            global_mapper=None,
            task_default=None,
            global_default="UNHANDLED",
        )
        error = TaskError(error_code=code)

        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["UNHANDLED"]),
        )
        session = _mock_session(row)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert code == "UNHANDLED"
        assert result is True

    @pytest.mark.asyncio
    async def test_task_default_wins_over_global_default(self) -> None:
        """Task default wins over global default; since it's not in auto_retry_for, no retry."""
        exc = RuntimeError("unexpected")

        code = resolve_exception_error_code(
            exc,
            task_mapper=None,
            global_mapper=None,
            task_default="TASK_DEFAULT",
            global_default="GLOBAL_DEFAULT",
        )
        error = TaskError(error_code=code)

        row = _make_row(
            retry_count=0,
            max_retries=3,
            task_options=_task_options_json(["GLOBAL_DEFAULT"]),
        )
        session = _mock_session(row)

        result = await Worker._should_retry_task(MagicMock(), "task-1", error, session)

        assert code == "TASK_DEFAULT"
        assert result is False


@pytest.mark.unit
class TestCalculateRetryDelay:
    """Tests for Worker._calculate_retry_delay."""

    def test_exponential_delays_strictly_increase_with_jitter(self) -> None:
        """Exponential backoff delays must strictly increase across attempts, even with jitter."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'exponential',
            'jitter': True,
        }
        # Run many iterations to catch jitter-induced collisions
        for _ in range(200):
            delay_1 = Worker._calculate_retry_delay(MagicMock(), 1, policy_data)
            delay_2 = Worker._calculate_retry_delay(MagicMock(), 2, policy_data)
            delay_3 = Worker._calculate_retry_delay(MagicMock(), 3, policy_data)
            assert delay_2 > delay_1, (
                f'delay_2={delay_2} must be > delay_1={delay_1}'
            )
            assert delay_3 > delay_2, (
                f'delay_3={delay_3} must be > delay_2={delay_2}'
            )

    def test_exponential_delay_returns_float(self) -> None:
        """Delay must be float to preserve sub-second precision from jitter."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'exponential',
            'jitter': True,
        }
        delay = Worker._calculate_retry_delay(MagicMock(), 1, policy_data)
        assert isinstance(delay, float)

    def test_fixed_delay_returns_float(self) -> None:
        """Fixed strategy also returns float for type consistency."""
        policy_data = {
            'intervals': [5, 10],
            'backoff_strategy': 'fixed',
            'jitter': False,
        }
        delay = Worker._calculate_retry_delay(MagicMock(), 1, policy_data)
        assert isinstance(delay, float)

    def test_minimum_delay_is_one_second(self) -> None:
        """Delay must never drop below 1.0 second."""
        policy_data = {
            'intervals': [1],
            'backoff_strategy': 'exponential',
            'jitter': True,
        }
        for _ in range(200):
            delay = Worker._calculate_retry_delay(MagicMock(), 1, policy_data)
            assert delay >= 1.0, f'delay={delay} is below 1.0s minimum'
