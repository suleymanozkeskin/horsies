"""Unit tests for _ensure_schema_with_retry in horsies.core.cli."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest

from psycopg import InterfaceError, OperationalError

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.cli import _ensure_schema_with_retry
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.types.result import Err, Ok


def _schema_err(
    message: str,
    *,
    retryable: bool = True,
    exception: BaseException | None = None,
) -> Err[BrokerOperationError]:
    """Build an Err for schema init failure in tests."""
    return Err(BrokerOperationError(
        code=BrokerErrorCode.SCHEMA_INIT_FAILED,
        message=message,
        retryable=retryable,
        exception=exception,
    ))


@pytest.mark.unit
@pytest.mark.asyncio(loop_scope='function')
class TestEnsureSchemaWithRetry:
    """Tests for _ensure_schema_with_retry."""

    async def test_succeeds_on_first_attempt(self) -> None:
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
        resilience = WorkerResilienceConfig()
        logger = logging.getLogger('test')

        await _ensure_schema_with_retry(broker, resilience, logger)

        broker.ensure_schema_initialized.assert_awaited_once()

    async def test_retries_on_transient_error_then_succeeds(self) -> None:
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[
                _schema_err('connection refused', exception=OperationalError('connection refused')),
                _schema_err('connection refused', exception=OperationalError('connection refused')),
                Ok(None),
            ]
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
        )
        logger = logging.getLogger('test')

        with patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock):
            await _ensure_schema_with_retry(broker, resilience, logger)

        assert broker.ensure_schema_initialized.await_count == 3

    async def test_raises_immediately_on_non_retryable_error(self) -> None:
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            return_value=_schema_err(
                'bad config',
                retryable=False,
                exception=ValueError('bad config'),
            ),
        )
        resilience = WorkerResilienceConfig()
        logger = logging.getLogger('test')

        with pytest.raises(RuntimeError, match='bad config'):
            await _ensure_schema_with_retry(broker, resilience, logger)

        broker.ensure_schema_initialized.assert_awaited_once()

    async def test_exhausts_max_attempts_then_raises(self) -> None:
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            return_value=_schema_err(
                'connection refused',
                exception=OperationalError('connection refused'),
            ),
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
            db_retry_max_attempts=3,
        )
        logger = logging.getLogger('test')

        with (
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock),
            pytest.raises(RuntimeError, match='connection refused'),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        # Should have tried: 1 initial + 3 retries = 4 calls
        # (attempts counter goes 1,2,3 and on 3 it checks 3 <= 3 → True,
        #  then 4th fail → attempts=4, 4 <= 3 → False → raise)
        assert broker.ensure_schema_initialized.await_count == 4

    async def test_infinite_retries_when_max_attempts_zero(self) -> None:
        """max_attempts=0 means infinite retries; verify it keeps going."""
        call_count = 0

        def fail_then_succeed() -> Ok[None] | Err[BrokerOperationError]:
            nonlocal call_count
            call_count += 1
            if call_count <= 10:
                return _schema_err(
                    'connection refused',
                    exception=OperationalError('connection refused'),
                )
            return Ok(None)

        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(side_effect=fail_then_succeed)
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
            db_retry_max_attempts=0,
        )
        logger = logging.getLogger('test')

        with patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock):
            await _ensure_schema_with_retry(broker, resilience, logger)

        assert call_count == 11

    async def test_backoff_delays_increase(self) -> None:
        """Verify that sleep delays increase between retries."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[
                _schema_err('fail', exception=OperationalError('fail')),
                _schema_err('fail', exception=OperationalError('fail')),
                _schema_err('fail', exception=OperationalError('fail')),
                Ok(None),
            ]
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=1000,
            db_retry_max_ms=100_000,
            db_retry_max_attempts=0,
        )
        logger = logging.getLogger('test')

        with (
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock) as mock_sleep,
            patch('horsies.core.cli.random.uniform', return_value=0.0),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        delays = [call.args[0] for call in mock_sleep.await_args_list]
        assert len(delays) == 3
        # With zero jitter: 1.0s, 2.0s, 4.0s — each exactly doubles
        for i in range(1, len(delays)):
            assert delays[i] > delays[i - 1], (
                f'delay[{i}]={delays[i]:.3f}s should be > delay[{i-1}]={delays[i-1]:.3f}s'
            )

    async def test_backoff_capped_at_max_ms(self) -> None:
        """Delay stops growing once it hits db_retry_max_ms."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[
                _schema_err('fail', exception=OperationalError('fail')),
                _schema_err('fail', exception=OperationalError('fail')),
                _schema_err('fail', exception=OperationalError('fail')),
                _schema_err('fail', exception=OperationalError('fail')),
                Ok(None),
            ]
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=1000,
            db_retry_max_ms=1500,
            db_retry_max_attempts=0,
        )
        logger = logging.getLogger('test')

        with (
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock) as mock_sleep,
            patch('horsies.core.cli.random.uniform', return_value=0.0),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        delays = [call.args[0] for call in mock_sleep.await_args_list]
        assert len(delays) == 4
        # attempt 1: min(1500, 1000*1) = 1000 → 1.0s
        # attempt 2: min(1500, 1000*2) = 1500 → 1.5s
        # attempt 3: min(1500, 1000*4) = 1500 → 1.5s (capped)
        # attempt 4: min(1500, 1000*8) = 1500 → 1.5s (capped)
        assert delays[0] == pytest.approx(1.0)
        assert delays[1] == pytest.approx(1.5)
        assert delays[2] == pytest.approx(1.5)
        assert delays[3] == pytest.approx(1.5)

    async def test_delay_floor_at_100ms(self) -> None:
        """Sleep delay never drops below 0.1s even with negative jitter."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[
                _schema_err('fail', exception=OperationalError('fail')),
                Ok(None),
            ],
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
            db_retry_max_attempts=0,
        )
        logger = logging.getLogger('test')

        # base_ms=100, jitter_range=25 → return -25 makes delay_ms=75 → 0.075s
        with (
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock) as mock_sleep,
            patch('horsies.core.cli.random.uniform', return_value=-25.0),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        delay = mock_sleep.await_args_list[0].args[0]
        assert delay == pytest.approx(0.1)

    async def test_retries_on_interface_error(self) -> None:
        """InterfaceError is also retryable, not just OperationalError."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[
                _schema_err(
                    'server closed connection',
                    exception=InterfaceError('server closed connection'),
                ),
                Ok(None),
            ],
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
        )
        logger = logging.getLogger('test')

        with patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock):
            await _ensure_schema_with_retry(broker, resilience, logger)

        assert broker.ensure_schema_initialized.await_count == 2

    async def test_max_attempts_one_boundary(self) -> None:
        """max_attempts=1: initial try + 1 retry = 2 total calls, then raise."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            return_value=_schema_err(
                'connection refused',
                exception=OperationalError('connection refused'),
            ),
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
            db_retry_max_attempts=1,
        )
        logger = logging.getLogger('test')

        with (
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock),
            pytest.raises(RuntimeError, match='connection refused'),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        assert broker.ensure_schema_initialized.await_count == 2

    async def test_logs_retry_message(self, caplog: pytest.LogCaptureFixture) -> None:
        """Each retry logs an error with attempt count and delay info."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[
                _schema_err('connection refused', exception=OperationalError('connection refused')),
                Ok(None),
            ],
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
            db_retry_max_attempts=0,
        )
        logger = logging.getLogger('test.retry_msg')

        with (
            caplog.at_level(logging.ERROR, logger='test.retry_msg'),
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock),
            patch('horsies.core.cli.random.uniform', return_value=0.0),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        retry_logs = [r for r in caplog.records if 'Retrying in' in r.message]
        assert len(retry_logs) == 1
        assert 'attempt 1/inf' in retry_logs[0].message

    async def test_logs_exhaustion_message(self, caplog: pytest.LogCaptureFixture) -> None:
        """Final failure logs error with total attempt count."""
        broker = AsyncMock()
        broker.ensure_schema_initialized = AsyncMock(
            return_value=_schema_err(
                'connection refused',
                exception=OperationalError('connection refused'),
            ),
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=100,
            db_retry_max_ms=500,
            db_retry_max_attempts=1,
        )
        logger = logging.getLogger('test.exhaust_msg')

        with (
            caplog.at_level(logging.ERROR, logger='test.exhaust_msg'),
            patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock),
            pytest.raises(RuntimeError),
        ):
            await _ensure_schema_with_retry(broker, resilience, logger)

        exhaustion_logs = [
            r for r in caplog.records
            if 'failed after' in r.message
        ]
        assert len(exhaustion_logs) == 1
        assert '2 attempts' in exhaustion_logs[0].message
