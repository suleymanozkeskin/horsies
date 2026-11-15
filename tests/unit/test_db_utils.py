"""Unit tests for horsies.core.utils.db error classification helpers."""

from __future__ import annotations

import asyncio

import pytest

from unittest.mock import AsyncMock, MagicMock

from psycopg import InterfaceError, OperationalError
from sqlalchemy import text
from sqlalchemy.exc import (
    DBAPIError,
    OperationalError as SAOperationalError,
    TimeoutError as SATimeoutError,
)

from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import (
    AUTOCOMMIT_EXECUTION_OPTIONS,
    HeldConnection,
    NoRecycle,
    NotSent,
    OutcomeUnknown,
    PooledStatements,
    RecycleAfter,
    SingleStatement,
    StatementRefused,
    StatementRows,
    autocommit_connection,
    held_recycle,
    is_dbapi_disconnect,
    is_retryable_connection_error,
    rows_or_raise,
    run_in_autocommit,
    run_in_autocommit_result,
)


def _make_dbapi_error(
    *,
    connection_invalidated: bool = False,
    is_disconnect: bool = False,
) -> DBAPIError:
    exc = DBAPIError(
        statement='SELECT 1',
        params=None,
        orig=Exception('test'),
        connection_invalidated=connection_invalidated,
    )
    if is_disconnect:
        exc.is_disconnect = is_disconnect  # type: ignore[reportAttributeAccessIssue]
    return exc


@pytest.mark.unit
class TestIsDBAPIDisconnect:
    """Tests for is_dbapi_disconnect."""

    def test_connection_invalidated_true(self) -> None:
        exc = _make_dbapi_error(connection_invalidated=True)
        assert is_dbapi_disconnect(exc) is True

    def test_is_disconnect_true(self) -> None:
        exc = _make_dbapi_error(is_disconnect=True)
        assert is_dbapi_disconnect(exc) is True

    def test_both_flags_set(self) -> None:
        exc = _make_dbapi_error(connection_invalidated=True, is_disconnect=True)
        assert is_dbapi_disconnect(exc) is True

    def test_neither_flag_set(self) -> None:
        exc = _make_dbapi_error()
        assert is_dbapi_disconnect(exc) is False


@pytest.mark.unit
class TestIsRetryableConnectionError:
    """Tests for is_retryable_connection_error."""

    def test_psycopg_operational_error(self) -> None:
        assert is_retryable_connection_error(OperationalError('connection refused')) is True

    def test_psycopg_interface_error(self) -> None:
        assert is_retryable_connection_error(InterfaceError('broken pipe')) is True

    def test_sqlalchemy_operational_error(self) -> None:
        exc = SAOperationalError('lost connection', {}, Exception('orig'))
        assert is_retryable_connection_error(exc) is True

    def test_sqlalchemy_pool_timeout_retryable(self) -> None:
        """Engine pool checkout timeout is transient, never a permanent failure.

        Regression pin: this shape ("QueuePool limit reached") classified as
        non-retryable and three consecutive reaper passes during a
        connection-slot exhaustion latched the requeue breaker off for the
        process lifetime, stranding orphaned CLAIMED tasks after recovery.
        """
        exc = SATimeoutError('QueuePool limit of size 5 overflow 2 reached')
        assert is_retryable_connection_error(exc) is True

    def test_psycopg_pool_timeout_retryable(self) -> None:
        """psycopg_pool.PoolTimeout subclasses OperationalError; pin it too."""
        from psycopg_pool import PoolTimeout

        assert is_retryable_connection_error(PoolTimeout('pool exhausted')) is True

    def test_dbapi_error_with_disconnect(self) -> None:
        exc = _make_dbapi_error(connection_invalidated=True)
        assert is_retryable_connection_error(exc) is True

    def test_dbapi_error_with_is_disconnect_only(self) -> None:
        exc = _make_dbapi_error(is_disconnect=True)
        assert is_retryable_connection_error(exc) is True

    def test_dbapi_error_without_disconnect(self) -> None:
        exc = _make_dbapi_error()
        assert is_retryable_connection_error(exc) is False

    def test_sa_operational_error_not_invalidated_still_retryable(self) -> None:
        exc = SAOperationalError('conn lost', {}, Exception('orig'))
        exc.connection_invalidated = False
        assert is_retryable_connection_error(exc) is True

    def test_unrelated_exception_not_retryable(self) -> None:
        assert is_retryable_connection_error(ValueError('bad value')) is False

    def test_runtime_error_not_retryable(self) -> None:
        assert is_retryable_connection_error(RuntimeError('something')) is False

    def test_connection_refused_error_not_retryable(self) -> None:
        assert is_retryable_connection_error(ConnectionRefusedError()) is False

    def test_os_error_not_retryable(self) -> None:
        assert is_retryable_connection_error(OSError('timeout')) is False

    def test_base_exception_not_retryable(self) -> None:
        assert is_retryable_connection_error(KeyboardInterrupt()) is False

    def test_empty_message_psycopg_operational_error(self) -> None:
        assert is_retryable_connection_error(OperationalError('')) is True

    def test_deadlock_detected_is_retryable(self) -> None:
        """Postgres 40P01 must stay retryable.

        Cancel cascades lock parent-then-child while completion bubbling
        locks child-then-parent; the resulting deadlock aborts one
        transaction and the retry paths re-drive it. DeadlockDetected
        subclasses psycopg OperationalError, so this holds today — this
        test pins it against classifier or driver-mapping changes.
        """
        from psycopg.errors import DeadlockDetected

        assert is_retryable_connection_error(DeadlockDetected('boom')) is True


@pytest.mark.unit
class TestSingleStatement:
    """``SingleStatement`` is the one input of ``run_in_autocommit``."""

    def test_a_statement_without_a_bound_parameter_is_refused(self) -> None:
        """psycopg sends such a text as a simple query, which can carry two
        statements."""
        with pytest.raises(ValueError, match='no bound parameter'):
            SingleStatement(statement=text('SELECT 1'), parameters={})

    def test_a_statement_with_a_bound_parameter_is_kept(self) -> None:
        statement = SingleStatement(
            statement=text('SELECT :value'),
            parameters={'value': 1},
        )

        assert statement.parameters == {'value': 1}


@pytest.mark.unit
class TestStatementRows:
    def test_each_row_maps_its_columns_to_its_values(self) -> None:
        rows = StatementRows(columns=('a', 'b'), rows=((1, 2), (3, 4)))

        assert rows.mappings() == [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]

    def test_no_rows_maps_to_no_mappings(self) -> None:
        assert StatementRows(columns=('a',), rows=()).mappings() == []


@pytest.mark.unit
class TestRunInAutocommit:
    @pytest.mark.asyncio
    async def test_it_runs_one_statement_on_an_autocommit_connection(self) -> None:
        result = MagicMock()
        result.returns_rows = True
        result.keys.return_value = ['outcome']
        result.fetchall.return_value = [('APPLIED',)]
        connection = AsyncMock()
        connection.execute = AsyncMock(return_value=result)
        session = AsyncMock()
        session.connection = AsyncMock(return_value=connection)
        factory = MagicMock()
        factory.return_value.__aenter__ = AsyncMock(return_value=session)
        factory.return_value.__aexit__ = AsyncMock(return_value=False)
        statement = text('SELECT :value')

        rows = await run_in_autocommit(
            factory,
            SingleStatement(statement=statement, parameters={'value': 1}),
        )

        session.connection.assert_awaited_once_with(
            execution_options=dict(AUTOCOMMIT_EXECUTION_OPTIONS),
        )
        connection.execute.assert_awaited_once_with(statement, {'value': 1})
        assert AUTOCOMMIT_EXECUTION_OPTIONS == {'isolation_level': 'AUTOCOMMIT'}
        assert rows == StatementRows(columns=('outcome',), rows=(('APPLIED',),))


def _factory_with(
    *,
    checkout_error: Exception | None = None,
    execute_error: Exception | None = None,
) -> tuple[MagicMock, AsyncMock]:
    """A session factory whose checkout or execute raises the given error."""
    result = MagicMock()
    result.returns_rows = True
    result.keys.return_value = ['outcome']
    result.fetchall.return_value = [('APPLIED',)]
    connection = AsyncMock()
    connection.execute = AsyncMock(
        side_effect=execute_error,
        return_value=result,
    )
    session = AsyncMock()
    session.connection = AsyncMock(
        side_effect=checkout_error,
        return_value=connection,
    )
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return factory, connection


_STATEMENT = SingleStatement(statement=text('SELECT :value'), parameters={'value': 1})


@pytest.mark.unit
class TestRunInAutocommitResult:
    """Each failure is classified by whether the statement can have run."""

    @pytest.mark.asyncio
    async def test_rows_are_ok(self) -> None:
        factory, _ = _factory_with()

        result = await run_in_autocommit_result(factory, _STATEMENT)

        assert result == Ok(StatementRows(columns=('outcome',), rows=(('APPLIED',),)))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'error',
        [SATimeoutError('QueuePool limit reached'), _make_dbapi_error()],
        ids=['pool_timeout', 'connect_failed'],
    )
    async def test_a_failed_checkout_is_not_sent(
        self,
        error: SATimeoutError | DBAPIError,
    ) -> None:
        factory, connection = _factory_with(checkout_error=error)

        result = await run_in_autocommit_result(factory, _STATEMENT)

        assert result == Err(NotSent(error=error))
        connection.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_error_on_a_usable_connection_is_refused(self) -> None:
        error = _make_dbapi_error()
        factory, _ = _factory_with(execute_error=error)

        result = await run_in_autocommit_result(factory, _STATEMENT)

        assert result == Err(StatementRefused(error=error))

    @pytest.mark.asyncio
    async def test_a_broken_connection_leaves_the_outcome_unknown(self) -> None:
        error = _make_dbapi_error(connection_invalidated=True)
        factory, _ = _factory_with(execute_error=error)

        result = await run_in_autocommit_result(factory, _STATEMENT)

        assert result == Err(OutcomeUnknown(error=error))

    @pytest.mark.asyncio
    async def test_another_error_is_a_defect_and_propagates(self) -> None:
        factory, _ = _factory_with(execute_error=RuntimeError('defect'))

        with pytest.raises(RuntimeError, match='defect'):
            await run_in_autocommit_result(factory, _STATEMENT)

    @pytest.mark.asyncio
    async def test_the_raising_form_raises_the_error_of_each_failure(self) -> None:
        for kind in ('checkout', 'refused', 'unknown'):
            error = _make_dbapi_error(connection_invalidated=kind == 'unknown')
            factory, _ = (
                _factory_with(checkout_error=error)
                if kind == 'checkout'
                else _factory_with(execute_error=error)
            )
            with pytest.raises(DBAPIError) as raised:
                await run_in_autocommit(factory, _STATEMENT)
            assert raised.value is error


@pytest.mark.unit
class TestAutocommitConnection:
    @pytest.mark.asyncio
    async def test_it_asks_the_session_for_an_autocommit_connection(self) -> None:
        """Several statements on one checkout, each its own transaction: the
        session's connection with the one autocommit setting."""
        connection = AsyncMock()
        session = AsyncMock()
        session.connection = AsyncMock(return_value=connection)

        got = await autocommit_connection(session)

        assert got is connection
        session.connection.assert_awaited_once_with(
            execution_options=dict(AUTOCOMMIT_EXECUTION_OPTIONS),
        )


_HELD_STATEMENT = SingleStatement(
    statement=text('SELECT CAST(:v AS integer)'),
    parameters={'v': 1},
)
_HELD_ROWS = StatementRows(columns=('v',), rows=((1,),))


class TestRowsOrRaise:
    def test_rows_are_returned(self) -> None:
        assert rows_or_raise(Ok(_HELD_ROWS)) is _HELD_ROWS

    @pytest.mark.parametrize(
        'failure',
        [
            NotSent(error=SATimeoutError('QueuePool limit reached')),
            StatementRefused(error=_make_dbapi_error()),
            OutcomeUnknown(error=_make_dbapi_error(connection_invalidated=True)),
        ],
        ids=['not_sent', 'refused', 'unknown'],
    )
    def test_the_error_of_a_failure_is_raised(
        self,
        failure: NotSent | StatementRefused | OutcomeUnknown,
    ) -> None:
        with pytest.raises(type(failure.error)) as raised:
            rows_or_raise(Err(failure))

        assert raised.value is failure.error


class TestHeldRecycle:
    @pytest.mark.parametrize(
        ('pool_recycle_s', 'expected'),
        [
            (-1, NoRecycle()),
            (-5, NoRecycle()),
            (0, RecycleAfter(seconds=0.0)),
            (1800, RecycleAfter(seconds=1800.0)),
        ],
        ids=['off', 'any-negative', 'every-statement', 'default'],
    )
    def test_it_reads_pool_recycle_as_sqlalchemy_does(
        self,
        pool_recycle_s: int,
        expected: NoRecycle | RecycleAfter,
    ) -> None:
        assert held_recycle(pool_recycle_s) == expected

    def test_a_negative_age_is_refused(self) -> None:
        with pytest.raises(ValueError, match='0 or more'):
            RecycleAfter(seconds=-0.5)


class _GatedCheckout:
    """A session whose checkout waits for ``open`` and then fails, as a pool
    with no connection in time does."""

    def __init__(self) -> None:
        self.open = asyncio.Event()
        self.closed = 0

    async def connection(self, execution_options: object) -> object:
        del execution_options
        await self.open.wait()
        raise SATimeoutError('QueuePool limit reached')

    async def close(self) -> None:
        self.closed += 1


class TestHeldConnectionWithoutADatabase:
    def test_nothing_touches_the_factory_before_the_first_statement(self) -> None:
        factory = MagicMock()

        _ = HeldConnection(factory, NoRecycle())

        factory.assert_not_called()

    def test_the_idle_time_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match='idle_release_s'):
            _ = HeldConnection(MagicMock(), NoRecycle(), idle_release_s=0.0)

    @pytest.mark.asyncio
    async def test_a_failed_checkout_is_not_sent_and_closes_its_session(self) -> None:
        session = _GatedCheckout()
        held = HeldConnection(MagicMock(return_value=session), NoRecycle())
        session.open.set()

        outcome = await held.run(_HELD_STATEMENT)

        match outcome:
            case Err(NotSent(error=error)):
                assert isinstance(error, SATimeoutError)
            case _:
                raise AssertionError(f'expected NotSent, got {outcome!r}')
        assert session.closed == 1

    @pytest.mark.asyncio
    async def test_a_second_statement_while_one_runs_is_a_defect(self) -> None:
        session = _GatedCheckout()
        held = HeldConnection(MagicMock(return_value=session), NoRecycle())
        first = asyncio.create_task(held.run(_HELD_STATEMENT))
        await asyncio.sleep(0)

        # Bounded: without the check, the second statement would wait on the
        # same gate, and the test would fail on the timeout, not hang.
        with pytest.raises(AssertionError, match='one statement at a time'):
            await asyncio.wait_for(held.run(_HELD_STATEMENT), 5.0)

        session.open.set()
        _ = await asyncio.wait_for(first, 5.0)

    @pytest.mark.asyncio
    async def test_release_with_nothing_held_does_nothing(self) -> None:
        factory = MagicMock()
        held = HeldConnection(factory, NoRecycle())

        await held.release()

        factory.assert_not_called()


class TestPooledStatements:
    @pytest.mark.asyncio
    async def test_it_runs_one_statement_per_checkout(self) -> None:
        session_factory = MagicMock()
        runner = PooledStatements(session_factory)
        expected = Ok(_HELD_ROWS)
        run = AsyncMock(return_value=expected)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr('horsies.core.utils.db.run_in_autocommit_result', run)
            outcome = await runner.run(_HELD_STATEMENT)

        assert outcome is expected
        run.assert_awaited_once_with(session_factory, _HELD_STATEMENT)
