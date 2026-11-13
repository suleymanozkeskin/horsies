# horsies/core/utils/db.py
"""Shared database helpers: transient error classification, engine hooks,
and the worker's ways to write without a transaction.

Autocommit has one owner, this module, and one setting
(``AUTOCOMMIT_EXECUTION_OPTIONS``). Three forms use it:

- One statement on one checkout: ``run_in_autocommit_result`` returns the
  rows of the statement, read in full before the connection goes back, or
  its closed failure; ``run_in_autocommit`` raises that failure instead.
  ``PooledStatements`` is this form as a ``StatementRunner``.
- One statement at a time on a connection that one loop keeps:
  ``HeldConnection``, a ``StatementRunner`` with the same outcomes.
- Several statements on one checkout: ``autocommit_connection``. Each
  statement commits alone; nothing makes them commit together.

A write whose statements must commit together runs in a transaction, never
in one of these forms."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, Protocol, assert_never, cast

import psycopg
from psycopg import InterfaceError, OperationalError
from psycopg.rows import TupleRow
from psycopg.types.string import TextLoader
from sqlalchemy import TextClause, event
from sqlalchemy.engine import Dialect
from sqlalchemy.exc import (
    DBAPIError,
    OperationalError as SAOperationalError,
    TimeoutError as SATimeoutError,
)
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.pool import PoolProxiedConnection
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.sqltypes import NullType

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import DBAPIModule

logger = get_logger('db')


def register_identity_text_reads(engine: AsyncEngine) -> None:
    """Make every connection of ``engine`` read uuid columns as text.

    Identity columns are uuid in the database and strings at the Python
    boundary. The ORM's ``Uuid(as_uuid=False)`` already presents them as
    strings; raw ``text()`` reads would otherwise hand back
    ``uuid.UUID`` objects, and every consumer written against the
    string presentation — kwargs serialization, log slicing, JSON
    payloads — would have to defend against the drift one seam at a
    time. One loader on the driver keeps the presentation single-owner.

    Scope: horsies registers this on the engines IT creates (the
    broker's runtime engine; the integration conftest engine). It is
    never assumed onto engines a consumer application builds — their
    uuid reads keep whatever presentation their driver configuration
    chooses.

    A test double standing in for the engine cannot host driver
    events; registration is a no-op for anything that is not a real
    ``AsyncEngine``.
    """
    if not isinstance(engine, AsyncEngine):
        return

    @event.listens_for(engine.sync_engine, 'connect')
    def _register_uuid_text_loader(
        dbapi_connection: Any, _connection_record: Any
    ) -> None:
        dbapi_connection.driver_connection.adapters.register_loader(
            'uuid', TextLoader
        )


class SessionSettings(Enum):
    """Whether a pooled server session keeps a client's session settings.

    KEPT: a direct or session-pooled connection; a session-level SET stays
    with the client that issued it. NOT_KEPT: a PgBouncer transaction-mode
    connection; server sessions are shared between clients, so a session
    SET would leak to other clients.
    """

    KEPT = 'KEPT'
    NOT_KEPT = 'NOT_KEPT'


# The partition coverage probe's cost estimate is above the default
# jit_above_cost, so a JIT-enabled server compiles it on every call.
DISABLE_JIT_SQL: Final = 'SET jit = off'


def register_session_settings(
    engine: AsyncEngine,
    settings: SessionSettings,
) -> None:
    """Run ``SET jit = off`` once on each new connection that keeps it.

    The SET runs in autocommit, so a rollback of the connection's first
    transaction cannot undo it. A NOT_KEPT engine keeps the server's JIT
    configuration. Like ``register_identity_text_reads``, registration is
    a no-op for anything that is not a real ``AsyncEngine``.
    """
    if not isinstance(engine, AsyncEngine):  # pyright: ignore[reportUnnecessaryIsInstance]
        return
    match settings:
        case SessionSettings.NOT_KEPT:
            return
        case SessionSettings.KEPT:
            pass

    def _disable_jit(dbapi_connection: Any, _connection_record: Any) -> None:
        autocommit = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(DISABLE_JIT_SQL)
            finally:
                cursor.close()
        finally:
            dbapi_connection.autocommit = autocommit

    event.listen(engine.sync_engine, 'connect', _disable_jit)


def is_dbapi_disconnect(exc: DBAPIError) -> bool:
    """Check whether a SQLAlchemy DBAPIError represents a connection disconnect."""
    connection_invalidated = bool(getattr(exc, 'connection_invalidated', False))
    is_disconnect = bool(getattr(exc, 'is_disconnect', False))
    return connection_invalidated or is_disconnect


def is_retryable_connection_error(exc: BaseException) -> bool:
    """Check whether an exception is a transient connection error worth retrying."""
    match exc:
        case OperationalError() | InterfaceError() | SAOperationalError():
            return True
        # Engine pool checkout timeout ("QueuePool limit reached"): transient
        # infrastructure pressure, same class as a failed connect. It only
        # subclasses SQLAlchemyError, so the arms above never match it;
        # misclassifying it as permanent latched the reaper's requeue breaker
        # off for the process lifetime during a connection-slot exhaustion.
        case SATimeoutError():
            return True
        case DBAPIError() as db_exc if is_dbapi_disconnect(db_exc):
            return True
        case _:
            return False


AUTOCOMMIT_EXECUTION_OPTIONS: Final[Mapping[str, str]] = {
    'isolation_level': 'AUTOCOMMIT',
}
"""The SQLAlchemy execution options of a connection that runs each
statement as its own transaction. ``autocommit_connection`` is the only
reader."""


async def autocommit_connection(session: AsyncSession) -> AsyncConnection:
    """The connection of a session that has not begun, in autocommit.

    Contract: several statements without a transaction. Each statement on
    the connection commits when it ends: no BEGIN and no COMMIT round trip.
    Nothing makes two statements commit together, so a caller sends a
    statement only when it is complete alone. The fence of a later statement
    is the only link to an earlier one; the failed-attempt write is the
    example: an unlocked read, then a fenced write.

    The session close sends nothing, and the pool gives the connection back
    to the next session in the engine's isolation level. The locks that a
    statement takes end with the statement.

    Raises:
        Exception: what the pool raises for the checkout: a pool timeout or
            a connection error.
    """
    return await session.connection(
        execution_options=dict(AUTOCOMMIT_EXECUTION_OPTIONS),
    )


@dataclass(frozen=True, slots=True)
class SingleStatement:
    """One SQL statement and its bound parameters.

    ``run_in_autocommit`` sends it with one execute call. psycopg sends a
    statement that has bound parameters with the extended query protocol, and
    Postgres refuses a text with a second command there ("cannot insert
    multiple commands into a prepared statement"). Without parameters psycopg
    sends a simple query, which can carry several commands, so construction
    refuses an empty parameter mapping.

    Raises:
        ValueError: ``parameters`` is empty. The statements are library
            constants, so this is a defect of the caller.
    """

    statement: TextClause
    parameters: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.parameters:
            raise ValueError(
                'SingleStatement has no bound parameter: psycopg would send '
                'it as a simple query, which can carry several statements',
            )


@dataclass(frozen=True, slots=True)
class StatementRows:
    """The rows of one statement, read in full before its connection went
    back to the pool. ``columns`` names the values of each row, in order."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]

    def mappings(self) -> list[dict[str, Any]]:
        """Each row as a mapping from column name to value."""
        return [dict(zip(self.columns, row, strict=True)) for row in self.rows]


@dataclass(frozen=True, slots=True)
class NotSent:
    """The statement did not reach the server: the pool had no connection in
    time (``error`` is a pool ``TimeoutError``), or the connect failed."""

    error: SATimeoutError | DBAPIError


@dataclass(frozen=True, slots=True)
class StatementRefused:
    """The driver or the server refused the statement, and the connection
    stayed usable. Nothing of the statement committed: the driver refused it
    before it sent it, or the server reported an ERROR, which ends the
    statement's own transaction."""

    error: DBAPIError


@dataclass(frozen=True, slots=True)
class OutcomeUnknown:
    """The connection broke after the statement went to the driver. The
    statement may have committed, as after a lost COMMIT reply."""

    error: DBAPIError


type AutocommitFailure = NotSent | StatementRefused | OutcomeUnknown


async def run_in_autocommit_result(
    session_factory: async_sessionmaker[AsyncSession],
    statement: SingleStatement,
) -> Result[StatementRows, AutocommitFailure]:
    """Run one statement as its own transaction and return its rows.

    Contract: one statement per checkout. Postgres commits the statement
    when it ends: no BEGIN and no COMMIT is sent, so the call is one round
    trip, plus the pool pre-ping when the engine has it on. The locks that
    the statement takes end with it. Use it only for a write that is
    complete in one statement: a write that needs a second statement to
    commit with it runs in a transaction. The connection comes from
    ``autocommit_connection``.

    The connection goes back to the pool in the engine's isolation level:
    SQLAlchemy resets it on return and sends nothing for that.

    Outcomes:
        Ok(rows): the statement committed; every row it returned.
        Err(NotSent): the checkout failed; nothing was sent.
        Err(StatementRefused): nothing committed; the connection is usable.
        Err(OutcomeUnknown): the connection broke; the statement may have
            committed. The caller classifies its replay.

    A statement that returns no rows is a defect of the caller
    (``AssertionError``). Other exceptions are defects and propagate.
    """
    async with session_factory() as session:
        try:
            connection = await autocommit_connection(session)
        except (SATimeoutError, DBAPIError) as exc:
            return Err(NotSent(error=exc))
        try:
            result = await connection.execute(
                statement.statement,
                dict(statement.parameters),
            )
        except DBAPIError as exc:
            if is_dbapi_disconnect(exc):
                return Err(OutcomeUnknown(error=exc))
            return Err(StatementRefused(error=exc))
        assert result.returns_rows, 'run_in_autocommit needs a statement with rows'
        columns = tuple(str(key) for key in result.keys())
        rows = tuple(tuple(row) for row in result.fetchall())
    return Ok(StatementRows(columns=columns, rows=rows))


async def run_in_autocommit(
    session_factory: async_sessionmaker[AsyncSession],
    statement: SingleStatement,
) -> StatementRows:
    """``run_in_autocommit_result``, raising the error of its failure.

    Raises:
        Exception: what the pool and the driver raise: a pool timeout, a
            connection error, a statement error. A connection error after
            the statement was sent leaves its outcome unknown: it may have
            committed, as after a lost COMMIT reply. The caller classifies
            its replay.
    """
    return rows_or_raise(await run_in_autocommit_result(session_factory, statement))


def rows_or_raise(
    result: Result[StatementRows, AutocommitFailure],
) -> StatementRows:
    """The rows of ``result``, or the error of its failure, raised. Pure.

    Raises:
        SATimeoutError | DBAPIError: the error of ``NotSent``,
            ``StatementRefused`` or ``OutcomeUnknown``.
    """
    match result:
        case Ok(rows):
            return rows
        case Err(
            NotSent(error=error)
            | StatementRefused(error=error)
            | OutcomeUnknown(error=error)
        ):
            raise error


class StatementRunner(Protocol):
    """Runs one statement as its own transaction and returns its rows, with
    the outcomes of ``run_in_autocommit_result``."""

    async def run(
        self,
        statement: SingleStatement,
    ) -> Result[StatementRows, AutocommitFailure]: ...


@dataclass(frozen=True, slots=True)
class PooledStatements:
    """A ``StatementRunner`` that checks out a connection of
    ``session_factory`` for each statement: ``run_in_autocommit_result``."""

    session_factory: async_sessionmaker[AsyncSession]

    async def run(
        self,
        statement: SingleStatement,
    ) -> Result[StatementRows, AutocommitFailure]:
        """``run_in_autocommit_result`` of ``statement``."""
        return await run_in_autocommit_result(self.session_factory, statement)


HELD_CONNECTION_IDLE_RELEASE_S: Final = 1.0
"""Seconds with no statement after which a ``HeldConnection`` gives its
connection back to the pool. Under load its loop sends a statement every few
milliseconds and keeps the connection; an idle loop leaves the pool as free
as the per-call path does."""

HELD_STATEMENTS_BOUND: Final = 8
"""The most statement texts that one ``HeldConnection`` compiles. Its
callers pass library constants: one loop sends two or three of them."""

DISCONNECT_PROBE_SQL: Final = 'SELECT 1'
"""The statement that ``HeldConnection`` sends through SQLAlchemy on a broken
connection, so that SQLAlchemy runs its own disconnect handling."""


@dataclass(frozen=True, slots=True)
class RecycleAfter:
    """The held connection goes back to the pool once it was held this
    long, and the next statement checks out another: the pool's
    ``pool_recycle``. 0 gives the connection back after every statement.

    Raises:
        ValueError: ``seconds`` is negative.
    """

    seconds: float

    def __post_init__(self) -> None:
        if self.seconds < 0:
            raise ValueError(
                f'RecycleAfter.seconds must be 0 or more, got {self.seconds}',
            )


@dataclass(frozen=True, slots=True)
class NoRecycle:
    """The pool does not recycle connections by age (a negative
    ``pool_recycle``): the held connection is kept until an idle period, a
    failure or ``release``."""


type HeldRecycle = RecycleAfter | NoRecycle


def held_recycle(pool_recycle_s: int) -> HeldRecycle:
    """The ``HeldRecycle`` of a pool whose ``pool_recycle`` is
    ``pool_recycle_s``, as SQLAlchemy reads it: a negative value turns
    recycling off. Pure."""
    if pool_recycle_s < 0:
        return NoRecycle()
    return RecycleAfter(seconds=float(pool_recycle_s))


@dataclass(frozen=True, slots=True)
class _HeldStatement:
    """A statement text compiled once for psycopg. ``query`` is the
    compiled SQL as bytes, the query form that psycopg takes from a
    variable."""

    clause: TextClause
    compiled: SQLCompiler
    query: bytes


@dataclass(slots=True)
class _Holding:
    """The connection that a ``HeldConnection`` holds, and the session that
    checked it out: closing the session gives it back. ``last_used`` and
    ``idle_check`` change after each statement."""

    session: AsyncSession
    connection: AsyncConnection
    pooled: PoolProxiedConnection
    driver: psycopg.AsyncConnection[TupleRow]
    taken_at: float
    last_used: float
    idle_check: asyncio.TimerHandle


@dataclass(frozen=True, slots=True)
class _Free:
    """A ``HeldConnection`` that holds no connection: the next statement
    checks one out."""


type _HeldState = _Holding | _Free


class HeldConnection:
    """A ``StatementRunner`` that keeps one connection of the pool of
    ``session_factory``, in autocommit, and sends each statement on its
    psycopg connection.

    Contract:

    - The first statement checks out a connection through a session of
      ``session_factory`` (``autocommit_connection``); later statements reuse
      it. Nothing touches the factory before the first statement. Each statement commits when it ends, as with
      ``run_in_autocommit_result``, and the locks it takes end with it.
    - One statement at a time: one loop owns the runner. A second
      concurrent ``run`` is a defect (``AssertionError``).
    - The connection goes back to the pool after ``idle_release_s`` with no
      statement, once it was held for the recycle age, and on ``release``.
    - A statement whose connection broke (the dialect's disconnect check)
      returns ``OutcomeUnknown``. The runner then sends
      ``DISCONNECT_PROBE_SQL`` on the broken connection through SQLAlchemy,
      whose disconnect handling invalidates that connection and every
      connection of the pool, as after a disconnect on the per-call path.
    - A cancelled statement invalidates the held connection: its state is
      unknown. The cancel propagates.
    - Statements carry untyped bind parameters only: the values go to
      psycopg unchanged. A typed bind is a defect (``AssertionError``).

    It does not hold a transaction open, and it does not retry.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        recycle: HeldRecycle,
        *,
        idle_release_s: float = HELD_CONNECTION_IDLE_RELEASE_S,
    ) -> None:
        if not idle_release_s > 0:
            raise ValueError(
                f'idle_release_s must be positive, got {idle_release_s}',
            )
        self._session_factory = session_factory
        self._recycle = recycle
        self._idle_release_s = idle_release_s
        self._state: _HeldState = _Free()
        self._running = False
        self._statements: dict[int, _HeldStatement] = {}
        self._giving_back: set[asyncio.Task[None]] = set()

    async def run(
        self,
        statement: SingleStatement,
    ) -> Result[StatementRows, AutocommitFailure]:
        """Run ``statement`` on the held connection; outcomes and failures as
        ``run_in_autocommit_result``.

        Outcomes:
            Ok(rows): the statement committed; every row it returned.
            Err(NotSent): the checkout failed; nothing was sent.
            Err(StatementRefused): nothing committed; the connection stays
                held.
            Err(OutcomeUnknown): the connection broke; the statement may
                have committed. The pool's connections are invalidated.
        """
        assert not self._running, 'HeldConnection runs one statement at a time'
        self._running = True
        try:
            return await self._run_once(statement)
        finally:
            self._running = False

    async def release(self) -> None:
        """Give the held connection back to the pool, and wait for the
        connections that idle periods gave back. The next ``run`` checks
        out another connection. Safe to call when nothing is held."""
        assert not self._running, 'HeldConnection.release during a statement'
        match self._state:
            case _Holding() as holding:
                self._state = _Free()
                await _give_back(holding)
            case _Free():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        if self._giving_back:
            _ = await asyncio.gather(*self._giving_back, return_exceptions=True)

    async def _run_once(
        self,
        statement: SingleStatement,
    ) -> Result[StatementRows, AutocommitFailure]:
        match await self._take():
            case Ok(holding):
                pass
            case Err(not_sent):
                return Err(not_sent)
        held = self._compiled(statement.statement, holding.connection.dialect)
        parameters = held.compiled.construct_params(dict(statement.parameters))
        try:
            async with holding.driver.cursor() as cursor:
                _ = await cursor.execute(held.query, parameters)
                description = cursor.description
                assert description is not None, (
                    'HeldConnection needs a statement with rows'
                )
                rows = await cursor.fetchall()
        except psycopg.Error as exc:
            return await self._failed(holding, held, parameters, exc)
        except asyncio.CancelledError:
            self._state = _Free()
            await _drop_after_cancel(holding)
            raise
        holding.last_used = asyncio.get_running_loop().time()
        return Ok(
            StatementRows(
                columns=tuple(column.name for column in description),
                rows=tuple(tuple(row) for row in rows),
            ),
        )

    async def _failed(
        self,
        holding: _Holding,
        held: _HeldStatement,
        parameters: Mapping[str, Any],
        exc: psycopg.Error,
    ) -> Result[StatementRows, AutocommitFailure]:
        """Classify the driver error of a statement as the per-call path does."""
        dialect = holding.connection.dialect
        disconnect = _dialect_sees_disconnect(dialect, exc, holding.pooled)
        error = DBAPIError.instance(
            held.compiled.string,
            dict(parameters),
            exc,
            psycopg.Error,
            connection_invalidated=disconnect,
            dialect=dialect,
        )
        assert isinstance(error, DBAPIError), error
        if not disconnect:
            holding.last_used = asyncio.get_running_loop().time()
            return Err(StatementRefused(error=error))
        self._state = _Free()
        holding.idle_check.cancel()
        await _invalidate_pool_through(holding)
        return Err(OutcomeUnknown(error=error))

    async def _take(self) -> Result[_Holding, NotSent]:
        """The held connection, or a new checkout when none is held or the
        held one reached the recycle age."""
        loop = asyncio.get_running_loop()
        match self._state:
            case _Holding() as holding if not _recycle_due(
                self._recycle,
                holding,
                loop.time(),
            ):
                return Ok(holding)
            case _Holding() as holding:
                self._state = _Free()
                await _give_back(holding)
            case _Free():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        session = self._session_factory()
        try:
            connection = await autocommit_connection(session)
            pooled = await connection.get_raw_connection()
        except (SATimeoutError, DBAPIError) as exc:
            await session.close()
            return Err(NotSent(error=exc))
        now = loop.time()
        holding = _Holding(
            session=session,
            connection=connection,
            pooled=pooled,
            driver=_driver_of(pooled),
            taken_at=now,
            last_used=now,
            idle_check=loop.call_later(self._idle_release_s, self._check_idle),
        )
        self._state = holding
        return Ok(holding)

    def _check_idle(self) -> None:
        """Timer callback: give the connection back when no statement ran
        for ``idle_release_s``; otherwise check again when that time would
        pass."""
        match self._state:
            case _Holding() as holding:
                pass
            case _Free():
                return
            case _ as unreachable:
                assert_never(unreachable)
        loop = asyncio.get_running_loop()
        idle_for = loop.time() - holding.last_used
        if self._running:
            holding.idle_check = loop.call_later(
                self._idle_release_s,
                self._check_idle,
            )
            return
        if idle_for < self._idle_release_s:
            holding.idle_check = loop.call_later(
                self._idle_release_s - idle_for,
                self._check_idle,
            )
            return
        self._state = _Free()
        task = loop.create_task(_give_back(holding))
        self._giving_back.add(task)
        task.add_done_callback(self._given_back)

    def _given_back(self, task: asyncio.Task[None]) -> None:
        self._giving_back.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error('Giving back an idle held connection failed: %r', error)

    def _compiled(self, clause: TextClause, dialect: Dialect) -> _HeldStatement:
        """``clause`` compiled for ``dialect``, once per runner: the dialect of
        one pool does not change."""
        cached = self._statements.get(id(clause))
        if cached is not None and cached.clause is clause:
            return cached
        assert len(self._statements) < HELD_STATEMENTS_BOUND, (
            f'HeldConnection compiled {len(self._statements)} statement '
            'texts; its callers pass library constants'
        )
        compiled = clause.compile(dialect=dialect)
        assert isinstance(compiled, SQLCompiler), type(compiled)
        assert not compiled.positional, 'HeldConnection needs named parameters'
        typed = [
            name
            for name, bind in compiled.binds.items()
            if not isinstance(bind.type, NullType)
        ]
        assert not typed, (
            f'HeldConnection sends parameters unchanged; typed binds {typed}'
        )
        held = _HeldStatement(
            clause=clause,
            compiled=compiled,
            query=compiled.string.encode(),
        )
        self._statements[id(clause)] = held
        return held


def _driver_of(pooled: PoolProxiedConnection) -> psycopg.AsyncConnection[TupleRow]:
    """The psycopg connection under ``pooled``. Any other driver is a defect:
    the held path sends psycopg calls."""
    driver = pooled.driver_connection
    assert isinstance(driver, psycopg.AsyncConnection), type(driver)
    # isinstance narrows to AsyncConnection[Unknown]; SQLAlchemy opens its
    # psycopg connections with the default row factory, tuple rows.
    return cast('psycopg.AsyncConnection[TupleRow]', driver)


def _dialect_sees_disconnect(
    dialect: Dialect,
    exc: psycopg.Error,
    pooled: PoolProxiedConnection,
) -> bool:
    """The dialect's disconnect check, as SQLAlchemy runs it on the per-call
    path (for psycopg: the connection is closed or broken)."""
    # SQLAlchemy types the error as the class of its DBAPI protocol; at run
    # time it passes the driver's own error, as here.
    return bool(
        dialect.is_disconnect(cast('DBAPIModule.Error', exc), pooled, None),
    )


def _recycle_due(recycle: HeldRecycle, holding: _Holding, now: float) -> bool:
    """Whether ``holding`` was held for the recycle age. Pure."""
    match recycle:
        case RecycleAfter(seconds=seconds):
            return now - holding.taken_at >= seconds
        case NoRecycle():
            return False
        case _ as unreachable:
            assert_never(unreachable)


async def _give_back(holding: _Holding) -> None:
    """Return the connection of ``holding`` to the pool: close its session."""
    holding.idle_check.cancel()
    await holding.session.close()


async def _drop_after_cancel(holding: _Holding) -> None:
    """Invalidate the connection of a cancelled statement: its state is
    unknown. A driver error of the invalidation is logged, so that the
    caller's cancel propagates."""
    holding.idle_check.cancel()
    try:
        await holding.connection.invalidate()
        await holding.session.close()
    except DBAPIError as exc:
        logger.warning('Invalidating a cancelled held connection failed: %r', exc)


async def _invalidate_pool_through(holding: _Holding) -> None:
    """Send ``DISCONNECT_PROBE_SQL`` through SQLAlchemy on the broken
    connection of ``holding``. SQLAlchemy sees the disconnect and invalidates
    the connection and every connection of the pool. Then the session is
    closed. A probe that runs (the connection works after all) invalidates
    only this connection."""
    connection = holding.connection
    try:
        _ = await connection.exec_driver_sql(DISCONNECT_PROBE_SQL)
    except DBAPIError as probed:
        if not probed.connection_invalidated:
            await connection.invalidate(probed)
    else:
        logger.warning(
            'A held connection that the dialect saw as broken answered the '
            'probe; it is invalidated alone',
        )
        await connection.invalidate()
    await holding.session.close()
