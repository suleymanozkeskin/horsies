# horsies/core/utils/db.py
"""Shared database helpers: transient error classification, engine hooks,
and the one way to run a statement in autocommit (``run_in_autocommit_result``,
and ``run_in_autocommit``, which raises its failure)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

from psycopg import InterfaceError, OperationalError
from psycopg.types.string import TextLoader
from sqlalchemy import TextClause, event
from sqlalchemy.exc import (
    DBAPIError,
    OperationalError as SAOperationalError,
    TimeoutError as SATimeoutError,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.types.result import Err, Ok, Result


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
statement as its own transaction."""


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

    Postgres commits the statement when it ends: no BEGIN and no COMMIT is
    sent, so the call is one round trip, plus the pool pre-ping when the
    engine has it on. The locks that the statement takes end with it. Use
    it only for a write that is complete in one statement: a write that
    needs a second statement to commit with it runs in a transaction.

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
            connection = await session.connection(
                execution_options=dict(AUTOCOMMIT_EXECUTION_OPTIONS),
            )
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
    match await run_in_autocommit_result(session_factory, statement):
        case Ok(rows):
            return rows
        case Err(
            NotSent(error=error)
            | StatementRefused(error=error)
            | OutcomeUnknown(error=error)
        ):
            raise error
