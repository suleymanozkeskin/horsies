"""Schema compatibility probing for the monitoring app.

The monitoring tool never executes DDL. Creating or migrating the schema
belongs to the library and its workers; pointing this tool at a database must
never change that database's structure. So instead of initializing anything,
the app reads the stored schema version from the ``horsies_migrations``
bookkeeping and adapts:

* the versions agree — full functionality;
* they disagree — reads are still served, because most of them keep working
  across a version step, but actions are refused server-side: writing through
  a contract this build does not share is how data gets corrupted;
* no version at all — the database has no horsies schema, and the app says so
  rather than creating one;
* a horsies 0.x schema — this build neither reads nor upgrades it, and the app
  says so: the remedy is a new database, not a worker start;
* the database could not be reached at all — which is deliberately NOT the
  same answer. "No schema" tells an operator to start a worker; saying that
  about a database that is merely down sends them to fix the wrong thing.

The version is read with a plain SELECT and cached briefly, so a dashboard
left open does not re-query it on every request.

The monitoring app runs the same probe and the action gate in the core
(``create_monitoring_app`` mounts the core's handler); this module keeps the
public states and texts, and ``SchemaProbe`` for Python callers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.baseline.migration import BASELINE_VERSION
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok

logger = get_logger('web')

SCHEMA_TTL_SECONDS = 60.0

SCHEMA_INCOMPATIBLE = 'SCHEMA_INCOMPATIBLE'
SCHEMA_UNKNOWN = 'SCHEMA_UNKNOWN'

EXPECTED_SCHEMA_VERSION = BASELINE_VERSION
"""The schema version this build writes through."""

_BOOKKEEPING_TABLES_SQL = text(
    "SELECT to_regclass('horsies_migrations') IS NOT NULL, "
    "to_regclass('horsies_schema_version') IS NOT NULL"
)
_READ_SCHEMA_VERSION_SQL = text(
    'SELECT COALESCE(max(version), 0) FROM horsies_migrations WHERE success'
)


class SchemaState(str, Enum):
    """How the database's schema relates to the one this build expects."""

    MATCH = 'MATCH'
    MISMATCH = 'MISMATCH'
    # Only ever the result of a successful observation.
    ABSENT = 'ABSENT'
    # A horsies 0.x schema: ``horsies_schema_version`` without
    # ``horsies_migrations`` (Python 0.x), or a highest version below the
    # baseline's (horsies-rust 0.x, whose versions end at 65).
    ZERO_X = 'ZERO_X'
    # The probe has never succeeded, so there is nothing to report yet.
    UNKNOWN = 'UNKNOWN'


@dataclass(frozen=True, slots=True)
class SchemaStatus:
    """The result of a schema probe."""

    state: SchemaState
    version: int | None
    expected_version: int

    @property
    def compatible(self) -> bool:
        """Whether this build may write through this schema."""
        return self.state is SchemaState.MATCH


class SchemaIncompatible(Exception):
    """Raised by the action gate when the schema is not this build's."""

    def __init__(self, status: SchemaStatus) -> None:
        self.status = status
        match status.state:
            case SchemaState.UNKNOWN:
                self.detail = (
                    'Cannot reach the database to determine its schema '
                    'state, so actions are unavailable.'
                )
            case SchemaState.ABSENT:
                self.detail = (
                    'This database has no horsies schema, so actions are '
                    'unavailable. Start a horsies app or worker to '
                    'initialize it; the monitoring tool never modifies the '
                    'database schema.'
                )
            case SchemaState.ZERO_X:
                self.detail = (
                    'This database holds a horsies 0.x schema, so actions '
                    'are unavailable. This build does not read or upgrade a '
                    '0.x database: drain it and point the fleet at a new '
                    'database.'
                )
            case SchemaState.MISMATCH:
                self.detail = (
                    f'Database schema is v{status.version}; this build '
                    f'expects v{status.expected_version}. Actions are '
                    f'disabled until the versions match.'
                )
            case SchemaState.MATCH:
                raise AssertionError(
                    'SchemaIncompatible needs a status this build cannot '
                    'write through; MATCH is compatible'
                )
        super().__init__(self.detail)

    @property
    def code(self) -> str:
        """The wire code naming why the action was refused."""
        return (
            SCHEMA_UNKNOWN
            if self.status.state is SchemaState.UNKNOWN
            else SCHEMA_INCOMPATIBLE
        )


class SchemaProbe:
    """Reads the stored schema version, cached with a short TTL."""

    def __init__(
        self,
        broker: PostgresBroker,
        *,
        ttl_seconds: float = SCHEMA_TTL_SECONDS,
    ) -> None:
        self._broker = broker
        self._ttl_seconds = ttl_seconds
        self._cached: SchemaStatus | None = None
        self._expires_at = 0.0

    async def status(self) -> SchemaStatus:
        """The current schema status, re-read once the cache expires.

        A failed read never becomes a verdict. If an earlier probe succeeded
        its answer is reused; if none ever has, the state is UNKNOWN rather
        than ABSENT — reporting "no schema" for a database that is merely
        unreachable would send an operator to initialize a healthy database.
        Either way actions stay disabled.
        """
        now = time.monotonic()
        if self._cached is not None and now < self._expires_at:
            return self._cached

        observed = await self._read()
        if observed is None:
            return self._cached or SchemaStatus(
                state=SchemaState.UNKNOWN,
                version=None,
                expected_version=EXPECTED_SCHEMA_VERSION,
            )

        self._cached = observed
        self._expires_at = now + self._ttl_seconds
        return observed

    async def _read(self) -> SchemaStatus | None:
        """Read the stored version, or None when the read fails.

        A read fails on a database error, or when the broker already serves
        its limit of open event loops. Neither is a verdict about the
        schema; the warning log names the cause.
        """
        match await self._broker.select_resources():
            case Err(loop_limit):
                logger.warning(f'Schema version probe failed: {loop_limit.describe()}')
                return None
            case Ok(resources):
                pass
        try:
            async with resources.session_factory() as session:
                bookkeeping = (await session.execute(_BOOKKEEPING_TABLES_SQL)).one()
                migrations_table, schema_version_table = bookkeeping
                if not bool(migrations_table):
                    return classify_without_migrations(
                        schema_version_table=bool(schema_version_table),
                    )
                stored = int(
                    (await session.execute(_READ_SCHEMA_VERSION_SQL)).scalar_one()
                )
        except SQLAlchemyError as exc:
            logger.warning(f'Schema version probe failed: {exc}')
            return None
        return classify_stored_version(stored)


def classify_without_migrations(*, schema_version_table: bool) -> SchemaStatus:
    """The verdict for a database without ``horsies_migrations``.

    ``horsies_schema_version`` is the bookkeeping of horsies 0.x Python, so
    with it the database holds a 0.x schema (ZERO_X); without it the database
    has no horsies schema (ABSENT). The 0.x version is not read: this build
    reports it as None.
    """
    return SchemaStatus(
        state=SchemaState.ZERO_X if schema_version_table else SchemaState.ABSENT,
        version=None,
        expected_version=EXPECTED_SCHEMA_VERSION,
    )


def classify_stored_version(stored: int) -> SchemaStatus:
    """The verdict for the highest successful version in ``horsies_migrations``.

    ``stored`` is 0 when no row is recorded as successful: ABSENT. A version
    below the baseline's is horsies-rust 0.x bookkeeping, since the first
    version of this schema line is the baseline: ZERO_X. Otherwise the
    version matches or differs from the one this build expects.
    """
    match stored:
        case 0:
            return SchemaStatus(
                state=SchemaState.ABSENT,
                version=None,
                expected_version=EXPECTED_SCHEMA_VERSION,
            )
        case _ if stored < BASELINE_VERSION:
            state = SchemaState.ZERO_X
        case _ if stored == EXPECTED_SCHEMA_VERSION:
            state = SchemaState.MATCH
        case _:
            state = SchemaState.MISMATCH
    return SchemaStatus(
        state=state,
        version=stored,
        expected_version=EXPECTED_SCHEMA_VERSION,
    )
