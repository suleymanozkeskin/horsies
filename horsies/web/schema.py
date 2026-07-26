"""Schema compatibility probing for the monitoring app.

The monitoring tool never executes DDL. Creating or migrating the schema
belongs to the library and its workers; pointing this tool at a database must
never change that database's structure. So instead of initializing anything,
the app reads the stored schema version and adapts:

* the versions agree — full functionality;
* they disagree — reads are still served, because most of them keep working
  across a version step, but actions are refused server-side: writing through
  a contract this build does not share is how data gets corrupted;
* no version at all — the database has no horsies schema, and the app says so
  rather than creating one;
* the database could not be reached at all — which is deliberately NOT the
  same answer. "No schema" tells an operator to start a worker; saying that
  about a database that is merely down sends them to fix the wrong thing.

The version is read with a plain SELECT and cached briefly, so a dashboard
left open does not re-query it on every request.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable

from sqlalchemy.exc import SQLAlchemyError

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import get_logger
from horsies.core.schemas.migrations import (
    READ_SCHEMA_VERSION_SQL,
    SCHEMA_VERSION,
    SCHEMA_VERSION_TABLE_EXISTS_SQL,
)

logger = get_logger('web')

SCHEMA_TTL_SECONDS = 60.0

SCHEMA_INCOMPATIBLE = 'SCHEMA_INCOMPATIBLE'
SCHEMA_UNKNOWN = 'SCHEMA_UNKNOWN'


class SchemaState(str, Enum):
    """How the database's schema relates to the one this build expects."""

    MATCH = 'MATCH'
    MISMATCH = 'MISMATCH'
    # Only ever the result of a successful observation.
    ABSENT = 'ABSENT'
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
            case _:
                self.detail = (
                    f'Database schema is v{status.version}; this build '
                    f'expects v{status.expected_version}. Actions are '
                    f'disabled until the versions match.'
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
                expected_version=SCHEMA_VERSION,
            )

        self._cached = observed
        self._expires_at = now + self._ttl_seconds
        return observed

    async def _read(self) -> SchemaStatus | None:
        """Read the stored version, or None when the database cannot answer."""
        try:
            async with self._broker.session_factory() as session:
                present = (
                    await session.execute(SCHEMA_VERSION_TABLE_EXISTS_SQL)
                ).scalar()
                if not bool(present):
                    return SchemaStatus(
                        state=SchemaState.ABSENT,
                        version=None,
                        expected_version=SCHEMA_VERSION,
                    )
                stored = int(
                    (await session.execute(READ_SCHEMA_VERSION_SQL)).scalar_one() or 0
                )
        except SQLAlchemyError as exc:
            logger.warning(f'Schema version probe failed: {exc}')
            return None

        if stored == 0:
            return SchemaStatus(
                state=SchemaState.ABSENT,
                version=None,
                expected_version=SCHEMA_VERSION,
            )
        return SchemaStatus(
            state=(
                SchemaState.MATCH if stored == SCHEMA_VERSION else SchemaState.MISMATCH
            ),
            version=stored,
            expected_version=SCHEMA_VERSION,
        )


def schema_guard(probe: SchemaProbe) -> Callable[[], Awaitable[None]]:
    """Build the dependency that refuses actions on a foreign schema.

    Applied to every action route, so a new action cannot be added without
    inheriting the rule.
    """

    async def guard() -> None:
        status = await probe.status()
        if not status.compatible:
            raise SchemaIncompatible(status)

    return guard
