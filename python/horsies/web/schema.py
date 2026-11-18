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

The core reads the version (``CoreHandle.schema_status_async``, on the
broker's core pool) and the probe caches the answer briefly, so a dashboard
left open does not re-query it on every request.

The monitoring app runs the same probe and the action gate in the core
(``create_monitoring_app`` mounts the core's handler); this module keeps the
public states and texts, and ``SchemaProbe`` for Python callers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import assert_never

from horsies._core import CoreError, SchemaStatusRecord
from horsies.core.baseline.migration import BASELINE_VERSION
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import get_logger

logger = get_logger('web')

SCHEMA_TTL_SECONDS = 60.0

SCHEMA_INCOMPATIBLE = 'SCHEMA_INCOMPATIBLE'
SCHEMA_UNKNOWN = 'SCHEMA_UNKNOWN'

EXPECTED_SCHEMA_VERSION = BASELINE_VERSION
"""The schema version this build writes through."""

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
        """Read the schema state in the core, or None when the read fails.

        One call of ``CoreHandle.schema_status_async`` on the broker's core
        pool. The core reads the bookkeeping tables and the stored version
        with the rules of ``classify_without_migrations`` and
        ``classify_stored_version``. Reads only.

        A read fails when the core cannot connect or read (the core answers
        ``UNKNOWN`` and logs the cause at WARNING), or when the core of this
        process cannot run the call (``CoreError``; the warning log here
        names it). Neither is a verdict about the schema.
        """
        try:
            record = await self._broker._core_handle.schema_status_async()  # pyright: ignore[reportPrivateUsage]
        except CoreError as exc:
            logger.warning(f'Schema version probe failed: {type(exc).__name__}: {exc}')
            return None
        return schema_status_of(record)


def schema_status_of(record: SchemaStatusRecord) -> SchemaStatus | None:
    """The status of the core's schema record, or None for ``UNKNOWN``. Pure.

    The core builds a new probe for each call, so its ``UNKNOWN`` means that
    this read failed: it is not a verdict, and ``SchemaProbe.status`` keeps
    its last answer. The other states are observations, with the version
    and the expected version of the core.
    """
    match record.state:
        case 'UNKNOWN':
            return None
        case 'MATCH' | 'MISMATCH' | 'ABSENT' | 'ZERO_X' as observed:
            return SchemaStatus(
                state=SchemaState(observed),
                version=record.version,
                expected_version=record.expected_version,
            )
        case _ as unreachable:
            assert_never(unreachable)


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
