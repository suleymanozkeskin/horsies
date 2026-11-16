"""Install the baseline schema into an empty database, and classify a database.

The install path is:

1. ``read_database_state``: two catalog reads, no lock, no change.
2. ``decide_install``: a pure decision over the state.
3. Under the caller's schema lock, read the state again and act:
   ``install_baseline`` (empty database) or ``record_client_family`` (a
   baseline database with no family row).

These functions take a psycopg connection in autocommit mode. They do not
take or release the schema lock; the caller owns it. A database that holds
anything other than an empty schema or this build's baseline is refused and
never changed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, LiteralString

import psycopg

from horsies.core.baseline.migration import BASELINE_VERSION, BaselineMigration
from horsies.core.types.result import Err, Ok, Result

CATALOG_NAME_LIMIT: Final = 64
"""Most ``horsies_`` table names, and most bookkeeping rows, that one state read returns."""


class ClientFamily(str, Enum):
    """The client library that installed a database (``horsies_client_family``)."""

    PYTHON = 'PYTHON'
    RUST = 'RUST'


OWN_CLIENT_FAMILY: Final = ClientFamily.PYTHON


@dataclass(frozen=True, slots=True)
class FamilyRecorded:
    """The family row exists."""

    family: ClientFamily


@dataclass(frozen=True, slots=True)
class FamilyNotRecorded:
    """The family table has no row: the install stopped before it wrote one."""


RecordedFamily = FamilyRecorded | FamilyNotRecorded


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    """One row of ``horsies_migrations``."""

    version: int
    success: bool
    checksum: bytes


# ---- Database states ----


@dataclass(frozen=True, slots=True)
class EmptyDatabase:
    """No ``horsies_`` table in the current schema."""


@dataclass(frozen=True, slots=True)
class ZeroXPythonDatabase:
    """A horsies 0.x Python schema: ``horsies_schema_version`` without ``horsies_migrations``."""

    tables: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UnknownHorsiesObjects:
    """``horsies_`` tables with neither bookkeeping table."""

    tables: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SchemaNewer:
    """The bookkeeping holds a version above this build's."""

    stored: int
    own: int


@dataclass(frozen=True, slots=True)
class ForeignMigrations:
    """The bookkeeping has no baseline row, or holds versions this build does not ship.

    A horsies-rust 0.x database is one: its versions end at 65.
    """

    versions: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MigrationIncomplete:
    """The baseline row is recorded as failed."""

    version: int


@dataclass(frozen=True, slots=True)
class MigrationEdited:
    """The baseline row's checksum differs from this build's baseline text."""

    version: int
    stored_checksum: bytes
    own_checksum: bytes


@dataclass(frozen=True, slots=True)
class BaselineCurrent:
    """The baseline is installed, recorded as successful, with this build's checksum."""

    family: RecordedFamily


DatabaseState = (
    EmptyDatabase
    | ZeroXPythonDatabase
    | UnknownHorsiesObjects
    | SchemaNewer
    | ForeignMigrations
    | MigrationIncomplete
    | MigrationEdited
    | BaselineCurrent
)


# ---- Decisions ----


@dataclass(frozen=True, slots=True)
class SchemaReady:
    """Nothing to do: the database holds this build's baseline and family."""


@dataclass(frozen=True, slots=True)
class InstallBaseline:
    """Run the baseline migration with its bookkeeping and family rows."""


@dataclass(frozen=True, slots=True)
class RecordFamily:
    """Write this client's family row."""


@dataclass(frozen=True, slots=True)
class RefuseInstall:
    """Change nothing and refuse, naming what the database holds."""

    state: DatabaseState
    cause: str


InstallDecision = SchemaReady | InstallBaseline | RecordFamily | RefuseInstall


@dataclass(frozen=True, slots=True)
class SchemaStatementFailed:
    """A statement of the state read or the install raised a database error.

    After a failure inside ``install_baseline`` the transaction is rolled
    back: the database is unchanged. ``cause.sqlstate`` tells a deadlock
    (40P01, the caller may retry) from other errors.
    """

    step: str
    cause: psycopg.Error


# ---- Statements ----

_CATALOG_SQL: Final[LiteralString] = """
    SELECT
        to_regclass('horsies_migrations') IS NOT NULL,
        to_regclass('horsies_schema_version') IS NOT NULL,
        to_regclass('horsies_client_family') IS NOT NULL,
        ARRAY(
            SELECT c.relname::text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relkind IN ('r', 'p')
              AND c.relname LIKE 'horsies\\_%%'
            ORDER BY 1
            LIMIT %(limit)s
        )
"""

_MIGRATIONS_WITH_FAMILY_SQL: Final[LiteralString] = """
    SELECT m.version, m.success, m.checksum, (SELECT f.family FROM horsies_client_family f)
    FROM horsies_migrations m
    ORDER BY m.version DESC
    LIMIT %(limit)s
"""

_MIGRATIONS_SQL: Final[LiteralString] = """
    SELECT m.version, m.success, m.checksum, NULL::text
    FROM horsies_migrations m
    ORDER BY m.version DESC
    LIMIT %(limit)s
"""

_INSERT_MIGRATION_SQL: Final[LiteralString] = """
    INSERT INTO horsies_migrations
        (version, description, installed_on, success, checksum, execution_time)
    VALUES (%(version)s, %(description)s, now(), TRUE, %(checksum)s, %(execution_time)s)
"""

_INSERT_FAMILY_SQL: Final[LiteralString] = """
    INSERT INTO horsies_client_family (only_row, family, recorded_at)
    VALUES (TRUE, %(family)s, now())
    ON CONFLICT (only_row) DO NOTHING
"""


# ---- State read ----


def _parse_family(value: object) -> RecordedFamily:
    match value:
        case None:
            return FamilyNotRecorded()
        case str() as text:
            # horsies_client_family_family_known holds the column to the enum.
            return FamilyRecorded(ClientFamily(text))
        case _:
            raise AssertionError(f'horsies_client_family.family is not text: {value!r}')


def classify_database(
    *,
    migrations_table: bool,
    schema_version_table: bool,
    tables: tuple[str, ...],
    applied: tuple[AppliedMigration, ...],
    family: RecordedFamily,
    own_checksum: bytes,
) -> DatabaseState:
    """Classify the catalog facts of one database. Pure."""
    if not migrations_table:
        if not tables:
            return EmptyDatabase()
        if schema_version_table:
            return ZeroXPythonDatabase(tables=tables)
        return UnknownHorsiesObjects(tables=tables)
    versions = tuple(row.version for row in applied)
    newest = max(versions, default=None)
    if newest is not None and newest > BASELINE_VERSION:
        return SchemaNewer(stored=newest, own=BASELINE_VERSION)
    if versions != (BASELINE_VERSION,):
        return ForeignMigrations(versions=versions)
    baseline = applied[0]
    if not baseline.success:
        return MigrationIncomplete(version=baseline.version)
    if baseline.checksum != own_checksum:
        return MigrationEdited(
            version=baseline.version,
            stored_checksum=baseline.checksum,
            own_checksum=own_checksum,
        )
    return BaselineCurrent(family=family)


async def read_database_state(
    connection: psycopg.AsyncConnection[Any],
    migration: BaselineMigration,
) -> Result[DatabaseState, SchemaStatementFailed]:
    """Read and classify the horsies objects of the current schema.

    One statement on an empty database, two otherwise. Changes nothing.
    """
    try:
        cursor = await connection.execute(_CATALOG_SQL, {'limit': CATALOG_NAME_LIMIT})
        catalog = await cursor.fetchone()
        assert catalog is not None, 'the catalog read returns one row'
        migrations_table, schema_version_table, family_table, names = catalog
        rows: list[tuple[Any, ...]] = []
        if migrations_table:
            query = _MIGRATIONS_WITH_FAMILY_SQL if family_table else _MIGRATIONS_SQL
            cursor = await connection.execute(query, {'limit': CATALOG_NAME_LIMIT})
            rows = await cursor.fetchall()
    except psycopg.Error as exc:
        return Err(SchemaStatementFailed(step='read the schema state', cause=exc))
    applied = tuple(
        AppliedMigration(version=int(row[0]), success=bool(row[1]), checksum=bytes(row[2]))
        for row in rows
    )
    family = _parse_family(rows[0][3]) if rows else FamilyNotRecorded()
    return Ok(
        classify_database(
            migrations_table=bool(migrations_table),
            schema_version_table=bool(schema_version_table),
            tables=tuple(str(name) for name in names),
            applied=applied,
            family=family,
            own_checksum=migration.checksum,
        ),
    )


# ---- Decision ----


def describe_state(state: DatabaseState) -> str:
    """What the database holds, in words, for a refusal."""
    match state:
        case EmptyDatabase():
            return 'the database holds no horsies table'
        case ZeroXPythonDatabase(tables=tables):
            return (
                'the database holds a horsies 0.x schema (horsies_schema_version without '
                f'horsies_migrations; tables: {", ".join(tables)}); this build does not '
                'upgrade a 0.x database: drain it and point the fleet at a new database'
            )
        case UnknownHorsiesObjects(tables=tables):
            return (
                'the database holds horsies tables with no schema bookkeeping '
                f'(tables: {", ".join(tables)})'
            )
        case SchemaNewer(stored=stored, own=own):
            return f'database schema version {stored} is newer than this build supports ({own})'
        case ForeignMigrations(versions=versions):
            listed = ', '.join(str(version) for version in versions) or 'none'
            return (
                'the schema bookkeeping holds migration versions this build does not ship '
                f'(found: {listed}; this build ships {BASELINE_VERSION})'
            )
        case MigrationIncomplete(version=version):
            return f'schema migration {version} is recorded as failed'
        case MigrationEdited(version=version, stored_checksum=stored, own_checksum=own):
            return (
                f'schema migration {version} was applied from another text '
                f'(stored checksum {stored.hex()}, this build {own.hex()})'
            )
        case BaselineCurrent(family=recorded):
            match recorded:
                case FamilyRecorded(family=family):
                    return f'the database was installed by the {family.value} client'
                case FamilyNotRecorded():
                    return 'the baseline schema is installed with no client family row'


def _decide_for_family(state: BaselineCurrent, recorded: RecordedFamily) -> InstallDecision:
    match recorded:
        case FamilyNotRecorded():
            return RecordFamily()
        case FamilyRecorded(family=family):
            match family:
                case ClientFamily.PYTHON:
                    return SchemaReady()
                case ClientFamily.RUST:
                    return RefuseInstall(
                        state=state,
                        cause=(
                            f'the database was installed by the {family.value} client; '
                            f'this is the {OWN_CLIENT_FAMILY.value} client'
                        ),
                    )


def decide_install(state: DatabaseState) -> InstallDecision:
    """What the install does for one state. Pure."""
    match state:
        case EmptyDatabase():
            return InstallBaseline()
        case BaselineCurrent(family=recorded):
            return _decide_for_family(state, recorded)
        case (
            ZeroXPythonDatabase()
            | UnknownHorsiesObjects()
            | SchemaNewer()
            | ForeignMigrations()
            | MigrationIncomplete()
            | MigrationEdited()
        ):
            return RefuseInstall(state=state, cause=describe_state(state))


# ---- Writes (the caller holds the schema lock) ----


async def install_baseline(
    connection: psycopg.AsyncConnection[Any],
    migration: BaselineMigration,
) -> Result[None, SchemaStatementFailed]:
    """Run the baseline migration, its bookkeeping row and this client's family row.

    One transaction: on failure nothing stays. The connection must be in
    autocommit mode. Does not take the schema lock.
    """
    try:
        async with connection.transaction():
            started = time.perf_counter_ns()
            # No parameters: psycopg sends the text with the simple query
            # protocol, which runs every statement of it and reads no
            # placeholders in it.
            await connection.execute(migration.sql.encode('utf-8'))
            elapsed = time.perf_counter_ns() - started
            await connection.execute(
                _INSERT_MIGRATION_SQL,
                {
                    'version': migration.version,
                    'description': migration.description,
                    'checksum': migration.checksum,
                    'execution_time': elapsed,
                },
            )
            await connection.execute(_INSERT_FAMILY_SQL, {'family': OWN_CLIENT_FAMILY.value})
    except psycopg.Error as exc:
        return Err(SchemaStatementFailed(step='install the baseline schema', cause=exc))
    return Ok(None)


async def record_client_family(
    connection: psycopg.AsyncConnection[Any],
) -> Result[None, SchemaStatementFailed]:
    """Write this client's family row when the table has none. Does not take the schema lock."""
    try:
        await connection.execute(_INSERT_FAMILY_SQL, {'family': OWN_CLIENT_FAMILY.value})
    except psycopg.Error as exc:
        return Err(SchemaStatementFailed(step='record the client family', cause=exc))
    return Ok(None)
