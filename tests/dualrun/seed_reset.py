"""The seed state of the session database, and the reset to it before each
run of an integration dual-run.

``save_seed`` reads the state that every run starts from: the rows of each
top-level ``horsies_`` table, every partition under a partitioned
``horsies_`` parent, and the published history readers. ``reset_to_seed``
brings the database back to that state in one transaction, with SQL only,
so that the reset runs no code of the module under test:

1. Read the partition tree. A seed partition that is gone, under another
   parent, bound to other values or pending detach fails the reset with
   ``SeedPartitionsChanged`` before any write: the reset does not rebuild
   it.
2. Drop every partition that the seed does not hold, and every relation
   that the leaf catalog names and the seed does not hold (a detached leaf).
3. Truncate every seed table in one statement and insert its saved rows,
   referenced tables first.
4. Put back each reader whose definition or comment differs from the seed.

What the reset does not do:

- it does not compare or repair the indexes of the seed leaves (the
  coverage pass of each test's own seeding repairs them);
- it does not touch a ``horsies_`` relation that is neither a seed table, a
  partition nor a leaf of the leaf catalog;
- it does not end other sessions: a lock held longer than
  ``RESET_LOCK_TIMEOUT`` fails it as ``ResetBlocked``, with no change.

Both functions take an idle connection (asserted) and leave it idle. Clock
and bound texts are read with ``TimeZone`` UTC and ``DateStyle`` ISO, so the
seed and the reset render them alike.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Final, LiteralString, TypeAlias

import psycopg
from psycopg import pq, sql

from horsies.core.types.result import Err, Ok, Result

RESET_LOCK_TIMEOUT: Final = timedelta(seconds=5)
"""Longest wait of the seed read and the reset for a lock that another session holds."""

SEED_TABLE_LIMIT: Final = 200
"""Most top-level ``horsies_`` tables that the seed reads."""

SEED_ROW_LIMIT: Final = 10_000
"""Most rows that the seed saves from one table."""

PARTITION_LIMIT: Final = 10_000
"""Most partitions that the seed and the reset read."""

FOREIGN_KEY_LIMIT: Final = 1_000
"""Most foreign keys between ``horsies_`` tables that the seed reads."""

READER_LIMIT: Final = 16
"""Most published readers that the seed reads."""

LEAF_CATALOG_TABLE: Final = 'horsies_task_history_leaf_catalog'

Connection: TypeAlias = psycopg.Connection[tuple[object, ...]]


# ---- Seed state ----


@dataclass(frozen=True, slots=True)
class SeedTable:
    """One top-level table and its rows, as the JSON array that ``jsonb_agg(to_jsonb(row))`` gives."""

    name: str
    rows_json: str
    row_count: int


@dataclass(frozen=True, slots=True)
class SeedPartition:
    """One partition: its parent, its name, its bound text and whether a detach is pending."""

    parent: str
    name: str
    bound: str
    detach_pending: bool


@dataclass(frozen=True, slots=True)
class ReaderComment:
    """The comment of a published reader (the manifest stamp)."""

    text: str


@dataclass(frozen=True, slots=True)
class NoComment:
    """A published reader without a comment."""


@dataclass(frozen=True, slots=True)
class PublishedReader:
    """A reader function that exists: its full definition and its comment."""

    signature: str
    definition: str
    comment: ReaderComment | NoComment


@dataclass(frozen=True, slots=True)
class AbsentReader:
    """A reader that ``horsies_history_reader_signatures()`` names and no function implements."""

    signature: str


SeedReader = PublishedReader | AbsentReader


@dataclass(frozen=True, slots=True)
class SeedState:
    """The state that each run starts from.

    ``tables`` is in insert order: a table comes after every table that its
    foreign keys reference.
    """

    tables: tuple[SeedTable, ...]
    partitions: tuple[SeedPartition, ...]
    readers: tuple[SeedReader, ...]


# ---- Errors ----


class SeedStep(Enum):
    """The read of the seed that failed."""

    TRANSACTION = 'begin or roll back the read transaction'
    SETTINGS = 'set the transaction settings'
    TABLES = 'read the top-level tables'
    ROWS = 'read the rows of a table'
    FOREIGN_KEYS = 'read the foreign keys'
    PARTITIONS = 'read the partition tree'
    READERS = 'read the published readers'


@dataclass(frozen=True, slots=True)
class SeedReadFailed:
    """A seed read raised a database error. The seed read changes nothing."""

    step: SeedStep
    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class SeedLimitExceeded:
    """``subject`` holds more items than ``limit``; the seed is not saved."""

    subject: str
    limit: int


@dataclass(frozen=True, slots=True)
class SeedForeignKeyCycle:
    """The foreign keys between ``tables`` form a cycle, so no insert order exists."""

    tables: tuple[str, ...]


SeedError = SeedReadFailed | SeedLimitExceeded | SeedForeignKeyCycle


class ResetStep(Enum):
    """The reset statement that failed."""

    BEGIN = 'begin the reset transaction'
    SETTINGS = 'set the transaction settings'
    READ_TREE = 'read the partition tree and the leaf catalog'
    DROP = 'drop the partitions and leaves that the seed does not hold'
    TRUNCATE = 'truncate the seed tables'
    INSERT = 'insert the seed rows'
    READERS = 'put back the published readers'


@dataclass(frozen=True, slots=True)
class ResetBlocked:
    """Another session held a lock longer than ``lock_timeout``. The
    transaction rolled back: nothing changed."""

    lock_timeout: timedelta


@dataclass(frozen=True, slots=True)
class ResetStatementFailed:
    """A reset statement raised a database error. The transaction rolled
    back: nothing changed."""

    step: ResetStep
    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class ResetOutcomeUnknown:
    """The commit of the reset raised a database error: the reset may or may
    not have committed. A later reset starts from the seed either way."""

    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class PartitionAbsent:
    """No partition of this name is attached."""


@dataclass(frozen=True, slots=True)
class PartitionChange:
    """A seed partition and what the database holds under its name."""

    seed: SeedPartition
    found: SeedPartition | PartitionAbsent


@dataclass(frozen=True, slots=True)
class SeedPartitionsChanged:
    """Seed partitions that a run removed, moved, rebound or began to
    detach. The reset does not rebuild them; nothing changed."""

    changes: tuple[PartitionChange, ...]


@dataclass(frozen=True, slots=True)
class ResetLimitExceeded:
    """``subject`` holds more items than ``limit``. Nothing changed."""

    subject: str
    limit: int


ResetError = (
    ResetBlocked
    | ResetStatementFailed
    | ResetOutcomeUnknown
    | SeedPartitionsChanged
    | ResetLimitExceeded
)


# ---- Statements ----

_SETTINGS_SQL: Final[LiteralString] = """
    SELECT
        set_config('lock_timeout', %(lock_timeout)s, true),
        set_config('TimeZone', 'UTC', true),
        set_config('DateStyle', 'ISO, YMD', true)
"""

_TABLES_SQL: Final[LiteralString] = """
    SELECT c.relname::text
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
      AND c.relkind IN ('r', 'p')
      AND NOT c.relispartition
      AND starts_with(c.relname::text, 'horsies_')
    ORDER BY c.relname
    LIMIT %(limit)s
"""

_FOREIGN_KEYS_SQL: Final[LiteralString] = """
    SELECT child.relname::text, parent.relname::text
    FROM pg_catalog.pg_constraint AS con
    JOIN pg_catalog.pg_class AS child ON child.oid = con.conrelid
    JOIN pg_catalog.pg_class AS parent ON parent.oid = con.confrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = con.connamespace
    WHERE con.contype = 'f'
      AND n.nspname = current_schema()
      AND starts_with(child.relname::text, 'horsies_')
    ORDER BY 1, 2
    LIMIT %(limit)s
"""

_PARTITIONS_SQL: Final[LiteralString] = """
    SELECT
        parent.relname::text,
        leaf.relname::text,
        pg_get_expr(leaf.relpartbound, leaf.oid),
        i.inhdetachpending
    FROM pg_catalog.pg_inherits AS i
    JOIN pg_catalog.pg_class AS parent ON parent.oid = i.inhparent
    JOIN pg_catalog.pg_class AS leaf ON leaf.oid = i.inhrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = parent.relnamespace
    WHERE n.nspname = current_schema()
      AND parent.relkind = 'p'
      AND starts_with(parent.relname::text, 'horsies_')
    ORDER BY parent.relname, leaf.relname
    LIMIT %(limit)s
"""

_CATALOGED_LEAVES_SQL: Final[LiteralString] = """
    SELECT c.leaf_name
    FROM horsies_task_history_leaf_catalog AS c
    WHERE to_regclass(quote_ident(c.leaf_name)) IS NOT NULL
    ORDER BY c.leaf_name
    LIMIT %(limit)s
"""

_READERS_SQL: Final[LiteralString] = """
    SELECT
        s.signature,
        pg_get_functiondef(to_regprocedure(s.signature)),
        obj_description(to_regprocedure(s.signature), 'pg_proc')
    FROM horsies_history_reader_signatures() AS s
    ORDER BY s.signature
    LIMIT %(limit)s
"""

_READER_SQL: Final[LiteralString] = """
    SELECT
        pg_get_functiondef(to_regprocedure(%(signature)s)),
        obj_description(to_regprocedure(%(signature)s), 'pg_proc')
"""

_COMMENT_STATEMENT_SQL: Final[LiteralString] = """
    SELECT format('COMMENT ON FUNCTION %%s IS %%L', to_regprocedure(%(signature)s), %(comment)s::text)
"""

_DROP_FUNCTION_STATEMENT_SQL: Final[LiteralString] = """
    SELECT format('DROP FUNCTION %%s', to_regprocedure(%(signature)s))
"""


# ---- Shared readers ----


def _text(value: object, subject: str) -> str:
    match value:
        case str():
            return value
        case _:
            raise AssertionError(f'{subject} is {value!r}, not text')


def _settings(connection: Connection) -> None:
    """The settings of the current transaction. Raises ``psycopg.Error``."""
    lock_timeout_ms = str(RESET_LOCK_TIMEOUT // timedelta(milliseconds=1))
    _ = connection.execute(_SETTINGS_SQL, {'lock_timeout': lock_timeout_ms}).fetchall()


def _partitions(connection: Connection) -> tuple[SeedPartition, ...]:
    """Every partition under a partitioned ``horsies_`` parent, at most
    ``PARTITION_LIMIT + 1``: more than ``PARTITION_LIMIT`` means the limit is
    exceeded. Raises ``psycopg.Error``."""
    records = connection.execute(
        _PARTITIONS_SQL, {'limit': PARTITION_LIMIT + 1}
    ).fetchall()
    partitions: list[SeedPartition] = []
    for record in records:
        match record:
            case (str() as parent, str() as name, str() as bound, bool() as pending):
                partitions.append(
                    SeedPartition(
                        parent=parent, name=name, bound=bound, detach_pending=pending
                    )
                )
            case _:
                raise AssertionError(
                    f'the partition read returned {record!r}, not (parent, name, bound, pending)'
                )
    return tuple(partitions)


def _comment(value: object) -> ReaderComment | NoComment:
    match value:
        case None:
            return NoComment()
        case str():
            return ReaderComment(text=value)
        case _:
            raise AssertionError(f'a reader comment is {value!r}, not text or NULL')


def _reader(signature: str, definition: object, comment: object) -> SeedReader:
    match definition:
        case None:
            return AbsentReader(signature=signature)
        case str():
            return PublishedReader(
                signature=signature, definition=definition, comment=_comment(comment)
            )
        case _:
            raise AssertionError(
                f'the definition of {signature} is {definition!r}, not text or NULL'
            )


# ---- Seed ----


def _table_names(connection: Connection) -> Result[tuple[str, ...], SeedError]:
    records = connection.execute(
        _TABLES_SQL, {'limit': SEED_TABLE_LIMIT + 1}
    ).fetchall()
    if len(records) > SEED_TABLE_LIMIT:
        return Err(
            SeedLimitExceeded(subject='top-level tables', limit=SEED_TABLE_LIMIT)
        )
    return Ok(tuple(_text(record[0], 'a table name') for record in records))


def _table_rows(connection: Connection, name: str) -> Result[SeedTable, SeedError]:
    statement = sql.SQL(
        'SELECT count(*)::integer, COALESCE(jsonb_agg(to_jsonb(t)), {empty})::text '
        'FROM (SELECT * FROM {table} LIMIT {limit}) AS t'
    ).format(
        empty=sql.Literal('[]'),
        table=sql.Identifier(name),
        limit=sql.Literal(SEED_ROW_LIMIT + 1),
    )
    match connection.execute(statement).fetchone():
        case (int() as count, str() as rows_json):
            pass
        case other:
            raise AssertionError(f'the row read of {name} returned {other!r}')
    if count > SEED_ROW_LIMIT:
        return Err(SeedLimitExceeded(subject=f'rows of {name}', limit=SEED_ROW_LIMIT))
    return Ok(SeedTable(name=name, rows_json=rows_json, row_count=count))


def _foreign_keys(
    connection: Connection, tables: tuple[str, ...]
) -> Result[tuple[tuple[str, str], ...], SeedError]:
    """(child, parent) for each foreign key between two seed tables, other than to itself."""
    records = connection.execute(
        _FOREIGN_KEYS_SQL, {'limit': FOREIGN_KEY_LIMIT + 1}
    ).fetchall()
    if len(records) > FOREIGN_KEY_LIMIT:
        return Err(SeedLimitExceeded(subject='foreign keys', limit=FOREIGN_KEY_LIMIT))
    known = set(tables)
    edges: list[tuple[str, str]] = []
    for record in records:
        child = _text(record[0], 'a referencing table')
        parent = _text(record[1], 'a referenced table')
        if child in known and parent in known and child != parent:
            edges.append((child, parent))
    return Ok(tuple(edges))


def insert_order(
    tables: tuple[str, ...], edges: tuple[tuple[str, str], ...]
) -> Result[tuple[str, ...], SeedForeignKeyCycle]:
    """``tables`` with each referenced table before the tables that
    reference it; ties in name order. ``edges`` are (child, parent) pairs of
    ``tables``. Pure; at most ``len(tables)`` rounds."""
    remaining = set(tables)
    order: list[str] = []
    for _ in range(len(tables)):
        if not remaining:
            break
        ready = sorted(
            table
            for table in remaining
            if not any(
                child == table and parent in remaining for child, parent in edges
            )
        )
        if not ready:
            return Err(SeedForeignKeyCycle(tables=tuple(sorted(remaining))))
        order.extend(ready)
        remaining.difference_update(ready)
    assert not remaining, f'each round orders at least one table; left: {remaining}'
    return Ok(tuple(order))


def _seed_readers(connection: Connection) -> Result[tuple[SeedReader, ...], SeedError]:
    records = connection.execute(_READERS_SQL, {'limit': READER_LIMIT + 1}).fetchall()
    if len(records) > READER_LIMIT:
        return Err(SeedLimitExceeded(subject='published readers', limit=READER_LIMIT))
    return Ok(
        tuple(
            _reader(_text(record[0], 'a reader signature'), record[1], record[2])
            for record in records
        )
    )


def _seed_tables(connection: Connection) -> Result[tuple[SeedTable, ...], SeedError]:
    try:
        named = _table_names(connection)
    except psycopg.Error as exc:
        return Err(SeedReadFailed(step=SeedStep.TABLES, cause=exc))
    match named:
        case Err(error):
            return Err(error)
        case Ok(names):
            pass
    try:
        keyed = _foreign_keys(connection, names)
    except psycopg.Error as exc:
        return Err(SeedReadFailed(step=SeedStep.FOREIGN_KEYS, cause=exc))
    match keyed:
        case Err(error):
            return Err(error)
        case Ok(edges):
            pass
    match insert_order(names, edges):
        case Err(cycle):
            return Err(cycle)
        case Ok(ordered):
            pass
    tables: list[SeedTable] = []
    for name in ordered:
        try:
            read = _table_rows(connection, name)
        except psycopg.Error as exc:
            return Err(SeedReadFailed(step=SeedStep.ROWS, cause=exc))
        match read:
            case Err(error):
                return Err(error)
            case Ok(table):
                tables.append(table)
    return Ok(tuple(tables))


def _read_seed(connection: Connection) -> Result[SeedState, SeedError]:
    """The reads of ``save_seed``, inside its transaction."""
    try:
        _ = connection.execute(
            'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
        )
        _settings(connection)
    except psycopg.Error as exc:
        return Err(SeedReadFailed(step=SeedStep.SETTINGS, cause=exc))
    match _seed_tables(connection):
        case Err(error):
            return Err(error)
        case Ok(tables):
            pass
    try:
        partitions = _partitions(connection)
    except psycopg.Error as exc:
        return Err(SeedReadFailed(step=SeedStep.PARTITIONS, cause=exc))
    if len(partitions) > PARTITION_LIMIT:
        return Err(SeedLimitExceeded(subject='partitions', limit=PARTITION_LIMIT))
    try:
        read = _seed_readers(connection)
    except psycopg.Error as exc:
        return Err(SeedReadFailed(step=SeedStep.READERS, cause=exc))
    match read:
        case Err(error):
            return Err(error)
        case Ok(readers):
            return Ok(SeedState(tables=tables, partitions=partitions, readers=readers))


def save_seed(connection: Connection) -> Result[SeedState, SeedError]:
    """Read the seed state in one REPEATABLE READ, read-only transaction,
    rolled back at the end. Changes nothing. The connection must be idle
    (asserted). The database must hold the baseline: the reader list comes
    from ``horsies_history_reader_signatures()``."""
    status = connection.info.transaction_status
    assert (
        status is pq.TransactionStatus.IDLE
    ), f'the seed read needs an idle connection, not {status.name}'
    try:
        with connection.transaction(force_rollback=True):
            return _read_seed(connection)
        raise AssertionError('the seed read transaction block swallowed an exception')
    except psycopg.Error as exc:
        return Err(SeedReadFailed(step=SeedStep.TRANSACTION, cause=exc))


# ---- Reset ----


def partition_changes(
    seed: tuple[SeedPartition, ...], found: tuple[SeedPartition, ...]
) -> tuple[PartitionChange, ...]:
    """Each seed partition that ``found`` does not hold as it is. Pure."""
    by_name = {partition.name: partition for partition in found}
    changes: list[PartitionChange] = []
    for partition in seed:
        match by_name.get(partition.name):
            case None:
                changes.append(PartitionChange(seed=partition, found=PartitionAbsent()))
            case current if current != partition:
                changes.append(PartitionChange(seed=partition, found=current))
            case _:
                pass
    return tuple(changes)


def relations_to_drop(
    seed: SeedState, found: tuple[SeedPartition, ...], cataloged: tuple[str, ...]
) -> tuple[str, ...]:
    """The partitions in ``found`` and the cataloged leaves that the seed
    holds neither as a partition nor as a table, in name order. Pure."""
    kept = {partition.name for partition in seed.partitions} | {
        table.name for table in seed.tables
    }
    extra = {partition.name for partition in found} | set(cataloged)
    return tuple(sorted(extra - kept))


def _cataloged_leaves(
    connection: Connection, seed: SeedState
) -> Result[tuple[str, ...], ResetLimitExceeded]:
    """The leaves that the leaf catalog names and that exist, when the seed
    holds the leaf catalog. Raises ``psycopg.Error``."""
    if not any(table.name == LEAF_CATALOG_TABLE for table in seed.tables):
        return Ok(())
    records = connection.execute(
        _CATALOGED_LEAVES_SQL, {'limit': PARTITION_LIMIT + 1}
    ).fetchall()
    if len(records) > PARTITION_LIMIT:
        return Err(
            ResetLimitExceeded(subject='cataloged leaves', limit=PARTITION_LIMIT)
        )
    return Ok(tuple(_text(record[0], 'a leaf name') for record in records))


def _read_tree(
    connection: Connection, seed: SeedState
) -> Result[tuple[str, ...], ResetError]:
    """The relations to drop, or the seed partitions that changed. Raises ``psycopg.Error``."""
    found = _partitions(connection)
    if len(found) > PARTITION_LIMIT:
        return Err(ResetLimitExceeded(subject='partitions', limit=PARTITION_LIMIT))
    changes = partition_changes(seed.partitions, found)
    if changes:
        return Err(SeedPartitionsChanged(changes=changes))
    match _cataloged_leaves(connection, seed):
        case Err(error):
            return Err(error)
        case Ok(cataloged):
            return Ok(relations_to_drop(seed, found, cataloged))


def _drop(connection: Connection, names: tuple[str, ...]) -> None:
    """Drop each relation. A partition dropped with its parent is gone already. Raises ``psycopg.Error``."""
    for name in names:
        _ = connection.execute(
            sql.SQL('DROP TABLE IF EXISTS {}').format(sql.Identifier(name))
        )


def _truncate(connection: Connection, seed: SeedState) -> None:
    """Truncate every seed table in one statement. Raises ``psycopg.Error``."""
    _ = connection.execute(
        sql.SQL('TRUNCATE {}').format(
            sql.SQL(', ').join(sql.Identifier(table.name) for table in seed.tables)
        )
    )


def _insert(connection: Connection, seed: SeedState) -> None:
    """Insert the saved rows, referenced tables first. Raises ``psycopg.Error``."""
    for table in seed.tables:
        if table.row_count == 0:
            continue
        _ = connection.execute(
            sql.SQL(
                'INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, %(rows)s::jsonb)'
            ).format(table=sql.Identifier(table.name)),
            {'rows': table.rows_json},
        )


def _built_statement(
    connection: Connection, statement: LiteralString, params: dict[str, object]
) -> bytes:
    """A statement that the database formats (``format(... %s ...)``), as bytes to run. Raises ``psycopg.Error``."""
    match connection.execute(statement, params).fetchone():
        case (str() as built,):
            return built.encode('utf-8')
        case other:
            raise AssertionError(f'the statement build returned {other!r}')


def _put_back_reader(connection: Connection, reader: SeedReader) -> None:
    """Make one reader as the seed holds it, when it differs. Raises ``psycopg.Error``."""
    match connection.execute(_READER_SQL, {'signature': reader.signature}).fetchone():
        case (definition, comment):
            current = _reader(reader.signature, definition, comment)
        case other:
            raise AssertionError(f'the reader read returned {other!r}')
    if current == reader:
        return
    match reader:
        case PublishedReader(definition=definition, comment=comment):
            _ = connection.execute(definition.encode('utf-8'))
            match comment:
                case ReaderComment(text=text):
                    stamp: str | None = text
                case NoComment():
                    stamp = None
            _ = connection.execute(
                _built_statement(
                    connection,
                    _COMMENT_STATEMENT_SQL,
                    {'signature': reader.signature, 'comment': stamp},
                )
            )
        case AbsentReader():
            _ = connection.execute(
                _built_statement(
                    connection,
                    _DROP_FUNCTION_STATEMENT_SQL,
                    {'signature': reader.signature},
                )
            )


def _reset_steps(connection: Connection, seed: SeedState) -> Result[None, ResetError]:
    """The statements of the reset. Each database error names its step.
    Raises ``psycopg.errors.LockNotAvailable`` for the caller."""
    step = ResetStep.SETTINGS
    try:
        _settings(connection)
        step = ResetStep.READ_TREE
        match _read_tree(connection, seed):
            case Err(error):
                return Err(error)
            case Ok(extra):
                pass
        step = ResetStep.DROP
        _drop(connection, extra)
        step = ResetStep.TRUNCATE
        _truncate(connection, seed)
        step = ResetStep.INSERT
        _insert(connection, seed)
        step = ResetStep.READERS
        for reader in seed.readers:
            _put_back_reader(connection, reader)
    except psycopg.errors.LockNotAvailable:
        raise
    except psycopg.Error as exc:
        return Err(ResetStatementFailed(step=step, cause=exc))
    return Ok(None)


class _RollBack(Exception):
    """Leaves the reset transaction block so that it rolls back."""

    def __init__(self, error: ResetError) -> None:
        super().__init__(error)
        self.error = error


def reset_to_seed(connection: Connection, seed: SeedState) -> Result[None, ResetError]:
    """Bring the database to ``seed`` in one transaction (see the module).

    On ``Ok`` the transaction committed. On any ``Err`` it rolled back and
    nothing changed. The connection must be idle (asserted).
    """
    status = connection.info.transaction_status
    assert (
        status is pq.TransactionStatus.IDLE
    ), f'the reset needs an idle connection, not {status.name}'
    steps_done = False
    try:
        with connection.transaction():
            match _reset_steps(connection, seed):
                case Err(error):
                    raise _RollBack(error)
                case Ok():
                    steps_done = True
    except _RollBack as rolled_back:
        return Err(rolled_back.error)
    except psycopg.errors.LockNotAvailable:
        return Err(ResetBlocked(lock_timeout=RESET_LOCK_TIMEOUT))
    except psycopg.Error as exc:
        match steps_done:
            case True:
                return Err(ResetOutcomeUnknown(cause=exc))
            case False:
                return Err(ResetStatementFailed(step=ResetStep.BEGIN, cause=exc))
    return Ok(None)
