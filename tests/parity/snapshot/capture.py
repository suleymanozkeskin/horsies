"""Read-only capture of a database into a normalized snapshot.

``capture_snapshot`` runs one read-only REPEATABLE READ transaction on the
caller's connection: one statement reads the live schema, one statement
reads every catalog table, one statement reads the partition tree.
Partitioned tables are read through their parent, with the leaf of each row
(``tableoid``). The partition tree is every leaf that ``pg_inherits``
attaches to a partitioned ``horsies_`` parent, with its bound (detached
leaves come from the leaf catalog). The transaction is rolled back at the
end; the capture changes no data and no setting that outlives it.

The notification section: ``open_listener`` listens on the channels of a
run before the run writes; ``collect_notifications`` sends a marker after
the run and reads every notification up to it; ``capture_with_notifications``
adds them to the snapshot as the ``NOTIFICATIONS`` table.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, LiteralString, cast

import psycopg
from psycopg import pq, sql

from horsies.core.types.result import Err, Ok, Result
from tests.parity.snapshot.catalog import (
    BASELINE_CATALOG,
    NOTIFICATIONS,
    NOTIFIED_CATALOG,
    notification_row,
)
from tests.parity.snapshot.cells import (
    RawRow,
    RawSnapshot,
    RawTable,
    UnanchoredTime,
    UnreadableBytes,
    UnreadableJson,
    UnreadableTime,
    UnsupportedStoredFormat,
)
from tests.parity.snapshot.coverage import (
    ColumnType,
    CoverageGap,
    DatabaseType,
    DetachedLeaf,
    SchemaColumn,
    UnknownType,
    check_coverage,
    detached_leaf,
    parse_column_type,
    split_detached_leaves,
)
from tests.parity.snapshot.normalize import NormalizedSnapshot, normalize_snapshot
from tests.parity.snapshot.rules import SnapshotCatalog, TableRules
from tests.parity.snapshot.values import RunWindow, RunWindowInvalid

TABLE_ROW_LIMIT: Final = 10_000
"""Most rows the capture reads from one table; a table with more fails the capture."""

SCHEMA_COLUMN_LIMIT: Final = 2_000
"""Most ``horsies_`` columns the schema read accepts; a schema with more fails the capture."""

PARTITION_TREE_LIMIT: Final = 10_000
"""Most attached leaves the partition tree read accepts; a tree with more fails the capture."""

PARTITION_TREE_SECTION: Final = 'partition tree'
"""The name of the partition tree section in a capture error and in a diff."""

DETACHED_LEAF_LIMIT: Final = 1_000
"""Most detached leaves the capture reads; the read stops there and the leaves past it fail the capture as unclassified tables."""

CAPTURE_LOCK_TIMEOUT: Final = timedelta(seconds=5)
"""Longest wait for a table lock that an open transaction holds."""

Connection = psycopg.Connection[tuple[object, ...]]


@dataclass(frozen=True, slots=True)
class CaptureStatementFailed:
    """A capture statement raised a database error. The capture is read only: nothing changed."""

    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class CaptureBlocked:
    """A table lock was held longer than ``lock_timeout``.

    An open transaction of the test holds it. The capture may succeed after
    that transaction ends.
    """

    lock_timeout: timedelta


@dataclass(frozen=True, slots=True)
class SchemaColumnLimitExceeded:
    """The schema has more ``horsies_`` columns than ``limit``."""

    limit: int


@dataclass(frozen=True, slots=True)
class TableRowLimitExceeded:
    """A table has more rows than ``limit``."""

    table: str
    limit: int


@dataclass(frozen=True, slots=True)
class CatalogCoverageGap:
    """The live schema and the catalog differ: a column without a rule, a rule without a column, or a type misfit."""

    gaps: tuple[CoverageGap, ...]


CaptureError = (
    CaptureStatementFailed
    | CaptureBlocked
    | SchemaColumnLimitExceeded
    | CatalogCoverageGap
    | TableRowLimitExceeded
    | UnreadableTime
    | UnanchoredTime
    | UnreadableJson
    | UnsupportedStoredFormat
    | UnreadableBytes
)


@dataclass(frozen=True, slots=True)
class ClockReadFailed:
    """The clock read raised a database error."""

    cause: psycopg.Error


_TRANSACTION_SQL: Final[LiteralString] = (
    'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
)

_SESSION_SQL: Final[LiteralString] = """
    SELECT
        set_config('TimeZone', 'UTC', true),
        set_config('DateStyle', 'ISO, YMD', true),
        set_config('IntervalStyle', 'postgres', true),
        set_config('extra_float_digits', '1', true),
        set_config('bytea_output', 'hex', true),
        set_config('lock_timeout', %(lock_timeout)s, true)
"""

_SCHEMA_SQL: Final[LiteralString] = """
    SELECT c.relname::text, a.attname::text, t.typname::text
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    WHERE n.nspname = current_schema()
      AND c.relkind IN ('r', 'p')
      AND NOT c.relispartition
      AND starts_with(c.relname::text, 'horsies_')
      AND NOT a.attisdropped
      AND (a.attnum > 0 OR (c.relkind = 'p' AND a.attname = 'tableoid'))
    ORDER BY c.relname, a.attnum
    LIMIT %(limit)s
"""

_CLOCK_SQL: Final[LiteralString] = 'SELECT clock_timestamp()'

_LEAF_CATALOG: Final = 'horsies_task_history_leaf_catalog'

_DETACHED_SQL: Final[LiteralString] = """
    SELECT leaf_name, class_key::text
    FROM horsies_task_history_leaf_catalog
    WHERE detached_at IS NOT NULL AND dropped_at IS NULL
    ORDER BY leaf_name
    LIMIT %(limit)s
"""


_CATALOG_BOUND: Final = 'the catalog bounds'
"""The bound of a cataloged leaf whose partition bound is the range of its
catalog row. The catalog row's anchors compare by their own rules, so a bound
that matches them is not compared as text (its text holds dates)."""

_PARTITION_TREE_SQL: Final[LiteralString] = """
    SELECT
        parent.relname::text,
        leaf.relname::text,
        i.inhdetachpending::text,
        CASE
            WHEN c.lower_anchor IS NOT NULL
                 AND pg_get_expr(leaf.relpartbound, leaf.oid)
                     = format('FOR VALUES FROM (%%L) TO (%%L)', c.lower_anchor, c.upper_anchor)
            THEN %(catalog_bound)s
            ELSE pg_get_expr(leaf.relpartbound, leaf.oid)
        END
    FROM pg_catalog.pg_inherits AS i
    JOIN pg_catalog.pg_class AS parent ON parent.oid = i.inhparent
    JOIN pg_catalog.pg_class AS leaf ON leaf.oid = i.inhrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = parent.relnamespace
    LEFT JOIN horsies_task_history_leaf_catalog AS c ON c.leaf_name = leaf.relname::text
    WHERE n.nspname = current_schema()
      AND parent.relkind = 'p'
      AND leaf.relkind IN ('r', 'p')
      AND starts_with(parent.relname::text, 'horsies_')
    ORDER BY parent.relname, leaf.relname
    LIMIT %(limit)s
"""

_UNCATALOGED_PARTITION_TREE_SQL: Final[LiteralString] = """
    SELECT
        parent.relname::text,
        leaf.relname::text,
        i.inhdetachpending::text,
        pg_get_expr(leaf.relpartbound, leaf.oid)
    FROM pg_catalog.pg_inherits AS i
    JOIN pg_catalog.pg_class AS parent ON parent.oid = i.inhparent
    JOIN pg_catalog.pg_class AS leaf ON leaf.oid = i.inhrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = parent.relnamespace
    WHERE n.nspname = current_schema()
      AND parent.relkind = 'p'
      AND leaf.relkind IN ('r', 'p')
      AND starts_with(parent.relname::text, 'horsies_')
    ORDER BY parent.relname, leaf.relname
    LIMIT %(limit)s
"""
"""The partition tree of a schema without the leaf catalog: every bound as text."""


def _partition_tree(
    connection: Connection, schema: tuple[SchemaColumn, ...]
) -> Result[tuple[RawRow, ...], TableRowLimitExceeded]:
    """The attached leaves of every partitioned ``horsies_`` parent, in
    (parent, leaf) order. Raises ``psycopg.Error`` on a database error."""
    params = {
        'limit': PARTITION_TREE_LIMIT + 1,
        'catalog_bound': _CATALOG_BOUND,
    }
    match any(column.table == _LEAF_CATALOG for column in schema):
        case True:
            records = connection.execute(_PARTITION_TREE_SQL, params).fetchall()
        case False:
            records = connection.execute(
                _UNCATALOGED_PARTITION_TREE_SQL, {'limit': params['limit']}
            ).fetchall()
    if len(records) > PARTITION_TREE_LIMIT:
        return Err(
            TableRowLimitExceeded(table=PARTITION_TREE_SECTION, limit=PARTITION_TREE_LIMIT)
        )
    return Ok(tuple(_raw_row(list(record), 4) for record in records))


def _text(value: object, subject: str) -> str:
    match value:
        case str():
            return value
        case _:
            raise AssertionError(f'{subject} is {value!r}, not text')


def _schema_columns(records: list[tuple[object, ...]]) -> tuple[SchemaColumn, ...]:
    columns: list[SchemaColumn] = []
    for record in records:
        assert (
            len(record) == 3
        ), f'the schema read returned {record!r}, not three columns'
        table, column, type_name = record
        columns.append(
            SchemaColumn(
                table=_text(table, 'a schema table name'),
                column=_text(column, 'a schema column name'),
                column_type=parse_column_type(_text(type_name, 'a schema type name')),
            ),
        )
    return tuple(columns)


def _cell_expression(column: str, column_type: ColumnType) -> sql.Composable:
    match column_type:
        case DatabaseType.OID:
            return sql.SQL('{}::regclass::text').format(sql.Identifier(column))
        case UnknownType(name=name):
            raise AssertionError(
                f'column {column} of unknown type {name} passed the coverage check'
            )
        case _:
            return sql.SQL('{}::text').format(sql.Identifier(column))


def _table_read(
    position: int,
    rules: TableRules,
    source: str,
    types: dict[tuple[str, str], ColumnType],
    row_limit: int,
) -> sql.Composable:
    cells = sql.SQL(', ').join(
        _cell_expression(spec.name, types[(rules.name, spec.name)])
        for spec in rules.columns
    )
    return sql.SQL(
        '(SELECT {position}::integer, ARRAY[{cells}]::text[] FROM {source} LIMIT {limit})'
    ).format(
        position=sql.Literal(position),
        cells=cells,
        source=sql.Identifier(source),
        limit=sql.Literal(row_limit + 1),
    )


def _rows_statement(
    catalog: SnapshotCatalog,
    types: dict[tuple[str, str], ColumnType],
    detached: tuple[DetachedLeaf, ...],
    row_limit: int,
) -> sql.Composed:
    """One statement for every catalog table, and for each detached leaf under its parent's position."""
    positions = {table.name: position for position, table in enumerate(catalog.tables)}
    parts = [
        _table_read(position, table, table.name, types, row_limit)
        for position, table in enumerate(catalog.tables)
    ]
    for leaf in detached:
        position = positions[leaf.parent]
        parts.append(
            _table_read(
                position, catalog.tables[position], leaf.table, types, row_limit
            )
        )
    return sql.SQL(' UNION ALL ').join(parts)


def _detached_leaves(
    connection: Connection, schema: tuple[SchemaColumn, ...]
) -> tuple[DetachedLeaf, ...]:
    """The leaf catalog's detached, not dropped leaves. Raises ``psycopg.Error`` on a database error."""
    if not any(column.table == _LEAF_CATALOG for column in schema):
        return ()
    records = connection.execute(
        _DETACHED_SQL, {'limit': DETACHED_LEAF_LIMIT}
    ).fetchall()
    return tuple(
        detached_leaf(
            _text(record[0], 'a detached leaf name'),
            _text(record[1], 'a leaf class key'),
        )
        for record in records
    )


def _raw_row(cells: object, width: int) -> RawRow:
    assert isinstance(cells, list), f'the row read returned {cells!r}, not a text array'
    items = cast(list[object], cells)
    assert (
        len(items) == width
    ), f'the row read returned {len(items)} cells, the rules have {width}'
    row: list[str | None] = []
    for item in items:
        match item:
            case str() | None:
                row.append(item)
            case _:
                raise AssertionError(
                    f'the row read returned the cell {item!r}, not text or NULL'
                )
    return tuple(row)


def _raw_snapshot(
    catalog: SnapshotCatalog,
    records: list[tuple[object, ...]],
    row_limit: int,
) -> Result[RawSnapshot, TableRowLimitExceeded]:
    rows: list[list[RawRow]] = [[] for _ in catalog.tables]
    for record in records:
        match record:
            case (int() as position, cells):
                rows[position].append(
                    _raw_row(cells, len(catalog.tables[position].columns))
                )
            case _:
                raise AssertionError(
                    f'the row read returned {record!r}, not (position, cells)'
                )
    for table, table_rows in zip(catalog.tables, rows, strict=True):
        if len(table_rows) > row_limit:
            return Err(TableRowLimitExceeded(table=table.name, limit=row_limit))
    return Ok(
        RawSnapshot(
            tables=tuple(
                RawTable(name=table.name, rows=tuple(table_rows))
                for table, table_rows in zip(catalog.tables, rows, strict=True)
            ),
        ),
    )


def _read_statements(
    connection: Connection,
    catalog: SnapshotCatalog,
    row_limit: int,
    lock_timeout: timedelta,
) -> Result[RawSnapshot, CaptureError]:
    """The statements of the capture transaction. Raises ``psycopg.Error`` on a database error."""
    _ = connection.execute(_TRANSACTION_SQL)
    lock_timeout_ms = str(lock_timeout // timedelta(milliseconds=1))
    _ = connection.execute(_SESSION_SQL, {'lock_timeout': lock_timeout_ms}).fetchall()
    records = connection.execute(
        _SCHEMA_SQL, {'limit': SCHEMA_COLUMN_LIMIT + 1}
    ).fetchall()
    if len(records) > SCHEMA_COLUMN_LIMIT:
        return Err(SchemaColumnLimitExceeded(limit=SCHEMA_COLUMN_LIMIT))
    schema = _schema_columns(records)
    covered, detached, leaf_gaps = split_detached_leaves(
        schema, _detached_leaves(connection, schema)
    )
    gaps = leaf_gaps + check_coverage(covered, catalog)
    if gaps:
        return Err(CatalogCoverageGap(gaps=gaps))
    types = {(column.table, column.column): column.column_type for column in covered}
    rows = connection.execute(
        _rows_statement(catalog, types, detached, row_limit)
    ).fetchall()
    match _partition_tree(connection, schema):
        case Err(limit):
            return Err(limit)
        case Ok(tree):
            pass
    match _raw_snapshot(catalog, rows, row_limit):
        case Err(limit):
            return Err(limit)
        case Ok(raw):
            return Ok(RawSnapshot(tables=raw.tables, partition_tree=tree))


def read_raw_snapshot(
    connection: Connection,
    catalog: SnapshotCatalog,
    *,
    row_limit: int = TABLE_ROW_LIMIT,
    lock_timeout: timedelta = CAPTURE_LOCK_TIMEOUT,
) -> Result[RawSnapshot, CaptureError]:
    """Read every catalog table as text, after checking the live schema against the catalog.

    The connection must be idle (asserted). The read runs in its own
    read-only transaction, rolled back at the end.
    """
    status = connection.info.transaction_status
    assert (
        status is pq.TransactionStatus.IDLE
    ), f'the capture needs an idle connection, not {status.name}'
    try:
        with connection.transaction(force_rollback=True):
            return _read_statements(connection, catalog, row_limit, lock_timeout)
        raise AssertionError('the capture transaction block swallowed an exception')
    except psycopg.errors.LockNotAvailable:
        return Err(CaptureBlocked(lock_timeout=lock_timeout))
    except psycopg.Error as exc:
        return Err(CaptureStatementFailed(cause=exc))


def capture_snapshot(
    connection: Connection,
    catalog: SnapshotCatalog,
    window: RunWindow,
    *,
    row_limit: int = TABLE_ROW_LIMIT,
    lock_timeout: timedelta = CAPTURE_LOCK_TIMEOUT,
) -> Result[NormalizedSnapshot, CaptureError]:
    """Read and normalize the database state. See ``read_raw_snapshot`` and ``normalize_snapshot``."""
    match read_raw_snapshot(
        connection, catalog, row_limit=row_limit, lock_timeout=lock_timeout
    ):
        case Err(error):
            return Err(error)
        case Ok(raw):
            return normalize_snapshot(raw, catalog, window)


def read_database_clock(connection: Connection) -> Result[datetime, ClockReadFailed]:
    """``clock_timestamp()`` of the database, in its own transaction. The connection must be idle (asserted)."""
    status = connection.info.transaction_status
    assert (
        status is pq.TransactionStatus.IDLE
    ), f'the clock read needs an idle connection, not {status.name}'
    try:
        with connection.transaction(force_rollback=True):
            return Ok(_clock_value(connection.execute(_CLOCK_SQL).fetchone()))
        raise AssertionError('the clock read transaction block swallowed an exception')
    except psycopg.Error as exc:
        return Err(ClockReadFailed(cause=exc))


def _clock_value(record: tuple[object, ...] | None) -> datetime:
    match record:
        case (datetime() as now,):
            return now
        case _:
            raise AssertionError(
                f'the clock read returned {record!r}, not one timestamp'
            )


# ---- Notifications ----

NOTIFICATION_LIMIT: Final = 50_000
"""Most notifications that one run may send; more fail the capture."""

NOTIFICATION_WAIT: Final = timedelta(seconds=10)
"""Longest wait for the capture marker after the run."""

MARKER_CHANNEL: Final = 'horsies_dualrun_capture_marker'
"""The channel of the capture marker. No production code sends on it."""


@dataclass(frozen=True, slots=True)
class Notification:
    """One notification that a run sent."""

    channel: str
    payload: str


@dataclass(frozen=True, slots=True)
class NotificationListener:
    """An autocommit connection that listens on ``channels`` and on ``MARKER_CHANNEL``.

    The caller closes ``connection``.
    """

    connection: Connection
    channels: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ListenFailed:
    """The listener did not connect or a ``LISTEN`` failed; no listener is open."""

    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class NotificationReadFailed:
    """The marker send or the read of the notifications raised a database error."""

    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class NotificationMarkerMissing:
    """The marker did not arrive within ``wait``; the notifications read so far are not complete."""

    wait: timedelta


@dataclass(frozen=True, slots=True)
class NotificationLimitExceeded:
    """The run sent more than ``limit`` notifications before the marker."""

    limit: int


NotificationError = (
    NotificationReadFailed | NotificationMarkerMissing | NotificationLimitExceeded
)


def open_listener(
    conninfo: str, channels: tuple[str, ...]
) -> Result[NotificationListener, ListenFailed]:
    """A new autocommit connection to ``conninfo`` that listens on
    ``channels`` and on ``MARKER_CHANNEL``. Notifications of transactions
    that commit after this returns reach it."""
    try:
        connection = psycopg.connect(conninfo, autocommit=True)
    except psycopg.Error as exc:
        return Err(ListenFailed(cause=exc))
    try:
        for channel in (*channels, MARKER_CHANNEL):
            _ = connection.execute(
                sql.SQL('LISTEN {}').format(sql.Identifier(channel))
            )
    except psycopg.Error as exc:
        connection.close()
        return Err(ListenFailed(cause=exc))
    return Ok(NotificationListener(connection=connection, channels=channels))


def _send_marker(sender: Connection, token: str) -> None:
    """Commit a marker notification on ``sender``. Raises ``psycopg.Error``."""
    with sender.transaction():
        _ = sender.execute(
            'SELECT pg_notify(%(channel)s, %(token)s)',
            {'channel': MARKER_CHANNEL, 'token': token},
        ).fetchall()


def collect_notifications(
    listener: NotificationListener, sender: Connection
) -> Result[tuple[Notification, ...], NotificationError]:
    """Every notification that reached ``listener``, in arrival order, up to a
    marker that ``sender`` (an idle connection, asserted) commits now.

    PostgreSQL delivers the notifications of committed transactions in
    commit order, so every notification committed before the marker
    arrives before it. Waits at most ``NOTIFICATION_WAIT`` and reads at most
    ``NOTIFICATION_LIMIT`` notifications.
    """
    status = sender.info.transaction_status
    assert (
        status is pq.TransactionStatus.IDLE
    ), f'the marker needs an idle connection, not {status.name}'
    token = uuid.uuid4().hex
    received: list[Notification] = []
    try:
        _send_marker(sender, token)
        for notify in listener.connection.notifies(
            timeout=NOTIFICATION_WAIT.total_seconds()
        ):
            match notify.channel, notify.payload:
                case (channel, payload) if channel == MARKER_CHANNEL and payload == token:
                    return Ok(tuple(received))
                case (channel, _) if channel == MARKER_CHANNEL:
                    continue
                case (channel, payload):
                    if len(received) >= NOTIFICATION_LIMIT:
                        return Err(NotificationLimitExceeded(limit=NOTIFICATION_LIMIT))
                    received.append(Notification(channel=channel, payload=payload))
    except psycopg.Error as exc:
        return Err(NotificationReadFailed(cause=exc))
    return Err(NotificationMarkerMissing(wait=NOTIFICATION_WAIT))


def capture_with_notifications(
    connection: Connection,
    notifications: tuple[Notification, ...],
    window: RunWindow,
    *,
    row_limit: int = TABLE_ROW_LIMIT,
) -> Result[NormalizedSnapshot, CaptureError]:
    """The snapshot of the baseline tables (``read_raw_snapshot``, at most
    ``row_limit`` rows per table) and of ``notifications`` as the
    ``NOTIFICATIONS`` table, normalized by ``NOTIFIED_CATALOG``. A
    notification on a channel without a payload rule is a defect of the
    caller (asserted)."""
    match read_raw_snapshot(connection, BASELINE_CATALOG, row_limit=row_limit):
        case Err(error):
            return Err(error)
        case Ok(raw):
            pass
    sent = RawTable(
        name=NOTIFICATIONS.name,
        rows=tuple(
            notification_row(notification.channel, notification.payload)
            for notification in notifications
        ),
    )
    return normalize_snapshot(
        RawSnapshot(tables=(*raw.tables, sent), partition_tree=raw.partition_tree),
        NOTIFIED_CATALOG,
        window,
    )


# ---- Capture failures ----

CaptureFailure = ClockReadFailed | RunWindowInvalid | NotificationError | CaptureError
"""Every way the capture after a passing body can fail: the run end clock
read, the run window, the notification read, the snapshot read and its
normalization."""


def _raised(cause: psycopg.Error) -> str:
    return f'{type(cause).__name__}: {cause}'


def render_capture_failure(error: CaptureFailure) -> str:
    """The subject and the cause of a capture failure, in one line. Pure."""
    match error:
        case ClockReadFailed(cause=cause):
            return f'the run end clock read raised {_raised(cause)}'
        case RunWindowInvalid(start=start, end=end, fault=fault):
            return f'the run window from {start.isoformat()} to {end.isoformat()}: {fault.value}'
        case NotificationReadFailed(cause=cause):
            return f'the notification read raised {_raised(cause)}'
        case NotificationMarkerMissing(wait=wait):
            return f'the notification marker did not arrive within {wait}'
        case NotificationLimitExceeded(limit=limit):
            return f'the run sent more than {limit} notifications'
        case CaptureStatementFailed(cause=cause):
            return f'a capture statement raised {_raised(cause)}'
        case CaptureBlocked(lock_timeout=lock_timeout):
            return f'a table lock was held longer than {lock_timeout} (an open transaction of the test)'
        case SchemaColumnLimitExceeded(limit=limit):
            return f'the schema has more than {limit} horsies_ columns'
        case CatalogCoverageGap(gaps=gaps):
            return f'the live schema and the snapshot catalog differ: {gaps}'
        case TableRowLimitExceeded(table=table, limit=limit):
            return f'{table}: more than {limit} rows'
        case UnreadableTime(table=table, column=column, text=text, cause=cause):
            return f'{table}.{column}: the clock text {text!r} does not parse: {cause}'
        case UnanchoredTime(table=table, column=column, anchor=anchor, reason=reason):
            return f'{table}.{column}: no offset from {anchor}: {reason.value}'
        case UnreadableJson(table=table, column=column, cause=cause):
            return f'{table}.{column}: {cause}'
        case UnsupportedStoredFormat(
            table=table, column=column, version_column=version_column, found=found
        ):
            return f'{table}.{column}: {version_column} is {found}, a format the rule does not read'
        case UnreadableBytes(table=table, column=column):
            return f'{table}.{column}: the cell is not the hex text of a bytea value'
