"""The leaf catalog: the partition manager's durable memory.

PostgreSQL's own catalogs say what is attached right now; they cannot say
what the manager created, detached, or dropped, with which bounds, under
which class, or carrying which index version. The leaf catalog records that,
and every maintenance decision cross-checks catalog against `pg_catalog`
before acting — a decision made from either source alone can be stale.

The catalog stores no value that it can derive: the parent relation, the
task index name and the rendered partition bound are derived from the class
key, the leaf name and the bounds when a row is read.

Reads decode fail-closed: a column of an unexpected type means this code and
the installed schema disagree, and that raises `HistoryContractError` rather
than flowing onward as data.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import (
    AnchoredLowerBound,
    LeafBounds,
    LowerBound,
    OpenLowerBound,
    is_safe_identifier,
)
from ..ddl.tables import FOREVER_CLASS_KEY
from ..errors import HistoryContractError
from ..names import (
    HEARTBEAT_CLASS_KEY,
    HEARTBEATS_TABLE,
    LEAF_CATALOG,
    RETENTION_CLASSES,
    TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
)

INDEX_SCHEMA_VERSION = 1
"""Current per-leaf index layout version recorded on creation."""


@dataclass(frozen=True, slots=True)
class RetentionClassRow:
    """One retention class, with the facts its kind implies.

    The table stores ``class_kind`` with ``retention_days`` (``PERIOD``) or
    ``heartbeat_horizon_hours`` (``HEARTBEAT``). The reader derives the rest:
    ``FOREVER`` has no duration, interval or finite parent; ``PERIOD`` has
    its days, a daily interval and the parent ``horsies_task_history_<key>``;
    ``HEARTBEAT`` has its horizon, an hourly interval and the heartbeat
    table as parent.
    """

    class_key: str
    duration: timedelta | None
    partition_interval: timedelta | None
    finite_parent_name: str | None


@dataclass(frozen=True, slots=True)
class LeafCatalogRow:
    """One leaf as the catalog remembers it.

    ``parent_name``, ``id_index_name`` and ``partition_bound`` are derived
    by the reader (``leaf_parent_name``, ``leaf_task_index_name``,
    ``render_partition_bound``); the table does not store them.
    """

    leaf_name: str
    parent_name: str
    class_key: str
    lower_bound: LowerBound
    upper_anchor: datetime
    index_schema_version: int
    id_index_name: str
    partition_bound: str
    created_at: datetime
    detached_at: datetime | None
    dropped_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeafPhysicalState:
    """What `pg_catalog` says about one leaf right now.

    `detach_pending` is three-valued: None means the leaf is not an
    inheritance child of the parent (detached or never attached); False
    means attached; True means a concurrent detach is awaiting FINALIZE.
    """

    relation_exists: bool
    parent_exists: bool
    partition_bound: str | None
    partition_bound_matches_expected: bool
    id_index_conformant: bool
    detach_pending: bool | None


class LeafIndexKind(Enum):
    """The exact canonical task lookup index shape for one leaf."""

    HISTORY = 'HISTORY'
    HEARTBEAT = 'HEARTBEAT'


@dataclass(frozen=True, slots=True)
class RequestedPartitionBounds:
    """Require the physical bound to match this leaf request."""

    bounds: LeafBounds


@dataclass(frozen=True, slots=True)
class CatalogPartitionBound:
    """Use only the stored physical bound for a legacy leaf."""


type LeafPartitionBoundExpectation = RequestedPartitionBounds | CatalogPartitionBound


def daily_leaf_name(parent_name: str, lower: datetime) -> str:
    """The canonical relation name for a daily leaf of `parent_name`."""
    if lower.tzinfo is not timezone.utc:
        raise ValueError('leaf name anchors must be in UTC')
    name = f'{parent_name}_{lower:%Y_%m_%d}'
    if not is_safe_identifier(name):
        raise ValueError(f'derived leaf name is not a safe identifier: {name!r}')
    return name


def open_ended_leaf_name(parent_name: str, lower: datetime) -> str:
    """The canonical relation name for the open-ended leaf of `parent_name`."""
    if lower.tzinfo is not timezone.utc:
        raise ValueError('leaf name anchors must be in UTC')
    name = f'{parent_name}_open_{lower:%Y_%m_%d}'
    if not is_safe_identifier(name):
        raise ValueError(f'derived leaf name is not a safe identifier: {name!r}')
    return name


def leaf_id_index_name(leaf_name: str) -> str:
    """The canonical per-leaf task-ID index name."""
    return f'{leaf_name}_task_idx'


def heartbeat_probe_index_name(leaf_name: str) -> str:
    """The per-leaf stale-probe index name of an hourly heartbeat leaf."""
    name = f'{leaf_name}_probe_idx'
    if not is_safe_identifier(name):
        raise ValueError(f'{name!r} is not a safe identifier')
    return name


def history_class_parent_name(class_key: str) -> str:
    """The parent relation of one history class: ``horsies_task_history_<key>``."""
    name = f'{TASK_HISTORY_PARENT}_{class_key}'
    if not is_safe_identifier(name):
        raise ValueError(
            f'class key {class_key!r} does not form a safe relation name'
        )
    return name


def leaf_parent_name(class_key: str) -> str:
    """The parent relation of a leaf of ``class_key``."""
    match class_key:
        case str() as key if key == HEARTBEAT_CLASS_KEY:
            return HEARTBEATS_TABLE
        case str() as key if key == FOREVER_CLASS_KEY:
            return TASK_HISTORY_FOREVER
        case _:
            return history_class_parent_name(class_key)


def leaf_task_index_name(class_key: str, leaf_name: str) -> str:
    """The task index of a leaf: the stale probe on heartbeats, the id index on history."""
    match class_key:
        case str() as key if key == HEARTBEAT_CLASS_KEY:
            return heartbeat_probe_index_name(leaf_name)
        case _:
            return leaf_id_index_name(leaf_name)


def _bound_literal(instant: datetime) -> str:
    """``instant`` as PostgreSQL renders a timestamptz literal under TimeZone UTC."""
    utc = instant.astimezone(timezone.utc)
    rendered = f'{utc:%Y-%m-%d %H:%M:%S}'
    if utc.microsecond:
        rendered += f'.{utc.microsecond:06d}'.rstrip('0')
    return f"'{rendered}+00'"


def render_partition_bound(lower: LowerBound, upper: datetime) -> str:
    """The partition bound of a leaf as ``pg_get_expr`` renders it under TimeZone UTC."""
    match lower:
        case OpenLowerBound():
            lower_text = 'MINVALUE'
        case AnchoredLowerBound(anchor=anchor):
            lower_text = _bound_literal(anchor)
    return f'FOR VALUES FROM ({lower_text}) TO ({_bound_literal(upper)})'


def decode_lower_bound(kind: Any, anchor: Any) -> LowerBound:
    """The typed lower bound of a ``(lower_bound_kind, lower_anchor)`` pair.

    The pair CHECK of the table holds the anchor to the kind; another pair
    means this code and the schema disagree.
    """
    match kind, anchor:
        case 'OPEN', None:
            return OpenLowerBound()
        case 'AT', datetime():
            return AnchoredLowerBound(required_utc_timestamp(anchor, 'lower_anchor'))
        case _:
            raise HistoryContractError(
                f'lower bound pair is not OPEN/NULL or AT/timestamp: {kind!r}, {anchor!r}'
            )


def anchored_lower(bound: LowerBound, leaf_name: str) -> datetime:
    """The lower anchor of a leaf that maintenance addresses by bounds.

    Only the static forever leaf has an open lower bound, and no
    maintenance command targets it by bounds.
    """
    match bound:
        case AnchoredLowerBound(anchor=anchor):
            return anchor
        case OpenLowerBound():
            raise HistoryContractError(
                f'leaf {leaf_name!r} has an open lower bound where an anchored one is required'
            )


def leaf_enqueued_index_name(leaf_name: str) -> str:
    """The canonical per-leaf enqueue-order index name.

    The index serves the monitoring list's default sort: with a btree on
    `enqueued_at` per leaf the planner merge-appends leaves in index
    order and stops at the LIMIT instead of sorting every matched row.
    """
    return f'{leaf_name}_enqueued_idx'


ORDERING_INDEX_COLUMN = 'enqueued_at'
"""The single key column every leaf's ordering index carries."""

# The probe pins the PROPERTY — a non-partial single-key btree whose key
# column is `enqueued_at` — never an index name. The transcode swap
# attaches replacement relations whose index names differ from the
# cataloged ones, so a name probe would report a conformant leaf absent.
_ORDERING_INDEX_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE i.indrelid = to_regclass(:leaf)
      AND am.amname = 'btree'
      AND i.indisvalid
      AND i.indisready
      AND i.indislive
      AND NOT i.indisunique
      AND NOT i.indisprimary
      AND NOT i.indisexclusion
      AND i.indpred IS NULL
      AND i.indexprs IS NULL
      AND i.indnkeyatts = 1
      AND i.indnatts = 1
      AND i.indkey[0] = (
          SELECT a.attnum
          FROM pg_attribute AS a
          WHERE a.attrelid = to_regclass(:leaf)
            AND a.attname = :column
      )
      AND i.indcollation[0] = (
          SELECT a.attcollation
          FROM pg_attribute AS a
          WHERE a.attrelid = to_regclass(:leaf)
            AND a.attname = :column
      )
      AND pg_get_indexdef(i.indexrelid, 1, false) = :column
      AND pg_get_indexdef(i.indexrelid)
          LIKE '% USING btree (enqueued_at)'
      AND i.indoption[0] = 0
)
"""


async def read_leaf_ordering_index_exists(
    connection: AsyncConnection,
    leaf_name: str,
) -> bool:
    """Whether the leaf carries the enqueue-order index, by composition.

    False for an absent relation: `to_regclass` resolves NULL and the
    probe matches nothing, which is the correct answer for both.
    """
    if not is_safe_identifier(leaf_name):
        raise HistoryContractError(
            f'relation name is not a safe identifier: {leaf_name!r}'
        )
    value = (
        await connection.execute(
            text(_ORDERING_INDEX_EXISTS_SQL),
            {'leaf': leaf_name, 'column': ORDERING_INDEX_COLUMN},
        )
    ).scalar_one()
    return _required_bool(value, 'ordering_index_exists')


async def read_retention_class(
    connection: AsyncConnection,
    class_key: str,
) -> RetentionClassRow | None:
    row = (
        await connection.execute(
            text(
                f"""
                SELECT class_key, class_kind, retention_days, heartbeat_horizon_hours
                FROM {RETENTION_CLASSES}
                WHERE class_key = :class_key
                """
            ),
            {'class_key': class_key},
        )
    ).one_or_none()
    if row is None:
        return None
    stored_key = _required_str(row.class_key, 'class_key')
    match row.class_kind, row.retention_days, row.heartbeat_horizon_hours:
        case 'FOREVER', None, None:
            return RetentionClassRow(
                class_key=stored_key,
                duration=None,
                partition_interval=None,
                finite_parent_name=None,
            )
        case 'PERIOD', int() as days, None:
            return RetentionClassRow(
                class_key=stored_key,
                duration=timedelta(days=days),
                partition_interval=timedelta(days=1),
                finite_parent_name=history_class_parent_name(stored_key),
            )
        case 'HEARTBEAT', None, int() as hours:
            return RetentionClassRow(
                class_key=stored_key,
                duration=timedelta(hours=hours),
                partition_interval=timedelta(hours=1),
                finite_parent_name=HEARTBEATS_TABLE,
            )
        case kind, days, hours:
            raise HistoryContractError(
                f'retention class {class_key!r} has no known shape: '
                f'kind={kind!r} days={days!r} horizon_hours={hours!r}'
            )


async def read_leaf_catalog_row(
    connection: AsyncConnection,
    leaf_name: str,
) -> LeafCatalogRow | None:
    row = (
        await connection.execute(
            text(
                f"""
                SELECT leaf_name, class_key,
                       lower_bound_kind, lower_anchor, upper_anchor,
                       index_schema_version,
                       created_at, detached_at, dropped_at
                FROM {LEAF_CATALOG}
                WHERE leaf_name = :leaf_name
                """
            ),
            {'leaf_name': leaf_name},
        )
    ).one_or_none()
    if row is None:
        return None
    stored_name = _required_str(row.leaf_name, 'leaf_name')
    if not is_safe_identifier(stored_name):
        raise HistoryContractError(
            f'cataloged leaf name is not a safe identifier: {stored_name!r}'
        )
    class_key = _required_str(row.class_key, 'class_key')
    lower_bound = decode_lower_bound(row.lower_bound_kind, row.lower_anchor)
    upper_anchor = required_utc_timestamp(row.upper_anchor, 'upper_anchor')
    return LeafCatalogRow(
        leaf_name=stored_name,
        parent_name=leaf_parent_name(class_key),
        class_key=class_key,
        lower_bound=lower_bound,
        upper_anchor=upper_anchor,
        index_schema_version=_required_int(
            row.index_schema_version, 'index_schema_version'
        ),
        id_index_name=leaf_task_index_name(class_key, stored_name),
        partition_bound=render_partition_bound(lower_bound, upper_anchor),
        created_at=required_utc_timestamp(row.created_at, 'created_at'),
        detached_at=optional_utc_timestamp(row.detached_at, 'detached_at'),
        dropped_at=optional_utc_timestamp(row.dropped_at, 'dropped_at'),
    )


async def read_attached_leaf_rows(
    connection: AsyncConnection,
    class_key: str,
) -> tuple[LeafCatalogRow, ...]:
    """Catalog rows of one class the manager believes attached, oldest first."""
    return await _read_attached(connection, class_key=class_key)


async def read_all_attached_leaf_rows(
    connection: AsyncConnection,
) -> tuple[LeafCatalogRow, ...]:
    """Every attached catalog row across all classes, oldest first.

    The staged lookup function is regenerated from this complete set: a
    per-class manifest would drop every other class's leaves from the
    function and turn their retained rows into false absence.
    """
    return await _read_attached(connection, class_key=None)


async def _read_attached(
    connection: AsyncConnection,
    *,
    class_key: str | None,
) -> tuple[LeafCatalogRow, ...]:
    class_filter = 'AND class_key = :class_key' if class_key is not None else ''
    parameters = {'class_key': class_key} if class_key is not None else {}
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT leaf_name
                FROM {LEAF_CATALOG}
                WHERE detached_at IS NULL
                  AND dropped_at IS NULL
                  {class_filter}
                ORDER BY lower_anchor NULLS FIRST, leaf_name
                """
            ),
            parameters,
        )
    ).all()
    manifest: list[LeafCatalogRow] = []
    for row in rows:
        leaf_name = _required_str(row.leaf_name, 'leaf_name')
        catalog_row = await read_leaf_catalog_row(connection, leaf_name)
        if catalog_row is None:
            raise HistoryContractError(
                f'attached leaf {leaf_name!r} vanished from the catalog mid-read'
            )
        manifest.append(catalog_row)
    return tuple(manifest)


async def execute_with_utc_rendering(
    connection: AsyncConnection,
    statement: Any,
    parameters: dict[str, Any] | None = None,
) -> Any:
    """Execute one statement with the session timezone pinned to UTC.

    `pg_get_expr` renders timestamptz literals in the SESSION timezone,
    so identical instants render differently across sessions and a text
    comparison of partition bounds is session-dependent. Every bound
    capture goes through this wrapper, making the stored convention —
    bounds render under UTC — hold by construction. Canonical rendering
    is chosen over parsing the rendered expression back into typed
    instants: parsing would re-implement the server's rendering in
    reverse, and the exact text keeps catching bound drift that has
    nothing to do with timezones. The session-set-and-restore form works
    on transactional and autocommit connections alike (the detach path's
    statement_timeout handling is the in-module precedent).
    """
    prior = (
        await connection.execute(text('SHOW timezone'))
    ).scalar_one()
    await connection.execute(
        text("SELECT set_config('timezone', 'UTC', false)")
    )
    try:
        result = await connection.execute(statement, parameters)
    except asyncio.CancelledError as error:
        await connection.invalidate(error)
        raise
    except BaseException:
        try:
            await connection.execute(
                text("SELECT set_config('timezone', :tz, false)"),
                {'tz': prior},
            )
        except BaseException as restore_error:
            await connection.invalidate(restore_error)
        raise
    try:
        await connection.execute(
            text("SELECT set_config('timezone', :tz, false)"),
            {'tz': prior},
        )
    except BaseException as error:
        await connection.invalidate(error)
        raise
    return result


async def capture_partition_bound_utc(
    connection: AsyncConnection,
    leaf_name: str,
) -> str | None:
    """The leaf's rendered partition bound under the UTC convention."""
    if not is_safe_identifier(leaf_name):
        raise HistoryContractError(
            f'relation name is not a safe identifier: {leaf_name!r}'
        )
    result = await execute_with_utc_rendering(
        connection,
        text(
            'SELECT pg_get_expr(c.relpartbound, c.oid) AS bound '
            'FROM pg_class AS c WHERE c.oid = to_regclass(:leaf)'
        ),
        {'leaf': leaf_name},
    )
    row = result.one_or_none()
    if row is None:
        return None
    return _optional_str(row.bound, 'partition_bound')


async def read_leaf_physical_state(
    connection: AsyncConnection,
    *,
    leaf_name: str,
    parent_name: str,
    id_index_name: str,
    bound_expectation: LeafPartitionBoundExpectation,
    index_kind: LeafIndexKind,
) -> LeafPhysicalState:
    for identifier in (leaf_name, parent_name, id_index_name):
        if not is_safe_identifier(identifier):
            raise HistoryContractError(
                f'relation name is not a safe identifier: {identifier!r}'
            )
    match bound_expectation:
        case RequestedPartitionBounds(bounds=bounds):
            expected_lower = bounds.lower
            expected_upper = bounds.upper
        case CatalogPartitionBound():
            expected_lower = None
            expected_upper = None
    match index_kind:
        case LeafIndexKind.HISTORY:
            heartbeat_index = False
        case LeafIndexKind.HEARTBEAT:
            heartbeat_index = True
    row = (
        await execute_with_utc_rendering(
            connection,
            text(
                """
                WITH selected_index AS MATERIALIZED (
                    SELECT index_state.*, access_method.amname
                    FROM pg_index AS index_state
                    JOIN pg_class AS index_relation
                      ON index_relation.oid = index_state.indexrelid
                    JOIN pg_am AS access_method
                      ON access_method.oid = index_relation.relam
                    WHERE index_state.indexrelid = to_regclass(:id_index)
                )
                SELECT to_regclass(:leaf)::oid AS leaf_oid,
                       to_regclass(:parent)::oid AS parent_oid,
                       (SELECT pg_get_expr(c.relpartbound, c.oid)
                        FROM pg_class AS c
                        WHERE c.oid = to_regclass(:leaf)) AS partition_bound,
                       COALESCE((
                           SELECT CAST(:expected_lower AS timestamptz) IS NULL
                               OR pg_get_expr(c.relpartbound, c.oid) = format(
                                   'FOR VALUES FROM (%L) TO (%L)',
                                   CAST(:expected_lower AS timestamptz),
                                   CAST(:expected_upper AS timestamptz)
                               )
                           FROM pg_class AS c
                           WHERE c.oid = to_regclass(:leaf)
                       ), FALSE) AS partition_bound_matches_expected,
                       EXISTS (
                           SELECT 1
                           FROM selected_index
                           WHERE selected_index.indrelid = to_regclass(:leaf)
                             AND selected_index.amname = 'btree'
                             AND selected_index.indisvalid
                             AND selected_index.indisready
                             AND selected_index.indislive
                             AND NOT selected_index.indisunique
                             AND NOT selected_index.indisprimary
                             AND NOT selected_index.indisexclusion
                             AND selected_index.indpred IS NULL
                             AND selected_index.indexprs IS NULL
                             AND (
                                 (
                                     NOT CAST(:heartbeat_index AS boolean)
                                     AND selected_index.indnkeyatts = 1
                                     AND selected_index.indnatts = 1
                                     AND selected_index.indkey[0] = (
                                         SELECT attribute.attnum
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'task_id'
                                     )
                                     AND selected_index.indcollation[0] = (
                                         SELECT attribute.attcollation
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'task_id'
                                     )
                                     AND pg_get_indexdef(
                                         selected_index.indexrelid, 1, false
                                     ) = 'task_id'
                                     AND pg_get_indexdef(selected_index.indexrelid)
                                         LIKE '% USING btree (task_id)'
                                     AND selected_index.indoption[0] = 0
                                 )
                                 OR (
                                     CAST(:heartbeat_index AS boolean)
                                     AND selected_index.indnkeyatts = 3
                                     AND selected_index.indnatts = 3
                                     AND selected_index.indkey[0] = (
                                         SELECT attribute.attnum
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'task_id'
                                     )
                                     AND selected_index.indkey[1] = (
                                         SELECT attribute.attnum
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'role'
                                     )
                                     AND selected_index.indkey[2] = (
                                         SELECT attribute.attnum
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'sent_at'
                                     )
                                     AND selected_index.indcollation[0] = (
                                         SELECT attribute.attcollation
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'task_id'
                                     )
                                     AND selected_index.indcollation[1] = (
                                         SELECT attribute.attcollation
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'role'
                                     )
                                     AND selected_index.indcollation[2] = (
                                         SELECT attribute.attcollation
                                         FROM pg_attribute AS attribute
                                         WHERE attribute.attrelid = to_regclass(:leaf)
                                           AND attribute.attname = 'sent_at'
                                     )
                                     AND pg_get_indexdef(
                                         selected_index.indexrelid, 1, false
                                     ) = 'task_id'
                                     AND pg_get_indexdef(
                                         selected_index.indexrelid, 2, false
                                     ) = 'role'
                                     AND pg_get_indexdef(
                                         selected_index.indexrelid, 3, false
                                     ) = 'sent_at'
                                     AND pg_get_indexdef(selected_index.indexrelid)
                                         LIKE '% USING btree (task_id, role, sent_at DESC)'
                                     AND selected_index.indoption[0] = 0
                                     AND selected_index.indoption[1] = 0
                                     AND selected_index.indoption[2] = 3
                                 )
                             )
                       ) AS id_index_conformant,
                       (SELECT i.inhdetachpending
                        FROM pg_inherits AS i
                        WHERE i.inhparent = to_regclass(:parent)
                          AND i.inhrelid = to_regclass(:leaf)) AS detach_pending
                """
            ),
            {
                'leaf': leaf_name,
                'parent': parent_name,
                'id_index': id_index_name,
                'expected_lower': expected_lower,
                'expected_upper': expected_upper,
                'heartbeat_index': heartbeat_index,
            },
        )
    ).one()
    detach_pending = row.detach_pending
    if detach_pending is not None and not isinstance(detach_pending, bool):
        raise HistoryContractError('inhdetachpending did not decode as boolean')
    return LeafPhysicalState(
        relation_exists=row.leaf_oid is not None,
        parent_exists=row.parent_oid is not None,
        partition_bound=_optional_str(row.partition_bound, 'partition_bound'),
        partition_bound_matches_expected=_required_bool(
            row.partition_bound_matches_expected,
            'partition_bound_matches_expected',
        ),
        id_index_conformant=_required_bool(
            row.id_index_conformant,
            'id_index_conformant',
        ),
        detach_pending=detach_pending,
    )


async def database_now(connection: AsyncConnection) -> datetime:
    """The database's statement timestamp, normalized to UTC.

    Maintenance never trusts host time — and it never trusts the SESSION
    timezone either: the driver decodes timestamptz into the session's
    zone, and flooring such a value to a day or hour boundary would
    anchor leaves at LOCAL boundaries with local-date names. Normalizing
    here makes every downstream floor and name render under UTC by
    construction; the instant is unchanged.
    """
    value = (
        await connection.execute(text('SELECT statement_timestamp()'))
    ).scalar_one()
    return required_utc_timestamp(value, 'statement_timestamp')


# ---------------------------------------------------------------------------
# Fail-closed column decoding
# ---------------------------------------------------------------------------


def _required_str(value: Any, column: str) -> str:
    if not isinstance(value, str):
        raise HistoryContractError(f'{column} did not decode as text')
    return value


def _optional_str(value: Any, column: str) -> str | None:
    if value is None:
        return None
    return _required_str(value, column)


def _required_bool(value: Any, column: str) -> bool:
    if not isinstance(value, bool):
        raise HistoryContractError(f'{column} did not decode as boolean')
    return value


def _required_int(value: Any, column: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise HistoryContractError(f'{column} did not decode as integer')
    return value


def required_utc_timestamp(value: Any, column: str) -> datetime:
    """A decoded timestamptz value as the same instant in UTC.

    The driver decodes timestamptz into the session's zone. Under a zone
    with DST, PEP 495 makes a value inside the repeated hour compare
    unequal to every other zone's value at the same instant, and a
    difference of two values in that zone is wall-clock time. UTC values
    compare and subtract by instant, and render UTC dates in leaf names.
    """
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise HistoryContractError(
            f'{column} did not decode as a timezone-aware timestamp'
        )
    return value.astimezone(timezone.utc)


def optional_utc_timestamp(value: Any, column: str) -> datetime | None:
    if value is None:
        return None
    return required_utc_timestamp(value, column)


def decode_leaf_bounds(lower: Any, upper: Any) -> LeafBounds:
    """The UTC `LeafBounds` of decoded catalog anchor values.

    Only for rows of an anchored lower bound; the reads that call it select
    ``lower_bound_kind = 'AT'``.
    """
    return LeafBounds(
        lower=required_utc_timestamp(lower, 'lower_anchor'),
        upper=required_utc_timestamp(upper, 'upper_anchor'),
    )
