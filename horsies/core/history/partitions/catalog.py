"""The leaf catalog: the partition manager's durable memory.

PostgreSQL's own catalogs say what is attached right now; they cannot say
what the manager created, detached, or dropped, with which bounds, under
which class, or carrying which index version. The leaf catalog records that,
and every maintenance decision cross-checks catalog against `pg_catalog`
before acting — a decision made from either source alone can be stale.

The catalog also carries each leaf's minimum UUIDv7 birth time. The staged
lookup loader prunes finite probes with it, and constant-time purged
detection depends on the retained global birth floor being recomputed at
retirement; both are catalog facts, not derivable from `pg_catalog`.

Reads decode fail-closed: a column of an unexpected type means this code and
the installed schema disagree, and that raises `HistoryContractError` rather
than flowing onward as data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import is_safe_identifier
from ..errors import HistoryContractError, HistoryParentAbsent
from ..names import LEAF_CATALOG, LEAF_LOCK_KEY_FUNCTION, RETENTION_CLASSES

INDEX_SCHEMA_VERSION = 1
"""Current per-leaf index layout version recorded on creation."""

LEAF_CATALOG_DDL = f"""
CREATE TABLE {LEAF_CATALOG} (
    leaf_name text PRIMARY KEY,
    parent_name text NOT NULL,
    class_key text NOT NULL
        REFERENCES {RETENTION_CLASSES}(class_key),
    lower_anchor timestamptz NOT NULL,
    upper_anchor timestamptz NOT NULL,
    index_schema_version smallint NOT NULL,
    id_index_name text NOT NULL,
    partition_bound text NOT NULL,
    min_birth_at timestamptz,
    min_birth_verified boolean NOT NULL,
    created_at timestamptz NOT NULL,
    detached_at timestamptz,
    dropped_at timestamptz,
    CHECK (lower_anchor < upper_anchor),
    CHECK (dropped_at IS NULL OR detached_at IS NOT NULL),
    UNIQUE (class_key, lower_anchor, upper_anchor)
)
"""

LEAF_LOCK_KEY_FUNCTION_DDL = f"""
CREATE FUNCTION {LEAF_LOCK_KEY_FUNCTION}(
    p_class_key text,
    p_anchor timestamptz
) RETURNS bigint
LANGUAGE sql
STABLE
STRICT
AS $function$
    SELECT hashtextextended(
        p_class_key || chr(31) ||
        extract(epoch FROM date_trunc('day', p_anchor, 'UTC'))
            ::bigint::text,
        1601
    )
$function$
"""


@dataclass(frozen=True, slots=True)
class RetentionClassRow:
    """One retention class as the frozen DDL module stores it.

    `duration` None means forever; forever classes have no finite parent and
    no partition interval. The column shape is owned by the frozen-DDL
    module; this reader is part of that frozen contract.
    """

    class_key: str
    duration: timedelta | None
    partition_interval: timedelta | None
    finite_parent_name: str | None


@dataclass(frozen=True, slots=True)
class LeafCatalogRow:
    """One leaf as the catalog remembers it."""

    leaf_name: str
    parent_name: str
    class_key: str
    lower_anchor: datetime
    upper_anchor: datetime
    index_schema_version: int
    id_index_name: str
    partition_bound: str
    min_birth_at: datetime | None
    min_birth_verified: bool
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
    id_index_exists: bool
    detach_pending: bool | None


def daily_leaf_name(parent_name: str, lower: datetime) -> str:
    """The canonical relation name for a daily leaf of `parent_name`."""
    name = f'{parent_name}_{lower:%Y_%m_%d}'
    if not is_safe_identifier(name):
        raise ValueError(f'derived leaf name is not a safe identifier: {name!r}')
    return name


def leaf_id_index_name(leaf_name: str) -> str:
    """The canonical per-leaf task-ID index name."""
    return f'{leaf_name}_task_idx'


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
      AND i.indpred IS NULL
      AND i.indnkeyatts = 1
      AND i.indkey[0] = (
          SELECT a.attnum
          FROM pg_attribute AS a
          WHERE a.attrelid = to_regclass(:leaf)
            AND a.attname = :column
      )
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
                SELECT class_key, duration, partition_interval,
                       finite_parent_name
                FROM {RETENTION_CLASSES}
                WHERE class_key = :class_key
                """
            ),
            {'class_key': class_key},
        )
    ).one_or_none()
    if row is None:
        return None
    duration = _optional_timedelta(row.duration, 'duration')
    partition_interval = _optional_timedelta(
        row.partition_interval, 'partition_interval'
    )
    finite_parent_name = _optional_str(row.finite_parent_name, 'finite_parent_name')
    if duration is not None and finite_parent_name is None:
        raise HistoryContractError(
            f'finite retention class {class_key!r} has no finite parent relation'
        )
    if finite_parent_name is not None and not is_safe_identifier(finite_parent_name):
        raise HistoryContractError(
            f'retention class {class_key!r} names an unsafe parent relation'
        )
    return RetentionClassRow(
        class_key=_required_str(row.class_key, 'class_key'),
        duration=duration,
        partition_interval=partition_interval,
        finite_parent_name=finite_parent_name,
    )


async def read_leaf_catalog_row(
    connection: AsyncConnection,
    leaf_name: str,
) -> LeafCatalogRow | None:
    row = (
        await connection.execute(
            text(
                f"""
                SELECT leaf_name, parent_name, class_key,
                       lower_anchor, upper_anchor,
                       index_schema_version, id_index_name, partition_bound,
                       min_birth_at, min_birth_verified,
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
    id_index_name = _required_str(row.id_index_name, 'id_index_name')
    if not is_safe_identifier(id_index_name):
        raise HistoryContractError(
            f'cataloged index name is not a safe identifier: {id_index_name!r}'
        )
    return LeafCatalogRow(
        leaf_name=_required_str(row.leaf_name, 'leaf_name'),
        parent_name=_required_str(row.parent_name, 'parent_name'),
        class_key=_required_str(row.class_key, 'class_key'),
        lower_anchor=_required_datetime(row.lower_anchor, 'lower_anchor'),
        upper_anchor=_required_datetime(row.upper_anchor, 'upper_anchor'),
        index_schema_version=_required_int(
            row.index_schema_version, 'index_schema_version'
        ),
        id_index_name=id_index_name,
        partition_bound=_required_str(row.partition_bound, 'partition_bound'),
        min_birth_at=_optional_datetime(row.min_birth_at, 'min_birth_at'),
        min_birth_verified=_required_bool(row.min_birth_verified, 'min_birth_verified'),
        created_at=_required_datetime(row.created_at, 'created_at'),
        detached_at=_optional_datetime(row.detached_at, 'detached_at'),
        dropped_at=_optional_datetime(row.dropped_at, 'dropped_at'),
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


@dataclass(frozen=True, slots=True)
class ManifestLeafSelection:
    """The attached set, split by whether the relation actually exists.

    The staged readers probe relations by name, and PostgreSQL does not
    validate PL/pgSQL bodies at CREATE — a generated function naming a
    relation that is gone builds happily and dies at execution, on the
    finalize path, for every queue and class at once. So the probe list
    must be filtered by existence.

    The catalog is NOT corrected to match. A cataloged-attached leaf whose
    relation vanished means someone changed the database behind the
    manager's back, and `CatalogConflictKind` states the stance: that needs
    an operator, not a heuristic. Stamping the row dropped would destroy
    the evidence of an accidental drop and reclassify it as ordinary
    ageing. The reader routes around the wound; the memory keeps it.

    Both halves are retained because they answer different questions. The
    probe list says which relations can be read; the complete attached set
    says what history was retained, which is what the birth floor means.
    """

    attached: tuple[LeafCatalogRow, ...]
    absent_relations: frozenset[str]


_MANIFEST_SELECTION_SQL = f"""
SELECT leaf_name,
       to_regclass(leaf_name) IS NOT NULL AS relation_exists,
       to_regclass(parent_name) IS NOT NULL AS parent_exists
FROM {LEAF_CATALOG}
WHERE detached_at IS NULL
  AND dropped_at IS NULL
ORDER BY lower_anchor, leaf_name
"""


async def read_manifest_leaf_rows(
    connection: AsyncConnection,
) -> ManifestLeafSelection:
    """The attached set for publication, with missing relations named.

    `to_regclass` resolves through `search_path`, so a session that cannot
    see the schema reports EVERY leaf missing — and publishing that would
    empty the manifest and turn all retained history into genuine false
    absence. The parent relation is the discriminator: one leaf gone with
    its parent still resolving is a dropped leaf, while a leaf whose parent
    is equally unresolvable is a session that cannot see the schema. The
    second raises rather than reporting a fleet-wide loss as data.
    """
    rows = (await connection.execute(text(_MANIFEST_SELECTION_SQL))).all()
    attached: list[LeafCatalogRow] = []
    absent: list[str] = []
    for row in rows:
        leaf_name = _required_str(row.leaf_name, 'leaf_name')
        catalog_row = await read_leaf_catalog_row(connection, leaf_name)
        if catalog_row is None:
            raise HistoryContractError(
                f'attached leaf {leaf_name!r} vanished from the catalog mid-read'
            )
        attached.append(catalog_row)
        relation_exists = _required_bool(row.relation_exists, 'relation_exists')
        parent_exists = _required_bool(row.parent_exists, 'parent_exists')
        match (relation_exists, parent_exists):
            case (True, _):
                pass
            case (False, True):
                absent.append(leaf_name)
            case (False, False):
                raise HistoryParentAbsent(
                    f'neither leaf {leaf_name!r} nor its parent '
                    f'{catalog_row.parent_name!r} resolves; the session '
                    f'cannot see the history schema'
                )
    return ManifestLeafSelection(
        attached=tuple(attached), absent_relations=frozenset(absent)
    )


async def read_attached_birth_floor(
    connection: AsyncConnection,
    *,
    excluded_class_key: str,
) -> datetime | None:
    """The minimum birth time across every attached leaf of a real class.

    Unfiltered by relation existence, deliberately. The floor answers
    "does this identifier predate all RETAINED history", and a leaf
    destroyed out of band was retained — its rows were real and then
    someone removed them. Dropping it from the floor would raise the floor
    and reclassify those tasks as too old to have been kept, which is a
    false explanation of a true absence.

    Two independent things currently keep heartbeat leaves out of the
    floor, and neither should be mistaken for the other. `min()` skips
    NULLs and heartbeat rows are created with `min_birth_at` NULL, so
    they cannot reach the floor today whatever this predicate says. The
    class filter is the one that states the INTENT: heartbeats are not
    task history and their birth times are not where history begins.
    Remove the filter and nothing breaks until someone gives heartbeat
    leaves a birth time, at which point the floor silently starts
    tracking liveness rows.
    """
    value = (
        await connection.execute(
            text(
                f"""
                SELECT min(min_birth_at)
                FROM {LEAF_CATALOG}
                WHERE detached_at IS NULL
                  AND dropped_at IS NULL
                  AND class_key <> :excluded_class_key
                """
            ),
            {'excluded_class_key': excluded_class_key},
        )
    ).scalar_one_or_none()
    return _optional_datetime(value, 'min_birth_at')


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
                ORDER BY lower_anchor, leaf_name
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
        return await connection.execute(statement, parameters)
    finally:
        await connection.execute(
            text("SELECT set_config('timezone', :tz, false)"),
            {'tz': prior},
        )


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
) -> LeafPhysicalState:
    for identifier in (leaf_name, parent_name, id_index_name):
        if not is_safe_identifier(identifier):
            raise HistoryContractError(
                f'relation name is not a safe identifier: {identifier!r}'
            )
    row = (
        await execute_with_utc_rendering(
            connection,
            text(
                """
                SELECT to_regclass(:leaf)::oid AS leaf_oid,
                       to_regclass(:parent)::oid AS parent_oid,
                       to_regclass(:id_index) IS NOT NULL AS id_index_exists,
                       (SELECT pg_get_expr(c.relpartbound, c.oid)
                        FROM pg_class AS c
                        WHERE c.oid = to_regclass(:leaf)) AS partition_bound,
                       (SELECT i.inhdetachpending
                        FROM pg_inherits AS i
                        WHERE i.inhparent = to_regclass(:parent)
                          AND i.inhrelid = to_regclass(:leaf)) AS detach_pending
                """
            ),
            {'leaf': leaf_name, 'parent': parent_name, 'id_index': id_index_name},
        )
    ).one()
    detach_pending = row.detach_pending
    if detach_pending is not None and not isinstance(detach_pending, bool):
        raise HistoryContractError('inhdetachpending did not decode as boolean')
    return LeafPhysicalState(
        relation_exists=row.leaf_oid is not None,
        parent_exists=row.parent_oid is not None,
        partition_bound=_optional_str(row.partition_bound, 'partition_bound'),
        id_index_exists=bool(row.id_index_exists),
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
    return _required_datetime(value, 'statement_timestamp').astimezone(
        timezone.utc
    )


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


def _required_datetime(value: Any, column: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise HistoryContractError(
            f'{column} did not decode as a timezone-aware timestamp'
        )
    return value


def _optional_datetime(value: Any, column: str) -> datetime | None:
    if value is None:
        return None
    return _required_datetime(value, column)


def _optional_timedelta(value: Any, column: str) -> timedelta | None:
    if value is None:
        return None
    if not isinstance(value, timedelta):
        raise HistoryContractError(f'{column} did not decode as an interval')
    return value
