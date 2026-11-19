"""The live schema against the catalog: every column has a rule, every rule has a column.

A schema change without a catalog change fails the capture. The capture
never guesses a rule for a column.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from tests.dualrun.rules import (
    AnchorReference,
    ColumnRule,
    DigestOf,
    Exact,
    FormatVersion,
    GeneratedName,
    Ignored,
    JsonStorage,
    NormalisedId,
    SnapshotCatalog,
    StoredJson,
    TimeOrderBeforeRun,
    TimeOrderOnly,
    TimeWithin,
)


class DatabaseType(Enum):
    """``pg_type.typname`` of the column types the snapshot reads."""

    BOOL = 'bool'
    BYTEA = 'bytea'
    FLOAT8 = 'float8'
    INT2 = 'int2'
    INT4 = 'int4'
    INT4_ARRAY = '_int4'
    INT8 = 'int8'
    INTERVAL = 'interval'
    JSONB = 'jsonb'
    OID = 'oid'
    TEXT = 'text'
    TIMESTAMPTZ = 'timestamptz'
    UUID = 'uuid'
    VARCHAR = 'varchar'
    VARCHAR_ARRAY = '_varchar'


@dataclass(frozen=True, slots=True)
class UnknownType:
    """A column type the snapshot has no reader for."""

    name: str


ColumnType = DatabaseType | UnknownType

_TYPE_BY_NAME: Final = {member.value: member for member in DatabaseType}

_ID_TYPES: Final = frozenset(
    {
        DatabaseType.UUID,
        DatabaseType.TEXT,
        DatabaseType.VARCHAR,
        DatabaseType.BYTEA,
        DatabaseType.OID,
    },
)
"""Types a ``NormalisedId`` rule accepts. ``OID`` is the ``tableoid`` of a partitioned table."""


def parse_column_type(name: str) -> ColumnType:
    """The type of a ``pg_type.typname``."""
    match _TYPE_BY_NAME.get(name):
        case None:
            return UnknownType(name=name)
        case known:
            return known


@dataclass(frozen=True, slots=True)
class SchemaColumn:
    """One column of a ``horsies_`` table in the live schema.

    For a partitioned table the schema also lists the system column
    ``tableoid``: it names the leaf of each row read through the parent.
    """

    table: str
    column: str
    column_type: ColumnType


# ---- Gaps ----


@dataclass(frozen=True, slots=True)
class UnclassifiedColumn:
    """A live column with no rule in the catalog."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class RuleWithoutColumn:
    """A catalog rule for a column the live schema does not have."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class UnsupportedColumnType:
    """A live column whose type the snapshot cannot read."""

    table: str
    column: str
    type_name: str


@dataclass(frozen=True, slots=True)
class RuleTypeMismatch:
    """A rule that does not apply to the column's type (for example a time rule on text)."""

    table: str
    column: str
    rule: ColumnRule
    column_type: DatabaseType


@dataclass(frozen=True, slots=True)
class ReferencedTypeMismatch:
    """A rule names another column of a type it cannot read.

    An anchor must be timestamptz, a format version an integer, a digest
    source bytea.
    """

    table: str
    column: str
    referenced: str
    referenced_type: ColumnType


@dataclass(frozen=True, slots=True)
class DetachedLeafShapeMismatch:
    """A detached leaf whose columns or column types differ from its former parent's."""

    table: str
    parent: str


CoverageGap = (
    UnclassifiedColumn
    | RuleWithoutColumn
    | UnsupportedColumnType
    | RuleTypeMismatch
    | ReferencedTypeMismatch
    | DetachedLeafShapeMismatch
)

# ---- Detached leaves ----

HISTORY_PARENT: Final = 'horsies_task_history'
HEARTBEAT_PARENT: Final = 'horsies_heartbeats'
HEARTBEAT_CLASS_KEY: Final = 'heartbeats'
"""The leaf catalog class of heartbeat leaves; every other class holds history leaves."""


@dataclass(frozen=True, slots=True)
class DetachedLeaf:
    """A table that the leaf catalog records as detached and not dropped.

    It is a standalone table now, not a partition, so the schema lists it.
    The capture reads it with the rules of ``parent`` and adds its rows to
    the parent's rows, with the leaf's own name as ``tableoid``.
    """

    table: str
    parent: str


def detached_leaf(table: str, class_key: str) -> DetachedLeaf:
    """The detached leaf ``table`` of the leaf catalog class ``class_key``."""
    match class_key:
        case key if key == HEARTBEAT_CLASS_KEY:
            return DetachedLeaf(table=table, parent=HEARTBEAT_PARENT)
        case _:
            return DetachedLeaf(table=table, parent=HISTORY_PARENT)


def split_detached_leaves(
    schema: tuple[SchemaColumn, ...],
    detached: tuple[DetachedLeaf, ...],
) -> tuple[tuple[SchemaColumn, ...], tuple[DetachedLeaf, ...], tuple[CoverageGap, ...]]:
    """Remove the detached leaves from the schema and check each against its parent.

    Returns the schema without the detached leaves, the detached leaves that
    the schema holds, and a gap for each leaf whose columns differ from its
    parent's (``tableoid`` aside). A catalog row whose table is gone is not
    a gap: the leaf catalog rows compare by their own rules.
    """
    present = {column.table for column in schema}
    leaves = tuple(leaf for leaf in detached if leaf.table in present)
    leaf_tables = {leaf.table for leaf in leaves}
    gaps: list[CoverageGap] = []
    for leaf in leaves:
        leaf_columns = {
            (column.column, column.column_type)
            for column in schema
            if column.table == leaf.table
        }
        parent_columns = {
            (column.column, column.column_type)
            for column in schema
            if column.table == leaf.parent and column.column != 'tableoid'
        }
        if leaf_columns != parent_columns:
            gaps.append(DetachedLeafShapeMismatch(table=leaf.table, parent=leaf.parent))
    remaining = tuple(column for column in schema if column.table not in leaf_tables)
    return remaining, leaves, tuple(gaps)


_INTEGER_TYPES: Final = frozenset(
    {DatabaseType.INT2, DatabaseType.INT4, DatabaseType.INT8}
)
_TEXT_JSON_TYPES: Final = frozenset(
    {DatabaseType.TEXT, DatabaseType.VARCHAR, DatabaseType.JSONB}
)
_NAME_TYPES: Final = frozenset(
    {DatabaseType.TEXT, DatabaseType.VARCHAR, DatabaseType.OID}
)


def rule_fits_type(rule: ColumnRule, column_type: DatabaseType) -> bool:
    """Whether a rule can read a column of this type."""
    match rule:
        case Exact() | Ignored():
            return True
        case NormalisedId():
            return column_type in _ID_TYPES
        case TimeOrderOnly() | TimeOrderBeforeRun() | TimeWithin():
            return column_type is DatabaseType.TIMESTAMPTZ
        case StoredJson(storage=storage):
            match storage:
                case JsonStorage.TEXT:
                    return column_type in _TEXT_JSON_TYPES
                case JsonStorage.UTF8_BYTES:
                    return column_type is DatabaseType.BYTEA
        case GeneratedName():
            return column_type in _NAME_TYPES
        case DigestOf():
            return column_type is DatabaseType.BYTEA


def _referenced_columns(rule: ColumnRule) -> list[tuple[str, frozenset[DatabaseType]]]:
    """The other columns a rule reads, with the types it accepts for each."""
    match rule:
        case TimeWithin(reference=AnchorReference(column=anchor)):
            return [(anchor, frozenset({DatabaseType.TIMESTAMPTZ}))]
        case StoredJson(format=FormatVersion(column=version_column)):
            return [(version_column, _INTEGER_TYPES)]
        case DigestOf(column=source):
            return [(source, frozenset({DatabaseType.BYTEA}))]
        case _:
            return []


def _referenced_gaps(
    table: str,
    column: str,
    rule: ColumnRule,
    types: dict[tuple[str, str], ColumnType],
) -> list[CoverageGap]:
    gaps: list[CoverageGap] = []
    for referenced, accepted in _referenced_columns(rule):
        referenced_type = types.get((table, referenced))
        match referenced_type:
            case None:
                # A missing column is reported as RuleWithoutColumn or as a
                # catalog defect, not twice.
                pass
            case DatabaseType() if referenced_type in accepted:
                pass
            case _:
                gaps.append(
                    ReferencedTypeMismatch(
                        table=table,
                        column=column,
                        referenced=referenced,
                        referenced_type=referenced_type,
                    ),
                )
    return gaps


def _type_gap(
    table: str, column: str, rule: ColumnRule, column_type: ColumnType
) -> list[CoverageGap]:
    match column_type:
        case UnknownType(name=name):
            return [UnsupportedColumnType(table=table, column=column, type_name=name)]
        case known if not rule_fits_type(rule, known):
            return [
                RuleTypeMismatch(
                    table=table, column=column, rule=rule, column_type=known
                )
            ]
        case _:
            return []


def check_coverage(
    schema: tuple[SchemaColumn, ...],
    catalog: SnapshotCatalog,
) -> tuple[CoverageGap, ...]:
    """Every gap between the live schema and the catalog. Pure; empty means full coverage."""
    types = {(column.table, column.column): column.column_type for column in schema}
    rules = {
        (table.name, spec.name): spec.rule
        for table in catalog.tables
        for spec in table.columns
    }
    gaps: list[CoverageGap] = []
    for column in schema:
        if (column.table, column.column) not in rules:
            gaps.append(UnclassifiedColumn(table=column.table, column=column.column))
    for (table, column), rule in rules.items():
        match types.get((table, column)):
            case None:
                gaps.append(RuleWithoutColumn(table=table, column=column))
            case column_type:
                gaps.extend(_type_gap(table, column, rule, column_type))
                gaps.extend(_referenced_gaps(table, column, rule, types))
    return tuple(gaps)
