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
    Exact,
    Ignored,
    NormalisedId,
    RunWindowReference,
    SnapshotCatalog,
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
class AnchorTypeMismatch:
    """An anchored time rule whose anchor column is not a timestamptz column."""

    table: str
    column: str
    anchor: str
    anchor_type: ColumnType


CoverageGap = (
    UnclassifiedColumn
    | RuleWithoutColumn
    | UnsupportedColumnType
    | RuleTypeMismatch
    | AnchorTypeMismatch
)


def rule_fits_type(rule: ColumnRule, column_type: DatabaseType) -> bool:
    """Whether a rule can read a column of this type."""
    match rule:
        case Exact() | Ignored():
            return True
        case NormalisedId():
            return column_type in _ID_TYPES
        case TimeOrderOnly() | TimeWithin():
            return column_type is DatabaseType.TIMESTAMPTZ


def _anchor_gap(
    table: str,
    column: str,
    rule: ColumnRule,
    types: dict[tuple[str, str], ColumnType],
) -> list[CoverageGap]:
    match rule:
        case TimeWithin(reference=reference):
            pass
        case Exact() | NormalisedId() | TimeOrderOnly() | Ignored():
            return []
    match reference:
        case RunWindowReference():
            return []
        case AnchorReference(column=anchor):
            pass
    anchor_type = types.get((table, anchor))
    match anchor_type:
        case DatabaseType.TIMESTAMPTZ | None:
            # A missing anchor column is reported as RuleWithoutColumn or as a
            # catalog defect, not twice.
            return []
        case _:
            return [
                AnchorTypeMismatch(
                    table=table, column=column, anchor=anchor, anchor_type=anchor_type
                )
            ]


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
                gaps.extend(_anchor_gap(table, column, rule, types))
    return tuple(gaps)
