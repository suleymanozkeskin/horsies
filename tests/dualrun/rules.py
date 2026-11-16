"""Column rules of the snapshot catalog, and the checks of a catalog's own shape.

Every captured column has exactly one rule. The rule decides how a stored
value becomes a normalized value, and how two normalized values compare.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum


class IdFamily(Enum):
    """Name space of a label. A label reads ``<family>#<ordinal>``."""

    TASK = 'task'
    WORKFLOW = 'workflow'
    WORKFLOW_NODE = 'workflow_node'
    WORKER = 'worker'
    COMMAND_FINGERPRINT = 'command_fingerprint'
    PARTITION_LEAF = 'partition_leaf'
    RECOVERY_PASS = 'recovery_pass'
    RECOVERY_CLAIM = 'recovery_claim'
    SCAN_POSITION = 'scan_position'


@dataclass(frozen=True, slots=True)
class Exact:
    """Compared as stored: the text form of the value under the capture session settings."""


@dataclass(frozen=True, slots=True)
class NormalisedId:
    """An opaque value replaced by a label.

    One raw value keeps one label in the whole snapshot, in every column
    that holds it. A new raw value gets the next ordinal of ``family``;
    order of first appearance is the catalog's table order, then the
    table's ``order_by`` order, then column order.
    """

    family: IdFamily


@dataclass(frozen=True, slots=True)
class TimeOrderOnly:
    """A clock value replaced by its dense rank among the non-null values of its column in its table."""


@dataclass(frozen=True, slots=True)
class RunWindowReference:
    """Clock values compared against the run window.

    A value inside [run start - bound, run end + bound] becomes the token
    ``DuringRun``. A value outside that range stays literal and compares
    exactly, so a fixed test time still compares exactly.
    """


@dataclass(frozen=True, slots=True)
class AnchorReference:
    """Clock values compared as an offset from another clock column of the same row.

    Two offsets are equal when they differ by at most the bound.
    """

    column: str


TimeReference = RunWindowReference | AnchorReference


@dataclass(frozen=True, slots=True)
class TimeWithin:
    """A clock value compared within ``bound`` of its reference."""

    reference: TimeReference
    bound: timedelta


@dataclass(frozen=True, slots=True)
class Ignored:
    """Not compared. ``reason`` states why the column carries no behaviour to compare."""

    reason: str


ColumnRule = Exact | NormalisedId | TimeOrderOnly | TimeWithin | Ignored


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One captured column and its rule."""

    name: str
    rule: ColumnRule


@dataclass(frozen=True, slots=True)
class TableRules:
    """The rules of one table.

    ``order_by``: columns whose normalized values order the rows before
    labels are assigned (the natural key). ``identity``: columns whose
    normalized values pair a reference row with a candidate row.
    """

    name: str
    columns: tuple[ColumnSpec, ...]
    order_by: tuple[str, ...]
    identity: tuple[str, ...]

    def position(self, column: str) -> int:
        """Index of ``column`` in ``columns``. The column must exist."""
        for index, spec in enumerate(self.columns):
            if spec.name == column:
                return index
        raise AssertionError(f'table {self.name} has no column {column!r} in its rules')


@dataclass(frozen=True, slots=True)
class SnapshotCatalog:
    """The rules of every captured table, in labelling order."""

    tables: tuple[TableRules, ...]


# ---- Catalog shape defects ----


class KeyRole(Enum):
    """Which key of a table a defect concerns."""

    ORDER_BY = 'order_by'
    IDENTITY = 'identity'


@dataclass(frozen=True, slots=True)
class DuplicateTable:
    """Two entries of the catalog name the same table."""

    table: str


@dataclass(frozen=True, slots=True)
class DuplicateColumn:
    """Two rules of one table name the same column."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class EmptyKey:
    """A table has no column in a key."""

    table: str
    role: KeyRole


@dataclass(frozen=True, slots=True)
class KeyColumnUnknown:
    """A key names a column that has no rule in its table."""

    table: str
    role: KeyRole
    column: str


@dataclass(frozen=True, slots=True)
class KeyColumnNotComparable:
    """A key names a column whose rule gives no value to order or pair by (ignored or anchored)."""

    table: str
    role: KeyRole
    column: str


@dataclass(frozen=True, slots=True)
class AnchorUnknown:
    """An anchored time rule names a column that has no rule in its table."""

    table: str
    column: str
    anchor: str


@dataclass(frozen=True, slots=True)
class EmptyIgnoreReason:
    """An ignored column has no written reason."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class NegativeBound:
    """A time rule has a bound below zero."""

    table: str
    column: str


CatalogDefect = (
    DuplicateTable
    | DuplicateColumn
    | EmptyKey
    | KeyColumnUnknown
    | KeyColumnNotComparable
    | AnchorUnknown
    | EmptyIgnoreReason
    | NegativeBound
)


def is_key_comparable(rule: ColumnRule) -> bool:
    """Whether a rule gives values that can order and pair rows by plain equality."""
    match rule:
        case Exact() | NormalisedId() | TimeOrderOnly():
            return True
        case TimeWithin(reference=reference):
            match reference:
                case RunWindowReference():
                    return True
                case AnchorReference():
                    return False
        case Ignored():
            return False


def _key_defects(
    table: TableRules, role: KeyRole, key: tuple[str, ...]
) -> list[CatalogDefect]:
    if not key:
        return [EmptyKey(table=table.name, role=role)]
    rules = {spec.name: spec.rule for spec in table.columns}
    defects: list[CatalogDefect] = []
    for column in key:
        match rules.get(column):
            case None:
                defects.append(
                    KeyColumnUnknown(table=table.name, role=role, column=column)
                )
            case rule if not is_key_comparable(rule):
                defects.append(
                    KeyColumnNotComparable(table=table.name, role=role, column=column)
                )
            case _:
                pass
    return defects


def _column_defects(
    table: TableRules, spec: ColumnSpec, names: set[str]
) -> list[CatalogDefect]:
    match spec.rule:
        case Ignored(reason=reason):
            if reason.strip():
                return []
            return [EmptyIgnoreReason(table=table.name, column=spec.name)]
        case TimeWithin(reference=reference, bound=bound):
            defects: list[CatalogDefect] = []
            if bound < timedelta(0):
                defects.append(NegativeBound(table=table.name, column=spec.name))
            match reference:
                case AnchorReference(column=anchor) if anchor not in names:
                    defects.append(
                        AnchorUnknown(table=table.name, column=spec.name, anchor=anchor)
                    )
                case AnchorReference() | RunWindowReference():
                    pass
            return defects
        case Exact() | NormalisedId() | TimeOrderOnly():
            return []


def catalog_defects(catalog: SnapshotCatalog) -> tuple[CatalogDefect, ...]:
    """Every shape defect of a catalog. Pure; an empty result means the catalog is usable."""
    defects: list[CatalogDefect] = []
    table_counts = Counter(table.name for table in catalog.tables)
    defects.extend(
        DuplicateTable(table=name) for name, count in table_counts.items() if count > 1
    )
    for table in catalog.tables:
        column_counts = Counter(spec.name for spec in table.columns)
        defects.extend(
            DuplicateColumn(table=table.name, column=name)
            for name, count in column_counts.items()
            if count > 1
        )
        names = set(column_counts)
        for spec in table.columns:
            defects.extend(_column_defects(table, spec, names))
        defects.extend(_key_defects(table, KeyRole.ORDER_BY, table.order_by))
        defects.extend(_key_defects(table, KeyRole.IDENTITY, table.identity))
    return tuple(defects)
