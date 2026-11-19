"""Two normalized snapshots to a typed diff. Pure: no I/O.

Rows pair by the normalized values of their table's ``identity``. A pair
of single rows compares column by column. An identity value that holds
several rows on either side compares as a multiset of whole rows, with
plain equality.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import Final

from crossclient.parity.snapshot.normalize import (
    PARTITION_TREE,
    NormalizedRow,
    NormalizedSnapshot,
    NormalizedTable,
)
from crossclient.parity.snapshot.rules import AnchorReference, ColumnRule, TimeWithin
from crossclient.parity.snapshot.values import (
    Cell,
    NormalizedValue,
    OffsetFromAnchor,
    OrderKeyCell,
    render_cells,
    render_value,
    sort_token,
)

DIFF_REPORT_ROW_LIMIT: Final = 50
"""Most diff entries that ``render_diff`` lists; it counts the rest."""


class Side(Enum):
    """Which snapshot of a compare."""

    REFERENCE = 'reference'
    CANDIDATE = 'candidate'


@dataclass(frozen=True, slots=True)
class AmbiguousRows:
    """Rows of one side that tie on ``order_by`` while their order decides labels."""

    side: Side
    table: str
    order_key: tuple[OrderKeyCell, ...]
    row_count: int


@dataclass(frozen=True, slots=True)
class MissingRow:
    """A reference row with no equal candidate row."""

    table: str
    identity: tuple[Cell, ...]
    cells: tuple[Cell, ...]


@dataclass(frozen=True, slots=True)
class ExtraRow:
    """A candidate row with no equal reference row."""

    table: str
    identity: tuple[Cell, ...]
    cells: tuple[Cell, ...]


@dataclass(frozen=True, slots=True)
class ColumnDifference:
    """One column of a paired row whose values differ under the column's rule."""

    table: str
    identity: tuple[Cell, ...]
    column: str
    reference: NormalizedValue
    candidate: NormalizedValue


DiffEntry = AmbiguousRows | MissingRow | ExtraRow | ColumnDifference


@dataclass(frozen=True, slots=True)
class SnapshotDiff:
    """Every difference between two snapshots. No entries means equal."""

    entries: tuple[DiffEntry, ...]


def values_match(
    rule: ColumnRule, reference: NormalizedValue, candidate: NormalizedValue
) -> bool:
    """Whether two values of one column are equal under the column's rule."""
    match rule, reference, candidate:
        case (
            TimeWithin(reference=AnchorReference(), bound=bound),
            OffsetFromAnchor(offset=reference_offset),
            OffsetFromAnchor(offset=candidate_offset),
        ):
            return abs(reference_offset - candidate_offset) <= bound
        case _:
            return reference == candidate


_Groups = dict[tuple[NormalizedValue, ...], list[NormalizedRow]]


def _groups(table: NormalizedTable) -> _Groups:
    positions = tuple(table.rules.position(column) for column in table.rules.identity)
    groups: _Groups = {}
    for row in table.rows:
        groups.setdefault(tuple(row[position] for position in positions), []).append(
            row
        )
    return groups


def _column_entries(
    table: NormalizedTable,
    reference: NormalizedRow,
    candidate: NormalizedRow,
) -> list[DiffEntry]:
    identity = table.identity(reference)
    return [
        ColumnDifference(
            table=table.rules.name,
            identity=identity,
            column=spec.name,
            reference=reference_value,
            candidate=candidate_value,
        )
        for spec, reference_value, candidate_value in zip(
            table.rules.columns, reference, candidate, strict=True
        )
        if not values_match(spec.rule, reference_value, candidate_value)
    ]


def _multiset_entries(
    table: NormalizedTable,
    reference: list[NormalizedRow],
    candidate: list[NormalizedRow],
) -> list[DiffEntry]:
    reference_counts = Counter(reference)
    candidate_counts = Counter(candidate)
    name = table.rules.name
    entries: list[DiffEntry] = [
        MissingRow(table=name, identity=table.identity(row), cells=table.cells(row))
        for row in (reference_counts - candidate_counts).elements()
    ]
    entries.extend(
        ExtraRow(table=name, identity=table.identity(row), cells=table.cells(row))
        for row in (candidate_counts - reference_counts).elements()
    )
    return entries


def _table_entries(
    reference: NormalizedTable, candidate: NormalizedTable
) -> list[DiffEntry]:
    reference_groups = _groups(reference)
    candidate_groups = _groups(candidate)
    keys = sorted(
        set(reference_groups) | set(candidate_groups),
        key=lambda key: tuple(sort_token(value) for value in key),
    )
    entries: list[DiffEntry] = []
    for key in keys:
        match reference_groups.get(key, []), candidate_groups.get(key, []):
            case [[reference_row], [candidate_row]]:
                entries.extend(_column_entries(reference, reference_row, candidate_row))
            case reference_rows, candidate_rows:
                entries.extend(
                    _multiset_entries(reference, reference_rows, candidate_rows)
                )
    return entries


def compare_snapshots(
    reference: NormalizedSnapshot, candidate: NormalizedSnapshot
) -> SnapshotDiff:
    """Every difference between two snapshots made with the same catalog (asserted).

    Ambiguous groups of either side come first, then row and column
    differences in catalog table order and identity order, then the
    differences of the partition tree (``PARTITION_TREE``).
    """
    reference_rules = tuple(table.rules for table in reference.tables)
    candidate_rules = tuple(table.rules for table in candidate.tables)
    assert (
        reference_rules == candidate_rules
    ), 'the two snapshots were made with different catalogs'
    entries: list[DiffEntry] = [
        AmbiguousRows(
            side=side,
            table=group.table,
            order_key=group.order_key,
            row_count=group.row_count,
        )
        for side, snapshot in ((Side.REFERENCE, reference), (Side.CANDIDATE, candidate))
        for group in snapshot.ambiguities
    ]
    for reference_table, candidate_table in zip(
        reference.tables, candidate.tables, strict=True
    ):
        entries.extend(_table_entries(reference_table, candidate_table))
    entries.extend(
        _table_entries(
            NormalizedTable(rules=PARTITION_TREE, rows=reference.partition_tree),
            NormalizedTable(rules=PARTITION_TREE, rows=candidate.partition_tree),
        )
    )
    return SnapshotDiff(entries=tuple(entries))


def render_entry(entry: DiffEntry) -> str:
    """One line of a diff report."""
    match entry:
        case AmbiguousRows(side=side, table=table, order_key=key, row_count=count):
            return (
                f'{table}: {count} rows of the {side.value} snapshot tie on order_by '
                f'({render_cells(key)}) and their order decides labels'
            )
        case MissingRow(table=table, identity=identity, cells=cells):
            return f'{table} [{render_cells(identity)}]: row only in the reference: {render_cells(cells)}'
        case ExtraRow(table=table, identity=identity, cells=cells):
            return f'{table} [{render_cells(identity)}]: row only in the candidate: {render_cells(cells)}'
        case ColumnDifference(
            table=table,
            identity=identity,
            column=column,
            reference=left,
            candidate=right,
        ):
            return (
                f'{table} [{render_cells(identity)}]: {column}: '
                f'reference {render_value(left)}, candidate {render_value(right)}'
            )


def render_diff(diff: SnapshotDiff, limit: int = DIFF_REPORT_ROW_LIMIT) -> str:
    """A report of at most ``limit`` entries, and the count of the rest."""
    match diff.entries:
        case ():
            return 'the snapshots are equal'
        case entries:
            lines = [render_entry(entry) for entry in entries[:limit]]
            rest = len(entries) - len(lines)
            if rest > 0:
                lines.append(f'... and {rest} more differences')
            return '\n'.join(lines)
