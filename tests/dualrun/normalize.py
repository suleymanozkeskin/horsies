"""Raw captured rows to a normalized snapshot. Pure: no I/O.

Order of work:

1. Each column is normalized by its rule, except ids, which wait for labels.
2. Tables are labelled in catalog order. Inside a table, rows are sorted by
   the normalized values of ``order_by``; an id without a label yet sorts as
   ``UnlabelledId``. Labels are then assigned row by row, column by column.
3. Rows that tie on ``order_by`` and assign new labels are checked: the
   assignment is arbitrary unless the tied rows are equal apart from their
   new labels, and those labels occur nowhere else. Otherwise the tie is
   reported as an ``AmbiguousGroup``; the labels are never guessed.
"""

from __future__ import annotations

import itertools
from collections import Counter
from dataclasses import dataclass
from enum import Enum

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.rules import (
    AnchorReference,
    ColumnRule,
    Exact,
    IdFamily,
    Ignored,
    NormalisedId,
    RunWindowReference,
    SnapshotCatalog,
    TableRules,
    TimeOrderOnly,
    TimeWithin,
    catalog_defects,
)
from tests.dualrun.values import (
    Cell,
    DuringRun,
    ExactValue,
    FiniteTime,
    IdLabel,
    IgnoredValue,
    InfiniteFuture,
    InfinitePast,
    LiteralTime,
    NormalizedValue,
    NullValue,
    OffsetFromAnchor,
    OrderKeyCell,
    OrderKeyValue,
    RunWindow,
    TimeRank,
    TimeValue,
    UnlabelledId,
    parse_time_text,
    sort_token,
    time_sort_token,
)

# ---- Raw input (from the capture) ----

RawCell = str | None
"""The text form of one stored value; ``None`` is SQL NULL."""

RawRow = tuple[RawCell, ...]


@dataclass(frozen=True, slots=True)
class RawTable:
    """The rows of one table, columns in catalog order."""

    name: str
    rows: tuple[RawRow, ...]


@dataclass(frozen=True, slots=True)
class RawSnapshot:
    """The rows of every catalog table, tables in catalog order."""

    tables: tuple[RawTable, ...]


# ---- Errors ----


@dataclass(frozen=True, slots=True)
class UnreadableTime:
    """A clock column holds a text form the parser rejects."""

    table: str
    column: str
    text: str
    cause: str


class UnanchoredReason(Enum):
    """Why an anchored clock value has no offset."""

    ANCHOR_NULL = 'the anchor column is NULL while the value is set'
    ANCHOR_INFINITE = 'the anchor column is infinite'
    VALUE_INFINITE = 'the value is infinite'


@dataclass(frozen=True, slots=True)
class UnanchoredTime:
    """An anchored clock value whose offset from its anchor is not defined."""

    table: str
    column: str
    anchor: str
    reason: UnanchoredReason


NormalizeError = UnreadableTime | UnanchoredTime

# ---- Output ----

NormalizedRow = tuple[NormalizedValue, ...]


@dataclass(frozen=True, slots=True)
class NormalizedTable:
    """The normalized rows of one table, in label order."""

    rules: TableRules
    rows: tuple[NormalizedRow, ...]

    def cells(self, row: NormalizedRow) -> tuple[Cell, ...]:
        """Every value of ``row`` with its column name."""
        return tuple(
            Cell(column=spec.name, value=value)
            for spec, value in zip(self.rules.columns, row, strict=True)
        )

    def identity(self, row: NormalizedRow) -> tuple[Cell, ...]:
        """The identity values of ``row`` with their column names."""
        return tuple(
            Cell(column=column, value=row[self.rules.position(column)])
            for column in self.rules.identity
        )


@dataclass(frozen=True, slots=True)
class AmbiguousGroup:
    """Rows that tie on ``order_by`` while the order between them decides labels."""

    table: str
    order_key: tuple[OrderKeyCell, ...]
    row_count: int


@dataclass(frozen=True, slots=True)
class NormalizedSnapshot:
    """A database state with ids, clock values and ignored columns normalized."""

    tables: tuple[NormalizedTable, ...]
    ambiguities: tuple[AmbiguousGroup, ...]

    def table(self, name: str) -> NormalizedTable:
        """The table called ``name``. The table must be in the catalog."""
        for table in self.tables:
            if table.rules.name == name:
                return table
        raise AssertionError(f'the snapshot has no table {name!r}')


# ---- Labels ----


@dataclass(frozen=True, slots=True)
class _PendingId:
    """An id cell before labelling."""

    raw: str
    family: IdFamily


@dataclass(frozen=True, slots=True)
class _PrivateId:
    """In a tie check only: a label new in the tied group, numbered by first appearance in its row."""

    family: IdFamily
    local: int


_StableCell = NormalizedValue | _PendingId
_StableRow = tuple[_StableCell, ...]


class _LabelBook:
    """One label per raw id value, numbered per family by first appearance."""

    def __init__(self, occurrences: Counter[str]) -> None:
        self._labels: dict[str, IdLabel] = {}
        self._issued: Counter[IdFamily] = Counter()
        self._occurrences = occurrences

    def lookup(self, raw: str) -> IdLabel | UnlabelledId:
        match self._labels.get(raw):
            case None:
                return UnlabelledId()
            case label:
                return label

    def introduce(self, raw: str, family: IdFamily) -> None:
        assert raw not in self._labels, f'id {raw!r} is labelled twice'
        self._issued[family] += 1
        self._labels[raw] = IdLabel(family=family, ordinal=self._issued[family])

    def occurrences(self, raw: str) -> int:
        """How many cells of the whole snapshot hold ``raw`` under an id rule."""
        return self._occurrences[raw]


def _is_id_rule(rule: ColumnRule) -> bool:
    match rule:
        case NormalisedId():
            return True
        case Exact() | TimeOrderOnly() | TimeWithin() | Ignored():
            return False


def _id_occurrences(raw: RawSnapshot, catalog: SnapshotCatalog) -> Counter[str]:
    counts: Counter[str] = Counter()
    for table, raw_table in zip(catalog.tables, raw.tables, strict=True):
        positions = [
            index for index, spec in enumerate(table.columns) if _is_id_rule(spec.rule)
        ]
        for row in raw_table.rows:
            counts.update(
                value
                for value in (row[index] for index in positions)
                if value is not None
            )
    return counts


# ---- Column rules ----


def _parse_times(
    table: str, column: str, texts: list[RawCell]
) -> Result[list[TimeValue | None], UnreadableTime]:
    parsed: list[TimeValue | None] = []
    for text in texts:
        match text:
            case None:
                parsed.append(None)
            case str():
                match parse_time_text(text):
                    case Ok(value):
                        parsed.append(value)
                    case Err(rejected):
                        return Err(
                            UnreadableTime(
                                table=table,
                                column=column,
                                text=text,
                                cause=rejected.cause,
                            )
                        )
    return Ok(parsed)


def _ranked(times: list[TimeValue | None]) -> list[_StableCell]:
    tokens = sorted({time_sort_token(time) for time in times if time is not None})
    rank_of = {token: rank for rank, token in enumerate(tokens, start=1)}
    return [
        NullValue() if time is None else TimeRank(rank=rank_of[time_sort_token(time)])
        for time in times
    ]


def _windowed(
    times: list[TimeValue | None], window: RunWindow, rule: TimeWithin
) -> list[_StableCell]:
    low = window.start - rule.bound
    high = window.end + rule.bound
    cells: list[_StableCell] = []
    for time in times:
        match time:
            case None:
                cells.append(NullValue())
            case FiniteTime(at=at) if low <= at <= high:
                cells.append(DuringRun())
            case FiniteTime() | InfinitePast() | InfiniteFuture():
                cells.append(LiteralTime(time=time))
    return cells


def _offset(
    value: TimeValue | None, anchor: TimeValue | None
) -> NormalizedValue | UnanchoredReason:
    match value, anchor:
        case None, _:
            return NullValue()
        case InfinitePast() | InfiniteFuture(), _:
            return UnanchoredReason.VALUE_INFINITE
        case FiniteTime(), None:
            return UnanchoredReason.ANCHOR_NULL
        case FiniteTime(), InfinitePast() | InfiniteFuture():
            return UnanchoredReason.ANCHOR_INFINITE
        case FiniteTime(at=at), FiniteTime(at=anchor_at):
            return OffsetFromAnchor(offset=at - anchor_at)


def _anchored(
    table: TableRules,
    index: int,
    anchor: str,
    rows: tuple[RawRow, ...],
) -> Result[list[_StableCell], NormalizeError]:
    column = table.columns[index].name
    anchor_index = table.position(anchor)
    match _parse_times(table.name, column, [row[index] for row in rows]):
        case Err(error):
            return Err(error)
        case Ok(values):
            pass
    match _parse_times(table.name, anchor, [row[anchor_index] for row in rows]):
        case Err(error):
            return Err(error)
        case Ok(anchors):
            pass
    cells: list[_StableCell] = []
    for value, anchor_value in zip(values, anchors, strict=True):
        match _offset(value, anchor_value):
            case UnanchoredReason() as reason:
                return Err(
                    UnanchoredTime(
                        table=table.name, column=column, anchor=anchor, reason=reason
                    )
                )
            case normalized:
                cells.append(normalized)
    return Ok(cells)


def _exact_cell(text: RawCell) -> _StableCell:
    match text:
        case None:
            return NullValue()
        case str():
            return ExactValue(text=text)


def _ignored_cell() -> _StableCell:
    return IgnoredValue()


def _id_cell(text: RawCell, family: IdFamily) -> _StableCell:
    match text:
        case None:
            return NullValue()
        case str():
            return _PendingId(raw=text, family=family)


def _column_cells(
    table: TableRules,
    index: int,
    rows: tuple[RawRow, ...],
    window: RunWindow,
) -> Result[list[_StableCell], NormalizeError]:
    spec = table.columns[index]
    texts = [row[index] for row in rows]
    match spec.rule:
        case Exact():
            return Ok([_exact_cell(text) for text in texts])
        case NormalisedId(family=family):
            return Ok([_id_cell(text, family) for text in texts])
        case Ignored():
            return Ok([_ignored_cell() for _ in texts])
        case TimeOrderOnly():
            return _parse_times(table.name, spec.name, texts).map(_ranked)
        case TimeWithin(reference=reference) as rule:
            match reference:
                case AnchorReference(column=anchor):
                    return _anchored(table, index, anchor, rows)
                case RunWindowReference():
                    match _parse_times(table.name, spec.name, texts):
                        case Err(error):
                            return Err(error)
                        case Ok(times):
                            return Ok(_windowed(times, window, rule))


def _stable_rows(
    table: TableRules,
    rows: tuple[RawRow, ...],
    window: RunWindow,
) -> Result[list[_StableRow], NormalizeError]:
    columns: list[list[_StableCell]] = []
    for index in range(len(table.columns)):
        match _column_cells(table, index, rows, window):
            case Err(error):
                return Err(error)
            case Ok(cells):
                columns.append(cells)
    return Ok([tuple(column[row] for column in columns) for row in range(len(rows))])


# ---- Labelling and ties ----


def _order_value(cell: _StableCell, book: _LabelBook) -> OrderKeyValue:
    match cell:
        case _PendingId(raw=raw):
            return book.lookup(raw)
        case _:
            return cell


def _final_value(cell: _StableCell, book: _LabelBook) -> NormalizedValue:
    match cell:
        case _PendingId(raw=raw):
            match book.lookup(raw):
                case IdLabel() as label:
                    return label
                case UnlabelledId():
                    raise AssertionError(
                        f'id {raw!r} has no label after its table was labelled'
                    )
        case _:
            return cell


def _assign_labels(
    stable: list[_StableRow], order: list[int], book: _LabelBook
) -> dict[int, tuple[str, ...]]:
    introduced: dict[int, tuple[str, ...]] = {}
    for row in order:
        new: list[str] = []
        for cell in stable[row]:
            match cell:
                case _PendingId(raw=raw, family=family) if isinstance(
                    book.lookup(raw), UnlabelledId
                ):
                    book.introduce(raw, family)
                    new.append(raw)
                case _:
                    pass
        introduced[row] = tuple(new)
    return introduced


def _masked_row(
    cells: _StableRow, group_new: set[str], book: _LabelBook
) -> tuple[NormalizedValue | _PrivateId, ...]:
    local: dict[str, int] = {}
    masked: list[NormalizedValue | _PrivateId] = []
    for cell in cells:
        match cell:
            case _PendingId(raw=raw, family=family) if raw in group_new:
                masked.append(
                    _PrivateId(family=family, local=local.setdefault(raw, len(local)))
                )
            case _:
                masked.append(_final_value(cell, book))
    return tuple(masked)


def _interchangeable(
    members: list[int],
    stable: list[_StableRow],
    introduced: dict[int, tuple[str, ...]],
    book: _LabelBook,
) -> bool:
    """Whether every order of the tied rows gives the same normalized table.

    True when the rows assign no new label. Otherwise each new value must
    occur only inside the group, in one row or in every row, and the rows
    must be equal once each new value is replaced by its first position
    in its row.
    """
    group_new = {raw for row in members for raw in introduced[row]}
    if not group_new:
        return True
    in_row = {
        row: Counter(cell.raw for cell in stable[row] if isinstance(cell, _PendingId))
        for row in members
    }
    for raw in group_new:
        holders = [row for row in members if in_row[row][raw] > 0]
        inside = sum(in_row[row][raw] for row in holders)
        if book.occurrences(raw) != inside or len(holders) not in (1, len(members)):
            return False
    masked = [_masked_row(stable[row], group_new, book) for row in members]
    return all(row == masked[0] for row in masked)


def _normalize_table(
    table: TableRules,
    raw: RawTable,
    window: RunWindow,
    book: _LabelBook,
) -> Result[tuple[NormalizedTable, tuple[AmbiguousGroup, ...]], NormalizeError]:
    match _stable_rows(table, raw.rows, window):
        case Err(error):
            return Err(error)
        case Ok(stable):
            pass
    positions = tuple(table.position(column) for column in table.order_by)
    order_keys = [
        tuple(_order_value(row[position], book) for position in positions)
        for row in stable
    ]
    tokens = [tuple(sort_token(value) for value in key) for key in order_keys]
    order = sorted(range(len(stable)), key=lambda row: tokens[row])
    introduced = _assign_labels(stable, order, book)
    ambiguities: list[AmbiguousGroup] = []
    for _, group in itertools.groupby(order, key=lambda row: tokens[row]):
        members = list(group)
        if len(members) > 1 and not _interchangeable(members, stable, introduced, book):
            key = tuple(
                OrderKeyCell(column=column, value=value)
                for column, value in zip(
                    table.order_by, order_keys[members[0]], strict=True
                )
            )
            ambiguities.append(
                AmbiguousGroup(table=table.name, order_key=key, row_count=len(members))
            )
    rows = tuple(
        tuple(_final_value(cell, book) for cell in stable[row]) for row in order
    )
    return Ok((NormalizedTable(rules=table, rows=rows), tuple(ambiguities)))


def _assert_shape(raw: RawSnapshot, catalog: SnapshotCatalog) -> None:
    defects = catalog_defects(catalog)
    assert not defects, f'the snapshot catalog has defects: {defects}'
    names = tuple(table.name for table in catalog.tables)
    raw_names = tuple(table.name for table in raw.tables)
    assert (
        raw_names == names
    ), f'raw tables {raw_names} do not follow the catalog tables {names}'
    for table, raw_table in zip(catalog.tables, raw.tables, strict=True):
        width = len(table.columns)
        for row in raw_table.rows:
            assert (
                len(row) == width
            ), f'a raw row of {table.name} has {len(row)} cells, the rules have {width}'


def normalize_snapshot(
    raw: RawSnapshot,
    catalog: SnapshotCatalog,
    window: RunWindow,
) -> Result[NormalizedSnapshot, NormalizeError]:
    """Normalize a raw snapshot by ``catalog``.

    ``window`` is the run window that ``RunWindowReference`` rules compare
    against. Ties that decide labels are reported in ``ambiguities``, not
    as an error, so that a compare can report them with the rest.
    The raw snapshot must follow the catalog's tables and columns (asserted).
    """
    _assert_shape(raw, catalog)
    book = _LabelBook(_id_occurrences(raw, catalog))
    tables: list[NormalizedTable] = []
    ambiguities: list[AmbiguousGroup] = []
    for table, raw_table in zip(catalog.tables, raw.tables, strict=True):
        match _normalize_table(table, raw_table, window, book):
            case Err(error):
                return Err(error)
            case Ok((normalized, groups)):
                tables.append(normalized)
                ambiguities.extend(groups)
    return Ok(NormalizedSnapshot(tables=tuple(tables), ambiguities=tuple(ambiguities)))
