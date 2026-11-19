"""Raw captured rows to a normalized snapshot. Pure: no I/O.

Order of work:

1. Every cell of every table is normalized by its column rule
   (``cells.stable_rows``). Ids, also ids inside JSON documents, wait for
   labels.
2. Tables are labelled in catalog order. Inside a table, rows are sorted by
   the normalized values of ``order_by``; an id without a label yet sorts as
   ``UnlabelledId``.
3. Rows that tie on ``order_by`` are sorted by their references: for each
   id the row brings in, the rows elsewhere in the snapshot that hold the
   same id, normalized with that id marked as ``self``. This order uses
   only normalized facts, so both runs of one state get the same order.
4. Labels are assigned row by row, column by column, in document order
   inside a JSON document.
5. Rows that still tie (same ``order_by`` values, same references) and
   bring in new labels are checked: their order decides nothing when every
   order gives the same normalized snapshot. A new value that every row of
   the group holds is shared: it gets the same label in every order. The
   other new values of a row are its own values, and rows with the same own
   values are one block: for example the two status events of one workflow
   that no table holds. Another order of the rows only gives the own values
   of one block the labels of another block. That holds when no own value
   is in two blocks, the rows of a block hold the new values in one order,
   every block is equal to each other block apart from its new labels, and
   the rows elsewhere that hold the own values of one block equal those of
   each other block, with those values marked, and hold no id without a
   label and no own value of another block. Otherwise the group is reported
   as an ``AmbiguousGroup``. A label is never guessed.
"""

from __future__ import annotations

import itertools
from collections import Counter
from dataclasses import dataclass
from typing import Final

from horsies.core.types.result import Err, Ok, Result
from tests.parity.snapshot.cells import (
    NormalizeError,
    PendingDocument,
    PendingId,
    PendingLeaf,
    PendingText,
    RawRow,
    RawSnapshot,
    StableCell,
    StableRow,
    cell_ids,
    generated_name,
    stable_rows,
)
from tests.parity.snapshot.rules import (
    ColumnSpec,
    Exact,
    GeneratedName,
    IdFamily,
    SnapshotCatalog,
    TableRules,
    catalog_defects,
)
from tests.parity.snapshot.values import (
    Cell,
    ExactValue,
    NullValue,
    IdLabel,
    JsonDocument,
    LabelledText,
    NormalizedValue,
    OffsetFromAnchor,
    OrderKeyCell,
    OrderKeyValue,
    RenderedDocument,
    RunWindow,
    UnlabelledId,
    map_json,
    render_json,
    render_text,
    render_value,
    sort_token,
)

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
    """Rows that tie on ``order_by`` and on their references while their order decides labels."""

    table: str
    order_key: tuple[OrderKeyCell, ...]
    row_count: int


PARTITION_TREE: Final = TableRules(
    name='partition tree',
    columns=(
        ColumnSpec(name='parent', rule=Exact()),
        ColumnSpec(name='leaf', rule=GeneratedName()),
        ColumnSpec(name='detach_pending', rule=Exact()),
        ColumnSpec(name='bound', rule=Exact()),
    ),
    order_by=('parent', 'leaf'),
    identity=('parent', 'leaf'),
)
"""The rules of the partition tree section: one row per leaf that
``pg_inherits`` attaches to a partitioned ``horsies_`` parent. A leaf name
holds a day or an hour (``GeneratedName``); a bound is ``the catalog bounds``
when it equals the range of the leaf's catalog row (whose anchors compare by
their own rules), else its text (a class parent's list bound, an open
bound)."""


@dataclass(frozen=True, slots=True)
class NormalizedSnapshot:
    """A database state with ids, clock values and ignored columns normalized.

    ``partition_tree`` holds the rows of ``PARTITION_TREE``, in (parent, leaf)
    order.
    """

    tables: tuple[NormalizedTable, ...]
    ambiguities: tuple[AmbiguousGroup, ...]
    partition_tree: tuple[NormalizedRow, ...] = ()

    def table(self, name: str) -> NormalizedTable:
        """The table called ``name``. The table must be in the catalog."""
        for table in self.tables:
            if table.rules.name == name:
                return table
        raise AssertionError(f'the snapshot has no table {name!r}')


# ---- Labels ----

REFERENCE_CONTEXT_LIMIT: Final = 64
"""Most rows whose content orders a tie through one shared id. A value held
by more rows orders by its holder count only, which keeps the work per row
bounded; ties it cannot separate stay ambiguous."""

_Token = tuple[int, str]
_Context = tuple[_Token, ...]
_Signature = tuple[tuple[_Context, ...], ...]
_SELF: _Token = (20, 'self')
_ANY_OFFSET: _Token = (7, 'offset')
"""Offsets compare within a bound, so a reference order must not depend on their exact value."""


class _LabelBook:
    """One label per raw id value, numbered per family by first appearance."""

    def __init__(self, stable: list[list[StableRow]]) -> None:
        self._labels: dict[str, IdLabel] = {}
        self._issued: Counter[IdFamily] = Counter()
        self._holders: dict[str, list[tuple[int, int]]] = {}
        for table_index, rows in enumerate(stable):
            for row_index, row in enumerate(rows):
                for cell in row:
                    for pending in cell_ids(cell):
                        holders = self._holders.setdefault(pending.raw, [])
                        if not holders or holders[-1] != (table_index, row_index):
                            holders.append((table_index, row_index))

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

    def holders(self, raw: str) -> list[tuple[int, int]]:
        """Every (table index, row index) whose row holds ``raw`` in an id cell."""
        return self._holders.get(raw, [])


def _known(pending: PendingId, book: _LabelBook) -> OrderKeyValue:
    return book.lookup(pending.raw)


def _order_value(cell: StableCell, book: _LabelBook) -> OrderKeyValue:
    match cell:
        case PendingId():
            return _known(cell, book)
        case PendingDocument(root=root):
            return RenderedDocument(
                text=render_json(root, lambda leaf: _leaf_text(leaf, book))
            )
        case _:
            return cell


def _leaf_text(leaf: PendingLeaf, book: _LabelBook) -> str:
    match leaf:
        case PendingId():
            return render_value(_known(leaf, book))
        case PendingText(parts=parts):
            return render_text(
                parts, lambda pending: render_value(_known(pending, book))
            )
        case _:
            return render_value(leaf)


def _final_label(pending: PendingId, book: _LabelBook) -> IdLabel:
    match book.lookup(pending.raw):
        case IdLabel() as label:
            return label
        case UnlabelledId():
            raise AssertionError(
                f'id {pending.raw!r} has no label after its table was labelled'
            )


def _final_part(part: str | PendingId, book: _LabelBook) -> str | IdLabel:
    match part:
        case str():
            return part
        case PendingId():
            return _final_label(part, book)


def _final_leaf(leaf: PendingLeaf, book: _LabelBook) -> NormalizedValue:
    match leaf:
        case PendingId():
            return _final_label(leaf, book)
        case PendingText(parts=parts):
            return LabelledText(parts=tuple(_final_part(part, book) for part in parts))
        case _:
            return leaf


def _final_value(cell: StableCell, book: _LabelBook) -> NormalizedValue:
    match cell:
        case PendingId():
            return _final_leaf(cell, book)
        case PendingDocument(root=root):
            return JsonDocument(
                root=map_json(root, lambda leaf: _final_leaf(leaf, book))
            )
        case _:
            return cell


# ---- Masked tokens (reference order and tie check) ----


def _id_token(pending: PendingId, marks: dict[str, str], book: _LabelBook) -> str:
    match marks.get(pending.raw):
        case None:
            return render_value(_known(pending, book))
        case mark:
            return mark


def _leaf_token(leaf: PendingLeaf, marks: dict[str, str], book: _LabelBook) -> str:
    match leaf:
        case PendingId():
            return _id_token(leaf, marks, book)
        case PendingText(parts=parts):
            return render_text(parts, lambda pending: _id_token(pending, marks, book))
        case _:
            return render_value(leaf)


def _masked_token(cell: StableCell, marks: dict[str, str], book: _LabelBook) -> _Token:
    """A cell as a token: ids in ``marks`` as their mark, other ids as their label or ``<new id>``."""
    match cell:
        case PendingId(raw=raw) if raw in marks:
            return (21, marks[raw])
        case PendingId():
            return sort_token(_known(cell, book))
        case PendingDocument(root=root):
            return (12, render_json(root, lambda leaf: _leaf_token(leaf, marks, book)))
        case OffsetFromAnchor():
            return _ANY_OFFSET
        case _:
            return sort_token(cell)


def _signature(
    table_index: int, row_index: int, stable: list[list[StableRow]], book: _LabelBook
) -> _Signature:
    """For each id that the row brings in, the sorted rows elsewhere that hold it."""
    row = stable[table_index][row_index]
    new_values = [
        pending.raw
        for cell in row
        for pending in cell_ids(cell)
        if isinstance(book.lookup(pending.raw), UnlabelledId)
    ]
    signature: list[tuple[_Context, ...]] = []
    for raw in dict.fromkeys(new_values):
        others = [
            holder for holder in book.holders(raw) if holder != (table_index, row_index)
        ]
        if len(others) > REFERENCE_CONTEXT_LIMIT:
            signature.append((((-2, f'{len(others):09d} holders'),),))
            continue
        marks = {raw: _SELF[1]}
        contexts = [
            (
                (-1, f'{holder_table:06d}'),
                *(
                    _masked_token(cell, marks, book)
                    for cell in stable[holder_table][holder_row]
                ),
            )
            for holder_table, holder_row in others
        ]
        signature.append(tuple(sorted(contexts)))
    return tuple(signature)


def _refined_groups(
    table_index: int,
    order: list[int],
    tokens: list[tuple[_Token, ...]],
    stable: list[list[StableRow]],
    book: _LabelBook,
) -> list[list[int]]:
    """Rows in label order, grouped by equal order key and equal references."""
    groups: list[list[int]] = []
    for _, tied in itertools.groupby(order, key=lambda row: tokens[row]):
        members = list(tied)
        if len(members) == 1:
            groups.append(members)
            continue
        signatures = {
            row: _signature(table_index, row, stable, book) for row in members
        }
        members.sort(key=lambda row: signatures[row])
        groups.extend(
            list(group)
            for _, group in itertools.groupby(members, key=lambda row: signatures[row])
        )
    return groups


def _assign_labels(
    rows: list[StableRow], order: list[int], book: _LabelBook
) -> dict[int, tuple[str, ...]]:
    introduced: dict[int, tuple[str, ...]] = {}
    for row in order:
        new: list[str] = []
        for cell in rows[row]:
            for pending in cell_ids(cell):
                if isinstance(book.lookup(pending.raw), UnlabelledId):
                    book.introduce(pending.raw, pending.family)
                    new.append(pending.raw)
        introduced[row] = tuple(new)
    return introduced


_Block = tuple[int, ...]
"""Rows of a tie, by row index in their table, that hold the same own values."""

_Footprint = tuple[tuple[tuple[_Token, ...], ...], tuple[_Context, ...]]
"""A block of a tie as the rest of the snapshot sees it: its rows, masked
and sorted, and the rows elsewhere that hold its own values, masked and
sorted."""


@dataclass(frozen=True, slots=True)
class _OrderDependent:
    """The labels of a tie, or the order of rows elsewhere, could follow the
    order of the tie: the rows of one block hold the new values of the tie
    in other orders, or a row outside a block holds an own value of the
    block and also an own value of another block, or an id without a label."""


def _holds_only_known_ids(
    cells: StableRow, marks: dict[str, str], owned: set[str], book: _LabelBook
) -> bool:
    """Whether every id of ``cells`` is marked, or is not an own value of the
    tie (``owned``) and has a label."""
    for cell in cells:
        for pending in cell_ids(cell):
            match pending.raw:
                case raw if raw in marks:
                    pass
                case raw if raw in owned:
                    return False
                case raw if isinstance(book.lookup(raw), UnlabelledId):
                    return False
                case _:
                    pass
    return True


def _blocks(
    members: list[int], in_row: dict[int, Counter[str]], group_new: set[str]
) -> dict[frozenset[str], _Block]:
    """The rows of a tie, grouped by their own values.

    A shared value is a new value that every row of the tie holds. The own
    values of a row are the other new values that it holds. Rows with the
    same own values are one block. When two blocks hold one own value, a
    row of each block holds an own value of the other block that the
    footprint of the other block does not mark, so ``_footprint`` finds the
    tie order-dependent.
    """
    shared = {
        raw for raw in group_new if all(in_row[row][raw] > 0 for row in members)
    }
    grouped: dict[frozenset[str], list[int]] = {}
    for row in members:
        own = frozenset(raw for raw in group_new - shared if in_row[row][raw] > 0)
        grouped.setdefault(own, []).append(row)
    return {own: tuple(rows) for own, rows in grouped.items()}


def _new_value_order(cells: StableRow, group_new: set[str]) -> tuple[str, ...]:
    """The new values of the tie that ``cells`` holds, in first-appearance order."""
    return tuple(
        dict.fromkeys(
            pending.raw
            for cell in cells
            for pending in cell_ids(cell)
            if pending.raw in group_new
        )
    )


def _footprint(
    table_index: int,
    block: _Block,
    own: frozenset[str],
    group_new: set[str],
    owned: set[str],
    stable: list[list[StableRow]],
    book: _LabelBook,
) -> _Footprint | _OrderDependent:
    """A block of a tie as the rest of the snapshot sees it.

    Every row of the block must hold the new values of the tie in one order;
    each new value is marked by its position in that order. The footprint is
    the rows of the block with the new values marked, and the rows elsewhere
    that hold an own value of the block (``own``), marked the same way, each
    list sorted. Order-dependent when the rows of the block hold the new
    values in other orders, or a row elsewhere holds an id that is neither
    marked nor labelled, or an own value of another block (``owned``).
    """
    rows = stable[table_index]
    match list(dict.fromkeys(_new_value_order(rows[row], group_new) for row in block)):
        case [order]:
            marks = {raw: f'new-{position}' for position, raw in enumerate(order)}
        case _:
            return _OrderDependent()
    in_block = {(table_index, row) for row in block}
    holders = dict.fromkeys(
        holder
        for raw in sorted(own)
        for holder in book.holders(raw)
        if holder not in in_block
    )
    if len(holders) > REFERENCE_CONTEXT_LIMIT:
        return _OrderDependent()
    contexts: list[_Context] = []
    for holder_table, holder_row in holders:
        holder_cells = stable[holder_table][holder_row]
        if not _holds_only_known_ids(holder_cells, marks, owned, book):
            return _OrderDependent()
        contexts.append(
            (
                (-1, f'{holder_table:06d}'),
                *(_masked_token(cell, marks, book) for cell in holder_cells),
            )
        )
    masked = sorted(
        tuple(_masked_token(cell, marks, book) for cell in rows[row]) for row in block
    )
    return (tuple(masked), tuple(sorted(contexts)))


def _interchangeable(
    table_index: int,
    members: list[int],
    stable: list[list[StableRow]],
    introduced: dict[int, tuple[str, ...]],
    book: _LabelBook,
) -> bool:
    """Whether every order of the tied rows gives the same normalized snapshot.

    True when the rows bring in no new label. Otherwise the rows split into
    blocks by their own values (``_blocks``): a shared value gets the same
    label in every order, and another order of the rows gives the own values
    of each block the labels of another block. That changes nothing when
    every block has the same footprint (``_footprint``): the same rows apart
    from their new labels, and the same rows elsewhere that hold its own
    values. A block of one row is a row whose own values are private to it.
    A row outside a block that holds the own values of two blocks (also a
    row of another block, when two blocks hold one own value) or an id
    without a label makes the group order-dependent.
    """
    rows = stable[table_index]
    group_new = {raw for row in members for raw in introduced[row]}
    if not group_new:
        return True
    in_row = {
        row: Counter(pending.raw for cell in rows[row] for pending in cell_ids(cell))
        for row in members
    }
    blocks = _blocks(members, in_row, group_new)
    owned = {raw for own in blocks for raw in own}
    footprints = [
        _footprint(table_index, block, own, group_new, owned, stable, book)
        for own, block in blocks.items()
    ]
    match footprints[0]:
        case _OrderDependent():
            return False
        case first:
            return all(footprint == first for footprint in footprints)


def _label_table(
    table_index: int,
    table: TableRules,
    stable: list[list[StableRow]],
    book: _LabelBook,
) -> tuple[NormalizedTable, tuple[AmbiguousGroup, ...]]:
    rows = stable[table_index]
    positions = tuple(table.position(column) for column in table.order_by)
    order_keys = [
        tuple(_order_value(row[position], book) for position in positions)
        for row in rows
    ]
    tokens = [tuple(sort_token(value) for value in key) for key in order_keys]
    initial = sorted(range(len(rows)), key=lambda row: tokens[row])
    groups = _refined_groups(table_index, initial, tokens, stable, book)
    order = [row for group in groups for row in group]
    introduced = _assign_labels(rows, order, book)
    ambiguities = [
        AmbiguousGroup(
            table=table.name,
            order_key=tuple(
                OrderKeyCell(column=column, value=value)
                for column, value in zip(
                    table.order_by, order_keys[group[0]], strict=True
                )
            ),
            row_count=len(group),
        )
        for group in groups
        if len(group) > 1
        and not _interchangeable(table_index, group, stable, introduced, book)
    ]
    normalized = tuple(
        tuple(_final_value(cell, book) for cell in rows[row]) for row in order
    )
    return NormalizedTable(rules=table, rows=normalized), tuple(ambiguities)


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


def _partition_tree_row(row: RawRow, window: RunWindow) -> NormalizedRow:
    """One partition tree row by the rules of ``PARTITION_TREE``. Pure."""
    match row:
        case (str() as parent, str() as leaf, str() as detach_pending, bound):
            return (
                ExactValue(text=parent),
                generated_name(leaf, window),
                ExactValue(text=detach_pending),
                NullValue() if bound is None else ExactValue(text=bound),
            )
        case _:
            raise AssertionError(
                f'a partition tree row is {row!r}, not (parent, leaf, detach pending, bound)'
            )


def normalize_snapshot(
    raw: RawSnapshot,
    catalog: SnapshotCatalog,
    window: RunWindow,
) -> Result[NormalizedSnapshot, NormalizeError]:
    """Normalize a raw snapshot by ``catalog``.

    ``window`` is the run window that run-window clock rules and generated
    names refer to. Ties that decide labels are reported in
    ``ambiguities``, not as an error, so that a compare can report them
    with the rest. The raw snapshot must follow the catalog's tables and
    columns (asserted).
    """
    _assert_shape(raw, catalog)
    stable: list[list[StableRow]] = []
    for table, raw_table in zip(catalog.tables, raw.tables, strict=True):
        match stable_rows(table, raw_table.rows, window):
            case Err(error):
                return Err(error)
            case Ok(rows):
                stable.append(rows)
    book = _LabelBook(stable)
    tables: list[NormalizedTable] = []
    ambiguities: list[AmbiguousGroup] = []
    for table_index, table in enumerate(catalog.tables):
        normalized, groups = _label_table(table_index, table, stable, book)
        tables.append(normalized)
        ambiguities.extend(groups)
    return Ok(
        NormalizedSnapshot(
            tables=tuple(tables),
            ambiguities=tuple(ambiguities),
            partition_tree=tuple(
                _partition_tree_row(row, window) for row in raw.partition_tree
            ),
        )
    )
