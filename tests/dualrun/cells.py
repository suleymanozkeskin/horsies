"""Column rules applied to raw text cells, before labels exist. Pure: no I/O.

``stable_rows`` normalizes every cell of a table by its column rule. An id
cell, and an id inside a JSON document, stays a ``PendingId`` until the
labelling step in ``normalize`` gives it a label.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Final, cast

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.rules import (
    CANONICAL_UUID,
    AnchorNull,
    AnchorReference,
    DigestOf,
    EachItem,
    EachMarkedMember,
    EachMember,
    Exact,
    FormatVersion,
    GeneratedName,
    IdFamily,
    IdsInText,
    Ignored,
    Index,
    JsonPath,
    JsonStorage,
    JsonTime,
    Key,
    NormalisedId,
    RunMeasure,
    RunMeasuresInText,
    RunWindowReference,
    StoredJson,
    TableRules,
    TimeEncoding,
    TimeOrderBeforeRun,
    TimeOrderOnly,
    TimeWithin,
    Unversioned,
)
from tests.dualrun.values import (
    CalendarUnit,
    DatedName,
    DigestMatches,
    DigestMismatch,
    DuringRun,
    ExactValue,
    FiniteTime,
    IgnoredValue,
    InfiniteFuture,
    InfinitePast,
    JsonArray,
    JsonLeaf,
    JsonNode,
    JsonObject,
    JsonScalar,
    LiteralTime,
    MeasuredText,
    MeasureToken,
    NormalizedValue,
    NullValue,
    OffsetFromAnchor,
    RunWindow,
    TimeRank,
    TimeValue,
    UnparsedDocument,
    json_leaves,
    map_json,
    parse_time_text,
    time_sort_token,
)

JSON_DEPTH_LIMIT: Final = 64
"""Deepest nesting of a stored JSON document that the snapshot reads."""

_UTC_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_DATED_NAME: Final = re.compile(
    r'(?P<prefix>.+)_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})(?:_(?P<hour>\d{2}))?',
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
    """The rows of every catalog table, tables in catalog order, and the
    attached leaves of every partitioned ``horsies_`` parent
    (``partition_tree``: parent, leaf, detach pending, bound; the columns of
    ``normalize.PARTITION_TREE``)."""

    tables: tuple[RawTable, ...]
    partition_tree: tuple[RawRow, ...] = ()


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


@dataclass(frozen=True, slots=True)
class UnreadableJson:
    """A stored JSON cell that is not its storage form (a bytea cell whose
    bytes are not UTF-8), a path value of the wrong kind, or a document
    nested deeper than ``JSON_DEPTH_LIMIT``. A text that is not JSON is not
    an error: it compares as its stored text (``UnparsedDocument``)."""

    table: str
    column: str
    cause: str


@dataclass(frozen=True, slots=True)
class UnsupportedStoredFormat:
    """The format version column of a stored document holds a version the rule does not read."""

    table: str
    column: str
    version_column: str
    found: str


@dataclass(frozen=True, slots=True)
class UnreadableBytes:
    """A bytea cell whose text is not the hex form ``\\x...``."""

    table: str
    column: str


NormalizeError = (
    UnreadableTime
    | UnanchoredTime
    | UnreadableJson
    | UnsupportedStoredFormat
    | UnreadableBytes
)

# ---- Cells before labelling ----


@dataclass(frozen=True, slots=True)
class PendingId:
    """An opaque value that waits for its label."""

    raw: str
    family: IdFamily


@dataclass(frozen=True, slots=True)
class PendingText:
    """A text of an ``IdsInText`` path whose ids wait for their labels.

    ``parts`` holds the literal text and the ids in text order.
    """

    parts: tuple[str | PendingId, ...]


PendingLeaf = NormalizedValue | PendingId | PendingText
"""A path-rule leaf of a document before labelling."""


@dataclass(frozen=True, slots=True)
class PendingDocument:
    """A JSON document whose id leaves wait for their labels."""

    root: JsonNode[PendingLeaf]


StableCell = NormalizedValue | PendingId | PendingDocument
StableRow = tuple[StableCell, ...]


def leaf_ids(leaf: PendingLeaf) -> list[PendingId]:
    """The ids of a document leaf that wait for labels, in text order."""
    match leaf:
        case PendingId():
            return [leaf]
        case PendingText(parts=parts):
            return [part for part in parts if isinstance(part, PendingId)]
        case _:
            return []


def cell_ids(cell: StableCell) -> list[PendingId]:
    """The ids of a cell that wait for labels, in document order."""
    match cell:
        case PendingId():
            return [cell]
        case PendingDocument(root=root):
            return [pending for leaf in json_leaves(root) for pending in leaf_ids(leaf)]
        case _:
            return []


def text_with_ids(text: str, family: IdFamily) -> PendingText:
    """``text`` split into its literal parts and the ids of ``family`` that
    ``CANONICAL_UUID`` finds in it. Empty literal parts are left out."""
    parts: list[str | PendingId] = []
    position = 0
    for found in CANONICAL_UUID.finditer(text):
        if found.start() > position:
            parts.append(text[position : found.start()])
        parts.append(PendingId(raw=found.group(), family=family))
        position = found.end()
    if position < len(text):
        parts.append(text[position:])
    return PendingText(parts=tuple(parts))


def _split_at_measure(
    text: str, pattern: re.Pattern[str], token: MeasureToken
) -> list[str | MeasureToken]:
    """``text`` with the group of each match of ``pattern`` replaced by ``token``."""
    pieces: list[str | MeasureToken] = []
    position = 0
    for found in pattern.finditer(text):
        start, end = found.span(1)
        assert start >= 0, f'the group of {pattern.pattern!r} did not take part in a match'
        pieces.append(text[position:start])
        pieces.append(token)
        position = end
    pieces.append(text[position:])
    return pieces


def measured_text(text: str, measures: tuple[RunMeasure, ...]) -> MeasuredText:
    """``text`` with the number of each match of each measure replaced by its
    token. Measures apply in order; each one reads only the literal text that
    the measures before it left. Empty literal parts are left out. Each
    pattern has one group (``catalog_defects`` checks it)."""
    parts: list[str | MeasureToken] = [text]
    for measure in measures:
        pattern = re.compile(measure.pattern)
        token = MeasureToken(name=measure.name)
        split: list[str | MeasureToken] = []
        for part in parts:
            match part:
                case MeasureToken():
                    split.append(part)
                case str():
                    split.extend(_split_at_measure(part, pattern, token))
        parts = split
    return MeasuredText(parts=tuple(part for part in parts if part != ''))


# ---- Clock columns ----


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


def _rank_map(times: list[TimeValue]) -> dict[tuple[int, str], int]:
    tokens = sorted({time_sort_token(time) for time in times})
    return {token: rank for rank, token in enumerate(tokens, start=1)}


def _ranked(times: list[TimeValue | None]) -> list[StableCell]:
    rank_of = _rank_map([time for time in times if time is not None])
    return [
        NullValue() if time is None else TimeRank(rank=rank_of[time_sort_token(time)])
        for time in times
    ]


def _written_during_run(time: TimeValue, window: RunWindow) -> bool:
    """Whether ``time`` is a finite value at or after the run start."""
    match time:
        case FiniteTime(at=at):
            return at >= window.start
        case InfinitePast() | InfiniteFuture():
            return False


def _ranked_before_run(
    times: list[TimeValue | None], window: RunWindow
) -> list[StableCell]:
    """``TimeOrderBeforeRun``: ``DuringRun`` from the run start on; the
    values before it ranked among themselves."""
    before = [
        time
        for time in times
        if time is not None and not _written_during_run(time, window)
    ]
    rank_of = _rank_map(before)
    cells: list[StableCell] = []
    for time in times:
        match time:
            case None:
                cells.append(NullValue())
            case _ if _written_during_run(time, window):
                cells.append(DuringRun())
            case _:
                cells.append(TimeRank(rank=rank_of[time_sort_token(time)]))
    return cells


def _windowed_value(
    time: TimeValue, window: RunWindow, bound: timedelta
) -> NormalizedValue:
    match time:
        case FiniteTime(at=at) if window.start - bound <= at <= window.end + bound:
            return DuringRun()
        case FiniteTime() | InfinitePast() | InfiniteFuture():
            return LiteralTime(time=time)


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


def _ranked_without_anchor(
    values: list[TimeValue | None], anchors: list[TimeValue | None]
) -> dict[int, StableCell]:
    """``AnchorNull.RANKED``: the rank of each set value whose anchor is NULL,
    among those values, by row position."""
    positions = [
        position
        for position, (value, anchor) in enumerate(zip(values, anchors, strict=True))
        if value is not None and anchor is None
    ]
    ranked = _ranked([values[position] for position in positions])
    return dict(zip(positions, ranked, strict=True))


def _anchored(
    table: TableRules,
    index: int,
    reference: AnchorReference,
    rows: tuple[RawRow, ...],
) -> Result[list[StableCell], NormalizeError]:
    column = table.columns[index].name
    anchor = reference.column
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
    match reference.when_anchor_null:
        case AnchorNull.RANKED:
            without_anchor = _ranked_without_anchor(values, anchors)
        case AnchorNull.REFUSED:
            without_anchor = {}
    cells: list[StableCell] = []
    for position, (value, anchor_value) in enumerate(zip(values, anchors, strict=True)):
        match without_anchor.get(position), _offset(value, anchor_value):
            case (None, UnanchoredReason() as reason):
                return Err(
                    UnanchoredTime(
                        table=table.name, column=column, anchor=anchor, reason=reason
                    )
                )
            case (None, normalized):
                cells.append(normalized)
            case (ranked, _):
                cells.append(ranked)
    return Ok(cells)


# ---- Generated names and digests ----


def _floor(at: datetime, unit: CalendarUnit) -> datetime:
    utc = at.astimezone(UTC)
    match unit:
        case CalendarUnit.DAY:
            return utc.replace(hour=0, minute=0, second=0, microsecond=0)
        case CalendarUnit.HOUR:
            return utc.replace(minute=0, second=0, microsecond=0)


def generated_name(text: str, window: RunWindow) -> NormalizedValue:
    """A ``DatedName`` when ``text`` ends in a valid UTC date or hour, else the exact text."""
    match _DATED_NAME.fullmatch(text):
        case None:
            return ExactValue(text=text)
        case found:
            pass
    hour = found.group('hour')
    unit = CalendarUnit.DAY if hour is None else CalendarUnit.HOUR
    try:
        stamp = datetime(
            int(found.group('year')),
            int(found.group('month')),
            int(found.group('day')),
            0 if hour is None else int(hour),
            tzinfo=UTC,
        )
    except ValueError:
        return ExactValue(text=text)
    step = timedelta(days=1) if unit is CalendarUnit.DAY else timedelta(hours=1)
    offset = (stamp - _floor(window.start, unit)) // step
    return DatedName(prefix=found.group('prefix'), unit=unit, offset=offset)


def _bytes(text: str) -> bytes | None:
    if not text.startswith('\\x'):
        return None
    try:
        return bytes.fromhex(text[2:])
    except ValueError:
        return None


def _digest_cells(
    table: TableRules, index: int, source: str, rows: tuple[RawRow, ...]
) -> Result[list[StableCell], NormalizeError]:
    column = table.columns[index].name
    source_index = table.position(source)
    cells: list[StableCell] = []
    for row in rows:
        match row[index], row[source_index]:
            case None, _:
                cells.append(NullValue())
            case str(), None:
                cells.append(DigestMismatch())
            case str() as digest_text, str() as source_text:
                digest = _bytes(digest_text)
                payload = _bytes(source_text)
                if digest is None:
                    return Err(UnreadableBytes(table=table.name, column=column))
                if payload is None:
                    return Err(UnreadableBytes(table=table.name, column=source))
                matches = hashlib.sha256(payload).digest() == digest
                cells.append(DigestMatches() if matches else DigestMismatch())
    return Ok(cells)


# ---- Stored JSON ----


@dataclass(frozen=True, slots=True)
class _ToRank:
    """A JSON clock value under ``TimeOrderOnly``, ranked once the whole column is read."""

    path: int
    time: TimeValue


_Building = PendingLeaf | _ToRank
_Alive = tuple[tuple[int, int], ...]
"""Path rules that still match the current location: (rule index, steps matched)."""


def _building_leaf(value: _Building) -> JsonNode[_Building]:
    return JsonLeaf(value=value)


def _reject_constant(name: str) -> object:
    raise ValueError(f'{name} is not valid JSON')


def _scalar_text(value: None | bool | int | float | str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _holds_marker(value: object, marker: str) -> bool:
    """Whether ``value`` is an object that holds ``marker`` with the value ``true``."""
    match value:
        case dict():
            return cast(dict[str, object], value).get(marker) is True
        case _:
            return False


def _advance(
    alive: _Alive, paths: tuple[JsonPath, ...], key: str | int, value: object
) -> _Alive:
    """The path rules that still match at the member or item ``key``, whose value is ``value``."""
    moved: list[tuple[int, int]] = []
    for path_index, matched in alive:
        steps = paths[path_index].steps
        if matched >= len(steps):
            continue
        match steps[matched], key:
            case Key(name=name), str() if name == key:
                moved.append((path_index, matched + 1))
            case Index(position=position), int() if position == key:
                moved.append((path_index, matched + 1))
            case EachItem(), int():
                moved.append((path_index, matched + 1))
            case EachMember(), str():
                moved.append((path_index, matched + 1))
            case EachMarkedMember(marker=marker), str() if _holds_marker(value, marker):
                moved.append((path_index, matched + 1))
            case _:
                pass
    return tuple(moved)


def _json_time(value: object, encoding: TimeEncoding) -> TimeValue | str:
    match encoding, value:
        case TimeEncoding.ISO_TEXT, str():
            match parse_time_text(value):
                case Ok(time):
                    return time
                case Err(rejected):
                    return rejected.cause
        case TimeEncoding.EPOCH_MICROSECONDS, int() if not isinstance(value, bool):
            try:
                return FiniteTime(at=_UTC_EPOCH + timedelta(microseconds=value))
            except OverflowError:
                return 'epoch microseconds out of range'
        case _:
            return f'{value!r} is not {encoding.value}'


def _leaf(
    value: object, path_index: int, path: JsonPath, window: RunWindow
) -> Result[JsonNode[_Building], str]:
    if value is None:
        return Ok(_building_leaf(NullValue()))
    match path.rule:
        case Ignored():
            return Ok(_building_leaf(IgnoredValue()))
        case NormalisedId(family=family):
            match value:
                case str():
                    return Ok(_building_leaf(PendingId(raw=value, family=family)))
                case _:
                    return Err(f'id path {path.steps} holds {value!r}, not text')
        case IdsInText(family=family):
            match value:
                case str():
                    return Ok(_building_leaf(text_with_ids(value, family)))
                case _:
                    return Err(f'text path {path.steps} holds {value!r}, not text')
        case RunMeasuresInText(measures=measures):
            match value:
                case str():
                    return Ok(_building_leaf(measured_text(value, measures)))
                case _:
                    return Err(f'text path {path.steps} holds {value!r}, not text')
        case JsonTime(encoding=encoding, rule=time_rule):
            match _json_time(value, encoding):
                case str() as cause:
                    return Err(f'clock path {path.steps}: {cause}')
                case time:
                    pass
            match time_rule:
                case TimeOrderOnly():
                    return Ok(_building_leaf(_ToRank(path=path_index, time=time)))
                case TimeWithin(bound=bound):
                    return Ok(_building_leaf(_windowed_value(time, window, bound)))


def _node(
    value: object, alive: _Alive, rule: StoredJson, window: RunWindow, depth: int
) -> Result[JsonNode[_Building], str]:
    if depth > JSON_DEPTH_LIMIT:
        return Err(f'nesting deeper than {JSON_DEPTH_LIMIT}')
    for path_index, matched in alive:
        if matched == len(rule.paths[path_index].steps):
            return _leaf(value, path_index, rule.paths[path_index], window)
    match value:
        case None | bool() | int() | float() | str():
            return Ok(JsonScalar(text=_scalar_text(value)))
        case list():
            items: list[JsonNode[_Building]] = []
            for position, item in enumerate(cast(list[object], value)):
                match _node(
                    item,
                    _advance(alive, rule.paths, position, item),
                    rule,
                    window,
                    depth + 1,
                ):
                    case Err(cause):
                        return Err(cause)
                    case Ok(node):
                        items.append(node)
            return Ok(JsonArray(items=tuple(items)))
        case dict():
            members: list[tuple[str, JsonNode[_Building]]] = []
            for key, member in sorted(cast(dict[str, object], value).items()):
                match _node(
                    member,
                    _advance(alive, rule.paths, key, member),
                    rule,
                    window,
                    depth + 1,
                ):
                    case Err(cause):
                        return Err(cause)
                    case Ok(node):
                        members.append((key, node))
            return Ok(JsonObject(members=tuple(members)))
        case _:
            raise AssertionError(f'json.loads returned {type(value).__name__}')


def _document_text(text: str, storage: JsonStorage) -> str | None:
    match storage:
        case JsonStorage.TEXT:
            return text
        case JsonStorage.UTF8_BYTES:
            payload = _bytes(text)
            if payload is None:
                return None
            try:
                return payload.decode('utf-8')
            except UnicodeDecodeError:
                return None


def _parse_document(
    text: str, rule: StoredJson, window: RunWindow
) -> Result[JsonNode[_Building] | UnparsedDocument, str]:
    """The document of a cell, or ``UnparsedDocument`` when its text is not
    JSON (``json.loads`` refuses it; ``NaN`` and ``Infinity`` are refused).
    A cell that is not the storage form, a path value of the wrong kind and a
    nesting deeper than ``JSON_DEPTH_LIMIT`` are errors."""
    match _document_text(text, rule.storage):
        case None:
            return Err(f'the cell is not {rule.storage.value}')
        case document:
            pass
    try:
        parsed: object = json.loads(document, parse_constant=_reject_constant)
    except ValueError:
        return Ok(UnparsedDocument(text=document))
    except RecursionError as exc:
        return Err(f'{type(exc).__name__}: {exc}')
    root: _Alive = tuple((index, 0) for index in range(len(rule.paths)))
    return _node(parsed, root, rule, window, 0)


def _format_error(
    table: TableRules, row: RawRow, column: str, rule: StoredJson
) -> UnsupportedStoredFormat | None:
    match rule.format:
        case Unversioned():
            return None
        case FormatVersion(column=version_column, version=version):
            found = row[table.position(version_column)]
            if found == str(version):
                return None
            return UnsupportedStoredFormat(
                table=table.name,
                column=column,
                version_column=version_column,
                found='NULL' if found is None else found,
            )


_StoredDocument = JsonNode[_Building] | UnparsedDocument | NullValue
"""A stored JSON cell after its parse: a document, a text that is not JSON, or SQL NULL."""


def _ranked_documents(documents: list[_StoredDocument]) -> list[StableCell]:
    by_path: dict[int, list[TimeValue]] = {}
    for document in documents:
        match document:
            case UnparsedDocument() | NullValue():
                pass
            case _:
                for leaf in json_leaves(document):
                    if isinstance(leaf, _ToRank):
                        by_path.setdefault(leaf.path, []).append(leaf.time)
    ranks = {path: _rank_map(times) for path, times in by_path.items()}

    def settle(leaf: _Building) -> PendingLeaf:
        match leaf:
            case _ToRank(path=path, time=time):
                return TimeRank(rank=ranks[path][time_sort_token(time)])
            case _:
                return leaf

    def settled(document: _StoredDocument) -> StableCell:
        match document:
            case UnparsedDocument() | NullValue():
                return document
            case _:
                return PendingDocument(root=map_json(document, settle))

    return [settled(document) for document in documents]


def _json_cells(
    table: TableRules,
    index: int,
    rule: StoredJson,
    rows: tuple[RawRow, ...],
    window: RunWindow,
) -> Result[list[StableCell], NormalizeError]:
    column = table.columns[index].name
    documents: list[_StoredDocument] = []
    for row in rows:
        match row[index]:
            case None:
                documents.append(NullValue())
                continue
            case text:
                pass
        unsupported = _format_error(table, row, column, rule)
        if unsupported is not None:
            return Err(unsupported)
        match _parse_document(text, rule, window):
            case Err(cause):
                return Err(UnreadableJson(table=table.name, column=column, cause=cause))
            case Ok(document):
                documents.append(document)
    return Ok(_ranked_documents(documents))


# ---- Columns and rows ----


def _each(
    texts: list[RawCell], change: Callable[[str], StableCell]
) -> list[StableCell]:
    return [NullValue() if text is None else change(text) for text in texts]


def _ignored() -> StableCell:
    return IgnoredValue()


def _window_cell(
    time: TimeValue | None, window: RunWindow, bound: timedelta
) -> StableCell:
    match time:
        case None:
            return NullValue()
        case _:
            return _windowed_value(time, window, bound)


def _column_cells(
    table: TableRules, index: int, rows: tuple[RawRow, ...], window: RunWindow
) -> Result[list[StableCell], NormalizeError]:
    spec = table.columns[index]
    texts = [row[index] for row in rows]
    match spec.rule:
        case Exact():
            return Ok(_each(texts, lambda text: ExactValue(text=text)))
        case NormalisedId(family=family):
            return Ok(_each(texts, lambda text: PendingId(raw=text, family=family)))
        case Ignored():
            return Ok([_ignored() for _ in texts])
        case GeneratedName():
            return Ok(_each(texts, lambda text: generated_name(text, window)))
        case DigestOf(column=source):
            return _digest_cells(table, index, source, rows)
        case StoredJson() as rule:
            return _json_cells(table, index, rule, rows, window)
        case TimeOrderOnly():
            return _parse_times(table.name, spec.name, texts).map(_ranked)
        case TimeOrderBeforeRun():
            match _parse_times(table.name, spec.name, texts):
                case Err(error):
                    return Err(error)
                case Ok(times):
                    return Ok(_ranked_before_run(times, window))
        case TimeWithin(reference=reference, bound=bound):
            match reference:
                case AnchorReference() as anchored:
                    return _anchored(table, index, anchored, rows)
                case RunWindowReference():
                    match _parse_times(table.name, spec.name, texts):
                        case Err(error):
                            return Err(error)
                        case Ok(times):
                            return Ok(
                                [_window_cell(time, window, bound) for time in times]
                            )


def stable_rows(
    table: TableRules, rows: tuple[RawRow, ...], window: RunWindow
) -> Result[list[StableRow], NormalizeError]:
    """Every cell of ``rows`` normalized by its column rule; ids wait for labels."""
    columns: list[list[StableCell]] = []
    for index in range(len(table.columns)):
        match _column_cells(table, index, rows, window):
            case Err(error):
                return Err(error)
            case Ok(cells):
                columns.append(cells)
    return Ok([tuple(column[row] for column in columns) for row in range(len(rows))])
