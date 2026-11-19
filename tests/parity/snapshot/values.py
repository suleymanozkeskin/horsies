"""Normalized cell values, clock values, and the run window."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Final

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.rules import IdFamily

_MICROSECOND: Final = timedelta(microseconds=1)
_OFFSET_SHIFT: Final = 10**9
"""Added to a name offset so that its zero-padded text sorts in numeric order."""

# ---- Clock values (timestamptz) ----


@dataclass(frozen=True, slots=True)
class FiniteTime:
    """A timestamptz value with a date and time. ``at`` is timezone-aware."""

    at: datetime


@dataclass(frozen=True, slots=True)
class InfinitePast:
    """The timestamptz value ``-infinity``."""


@dataclass(frozen=True, slots=True)
class InfiniteFuture:
    """The timestamptz value ``infinity``."""


TimeValue = FiniteTime | InfinitePast | InfiniteFuture


@dataclass(frozen=True, slots=True)
class TimeTextRejected:
    """A timestamptz text form that the parser does not accept."""

    text: str
    cause: str


def parse_time_text(text: str) -> Result[TimeValue, TimeTextRejected]:
    """Parse the ISO text form of a timestamptz (DateStyle ISO).

    Accepts ``infinity``, ``-infinity`` and ``YYYY-MM-DD HH:MM:SS[.ffffff]+HH[:MM]``.
    Rejects a value without an offset, and years outside 1..9999 (BC dates).
    """
    match text:
        case 'infinity':
            return Ok(InfiniteFuture())
        case '-infinity':
            return Ok(InfinitePast())
        case _:
            pass
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        return Err(TimeTextRejected(text=text, cause=str(exc)))
    if parsed.tzinfo is None:
        return Err(TimeTextRejected(text=text, cause='the value has no UTC offset'))
    return Ok(FiniteTime(at=parsed))


def time_sort_token(value: TimeValue) -> tuple[int, str]:
    """A token whose order is the order of time: -infinity, finite values, infinity."""
    match value:
        case InfinitePast():
            return (0, '')
        case FiniteTime(at=at):
            return (1, at.astimezone(UTC).isoformat(timespec='microseconds'))
        case InfiniteFuture():
            return (2, '')


def render_time(value: TimeValue) -> str:
    """The time in UTC, or ``-infinity`` / ``infinity``."""
    match value:
        case InfinitePast():
            return '-infinity'
        case FiniteTime(at=at):
            return at.astimezone(UTC).isoformat(timespec='microseconds')
        case InfiniteFuture():
            return 'infinity'


# ---- Run window ----


@dataclass(frozen=True, slots=True)
class RunWindow:
    """The database clock interval of one run. ``make_run_window`` builds it from two checked readings."""

    start: datetime
    end: datetime


class RunWindowFault(Enum):
    """Why two clock readings do not form a run window."""

    NAIVE_TIME = 'a reading has no timezone'
    END_BEFORE_START = 'the end reading is before the start reading'


@dataclass(frozen=True, slots=True)
class RunWindowInvalid:
    """Two clock readings that do not form a run window."""

    start: datetime
    end: datetime
    fault: RunWindowFault


def make_run_window(
    start: datetime, end: datetime
) -> Result[RunWindow, RunWindowInvalid]:
    """A run window from two timezone-aware readings with ``start <= end``."""
    if start.tzinfo is None or end.tzinfo is None:
        return Err(
            RunWindowInvalid(start=start, end=end, fault=RunWindowFault.NAIVE_TIME)
        )
    if end < start:
        return Err(
            RunWindowInvalid(
                start=start, end=end, fault=RunWindowFault.END_BEFORE_START
            )
        )
    return Ok(RunWindow(start=start, end=end))


# ---- Normalized values ----


@dataclass(frozen=True, slots=True)
class NullValue:
    """SQL NULL, under any rule except ``Ignored``."""


@dataclass(frozen=True, slots=True)
class ExactValue:
    """The stored value as text."""

    text: str


@dataclass(frozen=True, slots=True)
class IdLabel:
    """The label of an opaque value. ``family`` is the family of the column where the value appeared first."""

    family: IdFamily
    ordinal: int


@dataclass(frozen=True, slots=True)
class TimeRank:
    """Dense rank of a clock value in its column, from 1."""

    rank: int


@dataclass(frozen=True, slots=True)
class DuringRun:
    """A clock value inside the run window, widened by the rule's bound."""


@dataclass(frozen=True, slots=True)
class LiteralTime:
    """A clock value outside the run window, compared exactly."""

    time: TimeValue


@dataclass(frozen=True, slots=True)
class OffsetFromAnchor:
    """A clock value minus its anchor column's value."""

    offset: timedelta


@dataclass(frozen=True, slots=True)
class IgnoredValue:
    """A value of an ignored column."""


class CalendarUnit(Enum):
    """The unit of the date or hour in a generated name."""

    DAY = 'day'
    HOUR = 'hour'


@dataclass(frozen=True, slots=True)
class DatedName:
    """A generated name whose date or hour is replaced by an offset from the run start.

    ``offset`` counts ``unit`` steps from the start of the day or hour that
    holds the run window's start, in UTC.
    """

    prefix: str
    unit: CalendarUnit
    offset: int


@dataclass(frozen=True, slots=True)
class DigestMatches:
    """A digest column equals SHA-256 of its source column."""


@dataclass(frozen=True, slots=True)
class DigestMismatch:
    """A digest column differs from SHA-256 of its source column."""


# ---- JSON documents ----


@dataclass(frozen=True, slots=True)
class JsonScalar:
    """A JSON null, boolean, number or string, as canonical JSON text."""

    text: str


@dataclass(frozen=True, slots=True)
class JsonArray[L]:
    """A JSON array."""

    items: tuple[JsonNode[L], ...]


@dataclass(frozen=True, slots=True)
class JsonObject[L]:
    """A JSON object, members sorted by key."""

    members: tuple[tuple[str, JsonNode[L]], ...]


@dataclass(frozen=True, slots=True)
class JsonLeaf[L]:
    """A value that a path rule normalized."""

    value: L


type JsonNode[L] = JsonScalar | JsonArray[L] | JsonObject[L] | JsonLeaf[L]


def map_json[A, B](node: JsonNode[A], change: Callable[[A], B]) -> JsonNode[B]:
    """The same tree with every path-rule leaf changed. Depth is bounded by the parser."""
    match node:
        case JsonScalar():
            return node
        case JsonArray(items=items):
            return JsonArray(items=tuple(map_json(item, change) for item in items))
        case JsonObject(members=members):
            return JsonObject(
                members=tuple((key, map_json(value, change)) for key, value in members),
            )
        case JsonLeaf(value=value):
            return JsonLeaf(value=change(value))


def json_leaves[L](node: JsonNode[L]) -> list[L]:
    """Every path-rule leaf in document order (array order, sorted keys)."""
    match node:
        case JsonScalar():
            return []
        case JsonArray(items=items):
            return [leaf for item in items for leaf in json_leaves(item)]
        case JsonObject(members=members):
            return [leaf for _, value in members for leaf in json_leaves(value)]
        case JsonLeaf(value=value):
            return [value]


def render_json[L](node: JsonNode[L], render_leaf: Callable[[L], str]) -> str:
    """Canonical text of a tree: compact, sorted keys, leaves as ``render_leaf`` gives them."""
    match node:
        case JsonScalar(text=text):
            return text
        case JsonArray(items=items):
            return (
                '[' + ','.join(render_json(item, render_leaf) for item in items) + ']'
            )
        case JsonObject(members=members):
            parts = (
                json.dumps(key) + ':' + render_json(value, render_leaf)
                for key, value in members
            )
            return '{' + ','.join(parts) + '}'
        case JsonLeaf(value=value):
            return render_leaf(value)


def render_text[I](parts: tuple[str | I, ...], render_id: Callable[[I], str]) -> str:
    """Canonical text of a text with ids: ``<text ...>``, each literal part as
    a JSON string and each id as ``render_id`` gives it, separated by spaces.

    A literal part is quoted and an id is not, so two different part lists
    never give the same text.
    """
    rendered: list[str] = []
    for part in parts:
        match part:
            case str():
                rendered.append(json.dumps(part, ensure_ascii=False))
            case _:
                rendered.append(render_id(part))
    return '<text ' + ' '.join(rendered) + '>'


@dataclass(frozen=True, slots=True)
class LabelledText:
    """A text of an ``IdsInText`` path whose ids are replaced by their labels.

    ``parts`` holds the literal text and the labels in text order.
    """

    parts: tuple[str | IdLabel, ...]


@dataclass(frozen=True, slots=True)
class MeasureToken:
    """The place of a run measure (``RunMeasure``) inside a text."""

    name: str


@dataclass(frozen=True, slots=True)
class MeasuredText:
    """A text of a ``RunMeasuresInText`` path whose run measures are tokens.

    ``parts`` holds the literal text and the tokens in text order.
    """

    parts: tuple[str | MeasureToken, ...]


def render_measure(token: MeasureToken) -> str:
    """Text of a run measure token: ``<name>``."""
    return f'<{token.name}>'


@dataclass(frozen=True, slots=True)
class JsonDocument:
    """A stored JSON value, parsed; path-rule leaves are normalized values."""

    root: JsonNode[NormalizedValue]


@dataclass(frozen=True, slots=True)
class UnparsedDocument:
    """A cell of a stored JSON column whose text is not JSON, compared as
    that text (a bytea cell: its bytes as UTF-8 text). A test writes such
    text on purpose to check how a reader refuses it. Ids inside it stay raw,
    so a text that names a run's id differs between runs."""

    text: str


NormalizedValue = (
    NullValue
    | ExactValue
    | IdLabel
    | TimeRank
    | DuringRun
    | LiteralTime
    | OffsetFromAnchor
    | IgnoredValue
    | DatedName
    | DigestMatches
    | DigestMismatch
    | LabelledText
    | MeasuredText
    | JsonDocument
    | UnparsedDocument
)


@dataclass(frozen=True, slots=True)
class UnlabelledId:
    """In an order key only: an opaque value that has no label yet when its table is ordered."""


@dataclass(frozen=True, slots=True)
class RenderedDocument:
    """In an order key only: a JSON document as canonical text, ids not labelled yet shown as ``<new id>``."""

    text: str


OrderKeyValue = NormalizedValue | UnlabelledId | RenderedDocument


def sort_token(value: OrderKeyValue) -> tuple[int, str]:
    """A deterministic total order over normalized values.

    The order depends only on the normalized value, never on a raw id or an
    absolute run time, so both runs order their rows the same way.
    """
    match value:
        case NullValue():
            return (0, '')
        case ExactValue(text=text):
            return (1, text)
        case IdLabel(family=family, ordinal=ordinal):
            return (2, f'{family.value}#{ordinal:012d}')
        case UnlabelledId():
            return (3, '')
        case TimeRank(rank=rank):
            return (4, f'{rank:012d}')
        case DuringRun():
            return (5, '')
        case LiteralTime(time=time):
            kind, text = time_sort_token(time)
            return (6, f'{kind}{text}')
        case OffsetFromAnchor(offset=offset):
            return (7, f'{offset // _MICROSECOND:+021d}')
        case IgnoredValue():
            return (8, '')
        case DatedName(prefix=prefix, unit=unit, offset=offset):
            return (9, f'{prefix}|{unit.value}|{offset + _OFFSET_SHIFT:012d}')
        case DigestMatches():
            return (10, '')
        case DigestMismatch():
            return (11, '')
        case JsonDocument(root=root):
            return (12, render_json(root, render_value))
        case RenderedDocument(text=text):
            return (12, text)
        case LabelledText(parts=parts):
            return (13, render_text(parts, lambda label: sort_token(label)[1]))
        case MeasuredText(parts=parts):
            return (14, render_text(parts, render_measure))
        case UnparsedDocument(text=text):
            return (15, text)


def render_value(value: OrderKeyValue) -> str:
    """Text of a value for a diff report."""
    match value:
        case NullValue():
            return 'NULL'
        case ExactValue(text=text):
            return repr(text)
        case IdLabel(family=family, ordinal=ordinal):
            return f'{family.value}#{ordinal}'
        case UnlabelledId():
            return '<new id>'
        case TimeRank(rank=rank):
            return f'<time rank {rank}>'
        case DuringRun():
            return '<during run>'
        case LiteralTime(time=time):
            return render_time(time)
        case OffsetFromAnchor(offset=offset):
            return f'<anchor {offset.total_seconds():+.6f}s>'
        case IgnoredValue():
            return '<ignored>'
        case DatedName(prefix=prefix, unit=unit, offset=offset):
            return f'{prefix}_<{unit.value}{offset:+d}>'
        case DigestMatches():
            return '<digest matches>'
        case DigestMismatch():
            return '<digest does not match>'
        case JsonDocument(root=root):
            return render_json(root, render_value)
        case RenderedDocument(text=text):
            return text
        case LabelledText(parts=parts):
            return render_text(parts, render_value)
        case MeasuredText(parts=parts):
            return render_text(parts, render_measure)
        case UnparsedDocument(text=text):
            return f'<not json {text!r}>'


@dataclass(frozen=True, slots=True)
class Cell:
    """One named normalized value."""

    column: str
    value: NormalizedValue


@dataclass(frozen=True, slots=True)
class OrderKeyCell:
    """One named value of an order key."""

    column: str
    value: OrderKeyValue


def render_cells(cells: tuple[Cell, ...] | tuple[OrderKeyCell, ...]) -> str:
    """``column=value`` pairs for a diff report."""
    return ', '.join(f'{cell.column}={render_value(cell.value)}' for cell in cells)
