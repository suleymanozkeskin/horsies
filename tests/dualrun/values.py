"""Normalized cell values, clock values, and the run window."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Final

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.rules import IdFamily

_MICROSECOND: Final = timedelta(microseconds=1)

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


NormalizedValue = (
    NullValue
    | ExactValue
    | IdLabel
    | TimeRank
    | DuringRun
    | LiteralTime
    | OffsetFromAnchor
    | IgnoredValue
)


@dataclass(frozen=True, slots=True)
class UnlabelledId:
    """In an order key only: an opaque value that has no label yet when its table is ordered."""


OrderKeyValue = NormalizedValue | UnlabelledId


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
