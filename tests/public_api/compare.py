"""Compare a public API snapshot and its pyright view with the baseline.

The rule of the public API gate:

- blocking: a scope path or reachable entry that is gone or absent
  (``EntryRemoved``); any other difference of an entry that both have
  (``EntryChanged``: a signature, a type hint, a default, an enum member or
  value, a field, a base, a removed or changed member); a lower completeness
  score (``ScoreLowered``); a diagnostic of a symbol in scope that the
  baseline does not have (``DiagnosticAdded``);
- not blocking, reported: a new entry, or an entry that was absent in the
  baseline (``EntryAdded``), a new member of a
  class (``MemberAdded``), a diagnostic that is gone (``DiagnosticRemoved``),
  a higher score (``ScoreRaised``).

``defined_as`` is information only and is not compared. An intended change
updates the baseline files in the same PR (``runner.py``); the diff of the
files is what the owner approves. The module reads no file.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, assert_never

type JsonValue = str | int | float | bool | None | list[JsonValue] | dict[str, JsonValue]

SECTIONS: Final = ('entries', 'reachable')
INFORMATION_KEYS: Final = frozenset({'defined_as'})
MAX_COMPARE_DEPTH: Final = 64


@dataclass(frozen=True, slots=True)
class Ok[T]:
    value: T


@dataclass(frozen=True, slots=True)
class Err[E]:
    error: E


type Result[T, E] = Ok[T] | Err[E]


@dataclass(frozen=True, slots=True)
class EntryRemoved:
    section: str
    key: str
    cause: str


@dataclass(frozen=True, slots=True)
class EntryChanged:
    section: str
    key: str
    changes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EntryAdded:
    section: str
    key: str


@dataclass(frozen=True, slots=True)
class MemberAdded:
    section: str
    key: str
    member: str


@dataclass(frozen=True, slots=True)
class ScoreLowered:
    baseline: float
    current: float


@dataclass(frozen=True, slots=True)
class ScoreRaised:
    baseline: float
    current: float


@dataclass(frozen=True, slots=True)
class DiagnosticAdded:
    text: str


@dataclass(frozen=True, slots=True)
class DiagnosticRemoved:
    text: str


type Difference = (
    EntryRemoved
    | EntryChanged
    | EntryAdded
    | MemberAdded
    | ScoreLowered
    | ScoreRaised
    | DiagnosticAdded
    | DiagnosticRemoved
)


@dataclass(frozen=True, slots=True)
class DocumentMalformed:
    """A snapshot or view does not have the shape that the tools write."""

    document: str
    cause: str


def blocks(difference: Difference) -> bool:
    """Whether ``difference`` needs an update of the baseline."""
    match difference:
        case EntryRemoved() | EntryChanged() | ScoreLowered() | DiagnosticAdded():
            return True
        case EntryAdded() | MemberAdded() | ScoreRaised() | DiagnosticRemoved():
            return False
        case _:
            assert_never(difference)


def value_changes(baseline: JsonValue, current: JsonValue, path: str, depth: int = 0) -> list[str]:
    """``path: old -> new`` for each differing leaf; a list of another length
    differs as a whole. A nesting deeper than ``MAX_COMPARE_DEPTH`` is a
    change, so the comparison fails closed."""
    if depth > MAX_COMPARE_DEPTH:
        return [f'{path}: nests deeper than {MAX_COMPARE_DEPTH} levels']
    match baseline, current:
        case dict(), dict():
            changes: list[str] = []
            for key in sorted(baseline.keys() | current.keys()):
                if key in INFORMATION_KEYS:
                    continue
                if key not in current:
                    changes.append(f'{path}.{key}: removed')
                elif key not in baseline:
                    changes.append(f'{path}.{key}: added {_text(current[key])}')
                else:
                    changes.extend(value_changes(baseline[key], current[key], f'{path}.{key}', depth + 1))
            return changes
        case list(), list() if len(baseline) == len(current):
            return [
                change
                for index, (old, new) in enumerate(zip(baseline, current, strict=True))
                for change in value_changes(old, new, f'{path}[{index}]', depth + 1)
            ]
        case _ if baseline == current and type(baseline) is type(current):
            return []
        case _:
            return [f'{path}: {_text(baseline)} -> {_text(current)}']


def entry_differences(section: str, key: str, baseline: JsonValue, current: JsonValue) -> list[Difference]:
    """The differences of one entry that both documents have. A new member
    of a class is ``MemberAdded``; every other difference is in one
    ``EntryChanged``."""
    match baseline, current:
        case {'kind': 'class', 'members': dict(old_members)}, {'kind': 'class', 'members': dict(new_members)}:
            rest_old = {name: value for name, value in baseline.items() if name != 'members'}
            rest_new = {name: value for name, value in current.items() if name != 'members'}
            changes = value_changes(rest_old, rest_new, key)
            added: list[Difference] = []
            for member in sorted(old_members.keys() | new_members.keys()):
                if member not in new_members:
                    changes.append(f'{key}.members.{member}: removed')
                elif member not in old_members:
                    added.append(MemberAdded(section, key, member))
                else:
                    changes.extend(
                        value_changes(old_members[member], new_members[member], f'{key}.members.{member}')
                    )
        case _:
            changes = value_changes(baseline, current, key)
            added = []
    changed: list[Difference] = [EntryChanged(section, key, tuple(changes))] if changes else []
    return changed + added


def section_of(document: JsonValue, name: str, which: str) -> Result[dict[str, JsonValue], DocumentMalformed]:
    match document:
        case {'format': 1, **rest}:
            pass
        case _:
            return Err(DocumentMalformed(which, 'no "format": 1'))
    match rest.get(name):
        case dict(section):
            return Ok(section)
        case _:
            return Err(DocumentMalformed(which, f'no "{name}" object'))


def compare_snapshots(baseline: JsonValue, current: JsonValue) -> Result[list[Difference], DocumentMalformed]:
    """The differences of two runtime snapshots, section by section, in key order."""
    differences: list[Difference] = []
    for section in SECTIONS:
        match section_of(baseline, section, 'baseline snapshot'):
            case Err(malformed):
                return Err(malformed)
            case Ok(old):
                pass
        match section_of(current, section, 'current snapshot'):
            case Err(malformed):
                return Err(malformed)
            case Ok(new):
                pass
        for key in sorted(old.keys() | new.keys()):
            if key not in old:
                differences.append(EntryAdded(section, key))
                continue
            if key not in new:
                differences.append(EntryRemoved(section, key, 'not in the snapshot'))
                continue
            match old[key], new[key]:
                case {'kind': 'absent'}, {'kind': 'absent'}:
                    pass
                case _, {'kind': 'absent', 'cause': str(cause)}:
                    differences.append(EntryRemoved(section, key, cause))
                # A docs import that the baseline build did not have.
                case {'kind': 'absent'}, _:
                    differences.append(EntryAdded(section, key))
                case old_entry, new_entry:
                    differences.extend(entry_differences(section, key, old_entry, new_entry))
    return Ok(differences)


@dataclass(frozen=True, slots=True)
class PyrightView:
    completeness_score: float
    diagnostics: frozenset[str]


def parse_view(document: JsonValue, which: str) -> Result[PyrightView, DocumentMalformed]:
    match document:
        case {'format': 1, 'completeness_score': float() | int() as score, 'diagnostics': list(diagnostics)}:
            pass
        case _:
            return Err(DocumentMalformed(which, 'no "format": 1, "completeness_score" and "diagnostics"'))
    texts: set[str] = set()
    for diagnostic in diagnostics:
        match diagnostic:
            case str():
                texts.add(diagnostic)
            case _:
                return Err(DocumentMalformed(which, f'diagnostic {_text(diagnostic)} is not a string'))
    return Ok(PyrightView(float(score), frozenset(texts)))


def compare_views(baseline: JsonValue, current: JsonValue) -> Result[list[Difference], DocumentMalformed]:
    """The score change and the diagnostics that differ, in text order."""
    match parse_view(baseline, 'baseline pyright view'):
        case Err(malformed):
            return Err(malformed)
        case Ok(old):
            pass
    match parse_view(current, 'current pyright view'):
        case Err(malformed):
            return Err(malformed)
        case Ok(new):
            pass
    differences: list[Difference] = []
    if new.completeness_score < old.completeness_score:
        differences.append(ScoreLowered(old.completeness_score, new.completeness_score))
    elif new.completeness_score > old.completeness_score:
        differences.append(ScoreRaised(old.completeness_score, new.completeness_score))
    differences.extend(DiagnosticAdded(text) for text in sorted(new.diagnostics - old.diagnostics))
    differences.extend(DiagnosticRemoved(text) for text in sorted(old.diagnostics - new.diagnostics))
    return Ok(differences)


def describe(difference: Difference) -> str:
    match difference:
        case EntryRemoved(section, key, cause):
            return f'{section} {key}: removed ({cause})'
        case EntryChanged(section, key, changes):
            return f'{section} {key}: changed\n' + '\n'.join(f'    {change}' for change in changes)
        case EntryAdded(section, key):
            return f'{section} {key}: added'
        case MemberAdded(section, key, member):
            return f'{section} {key}: member {member} added'
        case ScoreLowered(baseline, current):
            return f'pyright completeness score lowered: {baseline} -> {current}'
        case ScoreRaised(baseline, current):
            return f'pyright completeness score raised: {baseline} -> {current}'
        case DiagnosticAdded(text):
            return f'pyright diagnostic added: {text}'
        case DiagnosticRemoved(text):
            return f'pyright diagnostic removed: {text}'
        case _:
            assert_never(difference)


def render(differences: Sequence[Difference]) -> str:
    """A report with the blocking differences first."""
    blocking = [describe(difference) for difference in differences if blocks(difference)]
    other = [describe(difference) for difference in differences if not blocks(difference)]
    return (
        f'{len(blocking)} blocking differences:\n'
        + '\n'.join(blocking)
        + f'\n\n{len(other)} other differences (additions and improvements):\n'
        + '\n'.join(other)
    )


def _text(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
