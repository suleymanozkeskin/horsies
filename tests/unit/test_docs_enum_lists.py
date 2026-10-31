"""Each hand-written docs list of an enum's members matches the enum in code.

The docs site, the skills files and `llms.txt` list enum members by hand.
A member added to or removed from an enum in code leaves those lists stale,
and no other test reads them. This module reads every list and compares it
with the enum.

Scope: `website/src/content/docs/` (`.md`, `.mdx`), `horsies/.agents/skills/`
(`.md`) and `website/public/llms.txt`. The changelog and the migration
guides describe past versions, so the scan skips them. The enums are the
`Enum` classes in `__all__` of `horsies`, `horsies.monitoring` and
`horsies.web`.

Keys. A table row's key is its first cell when that cell is one code span
or one bare token. A list item's key is the code span or bare token that
starts the item. A bare token is upper snake case or `HRS-NNN`. Quotes
around a key are dropped. A key matches a member by name or by value. A key
written `E.MEMBER` names its enum: in a marked list, `E` must be the
marker's enum.

Markers. A marker is the nearest non-blank line above a table or a list
level: `<!-- members: E -->`, or `{/* members: E */}` in `.mdx`.
- `members: E`: the list holds every member of `E`, each once.
- `members: E part`: the list is one part of a split list. The parts of `E`
  in one file together hold every member, each once.
- `members: E subset`: each key is a member of `E`.
Every row of a marked list has a key. A key that matches a member by a
value other than its name, followed by an upper snake case token, must be
followed by that member's name (`HRS-001 WORKFLOW_NO_NAME`).

Lists that name their enum are checked as full lists without a marker:
- an inline list `E: a, b, c` (code spans or bare tokens) in a paragraph or
  a list item;
- a `class E(...Enum...):` block in a fenced code block.

A table or a list level whose keys are all written `E.MEMBER`, with more
than one enum among them, mixes enums. It needs no marker; each key must be
a member of its own `E`.

Unmarked lists. A table or a list level with at least two rows, a key on
every row, and at least two distinct members of one enum among its keys,
which are at least half of its keys, is a member list. It must carry a
marker. A marker that is not directly above a table or a list fails.
"""

from __future__ import annotations

import enum
import importlib
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Final

import pytest

pytestmark = [pytest.mark.unit]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCS_DIR = _REPO_ROOT / 'website' / 'src' / 'content' / 'docs'
_SKILLS_DIR = _REPO_ROOT / 'horsies' / '.agents' / 'skills'
_LLMS_FILE = _REPO_ROOT / 'website' / 'public' / 'llms.txt'
_HISTORY_PAGES: Final = ('changelog.md',)
_HISTORY_DIRS: Final = ('migrations',)
PUBLIC_MODULES: Final = ('horsies', 'horsies.monitoring', 'horsies.web')

# A docs list nests a few levels at most; a deeper stack is a parser defect.
MAX_LIST_DEPTH: Final = 8

type Registry = Mapping[str, type[enum.Enum]]

_FENCE = re.compile(r'^\s*(```|~~~)')
_TABLE_LINE = re.compile(r'^\s*\|')
_TABLE_SEPARATOR = re.compile(r'^\s*\|?\s*:?-{3,}')
_HEADING = re.compile(r'^\s{0,3}#{1,6}\s')
_ITEM = re.compile(r'^(?P<indent>\s*)(?:[-*+]|\d+[.)])\s+(?P<text>\S.*)$')
_MARKER = re.compile(
    r'^\s*(?:<!--\s*members:\s*(?P<html>\w+)(?:\s+(?P<html_kind>part|subset))?\s*-->'
    r'|\{/\*\s*members:\s*(?P<mdx>\w+)(?:\s+(?P<mdx_kind>part|subset))?\s*\*/\})\s*$'
)
_TOKEN = r'(?:`[^`]+`|HRS-\d{3}(?![\w-])|[A-Z][A-Z0-9_]+(?![\w-]))'
_LEADING_TOKEN = re.compile(r'\s*(?P<token>' + _TOKEN + r')')
_UPPER_SNAKE = re.compile(r'^[A-Z][A-Z0-9_]+$')
_CLASS_LINE = re.compile(
    r'^(?P<indent>\s*)class\s+(?P<name>\w+)\((?P<bases>[^)]*)\)\s*:'
)
_MEMBER_LINE = re.compile(r'^\s+(?P<name>[A-Z][A-Z0-9_]*)\s*=')


class MarkerKind(enum.Enum):
    ALL = 'all'
    PART = 'part'
    SUBSET = 'subset'


class ListForm(enum.Enum):
    TABLE = 'table'
    LIST = 'list'
    INLINE = 'inline'
    CODE = 'code'


class FindingKind(enum.Enum):
    UNKNOWN_ENUM = 'names no public enum'
    NOT_A_MEMBER = 'is not a member'
    LISTED_TWICE = 'is listed twice'
    MISSING = 'misses members'
    NAME_MISMATCH = 'pairs a value with another name'
    UNKEYED_ROW = 'has a row without a key'
    UNMARKED_LIST = 'lists enum members without a marker'
    DANGLING_MARKER = 'is not above a table or a list'


@dataclass(frozen=True, slots=True)
class KeyedRow:
    """A row with a key. `qualifier` is the `E` of a key written `E.MEMBER`."""

    line: int
    key: str
    qualifier: str | None
    partners: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UnkeyedRow:
    line: int


type Row = KeyedRow | UnkeyedRow


@dataclass(frozen=True, slots=True)
class Marker:
    line: int
    enum_name: str
    kind: MarkerKind


@dataclass(frozen=True, slots=True)
class Unmarked:
    """A table or a list level with no marker above it."""


@dataclass(frozen=True, slots=True)
class MemberList:
    line: int
    form: ListForm
    rows: tuple[Row, ...]
    marker: Marker | Unmarked


@dataclass(frozen=True, slots=True)
class Finding:
    label: str
    line: int
    kind: FindingKind
    detail: str

    def __str__(self) -> str:
        return f'{self.label}:{self.line}: {self.kind.value}: {self.detail}'


@dataclass(frozen=True, slots=True)
class ScannedText:
    lists: tuple[MemberList, ...]
    dangling_markers: tuple[Marker, ...]


# ---- Keys ----


def _split_key(token: str) -> tuple[str | None, str]:
    """The qualifier and the key of a token, without code ticks and quotes."""
    text = token.strip('`').strip().strip('"\'')
    head, dot, tail = text.rpartition('.')
    return (head, tail) if dot and head.isidentifier() else (None, text)


def _key_text(token: str) -> str:
    return _split_key(token)[1]


def _keyed_row(line: int, token: str, partners: tuple[str, ...]) -> KeyedRow:
    qualifier, key = _split_key(token)
    return KeyedRow(line=line, key=key, qualifier=qualifier, partners=partners)


def _is_token(text: str) -> bool:
    return re.fullmatch(_TOKEN, text) is not None


def _item_row(line: int, text: str) -> Row:
    """An item's key is the token that starts it; its partner is the next token."""
    first = _LEADING_TOKEN.match(text)
    if first is None:
        return UnkeyedRow(line=line)
    second = _LEADING_TOKEN.match(text, first.end())
    partners = () if second is None else (_key_text(second['token']),)
    return _keyed_row(line, first['token'], partners)


def _table_row(line: int, text: str) -> Row:
    """A body row's key is its first cell when that cell is one token."""
    cells = [cell.strip() for cell in text.strip().strip('|').split('|')]
    if not _is_token(cells[0]):
        return UnkeyedRow(line=line)
    partners = (_key_text(cells[1]),) if len(cells) > 1 and _is_token(cells[1]) else ()
    return _keyed_row(line, cells[0], partners)


def _member_of(enum_cls: type[enum.Enum], key: str) -> enum.Enum | None:
    """The member that `key` names by name or by value, or None."""
    for member in enum_cls:
        if key == member.name or key == str(member.value):
            return member
    return None


# ---- Self-describing lists ----


def _code_lists(
    lines: Sequence[str], start: int, end: int, registry: Registry
) -> Iterator[MemberList]:
    """`class E(...Enum...):` blocks in the fenced lines `start` to `end`."""
    index = start
    while index < end:
        header = _CLASS_LINE.match(lines[index])
        index += 1
        if (
            header is None
            or header['name'] not in registry
            or 'Enum' not in header['bases']
        ):
            continue
        header_line = index
        indent = len(header['indent'])
        rows: list[Row] = []
        while index < end:
            text = lines[index]
            if text.strip() and len(text) - len(text.lstrip()) <= indent:
                break
            member = _MEMBER_LINE.match(text)
            if member is not None:
                rows.append(_keyed_row(index + 1, member['name'], ()))
            index += 1
        yield MemberList(
            line=header_line,
            form=ListForm.CODE,
            rows=tuple(rows),
            marker=Marker(
                line=header_line, enum_name=header['name'], kind=MarkerKind.ALL
            ),
        )


@cache
def _inline_pattern(names: frozenset[str]) -> re.Pattern[str]:
    alternatives = '|'.join(
        sorted((re.escape(name) for name in names), key=len, reverse=True)
    )
    return re.compile(
        r'(?<![\w.`])`?(?P<enum>' + alternatives + r')`?\s*:\s*'
        r'(?P<run>' + _TOKEN + r'(?:\s*,\s*(?:and\s+|or\s+)?' + _TOKEN + r')+)'
    )


def _inline_lists(text: str, line: int, registry: Registry) -> Iterator[MemberList]:
    """Each `E: a, b, c` in one paragraph or list item, where `E` is a public enum."""
    for found in _inline_pattern(frozenset(registry)).finditer(text):
        tokens = re.findall(_TOKEN, found['run'])
        yield MemberList(
            line=line,
            form=ListForm.INLINE,
            rows=tuple(_keyed_row(line, token, ()) for token in tokens),
            marker=Marker(line=line, enum_name=found['enum'], kind=MarkerKind.ALL),
        )


# ---- Scanning ----


@dataclass(slots=True)
class _OpenLevel:
    """A list level that the scanner is still reading."""

    indent: int
    line: int
    marker: Marker | Unmarked
    rows: list[Row]


@dataclass(slots=True)
class _Scan:
    """Scanner state for one text."""

    registry: Registry
    lists: list[MemberList]
    dangling: list[Marker]
    levels: list[_OpenLevel]
    unit_line: int
    unit_parts: list[str]
    pending: Marker | None
    blank_before: bool

    def take_marker(self) -> Marker | Unmarked:
        marker, self.pending = self.pending, None
        return Unmarked() if marker is None else marker

    def drop_marker(self) -> None:
        if self.pending is not None:
            self.dangling.append(self.pending)
            self.pending = None

    def close_unit(self) -> None:
        if self.unit_parts:
            text = ' '.join(self.unit_parts)
            self.lists.extend(_inline_lists(text, self.unit_line, self.registry))
        self.unit_parts = []

    def start_unit(self, line: int, text: str) -> None:
        self.close_unit()
        self.unit_line = line
        self.unit_parts = [text]

    def close_levels(self, deeper_than: int) -> None:
        while self.levels and self.levels[-1].indent > deeper_than:
            level = self.levels.pop()
            self.lists.append(
                MemberList(
                    line=level.line,
                    form=ListForm.LIST,
                    rows=tuple(level.rows),
                    marker=level.marker,
                )
            )

    def close_blocks(self) -> None:
        self.close_unit()
        self.close_levels(deeper_than=-1)


def _read_table(scan: _Scan, lines: Sequence[str], start: int) -> int:
    """Record the table that starts at `start`; return the index after it."""
    end = start
    while end < len(lines) and _TABLE_LINE.match(lines[end]):
        end += 1
    if end - start < 2 or not _TABLE_SEPARATOR.match(lines[start + 1]):
        scan.drop_marker()
        return end
    rows = tuple(_table_row(index + 1, lines[index]) for index in range(start + 2, end))
    scan.lists.append(
        MemberList(
            line=start + 1, form=ListForm.TABLE, rows=rows, marker=scan.take_marker()
        )
    )
    return end


def _read_item(scan: _Scan, line: int, indent: int, text: str) -> None:
    """Add a list item to its level, opening a level when the item starts one."""
    scan.close_levels(deeper_than=indent)
    if not scan.levels or scan.levels[-1].indent < indent:
        if len(scan.levels) >= MAX_LIST_DEPTH:
            raise AssertionError(
                f'line {line}: list nests deeper than {MAX_LIST_DEPTH}'
            )
        scan.levels.append(
            _OpenLevel(indent=indent, line=line, marker=scan.take_marker(), rows=[])
        )
    scan.levels[-1].rows.append(_item_row(line, text))
    scan.start_unit(line, text)


def _read_text_line(scan: _Scan, line: int, text: str) -> None:
    """A line that starts no block: an item continuation or a paragraph line."""
    indent = len(text) - len(text.lstrip())
    in_item = bool(scan.levels) and (
        indent > scan.levels[-1].indent or not scan.blank_before
    )
    if not in_item:
        scan.close_levels(deeper_than=-1)
        scan.drop_marker()
    if scan.unit_parts and not scan.blank_before:
        scan.unit_parts.append(text.strip())
    else:
        scan.start_unit(line, text.strip())


def _read_fence(scan: _Scan, lines: Sequence[str], start: int) -> int:
    """Record the enum classes in the fence that opens at `start`; return the index after it."""
    opener = _FENCE.match(lines[start])
    assert opener is not None
    end = start + 1
    while end < len(lines) and not lines[end].lstrip().startswith(opener.group(1)):
        end += 1
    scan.lists.extend(_code_lists(lines, start + 1, end, scan.registry))
    return end + 1


def scan_text(text: str, registry: Registry) -> ScannedText:
    """Every member list in one docs text, and each marker that is above no list."""
    lines = text.splitlines()
    scan = _Scan(
        registry=registry,
        lists=[],
        dangling=[],
        levels=[],
        unit_line=0,
        unit_parts=[],
        pending=None,
        blank_before=False,
    )
    index = 0
    while index < len(lines):
        text_line = lines[index]
        marker = _marker_of(text_line, index + 1)
        item = _ITEM.match(text_line)
        match text_line:
            case _ if not text_line.strip():
                scan.close_unit()
                scan.blank_before = True
                index += 1
                continue
            case _ if marker is not None:
                scan.close_blocks()
                scan.drop_marker()
                scan.pending = marker
                index += 1
            case _ if _FENCE.match(text_line):
                scan.close_blocks()
                scan.drop_marker()
                index = _read_fence(scan, lines, index)
            case _ if _TABLE_LINE.match(text_line):
                scan.close_blocks()
                index = _read_table(scan, lines, index)
            case _ if _HEADING.match(text_line):
                scan.close_blocks()
                scan.drop_marker()
                index += 1
            case _ if item is not None:
                _read_item(scan, index + 1, len(item['indent']), item['text'])
                index += 1
            case _:
                _read_text_line(scan, index + 1, text_line)
                index += 1
        scan.blank_before = False
    scan.close_blocks()
    scan.drop_marker()
    return ScannedText(lists=tuple(scan.lists), dangling_markers=tuple(scan.dangling))


def _marker_of(text: str, line: int) -> Marker | None:
    found = _MARKER.match(text)
    if found is None:
        return None
    kind_text = found['html_kind'] or found['mdx_kind'] or MarkerKind.ALL.value
    return Marker(
        line=line,
        enum_name=found['html'] or found['mdx'],
        kind=MarkerKind(kind_text),
    )


# ---- Checks ----


def _list_findings(
    label: str, member_list: MemberList, marker: Marker, enum_cls: type[enum.Enum]
) -> Iterator[Finding]:
    """Rows that are not members, listed twice, unkeyed or mispaired; missing members."""
    seen: set[enum.Enum] = set()
    for row in member_list.rows:
        match row:
            case UnkeyedRow(line=line):
                yield Finding(
                    label, line, FindingKind.UNKEYED_ROW, f'list of {marker.enum_name}'
                )
            case KeyedRow(line=line, key=key, qualifier=str() as qualifier) if (
                qualifier != marker.enum_name
            ):
                yield Finding(
                    label,
                    line,
                    FindingKind.NOT_A_MEMBER,
                    f'{qualifier}.{key} in a list of {marker.enum_name}',
                )
            case KeyedRow(line=line, key=key, partners=partners):
                member = _member_of(enum_cls, key)
                match member:
                    case None:
                        yield Finding(
                            label,
                            line,
                            FindingKind.NOT_A_MEMBER,
                            f'{key} in {marker.enum_name}',
                        )
                    case _ if member in seen:
                        yield Finding(
                            label,
                            line,
                            FindingKind.LISTED_TWICE,
                            f'{key} in {marker.enum_name}',
                        )
                    case _:
                        seen.add(member)
                        yield from _pairing_findings(label, line, key, partners, member)
    if marker.kind is MarkerKind.ALL:
        missing = [m.name for m in enum_cls if m not in seen]
        if missing:
            yield Finding(
                label,
                member_list.line,
                FindingKind.MISSING,
                f'{marker.enum_name}: {missing}',
            )


def _pairing_findings(
    label: str, line: int, key: str, partners: tuple[str, ...], member: enum.Enum
) -> Iterator[Finding]:
    """A key given as a value that differs from the name must be followed by that name."""
    match partners:
        case (partner, *_) if (
            key == str(member.value)
            and key != member.name
            and _UPPER_SNAKE.match(partner)
            and partner != member.name
        ):
            yield Finding(
                label,
                line,
                FindingKind.NAME_MISMATCH,
                f'{key} {partner}, expected {member.name}',
            )
        case _:
            return


def _part_findings(
    label: str, parts: Sequence[MemberList], enum_name: str, enum_cls: type[enum.Enum]
) -> Iterator[Finding]:
    """The parts of one enum in one file together hold every member, each once."""
    seen: set[enum.Enum] = set()
    for part in parts:
        in_part = {
            member
            for row in part.rows
            if isinstance(row, KeyedRow)
            and (member := _member_of(enum_cls, row.key)) is not None
        }
        for member in sorted(in_part & seen, key=lambda m: m.name):
            yield Finding(
                label,
                part.line,
                FindingKind.LISTED_TWICE,
                f'{member.name} in parts of {enum_name}',
            )
        seen |= in_part
    missing = [m.name for m in enum_cls if m not in seen]
    if missing:
        yield Finding(
            label,
            parts[0].line,
            FindingKind.MISSING,
            f'parts of {enum_name}: {missing}',
        )


def _names_own_enums(member_list: MemberList, registry: Registry) -> bool:
    """Every key is written `E.MEMBER` with a public `E`, and the keys name two enums or more."""
    qualifiers = {
        row.qualifier if isinstance(row, KeyedRow) else None for row in member_list.rows
    }
    return (
        None not in qualifiers and qualifiers <= registry.keys() and len(qualifiers) > 1
    )


def _qualified_findings(
    label: str, member_list: MemberList, registry: Registry
) -> Iterator[Finding]:
    """Each `E.MEMBER` key names a member of its own `E`."""
    for row in member_list.rows:
        match row:
            case KeyedRow(line=line, key=key, qualifier=str() as qualifier) if (
                _member_of(registry[qualifier], key) is None
            ):
                yield Finding(
                    label, line, FindingKind.NOT_A_MEMBER, f'{qualifier}.{key}'
                )
            case _:
                continue


def _member_list_enums(member_list: MemberList, registry: Registry) -> list[str]:
    """The enums whose members make this unmarked table or list level a member list."""
    keys: list[str] = []
    for row in member_list.rows:
        match row:
            case KeyedRow(key=key):
                keys.append(key)
            case UnkeyedRow():
                return []
    if len(keys) < 2:
        return []
    found: list[str] = []
    for name, enum_cls in registry.items():
        hits = [key for key in keys if _member_of(enum_cls, key) is not None]
        distinct = {_member_of(enum_cls, key) for key in hits}
        if len(distinct) >= 2 and 2 * len(hits) >= len(keys):
            found.append(name)
    return found


def check_text(label: str, text: str, registry: Registry) -> list[Finding]:
    """Every finding for one docs text."""
    return check_scanned(label, scan_text(text, registry), registry)


def check_scanned(
    label: str, scanned: ScannedText, registry: Registry
) -> list[Finding]:
    """Every finding for one scanned docs text."""
    findings = [
        Finding(
            label,
            marker.line,
            FindingKind.DANGLING_MARKER,
            f'members: {marker.enum_name}',
        )
        for marker in scanned.dangling_markers
    ]
    parts: dict[str, list[MemberList]] = {}
    for member_list in scanned.lists:
        match member_list.marker:
            case Unmarked() if member_list.rows and _names_own_enums(
                member_list, registry
            ):
                findings.extend(_qualified_findings(label, member_list, registry))
            case Unmarked():
                enums = _member_list_enums(member_list, registry)
                if enums:
                    findings.append(
                        Finding(
                            label,
                            member_list.line,
                            FindingKind.UNMARKED_LIST,
                            f'members of {enums}',
                        )
                    )
            case Marker(enum_name=name) as marker if name not in registry:
                findings.append(
                    Finding(label, marker.line, FindingKind.UNKNOWN_ENUM, name)
                )
            case Marker(enum_name=name, kind=MarkerKind.PART) as marker:
                parts.setdefault(name, []).append(member_list)
                findings.extend(
                    _list_findings(label, member_list, marker, registry[name])
                )
            case Marker(enum_name=name) as marker:
                findings.extend(
                    _list_findings(label, member_list, marker, registry[name])
                )
    for name, enum_parts in parts.items():
        findings.extend(_part_findings(label, enum_parts, name, registry[name]))
    return findings


# ---- The docs ----


def public_enums() -> dict[str, type[enum.Enum]]:
    """The `Enum` classes in `__all__` of the public modules."""
    registry: dict[str, type[enum.Enum]] = {}
    for module_name in PUBLIC_MODULES:
        module = importlib.import_module(module_name)
        for name in module.__all__:
            value = getattr(module, name)
            if isinstance(value, type) and issubclass(value, enum.Enum):
                registry[name] = value
    return registry


def docs_files() -> list[Path]:
    """The docs, skills and llms files in scope, without the history pages."""
    site = [
        path
        for pattern in ('*.md', '*.mdx')
        for path in _DOCS_DIR.rglob(pattern)
        if path.name not in _HISTORY_PAGES
        and not any(part in _HISTORY_DIRS for part in path.relative_to(_DOCS_DIR).parts)
    ]
    return sorted([*site, *_SKILLS_DIR.rglob('*.md'), _LLMS_FILE])


@dataclass(frozen=True, slots=True)
class DocsScan:
    checked_per_root: Mapping[str, int]
    findings: tuple[Finding, ...]


def _root_of(path: Path) -> str:
    for root in (_DOCS_DIR, _SKILLS_DIR):
        if path.is_relative_to(root):
            return str(root.relative_to(_REPO_ROOT))
    return str(path.relative_to(_REPO_ROOT))


@cache
def scan_docs() -> DocsScan:
    """The findings of every docs file, and the checked lists per docs root."""
    registry = public_enums()
    findings: list[Finding] = []
    checked: dict[str, int] = {}
    for path in docs_files():
        scanned = scan_text(path.read_text(encoding='utf-8'), registry)
        marked = sum(1 for item in scanned.lists if isinstance(item.marker, Marker))
        checked[_root_of(path)] = checked.get(_root_of(path), 0) + marked
        findings.extend(
            check_scanned(str(path.relative_to(_REPO_ROOT)), scanned, registry)
        )
    return DocsScan(checked_per_root=checked, findings=tuple(findings))


def _report(findings: Sequence[Finding]) -> str:
    return '\n'.join(str(finding) for finding in findings)


_LIST_CHECKS: Final = frozenset(FindingKind) - {
    FindingKind.UNMARKED_LIST,
    FindingKind.DANGLING_MARKER,
}


def test_each_docs_root_has_checked_lists() -> None:
    checked = scan_docs().checked_per_root

    assert checked.get('website/src/content/docs', 0) > 0
    assert checked.get('horsies/.agents/skills', 0) > 0
    assert checked.get('website/public/llms.txt', 0) > 0


def test_each_docs_member_list_matches_its_enum() -> None:
    wrong = [f for f in scan_docs().findings if f.kind in _LIST_CHECKS]

    assert not wrong, _report(wrong)


def test_each_docs_member_list_has_a_marker() -> None:
    unmarked = [f for f in scan_docs().findings if f.kind is FindingKind.UNMARKED_LIST]

    assert not unmarked, _report(unmarked)


def test_each_docs_marker_is_above_a_list() -> None:
    dangling = [
        f for f in scan_docs().findings if f.kind is FindingKind.DANGLING_MARKER
    ]

    assert not dangling, _report(dangling)


# ---- The checker, on small texts ----


class _Color(enum.Enum):
    RED = 'RED'
    GREEN = 'GREEN'
    BLUE = 'BLUE'


class _Code(enum.Enum):
    FIRST = 'HRS-001'
    SECOND = 'HRS-002'


_REGISTRY: Final[Registry] = {'Color': _Color, 'Code': _Code}

_TABLE_HEAD: Final = '| Code | Meaning |\n|---|---|\n'


def _table(*keys: str) -> str:
    return _TABLE_HEAD + ''.join(f'| {key} | text |\n' for key in keys)


def _kinds(text: str, label: str = 'doc.md') -> list[FindingKind]:
    return [finding.kind for finding in check_text(label, text, _REGISTRY)]


CHECKER_CASES: Final[list[tuple[str, str, list[FindingKind]]]] = [
    (
        'full table',
        '<!-- members: Color -->\n' + _table('`RED`', '`GREEN`', '`BLUE`'),
        [],
    ),
    (
        'missing member',
        '<!-- members: Color -->\n' + _table('`RED`', '`GREEN`'),
        [FindingKind.MISSING],
    ),
    (
        'extra key',
        '<!-- members: Color -->\n' + _table('`RED`', '`GREEN`', '`BLUE`', '`PINK`'),
        [FindingKind.NOT_A_MEMBER],
    ),
    (
        'key twice',
        '<!-- members: Color -->\n' + _table('`RED`', '`GREEN`', '`BLUE`', '`RED`'),
        [FindingKind.LISTED_TWICE],
    ),
    ('subset', '<!-- members: Color subset -->\n' + _table('`RED`', '`GREEN`'), []),
    (
        'subset with a stale key',
        '<!-- members: Color subset -->\n' + _table('`RED`', '`PINK`'),
        [FindingKind.NOT_A_MEMBER],
    ),
    (
        'parts complete',
        '<!-- members: Color part -->\n' + _table('`RED`') + '\ntext\n\n'
        '<!-- members: Color part -->\n' + _table('`GREEN`', '`BLUE`'),
        [],
    ),
    (
        'parts missing a member',
        '<!-- members: Color part -->\n' + _table('`RED`') + '\ntext\n\n'
        '<!-- members: Color part -->\n' + _table('`GREEN`'),
        [FindingKind.MISSING],
    ),
    (
        'parts listing a member twice',
        '<!-- members: Color part -->\n' + _table('`RED`', '`BLUE`') + '\ntext\n\n'
        '<!-- members: Color part -->\n' + _table('`GREEN`', '`BLUE`'),
        [FindingKind.LISTED_TWICE],
    ),
    ('unmarked table', _table('`RED`', '`GREEN`'), [FindingKind.UNMARKED_LIST]),
    ('unmarked table of one member', _table('`RED`', '`other`'), []),
    ('qualified keys of two enums', _table('`Color.RED`', '`Code.FIRST`'), []),
    (
        'qualified keys of one enum',
        _table('`Color.RED`', '`Color.GREEN`'),
        [FindingKind.UNMARKED_LIST],
    ),
    (
        'qualified key of another enum in a marked list',
        '<!-- members: Color subset -->\n' + _table('`Color.RED`', '`Code.FIRST`'),
        [FindingKind.NOT_A_MEMBER],
    ),
    (
        'mixed qualified keys with one that is not a member',
        _table('`Color.RED`', '`Code.FIRST`', '`Color.PINK`'),
        [FindingKind.NOT_A_MEMBER],
    ),
    (
        'marker above a paragraph',
        '<!-- members: Color -->\ntext\n',
        [FindingKind.DANGLING_MARKER],
    ),
    (
        'marker of an unknown enum',
        '<!-- members: Shade -->\n' + _table('`RED`', '`GREEN`'),
        [FindingKind.UNKNOWN_ENUM],
    ),
    (
        'marked table with a row without a key',
        '<!-- members: Color subset -->\n' + _table('`RED`', 'plain words'),
        [FindingKind.UNKEYED_ROW],
    ),
    (
        'value paired with another name',
        '<!-- members: Code -->\n'
        + _TABLE_HEAD
        + '| HRS-001 | `FIRST` |\n| HRS-002 | `FIRST` |\n',
        [FindingKind.NAME_MISMATCH],
    ),
    (
        'bullet list',
        '<!-- members: Color -->\n- `RED` — a\n- `GREEN` — b\n- `BLUE` — c\n',
        [],
    ),
    (
        'bare bullet keys',
        '<!-- members: Code -->\n- HRS-001 FIRST\n- HRS-002 SECOND\n',
        [],
    ),
    (
        'nested list level',
        '- group:\n  - `RED` — a\n  - `GREEN` — b\n',
        [FindingKind.UNMARKED_LIST],
    ),
    ('inline list', '- `Color`: `RED`, `GREEN`\n', [FindingKind.MISSING]),
    ('wrapped inline list', '- Color: RED, GREEN,\n  BLUE. More text.\n', []),
    (
        'enum class in a code block',
        '```python\nclass Color(Enum):\n    RED = "red"\n    GREEN = "green"\n```\n',
        [FindingKind.MISSING],
    ),
]


@pytest.mark.parametrize(
    ('text', 'expected'),
    [(text, expected) for _, text, expected in CHECKER_CASES],
    ids=[case_id for case_id, _, _ in CHECKER_CASES],
)
def test_the_checker_reports_each_finding(
    text: str, expected: list[FindingKind]
) -> None:
    assert _kinds(text) == expected


def test_the_mdx_marker_form_marks_a_list() -> None:
    text = '{/* members: Color */}\n' + _table('`RED`', '`GREEN`', '`BLUE`')

    assert _kinds(text, label='doc.mdx') == []


def test_a_finding_names_the_file_and_line() -> None:
    findings = check_text(
        'doc.md', 'intro\n\n<!-- members: Color -->\n' + _table('`RED`'), _REGISTRY
    )

    assert [str(finding) for finding in findings] == [
        "doc.md:4: misses members: Color: ['GREEN', 'BLUE']",
    ]
