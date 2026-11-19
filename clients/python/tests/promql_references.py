"""The series names and label names that a PromQL expression refers to.

A small scanner, not a PromQL parser: it reads the selectors (a metric name
and its label matchers) and the label lists of ``by``, ``without``, ``on``,
``ignoring``, ``group_left`` and ``group_right``. It skips function and
aggregation names, operators, numbers, durations, string literals, range and
subquery brackets, comments, and Grafana variables (``$name``, ``${name}``).

A text that it cannot read (an unclosed brace or bracket, a selector without
a metric name, a matcher that is not ``label op "value"``) fails the test,
with the expression and the cause. Prometheus checks the full syntax
(``promtool check rules``, the query API in the container test).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Final, NoReturn

import pytest

_IDENTIFIER: Final = re.compile(r'[A-Za-z_:][A-Za-z0-9_:]*')
_NUMBER: Final = re.compile(r'[0-9][A-Za-z0-9_.]*')
_GRAFANA_VARIABLE: Final = re.compile(r'\$(\{[^}]*\}|[A-Za-z0-9_]+)')
_MATCHER: Final = re.compile(
    r'\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*'
)
_STRING_QUOTES: Final = frozenset({'"', "'", '`'})

GROUPING_KEYWORDS: Final = frozenset(
    {'by', 'without', 'on', 'ignoring', 'group_left', 'group_right'}
)
"""Keywords followed by a list of label names."""
OTHER_KEYWORDS: Final = frozenset(
    {
        'and',
        'or',
        'unless',
        'bool',
        'offset',
        'atan2',
        # Aggregation operators, which take ``by``/``without`` before their
        # parentheses.
        'sum',
        'min',
        'max',
        'avg',
        'group',
        'stddev',
        'stdvar',
        'count',
        'count_values',
        'bottomk',
        'topk',
        'quantile',
        'limitk',
        'limit_ratio',
    }
)
"""Keywords that are neither a series name nor followed by label names."""


class MatchOperator(Enum):
    """The operator of a label matcher."""

    EQUAL = '='
    NOT_EQUAL = '!='
    MATCHES = '=~'
    NOT_MATCHES = '!~'


@dataclass(frozen=True)
class Matcher:
    """One label matcher of a selector."""

    label: str
    operator: MatchOperator
    value: str


@dataclass(frozen=True)
class Selector:
    """A series name and its label matchers."""

    series: str
    matchers: tuple[Matcher, ...]


@dataclass(frozen=True)
class References:
    """What an expression refers to."""

    selectors: tuple[Selector, ...]
    grouping_labels: frozenset[str]
    """The labels of every ``by``, ``without``, ``on``, ``ignoring``,
    ``group_left`` and ``group_right`` list."""


@dataclass
class _Scan:
    """The state of one scan of one expression."""

    text: str
    where: str
    position: int = 0

    def fail(self, cause: str) -> NoReturn:
        pytest.fail(
            f'{self.where}: {cause} at character {self.position} of {self.text!r}'
        )

    def at_end(self) -> bool:
        return self.position >= len(self.text)

    def current(self) -> str:
        return self.text[self.position]

    def skip_space(self) -> None:
        while not self.at_end() and self.current().isspace():
            self.position += 1

    def next_is(self, character: str) -> bool:
        """Whether the next character that is not a space is ``character``.
        Does not move."""
        position = self.position
        while position < len(self.text) and self.text[position].isspace():
            position += 1
        return position < len(self.text) and self.text[position] == character

    def closing(self, opening: str, closing: str) -> int:
        """The index of the ``closing`` that matches the ``opening`` at the
        current position, outside string literals."""
        depth = 0
        position = self.position
        quote: str | None = None
        while position < len(self.text):
            character = self.text[position]
            match (quote, character):
                case (None, _) if character in _STRING_QUOTES:
                    quote = character
                case (None, _) if character == opening:
                    depth += 1
                case (None, _) if character == closing:
                    depth -= 1
                    if depth == 0:
                        return position
                case (str(), '\\'):
                    position += 1
                case (str(), _) if character == quote:
                    quote = None
                case _:
                    pass
            position += 1
        self.fail(f'no {closing!r} for this {opening!r}')


def references(expression: str, where: str) -> References:
    """The selectors and grouping labels of ``expression``. ``where`` names
    the expression in a failure."""
    scan = _Scan(text=expression, where=where)
    selectors: list[Selector] = []
    grouping: set[str] = set()
    while True:
        scan.skip_space()
        if scan.at_end():
            return References(
                selectors=tuple(selectors), grouping_labels=frozenset(grouping)
            )
        start = scan.position
        _step(scan, selectors, grouping)
        if scan.position <= start:
            raise AssertionError(f'{where}: the scan did not move at character {start}')


def _step(scan: _Scan, selectors: list[Selector], grouping: set[str]) -> None:
    """Read one token at the current position."""
    character = scan.current()
    identifier = _IDENTIFIER.match(scan.text, scan.position)
    match character:
        case '#':
            end = scan.text.find('\n', scan.position)
            scan.position = len(scan.text) if end < 0 else end
        case _ if character in _STRING_QUOTES:
            _skip_string(scan)
        case '$':
            variable = _GRAFANA_VARIABLE.match(scan.text, scan.position)
            if variable is None:
                scan.fail('a "$" that starts no Grafana variable')
            scan.position = variable.end()
        case '[':
            scan.position = scan.closing('[', ']') + 1
        case '{':
            scan.fail('a selector without a metric name')
        case _ if character.isdigit():
            number = _NUMBER.match(scan.text, scan.position)
            scan.position = scan.position + 1 if number is None else number.end()
        case _ if identifier is not None:
            scan.position = identifier.end()
            _identifier(scan, identifier.group(0), selectors, grouping)
        case _:
            scan.position += 1


def _identifier(
    scan: _Scan,
    name: str,
    selectors: list[Selector],
    grouping: set[str],
) -> None:
    """Classify the identifier ``name``, which ends at the current position."""
    match name:
        case _ if name in GROUPING_KEYWORDS:
            if scan.next_is('('):
                grouping.update(_label_list(scan))
        case _ if name in OTHER_KEYWORDS:
            pass
        case _ if scan.next_is('('):
            pass  # a function call
        case _:
            selectors.append(Selector(series=name, matchers=_matchers(scan)))


def _label_list(scan: _Scan) -> list[str]:
    """The label names of the ``( a, b )`` that follows."""
    scan.skip_space()
    end = scan.closing('(', ')')
    inside = scan.text[scan.position + 1 : end]
    scan.position = end + 1
    names = [name.strip() for name in inside.split(',') if name.strip()]
    for name in names:
        if _IDENTIFIER.fullmatch(name) is None:
            scan.fail(f'{name!r} is not a label name')
    return names


def _matchers(scan: _Scan) -> tuple[Matcher, ...]:
    """The matchers of the ``{...}`` that follows a series name, if any."""
    if not scan.next_is('{'):
        return ()
    scan.skip_space()
    end = scan.closing('{', '}')
    inside = scan.text[scan.position + 1 : end]
    scan.position = end + 1
    matchers: list[Matcher] = []
    for part in _split_matchers(inside):
        found = _MATCHER.fullmatch(part)
        if found is None:
            scan.fail(f'{part!r} is not a matcher label op "value"')
        matchers.append(
            Matcher(
                label=found.group(1),
                operator=MatchOperator(found.group(2)),
                value=found.group(3),
            )
        )
    return tuple(matchers)


def _split_matchers(inside: str) -> list[str]:
    """The matcher texts of a brace body, split at commas outside quotes. A
    trailing comma gives no empty matcher."""
    parts: list[str] = []
    current: list[str] = []
    in_quote = False
    escaped = False
    for character in inside:
        match (in_quote, escaped, character):
            case (True, True, _):
                escaped = False
            case (True, False, '\\'):
                escaped = True
            case (_, False, '"'):
                in_quote = not in_quote
            case (False, _, ','):
                parts.append(''.join(current))
                current = []
                continue
            case _:
                pass
        current.append(character)
    parts.append(''.join(current))
    return [part for part in parts if part.strip()]


def _skip_string(scan: _Scan) -> None:
    """Move past the string literal that starts at the current position."""
    quote = scan.current()
    position = scan.position + 1
    while position < len(scan.text):
        match scan.text[position]:
            case '\\' if quote != '`':
                position += 2
            case character if character == quote:
                scan.position = position + 1
                return
            case _:
                position += 1
    scan.fail('an unclosed string literal')
