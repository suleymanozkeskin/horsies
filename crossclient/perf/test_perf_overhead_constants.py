"""The table of the constants that the telemetry overhead session sets
(``releases/README.md``, "The constants that the session sets") names
constants that exist.

A row names a constant and its file. A constant that moves to another file
or gets another name fails here, so the table keeps pointing at the code that
the session changes.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

import pytest

pytestmark = [pytest.mark.unit]

CHECKOUT_ROOT: Final = Path(__file__).resolve().parents[2]
README: Final = Path(__file__).resolve().parent / 'releases' / 'README.md'
TABLE_HEADING: Final = '### The constants that the session sets'
ROW: Final = re.compile(r'^\| `(?P<name>[A-Z][A-Z0-9_]*)` \| `(?P<path>[^`]+)` \|')


def _rows() -> list[tuple[str, str]]:
    """The (constant, file) of each row of the table."""
    text = README.read_text(encoding='utf-8')
    _, found, section = text.partition(TABLE_HEADING)
    assert found, f'{README} has no section {TABLE_HEADING!r}'
    section = section.split('\n## ', 1)[0].split('\n### ', 1)[0]
    rows: list[tuple[str, str]] = []
    for line in section.splitlines():
        matched = ROW.match(line)
        if matched is not None:
            rows.append((matched.group('name'), matched.group('path')))
    return rows


def _defines(source: str, name: str) -> bool:
    """Whether ``source`` defines ``name`` at the start of a line: a Rust
    ``const`` or a Python module-level assignment."""
    rust = re.compile(rf'^\s*(pub(\([a-z]+\))?\s+)?const\s+{name}\s*:', re.MULTILINE)
    python = re.compile(rf'^{name}\s*(:[^=\n]*)?=', re.MULTILINE)
    return rust.search(source) is not None or python.search(source) is not None


def test_the_table_has_rows() -> None:
    assert len(_rows()) >= 10


@pytest.mark.parametrize(('name', 'path'), _rows(), ids=lambda value: str(value))
def test_each_constant_is_defined_in_its_file(name: str, path: str) -> None:
    source = (CHECKOUT_ROOT / path).read_text(encoding='utf-8')
    assert _defines(source, name), f'{path} does not define {name}'
