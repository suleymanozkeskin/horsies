"""The docs page of the schema lists the SQL files of the migration chain of horsies._core."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

import horsies._core as _core

_REPOSITORY: Final = Path(__file__).resolve().parents[4]
_SCHEMA_PAGE: Final = (
    _REPOSITORY
    / 'website'
    / 'src'
    / 'content'
    / 'docs'
    / 'internals'
    / 'database-schema.md'
)
"""The docs page whose table lists each file of the migration."""


def _file_names() -> tuple[str, ...]:
    """The file names of the migration, in the order they run."""
    return tuple(name for name, _ in _core.baseline_sql_files())


def test_the_schema_page_lists_every_file() -> None:
    page = _SCHEMA_PAGE.read_text(encoding='utf-8')
    listed = tuple(
        re.findall(r'^\| `(\d{4}_[a-z_]+\.sql)` \|', page, flags=re.MULTILINE)
    )
    assert listed == _file_names()


_BASELINE_VERSION: Final = 1000
"""The version of the baseline migration, whose files the table above lists."""


def test_the_schema_page_names_every_later_migration_file() -> None:
    """Each migration after the baseline is one file of ``core/migrations/``;
    the page names its path in the row of its version."""
    page = _SCHEMA_PAGE.read_text(encoding='utf-8')
    later = [
        (version, name)
        for version, name, _ in _core.migration_sql_files()
        if version != _BASELINE_VERSION
    ]
    assert later
    for version, name in later:
        assert re.search(
            rf'^\| {version} \| `core/migrations/{re.escape(name)}` \|',
            page,
            flags=re.MULTILINE,
        ), (version, name)
