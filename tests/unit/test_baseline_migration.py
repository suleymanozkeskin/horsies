"""The baseline SQL files of horsies._core read as one migration."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Final

import horsies._core as _core
from horsies.core.baseline.migration import (
    BASELINE_DESCRIPTION,
    BASELINE_VERSION,
    baseline_checksum,
    load_baseline_migration,
)

PINNED_CHECKSUM_HEX = (
    '321a174469af19f8a721e31326bf024a0e230187c9ce40fb'
    '9aa82a1d503b6ccf206b9f5156c3b76e0b9e64cb6afe4b92'
)
"""SHA-384 of the migration text. An edit of a baseline SQL file changes it;
the edit then shows in this file's diff as well."""

_REPOSITORY: Final = Path(__file__).resolve().parents[2]
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


def test_migration_is_version_1000_with_its_description() -> None:
    loaded = load_baseline_migration()
    assert loaded.version == BASELINE_VERSION == 1000
    assert loaded.description == BASELINE_DESCRIPTION == 'baseline'


def test_checksum_is_sha384_of_the_text() -> None:
    loaded = load_baseline_migration()
    assert loaded.checksum == hashlib.sha384(loaded.sql.encode('utf-8')).digest()
    assert loaded.checksum == baseline_checksum(loaded.sql)
    assert len(loaded.checksum) == 48


def test_checksum_is_pinned() -> None:
    assert load_baseline_migration().checksum.hex() == PINNED_CHECKSUM_HEX


def test_text_is_the_files_in_name_order() -> None:
    loaded = load_baseline_migration()
    names = _file_names()
    assert names == tuple(sorted(names))
    tables = loaded.sql.index('-- horsies baseline schema, part 1:')
    indexes = loaded.sql.index('-- horsies baseline schema, part 2:')
    functions = loaded.sql.index('-- horsies baseline schema, part 3:')
    workflow_functions = loaded.sql.index('-- horsies baseline schema, part 5:')
    assert tables == 0 < indexes < functions < workflow_functions


def test_the_schema_page_lists_every_file() -> None:
    page = _SCHEMA_PAGE.read_text(encoding='utf-8')
    listed = tuple(
        re.findall(r'^\| `(\d{4}_[a-z_]+\.sql)` \|', page, flags=re.MULTILINE)
    )
    assert listed == _file_names()


def test_text_names_no_schema() -> None:
    # The install and the scratch-schema fixtures create the objects in the
    # current schema; a qualifier would put them elsewhere.
    assert 'public.' not in load_baseline_migration().sql


def test_text_has_no_transaction_control() -> None:
    # The install runs the text inside its own transaction with the
    # bookkeeping row; a COMMIT in the text would split the two.
    statements = {line.strip().upper() for line in load_baseline_migration().sql.splitlines()}
    assert statements.isdisjoint(
        {'BEGIN;', 'COMMIT;', 'ROLLBACK;', 'START TRANSACTION;'}
    )
