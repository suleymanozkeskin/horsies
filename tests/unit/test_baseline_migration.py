"""The baseline SQL files read as one migration."""

from __future__ import annotations

import hashlib
import re
from importlib import resources
from pathlib import Path
from typing import Final

import pytest

from horsies.core.baseline import migration
from horsies.core.baseline.migration import (
    BASELINE_DESCRIPTION,
    BASELINE_SQL_FILES,
    BASELINE_VERSION,
    BaselineMigration,
    BaselineSqlUnreadable,
    baseline_checksum,
    load_baseline_migration,
)
from horsies.core.types.result import Err, Ok

PINNED_CHECKSUM_HEX = (
    '913dbae62bfce7343c3199179102d14580e587908992a9bf'
    '76f3498bd9defd4af4d4384f42b2629b3fdd461e2f500415'
)
"""SHA-384 of the migration text. An edit of a baseline SQL file changes it;
the edit then shows in this file's diff as well."""

_REPOSITORY: Final = Path(__file__).resolve().parents[2]
_CORE_MIGRATIONS: Final = (
    _REPOSITORY / 'crates' / 'horsies-core' / 'src' / 'broker' / 'migrations.rs'
)
"""The core's migration runner: its ``baseline_sql_files!`` list includes the same files."""
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


def _loaded() -> BaselineMigration:
    match load_baseline_migration():
        case Ok(loaded):
            return loaded
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


def test_migration_is_version_1000_with_its_description() -> None:
    loaded = _loaded()
    assert loaded.version == BASELINE_VERSION == 1000
    assert loaded.description == BASELINE_DESCRIPTION == 'baseline'


def test_checksum_is_sha384_of_the_text() -> None:
    loaded = _loaded()
    assert loaded.checksum == hashlib.sha384(loaded.sql.encode('utf-8')).digest()
    assert loaded.checksum == baseline_checksum(loaded.sql)
    assert len(loaded.checksum) == 48


def test_checksum_is_pinned() -> None:
    assert _loaded().checksum.hex() == PINNED_CHECKSUM_HEX


def test_text_is_the_files_in_name_order() -> None:
    loaded = _loaded()
    assert BASELINE_SQL_FILES == tuple(sorted(BASELINE_SQL_FILES))
    tables = loaded.sql.index('-- horsies baseline schema, part 1:')
    indexes = loaded.sql.index('-- horsies baseline schema, part 2:')
    functions = loaded.sql.index('-- horsies baseline schema, part 3:')
    workflow_functions = loaded.sql.index('-- horsies baseline schema, part 5:')
    assert tables == 0 < indexes < functions < workflow_functions


def _package_sql_files() -> tuple[str, ...]:
    folder = resources.files('horsies.core.baseline').joinpath('sql')
    return tuple(
        sorted(entry.name for entry in folder.iterdir() if entry.name.endswith('.sql'))
    )


def _core_sql_files() -> tuple[str, ...]:
    """The quoted names of the ``baseline_sql_files!`` invocation in the core."""
    source = _CORE_MIGRATIONS.read_text(encoding='utf-8')
    start = source.index('\nbaseline_sql_files!(\n')
    end = source.index('\n);\n', start)
    return tuple(re.findall(r'"([^"]+)"', source[start:end]))


def test_the_list_holds_every_sql_file_of_the_package_folder() -> None:
    assert BASELINE_SQL_FILES == _package_sql_files()


def test_the_core_includes_the_same_files_in_the_same_order() -> None:
    # Both clients install the same migration 1000 text and checksum. The
    # core's test `baseline_file_list_is_python_s_list_and_the_whole_folder`
    # checks the same pair from its side.
    assert _core_sql_files() == BASELINE_SQL_FILES


def test_the_schema_page_lists_every_file() -> None:
    page = _SCHEMA_PAGE.read_text(encoding='utf-8')
    listed = tuple(
        re.findall(r'^\| `(\d{4}_[a-z_]+\.sql)` \|', page, flags=re.MULTILINE)
    )
    assert listed == BASELINE_SQL_FILES


def test_text_names_no_schema() -> None:
    # The install and the scratch-schema fixtures create the objects in the
    # current schema; a qualifier would put them elsewhere.
    assert 'public.' not in _loaded().sql


def test_text_has_no_transaction_control() -> None:
    # The install runs the text inside its own transaction with the
    # bookkeeping row; a COMMIT in the text would split the two.
    statements = {line.strip().upper() for line in _loaded().sql.splitlines()}
    assert statements.isdisjoint(
        {'BEGIN;', 'COMMIT;', 'ROLLBACK;', 'START TRANSACTION;'}
    )


def test_unreadable_file_is_a_named_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        migration,
        'BASELINE_SQL_FILES',
        ('0001_tables.sql', '9999_missing.sql'),
    )
    match load_baseline_migration():
        case Err(BaselineSqlUnreadable(file_name=file_name, cause=cause)):
            assert file_name == '9999_missing.sql'
            assert cause.startswith('FileNotFoundError')
        case Ok(loaded):
            pytest.fail(f'expected an unreadable file, got {loaded.version}')
