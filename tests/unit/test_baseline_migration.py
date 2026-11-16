"""The baseline SQL files read as one migration."""

from __future__ import annotations

import hashlib

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
    '9bf367b2e41f5a03eacd793dd3ee49fcdb69961454e9ba15'
    'defdd71a038ca4cbadc65e92f54b5b40f6223aeaff355776'
)
"""SHA-384 of the migration text. An edit of a baseline SQL file changes it;
the edit then shows in this file's diff as well."""


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
