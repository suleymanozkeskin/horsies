"""Fixtures of the extension tests."""

from __future__ import annotations

import pytest

from tests.extension.fresh_interpreter import Database, run_case, text_of


@pytest.fixture(scope='session')
def extension_database(tmp_path_factory: pytest.TempPathFactory) -> Database:
    """The test session's database (``tests/session_database.py``) with this
    build's schema, installed through ``CoreHandle.install_schema`` in a fresh
    interpreter. The cases that use it delete the task rows they write."""
    from tests.session_database import suite_database_url

    database = Database(url=suite_database_url())
    run = run_case('schema', 'install', tmp_path_factory.mktemp('schema'), database)
    assert text_of(run.report, 'outcome') == 'SchemaInstalled', run.report
    return database
