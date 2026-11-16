"""The baseline migration text, for tests that check the installed SQL."""

from __future__ import annotations

import pytest

from horsies.core.baseline.migration import load_baseline_migration
from horsies.core.types.result import Err, Ok


def baseline_sql() -> str:
    """The SQL text that the install runs; fails the test if it is unreadable."""
    match load_baseline_migration():
        case Ok(migration):
            return migration.sql
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')
