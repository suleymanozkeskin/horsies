"""The rerun field-provenance table is closed over the real column set.

The canonical set is derived from the live table's definition in the
baseline SQL — not hand-copied — so adding a column there makes this pin
fail until the new column declares its side of the replay principle.
"""

from __future__ import annotations

import re

import pytest

from horsies.core.baseline.migration import load_baseline_migration
from horsies.core.history.rerun.provenance import (
    RERUN_FIELD_PROVENANCE,
    FieldProvenance,
)
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]

_LIVE_TABLE_OPENING = 'CREATE TABLE horsies_tasks (\n'
_COLUMN_LINE = re.compile(r'^ {4}([a-z][a-z0-9_]*) [a-z]', re.MULTILINE)


def enqueue_visible_columns() -> set[str]:
    """The column names of ``horsies_tasks`` in the baseline SQL."""
    match load_baseline_migration():
        case Ok(ok_value=migration):
            sql = migration.sql
        case Err(err_value=unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')
    body = sql.split(_LIVE_TABLE_OPENING, 1)[1].split('\n);\n', 1)[0]
    columns = {match.group(1) for match in _COLUMN_LINE.finditer(body)}
    # A pattern that matches nothing would make every closure check pass.
    assert {'id', 'input', 'rerun_root_task_id'} <= columns
    return columns


class TestProvenanceIsClosed:
    def test_every_column_declares_exactly_one_side(self) -> None:
        expected = enqueue_visible_columns()
        classified = set(RERUN_FIELD_PROVENANCE)
        unclassified = expected - classified
        assert not unclassified, (
            f'columns without a declared provenance side: '
            f'{sorted(unclassified)}'
        )
        phantom = classified - expected
        assert not phantom, (
            f'provenance entries for columns that do not exist: '
            f'{sorted(phantom)}'
        )

    def test_every_side_is_used(self) -> None:
        used = set(RERUN_FIELD_PROVENANCE.values())
        assert used == set(FieldProvenance)

    def test_the_lineage_pair_travels_together(self) -> None:
        assert (
            RERUN_FIELD_PROVENANCE['rerun_of_task_id']
            is FieldProvenance.LINEAGE
        )
        assert (
            RERUN_FIELD_PROVENANCE['rerun_root_task_id']
            is FieldProvenance.LINEAGE
        )

    def test_deadline_is_caller_explicit_alone(self) -> None:
        caller_explicit = [
            column
            for column, side in RERUN_FIELD_PROVENANCE.items()
            if side is FieldProvenance.CALLER_EXPLICIT
        ]
        assert caller_explicit == ['good_until']
