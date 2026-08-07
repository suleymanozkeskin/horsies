"""The rerun field-provenance table is closed over the real column set.

The canonical set is derived from the live table's SQLAlchemy model
plus the declared cutover additions — not hand-copied — so adding a
column anywhere makes this pin fail until the new column declares its
side of the replay principle.
"""

from __future__ import annotations

import re

import pytest

from horsies.core.history.rerun.provenance import (
    RERUN_FIELD_PROVENANCE,
    FieldProvenance,
)
from horsies.core.history.terminalization.live_cutover import (
    LIVE_CUTOVER_COLUMNS_DDL,
)
from horsies.core.models.task_pg import TaskModel

pytestmark = [pytest.mark.unit]


def enqueue_visible_columns() -> set[str]:
    model_columns = {column.name for column in TaskModel.__table__.columns}
    cutover_columns = set(
        re.findall(r'ADD COLUMN (\w+)', '\n'.join(LIVE_CUTOVER_COLUMNS_DDL))
    )
    return model_columns | cutover_columns


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
