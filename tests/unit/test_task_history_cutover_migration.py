"""The transitional v27 migration never drifts from the cutover fragment.

One shape authority: the declared cutover fragment. The transitional
chain migration derives its column list from it programmatically; these
pins hold the derivation honest from both sides — every fragment column
appears, nothing else appears, and the transitional form strips every
constraint (presence half first: the fragment provably carries them).
The model side is pinned Optional so it never claims a strictness the
v27 catalog does not enforce.
"""

from __future__ import annotations

import pytest

from horsies.core.history.terminalization.live_cutover import (
    LIVE_CUTOVER_COLUMNS_DDL,
    cutover_column_definitions,
    transitional_cutover_columns_ddl,
)
from horsies.core.models.task_pg import TaskModel

pytestmark = [pytest.mark.unit]


class TestDerivationIsClosed:
    def test_every_fragment_column_appears_exactly_once(self) -> None:
        transitional = transitional_cutover_columns_ddl()
        definitions = cutover_column_definitions()
        assert len(definitions) == 15
        for name, column_type in definitions:
            assert (
                f'ADD COLUMN IF NOT EXISTS {name} {column_type}'
                in transitional
            ), name
        assert transitional.count('ADD COLUMN') == len(definitions)

    def test_transitional_form_is_constraint_free(self) -> None:
        fragment = '\n'.join(LIVE_CUTOVER_COLUMNS_DDL)
        # Presence half: the authoritative fragment carries constraints,
        # so stripping them below is a real transformation.
        assert 'NOT NULL' in fragment
        assert 'CHECK' in fragment
        assert 'ADD CONSTRAINT' in fragment
        transitional = transitional_cutover_columns_ddl()
        assert 'NOT NULL' not in transitional
        assert 'CHECK' not in transitional
        assert 'CONSTRAINT' not in transitional

    def test_model_columns_exist_and_are_nullable_at_v27(self) -> None:
        table_columns = TaskModel.__table__.columns
        for name, _ in cutover_column_definitions():
            assert name in table_columns, name
            assert table_columns[name].nullable is True, (
                f'{name} claims NOT NULL before the cutover migration '
                'tightens the catalog'
            )
