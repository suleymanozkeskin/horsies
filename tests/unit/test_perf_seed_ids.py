"""The ids that the measurement harness seeds.

Task and workflow ids are ``uuid`` columns, so a seeded id is a UUID, and a
run finds its own rows by an id range on the primary key: every id of one
role and kind of row shares its leading hex digits. The seeds and the range
deletes are SQL; these tests hold the Python side that both read.
"""

from __future__ import annotations

import uuid

import pytest

from tests.perf.scenarios import (
    ID_BIRTH,
    MAX_SEEDED_INDEX,
    Role,
    RowKind,
    SeedIds,
    seed_ids,
    seed_rows,
)

pytestmark = [pytest.mark.unit]


def _ids() -> SeedIds:
    return seed_ids('seed-ids-first', Role.BASELINE, RowKind.TASK)


class TestSeededId:
    def test_the_same_scenario_role_kind_and_index_give_the_same_id(self) -> None:
        """A later run deletes the rows of a crashed one by the same ids."""
        assert _ids().id_at(7) == _ids().id_at(7)

    def test_an_id_is_a_uuidv7_born_at_the_seed_birth(self) -> None:
        """A move records a clock exception for an id born after its terminal
        time; a birth in the past keeps the seeded rows out of that table."""
        seeded = uuid.UUID(_ids().id_at(1))
        birth_ms = int.from_bytes(seeded.bytes[0:6], 'big')
        assert seeded.version == 7
        assert seeded.variant == uuid.RFC_4122
        assert birth_ms == int(ID_BIRTH.timestamp() * 1000)

    def test_the_index_is_the_last_hex_digits(self) -> None:
        """The SQL seeds build an id as the prefix and the index in hex."""
        assert uuid.UUID(_ids().id_at(255)).hex == f'{_ids().prefix}00000000ff'

    @pytest.mark.parametrize('index', [0, MAX_SEEDED_INDEX + 1])
    def test_an_index_outside_the_range_is_refused(self, index: int) -> None:
        with pytest.raises(ValueError, match='seeded row index'):
            _ids().id_at(index)

    def test_ids_lists_rows_one_to_count_in_order(self) -> None:
        assert list(_ids().ids(3)) == [_ids().id_at(1), _ids().id_at(2), _ids().id_at(3)]


class TestIdRange:
    def test_every_id_lies_in_its_range(self) -> None:
        ids = _ids()
        lower, upper = uuid.UUID(ids.lower), uuid.UUID(ids.upper)
        for index in (1, 2, MAX_SEEDED_INDEX):
            assert lower < uuid.UUID(ids.id_at(index)) <= upper

    def test_roles_kinds_and_scenarios_have_disjoint_ranges(self) -> None:
        """One run's delete never removes another role's or scenario's rows."""
        ranges = [
            seed_ids(scenario, role, kind)
            for scenario in ('seed-ids-first', 'seed-ids-second')
            for role in Role
            for kind in RowKind
        ]
        assert len({ids.prefix for ids in ranges}) == len(ranges)
        ordered = sorted(ranges, key=lambda ids: uuid.UUID(ids.lower))
        for earlier, later in zip(ordered, ordered[1:]):
            assert uuid.UUID(earlier.upper) < uuid.UUID(later.lower)


class TestSeedRows:
    def test_seed_rows_names_one_range_per_kind_of_its_role(self) -> None:
        rows = seed_rows('seed-ids-workflow', Role.BALLAST)
        assert rows.tasks == seed_ids(
            'seed-ids-workflow', Role.BALLAST, RowKind.TASK
        )
        assert rows.workflows == seed_ids(
            'seed-ids-workflow', Role.BALLAST, RowKind.WORKFLOW
        )
        assert rows.nodes == seed_ids(
            'seed-ids-workflow', Role.BALLAST, RowKind.WORKFLOW_NODE
        )
        assert rows.dependent_nodes == seed_ids(
            'seed-ids-workflow', Role.BALLAST, RowKind.DEPENDENT_NODE
        )
