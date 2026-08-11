"""The staged lookup generator: manifest arithmetic and rendered structure.

The rendered function is what qualification measured, so these tests pin
its structural properties: probe order (live, forever, then finite),
oldest-first pruned probes against newest-first legacy probes, the
five-second clock-bound subtraction, the birth-floor fast path's presence
rules, and — the rejection basis of the union form — that no statement
ever references the partitioned parent.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.partitions.catalog import LeafCatalogRow
from horsies.core.history.reads.identity_lookup import (
    HistoryTaskIdentity,
    LiveTaskIdentity,
    TaskIdentityAbsent,
    decode_lookup_row,
)
from horsies.core.history.reads.lookup_generation import (
    LookupLeaf,
    LookupManifest,
    manifest_from_catalog,
    render_staged_lookup_function,
    render_staged_provenance_function,
)

pytestmark = [pytest.mark.unit]


UTC = timezone.utc
DAY = timedelta(days=1)
BASE = datetime(2026, 8, 1, tzinfo=UTC)


def make_catalog_row(
    day_offset: int,
    *,
    class_key: str = 'finite_30d_v1',
    min_birth_at: datetime | None = None,
    min_birth_verified: bool = True,
) -> LeafCatalogRow:
    lower = BASE + DAY * day_offset
    parent = f'horsies_task_history_{class_key}'
    leaf_name = f'{parent}_{lower:%Y_%m_%d}'
    return LeafCatalogRow(
        leaf_name=leaf_name,
        parent_name=parent,
        class_key=class_key,
        lower_anchor=lower,
        upper_anchor=lower + DAY,
        index_schema_version=1,
        id_index_name=f'{leaf_name}_task_idx',
        partition_bound='FOR VALUES FROM ... TO ...',
        min_birth_at=min_birth_at,
        min_birth_verified=min_birth_verified,
        created_at=lower,
        detached_at=None,
        dropped_at=None,
    )


class TestLookupLeaf:
    def test_rejects_unsafe_relation_name(self) -> None:
        with pytest.raises(ValueError, match='safe identifier'):
            LookupLeaf(
                relation_name='drop table; --',
                lower_anchor=BASE,
                upper_anchor=BASE + DAY,
                min_birth_at=None,
            )

    def test_rejects_naive_bounds(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            LookupLeaf(
                relation_name='leaf',
                lower_anchor=BASE.replace(tzinfo=None),
                upper_anchor=BASE + DAY,
                min_birth_at=None,
            )

    def test_rejects_inverted_bounds(self) -> None:
        with pytest.raises(ValueError, match='increasing'):
            LookupLeaf(
                relation_name='leaf',
                lower_anchor=BASE + DAY,
                upper_anchor=BASE,
                min_birth_at=None,
            )


class TestManifestFromCatalog:
    def test_orders_oldest_first_across_classes(self) -> None:
        rows = [
            make_catalog_row(2),
            make_catalog_row(0, class_key='finite_7d_v1'),
            make_catalog_row(1),
        ]
        manifest = manifest_from_catalog(rows)
        assert [leaf.lower_anchor for leaf in manifest.leaves] == [
            BASE,
            BASE + DAY,
            BASE + DAY * 2,
        ]

    def test_rejects_duplicate_relation_names(self) -> None:
        row = make_catalog_row(0)
        with pytest.raises(ValueError, match='distinct'):
            manifest_from_catalog([row, row])

    def test_floor_is_minimum_of_observed_births(self) -> None:
        birth_a = BASE - timedelta(hours=4)
        birth_b = BASE - timedelta(hours=1)
        manifest = manifest_from_catalog(
            [
                make_catalog_row(0, min_birth_at=birth_b),
                make_catalog_row(1, min_birth_at=birth_a),
            ]
        )
        assert manifest.birth_floor == birth_a

    def test_one_unverified_leaf_disables_the_floor(self) -> None:
        manifest = manifest_from_catalog(
            [
                make_catalog_row(0, min_birth_at=BASE),
                make_catalog_row(1, min_birth_verified=False),
            ]
        )
        assert manifest.birth_floor is None

    def test_verified_empty_leaves_do_not_block_the_floor(self) -> None:
        manifest = manifest_from_catalog(
            [
                make_catalog_row(0, min_birth_at=BASE),
                make_catalog_row(1, min_birth_at=None, min_birth_verified=True),
            ]
        )
        assert manifest.birth_floor == BASE

    def test_all_empty_verified_leaves_yield_no_floor(self) -> None:
        manifest = manifest_from_catalog([make_catalog_row(0)])
        assert manifest.birth_floor is None

    def test_heartbeat_leaves_are_excluded_at_assembly(self) -> None:
        history = make_catalog_row(0)
        heartbeat = make_catalog_row(1, class_key='heartbeats')
        manifest = manifest_from_catalog([history, heartbeat])
        assert [leaf.relation_name for leaf in manifest.leaves] == [
            history.leaf_name
        ]

    def test_empty_catalog_yields_empty_manifest(self) -> None:
        assert manifest_from_catalog([]) == LookupManifest(
            leaves=(), birth_floor=None
        )


class TestManifestSeparatesProbeListFromFloor:
    """A leaf whose relation is gone leaves the probes and stays in the floor.

    The two jobs answer different questions. The probe list says which
    relations can be read — naming a dropped one builds a function that
    dies at execution. The floor says where retained history begins, and
    a leaf destroyed out of band DID retain history until someone removed
    it. Narrowing the floor to the probe list would raise it over those
    tasks and report them as predating retained history, presenting an
    accidental drop as ordinary ageing.

    Floor expectations are derived by calling the same function without
    the exclusion rather than hand-written, so the assertion tracks the
    generator instead of restating it.
    """

    def test_absent_relation_leaves_the_probe_list(self) -> None:
        present = make_catalog_row(0)
        gone = make_catalog_row(1)
        manifest = manifest_from_catalog(
            [present, gone], absent_relations=frozenset({gone.leaf_name})
        )
        assert [leaf.relation_name for leaf in manifest.leaves] == [
            present.leaf_name
        ]

    def test_absent_relation_stays_in_the_birth_floor(self) -> None:
        rows = [
            make_catalog_row(0, min_birth_at=BASE - timedelta(hours=4)),
            make_catalog_row(1, min_birth_at=BASE - timedelta(hours=1)),
        ]
        unfiltered = manifest_from_catalog(rows)
        filtered = manifest_from_catalog(
            rows, absent_relations=frozenset({rows[0].leaf_name})
        )
        assert filtered.birth_floor == unfiltered.birth_floor

    def test_excluding_the_oldest_leaf_does_not_raise_the_floor(self) -> None:
        oldest_birth = BASE - timedelta(hours=9)
        rows = [
            make_catalog_row(0, min_birth_at=oldest_birth),
            make_catalog_row(1, min_birth_at=BASE),
        ]
        filtered = manifest_from_catalog(
            rows, absent_relations=frozenset({rows[0].leaf_name})
        )
        # Errs LOW by construction: the surviving leaf's own birth would
        # be the floor if the dropped leaf were excluded from it, and a
        # task born in between would then be declared absent without a
        # probe -- the aggressive direction this separation exists to
        # avoid.
        assert filtered.birth_floor == oldest_birth
        assert filtered.birth_floor != BASE

    def test_absent_relation_is_not_named_in_the_rendered_body(self) -> None:
        present = make_catalog_row(0)
        gone = make_catalog_row(1)
        rendered = render_staged_provenance_function(
            manifest_from_catalog(
                [present, gone],
                absent_relations=frozenset({gone.leaf_name}),
            )
        )
        assert present.leaf_name in rendered
        assert gone.leaf_name not in rendered

    def test_unknown_absent_name_changes_nothing(self) -> None:
        rows = [make_catalog_row(0), make_catalog_row(1)]
        assert manifest_from_catalog(
            rows, absent_relations=frozenset({'horsies_task_history_absent'})
        ) == manifest_from_catalog(rows)


class TestRenderedFunction:
    def render(self, *rows: LeafCatalogRow) -> str:
        return render_staged_lookup_function(manifest_from_catalog(list(rows)))

    def test_empty_manifest_probes_live_and_forever_only(self) -> None:
        body = self.render()
        assert 'FROM horsies_tasks\n' in body
        assert 'FROM horsies_task_history_forever\n' in body
        assert 'uuid_send' not in body

    def test_never_references_the_partitioned_parent(self) -> None:
        body = self.render(make_catalog_row(0), make_catalog_row(1))
        assert re.search(r'FROM horsies_task_history\s', body) is None

    def test_each_leaf_is_probed_in_both_walks(self) -> None:
        rows = [make_catalog_row(offset) for offset in range(3)]
        body = self.render(*rows)
        for row in rows:
            assert body.count(f'FROM {row.leaf_name}\n') == 2

    def test_live_probes_before_forever_before_finite(self) -> None:
        row = make_catalog_row(0)
        body = self.render(row)
        live = body.index('FROM horsies_tasks\n')
        forever = body.index('FROM horsies_task_history_forever\n')
        finite = body.index(f'FROM {row.leaf_name}\n')
        assert live < forever < finite

    def test_pruned_walk_is_oldest_first_legacy_walk_newest_first(self) -> None:
        old, new = make_catalog_row(0), make_catalog_row(1)
        body = self.render(old, new)
        pruned_old = body.index(f'FROM {old.leaf_name}\n')
        pruned_new = body.index(f'FROM {new.leaf_name}\n')
        assert pruned_old < pruned_new
        legacy_old = body.rindex(f'FROM {old.leaf_name}\n')
        legacy_new = body.rindex(f'FROM {new.leaf_name}\n')
        assert legacy_new < legacy_old

    def test_clock_bound_subtraction_is_rendered(self) -> None:
        body = self.render(make_catalog_row(0))
        assert "v_birth_at - INTERVAL '5 seconds'" in body

    def test_leaf_exclusion_guards_use_upper_bounds(self) -> None:
        row = make_catalog_row(0)
        body = self.render(row)
        upper = row.upper_anchor.isoformat().replace('+00:00', 'Z')
        assert f"IF v_effective_birth < TIMESTAMPTZ '{upper}' THEN" in body

    def test_floor_check_present_only_when_floor_known(self) -> None:
        birth = BASE - timedelta(hours=2)
        with_floor = self.render(make_catalog_row(0, min_birth_at=birth))
        without_floor = self.render(
            make_catalog_row(0, min_birth_verified=False)
        )
        floor_literal = birth.isoformat().replace('+00:00', 'Z')
        assert f"IF v_birth_at < TIMESTAMPTZ '{floor_literal}' THEN" in with_floor
        assert 'IF v_birth_at < TIMESTAMPTZ' not in without_floor

    def test_v7_version_and_variant_bits_are_checked(self) -> None:
        body = self.render(make_catalog_row(0))
        assert '(get_byte(v_uuid_bytes, 6) >> 4) = 7' in body
        assert '(get_byte(v_uuid_bytes, 8) & 192) = 128' in body

    def test_boundary_scale_renders_all_leaves(self) -> None:
        rows = [make_catalog_row(offset) for offset in range(512)]
        body = self.render(*rows)
        assert body.count('IF v_effective_birth <') == 512
        for row in (rows[0], rows[255], rows[511]):
            assert body.count(f'FROM {row.leaf_name}\n') == 2


class TestRenderedProvenanceFunction:
    """The provenance variant shares the staged skeleton and cannot drift."""

    def render(self, *rows: LeafCatalogRow) -> str:
        return render_staged_provenance_function(
            manifest_from_catalog(list(rows))
        )

    def test_never_references_the_partitioned_parent(self) -> None:
        body = self.render(make_catalog_row(0), make_catalog_row(1))
        assert re.search(r'FROM horsies_task_history\s', body) is None

    def test_live_probe_carries_null_terminal_facts(self) -> None:
        body = self.render(make_catalog_row(0))
        assert "TRUE, 'LIVE', v_task_id, NULL, NULL, NULL" in body

    def test_history_probes_carry_terminal_facts(self) -> None:
        row = make_catalog_row(0)
        body = self.render(row)
        assert 'status, terminal_at, terminalization_kind' in body
        assert body.count(f'FROM {row.leaf_name}\n') == 2

    def test_probe_order_matches_the_identity_variant(self) -> None:
        rows = (make_catalog_row(0), make_catalog_row(1))
        identity_body = render_staged_lookup_function(
            manifest_from_catalog(list(rows))
        )
        provenance_body = self.render(*rows)
        for body in (identity_body, provenance_body):
            live = body.index('FROM horsies_tasks\n')
            forever = body.index('FROM horsies_task_history_forever\n')
            oldest = body.index(f'FROM {rows[0].leaf_name}\n')
            assert live < forever < oldest
        assert "v_birth_at - INTERVAL '5 seconds'" in provenance_body

    def test_floor_check_shares_the_manifest_rule(self) -> None:
        birth = BASE - timedelta(hours=2)
        with_floor = self.render(make_catalog_row(0, min_birth_at=birth))
        without_floor = self.render(
            make_catalog_row(0, min_birth_verified=False)
        )
        assert 'IF v_birth_at < TIMESTAMPTZ' in with_floor
        assert 'IF v_birth_at < TIMESTAMPTZ' not in without_floor

    def test_absence_row_is_six_wide(self) -> None:
        body = self.render(make_catalog_row(0))
        assert (
            'RETURN ROW(FALSE, NULL, NULL, NULL, NULL, NULL)'
            '::horsies_task_provenance;'
        ) in body

    def test_live_probe_is_conditional_on_include_live(self) -> None:
        body = self.render(make_catalog_row(0))
        assert 'p_include_live boolean DEFAULT TRUE' in body
        assert 'IF p_include_live THEN' in body
        live_guard = body.index('IF p_include_live THEN')
        live_probe = body.index('FROM horsies_tasks\n')
        forever_probe = body.index('FROM horsies_task_history_forever\n')
        assert live_guard < live_probe < forever_probe

    def test_identity_variant_keeps_the_single_parameter(self) -> None:
        rows = (make_catalog_row(0),)
        identity_body = render_staged_lookup_function(
            manifest_from_catalog(list(rows))
        )
        assert 'p_include_live' not in identity_body


class TestLookupRowDecode:
    def test_absent(self) -> None:
        assert (
            decode_lookup_row(
                found=False,
                location=None,
                found_task_id=None,
                fingerprint_version=None,
                command_fingerprint=None,
            )
            == TaskIdentityAbsent()
        )

    def test_live(self) -> None:
        assert decode_lookup_row(
            found=True,
            location='LIVE',
            found_task_id='0198c0de-0000-7000-8000-000000000001',
            fingerprint_version=1,
            command_fingerprint=b'\x01' * 32,
        ) == LiveTaskIdentity(
            task_id='0198c0de-0000-7000-8000-000000000001',
            fingerprint_version=1,
            command_fingerprint=b'\x01' * 32,
        )

    def test_history(self) -> None:
        outcome = decode_lookup_row(
            found=True,
            location='HISTORY',
            found_task_id='0198c0de-0000-7000-8000-000000000001',
            fingerprint_version=1,
            command_fingerprint=b'\x01' * 32,
        )
        assert isinstance(outcome, HistoryTaskIdentity)

    def test_absent_with_identity_values_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='absent lookup row'):
            decode_lookup_row(
                found=False,
                location='LIVE',
                found_task_id=None,
                fingerprint_version=None,
                command_fingerprint=None,
            )

    def test_unknown_location_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='unknown location'):
            decode_lookup_row(
                found=True,
                location='QUARANTINE',
                found_task_id='0198c0de-0000-7000-8000-000000000001',
                fingerprint_version=1,
                command_fingerprint=b'\x01' * 32,
            )

    def test_boolean_fingerprint_version_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='did not decode'):
            decode_lookup_row(
                found=True,
                location='LIVE',
                found_task_id='0198c0de-0000-7000-8000-000000000001',
                fingerprint_version=True,
                command_fingerprint=b'\x01' * 32,
            )

    def test_non_boolean_found_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='found flag'):
            decode_lookup_row(
                found=1,
                location=None,
                found_task_id=None,
                fingerprint_version=None,
                command_fingerprint=None,
            )
