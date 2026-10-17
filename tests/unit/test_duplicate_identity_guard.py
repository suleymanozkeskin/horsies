"""The pruned duplicate-identity guard and its clock-skew exceptions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.history.names import CLOCK_SKEW_EXCEPTIONS
from horsies.core.history.reads.lookup_generation import (
    LookupLeaf,
    LookupManifest,
    render_staged_duplicate_guard_function,
    render_staged_provenance_function,
)
from horsies.core.history.terminalization.move import (
    completion_family_fragments,
    expiry_family_fragments,
)

pytestmark = [pytest.mark.unit]

DAY = datetime(2026, 10, 1, tzinfo=timezone.utc)
LEAVES = tuple(
    LookupLeaf(
        relation_name=f'horsies_task_history_probe_{index}',
        lower_anchor=DAY + timedelta(days=index),
        upper_anchor=DAY + timedelta(days=index + 1),
        min_birth_at=None,
    )
    for index in range(3)
)
MANIFEST = LookupManifest(leaves=LEAVES, birth_floor=DAY)
GUARD = render_staged_duplicate_guard_function(MANIFEST)


def test_guard_probes_pruned_and_legacy_leaves_only() -> None:
    for leaf in LEAVES:
        # One pruned probe and one legacy (non-v7) probe per leaf.
        assert GUARD.count(f'FROM {leaf.relation_name} h') == 2
    assert 'v_effective_birth <' in GUARD
    assert 'v_effective_birth >=' not in GUARD
    assert 'horsies_tasks' not in GUARD


def test_the_readers_keep_their_fallback_probes() -> None:
    assert 'v_effective_birth >=' in render_staged_provenance_function(MANIFEST)


def test_absence_consults_the_clock_skew_exceptions() -> None:
    absence = (
        f'RETURN EXISTS (SELECT 1 FROM {CLOCK_SKEW_EXCEPTIONS} '
        'WHERE task_id = p_task_id);'
    )
    # Below the birth floor, and after every probe found nothing.
    assert GUARD.count(absence) == 2


@pytest.mark.parametrize(
    'body',
    [completion_family_fragments()[1], expiry_family_fragments()[1]],
    ids=['single_move', 'batch_move'],
)
def test_moves_record_skewed_ids_between_the_insert_and_the_deletes(
    body: str,
) -> None:
    insert = body.index('INSERT INTO horsies_task_history')
    record = body.index(f'INSERT INTO {CLOCK_SKEW_EXCEPTIONS}')
    deletes = body.index('DELETE FROM horsies_tasks')
    assert insert < record < deletes
    assert "INTERVAL '5 seconds' >" in body[record:deletes]
