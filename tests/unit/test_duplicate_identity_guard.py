"""The moves record the clock-skew exceptions that the duplicate-identity guard reads."""

from __future__ import annotations

import pytest

from horsies.core.history.names import CLOCK_SKEW_EXCEPTIONS
from horsies.core.history.terminalization.move import (
    completion_family_fragments,
    expiry_family_fragments,
)

pytestmark = [pytest.mark.unit]


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
