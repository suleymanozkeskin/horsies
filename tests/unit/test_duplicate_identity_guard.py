"""The moves record the clock-skew exceptions that the duplicate-identity guard reads."""

from __future__ import annotations

import pytest

from horsies.core.history.names import CLOCK_SKEW_EXCEPTIONS
from tests.baseline_sql import baseline_function

pytestmark = [pytest.mark.unit]


@pytest.mark.parametrize(
    'body',
    [
        baseline_function('horsies_move_task_to_history'),
        baseline_function('horsies_move_tasks_to_history'),
    ],
    ids=['single_move', 'batch_move'],
)
def test_moves_record_skewed_ids_between_the_insert_and_the_deletes(
    body: str,
) -> None:
    insert = body.index('INSERT INTO horsies_task_history')
    record = body.index(f'INSERT INTO {CLOCK_SKEW_EXCEPTIONS}')
    deletes = body.index('DELETE FROM horsies_tasks')
    assert insert < record < deletes
    # The single move tests the skew before its record insert; the batch
    # move filters the rows of its record insert by it.
    assert "interval '5 seconds' >" in body[insert:deletes].lower()
