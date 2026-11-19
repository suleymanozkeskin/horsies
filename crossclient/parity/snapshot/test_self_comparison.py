"""Self-comparison runs: the same real writes into two fresh databases compare equal.

A difference here is a defect of the catalog or of the snapshot code, not of
the code under test: both databases ran the same writes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final, TypeVar

import pytest

from horsies.core.types.result import Err, Ok, Result
from crossclient.parity.snapshot.compare import compare_snapshots, render_diff
from crossclient.parity.snapshot.databases import DualRunDatabase
from crossclient.parity.snapshot.selfcheck import (
    SelfCheckRun,
    SelfCheckStepFailed,
    run_function_calls,
    run_seeded_history,
    straddles,
)
from crossclient.parity.snapshot.values import CalendarUnit, DatedName, ExactValue

pytestmark = [pytest.mark.integration]

BOUNDARY_RETRY_LIMIT: Final = 1
"""A pair whose run window crosses an hour boundary runs again once."""

T = TypeVar('T')
MakeDatabase = Callable[[], DualRunDatabase]


def _ok(result: Result[T, SelfCheckStepFailed]) -> T:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _pair(
    make: MakeDatabase,
    run: Callable[[DualRunDatabase], Result[SelfCheckRun, SelfCheckStepFailed]],
) -> tuple[SelfCheckRun, SelfCheckRun]:
    for _ in range(BOUNDARY_RETRY_LIMIT + 1):
        reference = _ok(run(make()))
        candidate = _ok(run(make()))
        if not any(
            straddles(each.window, CalendarUnit.HOUR) for each in (reference, candidate)
        ):
            return reference, candidate
    pytest.fail(
        f'the run windows crossed an hour boundary {BOUNDARY_RETRY_LIMIT + 1} times'
    )


def _row_counts(run: SelfCheckRun) -> dict[str, int]:
    return {
        table.rules.name: len(table.rows) for table in run.snapshot.tables if table.rows
    }


def test_every_baseline_function_writes_the_same_state_twice(
    empty_database: MakeDatabase,
) -> None:
    reference, candidate = _pair(empty_database, run_function_calls)

    diff = compare_snapshots(reference.snapshot, candidate.snapshot)

    assert diff.entries == (), render_diff(diff)
    counts = _row_counts(reference)
    assert counts['horsies_task_history'] >= 15
    assert counts['horsies_workflow_tasks'] >= 5
    assert counts['horsies_workflow_phase2_pending'] >= 1


def test_coverage_pass_history_move_and_detached_leaf_write_the_same_state_twice(
    empty_database: MakeDatabase,
) -> None:
    reference, candidate = _pair(empty_database, run_seeded_history)

    diff = compare_snapshots(reference.snapshot, candidate.snapshot)

    assert diff.entries == (), render_diff(diff)
    history = reference.snapshot.table('horsies_task_history')
    (moved,) = history.rows
    leaf = moved[history.rules.position('tableoid')]
    match leaf:
        case DatedName(
            prefix='horsies_task_history_standard_30d', unit=CalendarUnit.DAY
        ):
            pass
        case _:
            raise AssertionError(
                f'the moved row is in leaf {leaf}, not a daily standard_30d leaf'
            )
    catalog = reference.snapshot.table('horsies_task_history_leaf_catalog')
    names = {row[catalog.rules.position('leaf_name')] for row in catalog.rows}
    assert ExactValue(text='horsies_task_history_forever_open') in names
    assert any(
        isinstance(name, DatedName) and name.unit is CalendarUnit.HOUR for name in names
    )
