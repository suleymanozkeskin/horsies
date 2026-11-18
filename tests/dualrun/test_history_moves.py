"""Dual-run of the terminal writes moving onto the moves of 0006_history_functions.sql.

Reference side: the terminal writes before the switch, with the single move
that takes the task advisory lock again and reads the live row again
(``reference/interim_moves.sql``, the writers of 0003), the array overload
of the miss classifier that those writes call
(``reference/interim_miss_classifier.sql``), and the writers of 0004 that
call history, with the array classifier that takes a fence kind and the
two-argument provenance reader (``reference/lifecycle_history_calls.sql``).
Candidate side: the baseline, where each single write passes the row it
locked to ``horsies_move_task_to_history(horsies_tasks, ...)``, each batch
write calls ``horsies_move_tasks_to_history``, and each miss passes its kind
and fence kind to the classifier of 0008. The function-call script
(``crates/horsies-core/src/sql_tests/cases/baseline_function_calls.sql``)
calls 15 terminal writes on a path that moves their rows: each moves one
task, and the pending expiry moves two, so history holds 16 rows.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.compare import ColumnDifference, compare_snapshots, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.frozen_definitions import (
    REFERENCE_FOLDER,
    BaselineAsInstalled,
    Definitions,
    FrozenDefinitions,
    run_function_calls_with,
)
from tests.dualrun.selfcheck import SelfCheckRun, SelfCheckStepFailed, straddles
from tests.dualrun.values import CalendarUnit

pytestmark = [pytest.mark.integration]

BOUNDARY_RETRY_LIMIT: Final = 1
"""A pair whose run window crosses an hour boundary runs again once."""

MOVED_TASKS: Final = 16
"""The history rows of the function-call script: one per terminal write,
and two for ``horsies_expire_pending_tasks`` (a plain task and a workflow
task)."""

BEFORE_THE_SWITCH: Final = FrozenDefinitions(
    paths=(
        REFERENCE_FOLDER / 'interim_miss_classifier.sql',
        REFERENCE_FOLDER / 'interim_moves.sql',
        REFERENCE_FOLDER / 'lifecycle_history_calls.sql',
    )
)

MakeDatabase = Callable[[], DualRunDatabase]

_CHANGED_FAILED_REASON_SQL: Final = """
CREATE FUNCTION dualrun_control_failed_reason() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.terminalization_kind = 'FAIL_RUNNING' THEN
        NEW.final_failed_reason := 'changed by the control';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER dualrun_control_failed_reason BEFORE INSERT ON horsies_task_history
    FOR EACH ROW EXECUTE FUNCTION dualrun_control_failed_reason();
"""


def _ok(result: Result[SelfCheckRun, SelfCheckStepFailed]) -> SelfCheckRun:
    match result:
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _pair(
    make: MakeDatabase, reference: Definitions, candidate: Definitions
) -> tuple[SelfCheckRun, SelfCheckRun]:
    for _ in range(BOUNDARY_RETRY_LIMIT + 1):
        reference_run = _ok(run_function_calls_with(make(), reference))
        candidate_run = _ok(run_function_calls_with(make(), candidate))
        if not any(
            straddles(run.window, CalendarUnit.HOUR)
            for run in (reference_run, candidate_run)
        ):
            return reference_run, candidate_run
    pytest.fail(
        f'the run windows crossed an hour boundary {BOUNDARY_RETRY_LIMIT + 1} times'
    )


def test_the_terminal_writes_store_what_they_stored_before_the_switch(
    empty_database: MakeDatabase,
) -> None:
    reference, candidate = _pair(
        empty_database, BEFORE_THE_SWITCH, BaselineAsInstalled()
    )

    diff = compare_snapshots(reference.snapshot, candidate.snapshot)

    assert diff.entries == (), render_diff(diff)
    history = candidate.snapshot.table('horsies_task_history')
    pending = candidate.snapshot.table('horsies_workflow_phase2_pending')
    assert len(history.rows) == MOVED_TASKS
    assert len(pending.rows) >= 1


def test_a_write_that_stores_another_value_shows_in_the_diff(
    empty_database: MakeDatabase, tmp_path: Path
) -> None:
    changed = tmp_path / 'changed_failed_reason.sql'
    changed.write_text(_CHANGED_FAILED_REASON_SQL, encoding='utf-8')

    reference, candidate = _pair(
        empty_database, BEFORE_THE_SWITCH, FrozenDefinitions(paths=(changed,))
    )

    diff = compare_snapshots(reference.snapshot, candidate.snapshot)

    changed_columns = {
        (entry.table, entry.column)
        for entry in diff.entries
        if isinstance(entry, ColumnDifference)
    }
    assert changed_columns == {('horsies_task_history', 'final_failed_reason')}, (
        render_diff(diff)
    )
