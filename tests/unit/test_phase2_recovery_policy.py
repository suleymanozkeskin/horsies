"""The progression policy shared by both paths into a terminal node.

A workflow node reaches its terminal status two ways: the in-process
completion, which holds a TaskResult, and the outbox-driven recovery,
which holds the task's terminal status. Neither caller can honestly be
handed the other's input, so the policy is expressed twice — and these
pins hold the two expressions to one answer, walking the CANONICAL
terminal set so a status added later (as EXPIRED was) enters the pin by
construction rather than by someone remembering to extend a list.
"""

from __future__ import annotations

import pytest

from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.tasks import (
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import WorkflowStatus, WorkflowTaskStatus
from horsies.core.types.status import TASK_TERMINAL_STATES, TaskStatus
from horsies.core.workflows.engine import (
    node_status_for_terminal_task,
    pause_halts_progression,
)
from horsies.core.workflows.phase2_recovery import (
    DISCOVER_PENDING_SQL,
    DURABLE_DISPOSITIONS,
    _recovered_failure_result,
)

pytestmark = [pytest.mark.unit]


def _in_process_node_status(status: TaskStatus) -> str:
    """The in-process path's own derivation, over its own input.

    A completed task carries an ok result and every other terminal
    outcome carries an err, so reconstructing the TaskResult from the
    status reproduces exactly what that path would hold.
    """
    result: TaskResult[object, TaskError] = (
        TaskResult(ok=None)
        if status is TaskStatus.COMPLETED
        else TaskResult(err=TaskError(error_code='X', message='m'))
    )
    return (
        WorkflowTaskStatus.COMPLETED.value
        if result.is_ok()
        else WorkflowTaskStatus.FAILED.value
    )


class TestTerminalStatusMapping:
    def test_every_canonical_terminal_status_maps(self) -> None:
        # Drawn from the enum, never a hand-list: a member added later
        # is covered here the moment it joins the terminal set.
        assert TASK_TERMINAL_STATES
        for status in TASK_TERMINAL_STATES:
            mapped = node_status_for_terminal_task(status.value)
            assert mapped in {
                WorkflowTaskStatus.COMPLETED.value,
                WorkflowTaskStatus.FAILED.value,
            }, status

    def test_both_expressions_of_the_policy_agree(self) -> None:
        for status in TASK_TERMINAL_STATES:
            assert node_status_for_terminal_task(
                status.value
            ) == _in_process_node_status(status), status

    def test_only_completion_makes_a_completed_node(self) -> None:
        assert (
            node_status_for_terminal_task(TaskStatus.COMPLETED.value)
            == WorkflowTaskStatus.COMPLETED.value
        )
        for status in TASK_TERMINAL_STATES - {TaskStatus.COMPLETED}:
            assert (
                node_status_for_terminal_task(status.value)
                == WorkflowTaskStatus.FAILED.value
            ), status

    def test_a_non_terminal_status_raises_rather_than_defaulting(
        self,
    ) -> None:
        for status in set(TaskStatus) - TASK_TERMINAL_STATES:
            with pytest.raises(ValueError):
                node_status_for_terminal_task(status.value)

    def test_an_unknown_status_raises(self) -> None:
        with pytest.raises(ValueError):
            node_status_for_terminal_task('NOT_A_STATUS')


class TestPauseGuard:
    def test_paused_halts(self) -> None:
        assert pause_halts_progression(WorkflowStatus.PAUSED.value)

    def test_every_other_workflow_status_progresses(self) -> None:
        for status in set(WorkflowStatus) - {WorkflowStatus.PAUSED}:
            assert not pause_halts_progression(status.value), status

    def test_absent_status_progresses(self) -> None:
        assert not pause_halts_progression(None)


class TestDiscoveryBounds:
    def test_oldest_first_on_the_age_index(self) -> None:
        assert 'ORDER BY created_at, task_id' in DISCOVER_PENDING_SQL.text

    def test_the_grace_window_excludes_recent_rows(self) -> None:
        # Strictly younger-than is what leaves an in-flight finalizer
        # alone; an inclusive bound would race it.
        assert 'created_at < NOW()' in DISCOVER_PENDING_SQL.text
        assert ':grace_ms' in DISCOVER_PENDING_SQL.text

    def test_the_pass_is_capped(self) -> None:
        assert 'LIMIT CAST(:max_rows AS bigint)' in DISCOVER_PENDING_SQL.text


class TestDurableDispositions:
    def test_only_the_three_durable_ones_are_listed(self) -> None:
        # The retaining dispositions must NOT appear: this set is what
        # the driver treats as progress, and a retaining disposition
        # counted as progress would hide a stuck population.
        assert DURABLE_DISPOSITIONS == {
            'APPLIED_TO_NODE',
            'ALREADY_APPLIED',
            'SUPERSEDED_BY_WORKFLOW_TERMINAL',
        }


class TestRecoveredFailureResult:
    def test_the_stored_error_survives(self) -> None:
        # Built through the production encoder rather than hand-written,
        # so the pin reads what the node actually holds.
        stored = dumps_json(
            encode_task_result(
                TaskResult(
                    err=TaskError(
                        error_code=OperationalErrorCode.WORKER_CRASHED,
                        message='the real one',
                    ),
                ),
                JsonValue,
            )
        ).unwrap()
        result = _recovered_failure_result('FAILED', stored)
        assert result.is_err()
        assert result.unwrap_err().message == 'the real one'

    def test_a_missing_result_names_the_terminal_status(self) -> None:
        for status, code in (
            ('CANCELLED', 'TASK_CANCELLED'),
            ('EXPIRED', 'TASK_EXPIRED'),
            ('FAILED', 'WORKER_CRASHED'),
        ):
            result = _recovered_failure_result(status, None)
            assert result.is_err()
            assert result.unwrap_err().error_code.value == code, status

    def test_corrupt_stored_json_still_yields_a_true_error(self) -> None:
        result = _recovered_failure_result('FAILED', 'not json at all')
        assert result.is_err()
        assert result.unwrap_err().error_code.value == 'WORKER_CRASHED'
