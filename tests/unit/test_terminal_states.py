"""Unit tests for terminal state frozensets and is_terminal properties."""

from __future__ import annotations

import pytest

from horsies.core.types.status import TaskStatus, TASK_TERMINAL_STATES
from horsies.core.models.workflow import (
    WorkflowStatus,
    WorkflowTaskStatus,
    WORKFLOW_TERMINAL_STATES,
    WORKFLOW_TASK_TERMINAL_STATES,
)


@pytest.mark.unit
class TestTaskStatusTerminal:
    """Tests for TaskStatus terminal states."""

    def test_terminal_members(self) -> None:
        """is_terminal returns True for each terminal member."""
        assert TaskStatus.COMPLETED.is_terminal is True
        assert TaskStatus.FAILED.is_terminal is True
        assert TaskStatus.CANCELLED.is_terminal is True

    def test_non_terminal_members(self) -> None:
        """is_terminal returns False for each non-terminal member."""
        assert TaskStatus.PENDING.is_terminal is False
        assert TaskStatus.CLAIMED.is_terminal is False
        assert TaskStatus.RUNNING.is_terminal is False
        assert TaskStatus.REQUEUED.is_terminal is False

    def test_frozenset_contents(self) -> None:
        """Frozenset contains exactly the expected terminal states."""
        assert TASK_TERMINAL_STATES == frozenset({
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        })

    def test_exhaustiveness(self) -> None:
        """Every TaskStatus member is either terminal or non-terminal."""
        for member in TaskStatus:
            assert isinstance(member.is_terminal, bool)
        # All members accounted for
        terminal = {m for m in TaskStatus if m.is_terminal}
        non_terminal = {m for m in TaskStatus if not m.is_terminal}
        assert terminal | non_terminal == set(TaskStatus)
        assert terminal & non_terminal == set()

    def test_is_terminal_matches_frozenset(self) -> None:
        """is_terminal property agrees with TASK_TERMINAL_STATES membership."""
        for member in TaskStatus:
            assert member.is_terminal == (member in TASK_TERMINAL_STATES)

    def test_frozenset_is_immutable(self) -> None:
        """Frozenset cannot be modified."""
        with pytest.raises(AttributeError):
            TASK_TERMINAL_STATES.add(TaskStatus.PENDING)  # type: ignore[attr-defined]


@pytest.mark.unit
class TestWorkflowStatusTerminal:
    """Tests for WorkflowStatus terminal states."""

    def test_terminal_members(self) -> None:
        """is_terminal returns True for each terminal member."""
        assert WorkflowStatus.COMPLETED.is_terminal is True
        assert WorkflowStatus.FAILED.is_terminal is True
        assert WorkflowStatus.CANCELLED.is_terminal is True

    def test_non_terminal_members(self) -> None:
        """is_terminal returns False for each non-terminal member."""
        assert WorkflowStatus.PENDING.is_terminal is False
        assert WorkflowStatus.RUNNING.is_terminal is False
        assert WorkflowStatus.PAUSED.is_terminal is False

    def test_frozenset_contents(self) -> None:
        """Frozenset contains exactly the expected terminal states."""
        assert WORKFLOW_TERMINAL_STATES == frozenset({
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        })

    def test_exhaustiveness(self) -> None:
        """Every WorkflowStatus member is either terminal or non-terminal."""
        for member in WorkflowStatus:
            assert isinstance(member.is_terminal, bool)
        terminal = {m for m in WorkflowStatus if m.is_terminal}
        non_terminal = {m for m in WorkflowStatus if not m.is_terminal}
        assert terminal | non_terminal == set(WorkflowStatus)
        assert terminal & non_terminal == set()

    def test_is_terminal_matches_frozenset(self) -> None:
        """is_terminal property agrees with WORKFLOW_TERMINAL_STATES membership."""
        for member in WorkflowStatus:
            assert member.is_terminal == (member in WORKFLOW_TERMINAL_STATES)

    def test_frozenset_is_immutable(self) -> None:
        """Frozenset cannot be modified."""
        with pytest.raises(AttributeError):
            WORKFLOW_TERMINAL_STATES.add(WorkflowStatus.PENDING)  # type: ignore[attr-defined]


@pytest.mark.unit
class TestWorkflowTaskStatusTerminal:
    """Tests for WorkflowTaskStatus terminal states."""

    def test_terminal_members(self) -> None:
        """is_terminal returns True for each terminal member."""
        assert WorkflowTaskStatus.COMPLETED.is_terminal is True
        assert WorkflowTaskStatus.FAILED.is_terminal is True
        assert WorkflowTaskStatus.SKIPPED.is_terminal is True

    def test_non_terminal_members(self) -> None:
        """is_terminal returns False for each non-terminal member."""
        assert WorkflowTaskStatus.PENDING.is_terminal is False
        assert WorkflowTaskStatus.READY.is_terminal is False
        assert WorkflowTaskStatus.ENQUEUED.is_terminal is False
        assert WorkflowTaskStatus.RUNNING.is_terminal is False

    def test_frozenset_contents(self) -> None:
        """Frozenset contains exactly the expected terminal states."""
        assert WORKFLOW_TASK_TERMINAL_STATES == frozenset({
            WorkflowTaskStatus.COMPLETED,
            WorkflowTaskStatus.FAILED,
            WorkflowTaskStatus.SKIPPED,
        })

    def test_exhaustiveness(self) -> None:
        """Every WorkflowTaskStatus member is either terminal or non-terminal."""
        for member in WorkflowTaskStatus:
            assert isinstance(member.is_terminal, bool)
        terminal = {m for m in WorkflowTaskStatus if m.is_terminal}
        non_terminal = {m for m in WorkflowTaskStatus if not m.is_terminal}
        assert terminal | non_terminal == set(WorkflowTaskStatus)
        assert terminal & non_terminal == set()

    def test_is_terminal_matches_frozenset(self) -> None:
        """is_terminal property agrees with WORKFLOW_TASK_TERMINAL_STATES membership."""
        for member in WorkflowTaskStatus:
            assert member.is_terminal == (member in WORKFLOW_TASK_TERMINAL_STATES)

    def test_frozenset_is_immutable(self) -> None:
        """Frozenset cannot be modified."""
        with pytest.raises(AttributeError):
            WORKFLOW_TASK_TERMINAL_STATES.add(WorkflowTaskStatus.PENDING)  # type: ignore[attr-defined]
