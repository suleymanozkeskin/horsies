"""The pause guard of the engine's progression.

The worker's phase 2 and workflow recovery run the engine's progression
(``apply_node_progression``), which stops at a PAUSED workflow. Phase-2
recovery runs ``horsies_workflow_phase2_recover``, which applies the same
guard.
"""

from __future__ import annotations

import pytest

from horsies.core.models.workflow import WorkflowStatus
from horsies.core.workflows.engine import pause_halts_progression

pytestmark = [pytest.mark.unit]


class TestPauseGuard:
    def test_paused_halts(self) -> None:
        assert pause_halts_progression(WorkflowStatus.PAUSED.value)

    def test_every_other_workflow_status_progresses(self) -> None:
        for status in set(WorkflowStatus) - {WorkflowStatus.PAUSED}:
            assert not pause_halts_progression(status.value), status

    def test_absent_status_progresses(self) -> None:
        assert not pause_halts_progression(None)
