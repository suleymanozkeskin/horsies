"""Ctrl+C ends a blocking ``WorkflowHandle.get()``.

The sync ``get()`` waits in ``CoreHandle.wait_workflow_result`` on the
calling thread. With no limit the wait has no call bound; the blocking form
checks signals every 100 ms, so Python's SIGINT handler ends the wait with
``KeyboardInterrupt`` within one slice. The wait is a read: it is dropped,
the workflow stays RUNNING, and the handle stays usable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.extension.fresh_interpreter import Database, run_case, seconds_of, text_of
from tests.extension.test_blocking_calls import SCHEDULING_TOLERANCE_S, SIGNAL_CHECK_SLICE_S

pytestmark = [pytest.mark.integration]

PROGRAM = 'workflow_wait'


def test_ctrl_c_ends_a_blocking_workflow_get(tmp_path: Path, extension_database: Database) -> None:
    report = run_case(PROGRAM, 'ctrl_c_ends_a_blocking_get', tmp_path, extension_database).report

    assert text_of(report, 'started') == 'Ok'
    # The call waited until the signal: it did not return on its own.
    assert text_of(report, 'ended') == 'left', report
    assert seconds_of(report, 'waited_before_signal_s') >= seconds_of(
        report, 'wait_before_signal_s',
    )
    assert seconds_of(report, 'seconds_after_signal') <= SIGNAL_CHECK_SLICE_S + SCHEDULING_TOLERANCE_S
    # The left wait changed nothing, and the handle and the broker still work.
    assert text_of(report, 'status_after') == 'RUNNING'
    assert text_of(report, 'cancelled') == 'Ok'
    assert text_of(report, 'closed') == 'Ok'
