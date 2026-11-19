"""The panic rules of a producer process: a panic in an operation raises
``PanicException`` and the core keeps running; a panic in the log forwarder
stops the core, and the call in flight and every later call raise
``CoreStopped(subject, cause)``."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

import pytest

from tests.extension.fresh_interpreter import (
    Database,
    flag_of,
    integer_of,
    report_at,
    run_case,
    text_of,
    texts_of,
)
from tests.extension.programs.report import Report

pytestmark = [pytest.mark.integration]

PROGRAM = 'panics'

SOURCE_PANIC_TEXT: Final = 'planned panic in the child spec source'
FORWARDER_PANIC_TEXT: Final = 'planned panic in a log handler'
FORWARDER_SUBJECT: Final = 'thread horsies-log-forwarder'

SOURCE_LOCATION: Final = re.compile(r'\(at [^()]*child_spec\.rs:\d+\)')
"""The source location that the panic hook adds to the cause: the panic of
the operation is in ``clients/python/bridge/src/child_spec.rs``."""


def test_a_panic_in_an_operation_raises_panic_exception_and_the_core_runs_on(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'panic_in_an_operation', tmp_path, extension_database).report
    text = text_of(report, 'text')

    assert text_of(report, 'caught_by') == 'PanicException', report
    assert integer_of(report, 'source_calls') == 1
    assert text.startswith('panic in core operation CoreHandle.start_workflow_async: '), text
    assert SOURCE_PANIC_TEXT in text
    assert SOURCE_LOCATION.search(text) is not None, text
    # The core runs on, and the start wrote nothing.
    assert text_of(report, 'core_state_after') == 'running'
    assert text_of(report, 'ping_after') == 'DatabasePingRecord'
    assert text_of(report, 'status_after') == 'WorkflowAbsent'
    assert texts_of(report, 'bridge_errors') == [text]


def _assert_stopped(ended: Report) -> None:
    assert text_of(ended, 'ended') == 'raised', ended
    assert text_of(ended, 'class') == 'CoreStopped', ended
    arguments = texts_of(ended, 'arguments')
    assert len(arguments) == 2, arguments
    subject, cause = arguments
    assert subject == FORWARDER_SUBJECT
    assert cause.startswith(FORWARDER_PANIC_TEXT), cause


def test_a_panic_in_the_log_forwarder_stops_the_core(
    tmp_path: Path, extension_database: Database,
) -> None:
    run = run_case(PROGRAM, 'panic_in_the_log_forwarder', tmp_path, extension_database)
    report = run.report

    warm_up = report_at(report, 'warm_up')
    assert text_of(warm_up, 'class') == 'TaskInfoNotFound', warm_up
    assert flag_of(report_at(report, 'waiter'), 'waited'), report
    # The read that waited for the lock when the forwarder panicked.
    _assert_stopped(report_at(report, 'in_flight'))
    # Every later call, of each form.
    for later in ('later_async', 'later_blocking', 'later_pure'):
        _assert_stopped(report_at(report, later))
    assert text_of(report, 'core_state_after') == 'stopped'
    assert (
        f'horsies: the core stopped in process {integer_of(report, "pid")}: '
        f'panic in {FORWARDER_SUBJECT}: {FORWARDER_PANIC_TEXT}'
    ) in run.stderr
