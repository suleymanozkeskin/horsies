"""The fork rule: after the core started, a process forked without ``exec``
refuses every core call with ``ForkedAfterCoreStart`` within a bound; a fork
before the core started gives a child that starts its own core."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest

from tests.extension.fresh_interpreter import (
    Database,
    integer_of,
    report_at,
    run_case,
    seconds_of,
    text_of,
    texts_of,
)
from tests.extension.programs.report import Report

pytestmark = [pytest.mark.integration]

PROGRAM = 'fork_rule'

FORK_CHILD_BOUND_S: Final = 10.0
"""The bound of a fork child in ``programs/fork_rule.py``; a child that hangs
is killed at it."""


def _assert_refused(ended: Report, *, core_pid: int, this_pid: int) -> None:
    assert text_of(ended, 'ended') == 'raised', ended
    assert text_of(ended, 'class') == 'ForkedAfterCoreStart', ended
    arguments = texts_of(ended, 'arguments')
    assert len(arguments) == 3, arguments
    refused_core_pid, refused_this_pid, advice = arguments
    assert refused_core_pid == str(core_pid)
    assert refused_this_pid == str(this_pid)
    assert 'spawn' in advice and 'forkserver' in advice, advice


def test_a_fork_after_the_core_started_refuses_every_core_call(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'fork_after_start', tmp_path, extension_database).report
    parent_pid = integer_of(report, 'pid')
    forked = report_at(report, 'forked')
    child = report_at(forked, 'child')
    child_pid = integer_of(forked, 'child_pid')

    assert text_of(report, 'ping') == 'DatabasePingRecord'
    assert text_of(forked, 'child_status') == 'exited 0', forked
    assert seconds_of(forked, 'child_seconds') < FORK_CHILD_BOUND_S
    assert integer_of(child, 'pid') == child_pid
    assert text_of(child, 'core_state') == 'inherited'
    for form in ('pure', 'blocking', 'async'):
        _assert_refused(report_at(child, form), core_pid=parent_pid, this_pid=child_pid)
    # The parent's core is not touched by the fork.
    assert text_of(report, 'parent_ping_after_fork') == 'DatabasePingRecord'
    assert text_of(report, 'parent_core_state') == 'running'


def test_a_fork_before_the_core_started_gives_a_working_child(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'fork_before_start', tmp_path, extension_database).report
    forked = report_at(report, 'forked')
    child = report_at(forked, 'child')

    assert text_of(forked, 'child_status') == 'exited 0', forked
    assert text_of(child, 'core_state_before') == 'not_started'
    assert text_of(child, 'ping') == 'DatabasePingRecord'
    assert text_of(child, 'core_state_after') == 'running'
    assert text_of(report, 'parent_core_state') == 'not_started'


def test_a_fork_start_pool_worker_refuses_core_calls(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'fork_pool_after_start', tmp_path, extension_database).report
    worker = report_at(report, 'worker')

    assert text_of(report, 'ping') == 'DatabasePingRecord'
    assert seconds_of(report, 'worker_seconds') < FORK_CHILD_BOUND_S
    assert text_of(worker, 'core_state') == 'inherited'
    _assert_refused(
        report_at(worker, 'pure'),
        core_pid=integer_of(report, 'pid'),
        this_pid=integer_of(worker, 'pid'),
    )
