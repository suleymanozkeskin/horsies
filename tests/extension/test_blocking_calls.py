"""The blocking call form of ``horsies._core``: the GIL is released while a
call runs; a raising signal handler ends a call within one 100 ms slice; a
left read is dropped and a left write runs to its end."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest

from tests.extension.fresh_interpreter import (
    Database,
    flag_of,
    integer_of,
    report_at,
    run_case,
    seconds_of,
    text_of,
    texts_of,
)
from tests.extension.programs.report import Report

pytestmark = [pytest.mark.integration]

PROGRAM = 'blocking_calls'

SIGNAL_CHECK_SLICE_S: Final = 0.1
"""The time a blocking call keeps the GIL released before it checks signals
(``SIGNAL_CHECK_SLICE`` in ``crates/horsies-python/src/bridge/blocking.rs``)."""

SCHEDULING_TOLERANCE_S: Final = 0.3
"""Time on top of one slice for the OS to run the main thread and for the
handler to run on it, on a loaded test machine."""

MIN_TICKS: Final = 1_000
"""Counts the second thread must make while the call waits. A call that held
the GIL would leave it at zero, and the call would never end."""


def test_a_blocking_call_releases_the_gil(tmp_path: Path, extension_database: Database) -> None:
    report = run_case(PROGRAM, 'gil_released', tmp_path, extension_database).report

    assert text_of(report, 'warm_up') == 'DatabasePingRecord'
    assert flag_of(report_at(report, 'waiter'), 'waited'), report
    # The read ended only because the other thread released the lock.
    assert text_of(report, 'outcome') == 'TaskInfoNotFound'
    assert integer_of(report, 'ticks') >= MIN_TICKS


@pytest.fixture(scope='module')
def signal_report(
    tmp_path_factory: pytest.TempPathFactory, extension_database: Database,
) -> Report:
    """One run of the case that leaves a blocking read and a blocking write."""
    return run_case(
        PROGRAM,
        'signal_leaves_calls',
        tmp_path_factory.mktemp('signal'),
        extension_database,
    ).report


@pytest.mark.parametrize('call', ['read', 'write'])
def test_a_raising_signal_handler_ends_a_blocking_call_within_one_slice(
    signal_report: Report, call: str,
) -> None:
    assert text_of(signal_report, 'warm_up_ping') == 'DatabasePingRecord'
    assert text_of(signal_report, 'warm_up_send') == 'Enqueued'
    left = report_at(signal_report, call)
    assert flag_of(report_at(left, 'waiter'), 'waited'), left
    assert text_of(left, 'ended') == 'left'
    assert seconds_of(left, 'seconds_after_signal') <= SIGNAL_CHECK_SLICE_S + SCHEDULING_TOLERANCE_S


def test_a_left_write_runs_to_its_end_and_a_left_read_is_dropped(signal_report: Report) -> None:
    messages = texts_of(signal_report, 'bridge_messages')

    # The write: its row exists and the bridge logged its end.
    assert flag_of(signal_report, 'write_ended'), messages
    assert 'a write of CoreHandle.enqueue ended after its caller left' in messages
    # The read: dropped, so nothing of it ran on to an end to log.
    assert [message for message in messages if 'CoreHandle.task_info' in message] == []
    assert text_of(signal_report, 'ping_after') == 'DatabasePingRecord'
