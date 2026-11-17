"""The async call form of ``horsies._core``: a call returns a future of the
calling loop, on asyncio and on uvloop; 1,000 calls in flight add no OS
thread; a cancelled read is dropped and a cancelled write runs to its end."""

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
    text_of,
    texts_of,
)
from tests.extension.programs.report import Report

pytestmark = [pytest.mark.integration]

PROGRAM = 'async_calls'

LOOP_KINDS: Final = ('asyncio', 'uvloop')

RUNTIME_WORKER_THREADS: Final = 2
"""Worker threads of the core runtime of a Python process
(``PRODUCER_RUNTIME_WORKER_THREADS`` in ``crates/horsies-python/src/process_core.rs``)."""

LOG_FORWARDER_THREADS: Final = 1
"""The one log forwarder thread of a process
(``crates/horsies-python/src/logging/forwarder.rs``)."""

CONCURRENT_AWAITS: Final = 1_000


def _loop_report(report: Report) -> Report:
    """``report`` after the check that its loop was installed. A missing
    uvloop fails the test: the dev dependency group must carry it."""
    assert flag_of(report, 'loop_available'), (
        f'{text_of(report, "loop")} is not installed in the test environment; '
        'the dev dependency group of pyproject.toml must carry it (#1784)'
    )
    return report


@pytest.mark.parametrize('loop', LOOP_KINDS)
def test_an_async_call_returns_a_future_of_the_calling_loop(
    tmp_path: Path, extension_database: Database, loop: str,
) -> None:
    report = _loop_report(
        run_case(PROGRAM, f'future_on_calling_loop_{loop}', tmp_path, extension_database).report,
    )

    assert flag_of(report, 'is_future')
    assert flag_of(report, 'on_calling_loop')
    assert text_of(report, 'outcome') == 'DatabasePingRecord'


@pytest.mark.parametrize('loop', LOOP_KINDS)
def test_a_thousand_concurrent_awaits_add_no_os_thread(
    tmp_path: Path, extension_database: Database, loop: str,
) -> None:
    report = _loop_report(
        run_case(PROGRAM, f'thousand_awaits_{loop}', tmp_path, extension_database).report,
    )
    core_threads = 1 + RUNTIME_WORKER_THREADS + LOG_FORWARDER_THREADS

    assert text_of(report, 'first') == 'DatabasePingRecord'
    assert integer_of(report, 'threads_before_start') == 1
    assert integer_of(report, 'threads_after_start') == core_threads
    assert integer_of(report, 'max_threads_in_flight') == core_threads, report
    assert integer_of(report, 'min_threads_in_flight') == core_threads, report
    assert integer_of(report_at(report, 'outcomes'), 'DatabasePingRecord') == CONCURRENT_AWAITS


@pytest.fixture(scope='module')
def cancel_report(
    tmp_path_factory: pytest.TempPathFactory, extension_database: Database,
) -> Report:
    """One run of the case that cancels an awaited read and an awaited write."""
    return run_case(
        PROGRAM,
        'cancel_leaves_calls',
        tmp_path_factory.mktemp('cancel'),
        extension_database,
    ).report


@pytest.mark.parametrize('call', ['read', 'write'])
def test_a_cancel_reaches_an_awaited_call_in_flight(cancel_report: Report, call: str) -> None:
    assert text_of(cancel_report, 'warm_up_ping') == 'DatabasePingRecord'
    assert text_of(cancel_report, 'warm_up_send') == 'Enqueued'
    cancelled = report_at(cancel_report, call)
    assert flag_of(report_at(cancelled, 'waiter'), 'waited'), cancelled
    assert flag_of(cancelled, 'cancel_accepted')
    assert flag_of(cancelled, 'cancelled')


def test_a_cancelled_write_runs_to_its_end_and_a_cancelled_read_is_dropped(
    cancel_report: Report,
) -> None:
    messages = texts_of(cancel_report, 'bridge_messages')

    # The write: its row exists and the bridge logged its discarded outcome.
    assert flag_of(cancel_report, 'write_ended'), messages
    assert (
        'CoreHandle.enqueue_async ended after its awaiting future was cancelled; '
        'the outcome was discarded'
    ) in messages
    # The read: dropped on the runtime, so it has no outcome to discard.
    assert [message for message in messages if 'CoreHandle.task_info_async' in message] == []
    assert text_of(cancel_report, 'ping_after') == 'DatabasePingRecord'
