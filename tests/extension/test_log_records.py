"""The log bridge of ``horsies._core``: core records reach
``logging.getLogger("horsies.<component>")`` with the core's time and thread
name; a level change takes effect after ``refresh_log_levels()``; with a
full channel, DEBUG and INFO records are dropped and counted, WARNING and
ERROR records wait and then go to stderr, and one WARNING with both counts
follows."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest

from tests.extension.fresh_interpreter import (
    Database,
    flag_of,
    integer_of,
    numbers_of,
    report_at,
    reports_of,
    run_case,
    seconds_of,
    text_of,
    texts_of,
)
pytestmark = [pytest.mark.integration]

PROGRAM = 'log_records'

RUNTIME_THREAD_NAME: Final = 'horsies-core'
"""The name of the core runtime's worker threads (``RUNTIME_THREAD_NAME`` in
``crates/horsies-python/src/process_core.rs``)."""

HOLD_S: Final = 1.0
"""How long ``programs/log_records.py`` holds the forwarder in the time case."""

LEVEL_REFRESH_INTERVAL_S: Final = 5.0
"""The forwarder reads the levels again at this interval
(``LEVEL_REFRESH_INTERVAL`` in ``crates/horsies-python/src/logging/forwarder.rs``)."""

LOG_CHANNEL_CAPACITY: Final = 8_192
"""``LOG_CHANNEL_CAPACITY`` in ``crates/horsies-python/src/logging/mod.rs``."""

FORWARD_BATCH: Final = 256
"""``FORWARD_BATCH`` in ``crates/horsies-python/src/logging/forwarder.rs``: the
records the blocked forwarder holds besides the channel."""

FLOOD_PINGS: Final = 10_000
PANICKING_STARTS: Final = 2

SPILL_MARK: Final = '(horsies log channel full or closed;'
"""The text that each record written to stderr instead of Python carries."""

SPILLED_PANIC: Final = (
    'horsies.bridge Error panic in core operation CoreHandle.start_workflow_async'
)


def test_a_core_record_reaches_python_logging_with_the_core_time_and_thread(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'core_records', tmp_path, extension_database).report
    records = reports_of(report, 'records')

    assert text_of(report, 'first') == 'DatabasePingRecord'
    assert text_of(report, 'second') == 'DatabasePingRecord'
    assert flag_of(report, 'held') and flag_of(report, 'seen'), report
    assert len(records) == 1, records
    (record,) = records
    assert text_of(record, 'logger') == 'horsies.sql'
    assert text_of(record, 'statement') == 'health_check'
    assert text_of(record, 'thread_name') == RUNTIME_THREAD_NAME
    created = seconds_of(record, 'created')
    assert seconds_of(report, 'sent_before') <= created <= seconds_of(report, 'sent_after')
    # The record waited in the channel while the forwarder was held, and kept
    # the time of the core event.
    assert seconds_of(record, 'handled_at') - created >= HOLD_S / 2


def test_a_level_change_takes_effect_after_refresh_log_levels(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'level_refresh', tmp_path, extension_database).report

    assert texts_of(report, 'pings') == ['DatabasePingRecord'] * 3
    # Within the interval, only refresh_log_levels() reads the new level.
    assert seconds_of(report, 'seconds_before_refresh') < LEVEL_REFRESH_INTERVAL_S
    assert flag_of(report, 'seen'), report
    created = numbers_of(report, 'created')
    # One record: the ping after the refresh. The ping before it logged nothing.
    assert len(created) == 1, created
    assert created[0] >= seconds_of(report, 'refreshed_at')


def test_a_full_channel_drops_low_records_spills_high_ones_and_reports_both(
    tmp_path: Path, extension_database: Database,
) -> None:
    run = run_case(PROGRAM, 'full_channel', tmp_path, extension_database)
    report = run.report

    assert text_of(report, 'warm_up') == 'DatabasePingRecord'
    assert flag_of(report, 'warm_up_seen') and flag_of(report, 'blocked'), report
    assert not flag_of(report, 'resume_timed_out')
    assert integer_of(report_at(report, 'flood'), 'DatabasePingRecord') == FLOOD_PINGS
    panic_texts = texts_of(report, 'panic_texts')
    assert len(panic_texts) == PANICKING_STARTS
    for text in panic_texts:
        assert text.startswith('panic in core operation CoreHandle.start_workflow_async: '), text

    # One WARNING with both counts.
    assert flag_of(report, 'reported'), report
    assert texts_of(report, 'drop_report_levels') == ['WARNING']
    drop_reports = reports_of(report, 'drop_reports')
    assert len(drop_reports) == 1, drop_reports
    (drop_report,) = drop_reports
    dropped = integer_of(drop_report, 'dropped')
    spilled = integer_of(drop_report, 'spilled')
    # DEBUG records past the channel and the forwarder's batch were dropped.
    assert dropped >= FLOOD_PINGS - (LOG_CHANNEL_CAPACITY + FORWARD_BATCH)
    # Each record either reached Python or was dropped.
    assert flag_of(report, 'all_handled'), report
    assert integer_of(report, 'handled_statement_records') + dropped >= FLOOD_PINGS + 2
    # The ERROR records waited, then went to stderr and not to Python.
    assert run.stderr.count(SPILLED_PANIC) == PANICKING_STARTS, run.stderr
    assert run.stderr.count(SPILL_MARK) == spilled, run.stderr
    assert texts_of(report, 'bridge_errors') == []
