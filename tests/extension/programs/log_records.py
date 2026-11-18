"""Cases of the log bridge: core records reach Python ``logging`` with the
core's time and thread name; a level change takes effect at
``refresh_log_levels()``; a full channel drops DEBUG and INFO records,
writes WARNING and ERROR records to stderr after a wait, and reports both
counts in one WARNING.

The records come from the statement log of a handle built with ``echo``:
one DEBUG record on ``horsies.sql`` per statement, with the statement's name
in its ``statement`` field. ``ping_database`` runs ``health_check``. The
ERROR records of the full channel case come from operations that panic
(``workflow_spec.PanickingChildSpecSource``).

The handles use the ``ASSUME_INITIALIZED`` schema mode: the database has the
schema (``conftest.extension_database``), and no call installs it.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
import uuid
from collections import Counter
from typing import Final

from horsies._core import CoreHandle, PanicException, refresh_log_levels
from tests.extension.programs.report import JsonValue, Report, run_case
from tests.extension.programs.support import (
    RECORD_WAIT_BOUND_S,
    RecordCollector,
    SeenRecord,
    collect_records,
    database_url,
    manifest_json,
    wait_until_async,
)
from tests.extension.programs.workflow_spec import (
    PanickingChildSpecSource,
    subworkflow_start_request,
)

PING_STATEMENT: Final = 'health_check'
"""The statement that ``ping_database`` runs."""

HOLD_S: Final = 1.0
"""How long the collector of the time case holds the forwarder on its first record."""

FORWARDER_FIRST_READ_WAIT_S: Final = 0.2
"""The time the level case leaves the GIL free after the start, for the
forwarder's first read of the levels."""

FLOOD_PINGS: Final = 10_000
"""The pings of the full channel case while the forwarder is blocked."""

PANICKING_STARTS: Final = 2
"""The operations of the full channel case that panic, each with one ERROR record."""

RESUME_BOUND_S: Final = 60.0
"""The longest time the blocking collector holds the forwarder."""

DRAIN_BOUND_S: Final = 30.0
"""The longest wait for the forwarder to hand a full channel to Python."""

DROP_REPORT = re.compile(
    r'^the core log channel was full: (\d+) DEBUG/INFO records were dropped and '
    r'(\d+) WARNING/ERROR records were written to stderr$',
)
"""The message of the forwarder's WARNING after a full channel."""


def _handle() -> CoreHandle:
    return CoreHandle(
        manifest_json(database_url(), schema='ASSUME_INITIALIZED', echo=True),
        pool_profile='PRODUCER',
    )


def _ping_records(collector: RecordCollector) -> list[SeenRecord]:
    return [record for record in collector.records() if record.statement == PING_STATEMENT]


class HoldingCollector(RecordCollector):
    """Holds the forwarder for ``HOLD_S`` on the first record it handles.
    Only the forwarder thread calls ``emit``."""

    def __init__(self) -> None:
        super().__init__()
        self.holding = threading.Event()
        self._held_once = False

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        if not self._held_once:
            self._held_once = True
            self.holding.set()
            time.sleep(HOLD_S)


async def _core_records() -> Report:
    collector = HoldingCollector()
    sql_logger = logging.getLogger('horsies.sql')
    sql_logger.setLevel(logging.DEBUG)
    sql_logger.addHandler(collector)
    handle = _handle()
    first = await handle.ping_database_async()
    held = await wait_until_async(collector.holding.is_set, RECORD_WAIT_BOUND_S)
    sent_before = time.time()
    second = await handle.ping_database_async()
    sent_after = time.time()

    def second_seen() -> bool:
        return any(record.created >= sent_before for record in _ping_records(collector))

    seen = await wait_until_async(second_seen, RECORD_WAIT_BOUND_S + HOLD_S)
    records: list[JsonValue] = [
        record.to_report()
        for record in _ping_records(collector)
        if record.created >= sent_before
    ]
    return {
        'first': type(first).__name__,
        'second': type(second).__name__,
        'held': held,
        'seen': seen,
        'sent_before': sent_before,
        'sent_after': sent_after,
        'records': records,
    }


def core_records() -> Report:
    """Two pings: the collector holds the forwarder on the first record, so
    the second ping's record waits in the channel before Python gets it."""
    return asyncio.run(_core_records())


async def _level_refresh() -> Report:
    collector = collect_records('horsies.sql', logging.WARNING)
    handle = _handle()
    started = time.monotonic()
    first = await handle.ping_database_async()
    # The forwarder reads the levels once when it first takes the GIL after
    # the start. The loop releases the GIL while it waits here, so that read
    # sees WARNING; the next periodic read is LEVEL_REFRESH_INTERVAL later.
    await asyncio.sleep(FORWARDER_FIRST_READ_WAIT_S)
    logging.getLogger('horsies.sql').setLevel(logging.DEBUG)
    second = await handle.ping_database_async()
    seconds_before_refresh = time.monotonic() - started
    refreshed_at = time.time()
    refresh_log_levels()
    third = await handle.ping_database_async()
    seen = await wait_until_async(lambda: bool(_ping_records(collector)), RECORD_WAIT_BOUND_S)
    created: list[JsonValue] = [record.created for record in _ping_records(collector)]
    return {
        'pings': [type(ping).__name__ for ping in (first, second, third)],
        'seconds_before_refresh': seconds_before_refresh,
        'refreshed_at': refreshed_at,
        'seen': seen,
        'created': created,
    }


def level_refresh() -> Report:
    """The core starts with ``horsies.sql`` at WARNING. A ping after the
    level changed to DEBUG, then ``refresh_log_levels()``, then a ping."""
    return asyncio.run(_level_refresh())


class BlockingCollector(RecordCollector):
    """Once armed, blocks the forwarder on the first record it handles until
    ``resume`` is set, at most ``RESUME_BOUND_S``. Only the forwarder
    thread calls ``emit``."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = threading.Event()
        self.blocked = threading.Event()
        self.resume = threading.Event()
        self.resume_timed_out = threading.Event()
        self._blocked_once = False

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        if self.armed.is_set() and not self._blocked_once:
            self._blocked_once = True
            self.blocked.set()
            if not self.resume.wait(RESUME_BOUND_S):
                self.resume_timed_out.set()


def _drop_counts(messages: list[str]) -> list[tuple[int, int]]:
    """The (dropped, written to stderr) counts of each drop report in ``messages``."""
    counts: list[tuple[int, int]] = []
    for message in messages:
        match DROP_REPORT.match(message):
            case None:
                pass
            case found:
                counts.append((int(found.group(1)), int(found.group(2))))
    return counts


async def _full_channel() -> Report:
    sql = BlockingCollector()
    sql_logger = logging.getLogger('horsies.sql')
    sql_logger.setLevel(logging.DEBUG)
    sql_logger.addHandler(sql)
    bridge = collect_records('horsies.bridge', logging.WARNING)
    handle = _handle()
    handle.register_child_spec_source(PanickingChildSpecSource('planned panic'))
    warm_up = await handle.ping_database_async()
    warm_up_seen = await wait_until_async(lambda: bool(_ping_records(sql)), RECORD_WAIT_BOUND_S)
    sql.armed.set()
    trigger = await handle.ping_database_async()
    blocked = await wait_until_async(sql.blocked.is_set, RECORD_WAIT_BOUND_S)
    flood = await asyncio.gather(*(handle.ping_database_async() for _ in range(FLOOD_PINGS)))
    panic_texts: list[JsonValue] = []
    for _ in range(PANICKING_STARTS):
        try:
            outcome = await handle.start_workflow_async(subworkflow_start_request(uuid.uuid4()))
            panic_texts.append(f'returned {type(outcome).__name__}')
        except PanicException as panic:
            panic_texts.append(str(panic))
    sql.resume.set()
    reported = await wait_until_async(
        lambda: bool(_drop_counts(bridge.messages())), RECORD_WAIT_BOUND_S,
    )
    counts = _drop_counts(bridge.messages())
    dropped = counts[0][0] if counts else 0

    def drained() -> bool:
        return len(sql.records()) + dropped >= FLOOD_PINGS + 2

    all_handled = await wait_until_async(drained, DRAIN_BOUND_S)
    flood_counts: dict[str, JsonValue] = {
        name: count for name, count in Counter(type(ping).__name__ for ping in flood).items()
    }
    return {
        'warm_up': type(warm_up).__name__,
        'warm_up_seen': warm_up_seen,
        'trigger': type(trigger).__name__,
        'blocked': blocked,
        'resume_timed_out': sql.resume_timed_out.is_set(),
        'flood': flood_counts,
        'panic_texts': panic_texts,
        'reported': reported,
        'drop_reports': [
            {'dropped': dropped_low, 'spilled': spilled_high}
            for dropped_low, spilled_high in counts
        ],
        'drop_report_levels': [
            record.level for record in bridge.records() if DROP_REPORT.match(record.message)
        ],
        'bridge_errors': [record.message for record in bridge.records() if record.level == 'ERROR'],
        'all_handled': all_handled,
        'handled_statement_records': len(sql.records()),
    }


def full_channel() -> Report:
    """The collector blocks the forwarder; ``FLOOD_PINGS`` pings and
    ``PANICKING_STARTS`` panicking starts run; then the forwarder goes on."""
    return asyncio.run(_full_channel())


if __name__ == '__main__':
    run_case(
        {
            'core_records': core_records,
            'level_refresh': level_refresh,
            'full_channel': full_channel,
        },
    )
