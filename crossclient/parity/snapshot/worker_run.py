"""The whole-run compare rules that parity uses (``crossclient/parity``).

The snapshot of a whole run compares by ``WHOLE_RUN_CATALOG``: the baseline
rules, with the process ids of a run ignored (the pid columns, the process
names and the pid fields of the attempt archive), the run measures of a kill
report as tokens (``KILL_REPORT_MEASURES``), and ``enqueued_at`` as
``TimeOrderBeforeRun`` (``RUN_WRITTEN_ORDER_COLUMNS``). The heartbeat,
worker-state and reaper scan cursor tables are left out of the compare
(``compared_part``): their rows follow the run time, not a decision of the
code. The liveness generation table is left out too.

Two snapshots that differ are a ``DualRunMismatch`` when the run spans of
both sides lie in one UTC hour, and a ``ClockBoundaryStraddle`` when they do
not (``judge_snapshots``). The install and the worker of a side each create
the hourly heartbeat leaves from their own hour on, and leaf names are read
relative to the hour of the run window, so a pair across an hour can differ
by a leaf for the clock alone. The judgement leaves no entry out of the diff.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final


from crossclient.parity.snapshot.catalog import (
    ATTEMPT_ARCHIVE,
    BASELINE_CATALOG,
    KILL_REPORT_MEASURES,
    RESULT_ENVELOPE,
)
from crossclient.parity.snapshot.compare import SnapshotDiff
from crossclient.parity.snapshot.normalize import NormalizedSnapshot
from crossclient.parity.snapshot.rules import (
    ColumnRule,
    ColumnSpec,
    EachItem,
    Ignored,
    Index,
    JsonPath,
    Key,
    SnapshotCatalog,
    StoredJson,
    TableRules,
    TimeOrderBeforeRun,
    TimeOrderOnly,
)
from crossclient.parity.snapshot.values import RunWindow


UNCOMPARED_TABLES: Final = frozenset({
    'horsies_heartbeats',
    'horsies_worker_states',
    'horsies_recovery_scan_cursors',
    'horsies_worker_liveness',
})
"""Tables whose rows follow the run time: one heartbeat per interval, one
state sample per interval, and the scan cursors of the reaper, which move
once per reaper pass. Also the liveness generations: only the core worker
holds a liveness lock, so only its side writes a row."""

PROCESS_ID_COLUMNS: Final = frozenset({
    'worker_pid',
    'last_worker_pid',
    'worker_process_name',
    'last_worker_process_name',
})
"""Columns that hold the process id of a worker child of the run, or the
process name that holds it (``worker-<pid>``)."""

_RUN_PROCESS_ID: Final = Ignored(
    reason='the process id of a worker child; each run starts its own processes',
)

ARCHIVE_PROCESS_ID_POSITIONS: Final = (10, 11)
"""The fields of an attempt of the attempt archive (version 1) that hold the
process id of its worker child: ``worker_pid`` and ``worker_process_name``."""

ARCHIVE_REPORT_POSITIONS: Final = (6, 7)
"""The fields of an attempt of the attempt archive (version 1) that can hold a
kill report: ``error_message`` and ``failed_reason``."""

WHOLE_RUN_ATTEMPT_ARCHIVE: Final = StoredJson(
    storage=ATTEMPT_ARCHIVE.storage,
    format=ATTEMPT_ARCHIVE.format,
    paths=(
        *ATTEMPT_ARCHIVE.paths,
        *(
            JsonPath(steps=(EachItem(), Index(position=position)), rule=_RUN_PROCESS_ID)
            for position in ARCHIVE_PROCESS_ID_POSITIONS
        ),
        *(
            JsonPath(
                steps=(EachItem(), Index(position=position)), rule=KILL_REPORT_MEASURES
            )
            for position in ARCHIVE_REPORT_POSITIONS
        ),
    ),
)
"""The attempt archive with the process id fields of each attempt ignored, and
the run measures of its kill reports as tokens."""

WHOLE_RUN_RESULT_ENVELOPE: Final = StoredJson(
    storage=RESULT_ENVELOPE.storage,
    format=RESULT_ENVELOPE.format,
    paths=(
        *RESULT_ENVELOPE.paths,
        JsonPath(steps=(Key(name='err'), Key(name='message')), rule=KILL_REPORT_MEASURES),
    ),
)
"""The result envelope with the run measures of the kill report in
``err.message`` as tokens."""

RUN_WRITTEN_ORDER_COLUMNS: Final = frozenset({'enqueued_at'})
"""Ranked clock columns (``TimeOrderOnly`` in the baseline) that the run
writes: a send writes ``enqueued_at`` as its insert time before the run
start, in the scenario's order; a retry during the run writes it as the retry
time (``enqueued_at = next_retry_at``), whose order between two tasks follows
the order in which their children ended. They compare as
``TimeOrderBeforeRun``."""


# ---- The judgement of the two snapshots ----


@dataclass(frozen=True, slots=True)
class PairEqual:
    """The two snapshots compare equal."""


@dataclass(frozen=True, slots=True)
class DualRunMismatch:
    """The two snapshots differ, and both run spans lie in one UTC hour."""

    diff: SnapshotDiff


@dataclass(frozen=True, slots=True)
class ClockBoundaryStraddle:
    """The two snapshots differ, and the run spans do not lie in one UTC
    hour: a span crossed an hour, or the two sides ran in two hours. A leaf
    name or a time rank can differ for that reason alone: the install and
    the worker of a side each create the hourly heartbeat leaves from their
    own hour on, and a leaf name is read relative to the hour of the run
    window (``generated_name``). ``diff`` holds every entry."""

    diff: SnapshotDiff
    reference_span: RunWindow
    candidate_span: RunWindow


type PairJudgement = PairEqual | DualRunMismatch | ClockBoundaryStraddle


def _hour(at: datetime) -> datetime:
    return at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def crosses_hour(span: RunWindow) -> bool:
    """Whether the span holds the start of a UTC hour after its own start."""
    return _hour(span.start) != _hour(span.end)


def in_one_hour(reference: RunWindow, candidate: RunWindow) -> bool:
    """Whether both spans lie in one UTC hour: neither crosses an hour, and
    both start in the same hour."""
    return (
        not crosses_hour(reference)
        and not crosses_hour(candidate)
        and _hour(reference.start) == _hour(candidate.start)
    )


def judge_snapshots(
    diff: SnapshotDiff, reference_span: RunWindow, candidate_span: RunWindow
) -> PairJudgement:
    """``PairEqual`` for a diff with no entry; else ``DualRunMismatch`` when
    both run spans lie in one UTC hour, and ``ClockBoundaryStraddle`` when
    they do not. No entry is left out. Pure."""
    if not diff.entries:
        return PairEqual()
    match in_one_hour(reference_span, candidate_span):
        case True:
            return DualRunMismatch(diff=diff)
        case False:
            return ClockBoundaryStraddle(
                diff=diff, reference_span=reference_span, candidate_span=candidate_span
            )


# ---- The whole-run catalog ----


def _whole_run_table(table: TableRules) -> TableRules:
    match table.name in UNCOMPARED_TABLES:
        case True:
            return table
        case False:
            columns = tuple(_whole_run_column(column) for column in table.columns)
            return dataclasses.replace(table, columns=columns)


def _whole_run_rule(column: ColumnSpec) -> ColumnRule:
    """The whole-run rule of ``column``: a process id column ignored, a run
    written ranked column as ``TimeOrderBeforeRun``, the attempt archive and
    the result envelope with their whole-run paths, any other column as in
    the baseline. Pure."""
    match column.name, column.rule:
        case name, _ if name in PROCESS_ID_COLUMNS:
            return _RUN_PROCESS_ID
        case name, TimeOrderOnly() if name in RUN_WRITTEN_ORDER_COLUMNS:
            return TimeOrderBeforeRun()
        case _, rule if rule == ATTEMPT_ARCHIVE:
            return WHOLE_RUN_ATTEMPT_ARCHIVE
        case _, rule if rule == RESULT_ENVELOPE:
            return WHOLE_RUN_RESULT_ENVELOPE
        case _, rule:
            return rule


def _whole_run_column(column: ColumnSpec) -> ColumnSpec:
    return ColumnSpec(name=column.name, rule=_whole_run_rule(column))


def whole_run_catalog(base: SnapshotCatalog) -> SnapshotCatalog:
    """``base`` with the whole-run rules of ``_whole_run_rule``. The tables of
    ``UNCOMPARED_TABLES`` keep their rules: they are captured, then left out
    by ``compared_part``. Pure."""
    return SnapshotCatalog(
        tables=tuple(_whole_run_table(table) for table in base.tables)
    )


WHOLE_RUN_CATALOG: Final = whole_run_catalog(BASELINE_CATALOG)


def compared_part(snapshot: NormalizedSnapshot) -> NormalizedSnapshot:
    """``snapshot`` without the tables of ``UNCOMPARED_TABLES``. Pure."""
    return NormalizedSnapshot(
        tables=tuple(
            table
            for table in snapshot.tables
            if table.rules.name not in UNCOMPARED_TABLES
        ),
        ambiguities=tuple(
            group
            for group in snapshot.ambiguities
            if group.table not in UNCOMPARED_TABLES
        ),
        partition_tree=snapshot.partition_tree,
    )
