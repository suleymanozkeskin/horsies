"""A snapshot reduced to the fact of a pair rule. Pure: no I/O.

A pair rule (``tests.dualrun.operations.table.PairRule``) names one
integration test and one fact (``PairFact``) that both runs of the test fix,
while the stored values that hold the fact follow the run timing. Before the
compare, each snapshot of the pair is reduced to that fact. Every other table
and column stays as captured and compares by its column rule.

- ``ONE_RACE_WINNER``: in ``horsies_task_history``, the columns
  ``claimed_at`` and ``last_claimed_worker_id`` become one column,
  ``claim facts`` (``race_winner``). It holds ``CLAIM_FACTS_AGREE`` when
  both are NULL (the cancel took the PENDING row), or when ``claimed_at`` is
  in the run and ``last_claimed_worker_id`` holds a worker (the claim took
  the row first, then the cancel took the CLAIMED row). Any other pair of
  values becomes a text that names both. For each row that the claim took
  first, one ``horsies_task_status`` notification of its task leaves the
  ``notifications`` table: the status change of the claim.
- ``HEARTBEAT_SENDERS``: ``horsies_heartbeats`` becomes one row per distinct
  (``task_id``, ``role``, ``sender_id``, ``hostname``, ``pid``), paired by
  (``task_id``, ``role``). The send time, its leaf (``tableoid``) and the
  number of rows of one sender are not compared.
- ``ONE_KEYED_SEND_WINNER``: in ``horsies_tasks``, the ``input`` of a row
  with an ``idempotency_key_digest`` becomes ``WINNING_COMMAND`` when it is a
  JSON document. A NULL input and a text that is not JSON stay as captured.
- ``ONE_MOVER_PER_ROUND``: in ``horsies_task_history``, the columns of a
  row that the move writes and that differ by the mover
  (``ROUND_MOVE_COLUMNS``) become ``MOVED_IN_THE_ROUND`` when its
  ``terminalization_kind`` is one of the finalize (``FAIL_RUNNING``,
  ``COMPLETE_LOCKED``) or of the stale recovery (``FAIL_STALE``); so does
  the ``terminalization_kind`` of its ``horsies_workflow_phase2_pending``
  row. Another kind stays as captured. For each ``FAIL_RUNNING`` row of a
  plain task, one capacity wake of its queue leaves the ``notifications``
  table; a wake that is owed and not there becomes a row
  ``CAPACITY_WAKE_MISSING`` on that channel.

- ``CONCURRENT_STARTS_IN_ANY_ORDER``: in ``horsies_tasks``, the
  ``enqueued_at`` of a row becomes ``ENQUEUED_IN_THE_RACE`` when it is a
  time rank (``ORDER``). A NULL stays as captured.

A decided difference (``tests.dualrun.operations.table.DecidedDifference``)
reduces nothing: its pair compares the whole snapshot.
``decided_difference_explains`` tells whether every entry of the
difference is one that its fact explains.

- ``RECEIVED_WRITE_RUNS_TO_ITS_END``: each entry is part of the commit of
  a received write: a ``horsies_tasks`` row only in the reference with the
  status RUNNING; a ``horsies_task_history`` row only in the candidate with
  the status COMPLETED; a ``task_done`` or ``horsies_task_status``
  notification only in the candidate, for the task of such a history row.
  There is one RUNNING row of the reference per committed row, and one
  event of each channel per committed row.

No reduction hides which rows exist or who owns them. A task that one of the
concurrent starts did not commit is still a row on one side only. A history row, or a
task and role of a heartbeat, on one side only is still a row on one side
only. A sender that differs, claim facts that agree with neither winner
against facts that agree, a move that neither racer writes, and an input
that is not a document against one that is, are still column differences.
A claim first without its status notification still leaves one
notification less, and a finalize of a failure without its capacity wake
still leaves a missing wake.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final

from tests.dualrun.catalog import NOTIFICATIONS, QUEUE_CHANNEL_PREFIX
from tests.dualrun.compare import DiffEntry, ExtraRow, MissingRow, SnapshotDiff
from tests.dualrun.normalize import NormalizedRow, NormalizedSnapshot, NormalizedTable
from tests.dualrun.operations.table import (
    DecidedDifference,
    DecidedFact,
    PairCompare,
    PairFact,
    PairRule,
    WholeSnapshot,
)
from tests.dualrun.rules import ColumnSpec, Exact, TableRules
from tests.dualrun.values import (
    Cell,
    DuringRun,
    ExactValue,
    IdLabel,
    JsonDocument,
    NormalizedValue,
    NullValue,
    TimeRank,
    render_value,
    sort_token,
)

HISTORY_TABLE: Final = 'horsies_task_history'
HEARTBEATS_TABLE: Final = 'horsies_heartbeats'

CLAIM_TIME: Final = 'claimed_at'
CLAIM_WORKER: Final = 'last_claimed_worker_id'
CLAIM_FACTS: Final = 'claim facts'
"""The column of a reduced history row that takes the place of ``CLAIM_TIME`` and ``CLAIM_WORKER``."""

CLAIM_FACTS_AGREE: Final = ExactValue(text='agree with one winner')
"""The claim facts of a history row that agree with the cancel first or with the claim first."""

STATUS_EVENT: Final = ExactValue(text='horsies_task_status')
"""The channel of the status event, normalized (``Exact``)."""

SENDER_COLUMNS: Final = ('task_id', 'role', 'sender_id', 'hostname', 'pid')
"""The columns of a reduced heartbeat row."""

SENDER_IDENTITY: Final = ('task_id', 'role')
"""The columns that pair a reduced heartbeat row of the reference with one of the candidate."""


TASKS_TABLE: Final = 'horsies_tasks'
PHASE2_PENDING_TABLE: Final = 'horsies_workflow_phase2_pending'

WINNING_COMMAND: Final = ExactValue(text='the command of the send that won')
"""The input of a task that a keyed send of the two-command race committed."""

ROUND_MOVE_COLUMNS: Final = (
    'status',
    'terminalization_kind',
    'error_code',
    'final_failed_reason',
    'result_payload',
    'attempt_snapshot',
)
"""The columns of a history row that the move of a round writes and that
differ between the finalize and the stale recovery. The digest columns stay:
each one still checks its own payload."""

ENQUEUED_IN_THE_RACE: Final = ExactValue(text='enqueued by one of the concurrent starts')
"""The enqueue time of a task of the concurrent starts, in place of its rank."""

ENQUEUE_TIME: Final = 'enqueued_at'

MOVED_IN_THE_ROUND: Final = ExactValue(text='written by the mover of the round')
"""A value that the finalize or the stale recovery wrote, whichever moved the row."""

FINALIZE_KINDS: Final = ('FAIL_RUNNING', 'COMPLETE_LOCKED')
"""The terminalization kinds of the finalize in the round race: the failure
of a plain task and the success of a workflow task."""

STALE_RECOVERY_KIND: Final = 'FAIL_STALE'
CAPACITY_PAYLOAD: Final = ExactValue(text='capacity')
CAPACITY_WAKE_MISSING: Final = ExactValue(
    text='the capacity wake of a finalize of a failure, missing'
)
"""The payload of a row that takes the place of a capacity wake that a
``FAIL_RUNNING`` row of a plain task owes and the run did not send."""


class RaceWinner(Enum):
    """The side of a claim and cancel race that took the PENDING row."""

    CANCEL = 'the cancel took the PENDING row'
    CLAIM = 'the claim took the PENDING row; the cancel then took the CLAIMED row'


@dataclass(frozen=True, slots=True)
class NeitherWinner:
    """Claim facts of a history row that agree with neither side: one of
    them is NULL and the other is not, or the claim time is not a time of
    the run."""

    claim_time: NormalizedValue
    claim_worker: NormalizedValue


def race_winner(
    claim_time: NormalizedValue, claim_worker: NormalizedValue
) -> RaceWinner | NeitherWinner:
    """The side that the claim facts of a history row agree with."""
    match claim_time, claim_worker:
        case NullValue(), NullValue():
            return RaceWinner.CANCEL
        case DuringRun(), IdLabel():
            return RaceWinner.CLAIM
        case _:
            return NeitherWinner(claim_time=claim_time, claim_worker=claim_worker)


def _claim_facts(winner: RaceWinner | NeitherWinner) -> ExactValue:
    match winner:
        case RaceWinner.CANCEL | RaceWinner.CLAIM:
            return CLAIM_FACTS_AGREE
        case NeitherWinner(claim_time=claim_time, claim_worker=claim_worker):
            return ExactValue(
                text=(
                    f'agree with neither winner: {CLAIM_TIME} {render_value(claim_time)}, '
                    f'{CLAIM_WORKER} {render_value(claim_worker)}'
                ),
            )


def _history_by_race(
    history: NormalizedTable,
) -> tuple[NormalizedTable, tuple[NormalizedValue, ...]]:
    """``history`` with ``CLAIM_TIME`` and ``CLAIM_WORKER`` reduced to
    ``CLAIM_FACTS``, and the task of each row that the claim took first."""
    rules = history.rules
    dropped = (rules.position(CLAIM_TIME), rules.position(CLAIM_WORKER))
    keys = {*rules.order_by, *rules.identity}
    message = f'{rules.name}: a claim fact orders or pairs the rows'
    assert keys.isdisjoint((CLAIM_TIME, CLAIM_WORKER)), message
    task_at = rules.position('task_id')
    kept = tuple(index for index in range(len(rules.columns)) if index not in dropped)
    reduced = TableRules(
        name=rules.name,
        columns=(
            *(rules.columns[index] for index in kept),
            ColumnSpec(name=CLAIM_FACTS, rule=Exact()),
        ),
        order_by=rules.order_by,
        identity=rules.identity,
    )
    rows: list[NormalizedRow] = []
    claimed_first: list[NormalizedValue] = []
    for row in history.rows:
        winner = race_winner(row[dropped[0]], row[dropped[1]])
        rows.append((*(row[index] for index in kept), _claim_facts(winner)))
        match winner:
            case RaceWinner.CLAIM:
                claimed_first.append(row[task_at])
            case RaceWinner.CANCEL | NeitherWinner():
                pass
    return NormalizedTable(rules=reduced, rows=tuple(rows)), tuple(claimed_first)


def _without_claim_events(
    notifications: NormalizedTable, claimed_first: tuple[NormalizedValue, ...]
) -> NormalizedTable:
    """``notifications`` less one ``STATUS_EVENT`` row for each task in
    ``claimed_first``. A task without such a row loses nothing."""
    channel_at = notifications.rules.position('channel')
    task_at = notifications.rules.position('task_id')
    owed = Counter(claimed_first)
    kept: list[NormalizedRow] = []
    for row in notifications.rows:
        task = row[task_at]
        match row[channel_at] == STATUS_EVENT and owed[task] > 0:
            case True:
                owed[task] -= 1
            case False:
                kept.append(row)
    return NormalizedTable(rules=notifications.rules, rows=tuple(kept))


def _heartbeat_senders(heartbeats: NormalizedTable) -> NormalizedTable:
    """One row per distinct ``SENDER_COLUMNS`` of ``heartbeats``, paired by ``SENDER_IDENTITY``."""
    rules = heartbeats.rules
    positions = tuple(rules.position(column) for column in SENDER_COLUMNS)
    senders = {
        tuple(row[position] for position in positions) for row in heartbeats.rows
    }
    return NormalizedTable(
        rules=TableRules(
            name=f'{rules.name}, senders per task and role',
            columns=tuple(rules.columns[position] for position in positions),
            order_by=SENDER_COLUMNS,
            identity=SENDER_IDENTITY,
        ),
        rows=tuple(
            sorted(senders, key=lambda row: tuple(sort_token(value) for value in row))
        ),
    )


def _keyed_inputs_as_winning_command(tasks: NormalizedTable) -> NormalizedTable:
    """``tasks`` with the input of each row that holds an idempotency key
    digest as ``WINNING_COMMAND``, when it is a JSON document."""
    rules = tasks.rules
    input_at = rules.position('input')
    digest_at = rules.position('idempotency_key_digest')
    message = f'{rules.name}: the input orders or pairs the rows'
    assert 'input' not in rules.identity, message
    rows: list[NormalizedRow] = []
    for row in tasks.rows:
        match row[digest_at], row[input_at]:
            case NullValue(), _:
                rows.append(row)
            case _, JsonDocument():
                rows.append((*row[:input_at], WINNING_COMMAND, *row[input_at + 1 :]))
            case _:
                rows.append(row)
    return NormalizedTable(rules=rules, rows=tuple(rows))


def _enqueue_times_of_the_race(tasks: NormalizedTable) -> NormalizedTable:
    """``tasks`` with each ``enqueued_at`` that is a time rank as
    ``ENQUEUED_IN_THE_RACE``. A NULL stays as captured."""
    rules = tasks.rules
    enqueued_at = rules.position(ENQUEUE_TIME)
    message = f'{rules.name}: {ENQUEUE_TIME} orders or pairs the rows'
    assert ENQUEUE_TIME not in rules.identity, message
    rows: list[NormalizedRow] = []
    for row in tasks.rows:
        match row[enqueued_at]:
            case TimeRank():
                rows.append(
                    (*row[:enqueued_at], ENQUEUED_IN_THE_RACE, *row[enqueued_at + 1 :])
                )
            case _:
                rows.append(row)
    return NormalizedTable(rules=rules, rows=tuple(rows))


class RoundMover(Enum):
    """The side of the round race that moved the task to history."""

    FINALIZE = 'the finalize moved the row'
    STALE_RECOVERY = 'the stale recovery moved the row'


@dataclass(frozen=True, slots=True)
class NeitherMover:
    """A terminalization kind that neither side of the round race writes."""

    kind: NormalizedValue


def round_mover(kind: NormalizedValue) -> RoundMover | NeitherMover:
    """The side of the round race that wrote the terminalization kind ``kind``."""
    match kind:
        case ExactValue(text=text) if text in FINALIZE_KINDS:
            return RoundMover.FINALIZE
        case ExactValue(text=text) if text == STALE_RECOVERY_KIND:
            return RoundMover.STALE_RECOVERY
        case _:
            return NeitherMover(kind=kind)


def _replaced(
    row: NormalizedRow, positions: tuple[int, ...], value: NormalizedValue
) -> NormalizedRow:
    """``row`` with the value at each of ``positions`` replaced by ``value``."""
    return tuple(value if index in positions else cell for index, cell in enumerate(row))


def _moves_of_the_round(table: NormalizedTable, columns: tuple[str, ...]) -> NormalizedTable:
    """``table`` with ``columns`` as ``MOVED_IN_THE_ROUND`` in each row whose
    ``terminalization_kind`` one side of the round race writes."""
    rules = table.rules
    kind_at = rules.position('terminalization_kind')
    positions = tuple(rules.position(column) for column in columns)
    keys = {*rules.order_by, *rules.identity}
    message = f'{rules.name}: a column of the move orders or pairs the rows'
    assert keys.isdisjoint(columns), message
    rows: list[NormalizedRow] = []
    for row in table.rows:
        match round_mover(row[kind_at]):
            case RoundMover.FINALIZE | RoundMover.STALE_RECOVERY:
                rows.append(_replaced(row, positions, MOVED_IN_THE_ROUND))
            case NeitherMover():
                rows.append(row)
    return NormalizedTable(rules=rules, rows=tuple(rows))


def _owed_capacity_wakes(history: NormalizedTable) -> Counter[NormalizedValue]:
    """Per wake channel, the capacity wakes that the ``FAIL_RUNNING`` rows of
    plain tasks in ``history`` owe: one per row, on ``task_queue_<queue>``."""
    rules = history.rules
    kind_at = rules.position('terminalization_kind')
    queue_at = rules.position('queue_name')
    workflow_task_at = rules.position('is_workflow_task')
    owed: Counter[NormalizedValue] = Counter()
    for row in history.rows:
        match row[kind_at], row[workflow_task_at], row[queue_at]:
            case ExactValue(text='FAIL_RUNNING'), ExactValue(text='false'), ExactValue(
                text=queue
            ):
                owed[ExactValue(text=f'{QUEUE_CHANNEL_PREFIX}{queue}')] += 1
            case _:
                pass
    return owed


def _without_owed_wakes(
    notifications: NormalizedTable, owed: Counter[NormalizedValue]
) -> NormalizedTable:
    """``notifications`` less one capacity wake for each wake in ``owed``, and
    one ``CAPACITY_WAKE_MISSING`` row for each owed wake that is not there."""
    rules = notifications.rules
    channel_at = rules.position('channel')
    payload_at = rules.position('payload')
    left = Counter(owed)
    kept: list[NormalizedRow] = []
    for row in notifications.rows:
        channel = row[channel_at]
        match row[payload_at] == CAPACITY_PAYLOAD and left[channel] > 0:
            case True:
                left[channel] -= 1
            case False:
                kept.append(row)
    missing = [
        _missing_wake(rules, channel) for channel in sorted(left.elements(), key=sort_token)
    ]
    return NormalizedTable(rules=rules, rows=(*kept, *missing))


def _missing_wake(rules: TableRules, channel: NormalizedValue) -> NormalizedRow:
    """A notification row on ``channel`` with the payload ``CAPACITY_WAKE_MISSING``."""
    values: dict[str, NormalizedValue] = {
        'channel': channel,
        'payload': CAPACITY_WAKE_MISSING,
    }
    return tuple(values.get(spec.name, NullValue()) for spec in rules.columns)


def _with_tables(
    snapshot: NormalizedSnapshot, replaced: Mapping[str, NormalizedTable]
) -> NormalizedSnapshot:
    """``snapshot`` with each table that ``replaced`` names in the place of that table."""
    return dataclasses.replace(
        snapshot,
        tables=tuple(
            replaced.get(table.rules.name, table) for table in snapshot.tables
        ),
    )


def _reduced(snapshot: NormalizedSnapshot, fact: PairFact) -> NormalizedSnapshot:
    match fact:
        case PairFact.ONE_RACE_WINNER:
            history, claimed_first = _history_by_race(snapshot.table(HISTORY_TABLE))
            notifications = _without_claim_events(
                snapshot.table(NOTIFICATIONS.name), claimed_first
            )
            return _with_tables(
                snapshot, {HISTORY_TABLE: history, NOTIFICATIONS.name: notifications}
            )
        case PairFact.HEARTBEAT_SENDERS:
            senders = _heartbeat_senders(snapshot.table(HEARTBEATS_TABLE))
            return _with_tables(snapshot, {HEARTBEATS_TABLE: senders})
        case PairFact.ONE_KEYED_SEND_WINNER:
            tasks = _keyed_inputs_as_winning_command(snapshot.table(TASKS_TABLE))
            return _with_tables(snapshot, {TASKS_TABLE: tasks})
        case PairFact.CONCURRENT_STARTS_IN_ANY_ORDER:
            tasks = _enqueue_times_of_the_race(snapshot.table(TASKS_TABLE))
            return _with_tables(snapshot, {TASKS_TABLE: tasks})
        case PairFact.ONE_MOVER_PER_ROUND:
            history = snapshot.table(HISTORY_TABLE)
            notifications = _without_owed_wakes(
                snapshot.table(NOTIFICATIONS.name), _owed_capacity_wakes(history)
            )
            return _with_tables(
                snapshot,
                {
                    HISTORY_TABLE: _moves_of_the_round(history, ROUND_MOVE_COLUMNS),
                    PHASE2_PENDING_TABLE: _moves_of_the_round(
                        snapshot.table(PHASE2_PENDING_TABLE), ('terminalization_kind',)
                    ),
                    NOTIFICATIONS.name: notifications,
                },
            )


def project(snapshot: NormalizedSnapshot, compare: PairCompare) -> NormalizedSnapshot:
    """``snapshot`` as the pair of its test compares it: as captured under
    ``WholeSnapshot`` and under a ``DecidedDifference``, reduced to the fact
    of the rule under a ``PairRule``. The snapshot must hold the tables of
    the rule (asserted by ``NormalizedSnapshot.table``)."""
    match compare:
        case WholeSnapshot() | DecidedDifference():
            return snapshot
        case PairRule(fact=fact):
            return _reduced(snapshot, fact)


# ---- Decided differences ----

RUNNING_STATUS: Final = ExactValue(text='RUNNING')
COMMITTED_STATUS: Final = ExactValue(text='COMPLETED')
MOVE_EVENT_CHANNELS: Final = (
    ExactValue(text='task_done'),
    ExactValue(text='horsies_task_status'),
)
"""The channels of the events of a move to history, normalized (``Exact``)."""


def _cell_value(cells: tuple[Cell, ...], column: str) -> NormalizedValue:
    """The value of ``column`` in the cells of a diff row. The column must be there once."""
    match [cell.value for cell in cells if cell.column == column]:
        case [value]:
            return value
        case found:
            raise AssertionError(f'a diff row has {len(found)} cells of {column!r}')


def _received_writes_explain(entries: tuple[DiffEntry, ...]) -> bool:
    """Whether each entry is part of the commit of a received write, and each
    commit is whole: one RUNNING row of the reference per committed history
    row, and one event of each move channel per committed row."""
    left_running = 0
    committed: list[NormalizedValue] = []
    events: Counter[tuple[NormalizedValue, NormalizedValue]] = Counter()
    for entry in entries:
        match entry:
            case MissingRow(table=table, cells=cells) if (
                table == TASKS_TABLE and _cell_value(cells, 'status') == RUNNING_STATUS
            ):
                left_running += 1
            case ExtraRow(table=table, cells=cells) if (
                table == HISTORY_TABLE
                and _cell_value(cells, 'status') == COMMITTED_STATUS
            ):
                committed.append(_cell_value(cells, 'task_id'))
            case ExtraRow(table=table, cells=cells) if (
                table == NOTIFICATIONS.name
                and _cell_value(cells, 'channel') in MOVE_EVENT_CHANNELS
            ):
                events[(_cell_value(cells, 'channel'), _cell_value(cells, 'task_id'))] += 1
            case _:
                return False
    owed = Counter(
        (channel, task) for task in committed for channel in MOVE_EVENT_CHANNELS
    )
    return (
        left_running == len(committed)
        and len(set(committed)) == len(committed)
        and events == owed
    )


def decided_difference_explains(fact: DecidedFact, diff: SnapshotDiff) -> bool:
    """Whether ``fact`` explains every entry of ``diff`` (an empty diff: yes)."""
    match fact:
        case DecidedFact.RECEIVED_WRITE_RUNS_TO_ITS_END:
            return _received_writes_explain(diff.entries)
