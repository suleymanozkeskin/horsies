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

Neither reduction hides which rows exist or who owns them. A history row,
or a task and role of a heartbeat, on one side only is still a row on one
side only. A sender that differs, and claim facts that agree with neither
winner against facts that agree, are still column differences. A claim
first without its status notification still leaves one notification less.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final

from tests.dualrun.catalog import NOTIFICATIONS
from tests.dualrun.normalize import NormalizedRow, NormalizedSnapshot, NormalizedTable
from tests.dualrun.operations.table import (
    PairCompare,
    PairFact,
    PairRule,
    WholeSnapshot,
)
from tests.dualrun.rules import ColumnSpec, Exact, TableRules
from tests.dualrun.values import (
    DuringRun,
    ExactValue,
    IdLabel,
    NormalizedValue,
    NullValue,
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


def project(snapshot: NormalizedSnapshot, compare: PairCompare) -> NormalizedSnapshot:
    """``snapshot`` as the pair of its test compares it: as captured under
    ``WholeSnapshot``, reduced to the fact of the rule under a ``PairRule``.
    The snapshot must hold the tables of the rule (asserted by
    ``NormalizedSnapshot.table``)."""
    match compare:
        case WholeSnapshot():
            return snapshot
        case PairRule(fact=fact):
            return _reduced(snapshot, fact)
