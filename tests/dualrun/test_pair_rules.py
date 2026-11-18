"""The pair rules of the integration dual-run (``PAIR_RULES``), over raw
snapshots normalized by ``NOTIFIED_CATALOG``. For each rule, the difference
that the run timing makes passes, and a difference in the fact of the rule
still fails. No database."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.catalog import (
    BASELINE_CATALOG,
    NOTIFICATIONS,
    NOTIFIED_CATALOG,
    notification_row,
)
from tests.dualrun.cells import RawRow, RawSnapshot, RawTable
from tests.dualrun.compare import render_diff
from tests.dualrun.normalize import NormalizedSnapshot, normalize_snapshot
from tests.dualrun.operations.plugin import PAIR_RULES
from tests.dualrun.operations.table import (
    PairCompare,
    PairFact,
    PairRule,
    WholeSnapshot,
)
from tests.dualrun.pair_outcomes import (
    DualRunMismatch,
    PairEqual,
    ReferenceCaptured,
    judge_pair,
)
from tests.dualrun.pair_rules import HEARTBEATS_TABLE, HISTORY_TABLE
from tests.dualrun.rules import TableRules
from tests.dualrun.values import RunWindow

pytestmark = [pytest.mark.unit]

RUN_A: Final = datetime(2026, 11, 17, 10, 5, tzinfo=UTC)
RUN_B: Final = datetime(2026, 11, 17, 14, 20, tzinfo=UTC)

type Row = Mapping[str, str | None]


def _window(run: datetime) -> RunWindow:
    return RunWindow(start=run, end=run + timedelta(minutes=1))


def _text(at: datetime) -> str:
    return at.isoformat(sep=' ')


def _raw_rows(rules: TableRules, rows: Sequence[Row]) -> tuple[RawRow, ...]:
    """``rows`` in the column order of ``rules``; a column that a row does not name is NULL."""
    columns = {spec.name for spec in rules.columns}
    for row in rows:
        assert set(row) <= columns, f'{rules.name} has no column {set(row) - columns}'
    return tuple(tuple(row.get(spec.name) for spec in rules.columns) for row in rows)


def _snapshot(
    run: datetime, rows: Mapping[str, Sequence[Row]], events: Sequence[tuple[str, str]]
) -> NormalizedSnapshot:
    """The normalized snapshot of ``rows`` per table and of the notifications ``events``."""
    tables = tuple(
        RawTable(name=rules.name, rows=_raw_rows(rules, rows.get(rules.name, ())))
        for rules in BASELINE_CATALOG.tables
    )
    sent = RawTable(
        name=NOTIFICATIONS.name,
        rows=tuple(notification_row(channel, payload) for channel, payload in events),
    )
    raw = RawSnapshot(tables=(*tables, sent))
    match normalize_snapshot(raw, NOTIFIED_CATALOG, _window(run)):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def _rule(fact: PairFact) -> PairRule:
    match [rule for rule in PAIR_RULES if rule.fact is fact]:
        case [rule]:
            return rule
        case rules:
            raise AssertionError(f'{fact.name} has {len(rules)} rules')


def _judge(
    reference: NormalizedSnapshot, candidate: NormalizedSnapshot, compare: PairCompare
) -> list[str]:
    """The lines of the diff of the pair, empty when it is equal."""
    match judge_pair(
        ReferenceCaptured(snapshot=reference, window=_window(RUN_A)),
        candidate,
        _window(RUN_B),
        compare,
    ):
        case PairEqual():
            return []
        case DualRunMismatch(diff=diff):
            return render_diff(diff).splitlines()
        case straddle:
            raise AssertionError(f'both windows are inside one hour: {straddle}')


# ---- One race winner ----

RACE: Final = _rule(PairFact.ONE_RACE_WINNER)
CLAIMER: Final = 'worker-actions-bbbb'
CLAIM_AFTER: Final = timedelta(milliseconds=200)
STATUS: Final = 'horsies_task_status'
MOVE_EVENTS: Final = ('task_done', STATUS)
"""The notifications of the move to history."""

CLAIM_STATUS_LINE: Final = (
    "notifications [channel='horsies_task_status', task_id=task#1, workflow_id=NULL, "
    "payload=NULL]: row only in the {side}: channel='horsies_task_status', task_id=task#1, "
    'workflow_id=NULL, payload=NULL'
)


def _cancelled(
    task: str,
    run: datetime,
    claim_time: timedelta | None,
    claim_worker: str | None,
    changes: Row,
) -> Row:
    """The history row of a task that the monitoring cancel moved; ``changes`` replace its columns."""
    at = _text(run)
    return {
        'tableoid': f'horsies_task_history_standard_30d_{run:%Y_%m_%d}',
        'task_id': task,
        'task_name': 'action_task',
        'queue_name': 'default',
        'priority': '100',
        'command_fingerprint_version': '1',
        'command_fingerprint': f'{task}-fingerprint',
        'retention_class_key': 'standard_30d',
        'is_workflow_task': 'false',
        'sent_at': at,
        'created_at': at,
        'enqueued_at': at,
        'claimed_at': None if claim_time is None else _text(run + claim_time),
        'terminal_at': _text(run + timedelta(seconds=1)),
        'status': 'CANCELLED',
        'terminalization_kind': 'CANCEL_ADMIN',
        'retry_count': '0',
        'max_retries': '3',
        'last_claimed_worker_id': claim_worker,
        'error_code': 'TASK_CANCELLED',
        'final_failed_reason': 'Cancelled via monitoring API',
        'result_envelope_version': '1',
        'history_schema_version': '1',
        'attempt_archive_version': '1',
        'rerun_input_disposition': 'DECLINED_BY_POLICY',
        **changes,
    }


def _race(
    run: datetime,
    prefix: str,
    *,
    claim_time: timedelta | None,
    claim_worker: str | None,
    events: Sequence[str],
    changes: Row | None = None,
) -> NormalizedSnapshot:
    task = f'{prefix}-task'
    row = _cancelled(task, run, claim_time, claim_worker, changes or {})
    return _snapshot(
        run, {HISTORY_TABLE: [row]}, [(channel, task) for channel in events]
    )


def _cancel_first(run: datetime, prefix: str) -> NormalizedSnapshot:
    """The cancel took the PENDING row; the claim found nothing."""
    return _race(run, prefix, claim_time=None, claim_worker=None, events=MOVE_EVENTS)


def _claim_first(run: datetime, prefix: str) -> NormalizedSnapshot:
    """The claim took the PENDING row and sent its status event; the cancel then took the CLAIMED row."""
    return _race(
        run,
        prefix,
        claim_time=CLAIM_AFTER,
        claim_worker=CLAIMER,
        events=(STATUS, *MOVE_EVENTS),
    )


def test_without_a_rule_the_two_winners_of_the_race_differ() -> None:
    assert _judge(
        _claim_first(RUN_A, 'a'), _cancel_first(RUN_B, 'b'), WholeSnapshot()
    ) == [
        'horsies_task_history [task_id=task#1]: claimed_at: reference <during run>, candidate NULL',
        'horsies_task_history [task_id=task#1]: last_claimed_worker_id: reference worker#1, '
        'candidate NULL',
        CLAIM_STATUS_LINE.format(side='reference'),
    ]


type RaceRun = Callable[[datetime, str], NormalizedSnapshot]


@pytest.mark.parametrize(
    ('reference', 'candidate'),
    [
        (_claim_first, _cancel_first),
        (_cancel_first, _claim_first),
        (_claim_first, _claim_first),
        (_cancel_first, _cancel_first),
    ],
    ids=['claim-cancel', 'cancel-claim', 'claim-claim', 'cancel-cancel'],
)
def test_the_race_rule_passes_either_winner_against_either(
    reference: RaceRun, candidate: RaceRun
) -> None:
    assert _judge(reference(RUN_A, 'a'), candidate(RUN_B, 'b'), RACE) == []


@pytest.mark.parametrize(
    ('claim_time', 'claim_worker', 'facts'),
    [
        (CLAIM_AFTER, None, 'claimed_at <during run>, last_claimed_worker_id NULL'),
        (None, CLAIMER, 'claimed_at NULL, last_claimed_worker_id worker#1'),
    ],
    ids=['time-without-worker', 'worker-without-time'],
)
@pytest.mark.parametrize(
    'reference', [_claim_first, _cancel_first], ids=['claim', 'cancel']
)
def test_the_race_rule_fails_claim_facts_that_agree_with_neither_winner(
    reference: RaceRun,
    claim_time: timedelta | None,
    claim_worker: str | None,
    facts: str,
) -> None:
    candidate = _race(
        RUN_B,
        'b',
        claim_time=claim_time,
        claim_worker=claim_worker,
        events=(STATUS, *MOVE_EVENTS),
    )

    assert _judge(reference(RUN_A, 'a'), candidate, RACE) == [
        "horsies_task_history [task_id=task#1]: claim facts: reference 'agree with one "
        f"winner', candidate 'agree with neither winner: {facts}'",
        CLAIM_STATUS_LINE.format(side='candidate'),
    ]


def test_the_race_rule_fails_a_claim_first_without_the_status_event_of_the_claim() -> (
    None
):
    candidate = _race(
        RUN_B, 'b', claim_time=CLAIM_AFTER, claim_worker=CLAIMER, events=MOVE_EVENTS
    )

    assert _judge(_claim_first(RUN_A, 'a'), candidate, RACE) == [
        CLAIM_STATUS_LINE.format(side='reference'),
    ]


def test_the_race_rule_compares_every_other_column_as_captured() -> None:
    candidate = _race(
        RUN_B,
        'b',
        claim_time=None,
        claim_worker=None,
        events=MOVE_EVENTS,
        changes={'terminalization_kind': 'EXPIRE_PENDING'},
    )

    assert _judge(_claim_first(RUN_A, 'a'), candidate, RACE) == [
        'horsies_task_history [task_id=task#1]: terminalization_kind: reference '
        "'CANCEL_ADMIN', candidate 'EXPIRE_PENDING'",
    ]


# ---- Heartbeat senders ----

SENDERS: Final = _rule(PairFact.HEARTBEAT_SENDERS)
SEAT_WORKER: Final = 'thread-lifecycle-worker'
PID: Final = '17424'
SENDER: Final = f'{SEAT_WORKER}:{PID}'
SEAT_TASKS: Final = 3


@dataclass(frozen=True, slots=True)
class Beat:
    """One heartbeat row: the task by its seed position, and its send time after the run start."""

    task: int
    after_ms: int
    sender: str = SENDER
    role: str = 'runner'
    pid: str = PID


def _seat_task(task: str, run: datetime, position: int) -> Row:
    """A RUNNING task of a seat thread; the seed order sets its send time."""
    at = _text(run)
    return {
        'id': task,
        'task_name': 'thread_lifecycle_sleep',
        'queue_name': 'default',
        'priority': '100',
        'input': '{}',
        'execution': 'SHARED_THREAD',
        'retention_class_key': 'standard_30d',
        'sent_at': _text(
            run - timedelta(seconds=10) + timedelta(milliseconds=position)
        ),
        'created_at': at,
        'command_fingerprint_version': '1',
        'command_fingerprint': f'{task}-fingerprint',
        'status': 'RUNNING',
        'enqueued_at': at,
        'claimed_at': at,
        'claimed_by_worker_id': SEAT_WORKER,
        'started_at': at,
        'retry_count': '0',
        'max_retries': '0',
        'is_workflow_task': 'false',
        'updated_at': at,
        'retain_rerun_input': 'false',
    }


def _heartbeat(task: str, run: datetime, beat: Beat) -> Row:
    at = run + timedelta(milliseconds=beat.after_ms)
    return {
        'tableoid': f'horsies_heartbeats_{at:%Y_%m_%d_%H}',
        'task_id': task,
        'role': beat.role,
        'sent_at': _text(at),
        'sender_id': beat.sender,
        'hostname': 'host-a',
        'pid': beat.pid,
    }


def _seat_run(run: datetime, prefix: str, beats: Sequence[Beat]) -> NormalizedSnapshot:
    tasks = [f'{prefix}-task-{position}' for position in range(SEAT_TASKS)]
    return _snapshot(
        run,
        {
            'horsies_tasks': [
                _seat_task(task, run, position) for position, task in enumerate(tasks)
            ],
            HEARTBEATS_TABLE: [
                _heartbeat(tasks[beat.task], run, beat) for beat in beats
            ],
        },
        [],
    )


ONE_BEAT_EACH: Final = (Beat(0, 10), Beat(1, 20), Beat(2, 30))
OTHER_ORDER_AND_COUNT: Final = (Beat(0, 30), Beat(1, 10), Beat(1, 40), Beat(2, 20))
"""Other send ranks, and two beats of the second task."""

SENDER_ROW: Final = (
    'horsies_heartbeats, senders per task and role [task_id=task#2, role={role}]: row only '
    "in the {side}: task_id=task#2, role={role}, sender_id={sender}, hostname='host-a', "
    "pid='17424'"
)


def test_without_a_rule_other_send_ranks_and_counts_differ() -> None:
    lines = _judge(
        _seat_run(RUN_A, 'a', ONE_BEAT_EACH),
        _seat_run(RUN_B, 'b', OTHER_ORDER_AND_COUNT),
        WholeSnapshot(),
    )

    assert lines
    assert all(line.startswith('horsies_heartbeats [') for line in lines)


def test_the_heartbeat_rule_passes_other_send_ranks_and_counts() -> None:
    reference = _seat_run(RUN_A, 'a', ONE_BEAT_EACH)
    candidate = _seat_run(RUN_B, 'b', OTHER_ORDER_AND_COUNT)

    assert _judge(reference, candidate, SENDERS) == []
    assert _judge(candidate, reference, SENDERS) == []


@pytest.mark.parametrize(
    ('beats', 'expected'),
    [
        (
            (Beat(0, 10), Beat(1, 20, sender=SEAT_WORKER), Beat(2, 30)),
            "horsies_heartbeats, senders per task and role [task_id=task#2, role='runner']: "
            'sender_id: reference worker#2, candidate worker#1',
        ),
        (
            (Beat(0, 10), Beat(1, 20, pid='99'), Beat(2, 30)),
            "horsies_heartbeats, senders per task and role [task_id=task#2, role='runner']: "
            "pid: reference '17424', candidate '99'",
        ),
        (
            (Beat(0, 10), Beat(2, 30)),
            SENDER_ROW.format(role="'runner'", side='reference', sender='worker#2'),
        ),
        (
            (*ONE_BEAT_EACH, Beat(1, 25, sender='other-worker:1')),
            SENDER_ROW.format(role="'runner'", side='candidate', sender='worker#3'),
        ),
        (
            (*ONE_BEAT_EACH, Beat(1, 25, role='claimer')),
            SENDER_ROW.format(role="'claimer'", side='candidate', sender='worker#2'),
        ),
    ],
    ids=[
        'other-sender',
        'other-pid',
        'task-without-beats',
        'second-sender',
        'other-role',
    ],
)
def test_the_heartbeat_rule_fails_a_difference_in_the_senders(
    beats: tuple[Beat, ...], expected: str
) -> None:
    reference = _seat_run(RUN_A, 'a', ONE_BEAT_EACH)

    assert _judge(reference, _seat_run(RUN_B, 'b', beats), SENDERS) == [expected]
