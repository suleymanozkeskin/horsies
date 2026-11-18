"""The pair rules of the integration dual-run (``PAIR_RULES``) and the
decided differences (``DECIDED_DIFFERENCES``), over raw snapshots normalized
by ``NOTIFIED_CATALOG``. For each rule, the difference that the run timing
makes passes, and a difference in the fact of the rule still fails. For each
decided difference, the difference that the decision explains is reported as
decided, and any other difference still fails. No database."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
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
from tests.dualrun.operations.lifecycle import LIFECYCLE_DECIDED_DIFFERENCES
from tests.dualrun.operations.plugin import PAIR_RULES
from tests.dualrun.operations.table import (
    PairCompare,
    PairFact,
    PairRule,
    WholeSnapshot,
)
from tests.dualrun.pair_outcomes import (
    DecidedDifferenceSeen,
    DualRunMismatch,
    PairEqual,
    PairJudgement,
    ReferenceCaptured,
    judge_pair,
)
from tests.dualrun.pair_rules import (
    HEARTBEATS_TABLE,
    HISTORY_TABLE,
    PHASE2_PENDING_TABLE,
    TASKS_TABLE,
)
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


# ---- One keyed send winner ----

KEYED: Final = _rule(PairFact.ONE_KEYED_SEND_WINNER)
COMMANDS: Final = ('{"order":"order-1"}', '{"order":"order-2"}')
"""The two commands of the sends of one key."""
KEY_DIGEST: Final = 'key-digest'


def _keyed_task(task: str, run: datetime, task_input: str, digest: str | None) -> Row:
    """A PENDING task that a send committed; ``digest`` is its key digest, or NULL."""
    at = _text(run)
    return {
        'id': task,
        'task_name': 'keyed_task',
        'queue_name': 'default',
        'priority': '100',
        'input': task_input,
        'execution': 'ISOLATED',
        'retention_class_key': 'standard_30d',
        'sent_at': at,
        'created_at': at,
        'command_fingerprint_version': '1',
        'command_fingerprint': f'{task}-fingerprint',
        'idempotency_key_digest': digest,
        'status': 'PENDING',
        'enqueued_at': at,
        'retry_count': '0',
        'max_retries': '0',
        'is_workflow_task': 'false',
        'updated_at': at,
        'retain_rerun_input': 'false',
    }


def _sends(
    run: datetime,
    prefix: str,
    inputs: Sequence[str],
    *,
    digest: str | None = KEY_DIGEST,
    changes: Row | None = None,
) -> NormalizedSnapshot:
    """The tasks that the sends of one key committed, one per input, and their events."""
    tasks = [f'{prefix}-task-{position}' for position in range(len(inputs))]
    rows = [
        {**_keyed_task(task, run, task_input, digest), **(changes or {})}
        for task, task_input in zip(tasks, inputs, strict=True)
    ]
    return _snapshot(
        run,
        {TASKS_TABLE: rows},
        [event for task in tasks for event in (('task_new', task), ('task_queue_default', 'new'))],
    )


def test_without_a_rule_the_command_of_the_winner_differs() -> None:
    assert _judge(
        _sends(RUN_A, 'a', [COMMANDS[1]]), _sends(RUN_B, 'b', [COMMANDS[0]]), WholeSnapshot()
    ) == [
        'horsies_tasks [id=task#1]: input: reference {"order":"order-2"}, '
        'candidate {"order":"order-1"}',
    ]


@pytest.mark.parametrize(
    ('reference', 'candidate'),
    [(0, 1), (1, 0), (0, 0), (1, 1)],
    ids=['one-two', 'two-one', 'one-one', 'two-two'],
)
def test_the_keyed_send_rule_passes_either_command_against_either(
    reference: int, candidate: int
) -> None:
    assert (
        _judge(
            _sends(RUN_A, 'a', [COMMANDS[reference]]),
            _sends(RUN_B, 'b', [COMMANDS[candidate]]),
            KEYED,
        )
        == []
    )


def test_the_keyed_send_rule_fails_a_second_committed_task() -> None:
    lines = _judge(
        _sends(RUN_A, 'a', [COMMANDS[0]]),
        _sends(RUN_B, 'b', [COMMANDS[0], COMMANDS[1]]),
        KEYED,
    )

    assert lines
    assert all('row only in the candidate' in line for line in lines), lines
    assert lines[0].startswith('horsies_tasks [id=task#2]: row only in the candidate')


def test_the_keyed_send_rule_fails_an_input_that_is_not_a_document() -> None:
    assert _judge(
        _sends(RUN_A, 'a', [COMMANDS[0]]), _sends(RUN_B, 'b', ['order-1']), KEYED
    ) == [
        "horsies_tasks [id=task#1]: input: reference 'the command of the send that won', "
        "candidate <not json 'order-1'>",
    ]


def test_the_keyed_send_rule_compares_the_input_of_a_task_without_a_key() -> None:
    assert _judge(
        _sends(RUN_A, 'a', [COMMANDS[0]], digest=None),
        _sends(RUN_B, 'b', [COMMANDS[1]], digest=None),
        KEYED,
    ) == [
        'horsies_tasks [id=task#1]: input: reference {"order":"order-1"}, '
        'candidate {"order":"order-2"}',
    ]


def test_the_keyed_send_rule_compares_every_other_column_as_captured() -> None:
    assert _judge(
        _sends(RUN_A, 'a', [COMMANDS[0]]),
        _sends(RUN_B, 'b', [COMMANDS[1]], changes={'priority': '1'}),
        KEYED,
    ) == ["horsies_tasks [id=task#1]: priority: reference '100', candidate '1'"]


# ---- Concurrent starts in any order ----

STARTS: Final = _rule(PairFact.CONCURRENT_STARTS_IN_ANY_ORDER)
SEND_GAP: Final = timedelta(milliseconds=1)
ENQUEUE_GAP: Final = timedelta(milliseconds=5)
START_INPUT: Final = '{"value":1}'


def _starts(
    run: datetime,
    prefix: str,
    enqueue_order: Sequence[int],
    *,
    changes: Row | None = None,
) -> NormalizedSnapshot:
    """The task of each of the concurrent starts of one spec: sent in the
    order of their position, enqueued in ``enqueue_order`` (the position of
    the task that the database took first, then the next)."""
    rows: list[Row] = []
    for position in range(len(enqueue_order)):
        task = f'{prefix}-task-{position}'
        enqueued = run + ENQUEUE_GAP * (enqueue_order.index(position) + 1)
        rows.append(
            {
                **_keyed_task(task, run, START_INPUT, None),
                'task_name': 'auto_id_concurrent_a',
                'sent_at': _text(run + SEND_GAP * position),
                'enqueued_at': _text(enqueued),
                **(changes or {}),
            }
        )
    return _snapshot(
        run,
        {TASKS_TABLE: rows},
        [
            event
            for position in range(len(enqueue_order))
            for event in (('task_new', f'{prefix}-task-{position}'), ('task_queue_default', 'new'))
        ],
    )


def test_without_a_rule_the_enqueue_order_of_concurrent_starts_differs() -> None:
    assert _judge(
        _starts(RUN_A, 'a', [0, 1]), _starts(RUN_B, 'b', [1, 0]), WholeSnapshot()
    ) == [
        'horsies_tasks [id=task#1]: enqueued_at: reference <time rank 1>, '
        'candidate <time rank 2>',
        'horsies_tasks [id=task#2]: enqueued_at: reference <time rank 2>, '
        'candidate <time rank 1>',
    ]


@pytest.mark.parametrize(
    ('reference', 'candidate'),
    [((0, 1), (1, 0)), ((1, 0), (0, 1)), ((0, 1), (0, 1)), ((1, 0), (1, 0))],
    ids=['sent-order-against-reverse', 'reverse-against-sent-order', 'same', 'same-reverse'],
)
def test_the_start_rule_passes_either_enqueue_order_against_either(
    reference: tuple[int, int], candidate: tuple[int, int]
) -> None:
    assert _judge(_starts(RUN_A, 'a', reference), _starts(RUN_B, 'b', candidate), STARTS) == []


def test_the_start_rule_fails_a_start_whose_task_is_not_committed() -> None:
    lines = _judge(_starts(RUN_A, 'a', [0, 1]), _starts(RUN_B, 'b', [0]), STARTS)

    assert lines
    assert lines[0].startswith('horsies_tasks [id=task#2]: row only in the reference'), lines


def test_the_start_rule_keeps_a_task_with_no_enqueue_time() -> None:
    assert _judge(
        _starts(RUN_A, 'a', [0, 1]),
        _starts(RUN_B, 'b', [1, 0], changes={'enqueued_at': None}),
        STARTS,
    ) == [
        "horsies_tasks [id=task#1]: enqueued_at: reference 'enqueued by one of the "
        "concurrent starts', candidate NULL",
        "horsies_tasks [id=task#2]: enqueued_at: reference 'enqueued by one of the "
        "concurrent starts', candidate NULL",
    ]


def test_the_start_rule_compares_every_other_column_as_captured() -> None:
    assert _judge(
        _starts(RUN_A, 'a', [0, 1]),
        _starts(RUN_B, 'b', [1, 0], changes={'priority': '1'}),
        STARTS,
    ) == [
        "horsies_tasks [id=task#1]: priority: reference '100', candidate '1'",
        "horsies_tasks [id=task#2]: priority: reference '100', candidate '1'",
    ]


# ---- One mover per round ----

ROUNDS: Final = _rule(PairFact.ONE_MOVER_PER_ROUND)
RACE_WORKER: Final = 'lock-order-worker'
STALE_REASON: Final = 'Worker process crashed (no runner heartbeat for 1000ms = 1.0s)'


class Mover(Enum):
    """Who moved the task of one round, and what the round's task is."""

    FAILURE_BY_FINALIZE = 'the finalize of a plain task that failed'
    FAILURE_BY_STALE_RECOVERY = 'the stale recovery of a plain task'
    SUCCESS_BY_FINALIZE = 'the finalize of a workflow task that succeeded'
    SUCCESS_BY_STALE_RECOVERY = 'the stale recovery of a workflow task'
    WORKFLOW_FAILURE_BY_FINALIZE = 'the finalize of a workflow task that failed'


def _hex(payload: bytes) -> str:
    return '\\x' + payload.hex()


def _digest(payload: bytes) -> str:
    return _hex(hashlib.sha256(payload).digest())


def _micros(at: datetime) -> int:
    return (at - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _move(task: str, run: datetime, mover: Mover) -> tuple[Row, bytes, list[object]]:
    """The columns that ``mover`` writes, its result envelope and its attempt."""
    started, finished = run - timedelta(hours=1), run + timedelta(milliseconds=5)
    match mover:
        case Mover.FAILURE_BY_FINALIZE | Mover.WORKFLOW_FAILURE_BY_FINALIZE:
            code, reason, status, kind, outcome = (
                'BROKER_ERROR', 'worker lost the child', 'FAILED', 'FAIL_RUNNING', 'WORKER_FAILURE'
            )
            data: object = {'task_id': task}
        case Mover.SUCCESS_BY_FINALIZE:
            code, reason, status, kind, outcome = (None, None, 'COMPLETED', 'COMPLETE_LOCKED', 'COMPLETED')
            data = None
        case Mover.FAILURE_BY_STALE_RECOVERY | Mover.SUCCESS_BY_STALE_RECOVERY:
            code, reason, status, kind, outcome = (
                'WORKER_CRASHED', STALE_REASON, 'FAILED', 'FAIL_STALE', 'FAILED'
            )
            data = {
                'started_at': started.isoformat(),
                'detected_at': finished.isoformat(),
                'worker_id': RACE_WORKER,
                'worker_pid': 4242,
            }
    error = None if code is None else {
        'data': data,
        'error_code': {'__builtin_task_code__': code},
        'exception': None,
        'message': reason,
    }
    envelope = json.dumps({'__h_task_result__': True, 'err': error, 'ok': None}).encode()
    attempt: list[object] = [
        1, outcome, False, _micros(started), _micros(finished),
        code, reason, reason, RACE_WORKER, 'host-1', 4242, 'child-1',
    ]
    columns: Row = {
        'status': status,
        'terminalization_kind': kind,
        'error_code': code,
        'final_failed_reason': reason,
    }
    return columns, envelope, attempt


def _is_workflow_task(mover: Mover) -> bool:
    match mover:
        case Mover.FAILURE_BY_FINALIZE | Mover.FAILURE_BY_STALE_RECOVERY:
            return False
        case (
            Mover.SUCCESS_BY_FINALIZE
            | Mover.SUCCESS_BY_STALE_RECOVERY
            | Mover.WORKFLOW_FAILURE_BY_FINALIZE
        ):
            return True


def _round_row(task: str, run: datetime, position: int, mover: Mover, changes: Row) -> Row:
    """The history row of the task of one round; the round's position sets its send time."""
    columns, envelope, attempt = _move(task, run, mover)
    archive = json.dumps([attempt]).encode()
    sent = _text(run - timedelta(seconds=10) + timedelta(milliseconds=position))
    workflow_task = _is_workflow_task(mover)
    return {
        'tableoid': f'horsies_task_history_standard_30d_{run:%Y_%m_%d}',
        'task_id': task,
        'task_name': 'lock_order_task',
        'queue_name': 'default',
        'priority': '100',
        'retention_class_key': 'standard_30d',
        'is_workflow_task': 'true' if workflow_task else 'false',
        'workflow_id': f'{task}-workflow' if workflow_task else None,
        'sent_at': sent,
        'created_at': _text(run),
        'enqueued_at': _text(run),
        'claimed_at': _text(run - timedelta(hours=1)),
        'started_at': _text(run - timedelta(hours=1)),
        'terminal_at': _text(run + timedelta(milliseconds=5)),
        'retry_count': '0',
        'max_retries': '0',
        'last_claimed_worker_id': RACE_WORKER,
        'last_worker_hostname': 'host-1',
        'last_worker_pid': '4242',
        'last_worker_process_name': 'child-1',
        'result_envelope_version': '1',
        'result_payload': _hex(envelope),
        'result_digest': _digest(envelope),
        'history_schema_version': '1',
        'attempt_archive_version': '1',
        'attempt_snapshot': _hex(archive),
        'attempt_snapshot_digest': _digest(archive),
        'rerun_input_disposition': 'DECLINED_BY_POLICY',
        **columns,
        **changes,
    }


def _phase2_row(task: str, run: datetime, kind: str | None) -> Row:
    """The pending phase 2 of a workflow task's move."""
    return {
        'task_id': task,
        'workflow_id': f'{task}-workflow',
        'workflow_node_row_id': f'{task}-node',
        'terminalization_kind': kind,
        'history_class': 'standard_30d',
        'history_anchor': _text(run),
        'history_schema_version': '1',
        'created_at': _text(run),
        'attempt_count': '0',
    }


def _rounds(
    run: datetime,
    prefix: str,
    movers: Sequence[Mover],
    *,
    changes: Mapping[int, Row] | None = None,
    wakes: int | None = None,
) -> NormalizedSnapshot:
    """The history rows of the rounds and their events. ``wakes`` is the count
    of capacity wakes; by default one per failure that the finalize moved."""
    tasks = [f'{prefix}-task-{position}' for position in range(len(movers))]
    history = [
        _round_row(task, run, position, mover, (changes or {}).get(position, {}))
        for position, (task, mover) in enumerate(zip(tasks, movers, strict=True))
    ]
    phase2 = [
        _phase2_row(task, run, row['terminalization_kind'])
        for task, mover, row in zip(tasks, movers, history, strict=True)
        if _is_workflow_task(mover)
    ]
    owed = sum(1 for mover in movers if mover is Mover.FAILURE_BY_FINALIZE)
    events = [
        *(event for task in tasks for event in (('task_done', task), (STATUS, task))),
        *(('task_queue_default', 'capacity') for _ in range(owed if wakes is None else wakes)),
    ]
    return _snapshot(run, {HISTORY_TABLE: history, PHASE2_PENDING_TABLE: phase2}, events)


FAILURES_A: Final = (Mover.FAILURE_BY_STALE_RECOVERY, Mover.FAILURE_BY_FINALIZE, Mover.FAILURE_BY_FINALIZE)
FAILURES_B: Final = (Mover.FAILURE_BY_FINALIZE, Mover.FAILURE_BY_FINALIZE, Mover.FAILURE_BY_FINALIZE)
SUCCESSES_A: Final = (Mover.SUCCESS_BY_FINALIZE, Mover.SUCCESS_BY_FINALIZE)
SUCCESSES_B: Final = (Mover.SUCCESS_BY_STALE_RECOVERY, Mover.SUCCESS_BY_FINALIZE)


def test_without_a_rule_a_round_that_another_side_moved_differs() -> None:
    lines = _judge(
        _rounds(RUN_A, 'a', FAILURES_B), _rounds(RUN_B, 'b', FAILURES_A), WholeSnapshot()
    )

    assert [line.split(': reference')[0] for line in lines] == [
        'horsies_task_history [task_id=task#1]: terminalization_kind',
        'horsies_task_history [task_id=task#1]: error_code',
        'horsies_task_history [task_id=task#1]: final_failed_reason',
        'horsies_task_history [task_id=task#1]: result_payload',
        'horsies_task_history [task_id=task#1]: attempt_snapshot',
        "notifications [channel='task_queue_default', task_id=NULL, workflow_id=NULL, "
        "payload='capacity']: row only in the reference: channel='task_queue_default', "
        "task_id=NULL, workflow_id=NULL, payload='capacity'",
    ]


@pytest.mark.parametrize(
    ('reference', 'candidate'),
    [
        (FAILURES_B, FAILURES_A),
        (FAILURES_A, FAILURES_B),
        (FAILURES_A, FAILURES_A),
        (SUCCESSES_A, SUCCESSES_B),
        (SUCCESSES_B, SUCCESSES_A),
    ],
    ids=[
        'failure-finalize-stale',
        'failure-stale-finalize',
        'failure-same',
        'success-finalize-stale',
        'success-stale-finalize',
    ],
)
def test_the_round_rule_passes_either_mover_of_each_round(
    reference: tuple[Mover, ...], candidate: tuple[Mover, ...]
) -> None:
    assert _judge(_rounds(RUN_A, 'a', reference), _rounds(RUN_B, 'b', candidate), ROUNDS) == []


def test_a_failure_of_a_workflow_task_by_the_finalize_owes_no_capacity_wake() -> None:
    """Its phase 2 sends the wake, not the move."""
    reference = _rounds(RUN_A, 'a', (Mover.WORKFLOW_FAILURE_BY_FINALIZE,))
    candidate = _rounds(RUN_B, 'b', (Mover.SUCCESS_BY_STALE_RECOVERY,))

    assert _judge(reference, candidate, ROUNDS) == []


def test_the_round_rule_fails_a_move_that_neither_side_writes() -> None:
    """The second round's kind is one of neither side, and that row owes no
    capacity wake, so the candidate sends two."""
    candidate = _rounds(
        RUN_B,
        'b',
        FAILURES_B,
        changes={1: {'terminalization_kind': 'CANCEL_ADMIN'}},
        wakes=2,
    )

    assert _judge(_rounds(RUN_A, 'a', FAILURES_A), candidate, ROUNDS) == [
        "horsies_task_history [task_id=task#2]: status: reference 'written by the mover "
        "of the round', candidate 'FAILED'",
        "horsies_task_history [task_id=task#2]: terminalization_kind: reference 'written "
        "by the mover of the round', candidate 'CANCEL_ADMIN'",
        "horsies_task_history [task_id=task#2]: error_code: reference 'written by the "
        "mover of the round', candidate 'BROKER_ERROR'",
        "horsies_task_history [task_id=task#2]: final_failed_reason: reference 'written "
        "by the mover of the round', candidate 'worker lost the child'",
        'horsies_task_history [task_id=task#2]: result_payload: reference \'written by '
        'the mover of the round\', candidate {"__h_task_result__":true,"err":{"data":'
        '{"task_id":task#2},"error_code":{"__builtin_task_code__":"BROKER_ERROR"},'
        '"exception":null,"message":"worker lost the child"},"ok":null}',
        'horsies_task_history [task_id=task#2]: attempt_snapshot: reference \'written by '
        'the mover of the round\', candidate [[1,"WORKER_FAILURE",false,<during run>,'
        '<during run>,"BROKER_ERROR","worker lost the child","worker lost the child",'
        'worker#1,"host-1",4242,"child-1"]]',
    ]


@pytest.mark.parametrize(
    ('wakes', 'expected'),
    [
        (
            1,
            "notifications [channel='task_queue_default', task_id=NULL, workflow_id=NULL, "
            "payload='the capacity wake of a finalize of a failure, missing']: row only in "
            "the candidate: channel='task_queue_default', task_id=NULL, workflow_id=NULL, "
            "payload='the capacity wake of a finalize of a failure, missing'",
        ),
        (
            3,
            "notifications [channel='task_queue_default', task_id=NULL, workflow_id=NULL, "
            "payload='capacity']: row only in the candidate: channel='task_queue_default', "
            "task_id=NULL, workflow_id=NULL, payload='capacity'",
        ),
    ],
    ids=['finalize-without-its-wake', 'one-wake-more'],
)
def test_the_round_rule_fails_a_capacity_wake_that_disagrees_with_the_movers(
    wakes: int, expected: str
) -> None:
    candidate = _rounds(RUN_B, 'b', FAILURES_A, wakes=wakes)

    assert _judge(_rounds(RUN_A, 'a', FAILURES_B), candidate, ROUNDS) == [expected]


def test_the_round_rule_fails_a_round_without_its_history_row() -> None:
    lines = _judge(
        _rounds(RUN_A, 'a', FAILURES_A), _rounds(RUN_B, 'b', FAILURES_A[:2]), ROUNDS
    )

    assert lines
    assert lines[0].startswith('horsies_task_history [task_id=task#3]: row only in the reference')


def test_the_round_rule_compares_every_other_column_as_captured() -> None:
    candidate = _rounds(RUN_B, 'b', FAILURES_B, changes={2: {'retry_count': '1'}})

    assert _judge(_rounds(RUN_A, 'a', FAILURES_A), candidate, ROUNDS) == [
        "horsies_task_history [task_id=task#3]: retry_count: reference '0', candidate '1'",
    ]


def test_the_round_rule_fails_a_phase2_row_whose_move_neither_side_writes() -> None:
    candidate = _rounds(
        RUN_B, 'b', SUCCESSES_B, changes={0: {'terminalization_kind': 'COMPLETE_FUSED'}}
    )

    lines = _judge(_rounds(RUN_A, 'a', SUCCESSES_A), candidate, ROUNDS)

    assert "horsies_workflow_phase2_pending [task_id=task#1]: terminalization_kind: " \
        "reference 'written by the mover of the round', candidate 'COMPLETE_FUSED'" in lines


# ---- The decided difference of the finalize batcher close ----

(CLOSE,) = LIFECYCLE_DECIDED_DIFFERENCES
BATCH_WORKER: Final = 'batch-worker'
RECEIVED: Final = 4
"""Two batches of two rows that the writes had received at the close."""
UNSENT: Final = 2
"""Rows that no write received."""


def _batch_task(task: str, run: datetime, position: int, changes: Row) -> Row:
    """A RUNNING task of the batch test; its position sets its send time."""
    sent = _text(run - timedelta(seconds=10) + timedelta(milliseconds=position))
    at = _text(run)
    return {
        'id': task,
        'task_name': 'round_trip_task',
        'queue_name': 'round_trip_queue',
        'priority': '100',
        'input': '{}',
        'execution': 'ISOLATED',
        'retention_class_key': 'standard_30d',
        'sent_at': sent,
        'created_at': at,
        'command_fingerprint_version': '1',
        'command_fingerprint': 'one-fingerprint',
        'status': 'RUNNING',
        'enqueued_at': sent,
        'claimed_at': at,
        'claimed_by_worker_id': BATCH_WORKER,
        'started_at': at,
        'retry_count': '0',
        'max_retries': '0',
        'is_workflow_task': 'false',
        'updated_at': at,
        'retain_rerun_input': 'false',
        **changes,
    }


def _committed_row(task: str, run: datetime, position: int, status: str) -> Row:
    """The history row of a received row that its write committed after the close."""
    sent = _text(run - timedelta(seconds=10) + timedelta(milliseconds=position))
    at = _text(run)
    return {
        'tableoid': f'horsies_task_history_standard_30d_{run:%Y_%m_%d}',
        'task_id': task,
        'task_name': 'round_trip_task',
        'queue_name': 'round_trip_queue',
        'priority': '100',
        'command_fingerprint_version': '1',
        'command_fingerprint': 'one-fingerprint',
        'retention_class_key': 'standard_30d',
        'is_workflow_task': 'false',
        'sent_at': sent,
        'created_at': at,
        'enqueued_at': sent,
        'claimed_at': at,
        'started_at': at,
        'terminal_at': _text(run + timedelta(seconds=1)),
        'status': status,
        'terminalization_kind': 'COMPLETE_FUSED',
        'retry_count': '0',
        'max_retries': '0',
        'last_claimed_worker_id': BATCH_WORKER,
        'history_schema_version': '1',
        'attempt_archive_version': '1',
        'rerun_input_disposition': 'DECLINED_BY_POLICY',
    }


def _closed_batcher(
    run: datetime,
    prefix: str,
    committed: int,
    *,
    history_status: str = 'COMPLETED',
    events_of_a_commit: Sequence[str] = MOVE_EVENTS,
    live_changes: Mapping[int, Row] | None = None,
    extra: Mapping[str, Sequence[Row]] | None = None,
) -> NormalizedSnapshot:
    """The batch test after the close: the first ``committed`` received rows
    are in history with the events of their move; every other row is RUNNING."""
    tasks = [f'{prefix}-task-{position}' for position in range(RECEIVED + UNSENT)]
    live = [
        _batch_task(task, run, position, (live_changes or {}).get(position, {}))
        for position, task in enumerate(tasks)
        if position >= committed
    ]
    history = [
        _committed_row(task, run, position, history_status)
        for position, task in enumerate(tasks[:committed])
    ]
    events = [(channel, task) for task in tasks[:committed] for channel in events_of_a_commit]
    return _snapshot(
        run, {'horsies_tasks': live, HISTORY_TABLE: history, **(extra or {})}, events
    )


def _judgement(
    reference: NormalizedSnapshot, candidate: NormalizedSnapshot, compare: PairCompare
) -> PairJudgement:
    return judge_pair(
        ReferenceCaptured(snapshot=reference, window=_window(RUN_A)),
        candidate,
        _window(RUN_B),
        compare,
    )


def test_without_the_decision_the_commit_of_the_received_rows_differs() -> None:
    lines = _judge(
        _closed_batcher(RUN_A, 'a', 0), _closed_batcher(RUN_B, 'b', RECEIVED), WholeSnapshot()
    )

    assert len(lines) == 4 * RECEIVED, lines
    assert lines[0].startswith('horsies_tasks [id=task#3]: row only in the reference: ')


@pytest.mark.parametrize('committed', [1, RECEIVED])
def test_the_decided_difference_reports_the_commit_of_received_rows_as_decided(
    committed: int,
) -> None:
    reference = _closed_batcher(RUN_A, 'a', 0)
    candidate = _closed_batcher(RUN_B, 'b', committed)

    match _judgement(reference, candidate, CLOSE):
        case DecidedDifferenceSeen(diff=diff, decided=decided):
            assert decided == CLOSE
            assert len(diff.entries) == 4 * committed
        case other:
            raise AssertionError(f'expected the decided difference, got {other}')


def test_the_decided_difference_is_equal_when_no_received_row_committed() -> None:
    assert (
        _judgement(_closed_batcher(RUN_A, 'a', 0), _closed_batcher(RUN_B, 'b', 0), CLOSE)
        == PairEqual()
    )


@pytest.mark.parametrize(
    'candidate',
    [
        pytest.param(
            lambda: _closed_batcher(RUN_B, 'b', RECEIVED, history_status='FAILED'),
            id='committed-as-failed',
        ),
        pytest.param(
            lambda: _closed_batcher(RUN_B, 'b', RECEIVED, events_of_a_commit=('task_done',)),
            id='commit-without-its-status-event',
        ),
        pytest.param(
            lambda: _closed_batcher(
                RUN_B, 'b', RECEIVED, events_of_a_commit=(*MOVE_EVENTS, 'task_new')
            ),
            id='commit-with-another-event',
        ),
        pytest.param(
            lambda: _closed_batcher(
                RUN_B, 'b', RECEIVED, events_of_a_commit=(*MOVE_EVENTS, 'task_done')
            ),
            id='commit-with-two-done-events',
        ),
        pytest.param(
            lambda: _closed_batcher(
                RUN_B,
                'b',
                RECEIVED,
                live_changes={RECEIVED: {'claimed_by_worker_id': 'other-worker'}},
            ),
            id='unsent-row-of-another-worker',
        ),
        pytest.param(
            lambda: _closed_batcher(
                RUN_B,
                'b',
                RECEIVED,
                extra={'horsies_task_attempts': [{'task_id': 'b-task-0', 'attempt': '1'}]},
            ),
            id='row-in-another-table',
        ),
    ],
)
def test_the_decided_difference_fails_a_difference_that_it_does_not_explain(
    candidate: Callable[[], NormalizedSnapshot],
) -> None:
    match _judgement(_closed_batcher(RUN_A, 'a', 0), candidate(), CLOSE):
        case DualRunMismatch(diff=diff):
            assert diff.entries
        case other:
            raise AssertionError(f'expected a mismatch, got {other}')


def test_the_decided_difference_fails_a_received_row_that_left_without_a_commit() -> None:
    """Four rows left the live table, and only three committed."""
    reference = _closed_batcher(RUN_A, 'a', 0)
    tasks = [f'b-task-{position}' for position in range(RECEIVED + UNSENT)]
    candidate = _snapshot(
        RUN_B,
        {
            'horsies_tasks': [
                _batch_task(task, RUN_B, position, {})
                for position, task in enumerate(tasks)
                if position >= RECEIVED
            ],
            HISTORY_TABLE: [
                _committed_row(task, RUN_B, position, 'COMPLETED')
                for position, task in enumerate(tasks[: RECEIVED - 1])
            ],
        },
        [(channel, task) for task in tasks[: RECEIVED - 1] for channel in MOVE_EVENTS],
    )

    match _judgement(reference, candidate, CLOSE):
        case DualRunMismatch():
            pass
        case other:
            raise AssertionError(f'expected a mismatch, got {other}')
