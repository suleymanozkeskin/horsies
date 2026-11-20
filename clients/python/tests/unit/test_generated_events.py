"""The event classes and the decoder generated from horsies-core.

`horsies-codegen` renders the event contract of the core
(`horsies_core::events`) into `horsies/_catalog/events.py`. The shared wire
vectors (`core/tests/fixtures/event_wire/vectors.json`) hold the core's
encoding of one sample or more per event type; the core's own test pins
each vector to its encoder. This test decodes each vector into the event
that the core encoded, and pins one refusal per kind of malformed input.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Final, cast

import pytest

from horsies._catalog import events
from horsies._catalog.task_codes import OperationalErrorCode, OutcomeCode
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]

_REPO_ROOT: Final = Path(__file__).resolve().parents[4]
_VECTORS: Final = _REPO_ROOT / 'core' / 'tests' / 'fixtures' / 'event_wire' / 'vectors.json'

AT: Final = datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=timezone.utc)
"""The vectors' time, 1700000000123456789 ns, to the microsecond."""

STARTED_AT: Final = datetime(2023, 11, 14, 22, 13, 20, 100000, tzinfo=timezone.utc)
PLACED_AT: Final = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
NEXT_RETRY_AT: Final = datetime(2023, 11, 14, 22, 14, 20, tzinfo=timezone.utc)
TASK_ID: Final = uuid.UUID('0192f6a1-7b3c-7d4e-8f00-0123456789ab')
CARRIER: Final = '{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}'
WORKER_ID: Final = '7c9e6679-7425-40de-944b-e07fc1f90ae7'


def _ended(
    execution: events.Execution,
    attempt: int,
    end: events.AttemptEnd,
    run_duration: timedelta,
) -> events.AttemptEnded:
    return events.AttemptEnded(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        execution=execution,
        attempt=attempt,
        end=end,
        run_duration=run_duration,
    )


def _recorded(
    attempt: int,
    record: events.AttemptRecord,
    record_duration: timedelta,
) -> events.AttemptRecorded:
    return events.AttemptRecorded(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        attempt=attempt,
        record=record,
        record_duration=record_duration,
    )


EXPECTED: Final[dict[str, events.AnyEvent]] = {
    'task_claimed': events.TaskClaimed(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        execution='SHARED_ASYNC',
        attempt=2,
        priority=50,
        queue_wait=timedelta(microseconds=1500),
    ),
    'claim_released': events.ClaimReleased(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        reason='QUEUE_FULL',
    ),
    'attempt_placed': events.AttemptPlaced(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        execution='SHARED_ASYNC',
        attempt=1,
        child_kind='ISOLATED',
        child_pid=4242,
        slot_wait=timedelta(microseconds=250),
    ),
    'attempt_not_started': events.AttemptNotStarted(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        attempt=1,
        reason='WORKFLOW_STOPPED',
    ),
    'attempt_ended_completed': _ended(
        'SHARED_THREAD',
        1,
        events.AttemptEndReportedByChild(
            result=events.ChildResultCompleted(),
            start=events.AttemptStartReported(started_at=STARTED_AT),
        ),
        timedelta(seconds=2),
    ),
    'attempt_ended_user_code': _ended(
        'SHARED_ASYNC',
        2,
        events.AttemptEndReportedByChild(
            result=events.ChildResultFailed(
                error_code=events.AttemptErrorCodeUser(code='RATE_LIMITED'),
            ),
            start=events.AttemptStartReported(started_at=STARTED_AT),
        ),
        timedelta(seconds=2),
    ),
    'attempt_ended_built_in_code': _ended(
        'ISOLATED',
        1,
        events.AttemptEndReportedByChild(
            result=events.ChildResultFailed(
                error_code=events.AttemptErrorCodeBuiltIn(
                    code=OperationalErrorCode.TASK_EXCEPTION,
                ),
            ),
            start=events.AttemptStartNotReported(),
        ),
        timedelta(microseconds=5000),
    ),
    'attempt_ended_worker_failure': _ended(
        'ISOLATED',
        1,
        events.AttemptEndReportedByChild(
            result=events.ChildResultWorkerFailure(),
            start=events.AttemptStartNotReported(),
        ),
        timedelta(microseconds=5000),
    ),
    'attempt_ended_timed_out_in_child': _ended(
        'SHARED_ASYNC',
        3,
        events.AttemptEndReportedByChild(
            result=events.ChildResultTimedOut(),
            start=events.AttemptStartReported(started_at=STARTED_AT),
        ),
        timedelta(seconds=30),
    ),
    'attempt_ended_by_worker': _ended(
        'SHARED_ASYNC',
        3,
        events.AttemptEndDecidedByWorker(
            reason='TIMED_OUT',
            error_code=OutcomeCode.TASK_TIMEOUT,
            trace=events.RunTraceCarrier(text=CARRIER),
            placed_at=PLACED_AT,
            child_kind='SHARED_ASYNC',
            worker_id=WORKER_ID,
            workflow='NOT_IN_WORKFLOW',
        ),
        timedelta(seconds=30),
    ),
    'attempt_ended_by_worker_without_carrier': _ended(
        'SHARED_ASYNC',
        1,
        events.AttemptEndDecidedByWorker(
            reason='NEIGHBOUR_BLOCKED_LOOP',
            error_code=OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP,
            trace=events.RunTraceNoCarrier(),
            placed_at=PLACED_AT,
            child_kind='SHARED_ASYNC',
            worker_id=WORKER_ID,
            workflow='NOT_IN_WORKFLOW',
        ),
        timedelta(seconds=30),
    ),
    'attempt_ended_by_worker_in_workflow': _ended(
        'SHARED_THREAD',
        2,
        events.AttemptEndDecidedByWorker(
            reason='CHILD_LOST',
            error_code=OperationalErrorCode.WORKER_CRASHED,
            trace=events.RunTraceCarrier(text=CARRIER),
            placed_at=PLACED_AT,
            child_kind='ISOLATED',
            worker_id=WORKER_ID,
            workflow='WORKFLOW_NOT_KNOWN',
        ),
        timedelta(seconds=30),
    ),
    'attempt_ended_not_known': _ended(
        'ISOLATED',
        1,
        events.AttemptEndNotKnown(child_pid=4242),
        timedelta(seconds=60),
    ),
    'attempt_recorded_retry': _recorded(
        2,
        events.AttemptRecordRetryScheduled(retry_count=2, next_retry_at=NEXT_RETRY_AT),
        timedelta(microseconds=3000),
    ),
    'attempt_recorded_failed': _recorded(
        3,
        events.AttemptRecordFailed(),
        timedelta(microseconds=1000),
    ),
    'task_recovered': events.TaskRecovered(
        at=AT,
        task_id=TASK_ID,
        task_name='send_email',
        queue='default',
        action='RUNNING_FAILED',
    ),
    'child_started': events.ChildStarted(
        at=AT,
        child_kind='SHARED_THREAD',
        slot=3,
        child_pid=4243,
        boot_duration=timedelta(microseconds=850000),
    ),
    'child_replaced': events.ChildReplaced(
        at=AT,
        child_kind='ISOLATED',
        slot=0,
        child_pid=4242,
        reason='MEMORY_LIMIT',
    ),
    'slot_down': events.SlotDown(
        at=AT,
        child_kind='SHARED_ASYNC',
        slot=1,
        cause=events.SlotDownCauseResourceShortage(errno='EMFILE'),
    ),
    'slot_recovered': events.SlotRecovered(
        at=AT,
        child_kind='SHARED_ASYNC',
        slot=1,
    ),
}
"""The event of each vector, by the vector's name."""


def _vector_file() -> dict[str, object]:
    loaded: dict[str, object] = json.loads(_VECTORS.read_text(encoding='utf-8'))
    return loaded


def _vectors() -> dict[str, object]:
    """The wire value of each vector, by the vector's name."""
    vectors = _vector_file()['vectors']
    assert isinstance(vectors, list)
    wires: dict[str, object] = {}
    for vector in cast('list[object]', vectors):
        assert isinstance(vector, dict)
        entry = cast('dict[str, object]', vector)
        name = entry['name']
        assert isinstance(name, str)
        wires[name] = entry['wire']
    return wires


VECTORS: Final = _vectors()


def _sample(name: str) -> list[object]:
    """A deep copy of the wire items of the vector `name`."""
    wire: object = json.loads(json.dumps(VECTORS[name]))
    assert isinstance(wire, list)
    return cast('list[object]', wire)


@pytest.mark.parametrize('name', sorted(EXPECTED))
def test_each_vector_decodes_to_the_event_that_the_core_encoded(name: str) -> None:
    assert events.decode_event(VECTORS[name]) == Ok(EXPECTED[name])


def test_the_vectors_cover_every_event_type_of_the_contract_version() -> None:
    assert set(VECTORS) == set(EXPECTED)
    assert {type(event) for event in EXPECTED.values()} == set(events.EVENT_TYPES)
    assert _vector_file()['contract_version'] == events.EVENT_CONTRACT_VERSION


def test_an_internal_field_is_left_out_of_repr() -> None:
    ended = EXPECTED['attempt_ended_by_worker']
    assert isinstance(ended, events.AttemptEnded)
    assert isinstance(ended.end, events.AttemptEndDecidedByWorker)
    text = repr(ended.end)
    assert 'reason=' in text
    for internal in ('error_code', 'trace', 'placed_at', 'child_kind', 'worker_id', 'workflow'):
        assert f'{internal}=' not in text


def test_an_event_is_frozen() -> None:
    claimed = EXPECTED['task_claimed']
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(claimed, 'attempt', 3)


@pytest.mark.parametrize(
    'wire',
    [{'tag': 'TaskClaimed'}, [], [1, 2], 'TaskClaimed', ('TaskClaimed',)],
)
def test_a_value_that_is_not_an_array_with_a_tag_is_not_an_event(wire: object) -> None:
    assert events.decode_event(wire) == Err(events.NotAnEvent())


def test_an_unknown_tag_is_named_and_cut() -> None:
    assert events.decode_event(['TaskStarted', 1]) == Err(
        events.UnknownEventType(tag='TaskStarted'),
    )
    long = 'X' * 80
    assert events.decode_event([long]) == Err(events.UnknownEventType(tag=long[:64]))


def _refusal(wire: object) -> events.MalformedEvent:
    decoded = events.decode_event(wire)
    match decoded:
        case Err(events.MalformedEvent() as refusal):
            return refusal
        case _:
            raise AssertionError(f'{wire!r} is not refused as malformed: {decoded!r}')


def test_a_missing_or_extra_item_is_an_item_count_refusal() -> None:
    short = _sample('task_claimed')[:-1]
    assert _refusal(short) == events.MalformedEvent(
        event='TaskClaimed',
        path='',
        reason='expected 8 items, found 7',
    )
    long = [*_sample('task_claimed'), 1]
    assert _refusal(long).reason == 'expected 8 items, found 9'


# The item positions of the `task_claimed` vector.
_ATTEMPT: Final = 6
_TASK_ID: Final = 2
_TASK_NAME: Final = 3
_EXECUTION: Final = 5
_PRIORITY: Final = 7
_QUEUE_WAIT: Final = 8
_AT: Final = 1


def _claimed_with(position: int, value: object) -> list[object]:
    wire = _sample('task_claimed')
    wire[position] = value
    return wire


@pytest.mark.parametrize(
    ('position', 'value', 'path', 'reason'),
    [
        (_ATTEMPT, '2', 'attempt', 'expected a whole JSON number'),
        (_ATTEMPT, True, 'attempt', 'expected a whole JSON number'),
        (_QUEUE_WAIT, 1.5, 'queue_wait', 'expected a whole JSON number'),
        (_TASK_NAME, 7, 'task_name', 'expected a JSON string'),
        (_EXECUTION, None, 'execution', 'expected a JSON string'),
    ],
)
def test_a_value_of_another_json_kind_is_refused_with_its_path(
    position: int,
    value: object,
    path: str,
    reason: str,
) -> None:
    assert _refusal(_claimed_with(position, value)) == events.MalformedEvent(
        event='TaskClaimed',
        path=path,
        reason=reason,
    )


@pytest.mark.parametrize(
    ('position', 'value', 'path', 'reason'),
    [
        (_ATTEMPT, 0, 'attempt', 'the number is outside 1..=2147483648'),
        (_PRIORITY, 101, 'priority', 'the number is outside 1..=100'),
        (_QUEUE_WAIT, -1, 'queue_wait', f'the number is outside 0..={2**64 - 1}'),
        (_AT, 2**63, 'at', f'the number is outside {-(2**63)}..={2**63 - 1}'),
    ],
)
def test_a_number_outside_its_bound_is_refused(
    position: int,
    value: object,
    path: str,
    reason: str,
) -> None:
    refusal = _refusal(_claimed_with(position, value))
    assert (refusal.path, refusal.reason) == (path, reason)


def test_a_text_over_its_bound_is_refused() -> None:
    refusal = _refusal(_claimed_with(_TASK_NAME, 'é' * 256))
    assert (refusal.path, refusal.reason) == (
        'task_name',
        'the text has 256 characters; the bound is 255',
    )


@pytest.mark.parametrize(
    'text',
    [
        '0192F6A1-7B3C-7D4E-8F00-0123456789AB',
        '{0192f6a1-7b3c-7d4e-8f00-0123456789ab}',
        '0192f6a17b3c7d4e8f000123456789ab',
        'not a uuid',
    ],
)
def test_a_uuid_in_another_form_is_refused(text: str) -> None:
    refusal = _refusal(_claimed_with(_TASK_ID, text))
    assert (refusal.path, refusal.reason) == (
        'task_id',
        'the text is not a UUID in hyphenated lower-case form',
    )


def _ended_with_end(end: object) -> list[object]:
    wire = _sample('attempt_ended_not_known')
    wire[7] = end
    return wire


def test_a_name_outside_a_closed_type_is_refused_with_the_type() -> None:
    refusal = _refusal(_claimed_with(_EXECUTION, 'SHARED'))
    assert (refusal.path, refusal.reason) == ('execution', "Execution has no value 'SHARED'")
    refusal = _refusal(_ended_with_end(['EndedSomehow']))
    assert (refusal.event, refusal.path, refusal.reason) == (
        'AttemptEnded',
        'end',
        "AttemptEnd has no value 'EndedSomehow'",
    )
    unknown_code = ['ReportedByChild', ['Failed', ['BuiltIn', 'NOT_A_CODE']], ['NotReported']]
    refusal = _refusal(_ended_with_end(unknown_code))
    assert (refusal.path, refusal.reason) == (
        'end.ReportedByChild.result.Failed.error_code.BuiltIn.code',
        "BuiltInTaskCode has no value 'NOT_A_CODE'",
    )


def test_a_variant_value_without_its_tag_or_items_is_refused() -> None:
    refusal = _refusal(_ended_with_end('NotKnown'))
    assert (refusal.path, refusal.reason) == ('end', 'expected a JSON array')
    for end in ([], [42]):
        refusal = _refusal(_ended_with_end(end))
        assert (refusal.path, refusal.reason) == (
            'end',
            'a value of AttemptEnd is a JSON array whose first item is its variant name',
        )
    refusal = _refusal(_ended_with_end(['NotKnown']))
    assert (refusal.path, refusal.reason) == ('end.NotKnown', 'expected 1 items, found 0')


def test_a_timeout_that_the_child_reported_decodes_as_a_child_result() -> None:
    ended = EXPECTED['attempt_ended_timed_out_in_child']
    assert isinstance(ended, events.AttemptEnded)
    match ended.end:
        case events.AttemptEndReportedByChild(
            result=events.ChildResultTimedOut(),
            start=events.AttemptStartReported(started_at=started_at),
        ):
            assert started_at == STARTED_AT
        case other:
            raise AssertionError(f'a timeout that the child reported, not {other!r}')
    wire = _sample('attempt_ended_timed_out_in_child')
    end = wire[7]
    assert isinstance(end, list)
    cast('list[object]', end)[1] = ['TimedOut', 'extra']
    refusal = _refusal(wire)
    assert (refusal.path, refusal.reason) == (
        'end.ReportedByChild.result.TimedOut',
        'expected 0 items, found 1',
    )


def test_a_worker_ended_attempt_names_its_workflow_place_or_is_refused() -> None:
    ended = EXPECTED['attempt_ended_by_worker_in_workflow']
    assert isinstance(ended, events.AttemptEnded)
    assert isinstance(ended.end, events.AttemptEndDecidedByWorker)
    assert ended.end.workflow == 'WORKFLOW_NOT_KNOWN'
    wire = _sample('attempt_ended_by_worker_in_workflow')
    end = wire[7]
    assert isinstance(end, list)
    cast('list[object]', end)[7] = 'IN_WORKFLOW'
    refusal = _refusal(wire)
    assert (refusal.path, refusal.reason) == (
        'end.DecidedByWorker.workflow',
        "WorkflowPlace has no value 'IN_WORKFLOW'",
    )
