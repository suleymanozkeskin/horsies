"""The Python mirror of the attempt archive writes the bytes of the SQL encoder.

The pinned text of version 2 is the one that the Rust codec pins
(``core/src/core/history/archive/attempts.rs``,
``version_two_encodes_the_pinned_bytes``), so the mirror, the codec and
``horsies_encode_attempt_archive`` agree on one form.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import pytest

from tests.attempt_snapshot_support import (
    AttemptEnded,
    AttemptRecord,
    ClaimsCounted,
    ClaimsNotCounted,
    FinalizingMarked,
    FinalizingNotMarked,
    NotClaimed,
    OpenAttempt,
    PhaseRecord,
    PhasesCaptured,
    PhasesNotCaptured,
    Placed,
    PlacementNotObserved,
    RecordedByRecovery,
    RecordedByWorker,
    encode_attempt_archive,
    encode_attempt_snapshot,
)

pytestmark = [pytest.mark.unit]

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _at(micros: int) -> datetime:
    return _EPOCH + timedelta(microseconds=micros)


def _attempt(number: int, phases: PhaseRecord) -> AttemptRecord:
    return AttemptRecord(
        attempt=number,
        outcome='FAILED',
        will_retry=True,
        started_at=_at(1_000),
        finished_at=_at(2_000),
        error_code='E',
        error_message=None,
        failed_reason=None,
        worker_id='w1',
        worker_hostname='host',
        worker_pid=7,
        worker_process_name='proc',
        phases=phases,
    )


def _row(number: int) -> str:
    return f'{number},"FAILED",true,1000,2000,"E",null,null,"w1","host",7,"proc"'


def test_version_two_writes_the_pinned_bytes() -> None:
    attempts = (
        _attempt(
            1,
            PhasesCaptured(
                enqueued_at=_at(100),
                claimed_at=_at(300),
                claims=ClaimsCounted(count=2, first_claimed_at=_at(200)),
                finalizing=FinalizingMarked(at=_at(1_500)),
                recorded_by=RecordedByWorker(placement=Placed(at=_at(350))),
            ),
        ),
        _attempt(
            2,
            PhasesCaptured(
                enqueued_at=_at(2_100),
                claimed_at=_at(2_200),
                claims=ClaimsNotCounted(),
                finalizing=FinalizingNotMarked(),
                recorded_by=RecordedByRecovery(last_seen_at=_at(2_400)),
            ),
        ),
        _attempt(3, PhasesNotCaptured()),
    )
    open_attempt = OpenAttempt(
        claims=ClaimsCounted(count=1, first_claimed_at=_at(3_000)),
        finalizing=FinalizingNotMarked(),
    )

    stored = encode_attempt_archive(attempts, open_attempt)

    expected = (
        f'[[[{_row(1)},1,100,300,2,200,350,1500,"WORKER",null],'
        f'[{_row(2)},1,2100,2200,null,null,null,null,"RECOVERY",2400],'
        f'[{_row(3)},null,null,null,null,null,null,null,null,null]],'
        '[1,3000,null]]'
    )
    assert stored.payload.decode('utf-8') == expected
    assert stored.version == 2
    assert stored.digest == hashlib.sha256(stored.payload).digest()


@pytest.mark.parametrize(
    ('open_attempt', 'expected'),
    [
        (AttemptEnded(), 'null'),
        (OpenAttempt(claims=ClaimsNotCounted(), finalizing=FinalizingNotMarked()), '[null,null,null]'),
        (OpenAttempt(claims=NotClaimed(), finalizing=FinalizingNotMarked()), '[0,null,null]'),
        (
            OpenAttempt(
                claims=ClaimsCounted(count=3, first_claimed_at=_at(10)),
                finalizing=FinalizingMarked(at=_at(20)),
            ),
            '[3,10,20]',
        ),
    ],
    ids=['ended', 'not_counted', 'not_claimed', 'counted_and_marked'],
)
def test_each_open_attempt_writes_its_record(
    open_attempt: AttemptEnded | OpenAttempt,
    expected: str,
) -> None:
    stored = encode_attempt_archive((), open_attempt)
    assert stored.payload.decode('utf-8') == f'[[],{expected}]'


def test_a_worker_with_no_placement_writes_no_placement() -> None:
    phases = PhasesCaptured(
        enqueued_at=_at(1),
        claimed_at=_at(2),
        claims=ClaimsCounted(count=1, first_claimed_at=_at(2)),
        finalizing=FinalizingNotMarked(),
        recorded_by=RecordedByWorker(placement=PlacementNotObserved()),
    )
    stored = encode_attempt_archive((_attempt(1, phases),), AttemptEnded())
    assert stored.payload.decode('utf-8') == (
        f'[[[{_row(1)},1,1,2,1,2,null,null,"WORKER",null]],null]'
    )


def test_version_one_refuses_a_phase_record() -> None:
    phases = PhasesCaptured(
        enqueued_at=_at(1),
        claimed_at=_at(2),
        claims=ClaimsNotCounted(),
        finalizing=FinalizingNotMarked(),
        recorded_by=RecordedByWorker(placement=PlacementNotObserved()),
    )
    with pytest.raises(ValueError, match='version 1 stores no phase record'):
        encode_attempt_snapshot((_attempt(1, phases),))


def test_a_counted_attempt_has_a_claim() -> None:
    with pytest.raises(ValueError, match='1 claim or more'):
        ClaimsCounted(count=0, first_claimed_at=_at(1))
