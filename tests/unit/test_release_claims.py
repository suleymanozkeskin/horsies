"""``horsies.core.worker.release``: the call of ``horsies_release_claims`` and its decoder."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.worker.release import (
    MAX_RELEASE_ROWS,
    RELEASE_CLAIMS_SQL,
    ClaimRelease,
    HeldClaim,
    ReleaseOutcome,
    ReleasePlacement,
    decode_release_rows,
    release_claims,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 16, 9, 0, tzinfo=timezone.utc)
TASK_A = '019a8f3e-0000-7000-8000-00000000000a'
TASK_B = '019a8f3e-0000-7000-8000-00000000000b'


def _claim(task_id: str, minutes: int = 0) -> HeldClaim:
    return HeldClaim(task_id=task_id, claimed_at=CLAIMED_AT + timedelta(minutes=minutes))


def _row(position: int, task_id: str, outcome: str) -> SimpleNamespace:
    return SimpleNamespace(ordinality=position, task_id=task_id, outcome=outcome)


class TestDecodeReleaseRows:
    def test_each_row_pairs_with_the_claim_at_its_position(self) -> None:
        claims = (_claim(TASK_A), _claim(TASK_B, 1))

        decoded = decode_release_rows(
            claims,
            [_row(1, TASK_A, 'RELEASED'), _row(2, TASK_B, 'NOT_HELD')],
        )

        assert decoded == (
            ClaimRelease(claim=claims[0], outcome=ReleaseOutcome.RELEASED),
            ClaimRelease(claim=claims[1], outcome=ReleaseOutcome.NOT_HELD),
        )

    def test_a_row_count_other_than_the_claim_count_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 1 rows for 2 claims'):
            decode_release_rows(
                (_claim(TASK_A), _claim(TASK_B)),
                [_row(1, TASK_A, 'RELEASED')],
            )

    def test_a_row_of_another_task_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match=f'expected row 1 for task {TASK_A}'):
            decode_release_rows((_claim(TASK_A),), [_row(1, TASK_B, 'RELEASED')])

    def test_a_row_at_another_position_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned row 2'):
            decode_release_rows((_claim(TASK_A),), [_row(2, TASK_A, 'RELEASED')])

    def test_an_unknown_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="outcome 'APPLIED'"):
            decode_release_rows((_claim(TASK_A),), [_row(1, TASK_A, 'APPLIED')])


def _connection() -> MagicMock:
    """A connection whose release call returns one RELEASED row per claim it was given."""

    async def _execute(statement: Any, params: dict[str, Any]) -> MagicMock:
        assert statement is RELEASE_CLAIMS_SQL
        rows = [
            _row(position, task_id, 'RELEASED')
            for position, task_id in enumerate(params['task_ids'], start=1)
        ]
        return MagicMock(fetchall=MagicMock(return_value=rows))

    connection = MagicMock()
    connection.execute = AsyncMock(side_effect=_execute)
    return connection


class TestReleaseClaims:
    @pytest.mark.asyncio
    async def test_the_call_carries_each_generation_the_worker_and_the_placement(self) -> None:
        connection = _connection()
        claims = (_claim(TASK_A), _claim(TASK_B, 1))

        released = await release_claims(
            connection,
            worker_id='worker-1',
            claims=claims,
            placement=ReleasePlacement.ISOLATED,
        )

        assert [each.outcome for each in released] == [ReleaseOutcome.RELEASED] * 2
        connection.execute.assert_awaited_once_with(
            RELEASE_CLAIMS_SQL,
            {
                'task_ids': [TASK_A, TASK_B],
                'claimed_ats': [claims[0].claimed_at, claims[1].claimed_at],
                'worker_id': 'worker-1',
                'placement': 'ISOLATED',
            },
        )

    @pytest.mark.asyncio
    async def test_no_claims_send_nothing(self) -> None:
        connection = _connection()

        released = await release_claims(
            connection,
            worker_id='worker-1',
            claims=(),
            placement=ReleasePlacement.SAME_CLASS,
        )

        assert released == ()
        connection.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_list_longer_than_one_call_is_split_in_order(self) -> None:
        connection = _connection()
        claims = tuple(
            _claim(f'019a8f3e-0000-7000-8000-{index:012x}')
            for index in range(MAX_RELEASE_ROWS + 1)
        )

        released = await release_claims(
            connection,
            worker_id='worker-1',
            claims=claims,
            placement=ReleasePlacement.SAME_CLASS,
        )

        sizes = [len(call.args[1]['task_ids']) for call in connection.execute.await_args_list]
        assert sizes == [MAX_RELEASE_ROWS, 1]
        assert tuple(each.claim for each in released) == claims
