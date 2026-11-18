"""The lifecycle writes of a shared async child.

The pre-start transition and the finalizing marker go through the awaitable
methods of the core handle of the child: each test stands in that handle
with a fake that answers scripted records and keeps each call. The records
of ``horsies._core`` have no Python constructor, so a ``MagicMock`` of the
record class stands in for each; a class pattern matches it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.worker import shared_lifecycle
from horsies.core.worker.run_return import (
    MarkerDone,
    MarkerEnd,
    StopReason,
    StoppedBeforeRunning,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core`` of the task of these
    tests, with ``fields`` as its attributes."""
    record = MagicMock(spec=record_class)
    record.task_id = uuid.UUID(TASK_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


# The most time that a miss path of these tests may report as spent in its
# start attempt: the fakes answer at once.
_MOST_ELAPSED_MS = 5_000


def _attempt_elapsed_ms(arguments: dict[str, object]) -> dict[str, object]:
    """The arguments of a miss path call without its ``attempt_elapsed_ms``,
    after a check that it is the whole milliseconds of a start attempt that
    began right before the call."""
    rest = dict(arguments)
    elapsed = rest.pop('attempt_elapsed_ms')
    assert isinstance(elapsed, int)
    assert 0 <= elapsed <= _MOST_ELAPSED_MS
    return rest


_STARTED = _record(core.AttemptStarted)


def _missed(reason: str) -> MagicMock:
    return _record(core.AttemptMissed, reason=reason)


def _stopped(outcome: str, move_outcome: str | None) -> MagicMock:
    return _record(core.StoppedBeforeStart, outcome=outcome, move_outcome=move_outcome)


def _expired() -> MagicMock:
    return _record(
        core.TaskRowEnded,
        terminal_at=CLAIMED_AT,
        kind='EXPIRE_CLAIMED',
        observed_status='CLAIMED',
        observed_worker_id='worker-1',
        observed_claimed_at=CLAIMED_AT,
    )


class _Handle:
    """A stand-in for ``CoreChildHandle``: each awaitable method answers the
    next record of its queue, and each call is kept with its arguments, in
    order."""

    def __init__(
        self,
        *,
        starts: list[Any],
        stops: list[Any] | None = None,
        expiries: list[Any] | None = None,
        markers: list[Any] | None = None,
    ) -> None:
        self._starts = starts
        self._stops = stops or []
        self._expiries = expiries or []
        self._markers = markers or []
        self.calls: list[tuple[str, uuid.UUID, dict[str, object]]] = []

    async def start_attempt_async(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('start_attempt_async', task_id, kwargs))
        return self._starts.pop(0)

    async def stop_workflow_task_before_start_async(
        self, task_id: uuid.UUID, **kwargs: object,
    ) -> Any:
        self.calls.append(('stop_workflow_task_before_start_async', task_id, kwargs))
        return self._stops.pop(0)

    async def expire_owned_claim_async(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('expire_owned_claim_async', task_id, kwargs))
        return self._expiries.pop(0)

    async def mark_finalizing_async(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('mark_finalizing_async', task_id, kwargs))
        return self._markers.pop(0)

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]


class _HangingHandle(_Handle):
    """A handle whose start and marker never answer: a write that waits on a
    row lock, or a server that does not answer."""

    async def start_attempt_async(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('start_attempt_async', task_id, kwargs))
        await asyncio.Event().wait()

    async def mark_finalizing_async(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append(('mark_finalizing_async', task_id, kwargs))
        await asyncio.Event().wait()


def _confirm(
    handle: Any,
    *,
    orphan_self_heal: bool = True,
) -> Any:
    with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
        return asyncio.run(
            shared_lifecycle.confirm_until_not_resumed(
                TASK_ID,
                'worker-1',
                CLAIMED_AT,
                orphan_self_heal=orphan_self_heal,
            ),
        )


class TestSetRunning:
    def test_a_started_row_runs_the_task_after_one_call(self) -> None:
        handle = _Handle(starts=[_STARTED])

        assert _confirm(handle) is None

        assert handle.calls == [
            (
                'start_attempt_async',
                uuid.UUID(TASK_ID),
                {
                    'worker_id': 'worker-1',
                    'claimed_at': CLAIMED_AT,
                    'worker_pid': os.getpid(),
                    'worker_hostname': socket.gethostname(),
                    'deadline_in_ms': None,
                },
            ),
        ]

    def test_a_workflow_task_starts_with_the_same_call(self) -> None:
        """The function reads is_workflow_task from the locked row and
        writes the node in the same statement."""
        handle = _Handle(starts=[_STARTED])

        assert _confirm(handle) is None

        assert handle.names() == ['start_attempt_async']

    def test_a_failed_start_is_ownership_unconfirmed(self) -> None:
        handle = _Handle(
            starts=[
                _record(
                    core.StartFailed,
                    message='horsies_start_attempt returned 0 rows',
                    retryable=False,
                ),
            ],
        )

        assert _confirm(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')

    def test_a_core_that_cannot_run_the_call_is_ownership_unconfirmed(self) -> None:
        handle = MagicMock()
        handle.start_attempt_async.side_effect = core.TooManyPendingCalls('the queue is full')

        assert _confirm(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')

    def test_a_record_outside_the_catalog_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            _confirm(_Handle(starts=[object()]))

    def test_a_cancel_during_the_transition_propagates(self) -> None:
        """The deadline of the seat must end the run, not become a result."""

        async def cancel_it() -> None:
            run = asyncio.ensure_future(
                shared_lifecycle.confirm_until_not_resumed(
                    TASK_ID,
                    'worker-1',
                    CLAIMED_AT,
                    orphan_self_heal=True,
                ),
            )
            await asyncio.sleep(0.01)
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

        with patch.object(
            shared_lifecycle,
            '_get_child_core_handle',
            return_value=_HangingHandle(starts=[]),
        ):
            asyncio.run(cancel_it())


class TestMissedStart:
    def test_a_passed_deadline_expires_the_claim(self) -> None:
        handle = _Handle(starts=[_missed('DEADLINE_PASSED')], expiries=[_expired()])

        result = _confirm(handle)

        assert result == StoppedBeforeRunning('TASK_EXPIRED')
        name, task_id, arguments = handle.calls[1]
        assert (name, task_id, _attempt_elapsed_ms(arguments)) == (
            'expire_owned_claim_async',
            uuid.UUID(TASK_ID),
            {'worker_id': 'worker-1'},
        )

    def test_a_refused_expiry_is_a_lost_claim(self) -> None:
        handle = _Handle(
            starts=[_missed('DEADLINE_PASSED')],
            expiries=[
                _record(
                    core.AttemptLostClaim,
                    observed_status='CLAIMED',
                    observed_worker_id='worker-2',
                    observed_claimed_at=CLAIMED_AT,
                ),
            ],
        )

        assert _confirm(handle) == StoppedBeforeRunning('CLAIM_LOST')

    @pytest.mark.parametrize('reason', ['NOT_HELD', 'LEASE_EXPIRED'])
    def test_a_claim_that_is_not_held_or_expired_is_lost_with_no_write(
        self, reason: str,
    ) -> None:
        handle = _Handle(starts=[_missed(reason)])

        assert _confirm(handle) == StoppedBeforeRunning('CLAIM_LOST')
        assert handle.names() == ['start_attempt_async']

    def test_a_paused_workflow_stops_the_task_in_one_call(self) -> None:
        """The stop is one statement after the start, whose row lock ended
        with its statement."""
        handle = _Handle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[_stopped('ABANDONED', 'APPLIED')],
        )

        result = _confirm(handle)

        assert result == StoppedBeforeRunning('WORKFLOW_STOPPED')
        assert handle.names() == [
            'start_attempt_async',
            'stop_workflow_task_before_start_async',
        ]

    def test_a_cancelled_workflow_stops_the_task_under_the_claim_fence(self) -> None:
        handle = _Handle(
            starts=[_missed('WORKFLOW_CANCELLED')],
            stops=[_stopped('CANCELLED', 'APPLIED')],
        )

        result = _confirm(handle)

        assert result == StoppedBeforeRunning('WORKFLOW_STOPPED')
        assert _attempt_elapsed_ms(handle.calls[1][2]) == {
            'worker_id': 'worker-1',
            'claimed_at': CLAIMED_AT,
            'orphan_self_heal': True,
        }

    @pytest.mark.parametrize(
        ('orphan_self_heal', 'outcome', 'move_outcome', 'reason'),
        [
            (False, 'NODE_NOT_LINKED', None, 'NODE_NOT_LINKED'),
            (True, 'ORPHAN_CANCELLED', 'APPLIED', 'ORPHAN_CANCELLED'),
        ],
    )
    def test_a_node_that_is_not_linked_follows_the_orphan_self_heal(
        self,
        orphan_self_heal: bool,
        outcome: str,
        move_outcome: str | None,
        reason: StopReason,
    ) -> None:
        handle = _Handle(
            starts=[_missed('NODE_NOT_LINKED')],
            stops=[_stopped(outcome, move_outcome)],
        )

        result = _confirm(handle, orphan_self_heal=orphan_self_heal)

        assert result == StoppedBeforeRunning(reason)
        assert handle.calls[1][2]['orphan_self_heal'] is orphan_self_heal

    def test_a_resume_before_the_stop_starts_the_task_on_the_next_attempt(
        self,
    ) -> None:
        handle = _Handle(
            starts=[_missed('WORKFLOW_PAUSED'), _STARTED],
            stops=[_record(core.ResumedBeforeStart)],
        )

        result = _confirm(handle)

        assert result is None
        assert handle.names() == [
            'start_attempt_async',
            'stop_workflow_task_before_start_async',
            'start_attempt_async',
        ]


    def test_a_failed_stop_is_ownership_unconfirmed(self) -> None:
        handle = _Handle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[
                _record(core.StopBeforeStartFailed, message='connection lost', retryable=True),
            ],
        )

        assert _confirm(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


def _mark(handle: Any) -> MarkerEnd:
    with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
        return asyncio.run(
            shared_lifecycle.mark_task_finalizing(
                uuid.UUID(TASK_ID),
                'worker-1',
                CLAIMED_AT,
            ),
        )


class TestFinalizingMarker:

    def test_a_marker_that_finds_another_claim_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG, logger='horsies.worker')

        assert _mark(_Handle(starts=[], markers=[_record(core.FinalizingNotHeld)])) == (
            MarkerDone()
        )

        assert (
            f'Finalizing marker for task {TASK_ID} skipped: the claim changed'
            in caplog.text
        )
        assert f'Failed to mark task {TASK_ID} finalizing' not in caplog.text

    def test_a_marker_record_outside_the_catalog_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            _mark(_Handle(starts=[], markers=[object()]))


    def test_a_core_that_cannot_run_the_marker_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        handle = MagicMock()
        handle.mark_finalizing_async.side_effect = core.TooManyPendingCalls(
            'the queue is full',
        )

        assert _mark(handle) == MarkerDone()

        assert f'Failed to mark task {TASK_ID} finalizing' in caplog.text
