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
from horsies.core.worker import shared_lifecycle, slots, thread_runner
from horsies.core.worker.run_return import StopReason, StoppedBeforeRunning
from horsies.core.worker.child_runner import _PRE_START_CONFIRM_ATTEMPTS

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'
# A test of a bound fails, not hangs, when the bound is gone.
OUTER_BOUND_S = 5.0


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

    def test_a_transition_that_hangs_ends_at_its_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A start that waits on a row lock, or a server that does not
        answer, ends the wait of each attempt at its bound with
        OWNERSHIP_UNCONFIRMED: the run stops before user code, and the
        refusal backstop of the parent never fires for it."""
        monkeypatch.setattr(shared_lifecycle, 'RUNNING_ATTEMPT_TIMEOUT_S', 0.05)
        handle = _HangingHandle(starts=[])

        async def run() -> tuple[Any, float]:
            loop = asyncio.get_running_loop()
            started = loop.time()
            result = await asyncio.wait_for(
                shared_lifecycle.confirm_until_not_resumed(
                    TASK_ID,
                    'worker-1',
                    CLAIMED_AT,
                    orphan_self_heal=True,
                ),
                OUTER_BOUND_S,
            )
            return result, loop.time() - started

        with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
            result, elapsed_s = asyncio.run(run())

        assert result == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')
        assert elapsed_s < 1.0
        assert 'did not finish within' in caplog.text


class TestRefusalBound:
    """The refusal backstop of the parent follows from the bounds of the
    writes that a refusal makes in the child."""

    def test_the_writes_of_a_refusal_have_a_named_bound(self) -> None:
        attempt_s = (
            shared_lifecycle.RUNNING_ATTEMPT_TIMEOUT_S
            + shared_lifecycle.CANCELLED_WRITE_END_S
        )
        marker_s = (
            shared_lifecycle.FINALIZING_MARKER_TIMEOUT_S
            + shared_lifecycle.CANCELLED_WRITE_END_S
        )

        assert shared_lifecycle.REFUSAL_WRITES_BOUND_S == (
            _PRE_START_CONFIRM_ATTEMPTS * attempt_s + marker_s
        )
        assert shared_lifecycle.RUNNING_ATTEMPT_TIMEOUT_S == (
            shared_lifecycle.CHILD_WRITE_CHECKOUT_WAIT_S
            + shared_lifecycle.RUNNING_STATEMENTS_TIMEOUT_S
        )

    def test_the_writes_of_a_thread_refusal_have_the_same_bound(self) -> None:
        """A shared thread child makes the same writes through the same core
        handle: each start attempt and the marker have the bounds of the
        core, with the end of a statement that the core cancelled."""
        assert thread_runner.THREAD_REFUSAL_WRITES_BOUND_S == (
            shared_lifecycle.REFUSAL_WRITES_BOUND_S
        )

    def test_the_refusal_backstop_comes_after_the_writes_and_a_block_report(
        self,
    ) -> None:
        """A refusal whose writes end in time never meets the backstop, in
        either kind of shared child, and a blocked loop is reported first,
        with its culprit."""
        assert slots.SHARED_REFUSAL_BACKSTOP_S == (
            max(
                shared_lifecycle.REFUSAL_WRITES_BOUND_S,
                thread_runner.THREAD_REFUSAL_WRITES_BOUND_S,
            )
            + slots.SHARED_BACKSTOP_MARGIN_S
        )


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

    def test_a_resume_on_every_attempt_leaves_the_claim_to_its_lease(self) -> None:
        resumed = _record(core.ResumedBeforeStart)
        handle = _Handle(
            starts=[_missed('WORKFLOW_PAUSED'), _missed('WORKFLOW_PAUSED')],
            stops=[resumed, resumed],
        )

        result = _confirm(handle)

        assert result == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')

    def test_a_failed_stop_is_ownership_unconfirmed(self) -> None:
        handle = _Handle(
            starts=[_missed('WORKFLOW_PAUSED')],
            stops=[
                _record(core.StopBeforeStartFailed, message='connection lost', retryable=True),
            ],
        )

        assert _confirm(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


def _mark(handle: Any) -> None:
    with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
        asyncio.run(
            shared_lifecycle.mark_task_finalizing(
                uuid.UUID(TASK_ID),
                'worker-1',
                CLAIMED_AT,
            ),
        )


class TestFinalizingMarker:
    def test_the_marker_is_fenced_on_the_claim_generation(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG, logger='horsies.worker')
        handle = _Handle(starts=[], markers=[_record(core.FinalizingMarked)])

        _mark(handle)

        assert handle.calls == [
            (
                'mark_finalizing_async',
                uuid.UUID(TASK_ID),
                {'worker_id': 'worker-1', 'claimed_at': CLAIMED_AT},
            ),
        ]
        assert 'finalizing' not in caplog.text

    def test_a_marker_that_finds_another_claim_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG, logger='horsies.worker')

        _mark(_Handle(starts=[], markers=[_record(core.FinalizingNotHeld)]))

        assert (
            f'Finalizing marker for task {TASK_ID} skipped: the claim changed'
            in caplog.text
        )
        assert f'Failed to mark task {TASK_ID} finalizing' not in caplog.text

    def test_a_marker_record_outside_the_catalog_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            _mark(_Handle(starts=[], markers=[object()]))

    def test_a_failed_marker_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _mark(
            _Handle(
                starts=[],
                markers=[
                    _record(
                        core.MarkFinalizingFailed,
                        message='connection refused',
                        retryable=True,
                    ),
                ],
            ),
        )

        assert (
            f'Failed to mark task {TASK_ID} finalizing: connection refused'
            in caplog.text
        )

    def test_a_core_that_cannot_run_the_marker_is_logged_and_does_not_raise(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        handle = MagicMock()
        handle.mark_finalizing_async.side_effect = core.TooManyPendingCalls(
            'the queue is full',
        )

        _mark(handle)

        assert f'Failed to mark task {TASK_ID} finalizing' in caplog.text

    def test_a_marker_that_hangs_ends_at_its_bound(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The wait of the seat ends at the bound of the core, also when the
        core call does not answer."""
        monkeypatch.setattr(shared_lifecycle, 'FINALIZING_MARKER_TIMEOUT_S', 0.05)

        async def run() -> float:
            loop = asyncio.get_running_loop()
            started = loop.time()
            await asyncio.wait_for(
                shared_lifecycle.mark_task_finalizing(
                    uuid.UUID(TASK_ID),
                    'worker-1',
                    CLAIMED_AT,
                ),
                OUTER_BOUND_S,
            )
            return loop.time() - started

        with patch.object(
            shared_lifecycle,
            '_get_child_core_handle',
            return_value=_HangingHandle(starts=[]),
        ):
            elapsed_s = asyncio.run(run())

        assert elapsed_s < 1.0
        assert f'Failed to mark task {TASK_ID} finalizing' in caplog.text
        assert 'did not finish within' in caplog.text

    def test_the_bound_of_the_marker_is_the_core_bound(self) -> None:
        """The core cancels the marker at 10 s; a cancelled statement ends
        within 10 s more (the core's ``FINALIZING_MARKER_BOUND`` and
        ``CANCELLED_WRITE_END``)."""
        assert shared_lifecycle.FINALIZING_MARKER_TIMEOUT_S == 10.0
        assert shared_lifecycle.CANCELLED_WRITE_END_S == 10.0
