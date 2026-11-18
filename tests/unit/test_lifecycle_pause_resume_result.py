"""Unit tests for lifecycle pause/resume Result typing.

pause_workflow / resume_workflow return ``HandleResult[bool]``. A database
error is a ``HandleOperationError`` variant; any other exception propagates.
The pause is one ``horsies_workflow_pause`` statement that the core runs;
its record decides the result, and a failure is the core's generated
handle error class, returned as it is.
The resume runs in the core too (``CoreHandle.resume_workflow_async``): its
record decides the result in the same way. The tests of the resume transaction, the parent
advance and the recovery pass after the commit drive the Python caller
before the switch (``tests/dualrun/reference/workflow_resume_caller.py``),
the reference side of the resume's dual-run: a database error after the
commit is ``HandleResumeRecoveryFailed``. The sync wrappers return a
core boundary error of the blocking call as ``HandleDbOperationFailed`` and
let other exceptions propagate.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

import horsies._core as core
from tests.broker_resources_support import attach_resources, fake_resources
from tests.workflow_start_support import outcome_row

from horsies.core.types.result import is_ok, is_err
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)

WORKFLOW_ID = 'wf-1'


def _db_error() -> OperationalError:
    return OperationalError('SELECT 1', {}, Exception('connection refused'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_result() -> MagicMock:
    """A mock DB result that returns no rows."""
    r = MagicMock()
    r.fetchone.return_value = None
    r.fetchall.return_value = []
    return r


def _mock_broker(
    *,
    transition_row: tuple[Any, ...] | None = None,
    exists_row: tuple[Any, ...] | None = None,
    side_effect: Exception | None = None,
) -> MagicMock:
    """Build a mock broker whose session executes return pre-canned rows.

    Args:
        transition_row: row returned by the UPDATE (PAUSE/RESUME) SQL.
            None means the UPDATE matched nothing.
        exists_row: row returned by the existence-check SELECT.
            None means the workflow does not exist.
        side_effect: if set, the session context manager raises this
            instead of executing normally.
    """
    session = AsyncMock()

    if side_effect is not None:
        # Make the session_factory context manager raise immediately
        cm = AsyncMock()
        cm.__aenter__ = AsyncMock(side_effect=side_effect)
        cm.__aexit__ = AsyncMock(return_value=False)
        broker = MagicMock()
        attach_resources(broker, fake_resources(session_factory=MagicMock(return_value=cm)))
        return broker

    # Build a call counter so we can return different results for
    # sequential execute() calls.
    call_results: list[Any] = []

    # First call: the UPDATE SQL
    transition_result = MagicMock()
    transition_result.fetchone.return_value = transition_row
    call_results.append(transition_result)

    if transition_row is None:
        # Second call: the existence check
        exists_result = MagicMock()
        exists_result.fetchone.return_value = exists_row
        call_results.append(exists_result)

    # After the core calls, cascade/notify/etc. may fire more executes.
    # Return empty results for any subsequent calls.
    def _execute_side_effect(*args: Any, **kwargs: Any) -> MagicMock:
        if call_results:
            return call_results.pop(0)
        return _empty_result()

    session.execute = AsyncMock(side_effect=_execute_side_effect)
    session.commit = AsyncMock()

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)

    broker = MagicMock()
    attach_resources(broker, fake_resources(session_factory=MagicMock(return_value=cm)))
    return broker


# ===========================================================================
# pause_workflow async
# ===========================================================================


PAUSE_WORKFLOW_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e03'


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = uuid.UUID(PAUSE_WORKFLOW_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _pause_broker(answer: object = None, *, raises: BaseException | None = None) -> MagicMock:
    """A broker whose core handle answers the pause with ``answer``, or raises."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.pause_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.pause_workflow_async = AsyncMock(side_effect=raises)
    return broker


@pytest.mark.unit
class TestPauseWorkflowResult:
    """pause_workflow returns HandleResult[bool] from the core's record."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_running_returns_ok_true(self) -> None:
        """WorkflowPaused -> Ok(True); one call with the id as a UUID."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowPaused))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        broker._core_handle.pause_workflow_async.assert_awaited_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_wrong_state_returns_ok_false(self) -> None:
        """WorkflowNotRunning -> Ok(False)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowNotRunning))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_not_found_returns_err(self) -> None:
        """WorkflowAbsent -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowAbsent))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleWorkflowNotFound)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_db_failure_is_the_core_s_class_as_it_is(self) -> None:
        """The core's HandleDbOperationFailed -> Err of the same object, with
        the core's message, cause and retry rule."""
        from horsies.core.workflows.lifecycle import pause_workflow

        failed = HandleDbOperationFailed(
            message=(
                f'Pause failed for workflow {PAUSE_WORKFLOW_ID}: '
                'connection failed: connection refused'
            ),
            retryable=True,
            workflow_id=PAUSE_WORKFLOW_ID,
            exception=core.CoreConnectionError(
                'pause_workflow', 'connection failed: connection refused', True,
            ),
        )
        broker = _pause_broker(failed)
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert result.err_value is failed

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_stored_data_invalid_is_the_core_s_class_as_it_is(self) -> None:
        """The core's HandleStoredDataInvalid -> Err of the same object."""
        from horsies.core.workflows.lifecycle import pause_workflow

        invalid = HandleStoredDataInvalid(
            message=(
                f'Pause failed for workflow {PAUSE_WORKFLOW_ID}: invalid status: BOGUS'
            ),
            workflow_id=PAUSE_WORKFLOW_ID,
            exception=core.CoreContractViolation(
                'pause_workflow', 'invalid status: BOGUS',
            ),
        )
        broker = _pause_broker(invalid)
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert result.err_value is invalid

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_core_boundary_error_returns_db_operation_failed(self) -> None:
        """A core boundary error raised by the call -> Err(HandleDbOperationFailed),
        retryable by the core's rule."""
        from horsies.core.workflows.lifecycle import pause_workflow

        refused = core.TooManyPendingCalls('CoreHandle.pause_workflow_async', 64)
        broker = _pause_broker(raises=refused)
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is refused
        assert err.retryable is True

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_id_that_is_not_a_uuid_calls_nothing(self) -> None:
        """An id text that is not a UUID -> Err(HandleDbOperationFailed), not
        retryable, and no core call."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowPaused))
        result = await pause_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.retryable is False
        broker._core_handle.pause_workflow_async.assert_not_awaited()


# ===========================================================================
# resume_workflow through the core
# ===========================================================================


def _resume_broker(answer: object = None, *, raises: BaseException | None = None) -> MagicMock:
    """A broker whose core handle answers the resume with ``answer``, or raises."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.resume_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.resume_workflow_async = AsyncMock(side_effect=raises)
    return broker


@pytest.mark.unit
class TestResumeThroughCore:
    """resume_workflow returns HandleResult[bool] from the core's record."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resumed_returns_ok_true_with_the_id_as_a_uuid(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowResumed))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        broker._core_handle.resume_workflow_async.assert_awaited_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )

    @pytest.mark.asyncio(loop_scope='function')
    async def test_not_paused_returns_ok_false(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowNotPaused))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_absent_returns_workflow_not_found(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowAbsent))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, HandleWorkflowNotFound)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_recovery_failed_after_the_commit_is_the_core_s_class_as_it_is(
        self,
    ) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        recovery_failed = HandleResumeRecoveryFailed(
            message=(
                f'Workflow {PAUSE_WORKFLOW_ID} resumed; the recovery pass for its '
                'tree failed: broker error: connection failed: connection refused'
            ),
            workflow_id=PAUSE_WORKFLOW_ID,
            exception=core.CoreConnectionError(
                'resume_workflow', 'connection failed: connection refused', True,
            ),
        )
        broker = _resume_broker(recovery_failed)
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert result.err_value is recovery_failed
        assert result.err_value.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_failure_of_the_core_is_its_class_as_it_is(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        failed = HandleDbOperationFailed(
            message=(
                f'Resume failed for workflow {PAUSE_WORKFLOW_ID}: '
                'child spec source: build of the child did not run: queue full'
            ),
            retryable=True,
            workflow_id=PAUSE_WORKFLOW_ID,
            exception=core.CorePreconditionFailed(
                'resume_workflow',
                'child spec source: build of the child did not run: queue full',
                True,
            ),
        )
        broker = _resume_broker(failed)
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert result.err_value is failed

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_id_that_is_not_a_uuid_makes_no_core_call(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowResumed))
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        assert isinstance(result.err_value, HandleDbOperationFailed)
        assert result.err_value.retryable is False
        broker._core_handle.resume_workflow_async.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_core_boundary_error_returns_db_operation_failed(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(raises=core.TooManyPendingCalls('too many calls'))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, HandleDbOperationFailed)
        assert result.err_value.retryable is True


# ===========================================================================
# resume_workflow async
# ===========================================================================


def _resume_sessions(
    disposition: str = 'RESUMED',
) -> tuple[MagicMock, AsyncMock, MagicMock]:
    """A broker whose resume transaction answers ``horsies_workflow_resume``
    with one OUTCOME row of ``disposition``, its session, and its session
    factory, which the recovery pass of the tree receives.
    """
    resume_result = MagicMock()
    resume_result.fetchall.return_value = [outcome_row(disposition)]
    resume_session = AsyncMock()
    resume_session.info = {}
    resume_session.execute = AsyncMock(return_value=resume_result)
    resume_session.commit = AsyncMock()

    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=resume_session)
    context.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context)
    broker = MagicMock()
    attach_resources(broker, fake_resources(session_factory=session_factory))
    return broker, resume_session, session_factory


@contextmanager
def _recovery_stubbed(recover: AsyncMock) -> Generator[None, None, None]:
    """Stub the recovery pass of the tree after the resume commit."""
    with patch('horsies.core.workflows.recovery.recover_stuck_workflow_tree', new=recover):
        yield


@pytest.mark.unit
class TestResumeWorkflowResult:
    """resume_workflow returns HandleResult[bool], from the function's outcome."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_wrong_state_returns_ok_false(self) -> None:
        """NOT_PAUSED -> Ok(False); no recovery pass."""
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        broker, _, _ = _resume_sessions('NOT_PAUSED')
        recover = AsyncMock(return_value=None)
        with _recovery_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_not_found_returns_err(self) -> None:
        """NOT_FOUND -> Err(WORKFLOW_NOT_FOUND)."""
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        broker, _, _ = _resume_sessions('NOT_FOUND')
        result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleWorkflowNotFound)
        assert err.workflow_id == WORKFLOW_ID
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_db_failure_returns_db_operation_failed(self) -> None:
        """SQLAlchemy error -> Err(HandleDbOperationFailed) with the cause."""
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        db_exc = _db_error()
        broker = _mock_broker(side_effect=db_exc)
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == 'wf-1'
        assert err.exception is db_exc
        assert err.retryable is True

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_outcome_outside_the_contract_raises(self) -> None:
        from horsies.core.lifecycle.outcomes import OutcomeDecodeError
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        broker, resume_session, _ = _resume_sessions('RESUMED_TWICE')
        with pytest.raises(OutcomeDecodeError, match='RESUMED_TWICE'):
            await resume_workflow(broker, WORKFLOW_ID)

        resume_session.commit.assert_not_awaited()


# ===========================================================================
# resume_workflow: the commit splits the failure states
# ===========================================================================


@pytest.mark.unit
class TestResumeCommitSplit:
    """A database error before the resume commit and one after it are two states."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_runs_the_recovery_pass_after_the_commit(self) -> None:
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        broker, resume_session, session_factory = _resume_sessions()
        recover = AsyncMock(return_value=None)
        with _recovery_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        resume_session.commit.assert_awaited_once()
        recover.assert_awaited_once_with(session_factory, broker, WORKFLOW_ID)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_recovery_db_error_returns_resume_recovery_failed(self) -> None:
        """The resume committed; only the second transaction failed."""
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        db_exc = _db_error()
        broker, resume_session, _ = _resume_sessions()
        with _recovery_stubbed(AsyncMock(side_effect=db_exc)):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleResumeRecoveryFailed)
        assert err.workflow_id == WORKFLOW_ID
        assert err.exception is db_exc
        assert err.retryable is False
        resume_session.commit.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_failed_parent_advance_returns_resume_recovery_failed(self) -> None:
        """The resume committed and ended a child; the advance of its parent failed."""
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow
        from horsies.core.workflows.steps import ParentAdvanceFailure

        db_exc = _db_error()
        broker, resume_session, _ = _resume_sessions()
        failure = ParentAdvanceFailure(
            failed_child_workflow_id='child-wf', still_owed=('child-wf',), cause=db_exc,
        )
        recover = AsyncMock(return_value=None)
        with (
            _recovery_stubbed(recover),
            patch(
                'tests.dualrun.reference.workflow_resume_caller.advance_parents',
                new=AsyncMock(return_value=failure),
            ),
        ):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleResumeRecoveryFailed)
        assert err.exception is db_exc
        resume_session.commit.assert_awaited_once()
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_commit_db_error_returns_db_operation_failed(self) -> None:
        """A failed resume commit is not a recovery failure; no recovery pass runs."""
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        db_exc = _db_error()
        broker, resume_session, _ = _resume_sessions()
        resume_session.commit = AsyncMock(side_effect=db_exc)
        recover = AsyncMock(return_value=0)
        with _recovery_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is db_exc
        assert err.retryable is True
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    @pytest.mark.parametrize(
        'defect', [RuntimeError('logic bug'), AssertionError('invariant')],
        ids=type,
    )
    async def test_defect_in_the_resume_body_propagates(
        self, defect: Exception,
    ) -> None:
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        broker, resume_session, _ = _resume_sessions()
        recover = AsyncMock(return_value=None)
        with (
            _recovery_stubbed(recover),
            patch(
                'horsies.core.workflows.steps.decode_step_rows',
                side_effect=defect,
            ),
            pytest.raises(type(defect)) as exc_info,
        ):
            await resume_workflow(broker, WORKFLOW_ID)

        assert exc_info.value is defect
        resume_session.commit.assert_not_awaited()
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_defect_in_the_recovery_pass_propagates(self) -> None:
        from tests.dualrun.reference.workflow_resume_caller import resume_workflow

        defect = RuntimeError('logic bug')
        broker, resume_session, _ = _resume_sessions()
        with (
            _recovery_stubbed(AsyncMock(side_effect=defect)),
            pytest.raises(RuntimeError) as exc_info,
        ):
            await resume_workflow(broker, WORKFLOW_ID)

        assert exc_info.value is defect
        resume_session.commit.assert_awaited_once()


# ===========================================================================
# Sync wrappers: the blocking form, a core boundary error, other exceptions
# ===========================================================================


def _blocking_broker(method: str, answer: object) -> MagicMock:
    """A broker whose core handle answers the blocking ``method`` with
    ``answer``: a record, or an exception that the call raises."""
    broker = MagicMock()
    match answer:
        case BaseException():
            setattr(broker._core_handle, method, MagicMock(side_effect=answer))
        case _:
            setattr(broker._core_handle, method, MagicMock(return_value=answer))
    return broker


@pytest.mark.unit
class TestPauseSyncWrapper:
    """pause_workflow_sync calls CoreHandle.pause_workflow on the calling
    thread: a core boundary error is HandleDbOperationFailed, and other
    exceptions propagate."""

    def test_the_record_decides_the_result(self) -> None:
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        broker = _blocking_broker('pause_workflow', _record(core.WorkflowPaused))
        result = pause_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        broker._core_handle.pause_workflow.assert_called_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )
        broker.run_sync.assert_not_called()

    def test_core_boundary_error_returns_err(self) -> None:
        """A CoreError of the blocking call -> Err(HandleDbOperationFailed)."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        refused = core.TooManyPendingCalls('CoreHandle.pause_workflow', 64)
        broker = _blocking_broker('pause_workflow', refused)
        result = pause_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is True
        assert err.exception is refused

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a CoreError propagates unchanged."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        defect = RuntimeError('boom')
        broker = _blocking_broker('pause_workflow', defect)
        with pytest.raises(RuntimeError) as exc_info:
            pause_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert exc_info.value is defect


@pytest.mark.unit
class TestResumeSyncWrapper:
    """resume_workflow_sync calls CoreHandle.resume_workflow on the calling
    thread: a core boundary error is HandleDbOperationFailed, and other
    exceptions propagate."""

    def test_the_record_decides_the_result(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        broker = _blocking_broker('resume_workflow', _record(core.WorkflowNotPaused))
        result = resume_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False
        broker._core_handle.resume_workflow.assert_called_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )
        broker.run_sync.assert_not_called()

    def test_core_boundary_error_returns_err(self) -> None:
        """A CoreError of the blocking call -> Err(HandleDbOperationFailed)."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        refused = core.TooManyPendingCalls('CoreHandle.resume_workflow', 64)
        broker = _blocking_broker('resume_workflow', refused)
        result = resume_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is True
        assert err.exception is refused

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a CoreError propagates unchanged."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        defect = RuntimeError('boom')
        broker = _blocking_broker('resume_workflow', defect)
        with pytest.raises(RuntimeError) as exc_info:
            resume_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert exc_info.value is defect
