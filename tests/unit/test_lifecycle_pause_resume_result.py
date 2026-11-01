"""Unit tests for lifecycle pause/resume Result typing.

pause_workflow / resume_workflow return ``HandleResult[bool]``. A database
error is a ``HandleOperationError`` variant; any other exception propagates.
Resume runs its recovery pass in a second transaction after the commit, so a
database error there is ``HandleResumeRecoveryFailed``. The sync wrappers
return a ``LoopRunnerError`` as ``HandleLoopRunnerFailed`` and let other
exceptions propagate.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from tests.broker_resources_support import attach_resources, fake_resources

from horsies.core.types.result import is_ok, is_err
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleLoopRunnerFailed,
    HandleResumeRecoveryFailed,
    HandleWorkflowNotFound,
)
from horsies.core.workflows.recovery import GET_WORKFLOW_TREE_IDS_SQL

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


def _session_of(broker: MagicMock) -> AsyncMock:
    """The session that ``_mock_broker`` hands out."""
    resources = broker.select_resources.return_value.ok_value
    session: AsyncMock = resources.session_factory.return_value.__aenter__.return_value
    return session


# ===========================================================================
# pause_workflow async
# ===========================================================================


@pytest.mark.unit
class TestPauseWorkflowResult:
    """pause_workflow returns HandleResult[bool]."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_running_returns_ok_true(self) -> None:
        """Workflow in RUNNING state -> Ok(True)."""
        from horsies.core.lifecycle.commands import (
            AbandonNodesOfPausedWorkflows,
        )
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=('some-id',))
        with patch(
            'horsies.core.workflows.lifecycle.apply_batch_async',
            new=AsyncMock(return_value=[]),
        ) as apply:
            result = await pause_workflow(broker, 'wf-1')

        assert is_ok(result)
        assert result.ok_value is True
        assert apply.await_args is not None
        command = apply.await_args.args[1]
        assert isinstance(command, AbandonNodesOfPausedWorkflows)
        assert command.workflow_ids == ('wf-1',)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_wrong_state_returns_ok_false(self) -> None:
        """Workflow exists but not RUNNING -> Ok(False)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=None, exists_row=('wf-1',))
        result = await pause_workflow(broker, 'wf-1')

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_not_found_returns_err(self) -> None:
        """Workflow does not exist -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=None, exists_row=None)
        result = await pause_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleWorkflowNotFound)
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_db_failure_returns_db_operation_failed(self) -> None:
        """SQLAlchemy error -> Err(HandleDbOperationFailed) with the cause."""
        from horsies.core.workflows.lifecycle import pause_workflow

        db_exc = _db_error()
        broker = _mock_broker(side_effect=db_exc)
        result = await pause_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == 'wf-1'
        assert err.exception is db_exc
        assert err.retryable is True

    @pytest.mark.asyncio(loop_scope='function')
    @pytest.mark.parametrize(
        'defect', [RuntimeError('logic bug'), AssertionError('invariant')],
        ids=type,
    )
    async def test_pause_defect_in_the_body_propagates(
        self, defect: Exception,
    ) -> None:
        """An exception that is not a database error propagates; nothing commits."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=('some-id',))
        session = _session_of(broker)
        with (
            patch(
                'horsies.core.workflows.lifecycle.cascade_pause_to_children',
                new=AsyncMock(side_effect=defect),
            ),
            pytest.raises(type(defect)) as exc_info,
        ):
            await pause_workflow(broker, 'wf-1')

        assert exc_info.value is defect
        session.commit.assert_not_awaited()


# ===========================================================================
# resume_workflow async
# ===========================================================================


@pytest.mark.unit
class TestResumeWorkflowResult:
    """resume_workflow returns HandleResult[bool]."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_wrong_state_returns_ok_false(self) -> None:
        """Workflow exists but not PAUSED -> Ok(False)."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _mock_broker(transition_row=None, exists_row=('wf-1',))
        result = await resume_workflow(broker, 'wf-1')

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_not_found_returns_err(self) -> None:
        """Workflow does not exist -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _mock_broker(transition_row=None, exists_row=None)
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleWorkflowNotFound)
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_db_failure_returns_db_operation_failed(self) -> None:
        """SQLAlchemy error -> Err(HandleDbOperationFailed) with the cause."""
        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = _db_error()
        broker = _mock_broker(side_effect=db_exc)
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == 'wf-1'
        assert err.exception is db_exc
        assert err.retryable is True


# ===========================================================================
# resume_workflow: the commit splits the failure states
# ===========================================================================


def _resume_sessions(
    *, recovery_error: BaseException | None = None,
) -> tuple[MagicMock, AsyncMock, AsyncMock]:
    """A broker whose resume transaction moves one PAUSED workflow to RUNNING.

    The first session is the resume transaction, the second the recovery
    pass. ``recovery_error`` is raised by the recovery pass's tree query.
    """
    resume_row = MagicMock()
    resume_row.depth = 0
    resume_row.root_workflow_id = WORKFLOW_ID
    resume_result = MagicMock()
    resume_result.fetchone.return_value = resume_row
    resume_calls: list[Any] = [resume_result]

    def resume_execute(*_args: Any, **_kwargs: Any) -> MagicMock:
        return resume_calls.pop(0) if resume_calls else _empty_result()

    resume_session = AsyncMock()
    resume_session.info = {}
    resume_session.execute = AsyncMock(side_effect=resume_execute)
    resume_session.commit = AsyncMock()

    tree_row = MagicMock()
    tree_row.id = WORKFLOW_ID
    tree_result = MagicMock()
    tree_result.fetchall.return_value = [tree_row]

    def recovery_execute(statement: Any, *_args: Any, **_kwargs: Any) -> MagicMock:
        assert statement is GET_WORKFLOW_TREE_IDS_SQL
        if recovery_error is not None:
            raise recovery_error
        return tree_result

    recovery_session = AsyncMock()
    recovery_session.info = {}
    recovery_session.execute = AsyncMock(side_effect=recovery_execute)
    recovery_session.commit = AsyncMock()

    contexts: list[AsyncMock] = []
    for session in (resume_session, recovery_session):
        cm = AsyncMock()
        cm.__aenter__ = AsyncMock(return_value=session)
        cm.__aexit__ = AsyncMock(return_value=False)
        contexts.append(cm)
    broker = MagicMock()
    attach_resources(
        broker,
        fake_resources(session_factory=MagicMock(side_effect=contexts)),
    )
    return broker, resume_session, recovery_session


@contextmanager
def _engine_completion_stubbed(recover: AsyncMock) -> Generator[None, None, None]:
    """Stub the completion checks of the resume transaction and the tree pass."""
    with (
        patch(
            'horsies.core.workflows.engine.check_workflow_completion',
            new=AsyncMock(),
        ),
        patch(
            'horsies.core.workflows.engine.drain_parent_propagations_in_session',
            new=AsyncMock(),
        ),
        patch(
            'horsies.core.workflows.recovery.recover_stuck_workflow_tree',
            new=recover,
        ),
    ):
        yield


@pytest.mark.unit
class TestResumeCommitSplit:
    """A database error before the resume commit and one after it are two states."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_runs_the_recovery_pass_after_the_commit(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, resume_session, recovery_session = _resume_sessions()
        recover = AsyncMock(return_value=0)
        with _engine_completion_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        resume_session.commit.assert_awaited_once()
        recover.assert_awaited_once_with(recovery_session, broker, [WORKFLOW_ID])
        recovery_session.commit.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_recovery_db_error_returns_resume_recovery_failed(self) -> None:
        """The resume committed; only the second transaction failed."""
        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = _db_error()
        broker, resume_session, recovery_session = _resume_sessions(
            recovery_error=db_exc,
        )
        with _engine_completion_stubbed(AsyncMock(return_value=0)):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleResumeRecoveryFailed)
        assert err.workflow_id == WORKFLOW_ID
        assert err.exception is db_exc
        assert err.retryable is False
        resume_session.commit.assert_awaited_once()
        recovery_session.commit.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_commit_db_error_returns_db_operation_failed(self) -> None:
        """A failed resume commit is not a recovery failure; no recovery pass runs."""
        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = _db_error()
        broker, resume_session, recovery_session = _resume_sessions()
        resume_session.commit = AsyncMock(side_effect=db_exc)
        recover = AsyncMock(return_value=0)
        with _engine_completion_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is db_exc
        assert err.retryable is True
        recover.assert_not_awaited()
        recovery_session.execute.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    @pytest.mark.parametrize(
        'defect', [RuntimeError('logic bug'), AssertionError('invariant')],
        ids=type,
    )
    async def test_defect_in_the_resume_body_propagates(
        self, defect: Exception,
    ) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, resume_session, recovery_session = _resume_sessions()
        with (
            _engine_completion_stubbed(AsyncMock(return_value=0)),
            patch(
                'horsies.core.workflows.lifecycle.cascade_resume_to_children',
                new=AsyncMock(side_effect=defect),
            ),
            pytest.raises(type(defect)) as exc_info,
        ):
            await resume_workflow(broker, WORKFLOW_ID)

        assert exc_info.value is defect
        resume_session.commit.assert_not_awaited()
        recovery_session.execute.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_defect_in_the_recovery_pass_propagates(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        defect = RuntimeError('logic bug')
        broker, resume_session, recovery_session = _resume_sessions()
        with (
            _engine_completion_stubbed(AsyncMock(side_effect=defect)),
            pytest.raises(RuntimeError) as exc_info,
        ):
            await resume_workflow(broker, WORKFLOW_ID)

        assert exc_info.value is defect
        resume_session.commit.assert_awaited_once()
        recovery_session.commit.assert_not_awaited()


# ===========================================================================
# Sync wrappers: LoopRunnerError and other exceptions
# ===========================================================================


@pytest.mark.unit
class TestPauseSyncWrapper:
    """pause_workflow_sync returns LoopRunnerError as an Err and lets other exceptions propagate."""

    def test_loop_runner_error_returns_err(self) -> None:
        """LoopRunnerError -> Err(HandleLoopRunnerFailed)."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync
        from horsies.core.utils.loop_runner import LoopRunnerError

        broker = MagicMock()
        with patch.object(
            broker, 'run_sync',
        ) as mock_call:
            runner_error = LoopRunnerError('dead')
            mock_call.side_effect = runner_error
            result = pause_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleLoopRunnerFailed)
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False
        assert err.exception is runner_error

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a LoopRunnerError propagates unchanged."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        broker = MagicMock()
        defect = RuntimeError('boom')
        with (
            patch.object(broker, 'run_sync', side_effect=defect),
            pytest.raises(RuntimeError) as exc_info,
        ):
            pause_workflow_sync(broker, 'wf-1')

        assert exc_info.value is defect


@pytest.mark.unit
class TestResumeSyncWrapper:
    """resume_workflow_sync returns LoopRunnerError as an Err and lets other exceptions propagate."""

    def test_loop_runner_error_returns_err(self) -> None:
        """LoopRunnerError -> Err(HandleLoopRunnerFailed)."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync
        from horsies.core.utils.loop_runner import LoopRunnerError

        broker = MagicMock()
        with patch.object(
            broker, 'run_sync',
        ) as mock_call:
            runner_error = LoopRunnerError('dead')
            mock_call.side_effect = runner_error
            result = resume_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleLoopRunnerFailed)
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False
        assert err.exception is runner_error

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a LoopRunnerError propagates unchanged."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        broker = MagicMock()
        defect = RuntimeError('boom')
        with (
            patch.object(broker, 'run_sync', side_effect=defect),
            pytest.raises(RuntimeError) as exc_info,
        ):
            resume_workflow_sync(broker, 'wf-1')

        assert exc_info.value is defect
