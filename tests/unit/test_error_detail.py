"""The error code and the error detail of a broker or send error must agree.

``IDEMPOTENCY_KEY_CONFLICT`` carries an ``IdempotencyKeyOwner``; every other
code carries no detail. A wrong pair is a defect in the code that builds the
error, so construction fails an assertion.
"""

from __future__ import annotations

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError
from horsies.core.models.task_send_types import TaskSendError, TaskSendErrorCode
from horsies.core.types.error_detail import IdempotencyKeyOwner

pytestmark = [pytest.mark.unit]

OWNER = IdempotencyKeyOwner(owner_task_id='owner-task-1')


class TestBrokerOperationError:
    def test_key_conflict_with_owner_constructs(self) -> None:
        error = BrokerOperationError(
            code=BrokerErrorCode.IDEMPOTENCY_KEY_CONFLICT,
            message='reserved',
            retryable=False,
            detail=OWNER,
        )
        assert error.detail == OWNER

    def test_key_conflict_without_owner_fails_assertion(self) -> None:
        with pytest.raises(AssertionError, match='IDEMPOTENCY_KEY_CONFLICT carries'):
            BrokerOperationError(
                code=BrokerErrorCode.IDEMPOTENCY_KEY_CONFLICT,
                message='reserved',
                retryable=False,
            )

    def test_owner_on_another_code_fails_assertion(self) -> None:
        with pytest.raises(AssertionError, match='every other code carries no detail'):
            BrokerOperationError(
                code=BrokerErrorCode.PAYLOAD_MISMATCH,
                message='mismatch',
                retryable=False,
                detail=OWNER,
            )


class TestTaskSendError:
    def test_key_conflict_with_owner_constructs(self) -> None:
        error = TaskSendError(
            code=TaskSendErrorCode.IDEMPOTENCY_KEY_CONFLICT,
            message='reserved',
            retryable=False,
            detail=OWNER,
        )
        assert error.detail == OWNER

    def test_key_conflict_without_owner_fails_assertion(self) -> None:
        with pytest.raises(AssertionError, match='IDEMPOTENCY_KEY_CONFLICT carries'):
            TaskSendError(
                code=TaskSendErrorCode.IDEMPOTENCY_KEY_CONFLICT,
                message='reserved',
                retryable=False,
            )

    def test_owner_on_another_code_fails_assertion(self) -> None:
        with pytest.raises(AssertionError, match='every other code carries no detail'):
            TaskSendError(
                code=TaskSendErrorCode.ENQUEUE_FAILED,
                message='db gone',
                retryable=True,
                detail=OWNER,
            )
