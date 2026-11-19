"""Send-level outcomes of an idempotency key that conflicts.

Two states reach the broker's key check with different retry conditions:

- The key is reserved by another task with a different command. The send
  returns ``IDEMPOTENCY_KEY_CONFLICT`` with the owner task id, and the
  retry methods accept it, because the key clears when its reservation
  ends.
- The task id already exists with the same command but a different key.
  The sent payload differs from the original send, so the send returns
  ``PAYLOAD_MISMATCH``. The retry methods do not accept that variant: it
  is not a ``TaskSendRetryable``, so a retry of it fails type checking,
  and at run time it fails an assertion.
"""

from __future__ import annotations

import dataclasses
from typing import Any
from uuid import uuid4

import pytest

from horsies.core.app import Horsies
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendIdempotencyKeyConflict,
    TaskSendPayloadMismatch,
)
from horsies.core.types.result import is_err, is_ok
from tests.integration.conftest import make_simple_task

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _echo(app: Horsies) -> Any:
    return make_simple_task(app, name=f'idempotency_conflict_{uuid4().hex}')


class TestKeyReservedByAnotherTask:
    async def test_conflict_names_the_owner_and_retry_is_accepted(
        self, app: Horsies,
    ) -> None:
        echo = _echo(app)
        key = f'conflict-key-{uuid4()}'
        first = await echo.with_options(idempotency_key=key).send_async(value=1)
        assert is_ok(first), first

        conflict = await echo.with_options(idempotency_key=key).send_async(value=2)

        assert is_err(conflict)
        error = conflict.err_value
        assert isinstance(error, TaskSendIdempotencyKeyConflict)
        assert error.owner_task_id == first.ok_value.task_id
        # The retry is accepted and reaches the broker; the reservation is
        # still active, so the broker returns the same conflict.
        retried = await echo.retry_send_async(error)
        assert is_err(retried)
        assert isinstance(retried.err_value, TaskSendIdempotencyKeyConflict)
        assert retried.err_value.owner_task_id == error.owner_task_id


class TestTaskIdBoundToAnotherKey:
    async def test_changed_key_on_retry_is_payload_mismatch(
        self, app: Horsies,
    ) -> None:
        echo = _echo(app)
        first_key = f'first-key-{uuid4()}'
        # A sync send inside a running loop returns the prepared payload
        # without sending it; the async retry sends it.
        prepared = echo.with_options(idempotency_key=first_key).send(value=1)
        assert is_err(prepared)
        assert isinstance(prepared.err_value, TaskSendAsyncContext)
        assert prepared.err_value.payload is not None
        assert is_ok(await echo.retry_send_async(prepared.err_value))
        changed_key = dataclasses.replace(
            prepared.err_value,
            payload=dataclasses.replace(
                prepared.err_value.payload,
                idempotency_key=f'second-key-{uuid4()}',
            ),
        )

        mismatch = await echo.retry_send_async(changed_key)

        assert is_err(mismatch)
        assert isinstance(mismatch.err_value, TaskSendPayloadMismatch)
        assert mismatch.err_value.task_id == prepared.err_value.task_id
