"""The exact-id resend check and the idempotency key on one command.

The command fingerprint leaves ``sent_at`` out, so a keyed send of one
command at a later time replays the first task. The exact-id resend check
compares the fingerprint and the stored ``sent_at``: a send that repeats a
task id with another ``sent_at`` is not the same send.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.types.result import Err, Ok
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TASK_NAME = 'resend_check.echo'
KWARGS_JSON = '{"value":1}'


async def send(
    broker: PostgresBroker,
    *,
    task_id: UUID,
    sent_at: datetime,
    idempotency_key: str | None = None,
) -> BrokerResult[UUID]:
    """Send ``TASK_NAME`` with ``KWARGS_JSON`` as the task decorator does."""
    return await broker.enqueue_async(
        TASK_NAME,
        task_id=task_id,
        send_kind=SendKind.FIRST,
        input_json=KWARGS_JSON,
        sent_at=sent_at,
        idempotency_key=idempotency_key,
    )


async def stored_rows(broker: PostgresBroker, *task_ids: UUID) -> list[tuple[UUID, datetime]]:
    """The ``(id, sent_at)`` of each live row among ``task_ids``."""
    async with current_resources(broker).session_factory() as session:
        rows = (
            await session.execute(
                text(
                    'SELECT CAST(id AS text) AS id, sent_at FROM horsies_tasks '
                    'WHERE id = ANY(CAST(:ids AS uuid[])) ORDER BY id'
                ),
                {'ids': [str(task_id) for task_id in task_ids]},
            )
        ).all()
    return [(UUID(row.id), row.sent_at) for row in rows]


async def test_two_keyed_sends_of_one_command_at_different_times_return_one_task_id(
    broker: PostgresBroker,
) -> None:
    key = f'resend-check-{uuid4()}'
    first_id = mint_task_id()
    second_id = mint_task_id()
    first_sent_at = datetime.now(timezone.utc)
    second_sent_at = first_sent_at + timedelta(seconds=1)

    first = await send(
        broker, task_id=first_id, sent_at=first_sent_at, idempotency_key=key,
    )
    second = await send(
        broker, task_id=second_id, sent_at=second_sent_at, idempotency_key=key,
    )

    assert first == Ok(first_id)
    assert second == Ok(first_id)
    assert await stored_rows(broker, first_id, second_id) == [
        (first_id, first_sent_at),
    ]


async def test_exact_id_resend_with_another_sent_at_is_payload_mismatch(
    broker: PostgresBroker,
) -> None:
    task_id = mint_task_id()
    first_sent_at = datetime.now(timezone.utc)
    resent_at = first_sent_at - timedelta(seconds=1)

    first = await send(broker, task_id=task_id, sent_at=first_sent_at)
    resend = await send(broker, task_id=task_id, sent_at=resent_at)

    assert first == Ok(task_id)
    match resend:
        case Err(err_value=error):
            assert error.code is BrokerErrorCode.PAYLOAD_MISMATCH
        case Ok(ok_value=resend_id):
            raise AssertionError(f'resend with another sent_at returned {resend_id}')
    assert await stored_rows(broker, task_id) == [(task_id, first_sent_at)]


async def test_keyed_send_onto_a_task_id_with_another_sent_at_is_payload_mismatch(
    broker: PostgresBroker,
) -> None:
    """The key bind on a taken task id runs the same check, and binds no key."""
    task_id = mint_task_id()
    first_sent_at = datetime.now(timezone.utc)
    resent_at = first_sent_at - timedelta(seconds=1)

    first = await send(broker, task_id=task_id, sent_at=first_sent_at)
    keyed = await send(
        broker,
        task_id=task_id,
        sent_at=resent_at,
        idempotency_key=f'resend-check-{uuid4()}',
    )

    assert first == Ok(task_id)
    match keyed:
        case Err(err_value=error):
            assert error.code is BrokerErrorCode.PAYLOAD_MISMATCH
        case Ok(ok_value=keyed_id):
            raise AssertionError(f'keyed send with another sent_at returned {keyed_id}')
    async with current_resources(broker).session_factory() as session:
        reservations, bound_digest = (
            await session.execute(
                text(
                    'SELECT '
                    '(SELECT count(*) FROM horsies_key_reservations '
                    'WHERE task_id = CAST(:id AS uuid)), '
                    'idempotency_key_digest '
                    'FROM horsies_tasks WHERE id = CAST(:id AS uuid)'
                ),
                {'id': str(task_id)},
            )
        ).one()
    assert reservations == 0
    assert bound_digest is None

