"""The idempotency key of a send, through the broker, against the baseline schema.

A keyed send claims a reservation in `horsies_key_reservations` and inserts the
task in one transaction. The terminal move of the task turns the reservation
TERMINAL with `expires_at = terminal_at + reservation_window`. Until then a send
with the same key and command returns the task that holds the key.

The terminal move here is the administrative cancel of a PENDING task
(`horsies_cancel_task`): it runs the same move as every other kind.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerIdempotencyKeyConflict,
    BrokerResult,
)
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok, is_ok
from tests.broker_resources_support import current_resources
from tests.integration.terminal_writes import cancel_live_task

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TASK_NAME: Final = 'idempotency.key'
CONCURRENT_SENDS: Final = 64
"""Sends of one key that run at the same time, each on its own connection."""


@dataclass(frozen=True, slots=True)
class Send:
    """One send of one command: the id, the input and the call-site time."""

    task_id: str
    input_json: str
    sent_at: datetime


def make_send(input_json: str = '{"order":"order-1"}') -> Send:
    return Send(
        task_id=mint_task_id(),
        input_json=input_json,
        sent_at=datetime.now(timezone.utc),
    )


def resend_with_a_fresh_id(send: Send) -> Send:
    """The same command and call-site time under a new task id."""
    return Send(
        task_id=mint_task_id(),
        input_json=send.input_json,
        sent_at=send.sent_at,
    )


async def enqueue(
    broker: PostgresBroker, send: Send, key: str | None
) -> BrokerResult[str]:
    return await broker.enqueue_async(
        TASK_NAME,
        task_id=send.task_id,
        send_kind=SendKind.FIRST,
        input_json=send.input_json,
        sent_at=send.sent_at,
        idempotency_key=key,
    )


def unique_key() -> str:
    return f'order-{uuid4()}'


async def count_rows(
    engine: AsyncEngine, statement: str, task_ids: tuple[str, ...]
) -> int:
    async with engine.connect() as connection:
        return int(
            (
                await connection.execute(
                    text(statement), {'ids': list(task_ids)}
                )
            ).scalar_one()
        )


async def live_tasks(engine: AsyncEngine, *task_ids: str) -> int:
    return await count_rows(
        engine,
        'SELECT count(*) FROM horsies_tasks '
        'WHERE id = ANY(CAST(:ids AS uuid[]))',
        task_ids,
    )


async def history_rows(engine: AsyncEngine, *task_ids: str) -> int:
    return await count_rows(
        engine,
        'SELECT count(*) FROM horsies_task_history '
        'WHERE task_id = ANY(CAST(:ids AS uuid[]))',
        task_ids,
    )


async def reservations(engine: AsyncEngine, *task_ids: str) -> int:
    return await count_rows(
        engine,
        'SELECT count(*) FROM horsies_key_reservations '
        'WHERE task_id = ANY(CAST(:ids AS uuid[]))',
        task_ids,
    )


async def move_to_history(engine: AsyncEngine, task_id: str) -> None:
    """Cancel the PENDING task: the move writes its history row."""
    async with engine.begin() as connection:
        outcome = await cancel_live_task(
            connection, task_id, include_running=False
        )
    assert outcome == 'APPLIED'


async def test_an_unkeyed_send_writes_no_reservation(
    broker: PostgresBroker,
) -> None:
    engine = current_resources(broker).engine
    first = make_send()
    second = make_send()

    assert await enqueue(broker, first, None) == Ok(first.task_id)
    assert await enqueue(broker, second, None) == Ok(second.task_id)

    assert await live_tasks(engine, first.task_id, second.task_id) == 2
    assert await reservations(engine, first.task_id, second.task_id) == 0


async def test_a_key_replays_after_the_terminal_move_inside_its_window(
    broker: PostgresBroker,
) -> None:
    engine = current_resources(broker).engine
    key = unique_key()
    first = make_send()
    assert await enqueue(broker, first, key) == Ok(first.task_id)
    await move_to_history(engine, first.task_id)

    resend = resend_with_a_fresh_id(first)
    assert await enqueue(broker, resend, key) == Ok(first.task_id)

    assert await live_tasks(engine, first.task_id, resend.task_id) == 0
    assert await history_rows(engine, first.task_id) == 1
    async with engine.connect() as connection:
        reservation = (
            await connection.execute(
                text(
                    'SELECT r.disposition, '
                    'r.expires_at = h.terminal_at + r.reservation_window '
                    'AS expires_at_window_end '
                    'FROM horsies_key_reservations r '
                    'JOIN horsies_task_history h ON h.task_id = r.task_id '
                    'WHERE r.task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': first.task_id},
            )
        ).one()
    assert reservation.disposition == 'TERMINAL'
    assert reservation.expires_at_window_end is True


async def test_a_key_replays_after_its_history_row_is_removed(
    broker: PostgresBroker,
) -> None:
    """The reservation, not the history row, decides how long a key holds."""
    engine = current_resources(broker).engine
    key = unique_key()
    first = make_send()
    assert await enqueue(broker, first, key) == Ok(first.task_id)
    await move_to_history(engine, first.task_id)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                'DELETE FROM horsies_task_history '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': first.task_id},
        )

    resend = resend_with_a_fresh_id(first)
    assert await enqueue(broker, resend, key) == Ok(first.task_id)

    assert await live_tasks(engine, resend.task_id) == 0
    assert await reservations(engine, first.task_id) == 1


async def test_an_expired_key_applies_to_a_new_task_and_the_old_task_stays(
    broker: PostgresBroker,
) -> None:
    engine = current_resources(broker).engine
    key = unique_key()
    first = make_send()
    assert await enqueue(broker, first, key) == Ok(first.task_id)
    await move_to_history(engine, first.task_id)
    # The window ends: the reservation's expiry moves into the past.
    async with engine.begin() as connection:
        await connection.execute(
            text(
                'UPDATE horsies_key_reservations '
                "SET expires_at = statement_timestamp() - interval '1 second' "
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': first.task_id},
        )

    second = resend_with_a_fresh_id(first)
    assert await enqueue(broker, second, key) == Ok(second.task_id)

    assert await live_tasks(engine, second.task_id) == 1
    assert await history_rows(engine, first.task_id) == 1
    assert await reservations(engine, first.task_id) == 0
    async with engine.connect() as connection:
        holder = (
            await connection.execute(
                text(
                    'SELECT r.disposition, '
                    'r.idempotency_key_digest = t.idempotency_key_digest '
                    'AS digests_match '
                    'FROM horsies_key_reservations r '
                    'JOIN horsies_tasks t ON t.id = r.task_id '
                    'WHERE r.task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': second.task_id},
            )
        ).one()
    assert holder.disposition == 'LIVE'
    assert holder.digests_match is True


async def send_concurrently(
    db_url: str, sends: tuple[Send, ...], key: str
) -> tuple[BrokerResult[str], ...]:
    """Run every send at once, each on its own pooled connection."""
    broker = PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(db_url),
            pool_size=len(sends),
            max_overflow=0,
        )
    )
    try:
        initialized = await broker.ensure_schema_initialized()
        assert initialized.is_ok(), initialized
        return tuple(
            await asyncio.gather(*(enqueue(broker, send, key) for send in sends))
        )
    finally:
        await broker.close_async()


async def test_concurrent_sends_of_one_key_commit_one_task(
    broker: PostgresBroker,
    db_url: str,
) -> None:
    engine = current_resources(broker).engine
    key = unique_key()
    first = make_send()
    sends = (first,) + tuple(
        resend_with_a_fresh_id(first) for _ in range(CONCURRENT_SENDS - 1)
    )

    outcomes = await send_concurrently(db_url, sends, key)

    assert all(is_ok(outcome) for outcome in outcomes), outcomes
    returned_ids = {outcome.ok_value for outcome in outcomes if is_ok(outcome)}
    assert len(returned_ids) == 1, outcomes
    (winner_id,) = returned_ids
    assert winner_id in {send.task_id for send in sends}
    assert (
        await live_tasks(engine, *(send.task_id for send in sends)) == 1
    )
    assert await reservations(engine, *(send.task_id for send in sends)) == 1


async def test_concurrent_sends_with_two_commands_classify_against_the_winner(
    broker: PostgresBroker,
    db_url: str,
) -> None:
    """Each loser replays when it sends the winner's command, else conflicts."""
    engine = current_resources(broker).engine
    key = unique_key()
    sent_at = datetime.now(timezone.utc)
    commands = ('{"order":"order-1"}', '{"order":"order-2"}')
    sends = tuple(
        Send(
            task_id=mint_task_id(),
            input_json=commands[index % 2],
            sent_at=sent_at,
        )
        for index in range(CONCURRENT_SENDS)
    )

    outcomes = await send_concurrently(db_url, sends, key)

    winners = tuple(
        send
        for send, outcome in zip(sends, outcomes, strict=True)
        if outcome == Ok(send.task_id)
    )
    assert len(winners) == 1, outcomes
    (winner,) = winners
    for send, outcome in zip(sends, outcomes, strict=True):
        match outcome:
            case Ok(task_id):
                assert send.input_json == winner.input_json
                assert task_id == winner.task_id
            case Err(BrokerIdempotencyKeyConflict(owner_task_id=owner)):
                assert send.input_json != winner.input_json
                assert owner == winner.task_id
            case Err(other):
                raise AssertionError(f'{send.task_id}: {other!r}')
    assert (
        await live_tasks(engine, *(send.task_id for send in sends)) == 1
    )
