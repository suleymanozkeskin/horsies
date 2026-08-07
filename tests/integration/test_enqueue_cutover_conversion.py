"""The converted enqueue statement against a real v27-migrated database.

The uncertain-commit resend contract must survive the conversion:
resending the same task id with the same fingerprint deduplicates to
the committed request, and a differing fingerprint is a payload
mismatch — exactly the behavior producers rely on when a commit's
acknowledgment is lost. Alongside it, every new row carries real
cutover values: the version-1 command fingerprint, the retention
snapshot, the input digest, and the prepared envelope disposition.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.history.identity.uuid7 import mint_task_id, uuid7_birth_at
from horsies.core.history.rerun.input_envelope import (
    encode_input_envelope_v1,
)
from horsies.core.types.result import is_err, is_ok
from horsies.core.utils.fingerprint import enqueue_fingerprint

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(frozen=True, slots=True)
class Send:
    task_id: str
    enqueue_sha: str
    kwargs_json: str | None
    sent_at: datetime


def make_send(kwargs_json: str | None = '{"x":1}') -> Send:
    sent_at = datetime.now(timezone.utc)
    return Send(
        task_id=mint_task_id(),
        enqueue_sha=enqueue_fingerprint(
            task_name='cutover.enqueue',
            queue_name='default',
            priority=100,
            args_json=None,
            kwargs_json=kwargs_json,
            sent_at=sent_at,
            good_until=None,
            enqueue_delay_seconds=None,
            task_options=None,
        ),
        kwargs_json=kwargs_json,
        sent_at=sent_at,
    )


async def enqueue(broker: PostgresBroker, send: Send) -> BrokerResult[str]:
    return await broker.enqueue_async(
        'cutover.enqueue',
        task_id=send.task_id,
        enqueue_sha=send.enqueue_sha,
        kwargs_json=send.kwargs_json,
        sent_at=send.sent_at,
    )


class TestConvertedStatement:
    async def test_new_row_carries_real_cutover_values(
        self, broker: PostgresBroker
    ) -> None:
        send = make_send()
        result = await enqueue(broker, send)
        assert is_ok(result)

        async with broker.session_factory() as session:
            row = (
                await session.execute(
                    text(
                        'SELECT command_fingerprint_version, '
                        'command_fingerprint, retention_class_key, '
                        'retain_rerun_input, input_digest, '
                        'prepared_rerun_input_disposition, '
                        'prepared_rerun_input_inline '
                        'FROM horsies_tasks WHERE id = :id'
                    ),
                    {'id': send.task_id},
                )
            ).one()
        assert row.command_fingerprint_version == 1
        assert len(bytes(row.command_fingerprint)) == 32
        assert row.retention_class_key == 'forever'
        assert row.retain_rerun_input is False
        expected_payload = encode_input_envelope_v1(
            args=[], kwargs={'x': 1}, options=None
        )
        assert bytes(row.input_digest) == sha256(expected_payload).digest()
        assert row.prepared_rerun_input_disposition == 'DECLINED_BY_POLICY'
        assert row.prepared_rerun_input_inline is None

    async def test_minted_identity_is_v7_with_a_real_birth(
        self, broker: PostgresBroker
    ) -> None:
        send = make_send()
        birth = uuid7_birth_at(send.task_id)
        assert birth is not None
        assert abs((birth - datetime.now(timezone.utc)).total_seconds()) < 60

    async def test_exact_id_resend_deduplicates_across_the_conversion(
        self, broker: PostgresBroker
    ) -> None:
        send = make_send()
        first = await enqueue(broker, send)
        assert is_ok(first)
        resend = await enqueue(broker, send)
        assert is_ok(resend)
        assert resend.ok_value == send.task_id

        async with broker.session_factory() as session:
            count = (
                await session.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = :id'
                    ),
                    {'id': send.task_id},
                )
            ).scalar_one()
        assert count == 1

    async def test_same_id_different_fingerprint_is_a_payload_mismatch(
        self, broker: PostgresBroker
    ) -> None:
        send = make_send()
        first = await enqueue(broker, send)
        assert is_ok(first)
        tampered = Send(
            task_id=send.task_id,
            kwargs_json='{"x":2}',
            sent_at=send.sent_at,
            enqueue_sha=enqueue_fingerprint(
                task_name='cutover.enqueue',
                queue_name='default',
                priority=100,
                args_json=None,
                kwargs_json='{"x":2}',
                sent_at=send.sent_at,
                good_until=None,
                enqueue_delay_seconds=None,
                task_options=None,
            ),
        )
        conflicted = await enqueue(broker, tampered)
        assert is_err(conflicted)
        assert conflicted.err_value.code is BrokerErrorCode.PAYLOAD_MISMATCH
