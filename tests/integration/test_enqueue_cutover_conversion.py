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
from uuid import uuid4

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
        # The literal, not DEFAULT_RETENTION_CLASS_KEY: an enqueue that
        # names no class must land in a FINITE class. Importing the
        # constant would track a change back to forever instead of
        # failing on it, and a forever default exempts every
        # unconfigured task from retention.
        assert row.retention_class_key == 'standard_30d'
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


async def enqueue_keyed(
    broker: PostgresBroker, send: Send, key: str
) -> BrokerResult[str]:
    return await broker.enqueue_async(
        'cutover.enqueue',
        task_id=send.task_id,
        enqueue_sha=send.enqueue_sha,
        kwargs_json=send.kwargs_json,
        sent_at=send.sent_at,
        idempotency_key=key,
    )


class TestKeyedEnqueue:
    async def test_keyed_enqueue_reserves_and_inserts_in_one_commit(
        self, broker: PostgresBroker
    ) -> None:
        send = make_send()
        key = f'order-batch-{uuid4()}'
        result = await enqueue_keyed(broker, send, key)
        assert is_ok(result)
        async with broker.session_factory() as session:
            row = (
                await session.execute(
                    text(
                        'SELECT r.disposition, r.task_id, '
                        't.idempotency_key_digest '
                        'FROM horsies_key_reservations r '
                        'JOIN horsies_tasks t '
                        'ON t.id = r.task_id '
                        'WHERE r.task_id = CAST(:id AS uuid)'
                    ),
                    {'id': send.task_id},
                )
            ).one()
        assert row.disposition == 'LIVE'
        assert row.idempotency_key_digest is not None

    async def test_keyed_resend_with_a_fresh_id_replays_the_committed(
        self, broker: PostgresBroker
    ) -> None:
        """The uncertain-commit shape for keyed producers: the ack was
        lost, the client minted a NEW id for the resend, and the same
        key + same canonical command must resolve to the committed
        request — same result shape as a fresh success."""
        first = make_send()
        key = f'order-batch-{uuid4()}'
        committed = await enqueue_keyed(broker, first, key)
        assert is_ok(committed)
        resend = Send(
            task_id=mint_task_id(),
            enqueue_sha=first.enqueue_sha,
            kwargs_json=first.kwargs_json,
            sent_at=first.sent_at,
        )
        replayed = await enqueue_keyed(broker, resend, key)
        assert is_ok(replayed)
        assert replayed.ok_value == first.task_id
        async with broker.session_factory() as session:
            count = (
                await session.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id IN (:a, :b)'
                    ),
                    {'a': first.task_id, 'b': resend.task_id},
                )
            ).scalar_one()
        assert count == 1

    async def test_same_key_different_command_conflicts_with_zero_rows(
        self, broker: PostgresBroker
    ) -> None:
        first = make_send()
        key = f'order-batch-{uuid4()}'
        committed = await enqueue_keyed(broker, first, key)
        assert is_ok(committed)
        different = make_send(kwargs_json='{"x":99}')
        conflicted = await enqueue_keyed(broker, different, key)
        assert is_err(conflicted)
        assert (
            conflicted.err_value.code
            is BrokerErrorCode.IDEMPOTENCY_KEY_CONFLICT
        )
        async with broker.session_factory() as session:
            orphan = (
                await session.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = :id'
                    ),
                    {'id': different.task_id},
                )
            ).scalar_one()
        assert orphan == 0

    async def test_registry_indexes_are_present_since_emission(
        self, broker: PostgresBroker
    ) -> None:
        """The v28 deliberate absence ended when the emission migration
        landed the maintenance indexes: the recorded sequencing debt is
        discharged, and this pin flips from absence to presence — the
        inventory flip becoming a permanent structural invariant."""
        from horsies.core.history.ddl.conditional import (
            RESERVATION_REGISTRY_EXPIRY_INDEX,
        )

        async with broker.session_factory() as session:
            index_names = {
                row.indexname
                for row in (
                    await session.execute(
                        text(
                            'SELECT indexname FROM pg_indexes '
                            "WHERE tablename = 'horsies_key_reservations'"
                        )
                    )
                ).all()
            }
        assert index_names == {
            'horsies_key_reservations_pkey',
            RESERVATION_REGISTRY_EXPIRY_INDEX,
        }


class TestReservationWindowConfiguration:
    def test_boundary_values_are_legal_and_excess_is_typed(self) -> None:
        from datetime import timedelta

        from horsies.core.history.identity.keys import (
            validate_reservation_window,
        )

        assert validate_reservation_window(timedelta(days=30)) == (
            timedelta(days=30)
        )
        assert validate_reservation_window(timedelta(seconds=1)) == (
            timedelta(seconds=1)
        )
        with pytest.raises(ValueError, match='positive'):
            validate_reservation_window(timedelta(0))
        with pytest.raises(ValueError, match='inclusive maximum'):
            validate_reservation_window(
                timedelta(days=30, microseconds=1)
            )
