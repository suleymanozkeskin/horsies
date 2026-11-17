"""The enqueue statement: its parameters and the outcomes it answers (``horsies.core.brokers.enqueue_task``)."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from horsies.core.brokers.enqueue_task import (
    ENQUEUE_TASK_DRIVER_SQL,
    ENQUEUE_TASK_SQL,
    ConflictUnverifiable,
    EnqueueKey,
    EnqueueOutcome,
    EnqueueRow,
    EnqueueLineage,
    EnqueueStart,
    FirstRun,
    Inserted,
    KeyConflict,
    Keyed,
    KeyReplay,
    PayloadMismatch,
    RerunOf,
    SameCommand,
    SendKind,
    StartAfterDelay,
    StartAt,
    StartNow,
    Unkeyed,
    enqueue_result,
    enqueue_task_params,
    read_enqueue_outcome,
)
from horsies.core.brokers.result_types import (
    BrokerEnqueueFailed,
    BrokerIdempotencyKeyConflict,
    BrokerPayloadMismatch,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]

_AT = datetime(2030, 1, 1, tzinfo=timezone.utc)
_DIGEST = b'\x02' * 32
_SOURCE = uuid.UUID('0190f0f0-0000-7000-8000-0000000000a1')
_ROOT = uuid.UUID('0190f0f0-0000-7000-8000-0000000000a0')


def _row(
    start: EnqueueStart = StartNow(),
    key: EnqueueKey = Unkeyed(),
    lineage: EnqueueLineage = FirstRun(),
) -> EnqueueRow:
    return EnqueueRow(
        task_id=uuid.UUID('0190f0f0-0000-7000-8000-000000000001'),
        task_name='probe',
        queue_name='default',
        priority=100,
        stored_input='{}',
        task_options=None,
        execution=Execution.SHARED_THREAD,
        retention_class_key='standard_30d',
        retain_rerun_input=False,
        max_retries=3,
        good_until=None,
        sent_at=_AT,
        start=start,
        command_fingerprint=b'\x01' * 32,
        key=key,
        lineage=lineage,
    )


class TestParameters:
    def test_the_parameters_are_exactly_the_statement_binds(self) -> None:
        params = enqueue_task_params(_row(), SendKind.FIRST)

        assert set(params) == set(ENQUEUE_TASK_SQL.compile().params)

    def test_the_driver_statement_binds_the_same_parameters(self) -> None:
        driver_binds = re.findall(r'%\((\w+)\)s', ENQUEUE_TASK_DRIVER_SQL)

        assert set(driver_binds) == set(ENQUEUE_TASK_SQL.compile().params)
        assert ENQUEUE_TASK_DRIVER_SQL.count('%') == len(driver_binds)

    @pytest.mark.parametrize(
        ('start', 'expected'),
        [
            (StartNow(), ('NOW', None, None)),
            (StartAt(at=_AT), ('AT', _AT, None)),
            (StartAfterDelay(delay_s=30), ('AFTER_DELAY', None, 30)),
        ],
        ids=['now', 'at', 'after_delay'],
    )
    def test_each_start_fills_only_its_own_slot(
        self, start: EnqueueStart, expected: tuple[str, datetime | None, int | None],
    ) -> None:
        params = enqueue_task_params(_row(start=start), SendKind.FIRST)

        assert (params['start_kind'], params['start_at'], params['start_delay_s']) == expected

    @pytest.mark.parametrize(
        ('key', 'expected'),
        [
            (Unkeyed(), ('UNKEYED', None, None, None)),
            (
                Keyed(digest=_DIGEST, scope_version=1, window=timedelta(seconds=1.5)),
                ('KEYED', _DIGEST, 1, 1_500_000),
            ),
        ],
        ids=['unkeyed', 'keyed'],
    )
    def test_each_key_kind_fills_only_its_own_slots(
        self, key: EnqueueKey, expected: tuple[str, bytes | None, int | None, int | None],
    ) -> None:
        params = enqueue_task_params(_row(key=key), SendKind.FIRST)

        assert (
            params['key_kind'],
            params['key_digest'],
            params['key_scope_version'],
            params['key_window_us'],
        ) == expected

    @pytest.mark.parametrize(
        ('lineage', 'expected'),
        [
            (FirstRun(), ('FIRST_RUN', None, None)),
            (
                RerunOf(source_task_id=_SOURCE, root_task_id=_ROOT),
                ('RERUN', str(_SOURCE), str(_ROOT)),
            ),
        ],
        ids=['first_run', 'rerun'],
    )
    def test_each_lineage_fills_only_its_own_slots(
        self, lineage: EnqueueLineage, expected: tuple[str, str | None, str | None],
    ) -> None:
        params = enqueue_task_params(_row(lineage=lineage), SendKind.FIRST)

        assert (
            params['lineage_kind'],
            params['rerun_of_task_id'],
            params['rerun_root_task_id'],
        ) == expected

    @pytest.mark.parametrize('send_kind', [SendKind.FIRST, SendKind.REPEAT])
    def test_the_send_kind_binds_its_value(self, send_kind: SendKind) -> None:
        assert enqueue_task_params(_row(), send_kind)['send_kind'] == send_kind.value

    def test_the_row_values_bind_as_the_row_stores_them(self) -> None:
        params = enqueue_task_params(_row(), SendKind.FIRST)

        assert params['execution'] == 'SHARED_THREAD'
        assert params['fingerprint_version'] == 1
        assert params['fingerprint'] == b'\x01' * 32
        assert params['max_retries'] == 3
        assert params['sent_at'] == _AT


_ID = uuid.UUID('0190f0f0-0000-7000-8000-000000000002')
_SENT = uuid.UUID('0190f0f0-0000-7000-8000-000000000003')


class TestOutcomeRows:
    @pytest.mark.parametrize(
        ('outcome', 'expected'),
        [
            ('INSERTED', Inserted()),
            ('SAME_COMMAND', SameCommand()),
            ('KEY_REPLAY', KeyReplay(task_id=_ID)),
            ('KEY_CONFLICT', KeyConflict(owner_task_id=_ID)),
            ('PAYLOAD_MISMATCH', PayloadMismatch()),
            ('CONFLICT_UNVERIFIABLE', ConflictUnverifiable()),
        ],
    )
    def test_each_outcome_reads_as_its_variant(
        self, outcome: str, expected: EnqueueOutcome,
    ) -> None:
        assert read_enqueue_outcome([(outcome, _ID)]) == expected

    def test_an_id_in_text_form_reads_the_same(self) -> None:
        assert read_enqueue_outcome([('KEY_REPLAY', str(_ID))]) == KeyReplay(task_id=_ID)

    def test_a_holder_id_text_that_is_not_a_uuid_names_the_column(self) -> None:
        with pytest.raises(ValueError, match='horsies_enqueue_task.task_id'):
            read_enqueue_outcome([('KEY_CONFLICT', 'not-a-uuid')])

    @pytest.mark.parametrize(
        'rows',
        [
            [],
            [('INSERTED', _ID), ('INSERTED', _ID)],
            [('INSERTED', None)],
            [('UNKNOWN', _ID)],
        ],
        ids=['no_row', 'two_rows', 'no_id', 'unknown_outcome'],
    )
    def test_rows_outside_the_contract_are_a_defect(self, rows: list[tuple[Any, ...]]) -> None:
        with pytest.raises(AssertionError, match='horsies_enqueue_task'):
            read_enqueue_outcome(rows)


class TestResults:
    @pytest.mark.parametrize('outcome', [Inserted(), SameCommand()])
    def test_the_sent_id_holds_the_send(self, outcome: EnqueueOutcome) -> None:
        assert enqueue_result(outcome, task_id=_SENT, task_name='probe') == Ok(_SENT)

    def test_a_key_replay_answers_the_id_that_holds_the_key(self) -> None:
        result = enqueue_result(KeyReplay(task_id=_ID), task_id=_SENT, task_name='probe')

        assert result == Ok(_ID)

    def test_a_key_conflict_names_the_owner(self) -> None:
        match enqueue_result(KeyConflict(owner_task_id=_ID), task_id=_SENT, task_name='probe'):
            case Err(BrokerIdempotencyKeyConflict(owner_task_id=owner)) if owner == _ID:
                pass
            case other:
                raise AssertionError(f'expected a key conflict, got {other!r}')

    def test_a_payload_mismatch_names_the_task_id(self) -> None:
        match enqueue_result(PayloadMismatch(), task_id=_SENT, task_name='probe'):
            case Err(BrokerPayloadMismatch(message=message)):
                assert str(_SENT) in message
            case other:
                raise AssertionError(f'expected a payload mismatch, got {other!r}')

    def test_an_unverifiable_conflict_is_a_failure_that_is_not_retryable(self) -> None:
        match enqueue_result(ConflictUnverifiable(), task_id=_SENT, task_name='probe'):
            case Err(BrokerEnqueueFailed(retryable=False, exception=NoCause(), message=message)):
                assert 'cannot verify payload identity' in message
            case other:
                raise AssertionError(f'expected an enqueue failure, got {other!r}')
