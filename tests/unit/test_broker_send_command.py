# pyright: reportPrivateUsage=false
"""``PostgresBroker.prepare_send_command``: the stored values of a send, without a write.

The command holds the values that ``enqueue_async`` writes for the same
arguments, so a caller that writes its own rows (the schedule fire) stores
what a plain send stores. Refusals are those of the enqueue before its
write. Touches no database.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Final

import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker, SendCommand
from horsies.core.brokers.result_types import BrokerInvalidInput, BrokerInvalidTaskOptions
from horsies.core.models.broker import PostgresConfig
from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.tasks import Execution, RetryPolicy, TaskOptions
from horsies.core.types.result import Err, Ok
from horsies.core.history.identity.fingerprint import COMMAND_FINGERPRINT_VERSION

pytestmark = [pytest.mark.unit]

_URL: Final = 'postgresql://u:p@localhost:5432/db'

def _options_text() -> str:
    """The options text that the task decorator stores for a task with 4 retries."""
    match serialize_task_options(
        TaskOptions(
            task_name='tasks.report',
            retry_policy=RetryPolicy.fixed([5, 5, 5, 5], auto_retry_for=['E1']),
            execution=Execution.SHARED_ASYNC,
        ),
    ):
        case Ok(text):
            return text
        case Err(failed):
            raise AssertionError(f'the options do not encode: {failed}')


_OPTIONS: Final = _options_text()


def _broker(*, retain_rerun_input_default: bool = False) -> PostgresBroker:
    return PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(_URL),
            retain_rerun_input_default=retain_rerun_input_default,
        ),
    )


def _command(broker: PostgresBroker, **changes: object) -> SendCommand:
    arguments: dict[str, object] = {
        'task_name': 'tasks.report',
        'queue_name': 'default',
        'priority': 100,
        'kwargs_json': '{"value":1}',
        'task_options': _OPTIONS,
        'retention_class_key': 'standard_7d',
    }
    arguments.update(changes)
    match broker.prepare_send_command(**arguments):  # type: ignore[arg-type]
        case Ok(command):
            return command
        case Err(refused):
            raise AssertionError(f'the command was refused: {refused}')


def test_the_command_holds_the_values_that_an_enqueue_stores() -> None:
    broker = _broker()
    command = _command(broker)
    match PostgresBroker._prepare_enqueue(
        task_id='0195b4a0-0000-7000-8000-000000000001',
        task_name='tasks.report',
        queue_name='default',
        priority=100,
        input_json='{"value":1}',
        sent_at=datetime(2026, 1, 1, tzinfo=UTC),
        enqueued_at=None,
        enqueue_delay_seconds=None,
        good_until=None,
        task_options=_OPTIONS,
        retention_class_key='standard_7d',
        retain_rerun_input=False,
        idempotency_key=None,
        key_window=timedelta(days=1),
    ):
        case Ok(prepared):
            pass
        case Err(refused):
            raise AssertionError(f'the enqueue was refused: {refused}')

    row = prepared.row
    assert command == SendCommand(
        task_name=row.task_name,
        queue_name=row.queue_name,
        priority=row.priority,
        stored_input=row.stored_input,
        task_options=row.task_options,
        execution=row.execution,
        max_retries=row.max_retries,
        retention_class_key=row.retention_class_key,
        retain_rerun_input=row.retain_rerun_input,
        fingerprint_version=COMMAND_FINGERPRINT_VERSION,
        fingerprint=row.command_fingerprint,
    )
    assert (command.execution, command.max_retries) == (Execution.SHARED_ASYNC, 4)
    assert command.fingerprint_version == COMMAND_FINGERPRINT_VERSION


def test_no_kwargs_store_an_empty_object_and_no_options_store_no_retries() -> None:
    command = _command(_broker(), kwargs_json=None, task_options=None)

    assert (command.stored_input, command.task_options) == ('{}', None)
    assert (command.execution, command.max_retries) == (Execution.ISOLATED, 0)


def test_the_retention_class_and_the_rerun_input_posture_resolve_as_an_enqueue() -> None:
    forever = _command(_broker(retain_rerun_input_default=True), retention_class_key=None)

    assert (forever.retention_class_key, forever.retain_rerun_input) == ('forever', True)
    assert forever.fingerprint != _command(_broker()).fingerprint


def test_kwargs_that_are_not_an_object_are_refused_as_input() -> None:
    match _broker().prepare_send_command(
        task_name='tasks.report',
        queue_name='default',
        priority=100,
        kwargs_json='[1]',
        task_options=None,
    ):
        case Err(BrokerInvalidInput() as refused):
            assert refused.message == 'kwargs JSON for tasks.report must be an object or null, got list'
        case other:
            raise AssertionError(f'expected an input refusal, got {other!r}')


def test_options_that_do_not_parse_are_refused_as_task_options() -> None:
    match _broker().prepare_send_command(
        task_name='tasks.report',
        queue_name='default',
        priority=100,
        kwargs_json=None,
        task_options='{not json',
    ):
        case Err(BrokerInvalidTaskOptions() as refused):
            assert refused.message.startswith('task_options JSON corrupt: ')
        case other:
            raise AssertionError(f'expected a task options refusal, got {other!r}')
