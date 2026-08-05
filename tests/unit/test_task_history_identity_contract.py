"""Semantic gates shared by all three task-identity storage candidates."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from tests.task_history_prototypes.identity import (
    COMMAND_FINGERPRINT_VERSION,
    EnqueueCommandV1,
    ScopedIdempotencyKey,
)


def _command() -> EnqueueCommandV1:
    return EnqueueCommandV1(
        task_name='billing.capture',
        queue_name='payments',
        priority=10,
        args_json=None,
        kwargs_json='{"amount":42,"order_id":"order-1"}',
        good_until=datetime(2026, 8, 6, tzinfo=timezone.utc),
        enqueue_delay_seconds=None,
        task_options_json='{"retry_policy":{"max_retries":3}}',
        retention_class_key='finite_30d_v1',
        rerun_of_task_id=None,
        rerun_root_task_id=None,
    )


def test_key_is_opaque_case_sensitive_and_task_scoped() -> None:
    original = ScopedIdempotencyKey('billing.capture', ' Order-1 ')
    assert (
        original.digest == ScopedIdempotencyKey('billing.capture', ' Order-1 ').digest
    )
    assert original.digest != ScopedIdempotencyKey('billing.capture', 'order-1').digest
    assert original.digest != ScopedIdempotencyKey('billing.refund', ' Order-1 ').digest
    assert len(original.digest) == 32


@pytest.mark.parametrize('value', ['', 'x' * 256, 'é' * 128])
def test_key_enforces_nonempty_255_byte_bound(value: str) -> None:
    with pytest.raises(ValueError):
        ScopedIdempotencyKey('billing.capture', value)


def test_fingerprint_excludes_generated_task_identity_and_send_time() -> None:
    first = _command()
    replay = _command()
    assert first.fingerprint == replay.fingerprint
    assert len(first.fingerprint) == 32
    assert COMMAND_FINGERPRINT_VERSION == 1


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('queue_name', 'priority-payments'),
        ('priority', 11),
        ('kwargs_json', '{"amount":43,"order_id":"order-1"}'),
        ('good_until', datetime(2026, 8, 7, tzinfo=timezone.utc)),
        ('enqueue_delay_seconds', 5),
        ('task_options_json', '{"retry_policy":{"max_retries":4}}'),
        ('retention_class_key', 'forever'),
        ('rerun_of_task_id', 'source-task'),
    ],
)
def test_every_request_semantic_changes_fingerprint(field: str, value: object) -> None:
    original = _command()
    changes: dict[str, object] = {field: value}
    if field == 'rerun_of_task_id':
        changes['rerun_root_task_id'] = 'root-task'
    changed = replace(original, **changes)
    assert changed.fingerprint != original.fingerprint


def test_rerun_source_and_root_are_atomic_command_fields() -> None:
    with pytest.raises(ValueError, match='present together'):
        replace(_command(), rerun_of_task_id='source-task')
