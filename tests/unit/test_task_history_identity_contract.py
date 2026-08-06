"""Semantic gates shared by all three task-identity storage candidates."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from tests.task_history_prototypes import identity_schema
from tests.task_history_prototypes.identity import (
    CANDIDATE_IDEMPOTENCY_WINDOW_DEFAULT,
    CANDIDATE_IDEMPOTENCY_WINDOW_MAX,
    COMMAND_FINGERPRINT_VERSION,
    EnqueueCommandV1,
    ScopedIdempotencyKey,
)
from tests.task_history_prototypes.uuid7 import uuid7_birth_at, uuid7_for_row


def test_candidate_idempotency_window_bounds_are_explicit() -> None:
    assert CANDIDATE_IDEMPOTENCY_WINDOW_DEFAULT.total_seconds() == 24 * 60 * 60
    assert CANDIDATE_IDEMPOTENCY_WINDOW_MAX.total_seconds() == 30 * 24 * 60 * 60


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


def test_staged_lookup_renders_all_512_static_leaf_probes() -> None:
    leaves = tuple(
        f'key_registry_history_finite_{year}'
        for year in range(2537, 2025, -1)
    )
    rendered = identity_schema.render_staged_lookup_prototype(
        'prototype_schema',
        'key_registry',
        leaves,
    )
    assert rendered.count('key_registry_history_finite_') == 512
    assert rendered.index(leaves[0]) < rendered.index(leaves[-1])
    assert len(rendered.encode()) == 223_525
    assert 'EXECUTE' not in rendered


@pytest.mark.parametrize(
    ('prefix', 'leaves', 'message'),
    [
        ('combined', ('combined_history_finite_2026',), 'non-directory'),
        ('key_registry', (), 'at least one'),
        (
            'key_registry',
            (
                'key_registry_history_finite_2026',
                'key_registry_history_finite_2026',
            ),
            'distinct',
        ),
        ('key_registry', ('key_registry_history_finite_2026;drop',), 'invalid'),
        ('key_registry', ('no_directory_history_finite_2026',), 'invalid'),
    ],
)
def test_staged_lookup_rejects_ambiguous_or_unsafe_manifests(
    prefix: str,
    leaves: tuple[str, ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        identity_schema.render_staged_lookup_prototype(
            'prototype_schema',
            prefix,
            leaves,
        )


def _uuid7_leaves() -> tuple[identity_schema.FiniteLookupLeaf, ...]:
    boundaries = tuple(
        datetime(year, 1, 1, tzinfo=timezone.utc)
        for year in range(2026, 2030)
    )
    return tuple(
        identity_schema.FiniteLookupLeaf(
            relation_name=f'key_registry_history_finite_{lower.year}',
            lower_bound=lower,
            upper_bound=upper,
        )
        for lower, upper in zip(
            boundaries[:-1],
            boundaries[1:],
            strict=True,
        )
    )


def test_uuid7_helper_is_deterministic_and_millisecond_exact() -> None:
    birth_at = datetime(
        2026,
        8,
        6,
        12,
        34,
        56,
        789123,
        tzinfo=timezone.utc,
    )
    first = uuid7_for_row(birth_at, domain='live', row_number=1)
    replay = uuid7_for_row(birth_at, domain='live', row_number=1)
    other = uuid7_for_row(birth_at, domain='live', row_number=2)
    assert first == replay
    assert first != other
    assert UUID(first).version == 7
    assert uuid7_birth_at(first) == birth_at.replace(microsecond=789000)
    assert uuid7_birth_at(uuid4()) is None


def test_uuid7_helper_is_monotonic_within_one_millisecond() -> None:
    birth_at = datetime(2026, 8, 6, 12, 30, 45, 123000, tzinfo=timezone.utc)
    values = tuple(
        UUID(
            uuid7_for_row(
                birth_at,
                domain='same-millisecond',
                row_number=sequence + 1,
                sequence_within_millisecond=sequence,
            )
        )
        for sequence in range(1_000)
    )
    assert tuple(sorted(values)) == values
    assert {uuid7_birth_at(value) for value in values} == {birth_at}


def test_uuid7_staged_lookup_prunes_pre_birth_leaves_and_keeps_v4_fallback() -> None:
    leaves = _uuid7_leaves()
    rendered = identity_schema.render_uuid7_staged_lookup_prototype(
        'prototype_schema',
        'key_registry',
        leaves,
        maximum_request_lifetime=None,
    )
    uuid7_branch, legacy_branch = rendered.split('        ELSE\n', maxsplit=1)
    assert 'uuid_send(v_task_uuid)' in uuid7_branch
    assert '(get_byte(v_uuid_bytes, 6) >> 4) = 7' in uuid7_branch
    assert 'v_latest_terminal_at := NULL' in uuid7_branch
    assert uuid7_branch.index(leaves[0].relation_name) < uuid7_branch.index(
        leaves[-1].relation_name
    )
    assert legacy_branch.index(leaves[-1].relation_name) < legacy_branch.index(
        leaves[0].relation_name
    )
    for leaf in leaves:
        assert rendered.count(leaf.relation_name) == 2
    assert 'EXECUTE' not in rendered


def test_uuid7_bounded_lookup_has_constant_time_pre_retention_miss() -> None:
    leaves = _uuid7_leaves()
    rendered = identity_schema.render_uuid7_staged_lookup_prototype(
        'prototype_schema',
        'key_registry',
        leaves,
        maximum_request_lifetime=timedelta(days=30),
    )
    first_probe = rendered.index(leaves[0].relation_name)
    early_return = rendered.index(
        'IF v_latest_terminal_at < '
        "TIMESTAMPTZ '2026-01-01T00:00:00Z'"
    )
    assert early_return < first_probe
    assert 'make_interval(secs => 2592000.0)' in rendered
    assert (
        'v_latest_terminal_at >= '
        "TIMESTAMPTZ '2026-01-01T00:00:00Z'"
    ) in rendered


@pytest.mark.parametrize(
    ('leaves', 'message'),
    [
        ((), 'at least one'),
        (
            (
                identity_schema.FiniteLookupLeaf(
                    'key_registry_history_finite_2026',
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                    datetime(2027, 1, 1, tzinfo=timezone.utc),
                ),
                identity_schema.FiniteLookupLeaf(
                    'key_registry_history_finite_2028',
                    datetime(2028, 1, 1, tzinfo=timezone.utc),
                    datetime(2029, 1, 1, tzinfo=timezone.utc),
                ),
            ),
            'contiguous',
        ),
    ],
)
def test_uuid7_staged_lookup_rejects_incomplete_manifests(
    leaves: tuple[identity_schema.FiniteLookupLeaf, ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        identity_schema.render_uuid7_staged_lookup_prototype(
            'prototype_schema',
            'key_registry',
            leaves,
            maximum_request_lifetime=None,
        )
