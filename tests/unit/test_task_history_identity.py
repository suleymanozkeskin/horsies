"""Identity primitives: minting monotonicity, fingerprint v1, scoped keys.

The digest pins are wire-stability anchors: stored fingerprints and key
digests are compared byte-for-byte forever, so an accidental change to the
canonical form or the framing must fail here before it can ship. Field
enumeration keeps fingerprint v1 honest — a new field has to be accounted
for, and accounting for it means declaring version 2.
"""

from __future__ import annotations

import dataclasses
import threading
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from horsies.core.history.identity.fingerprint import (
    COMMAND_FINGERPRINT_VERSION,
    EnqueueCommandV1,
)
from horsies.core.history.identity.keys import (
    IDEMPOTENCY_KEY_MAX_BYTES,
    IDEMPOTENCY_WINDOW_DEFAULT,
    IDEMPOTENCY_WINDOW_MAX,
    ScopedIdempotencyKey,
    validate_reservation_window,
)
from horsies.core.history.identity.uuid7 import (
    MonotonicUuid7Generator,
    mint_task_id,
    uuid7_birth_at,
)

pytestmark = [pytest.mark.unit]


UTC = timezone.utc


class SteppingClock:
    """A millisecond clock that advances only when told to."""

    def __init__(self, start_ms: int) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms


def make_generator(
    clock: SteppingClock,
    *,
    entropy: int = (1 << 61) | 5,
) -> MonotonicUuid7Generator:
    return MonotonicUuid7Generator(
        clock_ms=clock,
        entropy_62_bits=lambda: entropy,
    )


class TestUuid7Minting:
    def test_mints_canonical_lowercase_version_7(self) -> None:
        clock = SteppingClock(1_754_000_000_000)
        minted = make_generator(clock).mint()
        assert minted == minted.lower()
        parsed = UUID(minted)
        assert parsed.version == 7
        assert parsed.variant == 'specified in RFC 4122'

    def test_birth_decode_roundtrip(self) -> None:
        clock = SteppingClock(1_754_000_000_123)
        minted = make_generator(clock).mint()
        assert uuid7_birth_at(minted) == datetime.fromtimestamp(
            1_754_000_000.123, tz=UTC
        )

    def test_non_v7_birth_decode_returns_none(self) -> None:
        assert uuid7_birth_at('550e8400-e29b-41d4-a716-446655440000') is None

    def test_within_millisecond_ids_strictly_increase(self) -> None:
        clock = SteppingClock(1_754_000_000_000)
        generator = make_generator(clock)
        minted = [generator.mint() for _ in range(100)]
        assert minted == sorted(minted)
        assert len(set(minted)) == 100

    def test_new_millisecond_resets_the_counter(self) -> None:
        clock = SteppingClock(1_754_000_000_000)
        generator = make_generator(clock)
        first = UUID(generator.mint())
        second = UUID(generator.mint())
        clock.now_ms += 1
        third = UUID(generator.mint())
        def counter_of(value: UUID) -> int:
            return (value.int >> 64) & 0xFFF

        assert counter_of(first) == 0
        assert counter_of(second) == 1
        assert counter_of(third) == 0
        assert str(second) < str(third)

    def test_counter_exhaustion_waits_for_the_clock(self) -> None:
        class AdvancingClock:
            def __init__(self) -> None:
                self.now_ms = 1_754_000_000_000
                self.reads_after_exhaustion = 0
                self.exhausted = False

            def __call__(self) -> int:
                if self.exhausted:
                    self.reads_after_exhaustion += 1
                    if self.reads_after_exhaustion >= 3:
                        return self.now_ms + 1
                return self.now_ms

        clock = AdvancingClock()
        generator = MonotonicUuid7Generator(
            clock_ms=clock,
            entropy_62_bits=lambda: 5,
        )
        minted = [generator.mint() for _ in range(4096)]
        clock.exhausted = True
        overflow = generator.mint()
        assert clock.reads_after_exhaustion >= 3
        assert minted == sorted(minted)
        assert minted[-1] < overflow
        assert (UUID(overflow).int >> 64) & 0xFFF == 0

    def test_clock_regression_is_absorbed_monotonically(self) -> None:
        clock = SteppingClock(1_754_000_000_500)
        generator = make_generator(clock)
        before = generator.mint()
        clock.now_ms -= 200
        after = generator.mint()
        assert before < after
        assert uuid7_birth_at(after) == uuid7_birth_at(before)

    def test_concurrent_minting_is_unique_and_locally_monotonic(self) -> None:
        generator = MonotonicUuid7Generator()
        per_thread: dict[int, list[str]] = {}

        def mint_many(thread_index: int) -> None:
            per_thread[thread_index] = [generator.mint() for _ in range(500)]

        threads = [
            threading.Thread(target=mint_many, args=(index,)) for index in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        combined = [minted for sequence in per_thread.values() for minted in sequence]
        assert len(set(combined)) == 2_000
        for sequence in per_thread.values():
            assert sequence == sorted(sequence)

    def test_process_generator_mints_valid_ids(self) -> None:
        assert UUID(mint_task_id()).version == 7

    def test_rejects_out_of_range_clock(self) -> None:
        clock = SteppingClock(1 << 48)
        with pytest.raises(ValueError, match='48-bit'):
            make_generator(clock).mint()


def make_command(**overrides: object) -> EnqueueCommandV1:
    values: dict[str, object] = {
        'task_name': 'billing.charge',
        'queue_name': 'default',
        'priority': 50,
        'args_json': '[1,2]',
        'kwargs_json': '{"a":true}',
        'good_until': datetime(2026, 8, 7, 12, 0, tzinfo=UTC),
        'enqueue_delay_seconds': 30,
        'task_options_json': '{"timeout_ms":1000}',
        'retention_class_key': 'finite_30d_v1',
        'retain_rerun_input': True,
        'rerun_of_task_id': None,
        'rerun_root_task_id': None,
    }
    values.update(overrides)
    return EnqueueCommandV1(**values)  # type: ignore[arg-type]


class TestCommandFingerprint:
    def test_version_is_one(self) -> None:
        assert COMMAND_FINGERPRINT_VERSION == 1

    def test_pinned_digest(self) -> None:
        """Wire-stability anchor: this digest may only change with version 2."""
        assert make_command().fingerprint.hex() == (
            '90c4323848b3880935ff136aa7e86dcfeb6509f25425ead6e2703843f129ac2f'
        )

    def test_every_field_is_accounted_for(self) -> None:
        assert {field.name for field in dataclasses.fields(EnqueueCommandV1)} == {
            'task_name',
            'queue_name',
            'priority',
            'args_json',
            'kwargs_json',
            'good_until',
            'enqueue_delay_seconds',
            'task_options_json',
            'retention_class_key',
            'retain_rerun_input',
            'rerun_of_task_id',
            'rerun_root_task_id',
        }

    @pytest.mark.parametrize(
        'overrides',
        [
            {'task_name': 'billing.refund'},
            {'queue_name': 'bulk'},
            {'priority': 51},
            {'args_json': '[1,3]'},
            {'args_json': None},
            {'kwargs_json': '{"a":false}'},
            {'good_until': datetime(2026, 8, 7, 12, 1, tzinfo=UTC)},
            {'good_until': None},
            {'enqueue_delay_seconds': 31},
            {'task_options_json': '{"timeout_ms":2000}'},
            {'retention_class_key': 'finite_7d_v1'},
            {'retain_rerun_input': False},
            {
                'rerun_of_task_id': '0198c0de-0000-7000-8000-000000000001',
                'rerun_root_task_id': '0198c0de-0000-7000-8000-000000000001',
            },
        ],
    )
    def test_every_field_changes_the_fingerprint(
        self, overrides: dict[str, object]
    ) -> None:
        assert make_command(**overrides).fingerprint != make_command().fingerprint

    def test_good_until_timezone_is_canonicalized(self) -> None:
        offset = timezone(timedelta(hours=2))
        same_instant = make_command(
            good_until=datetime(2026, 8, 7, 14, 0, tzinfo=offset)
        )
        assert same_instant.fingerprint == make_command().fingerprint

    def test_rejects_naive_good_until(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            make_command(good_until=datetime(2026, 8, 7, 12, 0))

    @pytest.mark.parametrize('priority', [0, 101])
    def test_rejects_out_of_range_priority(self, priority: int) -> None:
        with pytest.raises(ValueError, match='between 1 and 100'):
            make_command(priority=priority)

    def test_rejects_unpaired_rerun_lineage(self) -> None:
        with pytest.raises(ValueError, match='present together'):
            make_command(
                rerun_of_task_id='0198c0de-0000-7000-8000-000000000001',
            )

    def test_rejects_negative_delay(self) -> None:
        with pytest.raises(ValueError, match='non-negative'):
            make_command(enqueue_delay_seconds=-1)

    def test_zero_delay_is_valid(self) -> None:
        assert make_command(enqueue_delay_seconds=0).fingerprint


class TestScopedIdempotencyKey:
    def test_pinned_digest(self) -> None:
        """Wire-stability anchor: framing or domain changes must fail here."""
        key = ScopedIdempotencyKey(task_name='billing.charge', key='order-42')
        assert key.digest.hex() == (
            '1c4938c21057eb227c1cc32e7e01cb3d41718379448e2cd86ede36371c3c1bb5'
        )

    def test_keys_are_case_sensitive_and_untrimmed(self) -> None:
        base = ScopedIdempotencyKey(task_name='billing.charge', key='order-42')
        assert (
            ScopedIdempotencyKey(task_name='billing.charge', key='ORDER-42').digest
            != base.digest
        )
        assert (
            ScopedIdempotencyKey(task_name='billing.charge', key=' order-42').digest
            != base.digest
        )

    def test_framing_prevents_boundary_shifts(self) -> None:
        assert (
            ScopedIdempotencyKey(task_name='ab', key='c').digest
            != ScopedIdempotencyKey(task_name='a', key='bc').digest
        )

    def test_accepts_maximum_key_bytes(self) -> None:
        key = ScopedIdempotencyKey(
            task_name='billing.charge', key='k' * IDEMPOTENCY_KEY_MAX_BYTES
        )
        assert len(key.digest) == 32

    def test_rejects_over_bound_utf8_bytes(self) -> None:
        with pytest.raises(ValueError, match='255 UTF-8 bytes'):
            ScopedIdempotencyKey(task_name='billing.charge', key='ü' * 128)

    def test_rejects_empty_values(self) -> None:
        with pytest.raises(ValueError, match='non-empty'):
            ScopedIdempotencyKey(task_name='', key='order-42')
        with pytest.raises(ValueError, match='non-empty'):
            ScopedIdempotencyKey(task_name='billing.charge', key='')


class TestReservationWindow:
    def test_default_is_within_bounds(self) -> None:
        assert (
            validate_reservation_window(IDEMPOTENCY_WINDOW_DEFAULT)
            == IDEMPOTENCY_WINDOW_DEFAULT
        )

    def test_maximum_is_inclusive(self) -> None:
        assert (
            validate_reservation_window(IDEMPOTENCY_WINDOW_MAX)
            == IDEMPOTENCY_WINDOW_MAX
        )

    @pytest.mark.parametrize(
        'window',
        [timedelta(0), timedelta(seconds=-1)],
    )
    def test_rejects_non_positive_windows(self, window: timedelta) -> None:
        with pytest.raises(ValueError, match='positive'):
            validate_reservation_window(window)

    def test_rejects_above_maximum(self) -> None:
        with pytest.raises(ValueError, match='inclusive maximum'):
            validate_reservation_window(IDEMPOTENCY_WINDOW_MAX + timedelta(seconds=1))
