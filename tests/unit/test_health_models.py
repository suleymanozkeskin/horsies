"""Unit tests for the worker/database health wire payloads and result models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from horsies.core.codec.json_io import dumps_json, loads_json
from datetime import datetime, timedelta, timezone

from horsies.core.models.boot_failure import (
    AppLoadFailed,
    ClearableBootCause,
    ResourceShortage,
    ShortageErrno,
    SpawnFailed,
    TaskLoopCreateFailed,
    ThreadStartRefused,
)
from horsies.core.models.health import (
    WORKER_PING_CHANNEL,
    AllSlotsServing,
    DatabasePing,
    DownSlot,
    SlotHealthNotRecorded,
    SlotsDown,
    WorkerPingRequest,
    WorkerPong,
    WorkerPongPayload,
    WorkerStateSnapshot,
    parse_slot_health,
    slots_down_json,
)
from horsies.core.types.result import Err, Ok, is_ok


@pytest.mark.unit
class TestWorkerPingRequest:
    """Ping request payload: wire round-trip and strict validation."""

    def test_roundtrip_through_json_io(self) -> None:
        """A request survives encode → decode → re-validate unchanged."""
        req = WorkerPingRequest(
            correlation_id='abc123',
            reply_channel='horsies_worker_pong_abc123',
            target_worker_id='worker-7',
        )
        encoded = dumps_json(req.model_dump())
        assert is_ok(encoded)

        decoded = loads_json(encoded.ok_value)
        assert is_ok(decoded)
        restored = WorkerPingRequest.model_validate(decoded.ok_value)
        assert restored == req

    def test_target_defaults_to_broadcast(self) -> None:
        """Omitting target_worker_id yields None (broadcast)."""
        req = WorkerPingRequest(correlation_id='c', reply_channel='r')
        assert req.target_worker_id is None

    def test_rejects_extra_fields(self) -> None:
        """Unknown keys are rejected (extra='forbid')."""
        with pytest.raises(ValidationError):
            WorkerPingRequest.model_validate(
                {
                    'correlation_id': 'c',
                    'reply_channel': 'r',
                    'target_worker_id': None,
                    'rogue': 1,
                }
            )

    def test_rejects_missing_required(self) -> None:
        """Missing reply_channel is rejected."""
        with pytest.raises(ValidationError):
            WorkerPingRequest.model_validate({'correlation_id': 'c'})

    def test_is_frozen(self) -> None:
        """Request payloads are immutable."""
        req = WorkerPingRequest(correlation_id='c', reply_channel='r')
        with pytest.raises(ValidationError):
            req.correlation_id = 'other'  # type: ignore[misc]


@pytest.mark.unit
class TestWorkerPongPayload:
    """Pong reply payload: wire round-trip and strict validation."""

    def test_roundtrip_through_json_io(self) -> None:
        pong = WorkerPongPayload(
            correlation_id='abc', worker_id='w1', hostname='host', pid=999
        )
        encoded = dumps_json(pong.model_dump())
        assert is_ok(encoded)
        decoded = loads_json(encoded.ok_value)
        assert is_ok(decoded)
        assert WorkerPongPayload.model_validate(decoded.ok_value) == pong

    def test_rejects_extra_fields(self) -> None:
        with pytest.raises(ValidationError):
            WorkerPongPayload.model_validate(
                {
                    'correlation_id': 'c',
                    'worker_id': 'w',
                    'hostname': 'h',
                    'pid': 1,
                    'extra': True,
                }
            )

    def test_rejects_non_integer_pid(self) -> None:
        """pid must be an int; a non-numeric string is rejected."""
        with pytest.raises(ValidationError):
            WorkerPongPayload.model_validate(
                {
                    'correlation_id': 'c',
                    'worker_id': 'w',
                    'hostname': 'h',
                    'pid': 'not-a-pid',
                }
            )


@pytest.mark.unit
class TestResultModels:
    """Frozen result dataclasses returned to callers."""

    def test_channel_constant(self) -> None:
        """The shared ping channel name is stable."""
        assert WORKER_PING_CHANNEL == 'horsies_worker_ping'

    def test_database_ping_is_frozen(self) -> None:
        ping = DatabasePing(latency_ms=1.5)
        with pytest.raises(AttributeError):
            ping.latency_ms = 2.0  # type: ignore[misc]

    def test_worker_pong_is_frozen(self) -> None:
        pong = WorkerPong(worker_id='w', hostname='h', pid=1, round_trip_ms=3.0)
        with pytest.raises(AttributeError):
            pong.worker_id = 'other'  # type: ignore[misc]

    def test_worker_state_snapshot_holds_optional_fields(self) -> None:
        """Snapshot tolerates NULL psutil/config columns as None."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        snap = WorkerStateSnapshot(
            worker_id='w',
            snapshot_at=now,
            hostname='h',
            pid=1,
            processes=4,
            max_claim_batch=10,
            max_claim_per_worker=4,
            cluster_wide_cap=None,
            queues=['default'],
            queue_priorities=None,
            queue_max_concurrency=None,
            recovery_config=None,
            tasks_running=0,
            tasks_claimed=0,
            memory_usage_mb=None,
            memory_percent=None,
            cpu_percent=None,
            children_memory_mb=None,
            worker_started_at=now,
            slot_health=SlotHealthNotRecorded(),
        )
        assert snap.cluster_wide_cap is None
        assert snap.queues == ['default']
        assert snap.tasks_running == 0


def _down_slot(slot: int) -> DownSlot:
    return DownSlot(
        slot=slot,
        cause=ResourceShortage(ShortageErrno.EMFILE),
        detail='OSError: [Errno 24] Too many open files',
        down_since=datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2))),
        failed_boots=3,
    )


@pytest.mark.unit
class TestSlotHealth:
    """The stored ``slots_down`` value and the typed slot health."""

    def test_a_null_value_is_not_recorded(self) -> None:
        assert parse_slot_health(None, 4) == Ok(SlotHealthNotRecorded())

    def test_an_empty_list_means_every_slot_serves(self) -> None:
        assert parse_slot_health(slots_down_json([]), 4) == Ok(AllSlotsServing())

    @pytest.mark.parametrize(
        'cause',
        [
            ResourceShortage(ShortageErrno.ENOMEM),
            TaskLoopCreateFailed(),
            ThreadStartRefused(),
            SpawnFailed(),
        ],
    )
    def test_each_cause_that_can_clear_round_trips(
        self,
        cause: ClearableBootCause,
    ) -> None:
        down = DownSlot(
            slot=2,
            cause=cause,
            detail='x',
            down_since=datetime(2026, 1, 1, tzinfo=timezone.utc),
            failed_boots=1,
        )
        assert parse_slot_health(slots_down_json([down]), 4) == Ok(SlotsDown((down,)))

    def test_down_slots_come_back_in_slot_order(self) -> None:
        stored = slots_down_json([_down_slot(3), _down_slot(0)])
        assert parse_slot_health(stored, 4) == Ok(
            SlotsDown((_down_slot(0), _down_slot(3))),
        )

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('slot', 4),
            ('slot', -1),
            ('slot', True),
            ('failed_boots', 0),
            ('failed_boots', '3'),
            ('detail', None),
            ('detail', 'd' * 2401),
            ('down_since', '2026-01-01T12:00:00'),
            ('down_since', 'yesterday'),
            ('cause', {'kind': 'app_load_failed'}),
            ('cause', {'kind': 'resource_shortage', 'errno': 'ENOENT'}),
        ],
        ids=[
            'slot-past-processes',
            'slot-negative',
            'slot-bool',
            'failed-boots-zero',
            'failed-boots-text',
            'detail-missing',
            'detail-over-bound',
            'down-since-naive',
            'down-since-not-iso',
            'cause-that-repeats',
            'cause-errno-not-in-table',
        ],
    )
    def test_every_field_is_checked(self, key: str, value: object) -> None:
        entry = slots_down_json([_down_slot(1)])[0]
        entry[key] = value
        assert isinstance(parse_slot_health([entry], 4), Err)

    def test_more_entries_than_processes_is_unreadable(self) -> None:
        stored = slots_down_json([_down_slot(0), _down_slot(1)])
        assert isinstance(parse_slot_health(stored, 1), Err)

    def test_one_slot_twice_is_unreadable(self) -> None:
        stored = slots_down_json([_down_slot(1), _down_slot(1)])
        assert isinstance(parse_slot_health(stored, 4), Err)

    def test_a_value_that_is_not_a_list_is_unreadable(self) -> None:
        assert isinstance(parse_slot_health({'slot': 1}, 4), Err)

    def test_the_writer_covers_every_down_slot_field(self) -> None:
        """A new field must reach the stored value."""
        import dataclasses

        names = {field.name for field in dataclasses.fields(DownSlot)}
        assert set(slots_down_json([_down_slot(1)])[0]) == names

    def test_a_cause_that_repeats_cannot_be_written_as_a_down_cause(self) -> None:
        """The type admits only causes that can clear; a stored one that
        repeats is refused on read."""
        entry = slots_down_json([_down_slot(1)])[0]
        entry['cause'] = {'kind': 'app_load_failed'}
        match parse_slot_health([entry], 4):
            case Err(unreadable):
                assert repr(AppLoadFailed()) in unreadable.reason
            case other:
                raise AssertionError(other)
