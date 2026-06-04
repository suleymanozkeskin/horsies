"""Unit tests for the worker/database health wire payloads and result models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.models.health import (
    WORKER_PING_CHANNEL,
    DatabasePing,
    WorkerPingRequest,
    WorkerPong,
    WorkerPongPayload,
    WorkerStateSnapshot,
)
from horsies.core.types.result import is_ok


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
            worker_started_at=now,
        )
        assert snap.cluster_wide_cap is None
        assert snap.queues == ['default']
        assert snap.tasks_running == 0
