"""Unit tests for the worker/database health wire payloads and result models."""

from __future__ import annotations

import pytest


from horsies.core.models.health import (
    AllSlotsServing,
    ClassBudgets,
    DatabasePing,
    WorkerPong,
    WorkerStateSnapshot,
)


@pytest.mark.unit
class TestResultModels:
    """Frozen result dataclasses returned to callers."""


    def test_database_ping_is_frozen(self) -> None:
        ping = DatabasePing(latency_ms=1.5)
        with pytest.raises(AttributeError):
            ping.latency_ms = 2.0  # type: ignore[misc]

    def test_worker_pong_is_frozen(self) -> None:
        pong = WorkerPong(worker_id='w', hostname='h', pid=1, round_trip_ms=3.0)
        with pytest.raises(AttributeError):
            pong.worker_id = 'other'  # type: ignore[misc]

    def test_worker_state_snapshot_holds_optional_fields(self) -> None:
        """Snapshot tolerates NULL psutil columns as None; unset maps are empty."""
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
            queue_priorities={},
            queue_max_concurrency={},
            recovery_config={},
            tasks_running=0,
            tasks_claimed=0,
            memory_usage_mb=None,
            memory_percent=None,
            cpu_percent=None,
            children_memory_mb=None,
            worker_started_at=now,
            slot_health=AllSlotsServing(),
            class_capacity=ClassBudgets(()),
        )
        assert snap.cluster_wide_cap is None
        assert snap.queues == ['default']
        assert snap.tasks_running == 0
