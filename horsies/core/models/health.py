"""Typed models for the worker/database health API.

Two families live here:

* **Wire payloads** (`WorkerPingRequest`, `WorkerPongPayload`) — pydantic
  models that travel as JSON over the LISTEN/NOTIFY ping-pong channels.
  They forbid extras and are validated on both encode and decode, matching
  the library's "signature is the wire contract" stance.

* **Result models** (`DatabasePing`, `WorkerPong`, `WorkerStateSnapshot`) —
  frozen dataclasses returned to callers inside ``BrokerResult``. They carry
  no behaviour, only observed data.

The worker-state snapshot mirrors ``WorkerStateModel`` (the timeseries row
written every few seconds); reads expose it without coercion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


# Shared inbound channel every worker subscribes to. A ping NOTIFY carries a
# ``WorkerPingRequest`` payload; each worker replies on the request's
# per-request ``reply_channel``.
WORKER_PING_CHANNEL = 'horsies_worker_ping'


class WorkerPingRequest(BaseModel):
    """Request payload broadcast on ``WORKER_PING_CHANNEL``.

    ``target_worker_id`` of ``None`` is a broadcast (every worker replies);
    otherwise only the worker whose instance id matches replies.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    correlation_id: str
    reply_channel: str
    target_worker_id: str | None = None


class WorkerPongPayload(BaseModel):
    """Reply payload a worker sends on the request's ``reply_channel``.

    Carries only identity. A pong proves the worker's event loop is
    responsive and that it can reach Postgres (the reply travels through the
    worker's own connection). Load/config data lives in the worker-state
    snapshot, not here, so the reply path stays a pure liveness signal.
    """

    model_config = ConfigDict(extra='forbid', frozen=True)

    correlation_id: str
    worker_id: str
    hostname: str
    pid: int


@dataclass(slots=True, frozen=True)
class DatabasePing:
    """Result of a ``SELECT 1`` round-trip through the live broker pool."""

    latency_ms: float


@dataclass(slots=True, frozen=True)
class WorkerPong:
    """A single worker's reply to a ping, with measured round-trip latency."""

    worker_id: str
    hostname: str
    pid: int
    round_trip_ms: float


@dataclass(slots=True, frozen=True)
class WorkerStateSnapshot:
    """One ``horsies_worker_states`` row — a worker's state at a point in time.

    Mirrors ``WorkerStateModel``. Returned as the latest snapshot per worker
    (cluster-wide listing) or as a timeseries (single-worker history).
    """

    worker_id: str
    snapshot_at: datetime
    hostname: str
    pid: int
    processes: int
    max_claim_batch: int
    max_claim_per_worker: int
    cluster_wide_cap: int | None
    queues: list[str]
    queue_priorities: dict[str, int] | None
    queue_max_concurrency: dict[str, int] | None
    recovery_config: dict[str, Any] | None
    tasks_running: int
    tasks_claimed: int
    memory_usage_mb: float | None
    memory_percent: float | None
    cpu_percent: float | None
    children_memory_mb: float | None
    worker_started_at: datetime
