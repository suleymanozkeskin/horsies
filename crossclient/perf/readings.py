"""What a run reads besides the time of each operation: the CPU time of the
processes and the telemetry columns, per measured unit.

A unit is one drain of a worker drain scenario, or one block of calls of a
public call scenario. A statement scenario reads none: its server counts are
exact, and its client is the harness itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from crossclient.perf.metrics_scrape import EndpointCounts, ReadTally
from crossclient.perf.resources import CpuReading, WorkerCpu
from crossclient.perf.statistics import CostComparison
from crossclient.perf.telemetry_levels import NoEndpoint, NotRead


@dataclass(frozen=True, slots=True)
class StatementReadings:
    """A statement scenario: no CPU time and no telemetry column is read."""


@dataclass(frozen=True, slots=True)
class BlockReading:
    """One block of public calls: the CPU seconds of the harness process
    (the producer) and of PostgreSQL across the block."""

    calls: int
    producer_s: float
    postgres: CpuReading


@dataclass(frozen=True, slots=True)
class BlockReadings:
    blocks: tuple[BlockReading, ...]


@dataclass(frozen=True, slots=True)
class DrainReading:
    """One drain of ``tasks`` tasks.

    - ``worker``: the CPU seconds of the worker at the end of the drain,
      since its start.
    - ``postgres``: the CPU seconds of PostgreSQL from the start of the
      worker to the end of the drain.
    - ``stop_s``: the time from SIGTERM to the exit of the worker.
    - ``endpoint``: the telemetry columns of the read of ``/metrics`` at the
      end of the drain; ``NoEndpoint`` at a level with no endpoint.
    - ``reads``: the answers to the reads during the drain; ``NotRead`` at a
      level that does not read the endpoint during the drain.
    """

    tasks: int
    worker: WorkerCpu
    postgres: CpuReading
    stop_s: float
    endpoint: EndpointCounts | NoEndpoint
    reads: ReadTally | NotRead


@dataclass(frozen=True, slots=True)
class DrainReadings:
    drains: tuple[DrainReading, ...]


type Readings = StatementReadings | BlockReadings | DrainReadings


class CostMeasure(Enum):
    """One cost that a run compares between its sides."""

    WORKER_CPU = 'worker CPU per task (parent and children)'
    WORKER_PARENT_CPU = 'worker parent CPU per task'
    WORKER_CHILDREN_CPU = 'worker children CPU per task'
    WORKER_STOP = 'worker stop time per drain'
    PRODUCER_CPU = 'producer CPU per call'
    POSTGRES_CPU_PER_TASK = 'PostgreSQL CPU per task'
    POSTGRES_CPU_PER_CALL = 'PostgreSQL CPU per call'


@dataclass(frozen=True, slots=True)
class MeasuredCost:
    measure: CostMeasure
    comparison: CostComparison


@dataclass(frozen=True, slots=True)
class UnmeasuredCost:
    """The run did not measure this cost; ``reason`` says why."""

    measure: CostMeasure
    reason: str


type Cost = MeasuredCost | UnmeasuredCost
