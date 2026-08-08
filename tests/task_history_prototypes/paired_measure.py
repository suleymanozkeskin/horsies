"""Timing one operation on one side, one block at a time.

Group A's ten limits are deltas, so every one of them needs both builds
measured in the same window under an ordering that does not hand the window's
drift to whichever build ran later. This module runs the blocks that ordering
asks for.

**A block is a process.** The two sides are two interpreters, so an observation
cannot be cheaper than a process launch — which would swamp a sub-millisecond
enqueue. Blocks exist so one launch amortises over many observations, and the
interleave's block structure is therefore also the subprocess structure.

**The configuration comes from the seed's own snippet.** A measurement body
that built its configuration separately could differ from the run that filled
the database it measures, and that difference would be reported as the build's.
Both bodies are the one snippet plus their own tail, and each block re-emits
its configuration so drift between seeding and measuring is caught rather than
assumed absent.

**Migrations do not run here.** Seeding migrated each side through the
product's own chain. A measurement block that migrated again would time a
schema check it does not mean to measure, and would do it once per block.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

from .paired_cell import SampleUnit
from .paired_interleave import (
    InterleaveSpec,
    ScheduledObservation,
    assert_no_long_runs,
    assert_schedule_matches_its_model,
    block_sides,
)
from .paired_seed import (
    CONFIG_SOURCE_SNIPPET,
    REQUIRED_FIELD_SENTINEL,
    SECRET_SENTINEL,
    SIDE_CONFIG_MARKER,
    SeedConfigSpec,
    SideConfig,
    assert_config_equivalence,
    config_from_output,
    substitute_seed_tokens,
)
from .paired_sides import (
    PairedSide,
    SideIdentity,
    assert_side_identity,
    measurement_environment,
    run_side,
    side_identity_from_output,
)

SIDE_SAMPLES_MARKER: Final = '__horsies_side_samples__'


class MeasurementError(Exception):
    """A block did not produce observations that can be attributed."""


class Operation(StrEnum):
    """The hot path a cell times.

    Keyed enqueue's control is *ordinary enqueue* rather than the baseline
    build, and that is forced rather than chosen: ``with_options`` on the
    released baseline takes no ``idempotency_key`` at all, so there is no
    baseline keyed enqueue for a cross-build delta to be taken against.

    ``OPTIONS_ORDINARY_ENQUEUE`` exists to be that control. Comparing keyed
    against a plain ``send`` would fold the options call into the delta; this
    goes through the same call with the key set to ``None``, so the two differ
    in the key and in nothing else.
    """

    ORDINARY_ENQUEUE = 'ordinary-enqueue'
    OPTIONS_ORDINARY_ENQUEUE = 'options-ordinary-enqueue'
    KEYED_ENQUEUE = 'keyed-enqueue'
    CLAIM = 'claim'


# Operations that are a database statement rather than a task send. They need a
# different body, and their observations consume rows rather than create them.
STATEMENT_OPERATIONS: Final = frozenset({Operation.CLAIM})


# The operations the released baseline cannot perform. Refused there rather
# than left to fail as a send error inside a block, which would look like a
# product fault in the transcript.
CANDIDATE_ONLY_OPERATIONS: Final = frozenset(
    {Operation.OPTIONS_ORDINARY_ENQUEUE, Operation.KEYED_ENQUEUE}
)

# The one line each operation varies. Written as one expression per operation
# over one body, so that what differs between two operations is visible as a
# diff rather than argued from two bodies that look similar.
_SEND_EXPRESSION: Final = {
    Operation.ORDINARY_ENQUEUE: (
        '_measured.send(blob=_blobs[_index], tag=_index)'
    ),
    Operation.OPTIONS_ORDINARY_ENQUEUE: (
        '_measured.with_options(idempotency_key=None)'
        '.send(blob=_blobs[_index], tag=_index)'
    ),
    Operation.KEYED_ENQUEUE: (
        '_measured.with_options(idempotency_key=_keys[_index])'
        '.send(blob=_blobs[_index], tag=_index)'
    ),
}


@dataclass(frozen=True, slots=True)
class MeasurementPlan:
    """What one block does, identically on both sides."""

    task_name: str
    operation: Operation
    payload_bytes: int
    payload_seed: int
    key_prefix: str = 'paired-key-'
    # Eight, because that is where the effect measured out. Unprimed, every
    # block's maximum sat at position 0 at 28-38 ms against a 1.5-2.0 ms
    # steady state. One priming send left first observations at 2.2-2.8 ms,
    # still consistently above their own block's median. At eight, first
    # observations fall inside the spread of the rest and block maxima scatter
    # across positions instead of concentrating at the first.
    priming_sends: int = 8

    def __post_init__(self) -> None:
        if self.payload_bytes < 1:
            raise MeasurementError(
                f'payload_bytes must be at least 1, got {self.payload_bytes}'
            )
        if self.priming_sends < 1:
            raise MeasurementError(
                'a block needs priming sends. Each block is a fresh process, '
                'so its first sends pay connection setup that the operation '
                'being measured does not include, and discarding warm-up '
                'BLOCKS cannot reach it because every measured block has a '
                'first observation of its own'
            )

    def as_payload(self) -> dict[str, Any]:
        return {
            'task_name': self.task_name,
            'operation': self.operation.value,
            'payload_bytes': self.payload_bytes,
            'payload_seed': self.payload_seed,
            'key_prefix': self.key_prefix,
            'priming_sends': self.priming_sends,
        }


@dataclass(frozen=True, slots=True)
class BlockResult:
    """One block's observations, with the identity of the run that took them.

    ``arm`` is the schedule's half; ``side`` is the build that ran it. They
    coincide for a cross-build row and diverge for a row whose control is a
    sibling operation, where both arms run on the candidate. Keeping them
    separate is what lets an operation-paired row be interleaved at all:
    without it the schedule sees one side, and the two operations end up
    measured in different windows with the drift between them attributed to
    the operation.
    """

    block: int
    side: PairedSide
    arm: PairedSide
    identity: SideIdentity
    config: SideConfig
    samples: tuple[float, ...]
    wall_clock_seconds: float


_MEASURE_TAIL_TEMPLATE: Final = '''
_plan = _json.loads(__PLAN_JSON__)
_observations = _json.loads(__OBSERVATIONS_JSON__)
_block = _json.loads(__BLOCK_JSON__)

_app = _Horsies(config=_app_config, run_schema_migrations=False)


@_app.task(task_name=_plan['task_name'])
def _measured(*, blob: str, tag: int) -> _TaskResult[int, _TaskError]:
    return _TaskResult(ok=tag)


# Payload bytes are generated before the timed region. Generating inside it
# would time the generator, which is the same on both sides and therefore adds
# noise to a delta without adding signal to either side.
_rng = _random.Random(_plan['payload_seed'] + _block)
_blobs = []
for _index in range(_observations):
    _size = _plan['payload_bytes']
    _blob = _b64.b64encode(_rng.randbytes(_size)).decode('ascii')[:_size]
    if len(_blob) != _size:
        raise RuntimeError(
            'payload generator produced ' + str(len(_blob))
            + ' bytes for a declared size of ' + str(_size)
        )
    _blobs.append(_blob)

# Connection setup, statement preparation and whatever else a first send pays
# for happen HERE, outside the timed region. Each block is a new process, so
# without this its first observation carries a cost the operation does not
# have; discarding warm-up BLOCKS cannot reach it, because every measured block
# has a first observation of its own.
for _index in range(_plan['priming_sends']):
    _primed = _measured.send(blob=_blobs[0], tag=-1)
    if not _primed.is_ok():
        raise RuntimeError('priming send failed: ' + repr(_primed.err()))

# Keys are built before the timed region and are unique per observation: a
# repeated idempotency key is a deduplicated send, which is a different
# operation from the one this row is judging.
_keys = [
    _plan['key_prefix'] + str(_block) + '-' + str(_index)
    for _index in range(_observations)
]

_samples = []
for _index in range(_observations):
    _started = _time.perf_counter_ns()
    _sent = __SEND_EXPRESSION__
    _elapsed = _time.perf_counter_ns() - _started
    if not _sent.is_ok():
        raise RuntimeError('measured send failed: ' + repr(_sent.err()))
    _samples.append(_elapsed / 1000000.0)

print(
    __SAMPLES_MARKER__ + ' ' + _json.dumps({'samples': _samples}),
    flush=True,
)
'''


_CLAIM_TAIL_TEMPLATE: Final = '''
import uuid as _uuid
from sqlalchemy import create_engine as _create_engine

_plan = _json.loads(__PLAN_JSON__)
_observations = _json.loads(__OBSERVATIONS_JSON__)
_block = _json.loads(__BLOCK_JSON__)

# The statement is the product's own, taken from the build under measurement.
# Copying its text into this harness would let the two sides run a statement
# neither build ships.
from horsies.core.worker.sql import HORSIES_CLAIM_SQL as _claim_sql

_engine = _create_engine(
    _app_config.broker.database_url.get_secret_value(),
    pool_size=_config_spec['pool_size'],
    max_overflow=_config_spec['max_overflow'],
)

# Every parameter is declared rather than read out of a worker config, so the
# two sides call the function with values a reader can check. No caps are
# configured, so the lock-key list is empty and the queue list is the default
# one -- which is what the product itself computes under this configuration.
def _params(_worker_id):
    return {
        'p_worker_id': _worker_id,
        'p_queues': _json.dumps(_plan['queues']),
        'p_queue_priority': _json.dumps(
            {_q: 100 for _q in _plan['queues']}
        ),
        'p_queue_max_concurrency': _json.dumps({}),
        'p_hard_cap_mode': True,
        'p_processes': _plan['processes'],
        'p_prefetch_buffer': 0,
        'p_max_claim_per_worker': _plan['max_claim_per_worker'],
        'p_max_claim_batch': _plan['max_claim_batch'],
        'p_cluster_wide_cap': None,
        'p_lease_ms': _plan['lease_ms'],
        'p_lock_keys': _json.dumps([]),
    }


# A fresh worker id per observation. Reusing one would let the per-worker cap
# block every claim after the first, and an empty claim is a different
# operation from a claim that finds work.
_worker_ids = [
    'qual-' + str(_block) + '-' + str(_index)
    for _index in range(_observations + _plan['priming_sends'])
]

_samples = []
_claimed_total = 0
with _engine.connect() as _conn:
    for _index in range(_plan['priming_sends']):
        _rows = _conn.execute(_claim_sql, _params(_worker_ids[_index])).fetchall()
        _conn.commit()

    for _index in range(_observations):
        _worker_id = _worker_ids[_plan['priming_sends'] + _index]
        _started = _time.perf_counter_ns()
        _rows = _conn.execute(_claim_sql, _params(_worker_id)).fetchall()
        _conn.commit()
        _elapsed = _time.perf_counter_ns() - _started
        if not _rows:
            raise RuntimeError(
                'claim returned no rows at observation ' + str(_index)
                + '; an empty claim does the lock and the scan but claims '
                'nothing, which is a different operation from the one this '
                'row judges. Seed more pending work'
            )
        _claimed_total += len(_rows)
        _samples.append(_elapsed / 1000000.0)

print(
    __SAMPLES_MARKER__ + ' '
    + _json.dumps({'samples': _samples, 'claimed_rows': _claimed_total}),
    flush=True,
)
'''


@dataclass(frozen=True, slots=True)
class ClaimPlan:
    """The claim call's parameters, declared rather than read from a worker.

    The statement itself comes from each build; only its arguments are fixed
    here, so the two sides call the same function with the same inputs and any
    difference is the function's.
    """

    task_name: str
    queues: tuple[str, ...] = ('default',)
    processes: int = 1
    max_claim_per_worker: int = 1
    max_claim_batch: int = 1
    lease_ms: int = 30_000
    priming_sends: int = 8
    operation: Operation = Operation.CLAIM

    def as_payload(self) -> dict[str, Any]:
        return {
            'task_name': self.task_name,
            'operation': self.operation.value,
            'queues': list(self.queues),
            'processes': self.processes,
            'max_claim_per_worker': self.max_claim_per_worker,
            'max_claim_batch': self.max_claim_batch,
            'lease_ms': self.lease_ms,
            'priming_sends': self.priming_sends,
        }


def claim_source(
    plan: ClaimPlan,
    *,
    config_spec: SeedConfigSpec,
    database_url: str,
    observations: int,
    block: int,
) -> str:
    """One claim block's body, for one side."""
    if observations < 1:
        raise MeasurementError(
            f'a block needs at least one observation, got {observations}'
        )
    return substitute_seed_tokens(
        CONFIG_SOURCE_SNIPPET + _CLAIM_TAIL_TEMPLATE,
        {
            '__CONFIG_JSON__': json.dumps(json.dumps(config_spec.as_payload())),
            '__DSN_JSON__': json.dumps(json.dumps(database_url)),
            '__CONFIG_MARKER__': json.dumps(SIDE_CONFIG_MARKER),
            '__SECRET_SENTINEL__': json.dumps(SECRET_SENTINEL),
            '__REQUIRED_SENTINEL__': json.dumps(REQUIRED_FIELD_SENTINEL),
            '__PLAN_JSON__': json.dumps(json.dumps(plan.as_payload())),
            '__OBSERVATIONS_JSON__': json.dumps(str(observations)),
            '__BLOCK_JSON__': json.dumps(str(block)),
            '__SAMPLES_MARKER__': json.dumps(SIDE_SAMPLES_MARKER),
        },
    )


def measure_source(
    plan: MeasurementPlan,
    *,
    config_spec: SeedConfigSpec,
    database_url: str,
    observations: int,
    block: int,
) -> str:
    """One block's body, for one side."""
    if observations < 1:
        raise MeasurementError(
            f'a block needs at least one observation, got {observations}'
        )
    return substitute_seed_tokens(
        CONFIG_SOURCE_SNIPPET + _MEASURE_TAIL_TEMPLATE,
        {
            '__CONFIG_JSON__': json.dumps(json.dumps(config_spec.as_payload())),
            '__DSN_JSON__': json.dumps(json.dumps(database_url)),
            '__CONFIG_MARKER__': json.dumps(SIDE_CONFIG_MARKER),
            '__SECRET_SENTINEL__': json.dumps(SECRET_SENTINEL),
            '__REQUIRED_SENTINEL__': json.dumps(REQUIRED_FIELD_SENTINEL),
            '__PLAN_JSON__': json.dumps(json.dumps(plan.as_payload())),
            '__OBSERVATIONS_JSON__': json.dumps(str(observations)),
            '__BLOCK_JSON__': json.dumps(str(block)),
            '__SAMPLES_MARKER__': json.dumps(SIDE_SAMPLES_MARKER),
            '__SEND_EXPRESSION__': _SEND_EXPRESSION[plan.operation],
        },
    )


def samples_from_output(
    output: str, *, side: PairedSide, expected: int
) -> tuple[float, ...]:
    """Read a block's observations, and refuse a block that came up short.

    A block that returned fewer observations than it was asked for has had its
    schedule silently changed, and the position of every later observation with
    it — which is the quantity the ordering controls.
    """
    for line in output.splitlines():
        if not line.startswith(SIDE_SAMPLES_MARKER):
            continue
        try:
            payload = json.loads(line[len(SIDE_SAMPLES_MARKER) :].strip())
        except ValueError as error:
            raise MeasurementError(
                f'{side} emitted an unparseable samples line: {line[:200]!r}'
            ) from error
        samples = tuple(float(value) for value in payload['samples'])
        if len(samples) != expected:
            raise MeasurementError(
                f'{side} returned {len(samples)} observations for a block of '
                f'{expected}; the schedule that ran is not the schedule whose '
                'drift bound was computed'
            )
        return samples
    raise MeasurementError(
        f'{side} produced no samples line; a block that cannot report its own '
        'observations cannot contribute to a cell'
    )


@dataclass(frozen=True, slots=True)
class SideRuntime:
    """Where one side lives, and what it must prove it is."""

    side: PairedSide
    interpreter: Path
    expected_root: Path
    expected_schema_version: int
    database_url: str


def run_block(
    runtime: SideRuntime,
    *,
    arm: PairedSide | None = None,
    block: int,
    observations: int,
    plan: MeasurementPlan | ClaimPlan,
    config_spec: SeedConfigSpec,
    cwd: Path,
    environment: Mapping[str, str] | None = None,
    clock: Any,
) -> BlockResult:
    """Run one block and refuse anything it cannot account for.

    ``clock`` is supplied rather than read here so the wall-clock a dry pass
    reports comes from the caller's one source of time.
    """
    if (
        runtime.side is PairedSide.BASELINE
        and plan.operation in CANDIDATE_ONLY_OPERATIONS
    ):
        raise MeasurementError(
            f'{plan.operation} was scheduled on the baseline, which does not '
            'have it: the released build takes no idempotency key. A row for '
            'this operation is measured against a sibling operation on the '
            'candidate, never as a cross-build delta'
        )
    started = clock()
    match plan:
        case ClaimPlan():
            body = claim_source(
                plan,
                config_spec=config_spec,
                database_url=runtime.database_url,
                observations=observations,
                block=block,
            )
        case MeasurementPlan():
            body = measure_source(
                plan,
                config_spec=config_spec,
                database_url=runtime.database_url,
                observations=observations,
                block=block,
            )
    completed = run_side(
        runtime.interpreter,
        body,
        environment=(
            measurement_environment() if environment is None else environment
        ),
        cwd=cwd,
    )
    elapsed = clock() - started
    if completed.returncode != 0:
        raise MeasurementError(
            f'{runtime.side} block {block} exited {completed.returncode}: '
            f'{completed.stderr[-2000:]}'
        )
    identity = side_identity_from_output(
        completed.stdout,
        side=runtime.side,
        interpreter=runtime.interpreter,
        expected_root=runtime.expected_root,
        expected_schema_version=runtime.expected_schema_version,
    )
    assert_side_identity(identity)
    return BlockResult(
        block=block,
        side=runtime.side,
        arm=runtime.side if arm is None else arm,
        identity=identity,
        config=config_from_output(completed.stdout, side=runtime.side),
        samples=samples_from_output(
            completed.stdout, side=runtime.side, expected=observations
        ),
        wall_clock_seconds=elapsed,
    )


def assert_blocks_ran_in_schedule_order(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> None:
    """The blocks that ran are the ones the schedule asked for, in its order.

    The ordering's whole guarantee is positional. Blocks that ran in a
    different order, or a side that ran a block belonging to the other, leave
    the recorded bound describing a run that did not happen.
    """
    expected = block_sides(spec)
    if len(blocks) != len(expected):
        raise MeasurementError(
            f'{len(blocks)} blocks ran, the schedule describes {len(expected)}'
        )
    for position, (result, arm) in enumerate(zip(blocks, expected, strict=True)):
        if result.block != position:
            raise MeasurementError(
                f'block at position {position} reports index {result.block}; '
                'the blocks did not run in schedule order'
            )
        if result.arm is not arm:
            raise MeasurementError(
                f'block {position} ran as {result.arm}, the schedule assigns '
                f'it to {arm}'
            )


# Run before every block, on both sides and both arms alike, outside the timed
# region. Order matters: rows return to PENDING first, then the table is
# vacuumed so the dead versions those claims left behind are actually removed,
# and ANALYZE pins planner statistics rather than leaving them to drift with
# whatever autovacuum happened to do between blocks.
STEADY_STATE_STATEMENTS: Final = (
    "UPDATE horsies_tasks SET status = 'PENDING', claimed_at = NULL "
    "WHERE status = 'CLAIMED'",
    'VACUUM ANALYZE horsies_tasks',
)

# What the steady-state step costs a reader: absolute levels describe a
# vacuumed table, which is not every production state. The quoted quantity is
# the delta between two sides that both saw the same pre-block state.
STEADY_STATE_CONSEQUENCE: Final = (
    'absolute levels reflect a freshly vacuumed table; the quoted quantity is '
    'the delta between two sides given identical pre-block state'
)

# Derived from the control run rather than chosen. In steady state the per-block
# medians spanned 0.617-0.709 ms, a relative spread of 0.149. Without the step
# the same six blocks ran 0.598-2.144 ms, a spread of 2.586. The bound sits
# about 2.7x above the measured noise and about 6.5x below the defect, so it
# separates them without sitting on either.
BLOCK_MEDIAN_SPREAD_BOUND: Final = 0.40


def block_median_spread(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> float:
    """Relative spread of per-block medians over the measured half.

    The failure this detects is the measurand moving during the run: an
    operation that mutates the rows it reads gets slower as the versions it
    must skip accumulate. Counterbalancing cannot help — it holds the mean
    position gap at zero, which cancels the effect on a location statistic
    while leaving it in the tail.
    """
    medians = [
        _median(result.samples)
        for result in blocks
        if result.block >= spec.warmup_blocks
    ]
    if not medians:
        raise MeasurementError(
            'no measured blocks, so the run has no steady state to check'
        )
    lowest = min(medians)
    if lowest <= 0.0:
        raise MeasurementError(
            f'a block reported a non-positive median ({lowest}); a spread '
            'against it is undefined'
        )
    return (max(medians) - lowest) / lowest


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def assert_block_medians_are_flat(
    spec: InterleaveSpec,
    blocks: Sequence[BlockResult],
    *,
    bound: float = BLOCK_MEDIAN_SPREAD_BOUND,
) -> None:
    """The row must prove its own steady state, not assert it.

    Having a steady-state step is a claim about the harness. Refusing a run
    whose blocks did not hold still is a property of the row.
    """
    spread = block_median_spread(spec, blocks)
    if spread > bound:
        raise MeasurementError(
            f'per-block medians span {spread:.3f} relative, over the {bound} '
            'this row allows: the operation changed while it was being '
            'measured, so the row reports an average over a moving measurand. '
            'A zero mean-position gap cancels this in a location statistic and '
            'leaves it in the tail, which is why such a run can look stable at '
            'p50 and disagree with itself at p99'
        )


def assert_configurations_held(blocks: Sequence[BlockResult]) -> None:
    """Every baseline block ran the same configuration as every candidate block.

    Checked across the whole run rather than once at the start: the blocks are
    separate processes, and a configuration that changed midway would otherwise
    show up only as a delta.
    """
    by_side: dict[PairedSide, list[BlockResult]] = {
        PairedSide.BASELINE: [],
        PairedSide.CANDIDATE: [],
    }
    for result in blocks:
        by_side[result.side].append(result)
    if not any(by_side.values()):
        raise MeasurementError('no blocks ran at all')
    for side, results in by_side.items():
        if not results:
            # An operation-paired row runs both arms on one build, so the
            # other build having no blocks is expected rather than a fault.
            continue
        first = results[0]
        for other in results[1:]:
            if dict(other.config.effective) != dict(first.config.effective):
                raise MeasurementError(
                    f'{side} block {other.block} ran a different '
                    f'configuration from block {first.block}; the run changed '
                    'underneath the cell'
                )
    if by_side[PairedSide.BASELINE] and by_side[PairedSide.CANDIDATE]:
        assert_config_equivalence(
            by_side[PairedSide.BASELINE][0].config,
            by_side[PairedSide.CANDIDATE][0].config,
        )


def observations_in_schedule_order(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> tuple[ScheduledObservation, ...]:
    """The schedule the blocks actually realised, one entry per observation."""
    schedule: list[ScheduledObservation] = []
    for result in blocks:
        for _ in result.samples:
            schedule.append(
                ScheduledObservation(
                    global_index=len(schedule),
                    block=result.block,
                    side=result.arm,
                    warmup=result.block < spec.warmup_blocks,
                )
            )
    return tuple(schedule)


def samples_in_schedule_order(
    blocks: Sequence[BlockResult],
) -> tuple[float, ...]:
    return tuple(value for result in blocks for value in result.samples)


def assert_run_is_measurable(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> tuple[tuple[ScheduledObservation, ...], tuple[float, ...]]:
    """Every check that must hold before a cell may be built from this run."""
    assert_blocks_ran_in_schedule_order(spec, blocks)
    assert_configurations_held(blocks)
    assert_block_medians_are_flat(spec, blocks)
    schedule = observations_in_schedule_order(spec, blocks)
    assert_schedule_matches_its_model(spec, schedule)
    assert_no_long_runs(spec, schedule)
    return schedule, samples_in_schedule_order(blocks)


def run_conditions(
    plan: MeasurementPlan | ClaimPlan,
    blocks: Sequence[BlockResult],
    *,
    unit: SampleUnit,
) -> dict[str, Any]:
    """What the artifact records about how the observations were taken."""
    return {
        'plan': plan.as_payload(),
        'unit': unit.value,
        'blocks': [
            {
                'block': result.block,
                'side': result.side.value,
                'arm': result.arm.value,
                'observations': len(result.samples),
                'wall_clock_seconds': result.wall_clock_seconds,
            }
            for result in blocks
        ],
        'wall_clock_seconds_total': sum(
            result.wall_clock_seconds for result in blocks
        ),
        'steady_state': {
            'statements': list(STEADY_STATE_STATEMENTS),
            'applied': 'before every block, both sides and both arms',
            'consequence': STEADY_STATE_CONSEQUENCE,
        },
    }
