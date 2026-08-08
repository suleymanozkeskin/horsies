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
    plan: MeasurementPlan,
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
    completed = run_side(
        runtime.interpreter,
        measure_source(
            plan,
            config_spec=config_spec,
            database_url=runtime.database_url,
            observations=observations,
            block=block,
        ),
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
    schedule = observations_in_schedule_order(spec, blocks)
    assert_schedule_matches_its_model(spec, schedule)
    assert_no_long_runs(spec, schedule)
    return schedule, samples_in_schedule_order(blocks)


def run_conditions(
    plan: MeasurementPlan,
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
    }
