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
from .paired_mode import (
    ServerMode,
    assert_mode_conformance,
    assert_structure_conformance,
)
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
    TERMINALIZE = 'terminalize'


# Operations that are a database statement rather than a task send. They need a
# different body, and their observations consume rows rather than create them.
STATEMENT_OPERATIONS: Final = frozenset(
    {Operation.CLAIM, Operation.TERMINALIZE}
)


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
    # Sixteen, re-derived under section 2.1 at the mandated block size of 100
    # by the same method as the original: run several priming counts and watch
    # where each block's first observation sits relative to its own block's
    # median-of-the-rest, and where each block's maximum lands.
    #
    #   priming  1: ratio 1.58, maxima at position 0 in 9 of 12 blocks
    #   priming  4: ratio 1.29, 1 of 12
    #   priming  8: ratio 1.10, 0 of 12
    #   priming 16: ratio 1.01, 0 of 12
    #
    # Eight was enough on the previous instrument. Under section 2.1 it leaves
    # a systematic 10% elevation on the first observation of every block, and
    # sixteen removes it. The extra sends cost about 13 ms against a block that
    # takes roughly half a second.
    priming_sends: int = 16

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
    priming_sends: int = 16
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


# Harness-authored, replicating the product. The CLAIMED -> RUNNING transition
# lives as inline SQL inside `horsies/core/worker/child_runner.py` rather than
# as a shared constant, so unlike the claim statement and the terminalization
# command there is nothing to borrow. This is SETUP: it establishes the state
# the measurand requires, and it is not the thing being timed.
#
# Fidelity is derived rather than guessed. The product's statement writes eight
# columns, and all eight are written here, because the measurand's fence and
# its projection read this row: a RUNNING row missing a column the product sets
# would exercise NULL paths production never runs.
RUNNING_SETUP_STATEMENT: Final = """
UPDATE horsies_tasks
SET status = 'RUNNING',
    claimed = FALSE,
    claim_expires_at = NULL,
    started_at = NOW(),
    worker_pid = :worker_pid,
    worker_hostname = :worker_hostname,
    worker_process_name = :worker_process_name,
    updated_at = NOW()
WHERE id = :task_id
  AND status = 'CLAIMED'
  AND claimed_by_worker_id = :worker_id
RETURNING id
"""

# What the statement above writes, named so the pin can compare it against what
# the product writes on each build.
RUNNING_SETUP_COLUMNS: Final = frozenset(
    {
        'status',
        'claimed',
        'claim_expires_at',
        'started_at',
        'worker_pid',
        'worker_hostname',
        'worker_process_name',
        'updated_at',
    }
)

# How the artifact describes it: not the product's own statement, and not
# something invented either.
RUNNING_SETUP_PROVENANCE: Final = (
    'harness-authored, replicating the CLAIMED -> RUNNING transition inline in '
    'horsies/core/worker/child_runner.py over the columns both schemas share'
)


def running_transition_columns(child_runner_source: str) -> frozenset[str]:
    """The columns the product's inline CLAIMED -> RUNNING statement writes.

    Read out of a build's own source so the pin compares against what that
    build does, rather than against what this harness remembers it doing. A
    product change to the transition then breaks the pin instead of silently
    leaving the harness establishing a different precondition.
    """
    anchor = child_runner_source.find("UPDATE horsies_tasks\n                SET status = 'RUNNING'")
    if anchor < 0:
        raise MeasurementError(
            'no CLAIMED -> RUNNING transition found in the child runner '
            'source; the product moved it, and the setup this harness writes '
            'can no longer be checked against it'
        )
    body = child_runner_source[anchor : child_runner_source.index('WHERE', anchor)]
    columns: set[str] = set()
    for line in body.splitlines():
        stripped = line.strip().rstrip(',')
        if '=' not in stripped or stripped.startswith('UPDATE'):
            continue
        name = stripped.split('=', 1)[0].strip()
        if name.startswith('SET '):
            name = name[4:].strip()
        if name.isidentifier():
            columns.add(name)
    if not columns:
        raise MeasurementError(
            'the child runner transition was found but no columns were read '
            'out of it'
        )
    return frozenset(columns)


_TERMINALIZE_TAIL_TEMPLATE: Final = '''
from sqlalchemy import create_engine as _create_engine, text as _text

_plan = _json.loads(__PLAN_JSON__)
_observations = _json.loads(__OBSERVATIONS_JSON__)
_block = _json.loads(__BLOCK_JSON__)

# Both the claim statement and the terminalization command come from the build
# under measurement. The command is the product's own vocabulary and its SQL is
# produced by the product's own call_for, so the two sides run each build's
# terminalization rather than a statement this harness invented.
from horsies.core.worker.sql import HORSIES_CLAIM_SQL as _claim_sql
from horsies.core.lifecycle.commands import (
    CompleteTaskFused as _CompleteTaskFused,
    OwnedClaim as _OwnedClaim,
)
from horsies.core.lifecycle.persistence import (
    call_for as _call_for,
    decode_outcome_row as _decode_outcome_row,
)

_engine = _create_engine(
    _app_config.broker.database_url.get_secret_value(),
    pool_size=_config_spec['pool_size'],
    max_overflow=_config_spec['max_overflow'],
)


def _claim_params(_worker_id):
    return {
        'p_worker_id': _worker_id,
        'p_queues': _json.dumps(_plan['queues']),
        'p_queue_priority': _json.dumps({_q: 100 for _q in _plan['queues']}),
        'p_queue_max_concurrency': _json.dumps({}),
        'p_hard_cap_mode': True,
        'p_processes': 1,
        'p_prefetch_buffer': 0,
        'p_max_claim_per_worker': 1,
        'p_max_claim_batch': 1,
        'p_cluster_wide_cap': None,
        'p_lease_ms': _plan['lease_ms'],
        'p_lock_keys': _json.dumps([]),
    }


# The result payload is incompressible for the same reason the seed's is: a
# repeated character would be stored at a fraction of its declared size, so a
# row claiming a 1 MiB result would be timing a much smaller one.
_rng = _random.Random(_plan['payload_seed'] + _block)
_size = _plan['result_bytes']
_results = []
for _index in range(_observations + _plan['priming_sends']):
    _blob = _b64.b64encode(_rng.randbytes(_size)).decode('ascii')[:_size]
    if len(_blob) != _size:
        raise RuntimeError('result generator produced ' + str(len(_blob)))
    _results.append(_json.dumps({'ok': _blob}))


def _to_running(_conn, _row, _worker_id):
    \'\'\'Setup, outside the timed region: the state a completion requires.\'\'\'
    _updated = _conn.execute(
        _text(_plan['running_setup_statement']),
        {
            'task_id': str(_row.id),
            'worker_id': _worker_id,
            'worker_pid': _plan['worker_pid'],
            'worker_hostname': _plan['worker_hostname'],
            'worker_process_name': _plan['worker_process_name'],
        },
    ).fetchall()
    _conn.commit()
    if len(_updated) != 1:
        raise RuntimeError(
            'the CLAIMED to RUNNING setup updated ' + str(len(_updated))
            + ' rows, expected 1; the completion would then be refused for a '
            'source state the harness failed to establish'
        )


def _terminalize(_conn, _row, _result_json):
    _command = _CompleteTaskFused(
        task_id=str(_row.id),
        fence=_OwnedClaim(
            worker_id=_row_worker_id, claimed_at=_row.claimed_at,
        ),
        result_json=_result_json,
        notify_channel='task_queue_' + str(_row.queue_name or 'default'),
        notify_payload='capacity:' + str(_row.id),
    )
    _statement, _params = _call_for(_command)
    _returned = _conn.execute(_statement, _params).fetchall()
    _conn.commit()
    # The statement reports what it did. A rejected command still runs -- it
    # takes the lock, reads the row and returns an outcome -- so timing one
    # without reading the outcome measures a refusal at plausible,
    # sub-millisecond, stable cost while nothing is written.
    if len(_returned) != 1:
        raise RuntimeError(
            'terminalization returned ' + str(len(_returned))
            + ' outcome rows, expected exactly 1'
        )
    _outcome = dict(_returned[0]._mapping)
    if _outcome.get('outcome') != 'APPLIED':
        raise RuntimeError(
            'terminalization did not apply: outcome='
            + str(_outcome.get('outcome'))
            + ' observed_status=' + str(_outcome.get('observed_status'))
            + '. A command that is refused is not the operation this row '
            'judges, and it costs a fraction of what applying costs'
        )


_samples = []
with _engine.connect() as _conn:
    for _index in range(_observations + _plan['priming_sends']):
        _row_worker_id = 'qual-t-' + str(_block) + '-' + str(_index)
        _rows = _conn.execute(
            _claim_sql, _claim_params(_row_worker_id)
        ).fetchall()
        _conn.commit()
        if not _rows:
            raise RuntimeError(
                'no task to terminalize at observation ' + str(_index)
                + '; seed more pending work'
            )
        _row = _rows[0]
        _to_running(_conn, _row, _row_worker_id)
        if _index < _plan['priming_sends']:
            _terminalize(_conn, _row, _results[_index])
            continue
        _started = _time.perf_counter_ns()
        _terminalize(_conn, _row, _results[_index])
        _elapsed = _time.perf_counter_ns() - _started
        _samples.append(_elapsed / 1000000.0)

print(
    __SAMPLES_MARKER__ + ' ' + _json.dumps({'samples': _samples}),
    flush=True,
)
'''


@dataclass(frozen=True, slots=True)
class TerminalizePlan:
    """One terminalization observation: claim a task, then complete it.

    The claim is setup and sits outside the timed region -- the row judges the
    terminalization, not the claim that made it possible.
    """

    task_name: str
    result_bytes: int
    payload_seed: int
    queues: tuple[str, ...] = ('default',)
    lease_ms: int = 30_000
    priming_sends: int = 16
    worker_pid: int = 424242
    worker_hostname: str = 'qual-harness'
    worker_process_name: str = 'qual-terminalize'
    operation: Operation = Operation.TERMINALIZE

    def __post_init__(self) -> None:
        if self.result_bytes < 1:
            raise MeasurementError(
                f'result_bytes must be at least 1, got {self.result_bytes}'
            )

    def as_payload(self) -> dict[str, Any]:
        return {
            'task_name': self.task_name,
            'operation': self.operation.value,
            'result_bytes': self.result_bytes,
            'payload_seed': self.payload_seed,
            'queues': list(self.queues),
            'lease_ms': self.lease_ms,
            'priming_sends': self.priming_sends,
            'worker_pid': self.worker_pid,
            'worker_hostname': self.worker_hostname,
            'worker_process_name': self.worker_process_name,
            'running_setup_statement': RUNNING_SETUP_STATEMENT,
            'running_setup_provenance': RUNNING_SETUP_PROVENANCE,
        }


def terminalize_source(
    plan: TerminalizePlan,
    *,
    config_spec: SeedConfigSpec,
    database_url: str,
    observations: int,
    block: int,
) -> str:
    """One terminalization block's body, for one side."""
    if observations < 1:
        raise MeasurementError(
            f'a block needs at least one observation, got {observations}'
        )
    return substitute_seed_tokens(
        CONFIG_SOURCE_SNIPPET + _TERMINALIZE_TAIL_TEMPLATE,
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
    plan: MeasurementPlan | ClaimPlan | TerminalizePlan,
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
        case TerminalizePlan():
            body = terminalize_source(
                plan,
                config_spec=config_spec,
                database_url=runtime.database_url,
                observations=observations,
                block=block,
            )
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
# A row is returned WHOLE, not just relabelled. The product's own
# ck_horsies_tasks_terminal_at_terminal_only refuses a row whose status says
# PENDING while terminal_at still carries a terminal timestamp -- so a partial
# reset does not quietly leave an incoherent row, it fails. Every column the
# claim, the RUNNING setup and the terminalization write is cleared here.
#
# Every column touched here is present in BOTH schemas. The released build's
# task table is a strict subset of the checkout's -- 35 columns against 50, no
# baseline-only column -- and status, claimed_at, result and completed_at are
# all in the shared 35. A reset that named a candidate-only column would fail
# on the baseline half and leave that side unreset.
STEADY_STATE_STATEMENTS: Final = (
    """
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        completed_at = NULL,
        failed_at = NULL,
        failed_reason = NULL,
        error_code = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NULL,
        terminalization_kind = NULL,
        result = NULL,
        next_retry_at = NULL,
        retry_count = 0,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        updated_at = NOW()
    WHERE status <> 'PENDING' OR claimed OR terminal_at IS NOT NULL
    """,
    'VACUUM ANALYZE horsies_tasks',
)

# The columns above, named so a test can check the claim rather than trust it.
STEADY_STATE_COLUMNS: Final = frozenset(
    {
        'status',
        'claimed',
        'claimed_at',
        'claimed_by_worker_id',
        'claim_expires_at',
        'started_at',
        'completed_at',
        'failed_at',
        'failed_reason',
        'error_code',
        'finalizing_at',
        'finalizing_by_worker_id',
        'terminal_at',
        'terminalization_kind',
        'result',
        'next_retry_at',
        'retry_count',
        'worker_pid',
        'worker_hostname',
        'worker_process_name',
        'updated_at',
    }
)

# What the steady-state step costs a reader: absolute levels describe a
# vacuumed table, which is not every production state. The quoted quantity is
# the delta between two sides that both saw the same pre-block state.
STEADY_STATE_CONSEQUENCE: Final = (
    'absolute levels reflect a freshly vacuumed table; the quoted quantity is '
    'the delta between two sides given identical pre-block state'
)

# The gate measures TREND, not range. Max-minus-min is an extreme-value
# statistic: it grows with the number of blocks even when nothing is moving,
# so a bound derived at six blocks refuses good runs at two hundred. Measured
# on identical steady-state runs at the mandated block size, the spread came
# out 0.380, 0.192, 0.104 and 0.092 -- a fourfold range from an unchanged
# instrument -- while the trend statistic gave 0.046, 0.045 and 0.058.
#
# The statistic is the fitted slope over per-block medians, expressed as the
# total change it predicts across the measured run relative to the mean level:
# it answers "how far did the operation move from start to finish", which is
# the failure, and it does not grow with block count.
#
# Derived under section 2.1 conditions at blocks of 100, by the same method as
# the original: steady state 0.058 at worst, the draining defect 0.888. The
# bound leans toward the tighter side of the sandwich because the harms are
# asymmetric -- a bound too loose admits a moving measurand and produces
# numbers nobody can see are wrong, while a bound too tight only costs a re-run.
BLOCK_MEDIAN_DRIFT_BOUND: Final = 0.20

# What a passing trend does NOT establish, carried into the conditions for the
# same reason DriftAttributionBound carries its own exclusion: an unqualified
# "flat" reads as a claim the window held still in every respect.
DRIFT_GATE_EXCLUDES: Final = (
    'a symmetric mid-run excursion, which nets a slope near zero and passes; '
    'dispersion of that shape is answered by the recorded spread, the '
    'bootstrap interval width, and confirm-on-repeat'
)


def measured_block_medians(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> tuple[float, ...]:
    """Per-block medians over the measured half, in block order."""
    medians = tuple(
        _median(result.samples)
        for result in blocks
        if result.block >= spec.warmup_blocks
    )
    if len(medians) < 2:
        raise MeasurementError(
            'fewer than two measured blocks, so the run has no trend to check'
        )
    return medians


def block_median_drift(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> float:
    """How far the operation moved from the start of the run to its end.

    The failure this detects is the measurand moving during the run: an
    operation that mutates the rows it reads gets slower as the versions it
    must skip accumulate. Counterbalancing cannot help — it holds the mean
    position gap at zero, which cancels the effect on a location statistic
    while leaving it in the tail.

    Reported as the fitted slope's total predicted change across the measured
    blocks, relative to their mean. A range statistic would grow with the
    number of blocks on an instrument that never moved.

    **What it does not police.** A symmetric mid-run excursion — the machine
    slowing and recovering — nets a slope near zero and passes, because it did
    not move the run from start to finish. That shape lands instead in the
    recorded spread, in the bootstrap interval's width, and in confirm-on-repeat,
    each of which sees dispersion this statistic is blind to by construction. A
    zero trend says the operation ended where it began, never that the window
    held still throughout.
    """
    medians = measured_block_medians(spec, blocks)
    count = len(medians)
    mean_index = (count - 1) / 2.0
    mean_median = sum(medians) / count
    if mean_median <= 0.0:
        raise MeasurementError(
            f'the measured blocks average a non-positive median '
            f'({mean_median}); a relative drift against it is undefined'
        )
    covariance = sum(
        (index - mean_index) * (value - mean_median)
        for index, value in enumerate(medians)
    )
    variance = sum((index - mean_index) ** 2 for index in range(count))
    slope = covariance / variance
    return abs(slope) * (count - 1) / mean_median


def block_median_spread(
    spec: InterleaveSpec, blocks: Sequence[BlockResult]
) -> float:
    """Range of per-block medians, recorded but never gated on.

    Kept because it describes the run for a reader, and excluded from the gate
    because it is an extreme-value statistic: it grows with the number of
    blocks on an instrument that never moved.
    """
    medians = measured_block_medians(spec, blocks)
    return (max(medians) - min(medians)) / min(medians)


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
    bound: float = BLOCK_MEDIAN_DRIFT_BOUND,
) -> None:
    """The row must prove its own steady state, not assert it.

    Having a steady-state step is a claim about the harness. Refusing a run
    whose blocks did not hold still is a property of the row.
    """
    drift = block_median_drift(spec, blocks)
    if drift > bound:
        raise MeasurementError(
            f'per-block medians drift {drift:.3f} relative across the measured '
            f'run, over the {bound} this row allows: the operation changed '
            'while it was being measured, so the row reports an average over a '
            'moving measurand. A zero mean-position gap cancels this in a '
            'location statistic and leaves it in the tail, which is why such a '
            'run can look stable at p50 and disagree with itself at p99'
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
    spec: InterleaveSpec,
    blocks: Sequence[BlockResult],
    *,
    server: ServerMode,
) -> tuple[tuple[ScheduledObservation, ...], tuple[float, ...]]:
    """Every check that must hold before a cell may be built from this run.

    The server mode is a required argument rather than an optional one. A run
    that could be validated without naming the instrument it ran on is a run
    whose limits belong to a mode nobody checked, which is how five rows came
    to be measured on default durability settings.
    """
    assert_mode_conformance(server)
    assert_structure_conformance(
        server.mode,
        observations_per_side=spec.measured_observations_per_side(),
        block_size=spec.block_size,
    )
    assert_blocks_ran_in_schedule_order(spec, blocks)
    assert_configurations_held(blocks)
    assert_block_medians_are_flat(spec, blocks)
    schedule = observations_in_schedule_order(spec, blocks)
    assert_schedule_matches_its_model(spec, schedule)
    assert_no_long_runs(spec, schedule)
    return schedule, samples_in_schedule_order(blocks)


def run_conditions(
    plan: MeasurementPlan | ClaimPlan | TerminalizePlan,
    blocks: Sequence[BlockResult],
    *,
    unit: SampleUnit,
    server: ServerMode,
    spec: InterleaveSpec,
) -> dict[str, Any]:
    """What the artifact records about how the observations were taken."""
    return {
        'plan': plan.as_payload(),
        'unit': unit.value,
        'instrument': server.as_conditions(),
        'steady_state_trend': {
            'drift': block_median_drift(spec, blocks),
            'bound': BLOCK_MEDIAN_DRIFT_BOUND,
            'spread_recorded_not_gated': block_median_spread(spec, blocks),
            'excludes': DRIFT_GATE_EXCLUDES,
        },
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
