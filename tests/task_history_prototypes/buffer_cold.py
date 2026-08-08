"""Cache posture as something proven at measurement time, not declared once.

Groups C and D judge the same queries under three cache postures, and five of
the fifteen point-loading cells exist only under the coldest one. A run that
measures a warm relation and labels the result cold passes every one of those
cells, and passes them comfortably — the direction nobody investigates.

So the posture is not a flag the harness sets. It is read back out of the
server at the moment the measured query runs:

* **shared buffers** — how many of the relation's blocks the server is holding.
  Read from ``pg_buffercache``, which reports the buffer pool directly rather
  than inferring it. A relation with buffers resident is not cold, whatever
  procedure was supposed to have run.
* **the measured query's own reads** — ``EXPLAIN (ANALYZE, BUFFERS)`` separates
  blocks that were hit in the pool from blocks that were read into it. A cold
  measurement reads; a warm one hits.

**What this cannot prove, stated rather than glossed.** The host page cache is
invisible from SQL. A block that PostgreSQL reads may still come from the
operating system's cache rather than from the device, and no query can tell the
difference. The eviction procedure is what addresses that half, and this module
records whether each of its steps ran instead of assuming it did. Where I/O
timing is available it is recorded as evidence, and evidence is what it is
called.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

# The one definition for this batch. Groups C and D cite it; neither restates
# it, because two harnesses implementing "cold" independently would be two
# measurands wearing one word.
BUFFER_COLD_DEFINITION: Final = (
    'stop the PostgreSQL server, drop the host page cache, restart, and issue '
    'no warming query before the measured one'
)


class CachePostureError(Exception):
    """The posture a cell claims is not the posture it ran under."""


class CachePosture(StrEnum):
    """The three postures groups C and D measure under."""

    PREPARED_WARM = 'prepared-warm'
    UNPREPARED = 'unprepared'
    BUFFER_COLD = 'buffer-cold'


class EvictionStep(StrEnum):
    """The steps of the ruled definition, named so each can be accounted for."""

    STOP_SERVER = 'stop-server'
    DROP_HOST_PAGE_CACHE = 'drop-host-page-cache'
    START_SERVER = 'start-server'
    NO_WARMING_QUERY = 'no-warming-query'


# Every step, in the order the definition gives them. A step missing from a
# declaration is missing from the procedure, so the set is fixed here rather
# than assembled by whoever is reporting.
EVICTION_PROCEDURE: Final = (
    EvictionStep.STOP_SERVER,
    EvictionStep.DROP_HOST_PAGE_CACHE,
    EvictionStep.START_SERVER,
    EvictionStep.NO_WARMING_QUERY,
)

# The steps that need privileges this harness does not hold. Listed so a
# declaration that claims them without an operator is refused rather than
# believed.
# What a declaration says when a step did not happen. It is a refusal, not a
# value: recording the omission honestly and then proceeding would turn the
# declaration into paperwork.
NOT_PERFORMED: Final = 'not-performed'

PRIVILEGED_STEPS: Final = frozenset(
    {
        EvictionStep.STOP_SERVER,
        EvictionStep.DROP_HOST_PAGE_CACHE,
        EvictionStep.START_SERVER,
    }
)


@dataclass(frozen=True, slots=True)
class BufferCensus:
    """How much of a relation the server is holding, at one moment.

    ``buffers`` counts entries in the shared buffer pool. It is the count that
    decides the posture; the relation's size is carried alongside so a reader
    can see what fraction was resident rather than only that it was non-zero.
    """

    relation: str
    buffers: int
    relation_blocks: int

    def as_conditions(self) -> dict[str, Any]:
        return {
            'relation': self.relation,
            'buffers_resident': self.buffers,
            'relation_blocks': self.relation_blocks,
        }


@dataclass(frozen=True, slots=True)
class MeasuredReads:
    """What the measured query itself did with the buffer pool.

    Taken from the plan of the query whose latency the cell reports, not from a
    probe run beside it. A probe can be cold while the measured query is warm —
    the probe would have warmed it.
    """

    shared_hit: int
    shared_read: int
    io_time_ms: float | None

    def as_conditions(self) -> dict[str, Any]:
        return {
            'shared_hit': self.shared_hit,
            'shared_read': self.shared_read,
            'io_time_ms': self.io_time_ms,
        }


# The operator's visit, verbatim. One paste, in the order the definition gives.
# Written here rather than in a runbook so the commands and the checks that
# judge them cannot drift apart.
EVICTION_COMMAND_BLOCK: Final = """\
sudo launchctl stop com.edb.launchd.postgresql-18
sudo /usr/sbin/purge
sudo launchctl start com.edb.launchd.postgresql-18
docker stop horsies-throughput-pg18\
"""

# Read before the stop and again after the restart. A postmaster that came up
# fresh reports a later start time; one that never went down reports the same
# one it did before.
POSTMASTER_START_TIME_SQL: Final = 'SELECT pg_postmaster_start_time()'

# Run after the restart and before any cell. Accepting connections is not the
# same as being ready to measure, and a cell that opens the window during
# recovery measures recovery.
READINESS_PROBE_SQL: Final = (
    'SELECT pg_is_in_recovery(), pg_postmaster_start_time()'
)


@dataclass(frozen=True, slots=True)
class ServerRestart:
    """Proof that the server actually went down and came back.

    Two of the three privileged steps do not have to be taken on trust. A
    postmaster that restarted reports a start time later than the one recorded
    before the stop, and no amount of intending to restart moves that value —
    so the stop and the start are evidenced rather than attested.

    The host page-cache purge has no such witness. It stays attested, and that
    asymmetry is the honest shape of this procedure rather than a gap in it.
    """

    start_time_before: str
    start_time_after: str
    in_recovery_after: bool

    def __post_init__(self) -> None:
        if not self.start_time_before or not self.start_time_after:
            raise CachePostureError(
                'a restart claim needs the postmaster start time from before '
                'the stop and after the restart; without both there is '
                'nothing to compare'
            )
        if self.start_time_after <= self.start_time_before:
            raise CachePostureError(
                f'the postmaster start time did not move: {self.start_time_before} '
                f'before, {self.start_time_after} after. The server did not '
                'restart, so its buffer pool is the one it had, and any cold '
                'posture claimed on top of this is false'
            )
        if self.in_recovery_after:
            raise CachePostureError(
                'the server is still in recovery; a cell opened now measures '
                'recovery rather than the query it names'
            )

    def as_conditions(self) -> dict[str, Any]:
        return {
            'postmaster_start_time_before': self.start_time_before,
            'postmaster_start_time_after': self.start_time_after,
            'in_recovery_after': self.in_recovery_after,
        }


@dataclass(frozen=True, slots=True)
class EvictionDeclaration:
    """Which steps of the definition ran, and who ran them.

    Steps needing privileges the harness lacks must name an operator. A step
    claimed as harness-executed when it cannot be is refused: the point of the
    declaration is to make an unperformed step visible, and a declaration that
    can lie about that records nothing.

    The restart evidence is a required field, so a declaration cannot be
    written for a server that never went down.
    """

    performed_by: dict[EvictionStep, str]
    restart: ServerRestart
    stopped_resident_containers: tuple[str, ...]

    def __post_init__(self) -> None:
        missing = [
            step for step in EVICTION_PROCEDURE if step not in self.performed_by
        ]
        if missing:
            raise CachePostureError(
                f'the eviction declaration omits {[s.value for s in missing]}; '
                f'buffer-cold means: {BUFFER_COLD_DEFINITION}. A step nobody '
                'accounted for is a step that did not happen'
            )
        unperformed = [
            step
            for step in EVICTION_PROCEDURE
            if self.performed_by[step] == NOT_PERFORMED
        ]
        if unperformed:
            raise CachePostureError(
                f'the eviction declaration records '
                f'{[s.value for s in unperformed]} as not performed; the '
                'posture rests on the whole procedure, and a declaration that '
                'can record a skipped step as acceptable records nothing'
            )
        for step in PRIVILEGED_STEPS:
            if self.performed_by[step] == 'harness':
                raise CachePostureError(
                    f'{step.value} is claimed as harness-executed; it needs '
                    'privileges this harness does not hold, so the claim is '
                    'false and the posture it supports is not established'
                )

    def as_conditions(self) -> dict[str, Any]:
        return {
            'definition': BUFFER_COLD_DEFINITION,
            'commands': EVICTION_COMMAND_BLOCK,
            'performed_by': {
                step.value: self.performed_by[step]
                for step in EVICTION_PROCEDURE
            },
            'restart': self.restart.as_conditions(),
            'attested_only': [EvictionStep.DROP_HOST_PAGE_CACHE.value],
            'stopped_resident_containers': list(
                self.stopped_resident_containers
            ),
        }


@dataclass(frozen=True, slots=True)
class PostureEvidence:
    """Everything that decides whether a cell's posture claim stands.

    The declaration is required for the cold posture and refused for the other
    two: a warm cell that carries an eviction declaration is describing a run
    that cannot have been warm.
    """

    posture: CachePosture
    census: BufferCensus
    reads: MeasuredReads
    declaration: EvictionDeclaration | None

    def __post_init__(self) -> None:
        match self.posture:
            case CachePosture.BUFFER_COLD:
                if self.declaration is None:
                    raise CachePostureError(
                        'a buffer-cold cell carries no eviction declaration, '
                        'so nothing says the server was ever evicted'
                    )
                if self.reads.io_time_ms is None:
                    raise CachePostureError(
                        'a buffer-cold cell reports no I/O read timing, so it '
                        'carries no evidence at all about the host page cache '
                        '— the half the eviction procedure exists for. Run '
                        'the measured query with track_io_timing on'
                    )
                assert_shared_buffers_cold(self.census, self.reads)
            case CachePosture.PREPARED_WARM:
                if self.declaration is not None:
                    raise CachePostureError(
                        'a prepared-warm cell carries an eviction '
                        'declaration; the run it describes cannot have been '
                        'warm'
                    )
                assert_shared_buffers_warm(self.census, self.reads)
            case CachePosture.UNPREPARED:
                if self.declaration is not None:
                    raise CachePostureError(
                        'an unprepared cell carries an eviction declaration; '
                        'that is the buffer-cold posture, under another name'
                    )

    def as_conditions(self) -> dict[str, Any]:
        return {
            'posture': self.posture.value,
            'shared_buffers': self.census.as_conditions(),
            'measured_query_reads': self.reads.as_conditions(),
            'eviction': (
                None if self.declaration is None
                else self.declaration.as_conditions()
            ),
            'host_page_cache': (
                'not observable from SQL; the eviction procedure addresses it '
                'and io_time_ms is evidence, not proof'
            ),
        }


def assert_shared_buffers_cold(census: BufferCensus, reads: MeasuredReads) -> None:
    """The shared-buffers half of the cold posture.

    Named for the half it proves. The host page cache is not observable from
    here, so a function called ``assert_cold`` would be claiming more than it
    checks — and the eviction declaration is what covers the rest.

    Both signals are required, because either alone can be satisfied by a warm
    run: an empty pool with a query that hits nothing read is a query that
    touched nothing, and a query that reads while the pool holds the relation
    read something else.
    """
    if census.buffers != 0:
        raise CachePostureError(
            f'{census.relation} holds {census.buffers} of its '
            f'{census.relation_blocks} blocks in shared buffers; the relation '
            'is resident and the measurement is not cold'
        )
    if reads.shared_read == 0:
        raise CachePostureError(
            'the measured query read no blocks into the pool, so it found '
            'everything it needed already there; a cold measurement reads'
        )
    if reads.shared_hit > reads.shared_read:
        raise CachePostureError(
            f'the measured query hit {reads.shared_hit} blocks against '
            f'{reads.shared_read} read; most of its work came from a pool it '
            'was supposed to have been evicted from'
        )


def assert_shared_buffers_warm(census: BufferCensus, reads: MeasuredReads) -> None:
    """The shared-buffers half of the warm posture.

    Warm is a claim too, and it fails in the opposite direction: a cell
    labelled warm that ran cold reports a latency its limit never intended.
    """
    if census.buffers == 0:
        raise CachePostureError(
            f'{census.relation} holds none of its {census.relation_blocks} '
            'blocks in shared buffers; a prepared-warm cell claims a resident '
            'relation and this one had nothing resident'
        )
    if reads.shared_read > reads.shared_hit:
        raise CachePostureError(
            f'the measured query read {reads.shared_read} blocks against '
            f'{reads.shared_hit} hit; a prepared-warm cell should find its '
            'blocks resident, and this one was substantially cold'
        )


BUFFER_CENSUS_SQL: Final = """
SELECT count(*)::bigint AS buffers,
       (pg_relation_size(%(relation)s::regclass) / current_setting(
           'block_size')::bigint)::bigint AS relation_blocks
FROM pg_buffercache
WHERE relfilenode = pg_relation_filenode(%(relation)s::regclass)
"""


def buffer_census_from_row(
    relation: str, row: Sequence[int]
) -> BufferCensus:
    """Build the census from the row ``BUFFER_CENSUS_SQL`` returns."""
    if len(row) != 2:
        raise CachePostureError(
            f'the buffer census query returned {len(row)} columns, expected 2'
        )
    return BufferCensus(
        relation=relation,
        buffers=int(row[0]),
        relation_blocks=int(row[1]),
    )


# What a plan node calls its read timing, newest name first. PostgreSQL 17
# split the single counter into shared, local and temp; a reader that knows
# only the older name finds nothing on a current server and reports no timing
# at all — the evidence disappears rather than announcing itself, so both
# names are looked for.
IO_READ_TIME_KEYS: Final = ('Shared I/O Read Time', 'I/O Read Time')


def measured_reads_from_plan(plan: dict[str, Any]) -> MeasuredReads:
    """Total the buffer counters over every node of an EXPLAIN plan.

    Summed over the whole tree rather than read off the root: the root of a
    plan reports its own node's counters in some shapes, and a cell that reads
    only the root can miss the scan that did the reading.
    """
    hit = 0
    read = 0
    io_time = 0.0
    saw_io_time = False
    pending: list[dict[str, Any]] = [plan]
    while pending:
        node = pending.pop()
        hit += int(node.get('Shared Hit Blocks', 0))
        read += int(node.get('Shared Read Blocks', 0))
        for key in IO_READ_TIME_KEYS:
            if key in node:
                io_time += float(node[key])
                saw_io_time = True
                break
        children: object = node.get('Plans')
        if isinstance(children, list):
            for child in children:  # pyright: ignore[reportUnknownVariableType]
                if isinstance(child, dict):
                    pending.append(child)  # pyright: ignore[reportUnknownArgumentType]
    return MeasuredReads(
        shared_hit=hit,
        shared_read=read,
        io_time_ms=io_time if saw_io_time else None,
    )
