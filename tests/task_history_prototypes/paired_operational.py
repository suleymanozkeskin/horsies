"""Operational rows report intervals, never one number over all of them.

§2.2: operational reports separate first-run, warm-cache, steady-state,
checkpoint and autovacuum intervals, and do not average them into one number. A
pooled percentile — what every paired-micro row reports — is forbidden here, and
for a reason the pooling would hide: a checkpoint interval and a steady-state
interval are different operations wearing one name, and their mixture is a
statistic about the mixing ratio rather than about either.

**Boundaries come from the server, not from reading the trace.** Every
observation is bracketed by a sample of what the server says is happening —
checkpoint counters, autovacuum workers, the relation's residency — and is
classified by what changed across it. An interval assigned by looking at a
latency plot and deciding where the shape shifts is an interval assigned by the
person who wanted a particular answer.

**Unclassifiable observations are their own reported bucket.** They are never
folded into steady-state. Steady-state is the healthy class, so folding the
unexplained into it is the flattering direction, and the flattering direction is
the one nobody investigates.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final


class OperationalReportError(Exception):
    """An operational row cannot be reported as asked."""


class OperationalInterval(StrEnum):
    """The five §2.2 intervals, plus the bucket for what fits none of them."""

    FIRST_RUN = 'first-run'
    WARM_CACHE = 'warm-cache'
    STEADY_STATE = 'steady-state'
    CHECKPOINT = 'checkpoint'
    AUTOVACUUM = 'autovacuum'
    UNCLASSIFIED = 'unclassified'


# Read before and after every observation. Each field is something the server
# reports about itself; none of it is inferred from the observation's own
# duration, because a classifier that reads the measurement it is classifying
# will find whatever shape it is looking for.
SERVER_EVENT_QUERY: Final = """
SELECT
  (SELECT num_timed + num_requested FROM pg_stat_checkpointer) AS checkpoints,
  (SELECT count(*) FROM pg_stat_activity
    WHERE backend_type = 'autovacuum worker') AS autovacuum_workers,
  (SELECT count(*) FROM pg_buffercache
    WHERE relfilenode = pg_relation_filenode(%(relation)s::regclass))
    AS relation_buffers
"""


@dataclass(frozen=True, slots=True)
class ServerEventSample:
    """What the server said was happening, at one instant."""

    checkpoints: int
    autovacuum_workers: int
    relation_buffers: int

    def as_conditions(self) -> dict[str, Any]:
        return {
            'checkpoints': self.checkpoints,
            'autovacuum_workers': self.autovacuum_workers,
            'relation_buffers': self.relation_buffers,
        }


def classify(
    before: ServerEventSample, after: ServerEventSample
) -> OperationalInterval:
    """Assign one observation to an interval, from the evidence around it.

    Order matters, and it runs from the most specific external event to the
    least. A checkpoint that lands during an autovacuum is reported as a
    checkpoint interval because the checkpoint is the larger, rarer event and
    the one the budget separates for.

    First-run against warm-cache is decided by residency rather than by
    position in the run: an observation that began with nothing resident ran
    first against that relation whatever its index, and one that filled more
    buffers while it ran was still warming. Steady-state is the narrow case —
    resident, and not filling.
    """
    if after.checkpoints > before.checkpoints:
        return OperationalInterval.CHECKPOINT
    if before.autovacuum_workers or after.autovacuum_workers:
        return OperationalInterval.AUTOVACUUM
    if before.relation_buffers == 0:
        return OperationalInterval.FIRST_RUN
    if after.relation_buffers > before.relation_buffers:
        return OperationalInterval.WARM_CACHE
    if after.relation_buffers == before.relation_buffers:
        return OperationalInterval.STEADY_STATE
    # Residency fell while the observation ran: something evicted the relation
    # underneath it. That is none of the five, and it is not steady-state.
    return OperationalInterval.UNCLASSIFIED


@dataclass(frozen=True, slots=True)
class ClassifiedObservation:
    """One observation, its duration, and the evidence that placed it."""

    index: int
    duration_ms: float
    interval: OperationalInterval
    before: ServerEventSample
    after: ServerEventSample

    def as_conditions(self) -> dict[str, Any]:
        return {
            'index': self.index,
            'duration_ms': self.duration_ms,
            'interval': self.interval.value,
            'before': self.before.as_conditions(),
            'after': self.after.as_conditions(),
        }


def classify_observations(
    durations: Sequence[float], samples: Sequence[tuple[ServerEventSample, ServerEventSample]]
) -> tuple[ClassifiedObservation, ...]:
    """Pair each duration with the evidence bracketing it."""
    if len(durations) != len(samples):
        raise OperationalReportError(
            f'{len(durations)} observations against {len(samples)} evidence '
            'pairs; an observation with no evidence around it cannot be '
            'placed in an interval'
        )
    if not durations:
        raise OperationalReportError('an operational row needs observations')
    return tuple(
        ClassifiedObservation(
            index=index,
            duration_ms=duration,
            interval=classify(before, after),
            before=before,
            after=after,
        )
        for index, (duration, (before, after)) in enumerate(
            zip(durations, samples, strict=True)
        )
    )


def _percentile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    rank = int(round(fraction * (len(ordered) - 1)))
    return ordered[rank]


def _rank_limited(count: int, fraction: float) -> bool:
    """Whether this percentile resolves to the sample's maximum.

    An interval with few observations still has a p99 arithmetically, and that
    p99 is its largest value. Reporting it is useful; judging a budget against
    it is not, because one outlier sets it.
    """
    return int(round(fraction * (count - 1))) >= count - 1


@dataclass(frozen=True, slots=True)
class IntervalStatistics:
    """One interval's numbers, reported on their own."""

    interval: OperationalInterval
    count: int
    p50: float
    p95: float
    p99: float
    maximum: float
    rank_limited: frozenset[str]

    def as_conditions(self) -> dict[str, Any]:
        return {
            'interval': self.interval.value,
            'count': self.count,
            'p50': self.p50,
            'p95': self.p95,
            'p99': self.p99,
            'max': self.maximum,
            'rank_limited': sorted(self.rank_limited),
        }


@dataclass(frozen=True, slots=True)
class OperationalReport:
    """Every interval, separately, and no number spanning them.

    There is deliberately no pooled statistic on this type. §2.2 forbids
    averaging the intervals into one number, and the way to keep a forbidden
    number out of a report is to give the report no way to produce it.
    """

    row: str
    observations: tuple[ClassifiedObservation, ...]

    def __post_init__(self) -> None:
        if not self.observations:
            raise OperationalReportError(
                f'{self.row}: no observations to report'
            )

    def by_interval(self) -> dict[OperationalInterval, IntervalStatistics]:
        grouped: dict[OperationalInterval, list[float]] = {}
        for observation in self.observations:
            grouped.setdefault(observation.interval, []).append(
                observation.duration_ms
            )
        return {
            interval: IntervalStatistics(
                interval=interval,
                count=len(values),
                p50=_percentile(values, 0.50),
                p95=_percentile(values, 0.95),
                p99=_percentile(values, 0.99),
                maximum=max(values),
                rank_limited=frozenset(
                    name
                    for name, fraction in (
                        ('p50', 0.50), ('p95', 0.95), ('p99', 0.99)
                    )
                    if _rank_limited(len(values), fraction)
                ),
            )
            for interval, values in grouped.items()
        }

    def unclassified_count(self) -> int:
        return sum(
            1
            for observation in self.observations
            if observation.interval is OperationalInterval.UNCLASSIFIED
        )

    def as_conditions(self) -> dict[str, Any]:
        statistics = self.by_interval()
        return {
            'row': self.row,
            'observations': len(self.observations),
            'intervals': {
                interval.value: statistics[interval].as_conditions()
                for interval in OperationalInterval
                if interval in statistics
            },
            'unclassified': self.unclassified_count(),
            'pooled_statistic': (
                'not reported: section 2.2 separates first-run, warm-cache, '
                'steady-state, checkpoint and autovacuum intervals and does '
                'not average them into one number'
            ),
        }


def judge_interval(
    report: OperationalReport,
    *,
    interval: OperationalInterval,
    statistic: str,
    limit_ms: float,
) -> dict[str, Any]:
    """Judge one interval against a budget, naming the interval it judged.

    A §9 budget is a number about an operation, and which interval it governs
    is part of reading it: two seconds p99 for a health pass is not a promise
    about the pass that happened to run through a checkpoint. The interval is
    therefore named in the verdict rather than assumed.
    """
    statistics = report.by_interval()
    if interval not in statistics:
        raise OperationalReportError(
            f'{report.row}: no observations fell in the {interval} interval, '
            'so it cannot be judged. An interval with no observations is a '
            'row that was not measured, not a row that passed'
        )
    measured = statistics[interval]
    value = {
        'p50': measured.p50,
        'p95': measured.p95,
        'p99': measured.p99,
    }.get(statistic)
    if value is None:
        raise OperationalReportError(
            f'no statistic named {statistic} on an interval report'
        )
    if statistic in measured.rank_limited:
        raise OperationalReportError(
            f'{report.row}: {statistic} over {measured.count} observations in '
            f'the {interval} interval is the maximum of that sample, not a '
            'percentile; one outlier sets it, so it cannot carry a budget '
            'verdict'
        )
    return {
        'row': report.row,
        'interval': interval.value,
        'statistic': statistic,
        'value': value,
        'limit_ms': limit_ms,
        'count': measured.count,
        'within_limit': value <= limit_ms,
    }


@dataclass(frozen=True, slots=True)
class CompetingReader:
    """The process §2.2 permits to stay awake, declared rather than assumed.

    The no-blocking row needs something to be blocked by. That makes the reader
    an instrument setting: a row measured against a reader running every 10 ms
    and one measured against a reader running every 10 s are not the same row,
    and only the declaration says which was run.
    """

    statement: str
    cadence_ms: int
    relations: tuple[str, ...]
    backend_label: str

    def __post_init__(self) -> None:
        if self.cadence_ms < 1:
            raise OperationalReportError(
                f'cadence must be at least 1 ms, got {self.cadence_ms}'
            )
        if not self.relations:
            raise OperationalReportError(
                'a competing reader must name the relations it reads; a reader '
                'against unnamed relations cannot be shown to compete with the '
                'operation under test'
            )

    def as_conditions(self) -> Mapping[str, Any]:
        return {
            'statement': self.statement,
            'cadence_ms': self.cadence_ms,
            'relations': list(self.relations),
            'backend_label': self.backend_label,
        }


@dataclass(frozen=True, slots=True)
class ThresholdVerdict:
    """A budget stated as a threshold, judged as one.

    Some budgets name no percentile — "completes within 5 seconds for a
    1-million-row detached leaf" is a bound on the operation, not on a
    distribution. Choosing a percentile for such a line invents a statistic the
    budget never stated, which is the authoring-omission trap in another guise.

    So every observation must individually satisfy the threshold, and the
    verdict reports how many there were and the worst of them. The rank-limit
    machinery is inapplicable here by construction rather than by exemption:
    there is no percentile to be the maximum of, because the maximum IS what is
    judged.
    """

    row: str
    interval: OperationalInterval
    limit_ms: float
    count: int
    maximum_ms: float
    exceeded: tuple[float, ...]

    @property
    def within_limit(self) -> bool:
        return not self.exceeded

    def as_conditions(self) -> dict[str, Any]:
        return {
            'row': self.row,
            'interval': self.interval.value,
            'verdict_form': 'threshold',
            'limit_ms': self.limit_ms,
            'count': self.count,
            'max_ms': self.maximum_ms,
            'exceeded': list(self.exceeded),
            'within_limit': self.within_limit,
            'rank_limit': (
                'inapplicable by construction: a threshold judges every '
                'observation individually, so there is no percentile that '
                'could resolve to the sample maximum'
            ),
        }


MINIMUM_THRESHOLD_OBSERVATIONS: Final = 3


def judge_threshold(
    report: OperationalReport,
    *,
    interval: OperationalInterval,
    limit_ms: float,
    minimum_observations: int = MINIMUM_THRESHOLD_OBSERVATIONS,
) -> ThresholdVerdict:
    """Judge a stated threshold across repeats, never on a single draw.

    One observation under a bound is a lucky draw wearing a verdict. Several,
    each individually under it, is a claim.
    """
    statistics = report.by_interval()
    if interval not in statistics:
        raise OperationalReportError(
            f'{report.row}: no observations fell in the {interval} interval, '
            'so it cannot be judged. An interval with no observations is a row '
            'that was not measured, not a row that passed'
        )
    values = tuple(
        observation.duration_ms
        for observation in report.observations
        if observation.interval is interval
    )
    if len(values) < minimum_observations:
        raise OperationalReportError(
            f'{report.row}: {len(values)} observation(s) in the {interval} '
            f'interval, and a threshold verdict rests on at least '
            f'{minimum_observations}. One observation under a bound is a lucky '
            'draw wearing a verdict'
        )
    return ThresholdVerdict(
        row=report.row,
        interval=interval,
        limit_ms=limit_ms,
        count=len(values),
        maximum_ms=max(values),
        exceeded=tuple(value for value in values if value > limit_ms),
    )
