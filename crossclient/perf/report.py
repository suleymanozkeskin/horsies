"""Rendering a run as the artifact that outlives it.

A summary is a public repository artifact, so it states conditions and results
and nothing else: no internal identifiers, no references to planning documents,
no shorthand that resolves only for whoever ran it. "Fused completion, PG16,
10,000 paired observations, fsync off: dp50 +0.11 ms, pass" answers a reader's
question in a year. A verdict on its own answers nobody's.

Raw samples are not written here. They are large, they compress badly in a
repository, and nothing about them is readable — they belong in run artifact
storage, which is where the caller sends them.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict
from enum import Enum
from typing import Final, assert_never, cast

from crossclient.perf.metrics_scrape import EndpointCounts, ReadTally
from crossclient.perf.readings import (
    BlockReadings,
    Cost,
    DrainReading,
    DrainReadings,
    MeasuredCost,
    Readings,
    StatementReadings,
    UnmeasuredCost,
)
from crossclient.perf.runner import (
    WAL_RECORD_DELTA_PER_TERMINAL_ROW,
    Measurement,
    RunResult,
)
from crossclient.perf.telemetry_levels import NoEndpoint, NotRead
from crossclient.perf.scenarios import (
    BatchScenario,
    PublicCallScenario,
    SingleRowScenario,
    WorkerDrainScenario,
)
from crossclient.perf.statistics import Comparison, ThroughputComparison


def render_summary(result: RunResult) -> str:
    """The tracked artifact: conditions, then results, then the verdict."""
    conditions = result.conditions
    batch = (
        f'{conditions.batch_size} rows'
        if conditions.batch_size is not None
        else 'not applicable'
    )
    lines = [
        f'# {conditions.scenario}',
        '',
        conditions.description,
        '',
        '## Conditions',
        '',
        '| | |',
        '|---|---|',
        f'| measured | {conditions.measured_at} |',
        f'| server | PostgreSQL {conditions.server_version} |',
        f'| compared | {conditions.comparison} |',
        f'| observations per side | {conditions.observations_per_side} |',
        f'| block size | {conditions.block_size} |',
        f'| pre-existing terminal rows | {conditions.ballast_rows} |',
        f'| payload per operation | {conditions.payload_bytes} bytes |',
        f'| batch | {batch} |',
        f'| fsync | {conditions.fsync} |',
        f'| full_page_writes | {conditions.full_page_writes} |',
        f'| synchronous_commit | {conditions.synchronous_commit} |',
        f'| autovacuum | {conditions.autovacuum} |',
        f'| bootstrap resamples | {conditions.resamples} |',
        f'| bootstrap seed | {conditions.seed} |',
        f'| demo units quiesced | {_yes_no(conditions.demo_quiesced)} |',
        f'| telemetry of the candidate side | `{conditions.telemetry_level}`: '
        f'{conditions.telemetry_description} |',
        f'| PostgreSQL CPU time | {conditions.postgres_cpu_source} |',
    ]
    match result.scenario:
        case SingleRowScenario():
            lines += _latency_section(result.comparisons)
        case PublicCallScenario():
            lines += _latency_section(result.comparisons)
            lines += [
                '',
                'Calls per second (one client, one call at a time): baseline '
                f'{_calls_per_second(result.baseline.samples_ms):.0f}, candidate '
                f'{_calls_per_second(result.candidate.samples_ms):.0f}.',
            ]
        case BatchScenario():
            lines += _batch_performance(
                _required_throughput(result),
                result.comparisons,
            )
        case WorkerDrainScenario():
            lines += _drain_performance(_required_throughput(result))
        case _ as unreachable:
            assert_never(unreachable)
    lines += _cost_section(result.costs)
    lines += _telemetry_section(result.baseline.readings, result.candidate.readings)
    if result.plans is not None:
        lines += [
            '',
            '## Instrumented plan evidence',
            '',
            'One eligible transition per side, executed with '
            '`EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and '
            'rolled back. These executions are excluded from the latency and '
            'server-count measurements.',
            '',
            '| | baseline | candidate |',
            '|---|---|---|',
            *_plan_rows(result.plans.baseline, result.plans.candidate),
        ]
    lines += [
        '',
        '## Server counts',
        '',
        '| | baseline | candidate |',
        '|---|---|---|',
        *_counts_rows(result.baseline, result.candidate),
        '',
        '## Contract checks',
        '',
        'Limits: client statements may not increase; write transactions and '
        'terminal task rows must match; WAL-record delta must be at most '
        f'{WAL_RECORD_DELTA_PER_TERMINAL_ROW:.3f} per terminal task row; '
        'WAL-byte delta must be at most the greater of 10% or 128 bytes per '
        'terminal task row.',
        '',
        *(
            [f'- FAIL: {violation}' for violation in result.contract_violations]
            if result.contract_violations
            else ['- PASS']
        ),
        '',
        'Full-page writes are disabled in this disposable measurement '
        'environment, so checkpoint-dependent image bytes are absent from '
        'the WAL-byte comparison.',
        '',
        f'**Verdict: {result.verdict.value}**',
        '',
    ]
    return '\n'.join(lines)


def render_raw(result: RunResult) -> str:
    """Everything, including samples, for artifact storage rather than the repo."""
    return json.dumps(
        {
            'conditions': asdict(result.conditions),
            'baseline': {
                'samples_ms': result.baseline.samples_ms,
                'counts': asdict(result.baseline.counts),
                'readings': _raw_readings(result.baseline.readings),
            },
            'candidate': {
                'samples_ms': result.candidate.samples_ms,
                'counts': asdict(result.candidate.counts),
                'readings': _raw_readings(result.candidate.readings),
            },
            'costs': [_raw_cost(cost) for cost in result.costs],
            'comparisons': [
                {**asdict(c), 'verdict': c.verdict.value} for c in result.comparisons
            ],
            'throughput': (
                {
                    **asdict(result.throughput),
                    'verdict': result.throughput.verdict.value,
                }
                if result.throughput is not None
                else None
            ),
            'plans': asdict(result.plans) if result.plans is not None else None,
            'contract_violations': list(result.contract_violations),
            'verdict': result.verdict.value,
        },
        indent=2,
    )


def summary_filename(result: RunResult) -> str:
    """Name the run by its start second, scenario, telemetry level and server
    major, so repeated runs of one cell on one day keep a file each."""
    conditions = result.conditions
    day = conditions.measured_at[:10]
    clock = conditions.measured_at[11:19].replace(':', '')
    major = conditions.server_version.split('.')[0]
    return (
        f'{day}-{clock}-{conditions.scenario}-telemetry-'
        f'{conditions.telemetry_level}-pg{major}.md'
    )


def _latency_section(comparisons: list[Comparison]) -> list[str]:
    return [
        '',
        '## Latency',
        '',
        '| percentile | baseline | candidate | delta | 95% interval | budget | verdict |',
        '|---|---|---|---|---|---|---|',
        *[_latency_row(c) for c in comparisons],
    ]


def _calls_per_second(samples_ms: list[float]) -> float:
    """The calls of a side over the sum of their times."""
    return len(samples_ms) * 1_000 / sum(samples_ms)


def _latency_row(comparison: Comparison) -> str:
    return (
        f'| p{comparison.percentile:.0f} '
        f'| {comparison.baseline_ms:.3f} ms '
        f'| {comparison.candidate_ms:.3f} ms '
        f'| {comparison.delta_ms:+.3f} ms '
        f'| {comparison.ci_low_ms:+.3f} to {comparison.ci_high_ms:+.3f} ms '
        f'| {comparison.limit_ms:.3f} ms '
        f'| {comparison.verdict.value} |'
    )


def _batch_performance(
    throughput: ThroughputComparison,
    durations: list[Comparison],
) -> list[str]:
    if len(durations) != 1 or durations[0].percentile != 95.0:
        raise RuntimeError('a batch result requires exactly one p95 duration')
    duration = durations[0]
    return [
        '',
        '## Batch performance',
        '',
        '| metric | baseline | candidate | comparison | 95% interval | budget | verdict |',
        '|---|---|---|---|---|---|---|',
        '| throughput '
        f'| {throughput.baseline_rows_per_second:.0f} rows/s '
        f'| {throughput.candidate_rows_per_second:.0f} rows/s '
        f'| {throughput.ratio:.3f}x '
        f'| {throughput.ci_low_ratio:.3f}x to '
        f'{throughput.ci_high_ratio:.3f}x '
        f'| >= {throughput.minimum_ratio:.2f}x '
        f'| {throughput.verdict.value} |',
        '| p95 statement-to-commit duration '
        f'| {duration.baseline_ms:.3f} ms '
        f'| {duration.candidate_ms:.3f} ms '
        f'| {duration.delta_ms:+.3f} ms '
        f'| {duration.ci_low_ms:+.3f} to {duration.ci_high_ms:+.3f} ms '
        f'| {duration.limit_ms:.3f} ms '
        f'| {duration.verdict.value} |',
        '',
        'The duration envelope starts before the statement can acquire its '
        'first row lock and ends after commit releases every lock. It includes '
        'result decoding and outcome logging performed before commit.',
    ]


def _required_throughput(result: RunResult) -> ThroughputComparison:
    """The throughput of a batch or drain result, which the runner computes
    for every such result."""
    if result.throughput is None:
        raise RuntimeError(
            f'{result.conditions.scenario}: a batch or drain result has no '
            'throughput comparison'
        )
    return result.throughput


def _drain_performance(throughput: ThroughputComparison) -> list[str]:
    return [
        '',
        '## Drain throughput',
        '',
        '| metric | baseline | candidate | comparison | 95% interval | budget | verdict |',
        '|---|---|---|---|---|---|---|',
        '| tasks per second '
        f'| {throughput.baseline_rows_per_second:.0f} tasks/s '
        f'| {throughput.candidate_rows_per_second:.0f} tasks/s '
        f'| {throughput.ratio:.3f}x '
        f'| {throughput.ci_low_ratio:.3f}x to '
        f'{throughput.ci_high_ratio:.3f}x '
        f'| >= {throughput.minimum_ratio:.2f}x '
        f'| {throughput.verdict.value} |',
        '',
        'Throughput is the sent tasks of the drains divided by their total '
        'time. The time of a drain includes the start of the worker process, '
        'and ends at the start of the count that read every task terminal. '
        'The server counts include the statements of the worker from its '
        'start until that count, without the counts of the harness.',
    ]


def _counts_rows(baseline: Measurement, candidate: Measurement) -> list[str]:
    return [
        f'| client statements | {baseline.counts.client_statements} '
        f'| {candidate.counts.client_statements} |',
        f'| statements inside functions | {baseline.counts.nested_statements} '
        f'| {candidate.counts.nested_statements} |',
        f'| write transactions | {baseline.counts.write_transactions} '
        f'| {candidate.counts.write_transactions} |',
        f'| client rows | {baseline.counts.client_rows} '
        f'| {candidate.counts.client_rows} |',
        f'| nested rows | {baseline.counts.nested_rows} '
        f'| {candidate.counts.nested_rows} |',
        f'| terminal task rows | {baseline.counts.terminal_rows} '
        f'| {candidate.counts.terminal_rows} |',
        '| WAL records per terminal task row '
        f'| {baseline.counts.wal_records_per_row:.2f} '
        f'| {candidate.counts.wal_records_per_row:.2f} |',
        '| WAL bytes per terminal task row '
        f'| {baseline.counts.wal_bytes_per_row:.0f} '
        f'| {candidate.counts.wal_bytes_per_row:.0f} |',
        f'| full-page images | {baseline.counts.wal_fpi} '
        f'| {candidate.counts.wal_fpi} |',
    ]


def _plan_rows(
    baseline_document: dict[str, object],
    candidate_document: dict[str, object],
) -> list[str]:
    baseline = _root_plan(baseline_document)
    candidate = _root_plan(candidate_document)
    return [
        f'| node shape | `{_node_shape(baseline)}` ' f'| `{_node_shape(candidate)}` |',
        f'| shared hit blocks | {_integer(baseline, "Shared Hit Blocks")} '
        f'| {_integer(candidate, "Shared Hit Blocks")} |',
        f'| shared read blocks | {_integer(baseline, "Shared Read Blocks")} '
        f'| {_integer(candidate, "Shared Read Blocks")} |',
        f'| shared dirtied blocks | {_integer(baseline, "Shared Dirtied Blocks")} '
        f'| {_integer(candidate, "Shared Dirtied Blocks")} |',
        f'| shared written blocks | {_integer(baseline, "Shared Written Blocks")} '
        f'| {_integer(candidate, "Shared Written Blocks")} |',
        f'| WAL records | {_integer(baseline, "WAL Records")} '
        f'| {_integer(candidate, "WAL Records")} |',
        f'| WAL full-page images | {_integer(baseline, "WAL FPI")} '
        f'| {_integer(candidate, "WAL FPI")} |',
        f'| WAL bytes | {_integer(baseline, "WAL Bytes")} '
        f'| {_integer(candidate, "WAL Bytes")} |',
    ]


def _root_plan(document: dict[str, object]) -> dict[str, object]:
    plan = document.get('Plan')
    if not isinstance(plan, dict):
        raise RuntimeError('EXPLAIN document has no root Plan object')
    return cast(dict[str, object], plan)


def _node_shape(node: dict[str, object]) -> str:
    label = str(node.get('Node Type', 'unknown'))
    detail = node.get('Relation Name') or node.get('Function Name')
    if detail is not None:
        label += f'({detail})'
    children = node.get('Plans', [])
    if not isinstance(children, list):
        raise RuntimeError('EXPLAIN Plan children are not a list')
    descendants = [
        _node_shape(cast(dict[str, object], child))
        for child in cast(list[object], children)
        if isinstance(child, dict)
    ]
    return ' > '.join([label, *descendants])


def _integer(plan: dict[str, object], key: str) -> int:
    value = plan.get(key, 0)
    if not isinstance(value, int):
        raise RuntimeError(f'EXPLAIN field {key!r} is not an integer')
    return value


def _cost_section(costs: tuple[Cost, ...]) -> list[str]:
    """The CPU and stop costs of a public call or a drain run. No verdict: a
    reader judges each ratio against the spread of the control runs."""
    if not costs:
        return []
    return [
        '',
        '## Resources',
        '',
        '| measure | baseline | candidate | ratio | 95% interval |',
        '|---|---|---|---|---|',
        *[_cost_row(cost) for cost in costs],
        '',
        'A cost per operation is the sum of its readings over the sum of the '
        'operations of a side; the interval resamples the drains or the '
        'blocks. Worker CPU is the CPU time of the worker parent and of each '
        'process below it, from the start of the worker to the end of the '
        'drain; the stop time runs from SIGTERM to the exit. Producer CPU is '
        'the CPU time of the harness process across a block of calls.',
    ]


def _cost_row(cost: Cost) -> str:
    match cost:
        case MeasuredCost(measure=measure, comparison=comparison):
            return (
                f'| {measure.value} '
                f'| {comparison.baseline_per_operation_s * 1_000:.4f} ms '
                f'| {comparison.candidate_per_operation_s * 1_000:.4f} ms '
                f'| {comparison.ratio:.3f}x '
                f'| {comparison.ci_low_ratio:.3f}x to {comparison.ci_high_ratio:.3f}x |'
            )
        case UnmeasuredCost(measure=measure, reason=reason):
            return f'| {measure.value} | not measured: {reason} | | | |'
        case _ as unreachable:
            assert_never(unreachable)


def _telemetry_section(baseline: Readings, candidate: Readings) -> list[str]:
    """The telemetry columns of a drain run: the endpoint counts of each side
    and the answers to the reads during the drains."""
    match baseline, candidate:
        case DrainReadings(drains=baseline_drains), DrainReadings(
            drains=candidate_drains
        ):
            pass
        case _:
            return []
    return [
        '',
        '## Telemetry',
        '',
        '| | baseline | candidate |',
        '|---|---|---|',
        *[
            f'| {label} | {_endpoint_value(baseline_drains, over_drains, read)} '
            f'| {_endpoint_value(candidate_drains, over_drains, read)} |'
            for label, over_drains, read in _ENDPOINT_COLUMNS
        ],
        *[
            f'| {label} | {_tally_value(baseline_drains, read)} '
            f'| {_tally_value(candidate_drains, read)} |'
            for label, read in _TALLY_COLUMNS
        ],
        '',
        'The endpoint columns come from one read of `/metrics` at the end of '
        'each drain: the series count is the largest over the drains, each '
        'drop count is the sum over the drains. A read during a drain that '
        'was not answered includes the reads before the worker bound its '
        'endpoint.',
    ]


class _OverDrains(Enum):
    """How a column of the endpoint reads combines its drains."""

    LARGEST = 'largest'
    SUM = 'sum'


_ENDPOINT_COLUMNS: Final[
    tuple[tuple[str, _OverDrains, Callable[[EndpointCounts], int]], ...]
] = (
    ('series (largest)', _OverDrains.LARGEST, lambda counts: counts.series),
    (
        'events dropped, queue full',
        _OverDrains.SUM,
        lambda counts: counts.events_dropped_queue_full,
    ),
    (
        'events dropped, handler child down',
        _OverDrains.SUM,
        lambda counts: counts.events_dropped_child_down,
    ),
    ('series dropped', _OverDrains.SUM, lambda counts: counts.series_dropped),
    ('handler events dropped', _OverDrains.SUM, lambda counts: counts.handler_dropped),
    ('handler errors', _OverDrains.SUM, lambda counts: counts.handler_errors),
)

_TALLY_COLUMNS: Final[tuple[tuple[str, Callable[[ReadTally], int]], ...]] = (
    ('`/metrics` reads answered', lambda tally: tally.metrics_answered),
    ('`/metrics` reads not answered', lambda tally: tally.metrics_not_answered),
    ('`/livez` live', lambda tally: tally.livez_live),
    ('`/livez` late (503)', lambda tally: tally.livez_late),
    ('`/livez` not answered', lambda tally: tally.livez_not_answered),
)


def _endpoint_value(
    drains: tuple[DrainReading, ...],
    over_drains: _OverDrains,
    read: Callable[[EndpointCounts], int],
) -> str:
    """The column over the drains; 'no endpoint' at a level without one."""
    values: list[int] = []
    for drain in drains:
        match drain.endpoint:
            case EndpointCounts() as counts:
                values.append(read(counts))
            case NoEndpoint():
                return 'no endpoint'
            case _ as unreachable:
                assert_never(unreachable)
    if not values:
        return 'no drain'
    match over_drains:
        case _OverDrains.LARGEST:
            return str(max(values))
        case _OverDrains.SUM:
            return str(sum(values))
        case _ as unreachable:
            assert_never(unreachable)


def _tally_value(
    drains: tuple[DrainReading, ...],
    read: Callable[[ReadTally], int],
) -> str:
    """The sum of a read count over the drains; 'not read' at a level that
    does not read the endpoint during a drain."""
    total = 0
    for drain in drains:
        match drain.reads:
            case ReadTally() as tally:
                total += read(tally)
            case NotRead():
                return 'not read'
            case _ as unreachable:
                assert_never(unreachable)
    return str(total)


def _raw_readings(readings: Readings) -> dict[str, object]:
    match readings:
        case StatementReadings():
            return {'kind': 'statement'}
        case BlockReadings(blocks=blocks):
            return {'kind': 'blocks', 'blocks': [asdict(block) for block in blocks]}
        case DrainReadings(drains=drains):
            return {'kind': 'drains', 'drains': [asdict(drain) for drain in drains]}
        case _ as unreachable:
            assert_never(unreachable)


def _raw_cost(cost: Cost) -> dict[str, object]:
    match cost:
        case MeasuredCost(measure=measure, comparison=comparison):
            return {'measure': measure.value, **asdict(comparison)}
        case UnmeasuredCost(measure=measure, reason=reason):
            return {'measure': measure.value, 'not_measured': reason}
        case _ as unreachable:
            assert_never(unreachable)


def _yes_no(value: bool) -> str:
    return 'yes' if value else 'no'
