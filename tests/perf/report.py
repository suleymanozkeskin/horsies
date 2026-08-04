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
from dataclasses import asdict
from typing import cast

from tests.perf.runner import (
    WAL_RECORD_DELTA_PER_TERMINAL_ROW,
    Measurement,
    RunResult,
)
from tests.perf.statistics import Comparison


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
        f'| result payload | {conditions.payload_bytes} bytes |',
        f'| batch | {batch} |',
        f'| fsync | {conditions.fsync} |',
        f'| full_page_writes | {conditions.full_page_writes} |',
        f'| synchronous_commit | {conditions.synchronous_commit} |',
        f'| autovacuum | {conditions.autovacuum} |',
        f'| bootstrap resamples | {conditions.resamples} |',
        f'| bootstrap seed | {conditions.seed} |',
        f'| demo units quiesced | {_yes_no(conditions.demo_quiesced)} |',
        '',
        '## Latency',
        '',
        '| percentile | baseline | candidate | delta | 95% interval | budget | verdict |',
        '|---|---|---|---|---|---|---|',
    ]
    lines += [_latency_row(c) for c in result.comparisons]
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
            },
            'candidate': {
                'samples_ms': result.candidate.samples_ms,
                'counts': asdict(result.candidate.counts),
            },
            'comparisons': [
                {**asdict(c), 'verdict': c.verdict.value} for c in result.comparisons
            ],
            'plans': asdict(result.plans) if result.plans is not None else None,
            'contract_violations': list(result.contract_violations),
            'verdict': result.verdict.value,
        },
        indent=2,
    )


def summary_filename(result: RunResult) -> str:
    """Name the authoritative run for one day, scenario, and server major."""
    conditions = result.conditions
    day = conditions.measured_at[:10]
    major = conditions.server_version.split('.')[0]
    return f'{day}-{conditions.scenario}-pg{major}.md'


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


def _yes_no(value: bool) -> str:
    return 'yes' if value else 'no'
