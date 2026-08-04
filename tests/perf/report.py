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

from tests.perf.runner import Measurement, RunResult
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
    lines += [
        '',
        '## Server counts',
        '',
        '| | baseline | candidate |',
        '|---|---|---|',
        *_counts_rows(result.baseline, result.candidate),
        '',
        'Full-page images are reported and excluded from the byte comparison: '
        'whether a page is written for the first time since a checkpoint '
        'otherwise dominates the result.',
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
                {**asdict(c), 'verdict': c.verdict.value}
                for c in result.comparisons
            ],
            'verdict': result.verdict.value,
        },
        indent=2,
    )


def summary_filename(result: RunResult) -> str:
    """Dated and versioned, so two runs never overwrite each other."""
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
        f'| rows affected | {baseline.counts.rows_affected} '
        f'| {candidate.counts.rows_affected} |',
        f'| WAL records per row | {baseline.counts.wal_records_per_row:.2f} '
        f'| {candidate.counts.wal_records_per_row:.2f} |',
        f'| WAL bytes per row | {baseline.counts.wal_bytes_per_row:.0f} '
        f'| {candidate.counts.wal_bytes_per_row:.0f} |',
        f'| full-page images | {baseline.counts.wal_fpi} '
        f'| {candidate.counts.wal_fpi} |',
    ]


def _yes_no(value: bool) -> str:
    return 'yes' if value else 'no'
