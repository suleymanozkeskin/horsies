"""Run a measurement and write its artifacts.

Two scales, and the difference is not just sample count. A smoke run proves the
harness still works and is expected on any change to it; a gate run produces
evidence that a phase is allowed to proceed, and refuses to start unless the
conditions that make its numbers meaningful have been stated.

    uv run python -m tests.perf --scenario fused-completion-small-result \\
        --dsn postgresql+psycopg://postgres:testpassword@localhost:15446/horsies \\
        --scale smoke --control

    uv run python -m tests.perf --scenario fused-completion-small-result \\
        --dsn ... --scale gate --demo-quiesced
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import create_engine

from tests.perf.prepare import apply_schema
from tests.perf.report import render_raw, render_summary, summary_filename
from tests.perf.runner import RunResult, run_scenario, server_setting
from tests.perf.scenarios import (
    SCENARIOS,
    BatchScenario,
    Scenario,
    scenario_by_name,
)
from tests.perf.statistics import Verdict

RESULTS_DIR = Path(__file__).parent / 'results'
RAW_DIR = RESULTS_DIR / 'raw'

# A gate run has to say something about the tail, and 10,000 observations is
# what makes a p99 interval narrow enough to decide anything. Batches are
# counted in batches rather than rows: a hundred of them at the size the
# runtime issues is the stated requirement, and ten thousand would seed five
# million rows to answer a question a hundred already answers. A smoke run is
# proving the machinery, so it takes the smallest count that still exercises
# every code path.
GATE_OBSERVATIONS = 10_000
GATE_BATCHES = 100
SMOKE_OBSERVATIONS = 200
SMOKE_BATCHES = 10
GATE_RESAMPLES = 1_000
SMOKE_RESAMPLES = 200

# The server settings a gate run refuses to start without. Each is part of
# the measurement contract with its reason in the compose file; a gate run
# against a server reporting anything else is measuring a different
# environment than the one the budgets were declared for.
GATE_ENVIRONMENT: dict[str, str] = {
    'autovacuum': 'off',
    'fsync': 'off',
    'full_page_writes': 'off',
    'synchronous_commit': 'off',
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='tests.perf')
    parser.add_argument('--dsn', required=True)
    parser.add_argument(
        '--apply-schema',
        action='store_true',
        help='install the schema at --dsn and exit, measuring nothing',
    )
    parser.add_argument(
        '--scenario',
        choices=[s.name for s in SCENARIOS] + ['all', 'candidates'],
    )
    parser.add_argument('--scale', choices=['smoke', 'gate'])
    parser.add_argument('--block-size', type=int, default=100)
    parser.add_argument(
        '--observations',
        type=int,
        help=(
            'per-side operation count, or batch count for batch scenarios; '
            'use a value above the gate minimum when a confidence interval '
            'is inconclusive'
        ),
    )
    parser.add_argument('--seed', type=int, default=20260804)
    parser.add_argument(
        '--demo-quiesced',
        action='store_true',
        help=(
            'the host runs no background workload. Required for a gate run: '
            'the demo units on the showcase host generate constant churn, and '
            'measuring against it relocates the noisy-neighbour problem rather '
            'than solving it.'
        ),
    )
    parser.add_argument('--write-summary', action='store_true')
    parser.add_argument(
        '--control',
        action='store_true',
        help='run every candidate side with its baseline implementation',
    )
    arguments = parser.parse_args(argv)

    if arguments.apply_schema:
        apply_schema(arguments.dsn)
        print(f'schema applied at {arguments.dsn.rsplit("@", 1)[-1]}')
        return 0

    if arguments.scenario is None or arguments.scale is None:
        parser.error('--scenario and --scale are required unless --apply-schema')

    if arguments.scale == 'gate' and not arguments.demo_quiesced:
        parser.error(
            'a gate run requires --demo-quiesced. Stop the demo units first '
            '(the stop-units-first procedure), then state it here so the '
            'condition is recorded with the result.'
        )

    resamples = GATE_RESAMPLES if arguments.scale == 'gate' else SMOKE_RESAMPLES
    scenarios = _select_scenarios(arguments.scenario)
    if arguments.observations is not None and arguments.observations <= 0:
        parser.error('--observations must be positive')
    if arguments.scale == 'gate' and arguments.observations is not None:
        undersized = [
            scenario.name
            for scenario in scenarios
            if arguments.observations < _minimum_observations(scenario, 'gate')
        ]
        if undersized:
            parser.error(
                '--observations cannot lower the gate minimum for: '
                + ', '.join(undersized)
            )
    if arguments.scale == 'gate' and not arguments.control:
        missing = [
            scenario.name for scenario in scenarios if scenario.candidate is None
        ]
        if missing:
            parser.error(
                'a gate requires a real candidate; missing for: ' + ', '.join(missing)
            )

    engine = create_engine(arguments.dsn)
    outcomes: list[tuple[Verdict, bool]] = []
    control_failures: list[str] = []
    try:
        if arguments.scale == 'gate':
            with engine.connect() as connection:
                observed = {
                    name: server_setting(connection, name) for name in GATE_ENVIRONMENT
                }
            violations = _gate_environment_violations(observed)
            if violations:
                for violation in violations:
                    print(f'gate run refused: {violation}', file=sys.stderr)
                return 2
        for scenario in scenarios:
            result = run_scenario(
                engine,
                scenario=scenario,
                observations=_observations_for(
                    scenario,
                    arguments.scale,
                    arguments.observations,
                ),
                block_size=_block_size_for(scenario, arguments.block_size),
                resamples=resamples,
                seed=arguments.seed,
                demo_quiesced=arguments.demo_quiesced,
                control=arguments.control,
            )
            summary = render_summary(result)
            print(summary)
            outcomes.append(
                (result.verdict, 'control' in result.conditions.comparison),
            )
            control_failures += _control_disagreements(result)

            if arguments.write_summary:
                RESULTS_DIR.mkdir(parents=True, exist_ok=True)
                RAW_DIR.mkdir(parents=True, exist_ok=True)
                name = summary_filename(result)
                (RESULTS_DIR / name).write_text(summary, encoding='utf-8')
                (RAW_DIR / f'{name[:-3]}.json').write_text(
                    render_raw(result),
                    encoding='utf-8',
                )
                print(f'wrote {RESULTS_DIR / name}')
    finally:
        engine.dispose()

    if control_failures:
        for failure in control_failures:
            print(f'control run disagrees with itself: {failure}', file=sys.stderr)
        return 1

    return _exit_status(arguments.scale, outcomes)


def _gate_environment_violations(observed: dict[str, str]) -> list[str]:
    """Contract settings the server does not report as required.

    Verified by SHOW against the live server rather than trusted from the
    compose file, for the same reason --demo-quiesced must be stated: a gate
    number is only evidence under its stated conditions, and the conditions
    are the server's to answer for.
    """
    return [
        f'{name} is {observed.get(name)!r}; '
        f'the measurement contract requires {required!r}'
        for name, required in GATE_ENVIRONMENT.items()
        if observed.get(name) != required
    ]


def _select_scenarios(selector: str) -> tuple[Scenario, ...]:
    """Resolve an explicit scenario or a mechanically complete group."""
    match selector:
        case 'all':
            return SCENARIOS
        case 'candidates':
            return tuple(
                scenario for scenario in SCENARIOS if scenario.candidate is not None
            )
        case name:
            return (scenario_by_name(name),)


def _exit_status(
    scale: str,
    outcomes: Sequence[tuple[Verdict, bool]],
) -> int:
    """The run's exit code, from each scenario's verdict and comparison mode.

    A gate run has to establish something, so anything short of a pass leaves
    the phase blocked. A smoke run proves the machinery: a wide interval at
    200 observations is the expected answer, and a control run's latency
    verdict is not judged at all — p99 rests on a handful of samples there,
    so a neighbour's burst during one side's blocks yields a narrow interval
    around a real difference in the runner, not the code. What a control run
    answers for is the exact-count checks, which fail the run at every scale
    before this decision is reached.
    """
    match scale:
        case 'gate':
            return 0 if all(v is Verdict.PASS for v, _ in outcomes) else 1
        case _:
            judged_failures = [
                verdict
                for verdict, is_control in outcomes
                if verdict is Verdict.FAIL and not is_control
            ]
            return 1 if judged_failures else 0


def _minimum_observations(scenario: Scenario, scale: str) -> int:
    match scenario:
        case BatchScenario():
            return GATE_BATCHES if scale == 'gate' else SMOKE_BATCHES
        case _:
            return GATE_OBSERVATIONS if scale == 'gate' else SMOKE_OBSERVATIONS


def _observations_for(
    scenario: Scenario,
    scale: str,
    override: int | None,
) -> int:
    return override if override is not None else _minimum_observations(scenario, scale)


def _block_size_for(scenario: Scenario, requested: int) -> int:
    """Batches are seeded per block, so a block is a handful, not a hundred."""
    match scenario:
        case BatchScenario():
            return min(requested, 10)
        case _:
            return requested


def _control_disagreements(result: RunResult) -> list[str]:
    """Counts a control run has no business disagreeing on.

    When both sides run the same implementation, what the server attributed to
    them must match exactly — statements and affected rows are counted, not
    sampled, and a difference means the two sides are not doing the same work.
    That invalidates every number in the run, including the ones that look
    reasonable.

    Write transactions are held to a bounded environmental allowance rather
    than exact equality, for a stated reason: transaction ids come from a
    server-wide counter, so unattributable activity anywhere on the server
    lands in whichever side's block overlaps it. Routine maintenance is
    disabled in the measurement environment, so the allowance is headroom
    against what cannot be attributed — not tolerance on the operation's own
    commit count, which the per-side lower bound below and the declared
    budgets enforce.
    """
    if 'control' not in result.conditions.comparison:
        return []
    baseline, candidate = result.baseline.counts, result.candidate.counts
    operations = result.conditions.observations_per_side
    blocks = -(-operations // result.conditions.block_size)

    disagreements = [
        f'{result.conditions.scenario}: {label} {left} vs {right}'
        for label, left, right in (
            (
                'client statements',
                baseline.client_statements,
                candidate.client_statements,
            ),
            (
                'client rows',
                baseline.client_rows,
                candidate.client_rows,
            ),
            (
                'nested statements',
                baseline.nested_statements,
                candidate.nested_statements,
            ),
            (
                'nested rows',
                baseline.nested_rows,
                candidate.nested_rows,
            ),
            (
                'terminal rows',
                baseline.terminal_rows,
                candidate.terminal_rows,
            ),
        )
        if left != right
    ]
    for side, counts in (('baseline', baseline), ('candidate', candidate)):
        if counts.write_transactions < operations:
            disagreements.append(
                f'{result.conditions.scenario}: {side} committed '
                f'{counts.write_transactions} write transactions for '
                f'{operations} operations'
            )
    drift = abs(baseline.write_transactions - candidate.write_transactions)
    if drift > blocks:
        disagreements.append(
            f'{result.conditions.scenario}: write transactions differ by '
            f'{drift}, beyond the {blocks} a background writer could explain'
        )
    return disagreements


if __name__ == '__main__':
    sys.exit(main())
