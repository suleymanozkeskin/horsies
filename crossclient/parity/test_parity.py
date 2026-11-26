"""Parity of each candidate client with the Python client, by scenario.

Each test runs one scenario of `scenarios.py` on the Python client and on
one candidate side (`CANDIDATES`: the Rust client of
`clients/rust/tests/parity`, and the TypeScript client of
`clients/typescript/tests/parity` on Node and on Bun), each on its own fresh
database, and requires equal snapshots except the client family row, and
equal reports. Snapshots that differ while the run spans of the two sides do
not lie in one UTC hour (`ClockBoundaryStraddle`) run again on new
databases, at most `STRADDLE_REPEATS` times; the last run must have no
difference.

The test id is `<side>-<scenario>`, for example `rust-isolated_outcomes` or
`typescript-node-isolated_outcomes`. A pair that the side does not offer
(`scenario_offered`) is skipped with its reason. A missing program of the
side fails the test with the command that builds it; it never skips.

Run, from the repository root:
`uv run --project clients/python pytest -c clients/python/pyproject.toml --rootdir . crossclient/parity -m e2e`
with a test server (`HORSIES_TEST_DATABASE_URL`), a built `horsies._core`,
the Rust scenario binary (`cargo build -p horsies-parity-tests`, or
`HORSIES_PARITY_RUST_BIN`), and for the TypeScript sides Node 26 and Bun 1.4
on `PATH` (or `HORSIES_PARITY_NODE_BIN`, `HORSIES_PARITY_BUN_BIN`), the
installed workspace of `clients/typescript`, and a dev build of the addon in
`HORSIES_NATIVE_ADDON_PATH`. `-k typescript` selects the TypeScript pairs;
`-k "not typescript"` the Rust pairs.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok
from crossclient.parity.snapshot.compare import render_diff
from crossclient.parity.snapshot.databases import DualRunDatabase
from crossclient.parity.snapshot.worker_run import (
    ClockBoundaryStraddle,
    DualRunMismatch,
    PairEqual,
    PairJudgement,
)
from crossclient.parity.runner import (
    CANDIDATES,
    REFERENCE,
    CandidateSide,
    NotOffered,
    Offered,
    PingReport,
    ProgramMissing,
    ProgramPresent,
    compare_sides,
    family_of,
    run_side,
    scenario_offered,
    side_name,
    side_program,
)
from crossclient.parity.scenarios import SCENARIOS, Scenario

pytestmark = [pytest.mark.e2e]

STRADDLE_REPEATS: int = 1
"""How many times a scenario whose snapshots were judged
`ClockBoundaryStraddle` runs again, on new databases. The judgement of the
last run stands as it is: a straddle fails the test too."""

PAIRS: Final[tuple[tuple[CandidateSide, Scenario], ...]] = tuple(
    (candidate, scenario) for candidate in CANDIDATES for scenario in SCENARIOS
)
"""Each candidate with each scenario, the candidates in `CANDIDATES` order."""


@pytest.mark.parametrize(
    ('candidate', 'scenario'),
    PAIRS,
    ids=[f'{side_name(candidate)}-{scenario.name}' for candidate, scenario in PAIRS],
)
def test_the_candidate_leaves_what_the_python_client_leaves(
    candidate: CandidateSide,
    scenario: Scenario,
    empty_database: Callable[[], DualRunDatabase],
) -> None:
    match scenario_offered(candidate, scenario):
        case NotOffered(reason=reason):
            pytest.skip(reason)
        case Offered():
            pass
    match side_program(candidate, os.environ):
        case ProgramMissing(hint=hint):
            pytest.fail(f'the {side_name(candidate)} side cannot run: {hint}')
        case ProgramPresent():
            pass
    straddles: list[ClockBoundaryStraddle] = []
    for _ in range(STRADDLE_REPEATS + 1):
        match _checked_sides(candidate, scenario, empty_database):
            case PairEqual():
                return
            case DualRunMismatch(diff=diff):
                pytest.fail(f'{scenario.name}: the snapshots differ:\n{render_diff(diff)}')
            case ClockBoundaryStraddle() as straddle:
                straddles.append(straddle)
    pytest.fail(_straddles_text(candidate, scenario.name, straddles))


def _checked_sides(
    candidate: CandidateSide,
    scenario: Scenario,
    make_database: Callable[[], DualRunDatabase],
) -> PairJudgement:
    """Run `scenario` on the Python side and on `candidate`, each on a new
    database, and assert the pings of the Python side, the planned
    difference and equal reports. The judgement of the other snapshot
    differences."""
    name = side_name(candidate)
    match run_side(REFERENCE, make_database(), scenario):
        case Ok(reference):
            pass
        case Err(failed):
            pytest.fail(f'the Python side failed: {failed.detail}')
    match run_side(candidate, make_database(), scenario):
        case Ok(candidate_run):
            pass
        case Err(failed):
            pytest.fail(f'the {name} side failed: {failed.detail}')
    pings = [entry for entry in reference.report.values() if isinstance(entry, PingReport)]
    assert all(
        ping == PingReport(workers=len(scenario.profiles), targeted=1) for ping in pings
    ), f'the Python pings: {pings}'
    result = compare_sides(reference, candidate_run)
    assert result.planned_seen, (
        f'the client family rows are not PYTHON and {family_of(candidate).value}'
    )
    assert result.report_differences == (), '\n'.join(
        f'{key}: Python {left!r}, {name} {right!r}'
        for key, left, right in result.report_differences
    )
    return result.snapshots


def _straddles_text(
    candidate: CandidateSide, name: str, straddles: list[ClockBoundaryStraddle],
) -> str:
    spans = '; '.join(
        f'Python {straddle.reference_span}, {side_name(candidate)} {straddle.candidate_span}'
        for straddle in straddles
    )
    return (
        f'{name}: ClockBoundaryStraddle in each of {len(straddles)} runs: the run spans '
        f'did not lie in one UTC hour ({spans}); the snapshots of the last run differ:\n'
        f'{render_diff(straddles[-1].diff)}'
    )
