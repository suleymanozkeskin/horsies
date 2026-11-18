"""Parity of the Rust client with the Python client, by scenario.

Each test runs one scenario of `scenarios.py` on the Python client and on
the Rust client (`rust/tests/parity`), each on its own fresh database, and
requires equal snapshots except the client family row, and equal reports.
Snapshots that differ while the run spans of the two sides do not lie in one
UTC hour (`ClockBoundaryStraddle`) run again on new databases, at most
`STRADDLE_REPEATS` times; the last run must have no difference.

Run: `pytest tests/parity -m e2e` with a test server
(`HORSIES_TEST_DATABASE_URL`), a built `horsies._core`, and the Rust scenario
binary (`cargo build -p horsies-parity-tests`, or `HORSIES_PARITY_RUST_BIN`).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.worker_run import (
    ClockBoundaryStraddle,
    DualRunMismatch,
    PairEqual,
    PairJudgement,
)
from tests.parity.runner import Client, PingReport, compare_sides, run_side, rust_binary
from tests.parity.scenarios import SCENARIOS, Scenario

pytestmark = [pytest.mark.e2e]

STRADDLE_REPEATS: int = 1
"""How many times a scenario whose snapshots were judged
`ClockBoundaryStraddle` runs again, on new databases. The judgement of the
last run stands as it is: a straddle fails the test too."""


@pytest.mark.parametrize('scenario', SCENARIOS, ids=[scenario.name for scenario in SCENARIOS])
def test_the_rust_client_leaves_what_the_python_client_leaves(
    scenario: Scenario,
    empty_database: Callable[[], DualRunDatabase],
) -> None:
    binary = rust_binary()
    assert binary.is_file(), (
        f'the Rust scenario binary {binary} does not exist: '
        'cargo build -p horsies-parity-tests, or set HORSIES_PARITY_RUST_BIN'
    )
    straddles: list[ClockBoundaryStraddle] = []
    for _ in range(STRADDLE_REPEATS + 1):
        match _checked_sides(scenario, empty_database):
            case PairEqual():
                return
            case DualRunMismatch(diff=diff):
                pytest.fail(f'{scenario.name}: the snapshots differ:\n{render_diff(diff)}')
            case ClockBoundaryStraddle() as straddle:
                straddles.append(straddle)
    pytest.fail(_straddles_text(scenario.name, straddles))


def _checked_sides(
    scenario: Scenario,
    make_database: Callable[[], DualRunDatabase],
) -> PairJudgement:
    """Run `scenario` on each client, each on a new database, and assert the
    pings of the Python side, the planned difference and equal reports. The
    judgement of the other snapshot differences."""
    match run_side(Client.PYTHON, make_database(), scenario):
        case Ok(python):
            pass
        case Err(failed):
            pytest.fail(f'the Python side failed: {failed.detail}')
    match run_side(Client.RUST, make_database(), scenario):
        case Ok(rust):
            pass
        case Err(failed):
            pytest.fail(f'the Rust side failed: {failed.detail}')
    pings = [entry for entry in python.report.values() if isinstance(entry, PingReport)]
    assert all(
        ping == PingReport(workers=len(scenario.profiles), targeted=1) for ping in pings
    ), f'the Python pings: {pings}'
    result = compare_sides(python, rust)
    assert result.planned_seen, 'the client family rows are not PYTHON and RUST'
    assert result.report_differences == (), '\n'.join(
        f'{key}: Python {left!r}, Rust {right!r}'
        for key, left, right in result.report_differences
    )
    return result.snapshots


def _straddles_text(name: str, straddles: list[ClockBoundaryStraddle]) -> str:
    spans = '; '.join(
        f'Python {straddle.reference_span}, Rust {straddle.candidate_span}'
        for straddle in straddles
    )
    return (
        f'{name}: ClockBoundaryStraddle in each of {len(straddles)} runs: the run spans '
        f'did not lie in one UTC hour ({spans}); the snapshots of the last run differ:\n'
        f'{render_diff(straddles[-1].diff)}'
    )
