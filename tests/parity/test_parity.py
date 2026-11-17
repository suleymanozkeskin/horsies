"""Parity of the Rust client with the Python client, by scenario.

Each test runs one scenario of `scenarios.py` on the Python client and on
the Rust client (`rust/tests/parity`), each on its own fresh database, and
requires equal snapshots except the client family row, and equal reports.

Run: `pytest tests/parity -m e2e` with a test server
(`HORSIES_TEST_DATABASE_URL`), a built `horsies._core`, and the Rust scenario
binary (`cargo build -p horsies-parity-tests`, or `HORSIES_PARITY_RUST_BIN`).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.databases import DualRunDatabase
from tests.parity.runner import Client, compare_sides, run_side, rust_binary
from tests.parity.scenarios import SCENARIOS, Scenario

pytestmark = [pytest.mark.e2e]


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
    match run_side(Client.PYTHON, empty_database(), scenario):
        case Ok(python):
            pass
        case Err(failed):
            pytest.fail(f'the Python side failed: {failed.detail}')
    match run_side(Client.RUST, empty_database(), scenario):
        case Ok(rust):
            pass
        case Err(failed):
            pytest.fail(f'the Rust side failed: {failed.detail}')
    result = compare_sides(python, rust)
    assert result.snapshot_differences == (), '\n'.join(result.snapshot_differences)
    assert result.planned_seen, 'the client family rows are not PYTHON and RUST'
    assert result.report_differences == (), '\n'.join(
        f'{key}: Python {left!r}, Rust {right!r}'
        for key, left, right in result.report_differences
    )
