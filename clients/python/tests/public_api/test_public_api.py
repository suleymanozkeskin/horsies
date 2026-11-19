"""The public API gate: the installed build against the committed baseline.

The test takes the runtime snapshot and the pyright view of the installed
horsies (``runner.take_current_api``) and compares them with
``baseline.json`` and ``baseline-pyright.json`` (``compare.py``). A blocking
difference fails the test with the full report. An intended change of the
public API updates the baseline in the same PR::

    uv run --no-sync python -m tests.public_api.runner

CI runs this file in its own step, against the dev wheel.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.public_api.compare import (
    Err,
    JsonValue,
    Ok,
    blocks,
    compare_snapshots,
    compare_views,
    render,
)
from tests.public_api.runner import (
    BASELINE_PYRIGHT_VIEW,
    BASELINE_SNAPSHOT,
    describe_failure,
    take_current_api,
)


def _baseline(path: Path) -> JsonValue:
    document: JsonValue = json.loads(path.read_text(encoding='utf-8'))
    return document


def test_the_public_api_has_no_unapproved_change(tmp_path: Path) -> None:
    match take_current_api(tmp_path):
        case Err(failure):
            pytest.fail(describe_failure(failure))
        case Ok(current):
            pass
    match compare_snapshots(_baseline(BASELINE_SNAPSHOT), current.snapshot):
        case Err(malformed):
            pytest.fail(f'{malformed.document}: {malformed.cause}')
        case Ok(snapshot_differences):
            pass
    match compare_views(_baseline(BASELINE_PYRIGHT_VIEW), current.pyright_view):
        case Err(malformed):
            pytest.fail(f'{malformed.document}: {malformed.cause}')
        case Ok(view_differences):
            pass
    differences = snapshot_differences + view_differences
    assert not any(blocks(difference) for difference in differences), render(differences)
