"""The rerun operation is part of the package's top-level surface.

The pin is the export itself: `rerun_task` is unusable without its
command, policy, and exhaustive outcome union, so the whole callable
surface must resolve from `horsies` directly — a deep-module import
path is not a public API.
"""

from __future__ import annotations

import pytest

import horsies
from horsies.core.history.rerun import operations

pytestmark = pytest.mark.unit

RERUN_EXPORTS = (
    'rerun_task',
    'RerunTask',
    'RerunEnqueuePolicy',
    'RerunOutcome',
    'RerunEnqueued',
    'RerunSourceLive',
    'RerunSourceAbsent',
    'RerunNotEligible',
    'RerunInputUnavailable',
    'RerunInputCorrupt',
    'RerunKeyConflict',
    'RerunKeyReplay',
    'NotEligibleReason',
)


def test_every_rerun_name_is_exported_and_is_the_operations_object() -> None:
    for name in RERUN_EXPORTS:
        assert name in horsies.__all__, name
        assert getattr(horsies, name) is getattr(operations, name), name


def test_exported_outcome_union_covers_every_outcome_dataclass() -> None:
    """A new outcome variant must join the union before it can ship."""
    union_members = set(horsies.RerunOutcome.__value__.__args__)
    exported_variants = {
        horsies.RerunEnqueued,
        horsies.RerunSourceLive,
        horsies.RerunSourceAbsent,
        horsies.RerunNotEligible,
        horsies.RerunInputUnavailable,
        horsies.RerunInputCorrupt,
        horsies.RerunKeyConflict,
        horsies.RerunKeyReplay,
    }
    assert union_members == exported_variants
