"""The record that the Python case of an integration dual-run pair leaves,
and the judgement of the core case against it. Pure: no I/O.

The Python case runs first. After its body it leaves one record under the
pair's key; the core case reads it:

- ``ReferenceCaptured``: the body passed and called an operation of the
  module's table; the core case captures its own snapshot and compares.
- ``ReferenceNotInformative``: the body passed and called no operation; the
  core case does not compare (both runs ran the same code).
- ``ReferenceSkipped``: the body skipped; the core case does not compare.
- ``ReferenceCaptureFailed``: the body passed and its capture failed; the
  core case fails with ``DualRunCaptureFailed`` and that capture error.
- ``ReferenceFailed``: the body failed; the core case fails with
  ``ReferenceRunMissing``. So does a core case that finds no record (the
  Python case ran in another process, or not before it).

A capture that fails is an error of the harness or of the stored state, with
its subject and cause (``capture.render_capture_failure``). It is never an
outcome of the pair: no snapshot exists, so nothing compares equal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from tests.dualrun.capture import CaptureFailure
from tests.dualrun.compare import SnapshotDiff, compare_snapshots
from tests.dualrun.normalize import NormalizedSnapshot
from tests.dualrun.operations.table import (
    DecidedDifference,
    PairCompare,
    PairRule,
    WholeSnapshot,
)
from tests.dualrun.pair_rules import decided_difference_explains, project
from tests.dualrun.values import RunWindow


@dataclass(frozen=True, slots=True)
class PairKey:
    """A pair: the node id of its cases without the id of ``implementation``,
    for example ``tests/integration/test_x.py::test_y[same command]`` for
    ``test_y[python-same command]`` and ``test_y[core-same command]``."""

    case_name: str


@dataclass(frozen=True, slots=True)
class ReferenceCaptured:
    """The Python case passed, called an operation, and left its snapshot."""

    snapshot: NormalizedSnapshot
    window: RunWindow


@dataclass(frozen=True, slots=True)
class ReferenceNotInformative:
    """The Python case passed and called no operation of the table."""


@dataclass(frozen=True, slots=True)
class ReferenceSkipped:
    """The Python case skipped in its body."""


@dataclass(frozen=True, slots=True)
class ReferenceCaptureFailed:
    """The Python case passed in its body, and its capture failed with ``error``."""

    error: CaptureFailure


@dataclass(frozen=True, slots=True)
class ReferenceFailed:
    """The Python case failed in its body."""


@dataclass(frozen=True, slots=True)
class NoReference:
    """No record of the Python case: it ran in another process, or not before the core case."""


ReferenceRecord = (
    ReferenceCaptured
    | ReferenceNotInformative
    | ReferenceSkipped
    | ReferenceCaptureFailed
    | ReferenceFailed
)


@dataclass(frozen=True, slots=True)
class CompareSnapshots:
    """The core case captures and compares with this reference."""

    reference: ReferenceCaptured


@dataclass(frozen=True, slots=True)
class NoCompare:
    """The core case does not compare: the Python case was not informative or skipped."""


@dataclass(frozen=True, slots=True)
class ReferenceRunMissing:
    """The core case has no passing Python case to compare with."""

    found: ReferenceFailed | NoReference


@dataclass(frozen=True, slots=True)
class ReferenceNotCaptured:
    """The core case has nothing to compare with: the capture of its Python case failed."""

    error: CaptureFailure


CoreStep = CompareSnapshots | NoCompare | ReferenceNotCaptured | ReferenceRunMissing


def core_step(record: ReferenceRecord | NoReference) -> CoreStep:
    """What the core case does with the record of its Python case."""
    match record:
        case ReferenceCaptured():
            return CompareSnapshots(reference=record)
        case ReferenceNotInformative() | ReferenceSkipped():
            return NoCompare()
        case ReferenceCaptureFailed(error=error):
            return ReferenceNotCaptured(error=error)
        case ReferenceFailed() | NoReference():
            return ReferenceRunMissing(found=record)


@dataclass(frozen=True, slots=True)
class PairEqual:
    """The two snapshots compare equal."""


@dataclass(frozen=True, slots=True)
class DecidedDifferenceSeen:
    """The two snapshots differ, and the decided difference of the pair
    explains every entry of ``diff``."""

    diff: SnapshotDiff
    decided: DecidedDifference


@dataclass(frozen=True, slots=True)
class DualRunMismatch:
    """The two snapshots differ, and neither run window crossed an hour boundary."""

    diff: SnapshotDiff


@dataclass(frozen=True, slots=True)
class ClockBoundaryStraddle:
    """The two snapshots differ, and a run window crossed an hour boundary:
    a leaf name or a time rank can differ for that reason alone."""

    diff: SnapshotDiff
    reference_window: RunWindow
    candidate_window: RunWindow


PairJudgement = PairEqual | DecidedDifferenceSeen | DualRunMismatch | ClockBoundaryStraddle


def _hour(at: datetime) -> datetime:
    return at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def crosses_hour(window: RunWindow) -> bool:
    """Whether the window holds the start of a UTC hour after its own start."""
    return _hour(window.start) != _hour(window.end)


def judge_pair(
    reference: ReferenceCaptured,
    candidate: NormalizedSnapshot,
    candidate_window: RunWindow,
    compare: PairCompare,
) -> PairJudgement:
    """The judgement of the core case's snapshot against the reference.
    ``compare`` is how the pair of the test compares: under a pair rule both
    snapshots are reduced to its fact before the compare
    (``pair_rules.project``); under a decided difference a difference whose
    every entry its fact explains is ``DecidedDifferenceSeen``."""
    diff = compare_snapshots(
        project(reference.snapshot, compare), project(candidate, compare)
    )
    if not diff.entries:
        return PairEqual()
    match compare:
        case DecidedDifference(fact=fact) if decided_difference_explains(fact, diff):
            return DecidedDifferenceSeen(diff=diff, decided=compare)
        case WholeSnapshot() | PairRule() | DecidedDifference():
            pass
    match crosses_hour(reference.window) or crosses_hour(candidate_window):
        case True:
            return ClockBoundaryStraddle(
                diff=diff,
                reference_window=reference.window,
                candidate_window=candidate_window,
            )
        case False:
            return DualRunMismatch(diff=diff)
