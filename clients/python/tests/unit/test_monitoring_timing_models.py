"""The Pydantic models of the lifecycle record validate the JSON that the core writes.

``core/src/monitoring/timing_examples.json`` holds the JSON of each variant of
each union of the attempt phases and of the run and node timing. The core
test ``the_json_of_every_variant_is_the_shared_example``
(``core/src/monitoring/timing_tests.rs``) writes the same JSON from the Rust
models. This test validates each example into its Pydantic model, and the
JSON form of the model is the example again: no field is dropped or added.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final, cast

import pytest
from pydantic import TypeAdapter

from horsies.monitoring import (
    AttemptPhasesInfo,
    ClaimEarlierInfo,
    ClaimNotRecordedInfo,
    ClaimRecordedInfo,
    ConflictClockOrderInfo,
    NodeTimingExactInfo,
    NodeTimingInconsistentInfo,
    NodeTimingInfo,
    NodeTimingPartialInfo,
    NodeTimingUnavailableInfo,
    PauseOpenInfo,
    PhaseClaimedInfo,
    PhasesCapturedInfo,
    PhasesNotCapturedInfo,
    RecordedByCancelInfo,
    RecordedByRecoveryInfo,
    RunCoverageInconsistentInfo,
    RunTimingComposedInfo,
    RunTimingInfo,
    RunTimingUnavailableInfo,
    SegmentOpenInfo,
    SourceTaskInfo,
    TimingBoundary,
    UnknownReason,
)

pytestmark = [pytest.mark.unit]

EXAMPLES_PATH: Final = (
    Path(__file__).resolve().parents[4] / 'core' / 'src' / 'monitoring' / 'timing_examples.json'
)

_ATTEMPT_PHASES: Final = TypeAdapter[AttemptPhasesInfo](AttemptPhasesInfo)
_NODE_TIMING: Final = TypeAdapter[NodeTimingInfo](NodeTimingInfo)
_RUN_TIMING: Final = TypeAdapter[RunTimingInfo](RunTimingInfo)


def _examples(kind: str) -> list[object]:
    """The examples of one kind in the shared file."""
    loaded = cast(dict[str, list[object]], json.loads(EXAMPLES_PATH.read_text(encoding='utf-8')))
    return loaded[kind]


class TestAttemptPhases:
    def test_each_example_validates_and_dumps_back_to_itself(self) -> None:
        for example in _examples('attempt_phases'):
            model = _ATTEMPT_PHASES.validate_python(example)

            assert _ATTEMPT_PHASES.dump_python(model, mode='json') == example

    def test_the_examples_cover_the_captured_recovery_and_cancel_forms(self) -> None:
        models = [_ATTEMPT_PHASES.validate_python(item) for item in _examples('attempt_phases')]

        match models:
            case [
                PhasesNotCapturedInfo(),
                PhasesCapturedInfo(),
                PhasesCapturedInfo(recorded_by=RecordedByRecoveryInfo()),
                PhasesCapturedInfo(),
                PhasesCapturedInfo(recorded_by=RecordedByCancelInfo()),
            ]:
                pass
            case _:
                raise AssertionError(f'unexpected attempt phases: {models!r}')


class TestNodeTiming:
    def test_each_example_validates_and_dumps_back_to_itself(self) -> None:
        for example in _examples('node_timings'):
            model = _NODE_TIMING.validate_python(example)

            assert _NODE_TIMING.dump_python(model, mode='json') == example

    def test_the_examples_cover_each_coverage_label_and_claim(self) -> None:
        models = [_NODE_TIMING.validate_python(item) for item in _examples('node_timings')]

        match models:
            case [
                NodeTimingExactInfo(segments=exact_segments),
                NodeTimingPartialInfo(reasons=reasons, segments=partial_segments),
                NodeTimingUnavailableInfo(),
                NodeTimingInconsistentInfo(conflict=ConflictClockOrderInfo(earlier=earlier)),
                NodeTimingInconsistentInfo(),
                NodeTimingInconsistentInfo(),
            ]:
                claims = [
                    segment.phase.claim
                    for segment in exact_segments
                    if isinstance(segment.phase, PhaseClaimedInfo)
                ]
                assert [type(claim) for claim in claims] == [
                    ClaimEarlierInfo,
                    ClaimRecordedInfo,
                    ClaimNotRecordedInfo,
                ]
                assert reasons == [UnknownReason.HISTORY_PRUNED, UnknownReason.WORKER_LOST]
                assert isinstance(partial_segments[0].source, SourceTaskInfo)
                assert isinstance(partial_segments[1].end, SegmentOpenInfo)
                assert earlier == TimingBoundary.CLAIM
            case _:
                raise AssertionError(f'unexpected node timings: {models!r}')


class TestRunTiming:
    def test_each_example_validates_and_dumps_back_to_itself(self) -> None:
        for example in _examples('run_timings'):
            model = _RUN_TIMING.validate_python(example)

            assert _RUN_TIMING.dump_python(model, mode='json') == example

    def test_the_examples_cover_unavailable_and_each_composed_coverage(self) -> None:
        models = [_RUN_TIMING.validate_python(item) for item in _examples('run_timings')]

        match models:
            case [
                RunTimingUnavailableInfo(),
                RunTimingComposedInfo(pauses=[_, _, last_pause]),
                RunTimingComposedInfo(),
                RunTimingComposedInfo(coverage=RunCoverageInconsistentInfo(conflicts=[_, _])),
            ]:
                assert isinstance(last_pause.end, PauseOpenInfo)
            case _:
                raise AssertionError(f'unexpected run timings: {models!r}')
