// What the rows of the run table are drawn against: the axis of a composed
// run with its pauses and its coverage, or the reason that every node of the
// run is unavailable. Pure: no React, no DOM.

import { assertNever } from '@/lib/exhaustive';
import { instantMs } from '@/lib/time-text';
import type { PauseInterval, RunCoverage, UnavailableReason } from '@/types/timing';
import type { WorkflowRunDetail } from '@/types/workflows';

import { boxOf, pauseSpan, runAxis, type AxisBox, type TimeAxis } from './timeline-geometry';

/** One pause of the run with its box on the axis. */
export interface PauseBand {
  pause: PauseInterval;
  box: AxisBox;
}

export type TimelineFrame =
  | { kind: 'composed'; axis: TimeAxis; pauses: PauseBand[]; coverage: RunCoverage }
  | { kind: 'unavailable'; reason: UnavailableReason; capturedSinceMs: number };

export function timelineFrame(detail: WorkflowRunDetail, drawAtMs: number): TimelineFrame {
  const timing = detail.timing;
  switch (timing.kind) {
    case 'composed': {
      const axis = runAxis(timing, drawAtMs);
      return {
        kind: 'composed',
        axis,
        pauses: timing.pauses.map(pause => ({ pause, box: boxOf(pauseSpan(pause, drawAtMs), axis) })),
        coverage: timing.coverage,
      };
    }
    case 'unavailable':
      return {
        kind: 'unavailable',
        reason: timing.reason,
        capturedSinceMs: instantMs(timing.captured_since),
      };
    default:
      return assertNever(timing, 'run timing');
  }
}
