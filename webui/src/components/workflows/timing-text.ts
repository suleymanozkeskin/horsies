// The words of the lifecycle record: coverage labels, reasons, conflicts,
// segment details. Each union of types/timing.ts has one exhaustive mapping
// here, so the timeline, the graph and the detail panel say the same thing.

import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import { instantMs } from '@/lib/time-text';
import type {
  MissingRecord,
  NodeEndpoint,
  NodeTiming,
  RunConflict,
  RunCoverage,
  SegmentClaim,
  SegmentEnd,
  SegmentSource,
  TimingBoundary,
  TimingConflict,
  TimingSegment,
  UnavailableReason,
  UnknownReason,
  WorkflowEventKind,
} from '@/types/timing';

import { phaseLabel } from './phase';
import type { NodeWallTime } from './timeline-geometry';

const UNKNOWN_REASON_TEXT: Record<UnknownReason, string> = {
  ATTEMPT_NOT_CAPTURED: 'attempt row with no phase record',
  CLAIMS_NOT_COUNTED: 'claims not counted',
  HISTORY_PRUNED: 'history row removed by retention',
  WORKER_LOST: 'worker lost',
};

const UNAVAILABLE_REASON_TEXT: Record<UnavailableReason, string> = {
  RUN_BEFORE_CAPTURE: 'The run began before the lifecycle record.',
  TIMING_BOUND_EXCEEDED: 'The run has more than 100,000 events.',
};

const BOUNDARY_TEXT: Record<TimingBoundary, string> = {
  NODE_CREATED: 'node created',
  NODE_READY: 'node ready',
  NODE_ENQUEUED: 'node enqueued',
  NODE_COMPLETED: 'node completed',
  TASK_CREATED: 'task created',
  ATTEMPT_ENQUEUED: 'attempt due',
  FIRST_CLAIM: 'first claim',
  CLAIM: 'claim',
  PLACEMENT: 'placement',
  START: 'start',
  FINALIZING: 'finalizing marker',
  LAST_SEEN: 'last seen',
  ATTEMPT_RECORDED: 'attempt recorded',
  TASK_MOVED: 'task moved to history',
  CHILD_CREATED: 'child run created',
  CHILD_COMPLETED: 'child run ended',
  RUN_PAUSED: 'run paused',
  RUN_RESUMED: 'run resumed',
  RUN_COMPLETED: 'run ended',
  READ_TIME: 'read time',
};

const MISSING_RECORD_TEXT: Record<MissingRecord, string> = {
  LINKED_TASK: 'the task that the node links',
  LINKED_WORK: 'the work of an enqueued or running node',
  CHILD_RUN: 'the child run that the node names',
  COMPLETION_TIME: 'the completion time of a terminal node',
  TASK_TIMES: 'the claim or start time that the task status needs',
  ATTEMPT_ROW: 'the attempt row that the archive names',
  OPEN_ATTEMPT: 'the current attempt of the live task',
};

const EVENT_TEXT: Record<WorkflowEventKind, string> = {
  WORKFLOW_PAUSED: 'WORKFLOW_PAUSED',
  WORKFLOW_RESUMED: 'WORKFLOW_RESUMED',
  NODE_READY: 'NODE_READY',
  NODE_ENQUEUED: 'NODE_ENQUEUED',
  TASK_MOVED: 'TASK_MOVED',
};

export const unknownReasonText = (reason: UnknownReason): string =>
  UNKNOWN_REASON_TEXT[reason];

export const unavailableReasonText = (reason: UnavailableReason): string =>
  UNAVAILABLE_REASON_TEXT[reason];

/** The contradiction in words, with no closing period. */
export function conflictText(conflict: TimingConflict): string {
  switch (conflict.kind) {
    case 'clock_order':
      return (
        `Clock order: ${BOUNDARY_TEXT[conflict.earlier]} ` +
        `(${formatInstant(instantMs(conflict.earlier_at))}) is after ` +
        `${BOUNDARY_TEXT[conflict.later]} ` +
        `(${formatInstant(instantMs(conflict.later_at))})`
      );
    case 'missing_record':
      return `Missing record: ${MISSING_RECORD_TEXT[conflict.record]}`;
    case 'unexpected_event':
      return `Unexpected event ${EVENT_TEXT[conflict.event]} (#${conflict.event_id})`;
    default:
      return assertNever(conflict, 'timing conflict');
  }
}

/** The coverage label of a node with what it holds. */
export function nodeCoverageText(timing: NodeTiming): string {
  switch (timing.kind) {
    case 'exact':
      return 'exact timing';
    case 'partial':
      return `partial timing: ${timing.reasons.map(unknownReasonText).join(', ')}`;
    case 'unavailable':
      return `timing unavailable: ${unavailableReasonText(timing.reason)}`;
    case 'inconsistent':
      return `timing inconsistent: ${conflictText(timing.conflict)}`;
    default:
      return assertNever(timing, 'node timing');
  }
}

function runConflictText(conflict: RunConflict): string {
  switch (conflict.kind) {
    case 'node':
      return `node #${conflict.task_index}`;
    case 'pause':
      return `pause record: ${conflictText(conflict.conflict)}`;
    default:
      return assertNever(conflict, 'run conflict');
  }
}

/** The run coverage banner, or null for exact coverage, which needs none. */
export function runCoverageText(coverage: RunCoverage): string | null {
  switch (coverage.kind) {
    case 'exact':
      return null;
    case 'partial':
      return `Partial timing: ${coverage.reasons.map(unknownReasonText).join(', ')}.`;
    case 'inconsistent':
      return `Inconsistent timing: ${coverage.conflicts.map(runConflictText).join('; ')}.`;
    default:
      return assertNever(coverage, 'run coverage');
  }
}

/** The phase of a segment with what it holds: `slot wait · 2 earlier
 * claims`, `slot wait · placed +1ms` (from the start of the segment). */
export function segmentPhaseText(segment: TimingSegment): string {
  const phase = segment.phase;
  switch (phase.kind) {
    case 'pending':
    case 'ready':
    case 'enqueued':
    case 'retry_wait':
    case 'running':
    case 'finalizing':
    case 'advancing':
      return phaseLabel(phase.kind);
    case 'claimed':
      return `${phaseLabel(phase.kind)} · ${claimText(phase.claim, instantMs(segment.started_at))}`;
    case 'unknown':
      return `${phaseLabel(phase.kind)} · ${unknownReasonText(phase.reason)}`;
    default:
      return assertNever(phase, 'segment phase');
  }
}

function claimText(claim: SegmentClaim, claimedAtMs: number): string {
  switch (claim.kind) {
    case 'earlier':
      return claim.count === 1 ? '1 earlier claim' : `${claim.count} earlier claims`;
    case 'recorded':
      switch (claim.placement.kind) {
        case 'placed':
          return `placed +${formatMilliseconds(instantMs(claim.placement.placed_at) - claimedAtMs)}`;
        case 'not_observed':
          return 'placement not observed';
        default:
          return assertNever(claim.placement, 'placement');
      }
    case 'not_recorded':
      return 'claim with no attempt row';
    default:
      return assertNever(claim, 'segment claim');
  }
}

/** What the segment describes: `node`, `task 2`, `task 1 · attempt 3`. */
export function segmentSourceText(source: SegmentSource): string {
  switch (source.kind) {
    case 'node':
      return 'node';
    case 'task':
      return `task ${source.generation}`;
    case 'attempt':
      return `task ${source.generation} · attempt ${source.attempt}`;
    default:
      return assertNever(source, 'segment source');
  }
}

/** The wall time of a node, or a dash when it has no segment. */
export function wallTimeText(wall: NodeWallTime): string {
  switch (wall.kind) {
    case 'measured':
      return formatMilliseconds(wall.ms);
    case 'no_segments':
      return '—';
    default:
      return assertNever(wall, 'node wall time');
  }
}

/** The end of a segment: its time, or `open`. */
export function segmentEndText(end: SegmentEnd): string {
  switch (end.kind) {
    case 'ended':
      return formatInstant(instantMs(end.at));
    case 'open':
      return 'open';
    default:
      return assertNever(end, 'segment end');
  }
}

/** The duration of a segment from its span; an open one is a time so far. */
export function segmentDurationText(end: SegmentEnd, durationMs: number): string {
  switch (end.kind) {
    case 'ended':
      return formatMilliseconds(durationMs);
    case 'open':
      return `${formatMilliseconds(durationMs)} so far`;
    default:
      return assertNever(end, 'segment end');
  }
}

/** The end of a node: `completed at <time>`, or `not terminal`. */
export function endpointText(endpoint: NodeEndpoint): string {
  switch (endpoint.kind) {
    case 'terminal':
      return `${statusLabel(endpoint.status)} at ${formatInstant(instantMs(endpoint.at))}`;
    case 'not_terminal':
      return 'not terminal';
    default:
      return assertNever(endpoint, 'node endpoint');
  }
}
