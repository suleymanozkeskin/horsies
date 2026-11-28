// The words of the lifecycle record: coverage labels, reasons, conflicts,
// segment details. Each union of types/timing.ts has one exhaustive mapping
// here, so the timeline, the graph and the detail panel say the same thing.

import { assertNever } from '@/lib/exhaustive';
import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { instantMs } from '@/lib/time-text';
import type {
  MissingRecord,
  NodeTiming,
  RunConflict,
  RunCoverage,
  SegmentClaim,
  SegmentSource,
  TimingBoundary,
  TimingConflict,
  TimingSegment,
  UnavailableReason,
  UnknownReason,
  WorkflowEventKind,
} from '@/types/timing';

import { phaseLabel, segmentKind } from './phase';
import type { NodeTime } from './run-timing';
import { phaseComposition, type PhaseTotal } from './timeline-geometry';

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
      return `node #${conflict.node_index}`;
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

/** What a segment holds beyond its kind, or null when it holds nothing
 * more: the count of earlier claims, when a child took the task of a slot
 * wait, the reason of an unknown time. */
export function segmentDetailText(segment: TimingSegment): string | null {
  const phase = segment.phase;
  switch (phase.kind) {
    case 'pending':
    case 'ready':
    case 'enqueued':
    case 'retry_wait':
    case 'running':
    case 'finalizing':
    case 'advancing':
      return null;
    case 'claimed':
      return claimDetailText(phase.claim, segment);
    case 'unknown':
      return phase.reason === 'WORKER_LOST' ? null : unknownReasonText(phase.reason);
    default:
      return assertNever(phase, 'segment phase');
  }
}

/** The kind of a segment with what it holds: `earlier claims · 2 earlier
 * claims`, `unknown · claims not counted`, `worker lost`. */
export function segmentPhaseText(segment: TimingSegment): string {
  const label = phaseLabel(segmentKind(segment.phase));
  const detail = segmentDetailText(segment);
  return detail === null ? label : `${label} · ${detail}`;
}

/** The count of earlier claims, or when a child took the task of the claim
 * that ran the attempt. */
function claimDetailText(claim: SegmentClaim, segment: TimingSegment): string | null {
  switch (claim.kind) {
    case 'earlier':
      return earlierClaimsText(claim.count);
    case 'recorded':
      switch (claim.placement.kind) {
        case 'placed':
          return placedClaimText(segment, instantMs(claim.placement.placed_at));
        case 'not_observed':
          return null;
        default:
          return assertNever(claim.placement, 'placement');
      }
    case 'not_recorded':
      return null;
    default:
      return assertNever(claim, 'segment claim');
  }
}

/** `1 earlier claim`, `3 earlier claims`. */
export const earlierClaimsText = (count: number): string =>
  count === 1 ? '1 earlier claim' : `${count} earlier claims`;

/** The placement of a claim whose slot wait ended. A claim with no end yet
 * has no placement text: its wait is not over. */
function placedClaimText(segment: TimingSegment, placedAtMs: number): string | null {
  switch (segment.end.kind) {
    case 'open':
      return null;
    case 'closed':
      return placementText(instantMs(segment.started_at), placedAtMs);
    default:
      return assertNever(segment.end, 'segment end');
  }
}

/** When a child of the worker took the task after its claim: `a child took
 * the task after 273ms`. It tells which part of a slot wait was the wait for
 * a child, and which part the start on the child. */
export const placementText = (claimedAtMs: number, placedAtMs: number): string =>
  `a child took the task after ${formatMilliseconds(placedAtMs - claimedAtMs)}`;

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

/** A duration; one with no end yet is a time so far. */
export const durationText = (ms: number, open: boolean): string =>
  open ? `${formatMilliseconds(ms)} so far` : formatMilliseconds(ms);

/** The time of a node without its dependency wait, or a dash for a node
 * that did not start or has no timing. */
export function nodeTimeText(time: NodeTime): string {
  switch (time.kind) {
    case 'measured':
      return formatMilliseconds(time.ms);
    case 'not_started':
    case 'no_timing':
      return '—';
    default:
      return assertNever(time, 'node time');
  }
}

/** The time of each phase in words: `queue wait 1.0s, running 4.2s`. */
export function compositionText(totals: readonly PhaseTotal[]): string {
  return totals.length === 0
    ? 'no measured time'
    : totals.map(total => `${phaseLabel(total.key)} ${formatMilliseconds(total.ms)}`).join(', ');
}

/** The timing of a node in words, for an accessible name: its coverage and
 * its phase times. */
export function nodeTimingText(timing: NodeTiming, drawAtMs: number): string {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return `${nodeCoverageText(timing)}; ${compositionText(phaseComposition(timing.segments, drawAtMs))}`;
    case 'unavailable':
    case 'inconsistent':
      return nodeCoverageText(timing);
    default:
      return assertNever(timing, 'node timing');
  }
}
