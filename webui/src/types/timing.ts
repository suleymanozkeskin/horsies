// Mirrors the lifecycle-record models of the monitoring API
// (core/src/monitoring/timing_models.rs). Every union is tagged by `kind`;
// times are UTC text. No model holds a duration: the client subtracts the two
// times of a segment. core/src/monitoring/timing_examples.json holds the JSON
// of each variant; timing.test.ts compares it with the typed examples.

/** The phase record of one attempt row. */
export type AttemptPhases =
  | { kind: 'not_captured' }
  | {
      kind: 'captured';
      /** Due time of the attempt. */
      enqueued_at: string;
      /** The claim that ran the attempt. */
      claimed_at: string;
      claims: AttemptClaims;
      finalizing: AttemptFinalizing;
      recorded_by: AttemptRecordedBy;
    };

/** The claims of one attempt. A count above 1 holds lost leases and released
 * claims. */
export type AttemptClaims =
  | { kind: 'counted'; count: number; first_claimed_at: string }
  | { kind: 'not_counted' };

/** When a child of the worker took the task: the claim time plus the slot
 * wait that the worker measured. */
export type AttemptPlacement =
  | { kind: 'placed'; placed_at: string }
  | { kind: 'not_observed' };

/** The finalizing marker of the claim that ran the attempt. */
export type AttemptFinalizing =
  | { kind: 'marked'; finalizing_at: string }
  | { kind: 'not_marked' };

/** Who wrote the attempt row. `cancel`: an operator cancelled the task while
 * the attempt ran; the attempt ended at the cancel. */
export type AttemptRecordedBy =
  | { kind: 'worker'; placement: AttemptPlacement }
  | { kind: 'recovery'; last_seen_at: string }
  | { kind: 'cancel' };

/** The phase timing of one workflow node, tagged by its coverage label. */
export type NodeTiming =
  | { kind: 'exact'; segments: TimingSegment[]; endpoint: NodeEndpoint }
  | {
      kind: 'partial';
      /** The reason of each `unknown` segment, each once, in name order. */
      reasons: UnknownReason[];
      segments: TimingSegment[];
      endpoint: NodeEndpoint;
    }
  | { kind: 'unavailable'; reason: UnavailableReason }
  | { kind: 'inconsistent'; conflict: TimingConflict };

/** One phase interval of a node, from `started_at` to `end`. */
export interface TimingSegment {
  phase: SegmentPhase;
  started_at: string;
  end: SegmentEnd;
  source: SegmentSource;
}

export type SegmentPhase =
  | { kind: 'pending' }
  | { kind: 'ready' }
  | { kind: 'enqueued' }
  | { kind: 'retry_wait' }
  | { kind: 'claimed'; claim: SegmentClaim }
  | { kind: 'running' }
  | { kind: 'finalizing' }
  | { kind: 'advancing' }
  | { kind: 'unknown'; reason: UnknownReason };

/** Which claim a `claimed` segment shows. */
export type SegmentClaim =
  | { kind: 'earlier'; count: number }
  | { kind: 'recorded'; placement: AttemptPlacement }
  | { kind: 'not_recorded' };

/** The end of one segment. `open`: not ended at the read (`as_of`). */
export type SegmentEnd = { kind: 'ended'; at: string } | { kind: 'open' };

/** What one segment describes. `generation` counts the tasks of the node from
 * 1; `attempt` counts the attempts of that task from 1. */
export type SegmentSource =
  | { kind: 'node' }
  | { kind: 'task'; generation: number; task_id: string }
  | { kind: 'attempt'; generation: number; task_id: string; attempt: number };

/** The terminal state of a node: a marker, not a segment. */
export type NodeEndpoint =
  | { kind: 'terminal'; status: TerminalNodeStatus; at: string }
  | { kind: 'not_terminal' };

export type TerminalNodeStatus = 'COMPLETED' | 'FAILED' | 'SKIPPED';

export type UnknownReason =
  | 'ATTEMPT_NOT_CAPTURED'
  | 'CLAIMS_NOT_COUNTED'
  | 'HISTORY_PRUNED'
  | 'WORKER_LOST';

export type UnavailableReason = 'RUN_BEFORE_CAPTURE' | 'TIMING_BOUND_EXCEEDED';

/** A contradiction in the record of a node or a run. */
export type TimingConflict =
  | {
      kind: 'clock_order';
      earlier: TimingBoundary;
      earlier_at: string;
      later: TimingBoundary;
      later_at: string;
    }
  | { kind: 'missing_record'; record: MissingRecord }
  | { kind: 'unexpected_event'; event_id: number; event: WorkflowEventKind };

export type TimingBoundary =
  | 'NODE_CREATED'
  | 'NODE_READY'
  | 'NODE_ENQUEUED'
  | 'NODE_COMPLETED'
  | 'TASK_CREATED'
  | 'ATTEMPT_ENQUEUED'
  | 'FIRST_CLAIM'
  | 'CLAIM'
  | 'PLACEMENT'
  | 'START'
  | 'FINALIZING'
  | 'LAST_SEEN'
  | 'ATTEMPT_RECORDED'
  | 'TASK_MOVED'
  | 'CHILD_CREATED'
  | 'CHILD_COMPLETED'
  | 'RUN_PAUSED'
  | 'RUN_RESUMED'
  | 'RUN_COMPLETED'
  | 'READ_TIME';

export type MissingRecord =
  | 'LINKED_TASK'
  | 'LINKED_WORK'
  | 'CHILD_RUN'
  | 'COMPLETION_TIME'
  | 'TASK_TIMES'
  | 'ATTEMPT_ROW'
  | 'OPEN_ATTEMPT';

export type WorkflowEventKind =
  | 'WORKFLOW_PAUSED'
  | 'WORKFLOW_RESUMED'
  | 'NODE_READY'
  | 'NODE_ENQUEUED'
  | 'TASK_MOVED';

/** The phase timing of one run. `as_of` is the database time of the read;
 * `captured_since` is the start of the lifecycle record. */
export type RunTiming =
  | {
      kind: 'unavailable';
      as_of: string;
      captured_since: string;
      reason: UnavailableReason;
    }
  | ComposedRunTiming;

/** The composed timing of a run. `range_ended_at` is the latest segment end
 * (an open one at `as_of`), endpoint, pause end and run end. */
export interface ComposedRunTiming {
  kind: 'composed';
  as_of: string;
  captured_since: string;
  coverage: RunCoverage;
  range_started_at: string;
  range_ended_at: string;
  /** True when a segment or a pause is open. */
  open_segments: boolean;
  pauses: PauseInterval[];
}

export type RunCoverage =
  | { kind: 'exact' }
  | { kind: 'partial'; reasons: UnknownReason[] }
  | { kind: 'inconsistent'; conflicts: RunConflict[] };

export type RunConflict =
  | { kind: 'node'; node_index: number }
  | { kind: 'pause'; conflict: TimingConflict };

/** One pause of a run: from a `WORKFLOW_PAUSED` event to `end`. */
export interface PauseInterval {
  started_at: string;
  end: PauseEnd;
}

export type PauseEnd =
  | { kind: 'resumed'; at: string }
  | { kind: 'run_ended'; at: string }
  | { kind: 'open' };
