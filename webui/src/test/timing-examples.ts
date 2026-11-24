// The shared examples of the lifecycle-record JSON
// (core/src/monitoring/timing_examples.json), typed. The type annotation makes
// the type checker hold each example against types/timing.ts; timing.test.ts
// holds the file against these values. A change on either side fails one of
// the two.

import type { AttemptPhases, NodeTiming, RunTiming } from '@/types/timing';

export interface TimingExamples {
  attempt_phases: AttemptPhases[];
  node_timings: NodeTiming[];
  run_timings: RunTiming[];
}

export const TIMING_EXAMPLES: TimingExamples = {
  attempt_phases: [
    {
      kind: 'not_captured',
    },
    {
      kind: 'captured',
      enqueued_at: '2026-11-22T12:00:00Z',
      claimed_at: '2026-11-22T12:00:02Z',
      claims: {
        kind: 'counted',
        count: 2,
        first_claimed_at: '2026-11-22T12:00:01Z',
      },
      finalizing: {
        kind: 'marked',
        finalizing_at: '2026-11-22T12:00:08Z',
      },
      recorded_by: {
        kind: 'worker',
        placement: {
          kind: 'placed',
          placed_at: '2026-11-22T12:00:03Z',
        },
      },
    },
    {
      kind: 'captured',
      enqueued_at: '2026-11-22T12:00:00Z',
      claimed_at: '2026-11-22T12:00:02Z',
      claims: {
        kind: 'not_counted',
      },
      finalizing: {
        kind: 'not_marked',
      },
      recorded_by: {
        kind: 'recovery',
        last_seen_at: '2026-11-22T12:00:07Z',
      },
    },
    {
      kind: 'captured',
      enqueued_at: '2026-11-22T12:00:00Z',
      claimed_at: '2026-11-22T12:00:02Z',
      claims: {
        kind: 'counted',
        count: 1,
        first_claimed_at: '2026-11-22T12:00:02Z',
      },
      finalizing: {
        kind: 'not_marked',
      },
      recorded_by: {
        kind: 'worker',
        placement: {
          kind: 'not_observed',
        },
      },
    },
    {
      kind: 'captured',
      enqueued_at: '2026-11-22T12:00:00Z',
      claimed_at: '2026-11-22T12:00:02Z',
      claims: {
        kind: 'counted',
        count: 1,
        first_claimed_at: '2026-11-22T12:00:02Z',
      },
      finalizing: {
        kind: 'marked',
        finalizing_at: '2026-11-22T12:00:07Z',
      },
      recorded_by: {
        kind: 'cancel',
      },
    },
  ],
  node_timings: [
    {
      kind: 'exact',
      segments: [
        {
          phase: {
            kind: 'pending',
          },
          started_at: '2026-11-22T12:00:00Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:01Z',
          },
          source: {
            kind: 'node',
          },
        },
        {
          phase: {
            kind: 'ready',
          },
          started_at: '2026-11-22T12:00:01Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:02Z',
          },
          source: {
            kind: 'node',
          },
        },
        {
          phase: {
            kind: 'enqueued',
          },
          started_at: '2026-11-22T12:00:02Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:03Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 1,
          },
        },
        {
          phase: {
            kind: 'claimed',
            claim: {
              kind: 'earlier',
              count: 1,
            },
          },
          started_at: '2026-11-22T12:00:03Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:04Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 1,
          },
        },
        {
          phase: {
            kind: 'claimed',
            claim: {
              kind: 'recorded',
              placement: {
                kind: 'placed',
                placed_at: '2026-11-22T12:00:05Z',
              },
            },
          },
          started_at: '2026-11-22T12:00:04Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:06Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 1,
          },
        },
        {
          phase: {
            kind: 'running',
          },
          started_at: '2026-11-22T12:00:06Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:07Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 1,
          },
        },
        {
          phase: {
            kind: 'finalizing',
          },
          started_at: '2026-11-22T12:00:07Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:08Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 1,
          },
        },
        {
          phase: {
            kind: 'retry_wait',
          },
          started_at: '2026-11-22T12:00:08Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:09Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 2,
          },
        },
        {
          phase: {
            kind: 'claimed',
            claim: {
              kind: 'not_recorded',
            },
          },
          started_at: '2026-11-22T12:00:09Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:10Z',
          },
          source: {
            kind: 'attempt',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
            attempt: 2,
          },
        },
        {
          phase: {
            kind: 'advancing',
          },
          started_at: '2026-11-22T12:00:10Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:11Z',
          },
          source: {
            kind: 'node',
          },
        },
      ],
      endpoint: {
        kind: 'terminal',
        status: 'COMPLETED',
        at: '2026-11-22T12:00:12Z',
      },
    },
    {
      kind: 'partial',
      reasons: [
        'HISTORY_PRUNED',
        'WORKER_LOST',
      ],
      segments: [
        {
          phase: {
            kind: 'unknown',
            reason: 'HISTORY_PRUNED',
          },
          started_at: '2026-11-22T12:00:00Z',
          end: {
            kind: 'ended',
            at: '2026-11-22T12:00:05Z',
          },
          source: {
            kind: 'task',
            generation: 1,
            task_id: '00000000-0000-0000-0000-00000000000a',
          },
        },
        {
          phase: {
            kind: 'unknown',
            reason: 'WORKER_LOST',
          },
          started_at: '2026-11-22T12:00:05Z',
          end: {
            kind: 'open',
          },
          source: {
            kind: 'attempt',
            generation: 2,
            task_id: '00000000-0000-0000-0000-00000000000b',
            attempt: 1,
          },
        },
      ],
      endpoint: {
        kind: 'not_terminal',
      },
    },
    {
      kind: 'unavailable',
      reason: 'RUN_BEFORE_CAPTURE',
    },
    {
      kind: 'inconsistent',
      conflict: {
        kind: 'clock_order',
        earlier: 'CLAIM',
        earlier_at: '2026-11-22T12:00:05Z',
        later: 'START',
        later_at: '2026-11-22T12:00:04Z',
      },
    },
    {
      kind: 'inconsistent',
      conflict: {
        kind: 'missing_record',
        record: 'COMPLETION_TIME',
      },
    },
    {
      kind: 'inconsistent',
      conflict: {
        kind: 'unexpected_event',
        event_id: 7,
        event: 'NODE_ENQUEUED',
      },
    },
  ],
  run_timings: [
    {
      kind: 'unavailable',
      as_of: '2026-11-22T12:01:40Z',
      captured_since: '2026-11-22T12:00:00Z',
      reason: 'TIMING_BOUND_EXCEEDED',
    },
    {
      kind: 'composed',
      as_of: '2026-11-22T12:01:40Z',
      captured_since: '2026-11-22T12:00:00Z',
      coverage: {
        kind: 'exact',
      },
      range_started_at: '2026-11-22T12:00:00Z',
      range_ended_at: '2026-11-22T12:01:40Z',
      open_segments: true,
      pauses: [
        {
          started_at: '2026-11-22T12:00:01Z',
          end: {
            kind: 'resumed',
            at: '2026-11-22T12:00:02Z',
          },
        },
        {
          started_at: '2026-11-22T12:00:03Z',
          end: {
            kind: 'run_ended',
            at: '2026-11-22T12:00:04Z',
          },
        },
        {
          started_at: '2026-11-22T12:00:05Z',
          end: {
            kind: 'open',
          },
        },
      ],
    },
    {
      kind: 'composed',
      as_of: '2026-11-22T12:01:40Z',
      captured_since: '2026-11-22T12:00:00Z',
      coverage: {
        kind: 'partial',
        reasons: [
          'CLAIMS_NOT_COUNTED',
        ],
      },
      range_started_at: '2026-11-22T12:00:00Z',
      range_ended_at: '2026-11-22T12:00:50Z',
      open_segments: false,
      pauses: [],
    },
    {
      kind: 'composed',
      as_of: '2026-11-22T12:01:40Z',
      captured_since: '2026-11-22T12:00:00Z',
      coverage: {
        kind: 'inconsistent',
        conflicts: [
          {
            kind: 'node',
            task_index: 2,
          },
          {
            kind: 'pause',
            conflict: {
              kind: 'unexpected_event',
              event_id: 4,
              event: 'WORKFLOW_RESUMED',
            },
          },
        ],
      },
      range_started_at: '2026-11-22T12:00:00Z',
      range_ended_at: '2026-11-22T12:00:50Z',
      open_segments: false,
      pauses: [],
    },
  ],
};
