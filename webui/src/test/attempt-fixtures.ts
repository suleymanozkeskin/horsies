// Attempt rows and task rows for the attempt timing tests, on the times of
// the shared examples (test/timing-examples.ts): due at 12:00:00, started at
// 12:00:04, recorded at 12:00:09.

import type { AttemptPhases } from '@/types/timing';
import type { LeafTask, TaskAttempt } from '@/types/workflows';

import { TIMING_EXAMPLES } from './timing-examples';
import { at, nth } from './workflow-fixtures';

export { at };

export const NOT_CAPTURED: AttemptPhases = nth(TIMING_EXAMPLES.attempt_phases, 0);
export const PLACED_TWO_CLAIMS: AttemptPhases = nth(TIMING_EXAMPLES.attempt_phases, 1);
export const RECOVERED: AttemptPhases = nth(TIMING_EXAMPLES.attempt_phases, 2);
export const ONE_CLAIM_NOT_OBSERVED: AttemptPhases = nth(TIMING_EXAMPLES.attempt_phases, 3);
export const CANCELLED: AttemptPhases = nth(TIMING_EXAMPLES.attempt_phases, 4);

/** An attempt of the shared examples: started at 12:00:04, recorded at
 * 12:00:09. */
export function exampleAttempt(phases: AttemptPhases, number = 1): TaskAttempt {
  return {
    attempt: number,
    outcome: 'FAILED',
    will_retry: false,
    error_code: 'PSP_UNAVAILABLE',
    error_message: 'payment provider unreachable',
    failed_reason: null,
    worker_hostname: 'worker-a',
    started_at: at(4),
    finished_at: at(9),
    phases,
  };
}

/** A normal attempt from `start` seconds: due at `start`, claimed and placed
 * at +1, started at +2, finalizing at +5, recorded at +6. */
export function normalAttempt(number: number, start: number, outcome: string): TaskAttempt {
  return {
    attempt: number,
    outcome,
    will_retry: outcome !== 'COMPLETED',
    error_code: outcome === 'COMPLETED' ? null : 'PSP_UNAVAILABLE',
    error_message: outcome === 'COMPLETED' ? null : `unreachable on attempt ${number}`,
    failed_reason: null,
    worker_hostname: 'worker-a',
    started_at: at(start + 2),
    finished_at: at(start + 6),
    phases: {
      kind: 'captured',
      enqueued_at: at(start),
      claimed_at: at(start + 1),
      claims: { kind: 'counted', count: 1, first_claimed_at: at(start + 1) },
      finalizing: { kind: 'marked', finalizing_at: at(start + 5) },
      recorded_by: { kind: 'worker', placement: { kind: 'placed', placed_at: at(start + 1) } },
    },
  };
}

/** The task row of a task with `retryCount` retries made, of `maxRetries`. */
export function leafTask(
  status: string,
  retryCount: number,
  maxRetries: number,
  times: { enqueuedAt: string | null; startedAt: string | null }
): LeafTask {
  return {
    task_id: '00000000-0000-0000-0000-00000000000a',
    status,
    error_code: null,
    failed_reason: null,
    retry_count: retryCount,
    max_retries: maxRetries,
    enqueued_at: times.enqueuedAt,
    started_at: times.startedAt,
    completed_at: null,
    failed_at: null,
    queue_s: null,
    exec_s: null,
    worker_hostname: null,
    good_until: null,
    trace_id: null,
  };
}
