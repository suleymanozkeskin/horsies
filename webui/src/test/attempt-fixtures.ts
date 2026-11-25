// Attempt rows and task rows for the attempt timing tests. The example
// attempts are on the times of the shared examples (test/timing-examples.ts):
// due at 12:00:00, started at 12:00:04, recorded at 12:00:09: their phases
// are seconds long. The normal attempts have the times of real attempts:
// engine steps of milliseconds.

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

/** The times of one attempt, in milliseconds: its queue wait, its slot wait
 * (with the time after the claim at which a child took the task), the run of
 * its task code, and its finalizing time. */
export interface AttemptTimes {
  queueMs: number;
  slotMs: number;
  placedMs: number;
  runMs: number;
  finalizingMs: number;
}

/** The times of a normal attempt with a local database and a free child.
 * The engine steps are milliseconds; the task code runs for seconds. */
export const NORMAL_TIMES: AttemptTimes = {
  queueMs: 3,
  slotMs: 2,
  placedMs: 0,
  runMs: 3_000,
  finalizingMs: 1,
};

/** An attempt due at `start` seconds with the given times, with one claim,
 * recorded by the worker. */
export function timedAttempt(
  number: number,
  start: number,
  times: AttemptTimes,
  outcome: string
): TaskAttempt {
  const claimed = start + times.queueMs / 1_000;
  const started = claimed + times.slotMs / 1_000;
  const finalizing = started + times.runMs / 1_000;
  return {
    attempt: number,
    outcome,
    will_retry: outcome !== 'COMPLETED',
    error_code: outcome === 'COMPLETED' ? null : 'PSP_UNAVAILABLE',
    error_message: outcome === 'COMPLETED' ? null : `unreachable on attempt ${number}`,
    failed_reason: null,
    worker_hostname: 'worker-a',
    started_at: at(started),
    finished_at: at(finalizing + times.finalizingMs / 1_000),
    phases: {
      kind: 'captured',
      enqueued_at: at(start),
      claimed_at: at(claimed),
      claims: { kind: 'counted', count: 1, first_claimed_at: at(claimed) },
      finalizing: { kind: 'marked', finalizing_at: at(finalizing) },
      recorded_by: {
        kind: 'worker',
        placement: { kind: 'placed', placed_at: at(claimed + times.placedMs / 1_000) },
      },
    },
  };
}

/** A normal attempt due at `start` seconds: queue wait 3 ms, slot wait 2 ms,
 * running 3 s, finalizing 1 ms. It is recorded 3.006 s after its due time. */
export const normalAttempt = (number: number, start: number, outcome: string): TaskAttempt =>
  timedAttempt(number, start, NORMAL_TIMES, outcome);

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
