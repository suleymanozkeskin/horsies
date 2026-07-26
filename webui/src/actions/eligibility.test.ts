import { describe, expect, it } from 'vitest';

import {
  EXPIRY_PASSED_REASON,
  taskCancelAvailability,
  taskRetryAvailability,
  workflowActionAvailability,
} from '@/actions/eligibility';

const NOW = Date.parse('2026-07-26T12:00:00Z');
const PAST = '2026-07-26T11:59:59Z';
const FUTURE = '2026-07-26T12:00:01Z';

const task = (status: string, overrides: Partial<{ isWorkflowTask: boolean; goodUntil: string | null }> = {}) => ({
  isWorkflowTask: overrides.isWorkflowTask ?? false,
  status,
  goodUntil: overrides.goodUntil ?? null,
});

const TASK_STATUSES = [
  'PENDING',
  'CLAIMED',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
  'EXPIRED',
] as const;

const RUN_STATUSES = [
  'PENDING',
  'RUNNING',
  'PAUSED',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
] as const;

describe('task cancel', () => {
  const CANCELLABLE = new Set(['PENDING', 'CLAIMED', 'RUNNING']);

  it.each(TASK_STATUSES)('is shown only for non-terminal status %s', status => {
    const availability = taskCancelAvailability(task(status));
    expect(availability.shown).toBe(CANCELLABLE.has(status));
  });

  it.each(['PENDING', 'CLAIMED', 'RUNNING'])(
    'is always enabled when shown (%s)',
    status => {
      const availability = taskCancelAvailability(task(status));
      expect(availability).toEqual({ shown: true, enabled: true });
    }
  );

  it.each(TASK_STATUSES)('is hidden for a workflow-bound row (%s)', status => {
    expect(
      taskCancelAvailability(task(status, { isWorkflowTask: true }))
    ).toEqual({ shown: false });
  });

  it('hides for an unknown status rather than guessing', () => {
    expect(taskCancelAvailability(task('QUARANTINED'))).toEqual({ shown: false });
  });
});

describe('task retry', () => {
  const RETRYABLE = new Set(['FAILED', 'EXPIRED', 'CANCELLED']);

  it.each(TASK_STATUSES)(
    'is shown only for a settled-without-success status %s',
    status => {
      const availability = taskRetryAvailability(task(status), NOW);
      expect(availability.shown).toBe(RETRYABLE.has(status));
    }
  );

  it.each(['FAILED', 'EXPIRED', 'CANCELLED'])(
    'is enabled when good_until is null (%s)',
    status => {
      expect(taskRetryAvailability(task(status), NOW)).toEqual({
        shown: true,
        enabled: true,
      });
    }
  );

  it.each(['FAILED', 'EXPIRED', 'CANCELLED'])(
    'is enabled when good_until is in the future (%s)',
    status => {
      expect(
        taskRetryAvailability(task(status, { goodUntil: FUTURE }), NOW)
      ).toEqual({ shown: true, enabled: true });
    }
  );

  it.each(['FAILED', 'EXPIRED', 'CANCELLED'])(
    'is shown but disabled once good_until has passed (%s)',
    status => {
      expect(
        taskRetryAvailability(task(status, { goodUntil: PAST }), NOW)
      ).toEqual({
        shown: true,
        enabled: false,
        disabledReason: EXPIRY_PASSED_REASON,
      });
    }
  );

  it('treats good_until exactly at now as passed, matching the server CAS', () => {
    const availability = taskRetryAvailability(
      task('FAILED', { goodUntil: new Date(NOW).toISOString() }),
      NOW
    );
    expect(availability).toEqual({
      shown: true,
      enabled: false,
      disabledReason: EXPIRY_PASSED_REASON,
    });
  });

  it('stays enabled on an unparseable good_until so the server decides', () => {
    expect(
      taskRetryAvailability(task('FAILED', { goodUntil: 'not-a-date' }), NOW)
    ).toEqual({ shown: true, enabled: true });
  });

  it.each(TASK_STATUSES)('is hidden for a workflow-bound row (%s)', status => {
    expect(
      taskRetryAvailability(task(status, { isWorkflowTask: true }), NOW)
    ).toEqual({ shown: false });
  });
});

describe('workflow actions', () => {
  it.each(RUN_STATUSES)('pause is shown only while RUNNING (%s)', status => {
    const availability = workflowActionAvailability('workflow-pause', status);
    expect(availability.shown).toBe(status === 'RUNNING');
  });

  it.each(RUN_STATUSES)('resume is shown only while PAUSED (%s)', status => {
    const availability = workflowActionAvailability('workflow-resume', status);
    expect(availability.shown).toBe(status === 'PAUSED');
  });

  it.each(RUN_STATUSES)(
    'cancel is shown for PENDING/RUNNING/PAUSED only (%s)',
    status => {
      const availability = workflowActionAvailability('workflow-cancel', status);
      expect(availability.shown).toBe(
        status === 'PENDING' || status === 'RUNNING' || status === 'PAUSED'
      );
    }
  );

  it('enables every workflow action it shows', () => {
    expect(workflowActionAvailability('workflow-pause', 'RUNNING')).toEqual({
      shown: true,
      enabled: true,
    });
    expect(workflowActionAvailability('workflow-resume', 'PAUSED')).toEqual({
      shown: true,
      enabled: true,
    });
    expect(workflowActionAvailability('workflow-cancel', 'PAUSED')).toEqual({
      shown: true,
      enabled: true,
    });
  });

  it.each(RUN_STATUSES)(
    'never offers a task action on a run (%s)',
    status => {
      expect(workflowActionAvailability('task-cancel', status)).toEqual({
        shown: false,
      });
      expect(workflowActionAvailability('task-retry', status)).toEqual({
        shown: false,
      });
    }
  );
});
