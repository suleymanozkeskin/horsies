import { describe, expect, it } from 'vitest';

import {
  taskCancelAvailability,
  workflowActionAvailability,
} from '@/actions/eligibility';

const task = (status: string, overrides: Partial<{ isWorkflowTask: boolean }> = {}) => ({
  isWorkflowTask: overrides.isWorkflowTask ?? false,
  status,
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
    }
  );
});
