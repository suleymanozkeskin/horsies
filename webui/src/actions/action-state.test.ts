import { describe, expect, it } from 'vitest';

import {
  actionReducer,
  IDLE,
  isBusy,
  pendingAction,
  SETTLE_WINDOW_MS,
  type ActionEvent,
  type ActionState,
} from '@/actions/action-state';
import type { ActionKind } from '@/types/actions';

const ACTION: ActionKind = 'task-cancel';
const NOW = 1_700_000_000_000;

const confirming: ActionState = { phase: 'confirming', action: ACTION };
const submitting: ActionState = { phase: 'submitting', action: ACTION };
const verifying: ActionState = { phase: 'verifying', action: ACTION };
const settling: ActionState = {
  phase: 'settling',
  action: ACTION,
  deadlineAt: NOW + SETTLE_WINDOW_MS,
};
const conflict: ActionState = {
  phase: 'conflict',
  action: ACTION,
  currentStatus: 'CLAIMED',
};
const gone: ActionState = { phase: 'gone', action: ACTION };
const failed: ActionState = { phase: 'failed', action: ACTION, detail: 'boom' };

describe('idle', () => {
  it('enters confirming on click', () => {
    expect(actionReducer(IDLE, { type: 'click', action: ACTION })).toEqual(
      confirming
    );
  });

  it('ignores events that belong to a later phase', () => {
    const events: ActionEvent[] = [
      { type: 'confirm' },
      { type: 'succeeded', at: NOW },
      { type: 'conflict', currentStatus: 'RUNNING' },
      { type: 'not-found' },
      { type: 'unavailable', detail: null },
      { type: 'network-error' },
      { type: 'settled' },
    ];
    for (const event of events) {
      expect(actionReducer(IDLE, event)).toBe(IDLE);
    }
  });
});

describe('confirming', () => {
  it('returns to idle on dismiss', () => {
    expect(actionReducer(confirming, { type: 'dismiss' })).toEqual(IDLE);
  });

  it('enters submitting on confirm', () => {
    expect(actionReducer(confirming, { type: 'confirm' })).toEqual(submitting);
  });

  it('keeps the action identity across the transition', () => {
    const next = actionReducer(
      actionReducer(IDLE, { type: 'click', action: 'workflow-resume' }),
      { type: 'confirm' }
    );
    expect(pendingAction(next)).toBe('workflow-resume');
  });
});

describe('submitting', () => {
  it('enters settling on 200 with a 30 s deadline', () => {
    expect(actionReducer(submitting, { type: 'succeeded', at: NOW })).toEqual({
      phase: 'settling',
      action: ACTION,
      deadlineAt: NOW + SETTLE_WINDOW_MS,
    });
  });

  it('enters conflict on 409, carrying the re-read status', () => {
    expect(
      actionReducer(submitting, { type: 'conflict', currentStatus: 'CLAIMED' })
    ).toEqual(conflict);
  });

  it('carries a null current status when the server did not send one', () => {
    expect(
      actionReducer(submitting, { type: 'conflict', currentStatus: null })
    ).toEqual({ phase: 'conflict', action: ACTION, currentStatus: null });
  });

  it('enters gone on 404', () => {
    expect(actionReducer(submitting, { type: 'not-found' })).toEqual(gone);
  });

  it('returns to idle on 400 TASK_IS_WORKFLOW_TASK', () => {
    expect(actionReducer(submitting, { type: 'workflow-managed' })).toEqual(IDLE);
  });

  it('returns to idle on 403', () => {
    expect(actionReducer(submitting, { type: 'forbidden' })).toEqual(IDLE);
  });

  it('enters failed on 503, keeping the detail for the toast', () => {
    expect(
      actionReducer(submitting, { type: 'unavailable', detail: 'boom' })
    ).toEqual(failed);
  });

  it('enters verifying on a network error rather than reporting failure', () => {
    expect(actionReducer(submitting, { type: 'network-error' })).toEqual(
      verifying
    );
  });
});

describe('verifying', () => {
  it('enters settling when the effect is observed', () => {
    expect(
      actionReducer(verifying, { type: 'verify-observed', at: NOW })
    ).toEqual(settling);
  });

  it('enters failed when the effect is not observed', () => {
    expect(
      actionReducer(verifying, { type: 'verify-not-observed', detail: null })
    ).toEqual({ phase: 'failed', action: ACTION, detail: null });
  });
});

describe('settling', () => {
  it('returns to idle when the settle predicate is met', () => {
    expect(actionReducer(settling, { type: 'settled' })).toEqual(IDLE);
  });

  it('returns to idle at the 30 s deadline', () => {
    expect(actionReducer(settling, { type: 'settle-deadline' })).toEqual(IDLE);
  });
});

describe('terminal display states', () => {
  it('leaves conflict once the refetch lands', () => {
    expect(actionReducer(conflict, { type: 'refetched' })).toEqual(IDLE);
  });

  it('leaves gone once acknowledged', () => {
    expect(actionReducer(gone, { type: 'acknowledged' })).toEqual(IDLE);
  });

  it('resubmits from failed when the toast Retry is used', () => {
    expect(actionReducer(failed, { type: 'retry' })).toEqual(submitting);
  });

  it('leaves failed when acknowledged instead', () => {
    expect(actionReducer(failed, { type: 'acknowledged' })).toEqual(IDLE);
  });

  it('does not resubmit from a phase that never failed', () => {
    expect(actionReducer(settling, { type: 'retry' })).toBe(settling);
  });
});

describe('entity locking', () => {
  it('reports idle as not busy', () => {
    expect(isBusy(IDLE)).toBe(false);
    expect(pendingAction(IDLE)).toBeNull();
  });

  it('reports every non-idle phase as busy', () => {
    for (const state of [
      confirming,
      submitting,
      verifying,
      settling,
      conflict,
      gone,
      failed,
    ]) {
      expect(isBusy(state)).toBe(true);
      expect(pendingAction(state)).toBe(ACTION);
    }
  });
});
