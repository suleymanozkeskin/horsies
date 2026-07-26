// Per-entity action state machine.
//
// One instance per (entity kind, id). While it is not `idle`, every action
// button for that entity is disabled — one in-flight action per entity, always.
// The reducer is pure: `now` arrives on the events that need it, so the 30 s
// settling deadline is testable without a clock.

import type { ActionKind } from '@/types/actions';

/** How long the UI keeps explaining a just-submitted action before it stops
 * treating residual states as part of that action. */
export const SETTLE_WINDOW_MS = 30_000;

export type ActionState =
  | { phase: 'idle' }
  | { phase: 'confirming'; action: ActionKind }
  | { phase: 'submitting'; action: ActionKind }
  /** The POST never got a response; re-read the entity before claiming failure. */
  | { phase: 'verifying'; action: ActionKind }
  | { phase: 'settling'; action: ActionKind; deadlineAt: number }
  | { phase: 'conflict'; action: ActionKind; currentStatus: string | null }
  | { phase: 'gone'; action: ActionKind }
  | { phase: 'failed'; action: ActionKind; detail: string | null };

export type ActionPhase = ActionState['phase'];

export type ActionEvent =
  | { type: 'click'; action: ActionKind }
  | { type: 'dismiss' }
  | { type: 'confirm' }
  | { type: 'succeeded'; at: number }
  | { type: 'conflict'; currentStatus: string | null }
  | { type: 'not-found' }
  /** 400 TASK_IS_WORKFLOW_TASK — unreachable via the UI; treated as a no-op. */
  | { type: 'workflow-managed' }
  | { type: 'forbidden' }
  | { type: 'unavailable'; detail: string | null }
  | { type: 'network-error' }
  | { type: 'verify-observed'; at: number }
  | { type: 'verify-not-observed'; detail: string | null }
  | { type: 'settled' }
  | { type: 'settle-deadline' }
  /** The conflict refetch finished; the panel now shows server truth. */
  | { type: 'refetched' }
  /** The terminal toast was dismissed or replaced. */
  | { type: 'acknowledged' }
  /** Retry from the failure toast — confirmation was already given. */
  | { type: 'retry' };

export const IDLE: ActionState = { phase: 'idle' };

/** True while the entity has an action in flight or awaiting acknowledgement.
 * Every action affordance for that entity is disabled while this holds. */
export const isBusy = (state: ActionState): boolean => state.phase !== 'idle';

/** The action a non-idle state belongs to, or null when idle. */
export function pendingAction(state: ActionState): ActionKind | null {
  return state.phase === 'idle' ? null : state.action;
}

function assertNever(value: never): never {
  throw new Error(`unhandled action event: ${JSON.stringify(value)}`);
}

export function actionReducer(
  state: ActionState,
  event: ActionEvent
): ActionState {
  switch (event.type) {
    case 'click':
      return state.phase === 'idle'
        ? { phase: 'confirming', action: event.action }
        : state;

    case 'dismiss':
      return state.phase === 'confirming' ? IDLE : state;

    case 'confirm':
      return state.phase === 'confirming'
        ? { phase: 'submitting', action: state.action }
        : state;

    case 'succeeded':
      return state.phase === 'submitting'
        ? {
            phase: 'settling',
            action: state.action,
            deadlineAt: event.at + SETTLE_WINDOW_MS,
          }
        : state;

    case 'conflict':
      return state.phase === 'submitting'
        ? {
            phase: 'conflict',
            action: state.action,
            currentStatus: event.currentStatus,
          }
        : state;

    case 'not-found':
      return state.phase === 'submitting'
        ? { phase: 'gone', action: state.action }
        : state;

    // The buttons are hidden for workflow-bound rows, so this can only mean the
    // client's view was stale. Refetch and return to idle; nothing is pending.
    case 'workflow-managed':
    case 'forbidden':
      return state.phase === 'submitting' ? IDLE : state;

    case 'unavailable':
      return state.phase === 'submitting'
        ? { phase: 'failed', action: state.action, detail: event.detail }
        : state;

    case 'network-error':
      return state.phase === 'submitting'
        ? { phase: 'verifying', action: state.action }
        : state;

    case 'verify-observed':
      return state.phase === 'verifying'
        ? {
            phase: 'settling',
            action: state.action,
            deadlineAt: event.at + SETTLE_WINDOW_MS,
          }
        : state;

    case 'verify-not-observed':
      return state.phase === 'verifying'
        ? { phase: 'failed', action: state.action, detail: event.detail }
        : state;

    case 'settled':
    case 'settle-deadline':
      return state.phase === 'settling' ? IDLE : state;

    case 'refetched':
      return state.phase === 'conflict' ? IDLE : state;

    case 'acknowledged':
      return state.phase === 'gone' || state.phase === 'failed' ? IDLE : state;

    case 'retry':
      return state.phase === 'failed'
        ? { phase: 'submitting', action: state.action }
        : state;

    default:
      return assertNever(event);
  }
}
