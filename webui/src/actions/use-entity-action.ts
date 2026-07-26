// Drives one entity's action lifecycle: confirm -> submit -> settle, with every
// HTTP outcome mapped to its own state and message.
//
// The registry is per-entity by construction — one hook instance per detail
// panel, and the panel disables every action button while the state is not
// idle. Status is never rendered optimistically: the panel keeps showing server
// data, and the settle predicate is what ends the boost.

import { useCallback, useEffect, useReducer, useRef } from 'react';

import { useQueryClient } from '@tanstack/react-query';

import {
  actionReducer,
  IDLE,
  isBusy,
  pendingAction,
  type ActionState,
} from '@/actions/action-state';
import { useCapability } from '@/actions/capability';
import {
  conflictMessage,
  FORBIDDEN_MESSAGE,
  goneMessage,
  SCHEMA_INCOMPATIBLE_MESSAGE,
  successMessage,
  unavailableMessage,
  unverifiedMessage,
  verifiedMessage,
  WORKFLOW_MANAGED_MESSAGE,
  type SuccessContext,
} from '@/actions/copy';
import type { SettleContext } from '@/actions/settle';
import { useToast } from '@/components/ui/toast';
import { fallbackInterval, useLiveMode } from '@/events/live-provider';
import { ApiError, NetworkError } from '@/lib/http';
import { QUERY_ROOT } from '@/lib/query-keys';
import { invokeAction, type ActionArgs } from '@/services/actions-api';
import {
  SCHEMA_INCOMPATIBLE_CODE,
  type ActionKind,
  type EntityRef,
} from '@/types/actions';

/** Entity poll cadence while an action settles and the event stream is down. */
const SETTLE_BOOST_MS = 1_000;

export interface EntityActionOptions<TDetail> {
  entity: EntityRef;
  /** Freshest entity data, or undefined before the first load. */
  detail: TDetail | undefined;
  /** Pre-action values the settle predicates compare against. */
  snapshot: (detail: TDetail | undefined) => SettleContext;
  /** Pure: does this data show the action landed? */
  hasSettled: (
    action: ActionKind,
    detail: TDetail,
    context: SettleContext
  ) => boolean;
  /** Re-read the entity after a lost response. */
  reread: () => Promise<TDetail | undefined>;
  /** Data the success copy needs (draining node count, worker hostname). */
  successContext: (detail: TDetail | undefined) => SuccessContext;
  /** The entity 404'd — retention may have removed it; close the panel. */
  onGone?: () => void;
}

export interface EntityActionControls {
  state: ActionState;
  busy: boolean;
  pending: ActionKind | null;
  /** Poll interval for the entity query while an action settles offline. */
  boostInterval: number | false;
  begin: (action: ActionKind) => void;
  dismiss: () => void;
  confirm: (args: ActionArgs) => void;
}

export function useEntityAction<TDetail>(
  options: EntityActionOptions<TDetail>
): EntityActionControls {
  const [state, dispatch] = useReducer(actionReducer, IDLE);
  const queryClient = useQueryClient();
  const toast = useToast();
  const liveMode = useLiveMode();
  const { revokeActions } = useCapability();
  // Captured at submit time; the settle predicates compare against it.
  const contextRef = useRef<SettleContext>({
    wasStatus: null,
    retryCountBefore: null,
  });
  // Latest options without rebuilding every callback on each panel render.
  const optionsRef = useRef(options);
  optionsRef.current = options;

  const entityKind = options.entity.kind;
  const entityId = options.entity.id;

  /** §13.1.6 — aggregates are invalidated once on success, in both modes. In
   * fallback mode this is load-bearing: list polling is conditional, so an
   * action that removes the last active row would otherwise freeze the list on
   * stale data with polling stopped. */
  const invalidateAggregates = useCallback((): void => {
    const roots: string[] = [QUERY_ROOT.taskList, QUERY_ROOT.taskStats];
    if (entityKind === 'workflow') {
      roots.push(QUERY_ROOT.workflowRuns, QUERY_ROOT.workflowNode);
    }
    roots.push(
      entityKind === 'task' ? QUERY_ROOT.taskDetail : QUERY_ROOT.workflowRun
    );
    for (const root of roots) {
      void queryClient.invalidateQueries({ queryKey: [root] });
    }
  }, [entityKind, queryClient]);

  const refetchEntity = useCallback((): void => {
    void queryClient.invalidateQueries({
      queryKey: [
        entityKind === 'task' ? QUERY_ROOT.taskDetail : QUERY_ROOT.workflowRun,
      ],
    });
  }, [entityKind, queryClient]);

  const run = useCallback(
    async (
      action: ActionKind,
      args: ActionArgs,
      context: SettleContext
    ): Promise<void> => {
      const retryFromToast = (): void => {
        dispatch({ type: 'retry' });
        void run(action, args, context);
      };
      try {
        const response = await invokeAction(action, entityId, args);
        dispatch({ type: 'succeeded', at: Date.now() });
        toast.notify({
          tone:
            response.warning === 'post_resume_recovery_failed'
              ? 'warning'
              : 'success',
          message: successMessage(
            action,
            response,
            optionsRef.current.successContext(optionsRef.current.detail)
          ),
        });
        invalidateAggregates();
        return;
      } catch (error) {
        if (error instanceof NetworkError) {
          // A failed POST is not proof of no effect — look before reporting.
          dispatch({ type: 'network-error' });
          const fresh = await optionsRef.current.reread();
          const observed =
            fresh !== undefined &&
            optionsRef.current.hasSettled(action, fresh, context);
          if (observed) {
            dispatch({ type: 'verify-observed', at: Date.now() });
            toast.notify({ tone: 'success', message: verifiedMessage(action) });
            invalidateAggregates();
            return;
          }
          dispatch({ type: 'verify-not-observed', detail: null });
          toast.notify({
            tone: 'error',
            message: unverifiedMessage(action),
            durationMs: 0,
            action: { label: 'Retry', onSelect: retryFromToast },
          });
          return;
        }
        if (!(error instanceof ApiError)) {
          throw error;
        }
        switch (error.status) {
          case 404:
            dispatch({ type: 'not-found' });
            toast.notify({ tone: 'error', message: goneMessage(entityKind) });
            invalidateAggregates();
            optionsRef.current.onGone?.();
            return;
          case 409:
            // A schema mismatch arrives on the same status code but is a
            // standing condition, not a lost race: revoke instead of refetching.
            if (error.code === SCHEMA_INCOMPATIBLE_CODE) {
              dispatch({ type: 'forbidden' });
              toast.notify({
                tone: 'error',
                message: SCHEMA_INCOMPATIBLE_MESSAGE,
              });
              revokeActions();
              return;
            }
            dispatch({ type: 'conflict', currentStatus: error.currentStatus });
            toast.notify({
              tone: 'warning',
              message: conflictMessage(action, error.currentStatus),
            });
            refetchEntity();
            return;
          case 400:
            // The buttons are hidden for workflow-bound rows, so reaching this
            // means the client's view was stale.
            console.error('action rejected as workflow-managed', {
              action,
              entityId,
              code: error.code,
            });
            dispatch({ type: 'workflow-managed' });
            toast.notify({ tone: 'error', message: WORKFLOW_MANAGED_MESSAGE });
            refetchEntity();
            return;
          case 403:
            dispatch({ type: 'forbidden' });
            toast.notify({ tone: 'error', message: FORBIDDEN_MESSAGE });
            revokeActions();
            return;
          default:
            dispatch({ type: 'unavailable', detail: error.detail });
            toast.notify({
              tone: 'error',
              message: unavailableMessage(action, error.detail),
              durationMs: 0,
              action: { label: 'Retry', onSelect: retryFromToast },
            });
            return;
        }
      }
    },
    [entityId, entityKind, invalidateAggregates, refetchEntity, revokeActions, toast]
  );

  const begin = useCallback((action: ActionKind): void => {
    dispatch({ type: 'click', action });
  }, []);

  const dismiss = useCallback((): void => {
    dispatch({ type: 'dismiss' });
  }, []);

  const confirm = useCallback(
    (args: ActionArgs): void => {
      if (state.phase !== 'confirming') {
        return;
      }
      const action = state.action;
      const context = optionsRef.current.snapshot(optionsRef.current.detail);
      contextRef.current = context;
      dispatch({ type: 'confirm' });
      void run(action, args, context);
    },
    [run, state]
  );

  // `conflict` and `gone` are display-only stops: the toast carries the message
  // and the refetch is already in flight, so release the entity's buttons.
  useEffect(() => {
    switch (state.phase) {
      case 'conflict': {
        const timer = window.setTimeout(() => dispatch({ type: 'refetched' }), 0);
        return () => window.clearTimeout(timer);
      }
      case 'gone': {
        const timer = window.setTimeout(
          () => dispatch({ type: 'acknowledged' }),
          0
        );
        return () => window.clearTimeout(timer);
      }
      default:
        return;
    }
  }, [state.phase]);

  const settledNow =
    state.phase === 'settling' &&
    options.detail !== undefined &&
    options.hasSettled(state.action, options.detail, contextRef.current);

  useEffect(() => {
    if (settledNow) {
      dispatch({ type: 'settled' });
    }
  }, [settledNow]);

  const deadlineAt = state.phase === 'settling' ? state.deadlineAt : null;
  useEffect(() => {
    if (deadlineAt === null) {
      return;
    }
    const remaining = Math.max(0, deadlineAt - Date.now());
    const timer = window.setTimeout(
      () => dispatch({ type: 'settle-deadline' }),
      remaining
    );
    return () => window.clearTimeout(timer);
  }, [deadlineAt]);

  return {
    state,
    busy: isBusy(state),
    pending: pendingAction(state),
    boostInterval:
      state.phase === 'settling'
        ? fallbackInterval(liveMode, SETTLE_BOOST_MS)
        : false,
    begin,
    dismiss,
    confirm,
  };
}
