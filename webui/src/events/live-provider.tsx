// SSE consumption and the events <-> fallback-polling switch.
//
// While the stream is connected, freshness comes from event-driven
// invalidation and interval polling is off. Once the stream has been down for
// longer than the grace window, the client switches to the §12 fallback
// cadences; a successful reconnect switches back and sweeps every event-covered
// query once, because arbitrarily many events were missed while disconnected.

import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react';

import { useQueryClient } from '@tanstack/react-query';

import { uiConfig } from '@/lib/config';
import {
  invalidationRootsFor,
  RECONNECT_SWEEP_ROOTS,
} from '@/events/invalidation';
import { parseMonitoringEvent } from '@/events/types';

/** `events` = live stream connected. `fallback` = poll on the §12 cadences. */
export type LiveMode = 'events' | 'fallback';

/** Grace period before a disconnect turns polling back on, so a reconnect
 * inside the window costs nothing. */
const FALLBACK_AFTER_MS = 5_000;
const BACKOFF_MIN_MS = 1_000;
const BACKOFF_MAX_MS = 30_000;

const LiveModeContext = createContext<LiveMode>('fallback');

/** Fallback polling is active until the stream proves itself. */
export const useLiveMode = (): LiveMode => useContext(LiveModeContext);

/** Interval for a query that only polls while the stream is down. */
export const fallbackInterval = (
  mode: LiveMode,
  intervalMs: number
): number | false => (mode === 'fallback' ? intervalMs : false);

export function LiveProvider({ children }: { children: ReactNode }): ReactNode {
  const queryClient = useQueryClient();
  const [mode, setMode] = useState<LiveMode>('fallback');
  // Refs, not state: the connection loop must not restart when they change.
  const sourceRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const fallbackTimerRef = useRef<number | null>(null);
  const backoffRef = useRef(BACKOFF_MIN_MS);

  useEffect(() => {
    let disposed = false;

    const clearTimer = (ref: { current: number | null }): void => {
      if (ref.current !== null) {
        window.clearTimeout(ref.current);
        ref.current = null;
      }
    };

    const armFallback = (): void => {
      if (fallbackTimerRef.current !== null) {
        return;
      }
      fallbackTimerRef.current = window.setTimeout(() => {
        fallbackTimerRef.current = null;
        setMode('fallback');
      }, FALLBACK_AFTER_MS);
    };

    const scheduleReconnect = (): void => {
      if (disposed || reconnectTimerRef.current !== null) {
        return;
      }
      const delay = backoffRef.current;
      backoffRef.current = Math.min(delay * 2, BACKOFF_MAX_MS);
      reconnectTimerRef.current = window.setTimeout(() => {
        reconnectTimerRef.current = null;
        connect();
      }, delay);
    };

    const dropStream = (): void => {
      sourceRef.current?.close();
      sourceRef.current = null;
    };

    const connect = (): void => {
      if (disposed) {
        return;
      }
      const source = new EventSource(`${uiConfig.apiBase}/events`);
      sourceRef.current = source;

      source.onopen = (): void => {
        clearTimer(fallbackTimerRef);
        backoffRef.current = BACKOFF_MIN_MS;
        setMode('events');
        for (const root of RECONNECT_SWEEP_ROOTS) {
          void queryClient.invalidateQueries({ queryKey: [root] });
        }
      };

      source.onmessage = (message: MessageEvent<string>): void => {
        const event = parseMonitoringEvent(message.data);
        if (event === null) {
          return;
        }
        if (event.topic === 'degraded') {
          // The server's listener died; it closes the stream after this frame.
          dropStream();
          armFallback();
          scheduleReconnect();
          return;
        }
        for (const root of invalidationRootsFor(event)) {
          void queryClient.invalidateQueries({ queryKey: [root] });
        }
      };

      source.onerror = (): void => {
        // EventSource retries on its own schedule; take the connection over so
        // the backoff is the one this contract specifies.
        dropStream();
        armFallback();
        scheduleReconnect();
      };
    };

    connect();

    return () => {
      disposed = true;
      clearTimer(reconnectTimerRef);
      clearTimer(fallbackTimerRef);
      dropStream();
    };
  }, [queryClient]);

  return (
    <LiveModeContext.Provider value={mode}>{children}</LiveModeContext.Provider>
  );
}
