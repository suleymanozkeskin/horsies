import { useSyncExternalStore } from 'react';

import { assertNever } from '@/lib/exhaustive';
import { instantMs } from '@/lib/time-text';
import type { RunTiming } from '@/types/timing';

/** The clock of one run read: the database time of the read (`as_of`), the
 * browser time at which the response arrived, and whether a segment or a
 * pause was open at the read. */
export interface ReadClock {
  asOfMs: number;
  receivedAtMs: number;
  open: boolean;
}

/** The clock of a run read from its timing and its arrival time. */
export function readClockOf(timing: RunTiming, receivedAtMs: number): ReadClock {
  switch (timing.kind) {
    case 'unavailable':
      return { asOfMs: instantMs(timing.as_of), receivedAtMs, open: false };
    case 'composed':
      return {
        asOfMs: instantMs(timing.as_of),
        receivedAtMs,
        open: timing.open_segments,
      };
    default:
      return assertNever(timing, 'run timing');
  }
}

const TICK_INTERVAL_MS = 1_000;

// The store of one shared one-second ticker. The interval runs only while at
// least one component subscribes, so a page with no open segment runs none.
let latestTickMs = Date.now();
const tickListeners = new Set<() => void>();
let tickTimerId: ReturnType<typeof setInterval> | null = null;

function subscribeTicks(listener: () => void): () => void {
  tickListeners.add(listener);
  if (tickTimerId === null) {
    latestTickMs = Date.now();
    tickTimerId = setInterval(() => {
      latestTickMs = Date.now();
      for (const notify of tickListeners) {
        notify();
      }
    }, TICK_INTERVAL_MS);
  }
  return () => {
    tickListeners.delete(listener);
    if (tickListeners.size === 0 && tickTimerId !== null) {
      clearInterval(tickTimerId);
      tickTimerId = null;
    }
  };
}

const readTick = (): number => latestTickMs;
const subscribeNoTicks = (): (() => void) => () => undefined;
const readNoTick = (): number => 0;

/**
 * The time to which an open segment is drawn. With `active` it is `as_of`
 * plus the browser time elapsed since the response arrived, advanced by a
 * one-second clock; the elapsed time comes from the browser clock alone, so a
 * skew between the browser and the database does not move the drawing. With
 * `active` false it is `as_of` and no clock runs. No refetch happens per
 * second: a new read replaces the clock.
 */
export function useDrawTime(clock: ReadClock, active: boolean): number {
  const tickMs = useSyncExternalStore(
    active ? subscribeTicks : subscribeNoTicks,
    active ? readTick : readNoTick
  );
  return active
    ? clock.asOfMs + Math.max(0, tickMs - clock.receivedAtMs)
    : clock.asOfMs;
}
