import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { readClockOf, useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { runIsChanging } from '@/hooks/use-workflow-run';
import { instantMs } from '@/lib/time-text';
import {
  OPEN_RUN_WITH_PAUSES,
  UNAVAILABLE_RUN,
  workflowRun,
} from '@/test/workflow-fixtures';
import type { RunTiming } from '@/types/timing';

const AS_OF_MS = instantMs('2026-11-22T12:01:40Z');
/** The browser clock is 30 s behind the database clock. */
const RECEIVED_AT_MS = AS_OF_MS - 30_000;

const clock = (open: boolean): ReadClock => ({
  asOfMs: AS_OF_MS,
  receivedAtMs: RECEIVED_AT_MS,
  open,
});

const closedRun: RunTiming = {
  kind: 'composed',
  as_of: '2026-11-22T12:01:40Z',
  captured_since: '2026-11-22T12:00:00Z',
  coverage: { kind: 'exact' },
  range_started_at: '2026-11-22T12:00:00Z',
  range_ended_at: '2026-11-22T12:00:50Z',
  open_segments: false,
  pauses: [],
};

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(RECEIVED_AT_MS);
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe('readClockOf', () => {
  it('takes the read time of both run timings and the open flag of a composed one', () => {
    expect(readClockOf(OPEN_RUN_WITH_PAUSES, 5)).toEqual({
      asOfMs: AS_OF_MS,
      receivedAtMs: 5,
      open: true,
    });
    expect(readClockOf(UNAVAILABLE_RUN, 5)).toEqual({
      asOfMs: AS_OF_MS,
      receivedAtMs: 5,
      open: false,
    });
  });
});

describe('useDrawTime', () => {
  it('advances from as_of by the browser time since the read, once a second', () => {
    const { result } = renderHook(() => useDrawTime(clock(true), true));

    expect(result.current).toBe(AS_OF_MS);

    act(() => {
      vi.advanceTimersByTime(1_000);
    });
    expect(result.current).toBe(AS_OF_MS + 1_000);

    act(() => {
      vi.advanceTimersByTime(2_500);
    });
    // Two more ticks; the half second waits for the next one.
    expect(result.current).toBe(AS_OF_MS + 3_000);
  });

  it('stays at as_of and runs no timer while inactive', () => {
    const { result } = renderHook(() => useDrawTime(clock(true), false));

    act(() => {
      vi.advanceTimersByTime(5_000);
    });

    expect(result.current).toBe(AS_OF_MS);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('shares one timer between subscribers and stops it with the last one', () => {
    const first = renderHook(() => useDrawTime(clock(true), true));
    const second = renderHook(() => useDrawTime(clock(true), true));

    expect(vi.getTimerCount()).toBe(1);

    first.unmount();
    expect(vi.getTimerCount()).toBe(1);
    second.unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('runIsChanging', () => {
  it('is true for an active run status', () => {
    expect(runIsChanging(workflowRun('RUNNING', [], closedRun))).toBe(true);
  });

  it('is true for a paused or cancelled run whose timing is open', () => {
    expect(runIsChanging(workflowRun('PAUSED', [], OPEN_RUN_WITH_PAUSES))).toBe(true);
    expect(runIsChanging(workflowRun('CANCELLED', [], OPEN_RUN_WITH_PAUSES))).toBe(true);
  });

  it('is false for an ended run whose timing is closed or unavailable', () => {
    expect(runIsChanging(workflowRun('COMPLETED', [], closedRun))).toBe(false);
    expect(runIsChanging(workflowRun('COMPLETED', [], UNAVAILABLE_RUN))).toBe(false);
  });
});
