import { describe, expect, it } from 'vitest';

import {
  linearTicks,
  LOG_MIN_MS,
  logDomain,
  logMajorTicks,
  logMinorTicks,
  logPct,
} from '@/components/workflows/time-scale';
import { formatAxisTick } from '@/lib/format-duration';

describe('linearTicks', () => {
  it('labels round times from 0, at most 5 parts of the axis', () => {
    // 2m 3.9s: steps of 30s.
    expect(linearTicks(123_900)).toEqual([0, 30_000, 60_000, 90_000, 120_000]);
    // 3.5s: steps of 1s.
    expect(linearTicks(3_500)).toEqual([0, 1_000, 2_000, 3_000]);
  });

  it('keeps at most 8 ticks on a very long axis', () => {
    expect(linearTicks(30 * 86_400_000)).toHaveLength(8);
  });

  it('has the start alone on an axis of one instant', () => {
    expect(linearTicks(0.001)).toEqual([0]);
  });
});

describe('the log axis', () => {
  it('labels 0.1ms to 10s in decades, then 1m, 10m and 1h', () => {
    const domain = logDomain(2 * 3_600_000);

    expect(logMajorTicks(domain).map(formatAxisTick)).toEqual([
      '0.1ms',
      '1ms',
      '10ms',
      '100ms',
      '1s',
      '10s',
      '1m',
      '10m',
      '1h',
    ]);
    // The domain of 2 hours ends at 3 hours, a minor line.
    expect(domain.endMs).toBe(3 * 3_600_000);
  });

  it('ends at the first line above the longest time, and at least at 1s', () => {
    expect(logDomain(4_820).endMs).toBe(5_000);
    expect(logDomain(60_900).endMs).toBe(120_000);
    expect(logDomain(2).endMs).toBe(1_000);
    expect(logDomain(0).endMs).toBe(1_000);
  });

  it('reaches past 10 hours with decades of hours, so a long time is not cut', () => {
    const domain = logDomain(30 * 3_600_000);

    expect(domain.endMs).toBe(40 * 3_600_000);
    expect(logPct(30 * 3_600_000, domain)).toBeLessThan(100);
  });

  it('draws minor ticks that crowd toward the next labeled line: it cannot pass as linear', () => {
    const domain = logDomain(9_000);
    const minors = logMinorTicks(domain).filter(ms => ms > 1 && ms < 10);
    const positions = minors.map(ms => logPct(ms, domain));
    const gaps = positions.slice(1).map((position, index) => position - (positions[index] ?? 0));

    expect(minors).toEqual([2, 3, 4, 5, 6, 7, 8, 9]);
    for (let index = 1; index < gaps.length; index += 1) {
      expect(gaps[index]).toBeLessThan(gaps[index - 1] ?? 0);
    }
  });

  it('places a time by its decade, and clamps a time below the start or past the end', () => {
    const domain = logDomain(9_000);

    // 0.1ms to 10s is 5 decades; 10ms is 2 of them.
    expect(logPct(10, domain)).toBeCloseTo(40);
    expect(logPct(0, domain)).toBe(0);
    expect(logPct(LOG_MIN_MS / 10, domain)).toBe(0);
    expect(logPct(60_000, domain)).toBe(100);
  });
});
