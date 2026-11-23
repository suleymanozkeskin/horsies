import { describe, expect, it } from 'vitest';

import { formatInstant, formatMilliseconds } from '@/lib/format-duration';
import { instantMs } from '@/lib/time-text';

describe('formatMilliseconds', () => {
  it('keeps milliseconds below a second and one decimal below a minute', () => {
    expect(formatMilliseconds(0)).toBe('0ms');
    expect(formatMilliseconds(850)).toBe('850ms');
    expect(formatMilliseconds(4_200)).toBe('4.2s');
    expect(formatMilliseconds(59_900)).toBe('59.9s');
  });

  it('writes minutes with seconds, and hours with minutes', () => {
    expect(formatMilliseconds(60_000)).toBe('1m 0s');
    expect(formatMilliseconds(725_000)).toBe('12m 5s');
    expect(formatMilliseconds(12_000_000)).toBe('3h 20m');
  });

  it('keeps the sign of a negative duration', () => {
    expect(formatMilliseconds(-1_500)).toBe('-1.5s');
  });
});

describe('formatInstant', () => {
  it('writes the local date and time with milliseconds', () => {
    const epochMs = instantMs('2026-11-22T12:00:04.250000Z');
    const local = new Date(epochMs);

    expect(formatInstant(epochMs)).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.250$/);
    expect(formatInstant(epochMs)).toContain(String(local.getFullYear()));
  });
});

describe('instantMs', () => {
  it('reads the six fraction digits of the API to the millisecond', () => {
    expect(instantMs('2026-11-17T13:59:14.500000Z')).toBe(Date.UTC(2026, 10, 17, 13, 59, 14, 500));
  });

  it('throws on a text that is not a time, naming the text', () => {
    expect(() => instantMs('yesterday')).toThrow('time text does not parse: "yesterday"');
  });
});
