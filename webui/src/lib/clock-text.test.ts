import { describe, expect, it } from 'vitest';

import {
  clockText,
  clockTextFrom,
  dateLineText,
  dateText,
  dayOffset,
  zoneText,
} from '@/lib/clock-text';

/** A local time, so the expectations hold in every time zone. */
const local = (day: number, hour: number, minute: number, second: number, ms: number): number =>
  new Date(2026, 10, day, hour, minute, second, ms).getTime();

describe('dateText and clockText', () => {
  it('write the local date, and the local time of day with milliseconds', () => {
    const at = local(24, 9, 4, 26, 44);

    expect(dateText(at)).toBe('2026-11-24');
    expect(clockText(at)).toBe('09:04:26.044');
  });
});

describe('dayOffset', () => {
  it('counts local calendar days, not 24-hour steps', () => {
    expect(dayOffset(local(24, 23, 59, 0, 0), local(25, 0, 1, 0, 0))).toBe(1);
    expect(dayOffset(local(24, 0, 0, 0, 0), local(24, 23, 59, 59, 999))).toBe(0);
    expect(dayOffset(local(24, 0, 0, 0, 0), local(23, 12, 0, 0, 0))).toBe(-1);
  });
});

describe('clockTextFrom', () => {
  it('writes a time of the base day with no suffix', () => {
    expect(clockTextFrom(local(24, 9, 0, 0, 0), local(24, 9, 0, 1, 500))).toBe('09:00:01.500');
  });

  it('marks a time on another day with its day offset', () => {
    expect(clockTextFrom(local(24, 23, 59, 0, 0), local(25, 0, 0, 2, 0))).toBe('00:00:02.000 +1d');
    expect(clockTextFrom(local(24, 0, 0, 1, 0), local(23, 23, 59, 59, 0))).toBe('23:59:59.000 -1d');
  });
});

describe('zoneText and dateLineText', () => {
  it('name the local zone once, after the date', () => {
    const at = local(24, 9, 0, 0, 0);

    expect(zoneText(at)).not.toBe('');
    expect(dateLineText(at)).toBe(`2026-11-24 · times in ${zoneText(at)}`);
  });
});
