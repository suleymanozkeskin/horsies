import { describe, expect, it } from 'vitest';

import {
  formatAxisTick,
  formatDuration,
  formatInstant,
  formatMilliseconds,
} from '@/lib/format-duration';
import { instantMs } from '@/lib/time-text';

/** The 27 measured values of the approved duration rule: (case, ms, text). */
const VALUE_CASES: readonly (readonly [string, number, string])[] = [
  ["owner's preview: queue wait", 7.05, '7.05ms'],
  ["owner's preview: slot wait", 2.31, '2.31ms'],
  ["owner's preview: finalizing", 0.812, '0.812ms'],
  ["owner's preview: running", 4820.0, '4.82s'],
  ["owner's preview: retry wait", 11400.0, '11.4s'],
  ["owner's preview: retry wait of a minute", 60900.0, '1m 0.9s'],
  ["owner's preview: lost worker", 175400.0, '2m 55.4s'],
  ["owner's preview: task total", 123900.0, '2m 3.9s'],
  ['rule: below 1s, 3 digits', 48.2, '48.2ms'],
  ['rule: below 1s, no decimal', 482.0, '482ms'],
  ['rule: trailing zeros in ms', 5.0, '5.00ms'],
  ['rule: trailing zeros in s', 5000.0, '5.00s'],
  ['rule: from 1h', 3723400.0, '1h 2m 3.4s'],
  ['boundary: 999.6ms', 999.6, '1.00s'],
  ['boundary: 59.96s', 59960.0, '1m 0.0s'],
  ['boundary: 59m 59.96s', 3599960.0, '1h 0m 0.0s'],
  ['boundary: 9.996ms', 9.996, '10.0ms'],
  ['boundary: 99.96ms', 99.96, '100ms'],
  ['boundary: 9.996s', 9996.0, '10.0s'],
  ['a half rounds up: 2.295ms', 2.295, '2.30ms'],
  ['record: supplier queue wait 1', 7.052, '7.05ms'],
  ['record: supplier finalizing 1', 0.821, '0.821ms'],
  ['record: supplier running 1', 4820.107, '4.82s'],
  ['record: supplier retry wait 1', 11383.807, '11.4s'],
  ['record: supplier retry wait 3', 60880.712, '1m 0.9s'],
  ['record: supplier task total', 123910.222, '2m 3.9s'],
  ['record: lost worker', 174660.981, '2m 54.7s'],
];

/** The 7 axis ticks of the approved duration rule: (case, ms, label). */
const TICK_CASES: readonly (readonly [string, number, string])[] = [
  ['axis tick 0.1ms', 0.1, '0.1ms'],
  ['axis tick 1s', 1000.0, '1s'],
  ['axis tick 10s', 10000.0, '10s'],
  ['axis tick 1m', 60000.0, '1m'],
  ['axis tick 10m', 600000.0, '10m'],
  ['axis tick 1h', 3600000.0, '1h'],
  ['axis tick 1m 30s', 90000.0, '1m 30s'],
];

describe('formatMilliseconds: the one duration rule', () => {
  it.each(VALUE_CASES)('%s: %f ms is %s', (_case, ms, text) => {
    expect(formatMilliseconds(ms)).toBe(text);
  });

  it('writes zero as 0ms', () => {
    expect(formatMilliseconds(0)).toBe('0ms');
    expect(formatMilliseconds(0.0004)).toBe('0ms');
  });

  it('keeps the sign of a negative duration: two times in the wrong order', () => {
    expect(formatMilliseconds(-1_500)).toBe('-1.50s');
    expect(formatMilliseconds(-0.812)).toBe('-0.812ms');
  });

  it('keeps counting hours past a day, with no unit of days', () => {
    // 145 h 59 min 12.3 s: the uptime of a worker that runs for six days.
    expect(formatMilliseconds(525_552_300)).toBe('145h 59m 12.3s');
  });

  it('throws on a value that is not a finite number: a defect of the caller', () => {
    expect(() => formatMilliseconds(Number.NaN)).toThrow('duration is not a finite number of milliseconds: NaN');
    expect(() => formatMilliseconds(Number.POSITIVE_INFINITY)).toThrow('Infinity');
  });
});

describe('formatAxisTick', () => {
  it.each(TICK_CASES)('%s: %f ms is %s', (_case, ms, label) => {
    expect(formatAxisTick(ms)).toBe(label);
  });

  it('labels the start of an axis 0 and a round time with no decimals', () => {
    expect(formatAxisTick(0)).toBe('0');
    expect(formatAxisTick(15_000)).toBe('15s');
    expect(formatAxisTick(5_400_000)).toBe('1h 30m');
  });
});

describe('formatDuration', () => {
  it('writes seconds with the duration rule, and a dash for null', () => {
    expect(formatDuration(4.82)).toBe('4.82s');
    expect(formatDuration(0.012345)).toBe('12.3ms');
    expect(formatDuration(null)).toBe('—');
  });

  it('rounds the seconds part and carries at 60 (issue #2288)', () => {
    expect(formatDuration(240.023)).toBe('4m 0.0s');
    expect(formatDuration(3599.999)).toBe('1h 0m 0.0s');
    expect(formatDuration(61.5)).toBe('1m 1.5s');
    expect(formatDuration(59.9)).toBe('59.9s');
    expect(formatDuration(59.9996)).toBe('1m 0.0s');
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
  it('reads the six fraction digits of the API, the microseconds included', () => {
    expect(instantMs('2026-11-17T13:59:14.500000Z')).toBe(Date.UTC(2026, 10, 17, 13, 59, 14, 500));
    const due = instantMs('2026-11-24T14:37:08.409045Z');
    const claimed = instantMs('2026-11-24T14:37:08.412498Z');
    // 3.453ms: Date.parse alone keeps 409 and 412, and gives 3.00ms.
    expect(formatMilliseconds(claimed - due)).toBe('3.45ms');
  });

  it('reads a time with no fraction and a time with a zone offset', () => {
    expect(instantMs('2026-11-22T12:00:00Z')).toBe(Date.UTC(2026, 10, 22, 12, 0, 0));
    expect(instantMs('2026-11-22T13:00:00.250500+01:00')).toBe(Date.UTC(2026, 10, 22, 12, 0, 0, 250) + 0.5);
  });

  it('throws on a text that is not a time, naming the text', () => {
    expect(() => instantMs('yesterday')).toThrow('time text does not parse: "yesterday"');
  });
});
