// The two scales of the timing views, each with one job. A linear scale shows
// order and share: the strip of a task and the timeline of a run. A log scale
// shows size: the bars of a task and the lanes of a run. Pure: no React, no
// DOM.

const MS_PER_S = 1_000;
const MS_PER_MIN = 60_000;
const MS_PER_HOUR = 3_600_000;

/** The longest step of a linear axis: 1 day. */
const LONGEST_LINEAR_STEP_MS = 86_400_000;
/** The steps of a linear axis: round times from 1ms to 1 day. */
const LINEAR_STEPS_MS: readonly number[] = [
  1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 15_000, 30_000, 60_000,
  120_000, 300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 21_600_000,
  43_200_000, LONGEST_LINEAR_STEP_MS,
];
/** A linear axis takes the first step that splits it into this many parts or
 * fewer. */
const LINEAR_PARTS = 5;
/** The most ticks of one linear axis. */
const LINEAR_TICK_LIMIT = 8;

/** The ticks of a linear axis of `spanMs`, as offsets from its start: 0 and
 * round times after it. */
export function linearTicks(spanMs: number): number[] {
  const step = LINEAR_STEPS_MS.find(candidate => spanMs / candidate <= LINEAR_PARTS) ?? LONGEST_LINEAR_STEP_MS;
  const count = Math.min(Math.floor(spanMs / step) + 1, LINEAR_TICK_LIMIT);
  return Array.from({ length: count }, (_, index) => index * step);
}

/** The start of every log axis. */
export const LOG_MIN_MS = 0.1;
/** The decades of hours after 1 hour that the log axis can reach: 1h to
 * 100,000h (about 11 years). A longer time draws at the end of the axis. */
const HOUR_DECADES = 6;
/** The domain ends at the first line above the longest time times this, so
 * the longest bar ends before the last line. */
const LOG_HEADROOM = 1.02;
/** The smallest end of a domain: 1s. */
const LOG_SHORTEST_END_MS = 1_000;
/** A tick at the end of a domain, compared with tolerance for floating
 * point. */
const TICK_TOLERANCE = 1.0001;

const multiples = (base: number, factors: readonly number[]): number[] => factors.map(factor => base * factor);
const DECADE_FACTORS = [2, 3, 4, 5, 6, 7, 8, 9] as const;
const SIXTY_FACTORS = [2, 3, 4, 5] as const;
const HOUR_BASES = Array.from({ length: HOUR_DECADES }, (_, decade) => MS_PER_HOUR * 10 ** decade);

/** The labeled lines: 0.1ms to 10s in decades, then 1m, 10m, then hours in
 * decades. */
const LOG_MAJORS: readonly number[] = [0.1, 1, 10, 100, MS_PER_S, 10 * MS_PER_S, MS_PER_MIN, 10 * MS_PER_MIN, ...HOUR_BASES];

/** The lines between the labeled ones: they crowd toward the next labeled
 * line, so the axis cannot pass as linear. */
const LOG_MINORS: readonly number[] = [
  ...[0.1, 1, 10, 100, MS_PER_S].flatMap(base => multiples(base, DECADE_FACTORS)),
  ...multiples(10 * MS_PER_S, SIXTY_FACTORS),
  ...multiples(MS_PER_MIN, DECADE_FACTORS),
  ...multiples(10 * MS_PER_MIN, SIXTY_FACTORS),
  ...HOUR_BASES.slice(0, -1).flatMap(base => multiples(base, DECADE_FACTORS)),
];

const LOG_STOPS: readonly number[] = [...LOG_MAJORS, ...LOG_MINORS].sort((a, b) => a - b);
const LAST_LOG_STOP = LOG_MAJORS[LOG_MAJORS.length - 1] ?? LOG_SHORTEST_END_MS;

/** A log axis from LOG_MIN_MS to `endMs`. */
export interface LogDomain {
  endMs: number;
  /** The number of decades from the start to the end. */
  decades: number;
}

/** The log axis that holds `longestMs`: from 0.1ms to the first line above
 * it, and at least to 1s. */
export function logDomain(longestMs: number): LogDomain {
  const need = Math.max(longestMs * LOG_HEADROOM, LOG_SHORTEST_END_MS);
  const endMs = LOG_STOPS.find(stop => stop >= need) ?? LAST_LOG_STOP;
  return { endMs, decades: Math.log10(endMs / LOG_MIN_MS) };
}

/** The position of a time on the axis, in percent; a time below the start
 * is at 0, and a time past the end is at 100. */
export function logPct(ms: number, domain: LogDomain): number {
  const position = Math.log10(Math.max(ms, LOG_MIN_MS) / LOG_MIN_MS) / domain.decades;
  return Math.min(Math.max(position, 0), 1) * 100;
}

const onAxis = (domain: LogDomain) => (tick: number): boolean => tick <= domain.endMs * TICK_TOLERANCE;

/** The labeled lines of the axis, in time order. */
export const logMajorTicks = (domain: LogDomain): number[] => LOG_MAJORS.filter(onAxis(domain));

/** The lines between the labeled ones, in time order. */
export const logMinorTicks = (domain: LogDomain): number[] =>
  LOG_MINORS.filter(onAxis(domain)).sort((a, b) => a - b);
