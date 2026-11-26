// Deterministic draws over domain ids: the same parts give the same value in every process and every run. A fault of
// the demo is a draw, so a retry of a declined card declines again, and a test can search for an id with a fault.
//
// Each function is the one of the Rust showcase (`simulate.rs`): SHA-256 of the parts joined by `|`, its first 8 bytes
// as a big-endian unsigned integer, divided by 2^64.

import { createHash } from "node:crypto";

import {
  STEADY_HOURLY_DEMAND,
  STEADY_MIN_DEMAND,
  STEADY_RIPPLE_AMPLITUDE,
  STEADY_RIPPLE_PERIOD_MINUTES,
  STEADY_TIMEZONE,
} from "./tuning.ts";

/** The parts of one draw: at least one. */
export type DrawParts = readonly [string, ...string[]];

/** 2^64: the count of the values of the first 8 bytes of a digest. */
const DIGEST_PREFIX_VALUES = 2 ** 64;

const SECONDS_PER_MINUTE = 60;
const MINUTES_PER_HOUR = 60;
const HOURS_PER_DAY = 24;

/** A number in [0, 1) from `parts`. Pure. */
export function unit(parts: DrawParts): number {
  const digest = createHash("sha256").update(parts.join("|"), "utf8").digest();

  return Number(digest.readBigUInt64BE(0)) / DIGEST_PREFIX_VALUES;
}

/** Whether the draw of `parts` falls under `rate` (a number in [0, 1]). Pure. */
export function draw(rate: number, parts: DrawParts): boolean {
  return unit(parts) < rate;
}

/** An integer from `low` to `high`, both included (`high >= low`). Pure. */
export function integer(low: number, high: number, parts: DrawParts): number {
  return low + Math.floor(unit(parts) * (high - low + 1));
}

/** One member of `options`. Pure. */
export function choice<T>(options: readonly [T, ...T[]], parts: DrawParts): T {
  const index = Math.floor(unit(parts) * options.length);

  // The index is below `options.length`: `unit` is below 1.
  return options[index] ?? options[0];
}

/** `count` distinct members of `options`, in draw order; at most `options.length`. Pure. */
export function sample<T>(options: readonly T[], count: number, parts: DrawParts): readonly T[] {
  const remaining = [...options];
  const selected: T[] = [];
  const picks = Math.min(count, options.length);

  for (let index = 0; index < picks; index += 1) {
    const position = Math.floor(unit([...parts, String(index)]) * remaining.length);
    const [picked] = remaining.splice(position, 1);

    if (picked !== undefined) {
      selected.push(picked);
    }
  }

  return selected;
}

/**
 * The demand factor at `instant`: the hourly curve of `STEADY_TIMEZONE`, between two hours by the minutes, times a sine
 * ripple; at least `STEADY_MIN_DEMAND`. Pure.
 */
export function demandFactor(instant: Temporal.Instant): number {
  const local = instant.toZonedDateTimeISO(STEADY_TIMEZONE);
  const low = STEADY_HOURLY_DEMAND[local.hour] ?? STEADY_MIN_DEMAND;
  const high = STEADY_HOURLY_DEMAND[(local.hour + 1) % HOURS_PER_DAY] ?? STEADY_MIN_DEMAND;
  const base = low + (high - low) * (local.minute / MINUTES_PER_HOUR);
  const seconds = instant.epochMilliseconds / 1_000;
  const ripplePeriodSeconds = STEADY_RIPPLE_PERIOD_MINUTES * SECONDS_PER_MINUTE;
  const ripple = 1 + STEADY_RIPPLE_AMPLITUDE * Math.sin((2 * Math.PI * seconds) / ripplePeriodSeconds);

  return Math.max(base * ripple, STEADY_MIN_DEMAND);
}
