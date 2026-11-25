// Shared time/duration formatters for the monitoring surfaces. Every duration
// of the app gets its text from `formatMilliseconds`; the other duration
// formatters convert their input and delegate to it.

const US_PER_MS = 1_000;
const US_PER_S = 1_000_000;
const US_PER_MIN = 60 * US_PER_S;
const US_PER_TENTH_S = 100_000;
const TENTHS_PER_MIN = 600;
const TENTHS_PER_HOUR = 36_000;
const SECONDS_PER_MIN = 60;
const SECONDS_PER_HOUR = 3_600;
const MS_PER_S = 1_000;
const MS_PER_MIN = 60_000;
/** A duration below one minute keeps this many significant digits. */
const SIGNIFICANT_DIGITS = 3;
/** The largest rounded value with SIGNIFICANT_DIGITS digits, plus one. */
const DIGITS_OVERFLOW = 10 ** SIGNIFICANT_DIGITS;

/** A value with SIGNIFICANT_DIGITS digits: `digits / 10^decimals` of a unit.
 * `2.30` is { digits: 230, decimals: 2 }. */
interface SignificantValue {
  digits: number;
  decimals: number;
}

/** `us / unitUs` rounded to SIGNIFICANT_DIGITS significant digits; a half
 * rounds up. `us` is a whole positive number of microseconds. A value that
 * rounds up to the next power of ten loses one decimal: 9.996 is 10.0. */
function significantValue(us: number, unitUs: number): SignificantValue {
  const integerDigits = String(us).length - (String(unitUs).length - 1);
  const decimals = Math.max(SIGNIFICANT_DIGITS - integerDigits, 0);
  const digits = Math.round((us * 10 ** decimals) / unitUs);
  return digits >= DIGITS_OVERFLOW && decimals > 0
    ? { digits: digits / 10, decimals: decimals - 1 }
    : { digits, decimals };
}

/** The text of a significant value: `2.30`, `482`, `0.812`. */
function significantText(value: SignificantValue): string {
  const text = String(value.digits).padStart(value.decimals + 1, '0');
  return value.decimals === 0
    ? text
    : `${text.slice(0, -value.decimals)}.${text.slice(-value.decimals)}`;
}

/** Minutes and seconds to one decimal, with hours from one hour:
 * `1m 0.9s`, `1h 2m 3.4s`, `145h 59m 12.3s`. */
function minutesText(us: number): string {
  const tenths = Math.round(us / US_PER_TENTH_S);
  const hours = Math.floor(tenths / TENTHS_PER_HOUR);
  const minutes = Math.floor((tenths % TENTHS_PER_HOUR) / TENTHS_PER_MIN);
  const seconds = significantText({ digits: tenths % TENTHS_PER_MIN, decimals: 1 });
  return `${hours > 0 ? `${hours}h ` : ''}${minutes}m ${seconds}s`;
}

/** The text of a positive whole number of microseconds. A value that rounds
 * to the next unit takes that unit: 999.6ms is 1.00s, 59.96s is 1m 0.0s. */
function positiveDurationText(us: number): string {
  if (us < US_PER_S) {
    const value = significantValue(us, US_PER_MS);
    if (value.digits < DIGITS_OVERFLOW) {
      return `${significantText(value)}ms`;
    }
  }
  const secondsUs = Math.max(us, US_PER_S);
  if (secondsUs < US_PER_MIN) {
    const value = significantValue(secondsUs, US_PER_S);
    if (value.digits < SECONDS_PER_MIN * 10 ** value.decimals) {
      return `${significantText(value)}s`;
    }
  }
  return minutesText(Math.max(secondsUs, US_PER_MIN));
}

/**
 * The text of a duration in milliseconds. Every duration of the app gets its
 * text here:
 * - below 1s: milliseconds with 3 significant digits and their trailing
 *   zeros: `0.812ms`, `7.05ms`, `48.2ms`, `482ms`, `5.00ms`;
 * - from 1s: seconds with 3 significant digits: `4.82s`, `11.4s`, `5.00s`;
 * - from 1min: minutes and seconds to 1 decimal: `1m 0.9s`; from 1h:
 *   `1h 2m 3.4s`.
 * A value that rounds to the next unit takes that unit, and a half rounds
 * up. The record is exact to the microsecond, so the rounding is done on
 * whole microseconds. Zero is `0ms`. A negative value (two times in the wrong
 * order) keeps its sign: `-1.50s`. A value that is not finite is a defect of
 * the caller: it throws.
 */
export function formatMilliseconds(ms: number): string {
  if (!Number.isFinite(ms)) {
    throw new Error(`duration is not a finite number of milliseconds: ${ms}`);
  }
  const us = Math.round(ms * US_PER_MS);
  if (us === 0) {
    return '0ms';
  }
  return us < 0 ? `-${positiveDurationText(-us)}` : positiveDurationText(us);
}

/** A duration in seconds, as `formatMilliseconds` writes it, or `—` for
 * null. */
export function formatDuration(seconds: number | null): string {
  return seconds === null ? '—' : formatMilliseconds(seconds * MS_PER_S);
}

/**
 * The label of a tick of a duration axis. A tick is a round time, so the
 * rule of `formatMilliseconds` does not apply to it: `0`, `0.1ms`, `100ms`,
 * `1s`, `30s`, `1m 30s`, `1h`.
 */
export function formatAxisTick(ms: number): string {
  if (ms === 0) {
    return '0';
  }
  if (ms < MS_PER_S) {
    return `${ms}ms`;
  }
  if (ms < MS_PER_MIN) {
    return `${ms / MS_PER_S}s`;
  }
  const totalSeconds = Math.round(ms / MS_PER_S);
  const hours = Math.floor(totalSeconds / SECONDS_PER_HOUR);
  const minutes = Math.floor((totalSeconds % SECONDS_PER_HOUR) / SECONDS_PER_MIN);
  const seconds = totalSeconds % SECONDS_PER_MIN;
  return [
    ...(hours > 0 ? [`${hours}h`] : []),
    ...(minutes > 0 ? [`${minutes}m`] : []),
    ...(seconds > 0 ? [`${seconds}s`] : []),
  ].join(' ');
}

/** Locale datetime, or `—` for null. Falls back to the raw string if unparseable. */
export function formatTime(iso: string | null): string {
  if (!iso) {
    return '—';
  }
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? iso : date.toLocaleString();
}

/** Signed relative time from now: `in 4m`, `2h ago`, `—` for null. */
export function formatRelative(iso: string | null): string {
  if (!iso) {
    return '—';
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return iso;
  }
  const deltaS = Math.round((date.getTime() - Date.now()) / 1000);
  const future = deltaS >= 0;
  const abs = Math.abs(deltaS);
  const magnitude =
    abs < 60
      ? `${abs}s`
      : abs < 3600
        ? `${Math.floor(abs / 60)}m`
        : abs < 86400
          ? `${Math.floor(abs / 3600)}h`
          : `${Math.floor(abs / 86400)}d`;
  return future ? `in ${magnitude}` : `${magnitude} ago`;
}

const pad = (value: number, width: number): string =>
  String(value).padStart(width, '0');

/** Local date and time with milliseconds: `2026-11-22 13:00:04.250`. Phase
 * boundaries are often less than a second apart, so the milliseconds stay. */
export function formatInstant(epochMs: number): string {
  const date = new Date(epochMs);
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1, 2)}-${pad(date.getDate(), 2)} ` +
    `${pad(date.getHours(), 2)}:${pad(date.getMinutes(), 2)}:${pad(date.getSeconds(), 2)}` +
    `.${pad(date.getMilliseconds(), 3)}`
  );
}
