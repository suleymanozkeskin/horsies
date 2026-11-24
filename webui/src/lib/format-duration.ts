// Shared time/duration formatters for the monitoring surfaces.

/** Compact elapsed duration: `45s`, `12m`, `3h 20m`. `—` for null. */
export function formatElapsed(seconds: number | null): string {
  if (seconds === null) {
    return '—';
  }
  if (seconds < 60) {
    return `${seconds}s`;
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m`;
  }
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

/** Precise duration with seconds: `45s`, `12m 5s`. `—` for null. */
export function formatDuration(seconds: number | null): string {
  if (seconds === null) {
    return '—';
  }
  if (seconds < 60) {
    return `${seconds}s`;
  }
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
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

const MS_PER_SECOND = 1_000;
const MS_PER_MINUTE = 60_000;
const MS_PER_HOUR = 3_600_000;

/**
 * Duration from milliseconds, precise below a minute: `850ms`, `4.2s`,
 * `12m 5s`, `3h 20m`. A negative value (two times in the wrong order) keeps
 * its sign: `-1.5s`.
 */
export function formatMilliseconds(ms: number): string {
  const sign = ms < 0 ? '-' : '';
  const abs = Math.abs(ms);
  if (abs < MS_PER_SECOND) {
    return `${sign}${Math.round(abs)}ms`;
  }
  if (abs < MS_PER_MINUTE) {
    return `${sign}${(abs / MS_PER_SECOND).toFixed(1)}s`;
  }
  if (abs < MS_PER_HOUR) {
    const minutes = Math.floor(abs / MS_PER_MINUTE);
    const seconds = Math.floor((abs % MS_PER_MINUTE) / MS_PER_SECOND);
    return `${sign}${minutes}m ${seconds}s`;
  }
  const hours = Math.floor(abs / MS_PER_HOUR);
  const minutes = Math.floor((abs % MS_PER_HOUR) / MS_PER_MINUTE);
  return `${sign}${hours}h ${minutes}m`;
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
