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
