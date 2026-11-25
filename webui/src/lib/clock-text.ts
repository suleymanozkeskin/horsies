// One time format for the timing views: the local date once, then times of
// day with milliseconds, and the name of the local time zone. Phase
// boundaries are often less than a second apart, so the milliseconds stay.

const MS_PER_DAY = 86_400_000;

const pad = (value: number, width: number): string => String(value).padStart(width, '0');

/** Local date: `2026-11-24`. */
export function dateText(epochMs: number): string {
  const date = new Date(epochMs);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1, 2)}-${pad(date.getDate(), 2)}`;
}

/** Local time of day with milliseconds: `09:14:26.244`. */
export function clockText(epochMs: number): string {
  const date = new Date(epochMs);
  return (
    `${pad(date.getHours(), 2)}:${pad(date.getMinutes(), 2)}:${pad(date.getSeconds(), 2)}` +
    `.${pad(date.getMilliseconds(), 3)}`
  );
}

/** Short name of the local time zone at `epochMs` (`GMT+1`, `UTC`, `PST`),
 * or `local time` when the platform names none. */
export function zoneText(epochMs: number): string {
  const parts = new Intl.DateTimeFormat('en-US', { timeZoneName: 'short' }).formatToParts(
    new Date(epochMs)
  );
  return parts.find(part => part.type === 'timeZoneName')?.value ?? 'local time';
}

/** The number of local calendar days from the day of `baseMs` to the day of
 * `epochMs`: 0 on the same day, negative for an earlier day. */
export function dayOffset(baseMs: number, epochMs: number): number {
  const base = new Date(baseMs);
  const other = new Date(epochMs);
  const baseDay = Date.UTC(base.getFullYear(), base.getMonth(), base.getDate());
  const otherDay = Date.UTC(other.getFullYear(), other.getMonth(), other.getDate());
  return Math.round((otherDay - baseDay) / MS_PER_DAY);
}

/** Local time of day to the second: `09:14:26`. */
export function clockSecondText(epochMs: number): string {
  const date = new Date(epochMs);
  return `${pad(date.getHours(), 2)}:${pad(date.getMinutes(), 2)}:${pad(date.getSeconds(), 2)}`;
}

/** ` +1d` (or ` -1d`) when `epochMs` falls on another local day than
 * `baseMs`, else nothing. */
function dayOffsetSuffix(baseMs: number, epochMs: number): string {
  const offset = dayOffset(baseMs, epochMs);
  return offset === 0 ? '' : ` ${offset > 0 ? '+' : ''}${offset}d`;
}

/** The time of day of `epochMs` for a view whose date line shows the day of
 * `baseMs`: `09:14:26.244`, with `+1d` (or `-1d`) when the time falls on
 * another local day. */
export function clockTextFrom(baseMs: number, epochMs: number): string {
  return `${clockText(epochMs)}${dayOffsetSuffix(baseMs, epochMs)}`;
}

/** `clockSecondText` for a view whose date line shows the day of `baseMs`:
 * `09:14:26`, with the day offset of a time on another local day. */
export function clockSecondTextFrom(baseMs: number, epochMs: number): string {
  return `${clockSecondText(epochMs)}${dayOffsetSuffix(baseMs, epochMs)}`;
}

/** The date line of a timing view: `2026-11-24 · times in GMT+1`. */
export function dateLineText(epochMs: number): string {
  return `${dateText(epochMs)} · times in ${zoneText(epochMs)}`;
}
