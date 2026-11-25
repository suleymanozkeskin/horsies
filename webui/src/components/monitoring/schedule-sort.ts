// The order of the schedule table. Pure: no I/O, no clock.

import { assertNever } from '@/lib/exhaustive';
import type { ScheduleState } from '@/types/workers';

/** The columns of the schedule table. Each one has an order. */
export const SCHEDULE_SORT_COLUMNS = [
  'schedule_name',
  'last_run_at',
  'next_run_at',
  'run_count',
] as const;

export type ScheduleSortColumn = (typeof SCHEDULE_SORT_COLUMNS)[number];

/** The words of `aria-sort`, so the state goes to the header as it is. */
export type SortDirection = 'ascending' | 'descending';

export interface ScheduleSort {
  readonly column: ScheduleSortColumn;
  readonly direction: SortDirection;
}

/** The order of `GET /api/workers/schedules`: soonest next run first, then
 * by name. */
export const DEFAULT_SCHEDULE_SORT: ScheduleSort = {
  column: 'next_run_at',
  direction: 'ascending',
};

/** The instant that a time column orders a row by. */
type SortInstant =
  | { readonly kind: 'at'; readonly epochMs: number }
  /** No instant: the schedule never ran, or the text is not a time. */
  | { readonly kind: 'none' };

function sortInstant(iso: string | null): SortInstant {
  if (iso === null) {
    return { kind: 'none' };
  }
  const epochMs = Date.parse(iso);
  return Number.isNaN(epochMs) ? { kind: 'none' } : { kind: 'at', epochMs };
}

/** Names with numbers in number order (`feed-2` before `feed-10`), with a
 * fixed locale so the order does not depend on the browser. */
const nameOrder = new Intl.Collator('en', { numeric: true });

const signed = (ascending: number, direction: SortDirection): number =>
  direction === 'ascending' ? ascending : -ascending;

/** Two instants in `direction`. A row with no instant goes last in the two
 * directions, so the rows with a time stay on top. */
function compareInstants(
  left: SortInstant,
  right: SortInstant,
  direction: SortDirection
): number {
  if (left.kind === 'none' || right.kind === 'none') {
    return Number(left.kind === 'none') - Number(right.kind === 'none');
  }
  return signed(left.epochMs - right.epochMs, direction);
}

function compareColumn(
  left: ScheduleState,
  right: ScheduleState,
  sort: ScheduleSort
): number {
  switch (sort.column) {
    case 'schedule_name':
      return signed(
        nameOrder.compare(left.schedule_name, right.schedule_name),
        sort.direction
      );
    case 'last_run_at':
      return compareInstants(
        sortInstant(left.last_run_at),
        sortInstant(right.last_run_at),
        sort.direction
      );
    case 'next_run_at':
      return compareInstants(
        sortInstant(left.next_run_at),
        sortInstant(right.next_run_at),
        sort.direction
      );
    case 'run_count':
      return signed(left.run_count - right.run_count, sort.direction);
    default:
      return assertNever(sort.column, 'schedule sort column');
  }
}

/**
 * The schedules in the order of `sort`. Rows that are equal in the sort
 * column are in name order, ascending in the two directions, so the order
 * does not change between two reads. Returns a new array.
 */
export function sortSchedules(
  schedules: readonly ScheduleState[],
  sort: ScheduleSort
): ScheduleState[] {
  return [...schedules].sort(
    (left, right) =>
      compareColumn(left, right, sort) ||
      nameOrder.compare(left.schedule_name, right.schedule_name)
  );
}

/**
 * The sort after a click on the header of `column`: the active column
 * changes its direction, and another column starts ascending.
 */
export function sortAfterClick(
  current: ScheduleSort,
  column: ScheduleSortColumn
): ScheduleSort {
  if (current.column !== column) {
    return { column, direction: 'ascending' };
  }
  return {
    column,
    direction: current.direction === 'ascending' ? 'descending' : 'ascending',
  };
}

/** The `aria-sort` value of the header of `column`. */
export function ariaSortOf(
  sort: ScheduleSort,
  column: ScheduleSortColumn
): SortDirection | 'none' {
  return sort.column === column ? sort.direction : 'none';
}
