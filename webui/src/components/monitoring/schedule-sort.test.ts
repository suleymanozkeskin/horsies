import { describe, expect, it } from 'vitest';

import {
  ariaSortOf,
  DEFAULT_SCHEDULE_SORT,
  SCHEDULE_SORT_COLUMNS,
  sortAfterClick,
  sortSchedules,
  type ScheduleSort,
} from '@/components/monitoring/schedule-sort';
import type { ScheduleState } from '@/types/workers';

const schedule = (
  name: string,
  fields: Partial<Omit<ScheduleState, 'schedule_name'>> = {}
): ScheduleState => ({
  schedule_name: name,
  last_run_at: null,
  next_run_at: '2026-11-25T03:00:00Z',
  last_task_id: null,
  run_count: 0,
  updated_at: '2026-11-24T07:59:47Z',
  ...fields,
});

const names = (rows: ScheduleState[]): string[] =>
  rows.map(row => row.schedule_name);

/** Four schedules as the demo holds them: two that ran, two that never ran. */
const hourly = schedule('rollup-uk', {
  last_run_at: '2026-11-24T08:15:00Z',
  next_run_at: '2026-11-24T09:15:00Z',
  run_count: 1,
});
const frequent = schedule('supplier-feed-atlas', {
  last_run_at: '2026-11-24T09:10:17Z',
  next_run_at: '2026-11-24T09:11:47Z',
  run_count: 47,
});
const daily = schedule('sales-rollup-daily', {
  next_run_at: '2026-11-25T03:00:00Z',
});
const monthly = schedule('monthly-close', {
  next_run_at: '2026-12-01T05:00:00Z',
});
const rows = [monthly, hourly, daily, frequent];

describe('sortSchedules', () => {
  it('orders by the soonest next run by default, as the API does', () => {
    expect(names(sortSchedules(rows, DEFAULT_SCHEDULE_SORT))).toEqual([
      'supplier-feed-atlas',
      'rollup-uk',
      'sales-rollup-daily',
      'monthly-close',
    ]);
  });

  it('orders by the latest next run first when descending', () => {
    const sort: ScheduleSort = { column: 'next_run_at', direction: 'descending' };

    expect(names(sortSchedules(rows, sort))).toEqual([
      'monthly-close',
      'sales-rollup-daily',
      'rollup-uk',
      'supplier-feed-atlas',
    ]);
  });

  it('orders by name in number order, not in character order', () => {
    const feeds = [schedule('feed-10'), schedule('feed-2'), schedule('feed-1')];

    expect(
      names(
        sortSchedules(feeds, { column: 'schedule_name', direction: 'ascending' })
      )
    ).toEqual(['feed-1', 'feed-2', 'feed-10']);
    expect(
      names(
        sortSchedules(feeds, { column: 'schedule_name', direction: 'descending' })
      )
    ).toEqual(['feed-10', 'feed-2', 'feed-1']);
  });

  it('orders by run count, and equal counts by name', () => {
    const sort: ScheduleSort = { column: 'run_count', direction: 'descending' };

    expect(names(sortSchedules(rows, sort))).toEqual([
      'supplier-feed-atlas',
      'rollup-uk',
      'monthly-close',
      'sales-rollup-daily',
    ]);
  });

  it('keeps equal rows in ascending name order in the two directions', () => {
    const equal = [schedule('b'), schedule('c'), schedule('a')];

    for (const direction of ['ascending', 'descending'] as const) {
      expect(
        names(sortSchedules(equal, { column: 'run_count', direction }))
      ).toEqual(['a', 'b', 'c']);
    }
  });

  it('puts a schedule that never ran last in the two directions', () => {
    const oldestFirst = sortSchedules(rows, {
      column: 'last_run_at',
      direction: 'ascending',
    });
    const newestFirst = sortSchedules(rows, {
      column: 'last_run_at',
      direction: 'descending',
    });

    expect(names(oldestFirst)).toEqual([
      'rollup-uk',
      'supplier-feed-atlas',
      'monthly-close',
      'sales-rollup-daily',
    ]);
    expect(names(newestFirst)).toEqual([
      'supplier-feed-atlas',
      'rollup-uk',
      'monthly-close',
      'sales-rollup-daily',
    ]);
  });

  it('puts a row with no next run, or with text that is not a time, last', () => {
    const withoutNext = schedule('no-next', { next_run_at: null });
    const unreadable = schedule('bad-next', { next_run_at: 'not a time' });

    for (const direction of ['ascending', 'descending'] as const) {
      const sorted = sortSchedules([withoutNext, hourly, unreadable], {
        column: 'next_run_at',
        direction,
      });

      expect(names(sorted)).toEqual(['rollup-uk', 'bad-next', 'no-next']);
    }
  });

  it('returns a new array and leaves the input in its order', () => {
    const input = [...rows];

    const sorted = sortSchedules(input, DEFAULT_SCHEDULE_SORT);

    expect(sorted).not.toBe(input);
    expect(names(input)).toEqual(names(rows));
  });

  it('returns an empty list for no schedules', () => {
    expect(sortSchedules([], DEFAULT_SCHEDULE_SORT)).toEqual([]);
  });
});

describe('sortAfterClick', () => {
  it('starts another column in ascending order', () => {
    expect(sortAfterClick(DEFAULT_SCHEDULE_SORT, 'run_count')).toEqual({
      column: 'run_count',
      direction: 'ascending',
    });
  });

  it('changes the direction of the active column, and changes it back', () => {
    const descending = sortAfterClick(DEFAULT_SCHEDULE_SORT, 'next_run_at');

    expect(descending).toEqual({
      column: 'next_run_at',
      direction: 'descending',
    });
    expect(sortAfterClick(descending, 'next_run_at')).toEqual(
      DEFAULT_SCHEDULE_SORT
    );
  });
});

describe('ariaSortOf', () => {
  it('gives the direction to the active column and none to each other column', () => {
    const sort: ScheduleSort = { column: 'run_count', direction: 'descending' };

    expect(ariaSortOf(sort, 'run_count')).toBe('descending');
    for (const column of SCHEDULE_SORT_COLUMNS) {
      if (column !== 'run_count') {
        expect(ariaSortOf(sort, column)).toBe('none');
      }
    }
  });
});
