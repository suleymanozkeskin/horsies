import { describe, expect, it } from 'vitest';

import { rowKeyAction } from '@/components/workflows/timeline-inspection';
import {
  rowWindow,
  scrollTopForRow,
  TIMELINE_OVERSCAN_ROWS,
  UNMEASURED_ROW_COUNT,
} from '@/components/workflows/timeline-window';

/** A row of a run with six lanes. */
const ROW = 48;
const OVERSCAN = TIMELINE_OVERSCAN_ROWS;

describe('rowWindow', () => {
  it('renders a bounded first page before the list has a height', () => {
    expect(rowWindow({ kind: 'unmeasured' }, 10_000, ROW)).toEqual({
      start: 0,
      end: UNMEASURED_ROW_COUNT,
    });
    expect(rowWindow({ kind: 'unmeasured' }, 3, ROW)).toEqual({ start: 0, end: 3 });
  });

  it('renders the rows in view and the overscan around them, not every row', () => {
    // Rows 100 to 119 are in view (20 rows of height).
    const range = rowWindow({ kind: 'measured', scrollTopPx: 100 * ROW, heightPx: 20 * ROW }, 10_000, ROW);

    expect(range).toEqual({ start: 100 - OVERSCAN, end: 120 + OVERSCAN });
    expect(range.end - range.start).toBe(20 + 2 * OVERSCAN);
  });

  it('stops at the first and the last row', () => {
    expect(
      rowWindow({ kind: 'measured', scrollTopPx: 0, heightPx: 10 * ROW }, 12, ROW)
    ).toEqual({ start: 0, end: 12 });
    expect(
      rowWindow({ kind: 'measured', scrollTopPx: 90 * ROW, heightPx: 10 * ROW }, 100, ROW)
    ).toEqual({ start: 90 - OVERSCAN, end: 100 });
  });

  it('is empty for a run with no rows', () => {
    expect(rowWindow({ kind: 'measured', scrollTopPx: 0, heightPx: 300 }, 0, ROW)).toEqual({
      start: 0,
      end: 0,
    });
  });
});

describe('scrollTopForRow', () => {
  it('keeps the offset when the row is in view', () => {
    expect(scrollTopForRow(5, 0, 10 * ROW, ROW)).toBe(0);
  });

  it('scrolls up to a row above the view', () => {
    expect(scrollTopForRow(3, 10 * ROW, 10 * ROW, ROW)).toBe(3 * ROW);
  });

  it('scrolls down until a row below the view is the last one in view', () => {
    expect(scrollTopForRow(25, 0, 10 * ROW, ROW)).toBe(26 * ROW - 10 * ROW);
  });
});

describe('rowKeyAction', () => {
  it('moves between rows and stays on the first and the last row', () => {
    expect(rowKeyAction('ArrowDown', 3, 10)).toEqual({ kind: 'focus_row', index: 4 });
    expect(rowKeyAction('ArrowUp', 3, 10)).toEqual({ kind: 'focus_row', index: 2 });
    expect(rowKeyAction('ArrowUp', 0, 10)).toEqual({ kind: 'focus_row', index: 0 });
    expect(rowKeyAction('ArrowDown', 9, 10)).toEqual({ kind: 'focus_row', index: 9 });
    expect(rowKeyAction('Home', 7, 10)).toEqual({ kind: 'focus_row', index: 0 });
    expect(rowKeyAction('End', 2, 10)).toEqual({ kind: 'focus_row', index: 9 });
  });

  it('steps through segments, hides the tooltip, and leaves other keys alone', () => {
    expect(rowKeyAction('ArrowRight', 0, 10)).toEqual({ kind: 'step_segment', step: 1 });
    expect(rowKeyAction('ArrowLeft', 0, 10)).toEqual({ kind: 'step_segment', step: -1 });
    expect(rowKeyAction('Escape', 0, 10)).toEqual({ kind: 'hide_tooltip' });
    expect(rowKeyAction('Enter', 0, 10)).toEqual({ kind: 'none' });
  });
});
