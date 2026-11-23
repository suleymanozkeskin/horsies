// Row windowing of the timeline. Every row has one fixed height, so the rows
// in view follow from the scroll offset alone: the list renders those rows
// and a few more, never one row per node of a run (a run holds up to 10,000).

import { assertNever } from '@/lib/exhaustive';

/** Height of one timeline row, in pixels. */
export const TIMELINE_ROW_HEIGHT_PX = 28;

/** Rows rendered above and below the rows in view, so a fast scroll does not
 * show an empty band. */
export const TIMELINE_OVERSCAN_ROWS = 8;

/** Rows rendered before the list has a measured height (the first paint, or
 * a container with no layout). */
export const UNMEASURED_ROW_COUNT = 40;

/** The scroll state of the list. */
export type ListViewport =
  | { kind: 'unmeasured' }
  | { kind: 'measured'; scrollTopPx: number; heightPx: number };

/** The rows to render: indexes `start` (inclusive) to `end` (exclusive). */
export interface RowWindow {
  start: number;
  end: number;
}

export function rowWindow(viewport: ListViewport, rowCount: number): RowWindow {
  switch (viewport.kind) {
    case 'unmeasured':
      return { start: 0, end: Math.min(rowCount, UNMEASURED_ROW_COUNT) };
    case 'measured': {
      const firstInView = Math.floor(viewport.scrollTopPx / TIMELINE_ROW_HEIGHT_PX);
      const lastInView = Math.ceil(
        (viewport.scrollTopPx + viewport.heightPx) / TIMELINE_ROW_HEIGHT_PX
      );
      const start = Math.min(Math.max(firstInView - TIMELINE_OVERSCAN_ROWS, 0), rowCount);
      const end = Math.min(Math.max(lastInView + TIMELINE_OVERSCAN_ROWS, start), rowCount);
      return { start, end };
    }
    default:
      return assertNever(viewport, 'list viewport');
  }
}

/**
 * The scroll offset that brings row `index` into a viewport of `heightPx`,
 * or the current offset when the row is in view already.
 */
export function scrollTopForRow(index: number, scrollTopPx: number, heightPx: number): number {
  const rowTop = index * TIMELINE_ROW_HEIGHT_PX;
  const rowBottom = rowTop + TIMELINE_ROW_HEIGHT_PX;
  if (rowTop < scrollTopPx) {
    return rowTop;
  }
  if (rowBottom > scrollTopPx + heightPx) {
    return rowBottom - heightPx;
  }
  return scrollTopPx;
}
