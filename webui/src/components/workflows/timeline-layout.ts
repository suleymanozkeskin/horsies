// The layout of the run table: its columns (name, linear timeline, log
// lanes, duration), and the height of its rows from the number of lanes of
// the log cell. Pure: no React, no DOM.

import type { WorkflowNode } from '@/types/workflows';

import { TIMING_KIND_ORDER, type TimingKind } from './phase';
import { timedSegments } from './run-timing';
import { phaseComposition } from './timeline-geometry';

/** The columns of the table, in pixels; the linear cell takes the rest. */
export const NAME_COLUMN_PX = 168;
export const LANES_COLUMN_PX = 300;
export const TIME_COLUMN_PX = 62;
/** The inline padding of a row. */
export const ROW_PAD_X_PX = 20;
/** The space after the linear cell, before the log cell. */
export const LINEAR_GAP_PX = 28;
/** The margins of the lanes in the log cell. */
export const LANE_LEFT_PX = 10;
export const LANE_RIGHT_PX = 22;
/** The table scrolls sideways below this width. */
export const TABLE_MIN_WIDTH_PX = 900;
/** The height of the header of the table. */
export const HEADER_PX = 46;
/** The lanes of the log cell: one bar of LANE_BAR_PX for each kind, every
 * LANE_PITCH_PX. */
export const LANE_PITCH_PX = 6;
export const LANE_BAR_PX = 5;
/** A row is the lanes plus this space, and at least MIN_ROW_PX. */
const ROW_GAP_PX = 12;
const MIN_ROW_PX = 34;
/** The run row is this much taller than a node row. */
const RUN_ROW_EXTRA_PX = 6;

/** The grid columns of the header and of every row. */
export const TABLE_COLUMNS = `${NAME_COLUMN_PX}px minmax(0, 1fr) ${LANES_COLUMN_PX}px ${TIME_COLUMN_PX}px`;

/** The position of the linear cell over the rows: from its left and right
 * edges of the table. */
export const LINEAR_LEFT_PX = ROW_PAD_X_PX + NAME_COLUMN_PX;
export const LINEAR_RIGHT_PX = ROW_PAD_X_PX + TIME_COLUMN_PX + LANES_COLUMN_PX + LINEAR_GAP_PX;
/** The position of the lanes over the rows: from the right edge of the
 * table, and their width. */
export const LANES_RIGHT_PX = ROW_PAD_X_PX + TIME_COLUMN_PX + LANE_RIGHT_PX;
export const LANES_WIDTH_PX = LANES_COLUMN_PX - LANE_LEFT_PX - LANE_RIGHT_PX;

/** The rows of a run: the kinds of its lanes, the height of a node row and
 * the height of the run row. */
export interface TableRows {
  laneKinds: TimingKind[];
  rowPx: number;
  runPx: number;
}

/** The lane kinds of a run: every kind but dependency wait that a node of
 * the run has, in the order of the keys. */
export function laneKinds(nodes: readonly WorkflowNode[], drawAtMs: number): TimingKind[] {
  const present = new Set<TimingKind>();
  for (const node of nodes) {
    for (const total of phaseComposition(timedSegments(node.timing), drawAtMs)) {
      present.add(total.key);
    }
  }
  return TIMING_KIND_ORDER.filter(kind => kind !== 'pending' && present.has(kind));
}

export function tableRows(nodes: readonly WorkflowNode[], drawAtMs: number): TableRows {
  const kinds = laneKinds(nodes, drawAtMs);
  const rowPx = Math.max(MIN_ROW_PX, kinds.length * LANE_PITCH_PX + ROW_GAP_PX);
  return { laneKinds: kinds, rowPx, runPx: rowPx + RUN_ROW_EXTRA_PX };
}

/** The height of the lanes of `laneCount` kinds. */
export const lanesHeightPx = (laneCount: number): number => Math.max(laneCount * LANE_PITCH_PX - 1, 0);
