import { describe, expect, it, vi } from 'vitest';

import { fireEvent, render, screen } from '@testing-library/react';

import { TaskBreakdown } from '@/components/monitoring/task-breakdown';
import type { Breakdown, BreakdownCounts } from '@/types/tasks';

const COUNTS: BreakdownCounts = {
  total: 1,
  pending: 1,
  claimed: 0,
  running: 0,
  completed: 0,
  failed: 0,
  cancelled: 0,
  expired: 0,
  retried: 0,
};

const BREAKDOWN: Breakdown = {
  group_by: 'worker',
  groups: [
    { group: { kind: 'value', value: 'worker-a' }, ...COUNTS },
    { group: { kind: 'unclaimed' }, ...COUNTS },
  ],
  total: { ...COUNTS, total: 2, pending: 2 },
  group_count: 2,
};

/** The table row that holds the given group label. */
function rowOf(label: string): HTMLElement {
  const row = screen.getByText(label).closest('tr');
  if (row === null) {
    throw new Error(`no table row holds the label ${label}`);
  }
  return row;
}

describe('TaskBreakdown', () => {
  it('drills into a value group when its row is clicked', () => {
    const onDrill = vi.fn();
    render(
      <TaskBreakdown breakdown={BREAKDOWN} isLoading={false} onDrill={onDrill} />
    );

    const row = rowOf('worker-a');
    fireEvent.click(row);

    expect(onDrill).toHaveBeenCalledWith('worker-a');
    expect(row.className).toContain('cursor-pointer');
  });

  it('gives the unclaimed group no click handler', () => {
    const onDrill = vi.fn();
    render(
      <TaskBreakdown breakdown={BREAKDOWN} isLoading={false} onDrill={onDrill} />
    );

    const row = rowOf('unclaimed');
    fireEvent.click(row);

    expect(onDrill).not.toHaveBeenCalled();
    expect(row.className).not.toContain('cursor-pointer');
  });
});
