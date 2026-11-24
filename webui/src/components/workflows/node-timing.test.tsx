import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { groupSegments, NodeTimingView } from '@/components/workflows/node-timing';
import { readClockOf } from '@/hooks/use-draw-time';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  nth,
  UNAVAILABLE_NODE,
} from '@/test/workflow-fixtures';

/** A closed read, so no clock runs. */
const CLOSED_READ = readClockOf(
  nth(TIMING_EXAMPLES.run_timings, 2),
  instantMs('2026-11-22T12:01:40Z')
);

describe('groupSegments', () => {
  it('groups the segments of a retried node by node, task and attempt, in time order', () => {
    if (EXACT_RETRIED_NODE.kind !== 'exact') {
      throw new Error('the example node is exact');
    }

    const groups = groupSegments(EXACT_RETRIED_NODE.segments);

    expect(groups.map(group => [group.title, group.segments.length])).toEqual([
      ['node', 2],
      ['task 1 · attempt 1', 5],
      ['task 1 · attempt 2', 2],
      ['node', 1],
    ]);
  });

  it('is empty for no segment', () => {
    expect(groupSegments([])).toEqual([]);
  });
});

describe('NodeTimingView', () => {
  it('shows the coverage, the wall time, the endpoint and the time of each phase', () => {
    render(<NodeTimingView timing={EXACT_RETRIED_NODE} readClock={CLOSED_READ} />);

    const section = screen.getByRole('region', { name: 'Phase timing' });

    expect(section.textContent).toContain('exact timing');
    expect(section.textContent).toContain('wall time12.0s');
    expect(section.textContent).toContain('completed at');
    expect(section.textContent).toContain('claimed4.0s');
    expect(section.textContent).toContain('task 1 · attempt 2');
  });

  it('shows only the coverage of a node with no segments', () => {
    render(<NodeTimingView timing={UNAVAILABLE_NODE} readClock={CLOSED_READ} />);

    const section = screen.getByRole('region', { name: 'Phase timing' });

    expect(section.textContent).toContain(
      'timing unavailable: The run began before the lifecycle record.'
    );
    expect(section.textContent).not.toContain('wall time');
  });

  it('names the conflict of an inconsistent node', () => {
    render(<NodeTimingView timing={CLOCK_ORDER_NODE} readClock={CLOSED_READ} />);

    expect(screen.getByRole('region', { name: 'Phase timing' }).textContent).toContain(
      'timing inconsistent: Clock order: claim'
    );
  });
});
