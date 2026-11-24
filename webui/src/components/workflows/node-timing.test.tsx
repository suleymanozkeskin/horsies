import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { NodeTimingView, nodeTimePartText } from '@/components/workflows/node-timing';
import { readClockOf } from '@/hooks/use-draw-time';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  nth,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
} from '@/test/workflow-fixtures';

/** A closed read, so no clock runs. */
const CLOSED_READ = readClockOf(
  nth(TIMING_EXAMPLES.run_timings, 2),
  instantMs('2026-11-22T12:01:40Z')
);

const section = (): HTMLElement => screen.getByRole('region', { name: 'Node timing' });

describe('nodeTimePartText', () => {
  it('names the phases of the node, and its task by its attempt or task count', () => {
    expect(nodeTimePartText({ kind: 'phase', key: 'pending', ms: 5_200 })).toBe('dependency wait 5.2s');
    expect(nodeTimePartText({ kind: 'task', ms: 2_400, tasks: 1, attempts: 1 })).toBe('task 2.4s');
    expect(nodeTimePartText({ kind: 'task', ms: 32_100, tasks: 1, attempts: 3 })).toBe('3 attempts 32.1s');
    expect(nodeTimePartText({ kind: 'task', ms: 4_000, tasks: 2, attempts: 2 })).toBe('2 tasks 4.0s');
  });
});

describe('NodeTimingView', () => {
  it('shows one summary: the wall time, the end, a bar of the segments and the split of the time', () => {
    render(<NodeTimingView timing={EXACT_RETRIED_NODE} readClock={CLOSED_READ} membership="off_path" />);

    const text = section().textContent ?? '';
    expect(text).toContain('12.0s');
    expect(text).toContain('completed at');
    expect(text).toContain('dependency wait 1.0s · ready 1.0s · 2 attempts 8.0s · advancing 1.0s');
    // One bar part per segment, in time order.
    const bar = screen.getByRole('img');
    expect(bar.querySelectorAll('[data-phase]')).toHaveLength(10);
    // No per-attempt list: the attempts section holds the task detail.
    expect(text).not.toContain('task 1 · attempt 1');
  });

  it('marks a node on the critical path and explains the mark', () => {
    render(<NodeTimingView timing={EXACT_RETRIED_NODE} readClock={CLOSED_READ} membership="on_path" />);

    const badge = screen.getByText('critical path');
    expect(badge.getAttribute('title')).toContain('decided the end of the run');
  });

  it('names the reasons of a partial node', () => {
    render(<NodeTimingView timing={PARTIAL_OPEN_NODE} readClock={CLOSED_READ} membership="off_path" />);

    expect(section().textContent).toContain('partial timing: history row removed by retention, worker lost');
  });

  it('shows only the coverage of a node with no segments', () => {
    render(<NodeTimingView timing={UNAVAILABLE_NODE} readClock={CLOSED_READ} membership="off_path" />);

    expect(section().textContent).toContain(
      'timing unavailable: The run began before the lifecycle record.'
    );
    expect(section().textContent).not.toContain('wall');
  });

  it('names the conflict of an inconsistent node', () => {
    render(<NodeTimingView timing={CLOCK_ORDER_NODE} readClock={CLOSED_READ} membership="off_path" />);

    expect(section().textContent).toContain('timing inconsistent: Clock order: claim');
  });
});
