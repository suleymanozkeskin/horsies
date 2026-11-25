import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import {
  ChainBadge,
  nodeEndLine,
  nodeHeadline,
  nodePartsText,
  nodePartView,
  NodeTimingView,
  waitedForText,
} from '@/components/workflows/node-timing';
import { nodeParts } from '@/components/workflows/run-timing';
import { readClockOf } from '@/hooks/use-draw-time';
import { clockText } from '@/lib/clock-text';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  at,
  CASCADE_NODES,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  exactTiming,
  nth,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
} from '@/test/workflow-fixtures';

const READ_MS = instantMs('2026-11-22T12:01:40Z');

/** A closed read, so no clock runs. */
const CLOSED_READ = readClockOf(nth(TIMING_EXAMPLES.run_timings, 2), READ_MS);

const NO_DEPENDENCY = { kind: 'none' } as const;

/** A node of real times: 5 s of dependency wait, engine steps of
 * milliseconds, 3 s of task code. */
const NORMAL_NODE = exactTiming(
  [
    ['pending', 0, 5],
    ['enqueued', 5, 5.003],
    ['claimed', 5.003, 5.005],
    ['running', 5.005, 8.005],
    ['finalizing', 8.005, 8.006],
    ['advancing', 8.006, 8.007],
  ],
  'COMPLETED',
  8.007
);

const section = (): HTMLElement => screen.getByRole('region', { name: 'Node timing' });

/** The parts of the node as `label time`. */
const partRows = (): string[] =>
  [...section().querySelectorAll<HTMLElement>('[data-node-part]')].map(part => part.textContent ?? '');

describe('nodePartView and nodePartsText', () => {
  it('names the phases of the node, and its task by its attempt or task count', () => {
    const task = (tasks: number, attempts: number): string =>
      nodePartView({ kind: 'task', ms: 2_400, open: false, tasks, attempts }).label;

    expect(task(1, 1)).toBe('task');
    expect(task(1, 3)).toBe('3 attempts');
    expect(task(2, 2)).toBe('2 tasks');
    expect(
      nodePartView({ kind: 'phase', total: { key: 'pending', ms: 5_200, longestMs: 5_200, count: 1, open: false } })
    ).toMatchObject({ label: 'dependency wait', ms: 5_200, kind: { kind: 'phase', phase: 'pending' } });
  });

  it('writes the parts of a node in words, with the duration rule', () => {
    if (EXACT_RETRIED_NODE.kind !== 'exact') {
      throw new Error('the example node is exact');
    }

    expect(nodePartsText(nodeParts(EXACT_RETRIED_NODE.segments, READ_MS))).toBe(
      'dependency wait 1.00s, ready 1.00s, 2 attempts 8.00s, advancing 1.00s'
    );
  });
});

describe('nodeHeadline and nodeEndLine', () => {
  const terminal = { kind: 'terminal', status: 'SKIPPED', at: at(5) } as const;

  it('gives the time of the node without its dependency wait as a number', () => {
    expect(nodeHeadline({ kind: 'measured', ms: 3_002, open: false }, terminal)).toEqual({
      kind: 'time',
      text: '3.00s',
    });
    expect(nodeHeadline({ kind: 'measured', ms: 4_200, open: true }, { kind: 'not_terminal' })).toEqual({
      kind: 'time',
      text: '4.20s so far',
    });
  });

  it('says in short words what a node with no time of its own did', () => {
    expect(nodeHeadline({ kind: 'not_started' }, terminal)).toEqual({ kind: 'words', text: 'did not run' });
    expect(nodeHeadline({ kind: 'not_started' }, { kind: 'not_terminal' })).toEqual({
      kind: 'words',
      text: 'not started',
    });
  });

  it('gives a terminal node its end with its status, and a node that is not terminal no end line', () => {
    expect(nodeEndLine(terminal)).toMatchObject({
      kind: 'ended',
      status: 'SKIPPED',
      text: `skipped at ${clockText(instantMs(at(5)))}`,
    });
    expect(nodeEndLine({ kind: 'not_terminal' })).toEqual({ kind: 'not_ended' });
  });
});

describe('waitedForText', () => {
  const last = { kind: 'node', node: nth(CASCADE_NODES, 1) } as const;

  it('says at which dependency the wait of a node ended', () => {
    expect(waitedForText(NORMAL_NODE, READ_MS, last)).toBe('dependency wait ended with b');
  });

  it('says nothing for a node with no dependency wait, with no known dependency, or that still waits', () => {
    expect(waitedForText(nth(CASCADE_NODES, 0).timing, READ_MS, last)).toBeNull();
    expect(waitedForText(NORMAL_NODE, READ_MS, NO_DEPENDENCY)).toBeNull();
    expect(
      waitedForText(
        {
          kind: 'exact',
          segments: [
            { phase: { kind: 'pending' }, started_at: at(0), end: { kind: 'open' }, source: { kind: 'node' } },
          ],
          endpoint: { kind: 'not_terminal' },
        },
        READ_MS,
        last
      )
    ).toBeNull();
  });
});

describe('NodeTimingView', () => {
  it('shows the node time, the end, and each part of the time of the node', () => {
    render(
      <NodeTimingView timing={NORMAL_NODE} readClock={CLOSED_READ} waitedFor={{ kind: 'node', node: nth(CASCADE_NODES, 0) }} />
    );

    // 8.007 s less 5 s of dependency wait.
    expect(section().querySelector('[data-node-time]')?.textContent).toBe('3.01s');
    expect(section().textContent).toContain(`completed at ${clockText(instantMs(at(8.007)))}`);
    expect(partRows()).toEqual(['dependency wait5.00s', 'task3.01s', 'advancing1.00ms']);
    expect(section().textContent).toContain('dependency wait ended with a');
    expect(screen.queryAllByRole('note')).toHaveLength(0);
  });

  it('leaves the split of the task to the attempts, and notes no long step of the node itself', () => {
    render(<NodeTimingView timing={EXACT_RETRIED_NODE} readClock={CLOSED_READ} waitedFor={NO_DEPENDENCY} />);

    expect(partRows().map(row => row.replace(/[\d.]+m?s$/, ''))).toEqual([
      'dependency wait',
      'ready',
      '2 attempts',
      'advancing',
    ]);
    // The ready time and the advancing time of 1 s each get no note.
    expect(screen.queryAllByRole('note')).toHaveLength(0);
  });

  it('says in words that a skipped node did not run', () => {
    render(<NodeTimingView timing={nth(CASCADE_NODES, 3).timing} readClock={CLOSED_READ} waitedFor={NO_DEPENDENCY} />);

    expect(section().querySelector('[data-node-time]')?.textContent).toBe('did not run');
    expect(section().textContent).toContain('skipped at');
    expect(partRows()).toEqual(['dependency wait5.00s']);
  });

  it('names the reasons of a partial node', () => {
    render(<NodeTimingView timing={PARTIAL_OPEN_NODE} readClock={CLOSED_READ} waitedFor={NO_DEPENDENCY} />);

    expect(section().textContent).toContain('Partial timing: history row removed by retention, worker lost.');
    expect(section().querySelector('[data-node-time]')?.textContent).toBe('1m 40.0s so far');
  });

  it('shows only the coverage of a node with no segments', () => {
    render(<NodeTimingView timing={UNAVAILABLE_NODE} readClock={CLOSED_READ} waitedFor={NO_DEPENDENCY} />);

    expect(section().textContent).toContain('Timing unavailable: The run began before the lifecycle record.');
    expect(section().querySelector('[data-node-time]')).toBeNull();
  });

  it('names the conflict of an inconsistent node as a warning', () => {
    render(<NodeTimingView timing={CLOCK_ORDER_NODE} readClock={CLOSED_READ} waitedFor={NO_DEPENDENCY} />);

    const note = screen.getByRole('note');
    expect(note.textContent).toContain('Timing inconsistent: Clock order: claim');
    expect(note.getAttribute('data-tone')).toBe('warning');
  });
});

describe('ChainBadge', () => {
  it('marks a node on the longest chain and explains the mark; a node off it has none', () => {
    const { container, rerender } = render(<ChainBadge membership="on_chain" />);

    const badge = screen.getByText('longest chain');
    expect(badge.parentElement?.getAttribute('aria-description')).toContain('decided the end of the run');

    rerender(<ChainBadge membership="off_chain" />);
    expect(container.textContent).toBe('');
  });
});
