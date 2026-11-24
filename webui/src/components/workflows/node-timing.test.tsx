import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import {
  nodeEndLine,
  nodeHeadline,
  nodePartFigure,
  nodePartNotes,
  nodePartsText,
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

const figureRows = (): string[] =>
  [...section().querySelectorAll<HTMLElement>('[data-figure]')].map(
    figure =>
      `${figure.querySelector('span span')?.textContent} ${figure.querySelector('span span:last-child')?.textContent} ${figure.getAttribute('data-weight')}`
  );

describe('nodePartFigure and nodePartsText', () => {
  it('names the phases of the node, and its task by its attempt or task count', () => {
    const task = (tasks: number, attempts: number): string =>
      nodePartFigure({ kind: 'task', ms: 2_400, open: false, tasks, attempts }).label;

    expect(task(1, 1)).toBe('task');
    expect(task(1, 3)).toBe('3 attempts');
    expect(task(2, 2)).toBe('2 tasks');
    expect(
      nodePartFigure({ kind: 'phase', total: { key: 'pending', ms: 5_200, longestMs: 5_200, open: false } })
    ).toMatchObject({ label: 'dependency wait', ms: 5_200, weight: { kind: 'plain' } });
  });

  it('makes the task the main time of the node', () => {
    expect(nodePartFigure({ kind: 'task', ms: 2_400, open: false, tasks: 1, attempts: 1 }).weight).toEqual({
      kind: 'main',
    });
  });

  it('writes the parts of a node in words', () => {
    if (EXACT_RETRIED_NODE.kind !== 'exact') {
      throw new Error('the example node is exact');
    }

    expect(nodePartsText(nodeParts(EXACT_RETRIED_NODE.segments, READ_MS))).toBe(
      'dependency wait 1.0s, ready 1.0s, 2 attempts 8.0s, advancing 1.0s'
    );
  });
});

describe('nodeHeadline and nodeEndLine', () => {
  const terminal = { kind: 'terminal', status: 'SKIPPED', at: at(5) } as const;

  it('gives the time of the node without its dependency wait as a number', () => {
    expect(nodeHeadline({ kind: 'measured', ms: 3_002, open: false }, terminal)).toEqual({
      kind: 'time',
      text: '3.0s',
    });
    expect(nodeHeadline({ kind: 'measured', ms: 4_200, open: true }, { kind: 'not_terminal' })).toEqual({
      kind: 'time',
      text: '4.2s so far',
    });
  });

  it('says in words what a node with no time of its own did', () => {
    expect(nodeHeadline({ kind: 'not_started' }, terminal)).toEqual({ kind: 'words', text: 'Did not run' });
    expect(nodeHeadline({ kind: 'not_started' }, { kind: 'not_terminal' })).toEqual({
      kind: 'words',
      text: 'Not started',
    });
  });

  it('gives a terminal node its end, and a node that is not terminal no end line', () => {
    expect(nodeEndLine(terminal)).toMatchObject({
      kind: 'ended',
      text: `skipped at ${clockText(instantMs(at(5)))}`,
    });
    expect(nodeEndLine({ kind: 'not_terminal' })).toEqual({ kind: 'not_ended' });
  });
});

describe('waitedForText', () => {
  const last = { kind: 'node', node: nth(CASCADE_NODES, 1) } as const;

  it('says at which dependency the wait of a node ended', () => {
    expect(waitedForText(NORMAL_NODE, READ_MS, last)).toBe('The dependency wait ended when b ended.');
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

describe('nodePartNotes', () => {
  it('has no note for a node whose own engine steps are short', () => {
    if (NORMAL_NODE.kind !== 'exact') {
      throw new Error('the node is exact');
    }

    expect(nodePartNotes(nodeParts(NORMAL_NODE.segments, READ_MS))).toEqual([]);
  });

  it('notes a long step of the node itself with what it tells, and leaves the task to its attempts', () => {
    const lateAdvance = exactTiming(
      [
        ['enqueued', 0, 7.2],
        ['running', 7.2, 10],
        ['advancing', 10, 41],
      ],
      'COMPLETED',
      41
    );
    if (lateAdvance.kind !== 'exact') {
      throw new Error('the node is exact');
    }

    const notes = nodePartNotes(nodeParts(lateAdvance.segments, READ_MS));

    expect(notes).toHaveLength(1);
    expect(notes[0]?.tone).toBe('warning');
    expect(notes[0]?.text).toMatch(/^Advancing 31\.0s\. .*recovery pass/);
  });
});

describe('NodeTimingView', () => {
  it('shows the node time, the end, and the wall time of the node as named numbers', () => {
    render(
      <NodeTimingView
        timing={NORMAL_NODE}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={{ kind: 'node', node: nth(CASCADE_NODES, 0) }}
      />
    );

    // 8.007 s less 5 s of dependency wait.
    expect(section().querySelector('[data-node-time]')?.textContent).toBe('3.0s');
    expect(section().textContent).toContain(`completed at ${clockText(instantMs(at(8.007)))}`);
    expect(figureRows()).toEqual(['dependency wait 5.0s plain', 'task 3.0s main', 'advancing 1ms quiet']);
    expect(section().textContent).toContain('The dependency wait ended when a ended.');
    expect(screen.queryAllByRole('note')).toHaveLength(0);
  });

  it('draws no bar, and leaves the split of the task to the attempts', () => {
    render(
      <NodeTimingView
        timing={EXACT_RETRIED_NODE}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    expect(screen.queryByRole('img')).toBeNull();
    expect(section().querySelector('[data-phase]')).toBeNull();
    expect(figureRows().map(row => row.split(' ').slice(0, -2).join(' '))).toEqual([
      'dependency wait',
      'ready',
      '2 attempts',
      'advancing',
    ]);
  });

  it('notes the long steps of the node itself', () => {
    render(
      <NodeTimingView
        timing={EXACT_RETRIED_NODE}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    // The example has a ready time and an advancing time of 1 s each.
    expect(screen.getAllByRole('note').map(note => note.textContent?.split('.')[0])).toEqual([
      'Ready 1',
      'Advancing 1',
    ]);
  });

  it('says in words that a skipped node did not run', () => {
    render(
      <NodeTimingView
        timing={nth(CASCADE_NODES, 3).timing}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    expect(section().querySelector('[data-node-time]')?.textContent).toBe('Did not run');
    expect(section().textContent).toContain('skipped at');
    expect(figureRows()).toEqual(['dependency wait 5.0s plain']);
  });

  it('marks a node on the critical path and explains the mark', () => {
    render(
      <NodeTimingView
        timing={EXACT_RETRIED_NODE}
        readClock={CLOSED_READ}
        membership="on_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    const badge = screen.getByText('critical path');
    expect(badge.parentElement?.getAttribute('aria-description')).toContain(
      'decided the end of the run'
    );
  });

  it('names the reasons of a partial node', () => {
    render(
      <NodeTimingView
        timing={PARTIAL_OPEN_NODE}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    expect(section().textContent).toContain('Partial timing: history row removed by retention, worker lost.');
    expect(section().querySelector('[data-node-time]')?.textContent).toBe('1m 40s so far');
  });

  it('shows only the coverage of a node with no segments', () => {
    render(
      <NodeTimingView
        timing={UNAVAILABLE_NODE}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    expect(section().textContent).toContain(
      'Timing unavailable: The run began before the lifecycle record.'
    );
    expect(section().querySelector('[data-node-time]')).toBeNull();
  });

  it('names the conflict of an inconsistent node as a warning', () => {
    render(
      <NodeTimingView
        timing={CLOCK_ORDER_NODE}
        readClock={CLOSED_READ}
        membership="off_path"
        waitedFor={NO_DEPENDENCY}
      />
    );

    const note = screen.getByRole('note');
    expect(note.textContent).toContain('Timing inconsistent: Clock order: claim');
    expect(note.getAttribute('data-tone')).toBe('warning');
  });
});
