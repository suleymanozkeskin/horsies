import { describe, expect, it } from 'vitest';

import {
  activeSpan,
  criticalMembership,
  criticalPath,
  longestWall,
  nodeEnd,
  nodeTimeParts,
  runPhaseTotals,
} from '@/components/workflows/run-timing';
import { instantMs } from '@/lib/time-text';
import {
  at,
  CASCADE_EDGES,
  CASCADE_NODES,
  EXACT_RETRIED_NODE,
  exactTiming,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
  workflowNode,
} from '@/test/workflow-fixtures';

const ms = (seconds: number): number => instantMs(at(seconds));
const READ_MS = instantMs('2026-11-22T12:01:40Z');

describe('criticalPath', () => {
  it('walks back from the node that ended last through the dependency that ended last', () => {
    const path = criticalPath(CASCADE_NODES, CASCADE_EDGES, READ_MS);

    expect(path.kind).toBe('path');
    if (path.kind !== 'path') {
      return;
    }
    // e ends last; d and b end at one instant (the skip cascade), and b ran.
    expect(path.taskIndices).toEqual([0, 1, 4]);
    // a: 3.5 s, b: 1.5 s, e: 3 s, without dependency wait.
    expect(path.lengthMs).toBe(8_000);
    expect(path.phases.map(share => [share.key, share.ms])).toEqual([
      ['enqueued', 2_000],
      ['running', 5_500],
      ['advancing', 500],
    ]);
  });

  it('takes the lower task index when two dependencies tie in end and active time', () => {
    const twins = [
      workflowNode(0, 'left', 'COMPLETED', exactTiming([['running', 0, 2]], 'COMPLETED', 2)),
      workflowNode(1, 'right', 'COMPLETED', exactTiming([['running', 0, 2]], 'COMPLETED', 2)),
      workflowNode(2, 'join', 'COMPLETED', exactTiming([['pending', 0, 2], ['running', 2, 3]], 'COMPLETED', 3)),
    ];
    const path = criticalPath(
      twins,
      [
        { from_index: 1, to_index: 2 },
        { from_index: 0, to_index: 2 },
      ],
      READ_MS
    );

    expect(path.kind === 'path' ? path.taskIndices : []).toEqual([0, 2]);
  });

  it('stops on a cycle in a malformed edge list instead of walking forever', () => {
    const cyclic = criticalPath(
      CASCADE_NODES.slice(0, 2),
      [
        { from_index: 0, to_index: 1 },
        { from_index: 1, to_index: 0 },
      ],
      READ_MS
    );

    expect(cyclic.kind === 'path' ? cyclic.taskIndices : []).toEqual([0, 1]);
  });

  it('has no path when no node has timing', () => {
    expect(criticalPath([workflowNode(0, 'x', 'COMPLETED', UNAVAILABLE_NODE)], [], READ_MS)).toEqual({
      kind: 'no_timing',
    });
  });

  it('tells each node whether it is on the path', () => {
    const path = criticalPath(CASCADE_NODES, CASCADE_EDGES, READ_MS);

    expect(criticalMembership(path, 1)).toBe('on_path');
    expect(criticalMembership(path, 2)).toBe('off_path');
    expect(criticalMembership({ kind: 'no_timing' }, 1)).toBe('off_path');
  });
});

describe('nodeEnd and activeSpan', () => {
  it('ends a node at its latest segment end or endpoint; an open segment runs to the draw time', () => {
    expect(nodeEnd(EXACT_RETRIED_NODE, READ_MS)).toEqual({ kind: 'measured', endMs: ms(12) });
    expect(nodeEnd(PARTIAL_OPEN_NODE, READ_MS)).toEqual({ kind: 'measured', endMs: READ_MS });
    expect(nodeEnd(UNAVAILABLE_NODE, READ_MS)).toEqual({ kind: 'no_timing' });
  });

  it('starts the active span where the dependency wait ends', () => {
    expect(activeSpan(EXACT_RETRIED_NODE, READ_MS)).toEqual({ kind: 'span', startMs: ms(1), endMs: ms(12) });
    expect(activeSpan(exactTiming([['pending', 0, 5]], 'SKIPPED', 5), READ_MS)).toEqual({
      kind: 'no_active_time',
    });
  });
});

describe('runPhaseTotals', () => {
  it('sums each phase over every node with timing, in lifecycle order', () => {
    const totals = runPhaseTotals(CASCADE_NODES, READ_MS);

    expect(totals.map(share => [share.key, share.ms])).toEqual([
      ['pending', 17_000],
      ['enqueued', 2_000],
      ['running', 6_000],
      ['advancing', 500],
    ]);
    expect(totals.reduce((sum, share) => sum + share.fraction, 0)).toBeCloseTo(1, 9);
  });
});

describe('longestWall', () => {
  it('names the node with the longest wall time, the time of its timeline row', () => {
    expect(longestWall(CASCADE_NODES, READ_MS)).toEqual({ kind: 'node', node: CASCADE_NODES[4], ms: 8_000 });
    expect(longestWall([workflowNode(0, 'x', 'COMPLETED', UNAVAILABLE_NODE)], READ_MS)).toEqual({
      kind: 'no_timing',
    });
  });
});

describe('nodeTimeParts', () => {
  it('keeps the phases of the node itself and sums its task, with its attempt count', () => {
    if (EXACT_RETRIED_NODE.kind !== 'exact') {
      throw new Error('the example node is exact');
    }

    expect(nodeTimeParts(EXACT_RETRIED_NODE.segments, READ_MS)).toEqual([
      { kind: 'phase', key: 'pending', ms: 1_000 },
      { kind: 'phase', key: 'ready', ms: 1_000 },
      { kind: 'task', ms: 8_000, tasks: 1, attempts: 2 },
      { kind: 'phase', key: 'advancing', ms: 1_000 },
    ]);
  });

  it('counts the tasks of a node that has more than one', () => {
    if (PARTIAL_OPEN_NODE.kind !== 'partial') {
      throw new Error('the example node is partial');
    }

    const parts = nodeTimeParts(PARTIAL_OPEN_NODE.segments, READ_MS);

    expect(parts).toEqual([{ kind: 'task', ms: 100_000, tasks: 2, attempts: 2 }]);
  });
});
