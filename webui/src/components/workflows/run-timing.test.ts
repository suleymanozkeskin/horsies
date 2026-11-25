import { describe, expect, it } from 'vitest';

import {
  activeSpan,
  chainMembership,
  dependencyWait,
  lastDependency,
  longestChain,
  longestNodeTime,
  nodeEnd,
  nodeFacts,
  nodeParts,
  nodeRunCount,
  nodeTime,
  pausedTime,
  runCountText,
} from '@/components/workflows/run-timing';
import { instantMs } from '@/lib/time-text';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import {
  at,
  CASCADE_EDGES,
  CASCADE_NODES,
  CLOCK_ORDER_NODE,
  EXACT_RETRIED_NODE,
  exactTiming,
  nth,
  OPEN_RUN_WITH_PAUSES,
  PARTIAL_OPEN_NODE,
  UNAVAILABLE_NODE,
  workflowNode,
} from '@/test/workflow-fixtures';
import type { ComposedRunTiming, RunTiming, TimingSegment } from '@/types/timing';

const ms = (seconds: number): number => instantMs(at(seconds));
const READ_MS = instantMs('2026-11-22T12:01:40Z');

describe('longestChain', () => {
  it('walks back from the node that ended last through the dependency that ended last', () => {
    const path = longestChain(CASCADE_NODES, CASCADE_EDGES, READ_MS);

    expect(path.kind).toBe('chain');
    if (path.kind !== 'chain') {
      return;
    }
    // e ends last; d and b end at one instant (the skip cascade), and b ran.
    expect(path.taskIndices).toEqual([0, 1, 4]);
    // a: 3.5 s, b: 1.5 s, e: 3 s, without dependency wait.
    expect(path.lengthMs).toBe(8_000);
    expect(path.phases.map(total => [total.key, total.ms])).toEqual([
      ['enqueued', 2_000],
      ['running', 5_500],
      ['advancing', 500],
    ]);
  });

  it('gives phases that add up to the length of the chain: the split of the run duration', () => {
    const path = longestChain(CASCADE_NODES, CASCADE_EDGES, READ_MS);

    expect(path.kind === 'chain' ? path.phases.reduce((sum, total) => sum + total.ms, 0) : 0).toBe(8_000);
    // The run of the cascade ends at 8 s: the chain covers it with no gap.
    expect(nodeEnd(nth(CASCADE_NODES, 4).timing, READ_MS)).toEqual({ kind: 'measured', endMs: ms(8) });
  });

  it('takes the lower task index when two dependencies tie in end and active time', () => {
    const twins = [
      workflowNode(0, 'left', 'COMPLETED', exactTiming([['running', 0, 2]], 'COMPLETED', 2)),
      workflowNode(1, 'right', 'COMPLETED', exactTiming([['running', 0, 2]], 'COMPLETED', 2)),
      workflowNode(2, 'join', 'COMPLETED', exactTiming([['pending', 0, 2], ['running', 2, 3]], 'COMPLETED', 3)),
    ];
    const path = longestChain(
      twins,
      [
        { from_index: 1, to_index: 2 },
        { from_index: 0, to_index: 2 },
      ],
      READ_MS
    );

    expect(path.kind === 'chain' ? path.taskIndices : []).toEqual([0, 2]);
  });

  it('stops on a cycle in a malformed edge list instead of walking forever', () => {
    const cyclic = longestChain(
      CASCADE_NODES.slice(0, 2),
      [
        { from_index: 0, to_index: 1 },
        { from_index: 1, to_index: 0 },
      ],
      READ_MS
    );

    expect(cyclic.kind === 'chain' ? cyclic.taskIndices : []).toEqual([0, 1]);
  });

  it('has no chain when no node has timing', () => {
    expect(longestChain([workflowNode(0, 'x', 'COMPLETED', UNAVAILABLE_NODE)], [], READ_MS)).toEqual({
      kind: 'no_timing',
    });
  });

  it('tells each node whether it is on the chain', () => {
    const path = longestChain(CASCADE_NODES, CASCADE_EDGES, READ_MS);

    expect(chainMembership(path, 1)).toBe('on_chain');
    expect(chainMembership(path, 2)).toBe('off_chain');
    expect(chainMembership({ kind: 'no_timing' }, 1)).toBe('off_chain');
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

const segmentsOf = (timing: typeof EXACT_RETRIED_NODE): TimingSegment[] => {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return timing.segments;
    case 'unavailable':
    case 'inconsistent':
      throw new Error('the example node has segments');
  }
};

const composed = (timing: RunTiming): ComposedRunTiming => {
  if (timing.kind !== 'composed') {
    throw new Error('the example run is composed');
  }
  return timing;
};

describe('nodeTime', () => {
  it('is the time of the node without its dependency wait', () => {
    // 12:00:01 to 12:00:11 less nothing: ten segments of 1 s to 2 s after the
    // dependency wait of 1 s.
    expect(nodeTime(EXACT_RETRIED_NODE, READ_MS)).toEqual({ kind: 'measured', ms: 10_000, open: false });
    expect(nodeTime(nth(CASCADE_NODES, 1).timing, READ_MS)).toEqual({
      kind: 'measured',
      ms: 1_500,
      open: false,
    });
  });

  it('grows with the draw time while a segment is open', () => {
    expect(nodeTime(PARTIAL_OPEN_NODE, READ_MS)).toEqual({ kind: 'measured', ms: 100_000, open: true });
    expect(nodeTime(PARTIAL_OPEN_NODE, READ_MS + 2_000)).toEqual({
      kind: 'measured',
      ms: 102_000,
      open: true,
    });
  });

  it('has no time for a node with dependency wait only, and for a node with no timing', () => {
    expect(nodeTime(nth(CASCADE_NODES, 3).timing, READ_MS)).toEqual({ kind: 'not_started' });
    expect(nodeTime(exactTiming([], 'SKIPPED', 0), READ_MS)).toEqual({ kind: 'not_started' });
    expect(nodeTime(UNAVAILABLE_NODE, READ_MS)).toEqual({ kind: 'no_timing' });
    expect(nodeTime(CLOCK_ORDER_NODE, READ_MS)).toEqual({ kind: 'no_timing' });
  });
});

describe('dependencyWait and lastDependency', () => {
  it('sums the dependency wait of a node, and has none for a node with no such segment', () => {
    expect(dependencyWait(EXACT_RETRIED_NODE, READ_MS)).toEqual({ kind: 'waited', ms: 1_000, open: false });
    expect(dependencyWait(nth(CASCADE_NODES, 0).timing, READ_MS)).toEqual({ kind: 'none' });
    expect(dependencyWait(UNAVAILABLE_NODE, READ_MS)).toEqual({ kind: 'none' });
  });

  it('says when a dependency wait has no end yet', () => {
    const waiting: TimingSegment = {
      phase: { kind: 'pending' },
      started_at: at(0),
      end: { kind: 'open' },
      source: { kind: 'node' },
    };

    expect(
      dependencyWait(
        { kind: 'exact', segments: [waiting], endpoint: { kind: 'not_terminal' } },
        READ_MS
      )
    ).toEqual({ kind: 'waited', ms: 100_000, open: true });
  });

  it('names the dependency that ended last: the node that the wait was for', () => {
    // d and b end at one instant; b ran, so the wait of e was for b.
    expect(lastDependency(CASCADE_NODES, CASCADE_EDGES, 4, READ_MS)).toEqual({
      kind: 'node',
      node: CASCADE_NODES[1],
    });
    expect(lastDependency(CASCADE_NODES, CASCADE_EDGES, 0, READ_MS)).toEqual({ kind: 'none' });
  });
});

describe('longestNodeTime', () => {
  it('names the node with the longest time without dependency wait, not the node that ended last', () => {
    // e ends last (8 s) with 3 s of its own; a took 3.5 s.
    expect(longestNodeTime(CASCADE_NODES, READ_MS)).toEqual({
      kind: 'node',
      node: CASCADE_NODES[0],
      ms: 3_500,
    });
    expect(longestNodeTime([workflowNode(0, 'x', 'COMPLETED', UNAVAILABLE_NODE)], READ_MS)).toEqual({
      kind: 'no_timing',
    });
  });
});

describe('pausedTime', () => {
  it('sums the pauses of a run; an open pause runs to the draw time', () => {
    // 1 s + 1 s + from 5 s to the read at 100 s.
    expect(pausedTime(composed(OPEN_RUN_WITH_PAUSES), READ_MS)).toEqual({
      kind: 'paused',
      ms: 97_000,
      open: true,
    });
  });

  it('is not paused for a run with no pause', () => {
    expect(pausedTime(composed(nth(TIMING_EXAMPLES.run_timings, 2)), READ_MS)).toEqual({
      kind: 'not_paused',
    });
  });
});

describe('nodeParts', () => {
  it('keeps the phases of the node itself and sums its task, with its attempt count', () => {
    expect(nodeParts(segmentsOf(EXACT_RETRIED_NODE), READ_MS)).toEqual([
      { kind: 'phase', total: { key: 'pending', ms: 1_000, longestMs: 1_000, count: 1, open: false } },
      { kind: 'phase', total: { key: 'ready', ms: 1_000, longestMs: 1_000, count: 1, open: false } },
      { kind: 'task', ms: 8_000, open: false, tasks: 1, attempts: 2 },
      { kind: 'phase', total: { key: 'advancing', ms: 1_000, longestMs: 1_000, count: 1, open: false } },
    ]);
  });

  it('counts the tasks of a node that has more than one, and says when its task is open', () => {
    expect(nodeParts(segmentsOf(PARTIAL_OPEN_NODE), READ_MS)).toEqual([
      { kind: 'task', ms: 100_000, open: true, tasks: 2, attempts: 2 },
    ]);
  });

  it('has no part for a node with no segment', () => {
    expect(nodeParts([], READ_MS)).toEqual([]);
  });
});

describe('nodeFacts and nodeRunCount', () => {
  const normal = exactTiming(
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

  it('has no fact for a node that ran once', () => {
    expect(nodeFacts(segmentsOf(normal))).toEqual([]);
    expect(runCountText(nodeRunCount(segmentsOf(normal)))).toBe('1 attempt');
  });

  it('names no long step: a queue wait of 7.2s is not a fact of its own', () => {
    const queued = exactTiming(
      [
        ['enqueued', 0, 7.2],
        ['claimed', 7.2, 7.202],
        ['running', 7.202, 10.9],
      ],
      'COMPLETED',
      10.9
    );

    expect(nodeFacts(segmentsOf(queued))).toEqual([]);
  });

  it('counts the attempts of a retried node, and the tasks of a node that has more than one', () => {
    expect(nodeFacts(segmentsOf(EXACT_RETRIED_NODE))).toEqual([{ kind: 'attempts', count: 2 }]);
    expect(nodeFacts(segmentsOf(PARTIAL_OPEN_NODE))).toEqual([{ kind: 'tasks', count: 2 }]);
    expect(runCountText(nodeRunCount(segmentsOf(EXACT_RETRIED_NODE)))).toBe('2 attempts');
    expect(runCountText(nodeRunCount(segmentsOf(PARTIAL_OPEN_NODE)))).toBe('2 tasks');
  });

  it('has no task for a node with its dependency wait alone', () => {
    expect(nodeRunCount(segmentsOf(nth(CASCADE_NODES, 3).timing))).toEqual({ kind: 'no_task' });
    expect(runCountText({ kind: 'no_task' })).toBe('no task');
  });
});
