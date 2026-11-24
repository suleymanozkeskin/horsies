import { describe, expect, it } from 'vitest';

import {
  activeSpan,
  criticalMembership,
  criticalPath,
  dependencyWait,
  lastDependency,
  longestNodeTime,
  nodeEnd,
  nodeFacts,
  nodeParts,
  nodeTime,
  offPathLongSteps,
  pausedTime,
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
    expect(path.phases.map(total => [total.key, total.ms])).toEqual([
      ['enqueued', 2_000],
      ['running', 5_500],
      ['advancing', 500],
    ]);
  });

  it('gives phases that add up to the length of the path: the split of the run duration', () => {
    const path = criticalPath(CASCADE_NODES, CASCADE_EDGES, READ_MS);

    expect(path.kind === 'path' ? path.phases.reduce((sum, total) => sum + total.ms, 0) : 0).toBe(8_000);
    // The run of the cascade ends at 8 s: the path covers it with no gap.
    expect(nodeEnd(nth(CASCADE_NODES, 4).timing, READ_MS)).toEqual({ kind: 'measured', endMs: ms(8) });
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
      { kind: 'phase', total: { key: 'pending', ms: 1_000, longestMs: 1_000, open: false } },
      { kind: 'phase', total: { key: 'ready', ms: 1_000, longestMs: 1_000, open: false } },
      { kind: 'task', ms: 8_000, open: false, tasks: 1, attempts: 2 },
      { kind: 'phase', total: { key: 'advancing', ms: 1_000, longestMs: 1_000, open: false } },
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

describe('nodeFacts', () => {
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

  it('has no fact for a node that ran once with engine steps of milliseconds', () => {
    expect(nodeFacts(segmentsOf(normal), READ_MS)).toEqual([]);
  });

  it('names a long engine step with its time and what it tells', () => {
    const queued = exactTiming(
      [
        ['enqueued', 0, 7.2],
        ['claimed', 7.2, 7.202],
        ['running', 7.202, 10.9],
      ],
      'COMPLETED',
      10.9
    );

    const facts = nodeFacts(segmentsOf(queued), READ_MS);

    expect(facts).toHaveLength(1);
    expect(facts[0]).toMatchObject({ kind: 'long_step', key: 'enqueued', ms: 7_200 });
    expect(facts[0]?.kind === 'long_step' ? facts[0].tells : '').toContain('concurrency limit');
  });

  it('does not call a sum of short steps a long step', () => {
    // Three queue waits of 100 ms: 300 ms in all, each below the limit.
    const segments: TimingSegment[] = [1, 2, 3].map(attempt => ({
      phase: { kind: 'enqueued' },
      started_at: at(attempt * 10),
      end: { kind: 'ended', at: at(attempt * 10 + 0.1) },
      source: { kind: 'attempt', generation: 1, task_id: 't', attempt },
    }));

    expect(nodeFacts(segments, READ_MS)).toEqual([{ kind: 'attempts', count: 3 }]);
  });

  it('lists each long step in lifecycle order, then the attempt count of a retried node', () => {
    expect(
      nodeFacts(segmentsOf(EXACT_RETRIED_NODE), READ_MS).map(fact =>
        fact.kind === 'long_step' ? `${fact.key} ${fact.ms}` : `${fact.kind} ${fact.count}`
      )
    ).toEqual([
      'ready 1000',
      'enqueued 1000',
      'claimed 4000',
      'finalizing 1000',
      'advancing 1000',
      'attempts 2',
    ]);
  });

  it('counts the tasks of a node that has more than one, and marks a gap of the record', () => {
    expect(
      nodeFacts(segmentsOf(PARTIAL_OPEN_NODE), READ_MS).map(fact =>
        fact.kind === 'long_step' ? `${fact.key} ${fact.ms}` : `${fact.kind} ${fact.count}`
      )
    ).toEqual(['unknown 100000', 'tasks 2']);
  });
});

describe('offPathLongSteps', () => {
  /** A fan-out: `slow` (queue wait 7 s) and `slower` (queue wait 2 s, then a
   * long run) start after `first`; `slower` ends last, so it is on the path. */
  const nodes = [
    workflowNode(0, 'first', 'COMPLETED', exactTiming([['running', 0, 2]], 'COMPLETED', 2)),
    workflowNode(
      1,
      'slow',
      'COMPLETED',
      exactTiming([['pending', 0, 2], ['enqueued', 2, 9], ['running', 9, 10]], 'COMPLETED', 10)
    ),
    workflowNode(
      2,
      'slower',
      'COMPLETED',
      exactTiming([['pending', 0, 2], ['enqueued', 2, 4], ['running', 4, 20]], 'COMPLETED', 20)
    ),
    workflowNode(
      3,
      'quick',
      'COMPLETED',
      exactTiming([['pending', 0, 2], ['enqueued', 2, 2.003], ['running', 2.003, 5]], 'COMPLETED', 5)
    ),
    workflowNode(
      4,
      'held',
      'COMPLETED',
      exactTiming([['pending', 0, 2], ['enqueued', 2, 5], ['claimed', 5, 5.8], ['running', 5.8, 6]], 'COMPLETED', 6)
    ),
  ];
  const edges = [1, 2, 3, 4].map(to => ({ from_index: 0, to_index: to }));
  const path = criticalPath(nodes, edges, READ_MS);

  it('lists the long steps of the nodes off the path by phase, the longest node first', () => {
    expect(path.kind === 'path' ? path.taskIndices : []).toEqual([0, 2]);

    const steps = offPathLongSteps(nodes, path, READ_MS);

    expect(steps.map(step => [step.key, step.nodes.map(entry => [entry.node.node_id, entry.longestMs])])).toEqual([
      ['enqueued', [['slow', 7_000], ['held', 3_000]]],
      ['claimed', [['held', 800]]],
    ]);
    expect(steps[0]?.tells).toContain('concurrency limit');
  });

  it('leaves out the nodes of the path: their steps are in the split of the duration', () => {
    const steps = offPathLongSteps(nodes, path, READ_MS);

    expect(steps.flatMap(step => step.nodes.map(entry => entry.node.node_id))).not.toContain('slower');
  });

  it('is empty when no node off the path has a long step', () => {
    expect(offPathLongSteps(nodes.slice(0, 1).concat(nodes.slice(2, 4)), path, READ_MS)).toEqual([]);
    expect(offPathLongSteps(nodes, { kind: 'no_timing' }, READ_MS)).toHaveLength(2);
  });
});
