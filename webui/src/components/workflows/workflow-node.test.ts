import { describe, expect, it } from 'vitest';

import { NODE_H } from '@/components/workflows/layout';
import { cardDuration, cardFacts, childRollupBadge } from '@/components/workflows/workflow-node';
import { instantMs } from '@/lib/time-text';
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

/** A node of real times: engine steps of milliseconds, 3 s of task code. */
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

describe('childRollupBadge', () => {
  it('shows the child count alone when nothing failed', () => {
    // `0/3` was read as "0 of 3 done" — the opposite of a fully successful run.
    expect(childRollupBadge(3, 0)).toEqual({
      text: '3',
      title: '3 child nodes, 0 failed',
      failed: false,
    });
  });

  it('treats a missing failure count as no failures', () => {
    expect(childRollupBadge(3, null)).toEqual({
      text: '3',
      title: '3 child nodes, 0 failed',
      failed: false,
    });
  });

  it('names the failures, and only then spends the fraction', () => {
    expect(childRollupBadge(3, 2)).toEqual({
      text: '2/3 failed',
      title: '3 child nodes, 2 failed',
      failed: true,
    });
  });

  it('marks an all-failed rollup as failing', () => {
    expect(childRollupBadge(3, 3)).toEqual({
      text: '3/3 failed',
      title: '3 child nodes, 3 failed',
      failed: true,
    });
  });

  it('has no badge for a leaf node, which has no rollup', () => {
    expect(childRollupBadge(null, null)).toBeNull();
    expect(childRollupBadge(null, 2)).toBeNull();
  });

  it('renders a childless run as a zero count, not as absent', () => {
    // A child run with no task rows yet reports total 0; the node is still a
    // subworkflow and still says so.
    expect(childRollupBadge(0, 0)).toEqual({
      text: '0',
      title: '0 child nodes, 0 failed',
      failed: false,
    });
  });

  it('says node, singular, for one child', () => {
    expect(childRollupBadge(1, 0)?.title).toBe('1 child node, 0 failed');
  });
});

describe('cardDuration', () => {
  it('shows the time of a node without its dependency wait', () => {
    // The example node: 11 s of segments, of which 1 s is dependency wait.
    expect(cardDuration(EXACT_RETRIED_NODE, 1, READ_MS)).toEqual({ kind: 'node_time', ms: 10_000 });
  });

  it('shows the execution time of a node with no phase timing', () => {
    expect(cardDuration(UNAVAILABLE_NODE, 42, READ_MS)).toEqual({ kind: 'execution', seconds: 42 });
    expect(cardDuration(CLOCK_ORDER_NODE, 7, READ_MS)).toEqual({ kind: 'execution', seconds: 7 });
  });

  it('shows nothing for a node with neither, and for a node that did not start', () => {
    expect(cardDuration(UNAVAILABLE_NODE, null, READ_MS)).toEqual({ kind: 'none' });
    expect(cardDuration(nth(CASCADE_NODES, 3).timing, null, READ_MS)).toEqual({ kind: 'none' });
  });
});

describe('cardFacts', () => {
  it('states nothing on the card of a node that ran once', () => {
    expect(cardFacts(NORMAL_NODE)).toEqual([]);
  });

  it('marks no long step: a queue wait of 7.2s is a plain fact of the timeline, not of the card', () => {
    const queuedAndRetried = exactTiming(
      [
        ['enqueued', 0, 7.2],
        ['running', 7.2, 10],
      ],
      'COMPLETED',
      10
    );
    if (queuedAndRetried.kind !== 'exact') {
      throw new Error('the node is exact');
    }
    const retried = {
      ...queuedAndRetried,
      segments: [
        ...queuedAndRetried.segments,
        {
          phase: { kind: 'running' as const },
          started_at: at(12),
          end: { kind: 'ended' as const, at: at(15) },
          source: { kind: 'attempt' as const, generation: 1, task_id: 't', attempt: 2 },
        },
      ],
    };

    expect(cardFacts(queuedAndRetried)).toEqual([]);
    expect(cardFacts(retried)).toEqual([{ tone: 'plain', text: '2 attempts' }]);
  });

  it('states the coverage of a node whose timing is not exact, a contradiction as a warning', () => {
    expect(cardFacts(UNAVAILABLE_NODE)).toEqual([{ tone: 'plain', text: 'timing unavailable' }]);
    expect(cardFacts(CLOCK_ORDER_NODE)).toEqual([{ tone: 'warning', text: 'timing inconsistent' }]);
    expect(cardFacts(PARTIAL_OPEN_NODE).map(fact => fact.text)).toEqual(['2 tasks', 'partial timing']);
  });

  it('fits three lines of text: the card is 60 px high', () => {
    expect(NODE_H).toBe(60);
  });
});
