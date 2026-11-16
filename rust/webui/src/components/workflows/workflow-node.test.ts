import { describe, expect, it } from 'vitest';

import { childRollupBadge } from '@/components/workflows/workflow-node';

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
