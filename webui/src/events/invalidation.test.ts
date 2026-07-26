import { describe, expect, it } from 'vitest';

import {
  invalidationRootsFor,
  RECONNECT_SWEEP_ROOTS,
  TOPIC_INVALIDATIONS,
} from '@/events/invalidation';
import { parseMonitoringEvent } from '@/events/types';
import { QUERY_ROOT } from '@/lib/query-keys';

describe('event parsing', () => {
  it('parses a data topic with ids', () => {
    expect(parseMonitoringEvent('{"topic":"tasks","ids":["a","b"]}')).toEqual({
      topic: 'tasks',
      ids: ['a', 'b'],
    });
  });

  it('parses the cap-exceeded form as an empty id list', () => {
    expect(parseMonitoringEvent('{"topic":"workers","ids":[]}')).toEqual({
      topic: 'workers',
      ids: [],
    });
  });

  it('tolerates a missing ids field', () => {
    expect(parseMonitoringEvent('{"topic":"workflows"}')).toEqual({
      topic: 'workflows',
      ids: [],
    });
  });

  it('parses the degraded signal', () => {
    expect(parseMonitoringEvent('{"topic":"degraded"}')).toEqual({
      topic: 'degraded',
    });
  });

  it('returns null for an unknown topic so a new server cannot crash an old client', () => {
    expect(parseMonitoringEvent('{"topic":"schedules","ids":[]}')).toBeNull();
  });

  it('returns null for malformed payloads', () => {
    expect(parseMonitoringEvent('not json')).toBeNull();
    expect(parseMonitoringEvent('null')).toBeNull();
    expect(parseMonitoringEvent('[]')).toBeNull();
  });
});

describe('topic to invalidation mapping', () => {
  it('maps tasks to the list, aggregates and both open details', () => {
    expect(invalidationRootsFor({ topic: 'tasks', ids: ['t1'] })).toEqual([
      QUERY_ROOT.taskList,
      QUERY_ROOT.taskStats,
      QUERY_ROOT.taskFacets,
      QUERY_ROOT.taskBreakdown,
      QUERY_ROOT.taskDetail,
      QUERY_ROOT.workflowRun,
      QUERY_ROOT.workflowNode,
    ]);
  });

  it('maps workflows to the runs list and the open run detail', () => {
    expect(invalidationRootsFor({ topic: 'workflows', ids: [] })).toEqual([
      QUERY_ROOT.workflowRuns,
      QUERY_ROOT.workflowRun,
    ]);
  });

  it('maps workers to the grid and the focused history', () => {
    expect(invalidationRootsFor({ topic: 'workers', ids: ['w1'] })).toEqual([
      QUERY_ROOT.workers,
      QUERY_ROOT.workerHistory,
    ]);
  });

  it('invalidates nothing for degraded — it is a transport signal', () => {
    expect(invalidationRootsFor({ topic: 'degraded' })).toEqual([]);
  });

  it('does not invalidate the manual liveness ping from any topic', () => {
    for (const topic of ['tasks', 'workflows', 'workers'] as const) {
      expect(TOPIC_INVALIDATIONS[topic]).not.toContain(
        QUERY_ROOT.workerLiveness
      );
    }
  });

  it('does not invalidate schedules — that table has no trigger and always polls', () => {
    for (const topic of ['tasks', 'workflows', 'workers'] as const) {
      expect(TOPIC_INVALIDATIONS[topic]).not.toContain(QUERY_ROOT.schedules);
    }
  });
});

describe('reconnect sweep', () => {
  it('covers every root any topic can invalidate, without duplicates', () => {
    const union = new Set([
      ...TOPIC_INVALIDATIONS.tasks,
      ...TOPIC_INVALIDATIONS.workflows,
      ...TOPIC_INVALIDATIONS.workers,
    ]);
    expect(new Set(RECONNECT_SWEEP_ROOTS)).toEqual(union);
    expect(RECONNECT_SWEEP_ROOTS.length).toBe(union.size);
  });

  it('leaves the manual ping and the fetch-once name list alone', () => {
    expect(RECONNECT_SWEEP_ROOTS).not.toContain(QUERY_ROOT.workerLiveness);
    expect(RECONNECT_SWEEP_ROOTS).not.toContain(QUERY_ROOT.workflowNames);
    expect(RECONNECT_SWEEP_ROOTS).not.toContain(QUERY_ROOT.meta);
  });
});
