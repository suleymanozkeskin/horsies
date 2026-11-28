import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiGet, type QueryParams } from '@/lib/http';
import {
  getBreakdown,
  getFacets,
  getTaskStats,
  listTasks,
} from '@/services/tasks-api';
import { getWorkerHistory, pingWorkers } from '@/services/workers-api';
import { listWorkflowRuns } from '@/services/workflows-api';
import type { TaskFilters } from '@/types/tasks';

vi.mock('@/lib/http', () => ({ apiGet: vi.fn() }));

/**
 * The query parameters that each route of the web API knows. The server
 * refuses any other parameter with 422 (`extra_forbidden`), so a query that
 * the UI builds must use only these names. The lists are the ones of
 * `core/src/web/routes.rs`.
 */
const KNOWN_PARAMETERS = {
  '/tasks': [
    'since',
    'until',
    'status',
    'task_name',
    'queue',
    'worker',
    'error_code',
    'error_category',
    'retried_only',
    'sort_field',
    'sort_direction',
    'offset',
    'limit',
  ],
  '/tasks/status-counts': [
    'since',
    'until',
    'task_name',
    'queue',
    'worker',
    'error_code',
    'error_category',
    'retried_only',
  ],
  '/tasks/facets': [
    'since',
    'until',
    'status',
    'error_category',
    'retried_only',
  ],
  '/tasks/breakdown': [
    'since',
    'until',
    'group_by',
    'status',
    'task_name',
    'queue',
    'worker',
    'error_code',
    'error_category',
    'retried_only',
    'limit',
  ],
  '/workflows': ['workflow_name', 'status', 'limit'],
  '/workers/ping': ['timeout_seconds'],
  '/workers/worker-1/history': ['limit'],
} as const satisfies Record<string, readonly string[]>;

/** Every filter dimension set, so every parameter that a filter builds is sent. */
const FULL_FILTERS: TaskFilters = {
  status: ['FAILED'],
  task_name: ['a'],
  queue: ['b'],
  worker: ['c'],
  error_code: ['d'],
  error_category: ['TASK_ERROR'],
  retried_only: true,
};

const mockedGet = vi.mocked(apiGet);

/** The path and the query parameter names of the one `apiGet` call. */
function sentQuery(): { path: string; names: string[] } {
  expect(mockedGet).toHaveBeenCalledTimes(1);
  const [path, params] = mockedGet.mock.calls[0] as [
    string,
    QueryParams | undefined,
  ];
  return { path, names: Object.keys(params ?? {}) };
}

function expectOnlyKnownParameters(): void {
  const { path, names } = sentQuery();
  const known: readonly string[] =
    KNOWN_PARAMETERS[path as keyof typeof KNOWN_PARAMETERS];
  expect(known, `no known parameters for ${path}`).toBeDefined();
  expect(names.filter(name => !known.includes(name))).toEqual([]);
}

describe('the queries that the UI builds use only known parameters', () => {
  beforeEach(() => {
    mockedGet.mockReset();
    mockedGet.mockResolvedValue([]);
  });

  it('task list', async () => {
    await listTasks({
      filters: FULL_FILTERS,
      sortBy: 'enqueued_at',
      sortDir: 'desc',
      offset: 0,
      limit: 50,
    });
    expectOnlyKnownParameters();
  });

  it('task status counts', async () => {
    await getTaskStats(FULL_FILTERS);
    expectOnlyKnownParameters();
  });

  it('task facets', async () => {
    await getFacets({
      status: ['FAILED'],
      error_category: ['TASK_ERROR'],
      retried_only: true,
    });
    expectOnlyKnownParameters();
  });

  it('task breakdown', async () => {
    await getBreakdown('worker', FULL_FILTERS);
    expectOnlyKnownParameters();
  });

  it('workflow list', async () => {
    await listWorkflowRuns({ name: 'a', status: 'RUNNING', limit: 30 });
    expectOnlyKnownParameters();
  });

  it('worker ping', async () => {
    await pingWorkers();
    expectOnlyKnownParameters();
  });

  it('worker history', async () => {
    await getWorkerHistory('worker-1', 120);
    expectOnlyKnownParameters();
  });
});
