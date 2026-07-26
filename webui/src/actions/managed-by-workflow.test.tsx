import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  RouterProvider,
} from '@tanstack/react-router';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { TaskActionBar } from '@/actions/action-bar';
import { CapabilityProvider } from '@/actions/capability';
import type { EntityActionControls } from '@/actions/use-entity-action';
import type { TaskActionsView } from '@/actions/use-task-actions';
import { validateWorkflowSearch } from '@/routes/search';
import type { MonitoringMeta } from '@/types/meta';
import type { TaskDetail } from '@/types/tasks';

const META: MonitoringMeta = {
  horsies_version: '0.3.1',
  base_path: '/',
  actions_enabled: true,
  can_act: true,
  schema_version: 14,
  expected_schema_version: 14,
  schema_compatible: true,
  actions_disabled_reason: null,
};

const IDLE_CONTROLS: EntityActionControls = {
  state: { phase: 'idle' },
  busy: false,
  pending: null,
  boostInterval: false,
  begin: () => {},
  dismiss: () => {},
  confirm: () => {},
};

function workflowBoundTask(
  overrides: Partial<Pick<TaskDetail, 'workflow_id' | 'workflow_task_index'>>
): TaskDetail {
  return {
    leaf: {
      task_id: 't1',
      status: 'FAILED',
      error_code: null,
      failed_reason: null,
      retry_count: 0,
      max_retries: 3,
      enqueued_at: null,
      started_at: null,
      completed_at: null,
      failed_at: null,
      queue_s: null,
      exec_s: null,
      worker_hostname: null,
      good_until: null,
    },
    task_name: 'demo',
    queue_name: 'default',
    priority: 0,
    is_workflow_task: true,
    error_category: null,
    attempts: [],
    workflow_id: null,
    workflow_task_index: null,
    ...overrides,
  };
}

/** `Link` needs router context, so the bar is mounted as a route component. */
async function renderBar(detail: TaskDetail): Promise<void> {
  const view: TaskActionsView = {
    detail,
    isLoading: false,
    isError: false,
    controls: IDLE_CONTROLS,
    cancel: { shown: false },
    retry: { shown: false },
    nextAttempt: 1,
  };
  const rootRoute = createRootRoute({ component: () => <Outlet /> });
  const indexRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: '/',
    component: () => (
      <CapabilityProvider meta={META}>
        <TaskActionBar view={view} />
      </CapabilityProvider>
    ),
  });
  const workflowsRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: '/workflows',
    validateSearch: validateWorkflowSearch,
    component: () => null,
  });
  const router = createRouter({
    routeTree: rootRoute.addChildren([indexRoute, workflowsRoute]),
    history: createMemoryHistory({ initialEntries: ['/'] }),
  });
  render(<RouterProvider router={router} />);
  await screen.findByText(/Managed by workflow/);
}

describe('workflow-bound task note', () => {
  it('deep-links to the owning run and the node', async () => {
    await renderBar(
      workflowBoundTask({ workflow_id: 'abcdef1234', workflow_task_index: 4 })
    );

    const link = screen.getByRole('link');
    expect(link.getAttribute('href')).toBe('/workflows?run=abcdef1234&node=4');
    expect(link.textContent).toBe('abcdef1234'.slice(0, 8));
  });

  it('links to node 0 — task_index is zero-based, not falsy-absent', async () => {
    await renderBar(
      workflowBoundTask({ workflow_id: 'abcdef1234', workflow_task_index: 0 })
    );

    expect(screen.getByRole('link').getAttribute('href')).toBe(
      '/workflows?run=abcdef1234&node=0'
    );
  });

  it('links to the run alone when the node index is absent', async () => {
    await renderBar(
      workflowBoundTask({ workflow_id: 'abcdef1234', workflow_task_index: null })
    );

    expect(screen.getByRole('link').getAttribute('href')).toBe(
      '/workflows?run=abcdef1234'
    );
  });

  it('degrades to a plain note when the server supplies no reference', async () => {
    await renderBar(workflowBoundTask({ workflow_id: null }));

    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.getByText('Managed by workflow.')).toBeTruthy();
  });

  it('never renders task actions on a workflow-bound row', async () => {
    await renderBar(
      workflowBoundTask({ workflow_id: 'abcdef1234', workflow_task_index: 1 })
    );

    expect(screen.queryByRole('button', { name: /cancel/i })).toBeNull();
    expect(screen.queryByRole('button', { name: /retry/i })).toBeNull();
  });
});
