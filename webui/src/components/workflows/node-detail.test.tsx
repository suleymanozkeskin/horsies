import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { NodeDetailContent } from '@/components/workflows/node-detail';
import { leafTask, normalAttempt } from '@/test/attempt-fixtures';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import { CASCADE_EDGES, CASCADE_NODES, nth, workflowRun } from '@/test/workflow-fixtures';
import type { TaskDetail } from '@/types/tasks';
import type { WorkflowNode, WorkflowRunDetail, WorkflowNodeDetail } from '@/types/workflows';

const RUN: WorkflowRunDetail = {
  ...workflowRun('FAILED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
  edges: CASCADE_EDGES,
};

const LEAF = { ...leafTask('COMPLETED', 0, 2, { enqueuedAt: null, startedAt: null }), task_id: 'task-a' };

const nodeDetail = (node: WorkflowNode, patch: Partial<WorkflowNodeDetail>): WorkflowNodeDetail => ({
  node_index: node.node_index,
  node_id: node.node_id,
  task_name: node.task_name,
  node_status: node.node_status,
  is_sub_workflow: false,
  node_error: null,
  task_row: null,
  attempts: [],
  ...patch,
});

/** The task read of the node: the same attempts, with the queue and the
 * priority that the node read does not hold. */
const taskOf = (detail: WorkflowNodeDetail): TaskDetail => ({
  task_row: detail.task_row ?? LEAF,
  task_name: detail.task_name,
  queue: 'payments',
  priority: 7,
  is_workflow_task: true,
  error_category: null,
  attempts: detail.attempts,
  workflow_id: 'run-1',
  workflow_node_index: detail.node_index,
});

/** Answers the run read, the node read and the task read of the panel. */
function stubApi(detail: WorkflowNodeDetail): void {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      const body = url.includes('/workflows/run-1/tasks/') ? detail : url.includes('/tasks/') ? taskOf(detail) : RUN;
      return Promise.resolve(
        new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
      );
    })
  );
}

function renderPanel(node: WorkflowNode): ReturnType<typeof render> {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <NodeDetailContent workflowId="run-1" runStatus="FAILED" node={node} onOpenSubworkflow={vi.fn()} />
    </QueryClientProvider>
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('NodeDetailContent', () => {
  it('shows the node timing and the attempt panel of the task of a node that ran', async () => {
    const node = nth(CASCADE_NODES, 0);
    stubApi(nodeDetail(node, { task_row: LEAF, attempts: [normalAttempt(1, 0, 'COMPLETED')] }));
    renderPanel(node);

    expect(await screen.findByRole('region', { name: 'Node timing' })).not.toBeNull();
    expect(await screen.findByRole('region', { name: 'Timing' })).not.toBeNull();
    // The attempt panel reads the task: its queue and priority.
    expect(document.querySelector('[data-task-meta]')?.textContent).toContain('queuepaymentspriority7');
  });

  it('marks a node on the longest chain on its chip line', async () => {
    const node = nth(CASCADE_NODES, 0);
    stubApi(nodeDetail(node, { task_row: LEAF, attempts: [normalAttempt(1, 0, 'COMPLETED')] }));
    renderPanel(node);

    expect(await screen.findByText('longest chain')).not.toBeNull();
    expect(document.body.textContent).not.toMatch(/critical/i);
  });

  it('shows a node error once: not again when an attempt has it as its message', async () => {
    const node = nth(CASCADE_NODES, 1);
    const message = 'unreachable on attempt 1';
    stubApi(nodeDetail(node, { node_error: message, task_row: LEAF, attempts: [normalAttempt(1, 0, 'FAILED')] }));
    renderPanel(node);

    await screen.findByRole('region', { name: 'Errors' });
    expect(screen.getAllByText(message)).toHaveLength(1);
    expect(screen.queryByText('Node error')).toBeNull();
  });

  it('shows a node error that no attempt has', async () => {
    const node = nth(CASCADE_NODES, 1);
    stubApi(
      nodeDetail(node, {
        node_error: 'upstream dependency failed',
        task_row: LEAF,
        attempts: [normalAttempt(1, 0, 'FAILED')],
      })
    );
    renderPanel(node);

    expect(await screen.findByText('upstream dependency failed')).not.toBeNull();
    expect(screen.getByText('Node error')).not.toBeNull();
  });

  it('has no attempt panel for a skipped node: it never has an attempt', async () => {
    const node = nth(CASCADE_NODES, 3);
    stubApi(nodeDetail(node, {}));
    renderPanel(node);

    expect((await screen.findByRole('region', { name: 'Node timing' })).textContent).toContain('did not run');
    expect(screen.queryByRole('region', { name: 'Timing' })).toBeNull();
  });
});
