import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { NodeDetailContent } from '@/components/workflows/node-detail';
import { normalAttempt } from '@/test/attempt-fixtures';
import { TIMING_EXAMPLES } from '@/test/timing-examples';
import { CASCADE_EDGES, CASCADE_NODES, nth, workflowRun } from '@/test/workflow-fixtures';
import type { WorkflowNode, WorkflowRunDetail, WorkflowTaskDetail } from '@/types/workflows';

const RUN: WorkflowRunDetail = {
  ...workflowRun('FAILED', CASCADE_NODES, nth(TIMING_EXAMPLES.run_timings, 2)),
  edges: CASCADE_EDGES,
};

const taskDetail = (node: WorkflowNode, patch: Partial<WorkflowTaskDetail>): WorkflowTaskDetail => ({
  task_index: node.task_index,
  node_id: node.node_id,
  task_name: node.task_name,
  node_status: node.node_status,
  is_subworkflow: false,
  node_error: null,
  leaf: null,
  attempts: [],
  ...patch,
});

/** Answers the run read and the node read of the panel. */
function stubApi(detail: WorkflowTaskDetail): void {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) =>
      Promise.resolve(
        new Response(JSON.stringify(String(input).includes('/tasks/') ? detail : RUN), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      )
    )
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
  it('shows the node timing and the attempts of a node that ran', async () => {
    const node = nth(CASCADE_NODES, 0);
    stubApi(taskDetail(node, { attempts: [normalAttempt(1, 0, 'COMPLETED')] }));
    renderPanel(node);

    expect(await screen.findByRole('region', { name: 'Node timing' })).not.toBeNull();
    expect(await screen.findByRole('article', { name: 'Attempt 1' })).not.toBeNull();
  });

  it('shows a node error once: not again when an attempt has it as its message', async () => {
    const node = nth(CASCADE_NODES, 1);
    const message = 'unreachable on attempt 1';
    stubApi(taskDetail(node, { node_error: message, attempts: [normalAttempt(1, 0, 'FAILED')] }));
    renderPanel(node);

    await screen.findByRole('article', { name: 'Attempt 1' });
    expect(screen.getAllByText(message)).toHaveLength(1);
    expect(screen.queryByText('Node error')).toBeNull();
  });

  it('shows a node error that no attempt has', async () => {
    const node = nth(CASCADE_NODES, 1);
    stubApi(
      taskDetail(node, {
        node_error: 'upstream dependency failed',
        attempts: [normalAttempt(1, 0, 'FAILED')],
      })
    );
    renderPanel(node);

    expect(await screen.findByText('upstream dependency failed')).not.toBeNull();
    expect(screen.getByText('Node error')).not.toBeNull();
  });

  it('has no attempts section for a skipped node: it never has an attempt', async () => {
    const node = nth(CASCADE_NODES, 3);
    stubApi(taskDetail(node, {}));
    renderPanel(node);

    expect((await screen.findByRole('region', { name: 'Node timing' })).textContent).toContain('Did not run');
    expect(screen.queryByRole('region', { name: 'Attempts' })).toBeNull();
  });
});
