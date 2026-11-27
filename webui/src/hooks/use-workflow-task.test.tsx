import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useNodeErrorCode } from '@/hooks/use-workflow-task';
import type { WorkflowNodeDetail } from '@/types/workflows';

const detail = (errorCode: string | null): WorkflowNodeDetail => ({
  node_index: 4,
  node_id: 'authorize_payment',
  task_name: 'authorize_payment',
  node_status: 'FAILED',
  is_sub_workflow: false,
  node_error: null,
  task_row: {
    task_id: 't',
    status: 'FAILED',
    error_code: errorCode,
    failed_reason: null,
    retry_count: 0,
    max_retries: 0,
    enqueued_at: null,
    started_at: null,
    completed_at: null,
    failed_at: null,
    queue_seconds: null,
    execution_seconds: null,
    worker_hostname: null,
    good_until: null,
    trace_id: null,
  },
  attempts: [],
});

const stubFetch = (body: WorkflowNodeDetail): ReturnType<typeof vi.fn> => {
  const stub = vi.fn((_input: RequestInfo | URL) =>
    Promise.resolve(
      new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
    )
  );
  vi.stubGlobal('fetch', stub);
  return stub;
};

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('useNodeErrorCode', () => {
  it('reads the error code of the task of the node', async () => {
    const stub = stubFetch(detail('CARD_DECLINED'));

    const { result } = renderHook(() => useNodeErrorCode('run-1', 4, true), { wrapper });

    expect(result.current).toEqual({ kind: 'not_loaded' });
    await waitFor(() => expect(result.current).toEqual({ kind: 'code', code: 'CARD_DECLINED' }));
    expect(String(stub.mock.calls[0]?.[0])).toContain('/workflows/run-1/tasks/4');
  });

  it('says that a node has no error code', async () => {
    stubFetch(detail(null));

    const { result } = renderHook(() => useNodeErrorCode('run-1', 4, true), { wrapper });

    await waitFor(() => expect(result.current).toEqual({ kind: 'none' }));
  });

  it('reads nothing while it is not enabled', () => {
    const stub = stubFetch(detail('CARD_DECLINED'));

    const { result } = renderHook(() => useNodeErrorCode('run-1', 4, false), { wrapper });

    expect(result.current).toEqual({ kind: 'not_loaded' });
    expect(stub).not.toHaveBeenCalled();
  });
});
