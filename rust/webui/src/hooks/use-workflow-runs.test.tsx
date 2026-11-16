import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { LiveProvider } from '@/events/live-provider';
import { useWorkflowRuns } from '@/hooks/use-workflow-runs';
import type { WorkflowRunSummary } from '@/types/workflows';

/**
 * The name and status filters are part of the run query's key, so changing one
 * starts a query with nothing cached under it. The rail renders "Loading runs…"
 * whenever `runs` is empty, so a filter change that emptied it would blank the
 * list and drop its scroll position on every keystroke of the picker.
 */

const run = (id: string): WorkflowRunSummary => ({
  id,
  name: 'nightly_rollup',
  definition_key: 'nightly_rollup:v1',
  status: 'COMPLETED',
  created_at: '2026-08-12T10:00:00Z',
  completed_at: '2026-08-12T10:04:00Z',
  wall_s: 240,
});

const stubFetch = (byQuery: (url: string) => WorkflowRunSummary[] | 'pending'): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const answer = byQuery(String(input));
      return answer === 'pending'
        ? new Promise<Response>(() => {})
        : Promise.resolve(
            new Response(JSON.stringify(answer), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            })
          );
    })
  );
};

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return (
    <QueryClientProvider client={client}>
      <LiveProvider>{children}</LiveProvider>
    </QueryClientProvider>
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('useWorkflowRuns', () => {
  it('keeps the previous filter’s runs while the new filter is in flight', async () => {
    // Only the unfiltered request is answered; adding a status leaves the new
    // query outstanding, which is the window the rail used to render empty.
    stubFetch(url => (url.includes('status=') ? 'pending' : [run('run-1')]));

    const { result, rerender } = renderHook(
      ({ status }: { status: string | null }) =>
        useWorkflowRuns(null, status),
      { wrapper, initialProps: { status: null as string | null } }
    );

    await waitFor(() => expect(result.current.runs).toHaveLength(1));

    rerender({ status: 'FAILED' });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.runs).toHaveLength(1);
    expect(result.current.runs[0]?.id).toBe('run-1');
  });
});
