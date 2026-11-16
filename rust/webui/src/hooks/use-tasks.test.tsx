import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useFacets } from '@/hooks/use-tasks';
import type { Facets } from '@/types/tasks';

/**
 * The scope is part of the facets query key, so engaging a status or category
 * filter starts a query with nothing cached under it. The taxonomy strip
 * renders nothing when its totals are empty, and the filter comboboxes list
 * nothing when their options are — so a scope change that emptied the facets
 * would take the strip's whole row off the page until the request landed.
 */

const FACETS: Facets = {
  workers: [{ value: 'box-1', count: 12 }],
  task_names: [{ value: 'apply_promotions', count: 12 }],
  queues: [{ value: 'billing', count: 12 }],
  error_codes: [{ value: 'TIMEOUT', count: 3, category: 'OPERATIONAL' }],
  error_category_totals: { OPERATIONAL: 3 },
};

/** Answers the unscoped request; leaves a scoped one in flight. */
const stubFetch = (): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) =>
      String(input).includes('status=')
        ? new Promise<Response>(() => {})
        : Promise.resolve(
            new Response(JSON.stringify(FACETS), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            })
          )
    )
  );
};

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('useFacets', () => {
  it('keeps the previous scope’s facets while the new scope is in flight', async () => {
    stubFetch();

    const { result, rerender } = renderHook(
      ({ status }: { status: string[] }) => useFacets({ status }),
      { wrapper, initialProps: { status: [] as string[] } }
    );

    await waitFor(() => expect(result.current.facets).toBeDefined());

    rerender({ status: ['FAILED'] });

    // The scoped request is still outstanding here.
    expect(result.current.facets?.error_category_totals).toEqual({
      OPERATIONAL: 3,
    });
    expect(result.current.facets?.task_names).toHaveLength(1);
  });
});
