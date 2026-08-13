import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { StatsBar } from '@/components/monitoring/stats-bar';
import { TASK_STATUS_ORDER } from '@/lib/status-utils';
import type { StatusCount } from '@/types/tasks';

/**
 * These assert the strip's row survives a filter change. The filters are part
 * of the stats query key, so a filter change starts a new query with nothing
 * cached under the new key; a strip that renders nothing while that request is
 * in flight removes its own row from the page and shifts everything below it
 * twice — once out, once back.
 */

const counts = (failed: number): StatusCount[] =>
  TASK_STATUS_ORDER.map(status => ({
    status,
    count: status === 'FAILED' ? failed : 0,
  }));

/** A stats endpoint whose in-flight request is resolved by the test. */
function controllableStats() {
  const pending: ((rows: StatusCount[]) => void)[] = [];
  const fetchMock = vi.fn(
    () =>
      new Promise<Response>(resolve => {
        pending.push(rows =>
          resolve(
            new Response(JSON.stringify(rows), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            })
          )
        );
      })
  );
  vi.stubGlobal('fetch', fetchMock);
  return {
    calls: (): number => fetchMock.mock.calls.length,
    /** Resolve the oldest outstanding request. */
    settle: (rows: StatusCount[]): void => {
      const resolve = pending.shift();
      if (resolve === undefined) {
        throw new Error('no request in flight');
      }
      resolve(rows);
    },
  };
}

/** A cache per test: nothing cached under a key is the state being exercised. */
const newClient = (): QueryClient =>
  new QueryClient({ defaultOptions: { queries: { retry: false } } });

const wrapperFor =
  (client: QueryClient) =>
  ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );

const cards = (): HTMLElement[] => screen.queryAllByRole('button');

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('StatsBar', () => {
  it('holds the row with one placeholder card per status before any counts arrive', () => {
    controllableStats();
    const { container } = render(<StatsBar filters={{}} onToggle={vi.fn()} />, {
      wrapper: wrapperFor(newClient()),
    });

    // Not `container.firstChild === null`: an absent strip is the layout shift.
    const strip = container.firstElementChild;
    expect(strip).not.toBeNull();
    expect(strip?.childElementCount).toBe(TASK_STATUS_ORDER.length);
    expect(container.textContent).toContain('pending');
    expect(container.textContent).toContain('expired');
    // Placeholder cards state no count and are not clickable filters.
    expect(cards()).toHaveLength(0);
  });

  it('keeps the previous scope’s cards while the new scope is in flight', async () => {
    const stats = controllableStats();
    const { rerender } = render(
      <StatsBar filters={{ task_name: ['apply_promotions'] }} onToggle={vi.fn()} />,
      { wrapper: wrapperFor(newClient()) }
    );

    stats.settle(counts(6769));
    await waitFor(() => expect(cards()).toHaveLength(TASK_STATUS_ORDER.length));

    // A second filter dimension: a new query key, nothing cached under it.
    rerender(
      <StatsBar
        filters={{ task_name: ['apply_promotions'], queue: ['billing'] }}
        onToggle={vi.fn()}
      />
    );
    await waitFor(() => expect(stats.calls()).toBe(2));

    // The request is still in flight here — the cards must not have gone.
    expect(cards()).toHaveLength(TASK_STATUS_ORDER.length);
    expect(screen.getByText('6769')).toBeDefined();

    stats.settle(counts(12));
    await waitFor(() => expect(screen.getByText('12')).toBeDefined());
    expect(cards()).toHaveLength(TASK_STATUS_ORDER.length);
  });

  it('keeps counts on screen when a later poll fails', async () => {
    const stats = controllableStats();
    const client = newClient();
    render(<StatsBar filters={{}} onToggle={vi.fn()} />, {
      wrapper: wrapperFor(client),
    });

    stats.settle(counts(6769));
    await waitFor(() => expect(screen.getByText('6769')).toBeDefined());

    const failing = vi.fn(() => Promise.reject(new Error('network down')));
    vi.stubGlobal('fetch', failing);
    await act(async () => {
      await client.invalidateQueries();
    });
    await waitFor(() => expect(failing).toHaveBeenCalled());

    // The failure is real, and the counts it would have replaced are still the
    // operator's only reading of the queue — the row keeps them.
    expect(screen.queryByRole('alert')).toBeNull();
    expect(cards()).toHaveLength(TASK_STATUS_ORDER.length);
    expect(screen.getByText('6769')).toBeDefined();
  });
});
