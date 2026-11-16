import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { WorkerHistoryChart } from '@/components/monitoring/worker-history-chart';
import type { WorkerHistoryPoint } from '@/types/workers';

/**
 * Focusing another worker changes the query key, so the new worker's history
 * starts from nothing cached. Carrying the previous worker's series over would
 * caption one worker's data with another's name, so the panel instead keeps its
 * two chart blocks and their heights while the request is in flight — otherwise
 * the whole focus card collapses and re-expands on every click.
 */

const point = (running: number): WorkerHistoryPoint => ({
  snapshot_at: '2026-08-12T10:00:00Z',
  tasks_running: running,
  tasks_claimed: 0,
  cpu_percent: 10,
  memory_percent: 20,
  memory_usage_mb: 512,
});

const jsonRows = (rows: WorkerHistoryPoint[]): Promise<Response> =>
  Promise.resolve(
    new Response(JSON.stringify(rows), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  );

const stubFetch = (rows: WorkerHistoryPoint[] | 'pending'): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() =>
      rows === 'pending' ? new Promise<Response>(() => {}) : jsonRows(rows)
    )
  );
};

/** Answers for the first worker; leaves any other worker's request in flight. */
const stubFetchPerWorker = (answered: string, rows: WorkerHistoryPoint[]): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) =>
      String(input).includes(encodeURIComponent(answered))
        ? jsonRows(rows)
        : new Promise<Response>(() => {})
    )
  );
};

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

/** The fixed-height body of each chart slot. */
const slotBodies = (container: HTMLElement): HTMLElement[] =>
  Array.from(container.querySelectorAll<HTMLElement>('[style*="height"]')).filter(
    element => element.style.height === '140px'
  );

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('WorkerHistoryChart', () => {
  it('holds both chart blocks while a worker’s history is in flight', () => {
    stubFetch('pending');
    const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
      wrapper,
    });

    expect(slotBodies(container)).toHaveLength(2);
    expect(container.textContent).toContain('Load (running / claimed)');
    expect(container.textContent).toContain('CPU % / Memory %');
    expect(container.textContent).toContain('Loading history…');
  });

  it('holds both chart blocks for a worker that has recorded nothing', async () => {
    stubFetch([]);
    const { container } = render(<WorkerHistoryChart workerId="worker-1" />, {
      wrapper,
    });

    await waitFor(() =>
      expect(container.textContent).toContain('No history recorded yet.')
    );
    expect(slotBodies(container)).toHaveLength(2);
  });

  it('holds the blocks — without the old series — when the focus moves to another worker', async () => {
    stubFetchPerWorker('worker-1', [point(3), point(1)]);
    const { container, rerender } = render(
      <WorkerHistoryChart workerId="worker-1" />,
      { wrapper }
    );

    await waitFor(() =>
      expect(container.textContent).not.toContain('Loading history…')
    );
    expect(slotBodies(container)).toHaveLength(2);

    rerender(<WorkerHistoryChart workerId="worker-2" />);

    // Worker 2's request is still in flight: the blocks stay, and they state
    // that they are loading rather than showing worker 1's series under it.
    await waitFor(() =>
      expect(container.textContent).toContain('Loading history…')
    );
    expect(slotBodies(container)).toHaveLength(2);
  });
});
