import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { WorkerOverview } from '@/components/monitoring/worker-overview';
import {
  ALL_CLASSES,
  budgetLoad,
  classLoad,
  classReport,
  named,
  NOT_RECORDED,
  workerState,
} from '@/test/worker-fixtures';
import type {
  ExecutionClassReport,
  LivenessReport,
  ScheduleState,
  WorkerState,
} from '@/types/workers';

/**
 * These tests enter at the page and read what the page draws, so they hold
 * the wiring: the page reads the worker list and the class report, groups
 * the live workers by profile, and mounts the schedule table and the history
 * charts.
 */

const json = (body: unknown): Response =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });

const unavailable = (): Response =>
  new Response(JSON.stringify({ detail: 'Class report query failed' }), {
    status: 503,
    headers: { 'Content-Type': 'application/json' },
  });

interface Answers {
  workers: WorkerState[];
  classes: ExecutionClassReport | 'failed';
  schedules?: ScheduleState[];
}

/** Answers each read of the Workers page by its path. Each worker that is
 * not stale answers the ping. */
function stubApi(answers: Answers): void {
  const liveness: LivenessReport = {
    database: { kind: 'ping', latency_ms: 0.4 },
    workers: answers.workers
      .filter(worker => !worker.stale)
      .map(worker => ({
        worker_id: worker.worker_id,
        hostname: worker.hostname,
        pid: worker.pid,
        round_trip_ms: 0.5,
      })),
  };
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const path = new URL(String(input), 'http://monitoring.test').pathname;
      if (path.endsWith('/workers/classes')) {
        return Promise.resolve(
          answers.classes === 'failed' ? unavailable() : json(answers.classes)
        );
      }
      if (path.endsWith('/workers/ping')) {
        return Promise.resolve(json(liveness));
      }
      if (path.endsWith('/workers/schedules')) {
        return Promise.resolve(json(answers.schedules ?? []));
      }
      if (path.endsWith('/history')) {
        return Promise.resolve(json([]));
      }
      if (path.endsWith('/workers')) {
        return Promise.resolve(json(answers.workers));
      }
      return Promise.reject(new Error(`unexpected request: ${path}`));
    })
  );
}

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

const isolated = (inFlight: number) => [
  budgetLoad('ISOLATED', 12, ALL_CLASSES, inFlight),
];

const sharedAsync = (inFlight: number) => [
  budgetLoad('ISOLATED', 2, ['ISOLATED', 'SHARED_THREAD'], 1),
  budgetLoad('SHARED_ASYNC', 100, ['SHARED_ASYNC'], inFlight),
];

const stale = { stale: true, snapshot_age_s: 7200 };

const FLEET: Answers = {
  workers: [
    workerState('io-1', 6, { profile: named('io') }),
    workerState('batch-1', 12, { profile: named('batch') }),
    workerState('batch-2', 12, { profile: named('batch') }),
    workerState('old-batch', 12, { profile: named('batch'), ...stale }),
    workerState('older-batch', 12, { profile: NOT_RECORDED, ...stale }),
  ],
  classes: classReport([
    classLoad('batch-1', isolated(5)),
    classLoad('batch-2', isolated(12)),
    classLoad('io-1', sharedAsync(64)),
  ]),
};

const profile = (name: string): HTMLElement =>
  screen.getByRole('group', { name: `Profile ${name}` });

/** The worker cards inside an element, in document order. */
const cards = (element: HTMLElement): HTMLElement[] =>
  within(element).getAllByRole('button');

/** The host names of the worker cards inside an element, in document order. */
const cardHosts = (element: HTMLElement): string[] =>
  cards(element).map(
    card => card.querySelector('[title]')?.getAttribute('title') ?? ''
  );

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('WorkerOverview', () => {
  it('puts the live workers in the panel of their profile, each with its cards', async () => {
    stubApi(FLEET);
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(3));
    const batch = profile('batch');
    const io = profile('io');
    expect(cardHosts(batch)).toEqual(['batch-1.internal', 'batch-2.internal']);
    expect(cardHosts(io)).toEqual(['io-1.internal']);
    expect(batch.textContent).toContain('2 workers · 24 seats in service');
    expect(batch.textContent).toContain(
      '12 children per worker · ISOLATED budget'
    );
    expect(batch.textContent).toContain('17 in flight · 7 free');
    expect(io.textContent).toContain('64 in flight · 36 free');
  });

  it('puts the profiles in name order', async () => {
    stubApi(FLEET);
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(3));
    const section = screen.getByRole('region', { name: 'Workers by profile' });
    expect(
      within(section)
        .getAllByRole('group')
        .map(panel => panel.getAttribute('aria-label'))
    ).toEqual(['Profile batch', 'Profile io']);
  });

  it('does not name the profile on a card inside the panel of that profile', async () => {
    stubApi(FLEET);
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(3));
    for (const card of cards(profile('batch'))) {
      expect(card.textContent).not.toContain('profile');
    }
  });

  it('keeps a stale worker out of the profile panels, behind the stale button, with its profile on its card', async () => {
    stubApi(FLEET);
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(3));
    const section = screen.getByRole('region', { name: 'Workers by profile' });
    expect(section.textContent).not.toContain('old-batch.internal');

    fireEvent.click(screen.getByRole('button', { name: 'Show 2 stale workers' }));

    expect(section.textContent).not.toContain('old-batch.internal');
    const oldCard = screen.getByTitle('old-batch.internal').closest('button');
    const olderCard = screen.getByTitle('older-batch.internal').closest('button');
    expect(oldCard?.textContent).toContain('12 proc · profile batch');
    expect(olderCard?.textContent).toContain('12 proc · profile not recorded');
  });

  it('names the profile of the focused worker', async () => {
    stubApi(FLEET);
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(3));
    const focus = screen.getByRole('complementary');
    expect(focus.textContent).toContain('io-1.internal');
    expect(focus.textContent).toContain('profile io · pid 4000 · 6 proc');

    fireEvent.click(
      screen.getByTitle('batch-2.internal').closest('button') as HTMLElement
    );

    expect(focus.textContent).toContain('batch-2.internal');
    expect(focus.textContent).toContain('profile batch · pid 4000 · 12 proc');
  });

  it('puts the live workers with no profile name in one last panel', async () => {
    stubApi({
      workers: [
        workerState('old-build', 8, { profile: NOT_RECORDED }),
        workerState('batch-1', 12, { profile: named('batch') }),
      ],
      classes: classReport([
        classLoad('old-build', [budgetLoad('ISOLATED', 8, ALL_CLASSES, 2)]),
        classLoad('batch-1', isolated(5)),
      ]),
    });
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(2));
    const section = screen.getByRole('region', { name: 'Workers by profile' });
    expect(
      within(section)
        .getAllByRole('group')
        .map(panel => panel.getAttribute('aria-label'))
    ).toEqual(['Profile batch', 'Profile not recorded']);
    expect(cardHosts(profile('not recorded'))).toEqual(['old-build.internal']);
    expect(profile('not recorded').textContent).toContain(
      'schema migration 1004'
    );
  });

  it('keeps every live worker in its profile panel when the class report fails', async () => {
    stubApi({ ...FLEET, classes: 'failed' });
    render(<WorkerOverview />, { wrapper });

    await waitFor(() =>
      expect(
        screen.getAllByText(
          'The execution class report could not be read, so the budgets of these workers are not known.'
        )
      ).toHaveLength(2)
    );
    expect(cardHosts(profile('batch'))).toEqual([
      'batch-1.internal',
      'batch-2.internal',
    ]);
    expect(cardHosts(profile('io'))).toEqual(['io-1.internal']);
    expect(profile('batch').textContent).toContain('2 workers');
    expect(profile('batch').textContent).not.toContain('seats in service');
    expect(screen.queryAllByRole('meter')).toHaveLength(0);
  });

  it('keeps a live worker that the class report does not hold in its profile panel', async () => {
    stubApi({
      workers: [
        workerState('batch-1', 12, { profile: named('batch') }),
        workerState('late', 12, { profile: named('batch') }),
      ],
      classes: classReport([classLoad('batch-1', isolated(5))]),
    });
    render(<WorkerOverview />, { wrapper });

    await waitFor(() => expect(screen.getAllByRole('meter')).toHaveLength(1));
    expect(cardHosts(profile('batch'))).toEqual([
      'batch-1.internal',
      'late.internal',
    ]);
    expect(profile('batch').textContent).toContain(
      '1 worker has no budgets in the execution class report'
    );
  });

  it('says that no worker is live when every worker is stale', async () => {
    stubApi({
      workers: [workerState('old-batch', 12, stale)],
      classes: classReport([]),
    });
    render(<WorkerOverview />, { wrapper });

    // The stale worker does not answer the ping: a worker that answers is
    // shown with the live workers.
    await waitFor(() => expect(screen.getByText('No live worker.')).toBeTruthy());
    expect(screen.queryAllByRole('group', { name: /^Profile / })).toEqual([]);
    expect(screen.getByRole('button', { name: 'Show 1 stale worker' })).toBeTruthy();
  });

  it('mounts the schedule table with its sortable headers', async () => {
    stubApi({
      ...FLEET,
      schedules: [
        {
          schedule_name: 'rollup-uk',
          last_run_at: null,
          next_run_at: '2999-01-01T00:00:00Z',
          last_task_id: null,
          run_count: 0,
          updated_at: '2026-11-24T07:59:47Z',
        },
      ],
    });
    render(<WorkerOverview />, { wrapper });

    await waitFor(() =>
      expect(screen.getByRole('columnheader', { name: 'Next run' })).toBeTruthy()
    );
    expect(
      screen.getByRole('columnheader', { name: 'Next run' }).getAttribute('aria-sort')
    ).toBe('ascending');
    expect(screen.getByRole('button', { name: 'Runs' })).toBeTruthy();
  });

  it('mounts the history charts of the focused worker with their legends', async () => {
    stubApi(FLEET);
    render(<WorkerOverview />, { wrapper });

    await waitFor(() =>
      expect(screen.getByRole('group', { name: 'Series of Load' })).toBeTruthy()
    );
    expect(
      screen.getByRole('group', { name: 'Series of Worker process' })
    ).toBeTruthy();
    for (const label of ['running', 'claimed', 'CPU', 'memory']) {
      expect(
        screen.getByRole('button', { name: label }).getAttribute('aria-pressed')
      ).toBe('true');
    }
  });
});
