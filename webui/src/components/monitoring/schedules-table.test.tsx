import type { ReactNode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  schedulePanelState,
  SchedulesTable,
} from '@/components/monitoring/schedules-table';
import { queryKeys } from '@/lib/query-keys';
import type { ScheduleState } from '@/types/workers';

const schedule = (
  name: string,
  fields: Partial<Omit<ScheduleState, 'schedule_name'>> = {}
): ScheduleState => ({
  schedule_name: name,
  last_run_at: null,
  next_run_at: '2999-01-01T00:00:00Z',
  last_task_id: null,
  run_count: 0,
  updated_at: '2026-11-24T07:59:47Z',
  ...fields,
});

/** In the order of the API: soonest next run first. */
const SCHEDULES: ScheduleState[] = [
  schedule('supplier-feed', {
    last_run_at: '2020-01-02T00:00:00Z',
    next_run_at: '2999-01-01T00:00:00Z',
    run_count: 47,
  }),
  schedule('rollup-uk', {
    last_run_at: '2020-01-01T00:00:00Z',
    next_run_at: '2999-02-01T00:00:00Z',
    run_count: 1,
  }),
  schedule('monthly-close', { next_run_at: '2999-03-01T00:00:00Z' }),
  schedule('daily-report', { next_run_at: '2999-04-01T00:00:00Z' }),
];

const stubSchedules = (
  answer: ScheduleState[] | 'pending' | 'failed'
): void => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => {
      switch (answer) {
        case 'pending':
          return new Promise<Response>(() => {});
        case 'failed':
          return Promise.resolve(new Response('no', { status: 503 }));
        default:
          return Promise.resolve(
            new Response(JSON.stringify(answer), {
              status: 200,
              headers: { 'Content-Type': 'application/json' },
            })
          );
      }
    })
  );
};

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

/** The schedule names of the body rows, from the top. */
const rowNames = (): string[] =>
  screen
    .getAllByRole('row')
    .slice(1)
    .map(row => within(row).getAllByRole('cell')[0]?.textContent ?? '');

const header = (label: string): HTMLElement =>
  screen.getByRole('columnheader', { name: label });

const sortButton = (label: string): HTMLElement =>
  within(header(label)).getByRole('button', { name: label });

/** The `aria-sort` of each header, in column order. */
const sortStates = (): (string | null)[] =>
  ['Schedule', 'Last run', 'Next run', 'Runs'].map(label =>
    header(label).getAttribute('aria-sort')
  );

async function renderListed(): Promise<void> {
  stubSchedules(SCHEDULES);
  render(<SchedulesTable />, { wrapper });
  await waitFor(() => expect(screen.getAllByRole('row')).toHaveLength(5));
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('SchedulesTable', () => {
  it('lists the schedules by the soonest next run and marks that column as sorted', async () => {
    await renderListed();

    expect(rowNames()).toEqual([
      'supplier-feed',
      'rollup-uk',
      'monthly-close',
      'daily-report',
    ]);
    expect(sortStates()).toEqual(['none', 'none', 'ascending', 'none']);
  });

  it('gives each of the four columns a header button', async () => {
    await renderListed();

    for (const label of ['Schedule', 'Last run', 'Next run', 'Runs']) {
      const button = sortButton(label);
      expect(button.tagName).toBe('BUTTON');
      expect(button.getAttribute('type')).toBe('button');
      expect(button.tabIndex).toBe(0);
    }
  });

  it('sorts by a column on a click of its header, ascending first', async () => {
    await renderListed();

    fireEvent.click(sortButton('Schedule'));

    expect(rowNames()).toEqual([
      'daily-report',
      'monthly-close',
      'rollup-uk',
      'supplier-feed',
    ]);
    expect(sortStates()).toEqual(['ascending', 'none', 'none', 'none']);
  });

  it('changes the direction on a second click of the same header', async () => {
    await renderListed();

    fireEvent.click(sortButton('Runs'));
    expect(sortStates()).toEqual(['none', 'none', 'none', 'ascending']);
    expect(rowNames()).toEqual([
      'daily-report',
      'monthly-close',
      'rollup-uk',
      'supplier-feed',
    ]);

    fireEvent.click(sortButton('Runs'));

    expect(sortStates()).toEqual(['none', 'none', 'none', 'descending']);
    expect(rowNames()).toEqual([
      'supplier-feed',
      'rollup-uk',
      'daily-report',
      'monthly-close',
    ]);
  });

  it('keeps the schedules that never ran below the others in a sort by last run', async () => {
    await renderListed();

    fireEvent.click(sortButton('Last run'));
    expect(rowNames()).toEqual([
      'rollup-uk',
      'supplier-feed',
      'daily-report',
      'monthly-close',
    ]);

    fireEvent.click(sortButton('Last run'));

    expect(rowNames()).toEqual([
      'supplier-feed',
      'rollup-uk',
      'daily-report',
      'monthly-close',
    ]);
  });

  it('writes "never" for a schedule that has not run', async () => {
    await renderListed();

    const neverRan = screen
      .getAllByRole('row')
      .find(row => row.textContent?.includes('monthly-close'));
    expect(neverRan).toBeDefined();
    const cells = within(neverRan!).getAllByRole('cell');
    expect(cells[1]?.textContent).toBe('never');
    expect(cells[1]?.getAttribute('title')).toBe('This schedule has not run.');
    expect(cells[3]?.textContent).toBe('0');
  });

  it('names in the header title the sort that a click gives', async () => {
    await renderListed();

    expect(sortButton('Next run').title).toBe('Sort by next run, descending');
    expect(sortButton('Runs').title).toBe('Sort by runs, ascending');

    fireEvent.click(sortButton('Runs'));

    expect(sortButton('Runs').title).toBe('Sort by runs, descending');
    expect(sortButton('Next run').title).toBe('Sort by next run, ascending');
  });

  it('says the sort in words for a screen reader', async () => {
    await renderListed();
    const status = (): string =>
      document.querySelector('[aria-live="polite"]')?.textContent ?? '';

    expect(status()).toBe('Sorted by next run, ascending.');

    fireEvent.click(sortButton('Last run'));
    fireEvent.click(sortButton('Last run'));

    expect(status()).toBe('Sorted by last run, descending.');
  });

  it('keeps the sort when a later read brings other rows', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    stubSchedules(SCHEDULES);
    render(
      <QueryClientProvider client={client}>
        <SchedulesTable />
      </QueryClientProvider>
    );
    await waitFor(() => expect(screen.getAllByRole('row')).toHaveLength(5));
    fireEvent.click(sortButton('Schedule'));

    act(() => {
      client.setQueryData(queryKeys.schedules(), [
        ...[...SCHEDULES].reverse(),
        schedule('abandoned-cart-sweep'),
      ]);
    });

    await waitFor(() =>
      expect(rowNames()).toEqual([
        'abandoned-cart-sweep',
        'daily-report',
        'monthly-close',
        'rollup-uk',
        'supplier-feed',
      ])
    );
    expect(sortStates()).toEqual(['ascending', 'none', 'none', 'none']);
  });

  it('says that it loads while the first read is in flight', () => {
    stubSchedules('pending');
    render(<SchedulesTable />, { wrapper });

    expect(screen.getByText('Loading schedules…')).toBeTruthy();
    expect(screen.queryByRole('table')).toBeNull();
  });

  it('says that no schedule is registered for an empty answer', async () => {
    stubSchedules([]);
    render(<SchedulesTable />, { wrapper });

    await waitFor(() =>
      expect(screen.getByText('No schedules registered.')).toBeTruthy()
    );
    expect(screen.queryByRole('table')).toBeNull();
  });

  it('shows a failed read as an error, never as no schedules', async () => {
    stubSchedules('failed');
    render(<SchedulesTable />, { wrapper });

    await waitFor(() =>
      expect(screen.getByText('Could not load schedules.')).toBeTruthy()
    );
    expect(screen.queryByText('No schedules registered.')).toBeNull();
  });
});

describe('schedulePanelState', () => {
  const rows = [schedule('a')];

  it('is failed for a failed read, also when an earlier read left rows', () => {
    expect(
      schedulePanelState({ schedules: rows, isLoading: false, isError: true })
    ).toEqual({ kind: 'failed' });
    expect(
      schedulePanelState({ schedules: [], isLoading: false, isError: true })
    ).toEqual({ kind: 'failed' });
  });

  it('lists the rows of a read, also while the next read is in flight', () => {
    expect(
      schedulePanelState({ schedules: rows, isLoading: true, isError: false })
    ).toEqual({ kind: 'listed', schedules: rows });
  });

  it('tells a first read in flight from an empty answer', () => {
    expect(
      schedulePanelState({ schedules: [], isLoading: true, isError: false })
    ).toEqual({ kind: 'loading' });
    expect(
      schedulePanelState({ schedules: [], isLoading: false, isError: false })
    ).toEqual({ kind: 'none_registered' });
  });
});
