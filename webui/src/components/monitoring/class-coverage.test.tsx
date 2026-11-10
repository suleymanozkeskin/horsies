import { describe, expect, it, vi } from 'vitest';

import { fireEvent, render, screen } from '@testing-library/react';

import {
  budgetFreeText,
  budgetsOf,
  coverageText,
  fleetWarningText,
  uncoveredStreams,
  waitText,
} from '@/components/monitoring/class-coverage';
import {
  BudgetTable,
  ClassCoveragePanel,
  DownSlotList,
} from '@/components/monitoring/class-coverage-panel';
import type { ClassReport, PendingStream } from '@/types/workers';

const served: PendingStream = {
  queue_name: 'default',
  execution: 'SHARED_ASYNC',
  coverage: { kind: 'served', workers: 2, capacity: 80 },
  wait: { kind: 'waiting', oldest_due_at: '2026-11-10T12:00:00Z', wait_s: 95 },
};

const unservedRetry: PendingStream = {
  queue_name: 'default',
  execution: 'ISOLATED',
  coverage: { kind: 'unserved' },
  wait: { kind: 'waiting', oldest_due_at: '2026-11-10T11:00:00Z', wait_s: 3700 },
};

const withoutCapacity: PendingStream = {
  queue_name: 'emails',
  execution: 'SHARED_THREAD',
  coverage: { kind: 'no_capacity', workers: 1 },
  wait: { kind: 'none_due', next_due_at: '2999-01-01T00:00:00Z' },
};

const report: ClassReport = {
  checked_at: '2026-11-10T12:01:35Z',
  live_window_s: 120,
  streams: [unservedRetry, served, withoutCapacity],
  workers: [
    {
      worker_id: 'w-async',
      snapshot_at: '2026-11-10T12:01:30Z',
      budgets: [
        {
          budget: 'SHARED_ASYNC',
          capacity: 40,
          serves: ['SHARED_ASYNC'],
          in_flight: 37,
          free: 3,
        },
      ],
    },
  ],
};

describe('class report helpers', () => {
  it('finds the streams that no live worker can run, in report order', () => {
    expect(uncoveredStreams(report)).toEqual([unservedRetry, withoutCapacity]);
  });

  it('finds a live worker by id and nothing for a worker the report lacks', () => {
    expect(budgetsOf(report, 'w-async')).toEqual(report.workers[0]?.budgets);
    expect(budgetsOf(report, 'w-gone')).toBeUndefined();
    expect(budgetsOf(undefined, 'w-async')).toBeUndefined();
  });

  it('tells an operator what to start for an unserved isolated stream', () => {
    expect(fleetWarningText(unservedRetry)).toBe(
      "PENDING ISOLATED tasks on queue 'default' wait: no live worker serves this class. Start a worker with isolated children on this queue."
    );
  });

  it('names both kinds of children for an unserved shared stream', () => {
    expect(
      fleetWarningText({ ...served, coverage: { kind: 'unserved' } })
    ).toContain('Start a worker with SHARED_ASYNC children or with isolated children');
  });

  it('points at the down slots for a stream without capacity', () => {
    expect(fleetWarningText(withoutCapacity)).toContain(
      '1 live worker(s) serve this class, and none has a child in service for it'
    );
  });

  it('writes each coverage state', () => {
    expect(coverageText(served)).toBe('2 worker(s) · capacity 80');
    expect(coverageText(withoutCapacity)).toBe('1 worker(s) · no capacity');
    expect(coverageText(unservedRetry)).toBe('no live worker');
  });

  it('writes a wait and a stream with no due row', () => {
    expect(waitText(served.wait)).toBe('1m');
    expect(waitText(withoutCapacity.wait)).toMatch(/^next due in /);
  });

  it('writes the free capacity of a budget out of its capacity', () => {
    const budget = report.workers[0]?.budgets[0];
    expect(budget).toBeDefined();
    expect(budgetFreeText(budget!)).toBe('3/40 free');
  });
});

describe('ClassCoveragePanel', () => {
  it('shows one fleet warning per stream that no live worker can run', () => {
    render(
      <ClassCoveragePanel
        report={report}
        isLoading={false}
        isError={false}
        onRetry={() => {}}
      />
    );

    const alert = screen.getByRole('alert');
    expect(alert.textContent).toContain(fleetWarningText(unservedRetry));
    expect(alert.textContent).toContain(fleetWarningText(withoutCapacity));
    expect(alert.textContent).not.toContain('SHARED_ASYNC tasks');
  });

  it('lists every pending stream with its wait and coverage', () => {
    render(
      <ClassCoveragePanel
        report={report}
        isLoading={false}
        isError={false}
        onRetry={() => {}}
      />
    );

    expect(screen.getByText('2 worker(s) · capacity 80')).toBeTruthy();
    expect(screen.getByText('no live worker')).toBeTruthy();
    expect(screen.getByText('1h 1m')).toBeTruthy();
  });

  it('shows no warning when every stream is served', () => {
    render(
      <ClassCoveragePanel
        report={{ ...report, streams: [served] }}
        isLoading={false}
        isError={false}
        onRetry={() => {}}
      />
    );

    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('says that nothing is pending instead of an empty table', () => {
    render(
      <ClassCoveragePanel
        report={{ ...report, streams: [] }}
        isLoading={false}
        isError={false}
        onRetry={() => {}}
      />
    );

    expect(screen.getByText('No pending rows.')).toBeTruthy();
  });

  it('keeps the last report visible beside a failed refresh', () => {
    render(
      <ClassCoveragePanel
        report={report}
        isLoading={false}
        isError={true}
        onRetry={() => {}}
      />
    );

    expect(
      screen.getByText('Could not load the execution class report.')
    ).toBeTruthy();
    expect(screen.getByText('2 worker(s) · capacity 80')).toBeTruthy();
  });

  it('renders a failed read as an error with a retry, never as no data', () => {
    const onRetry = vi.fn();
    render(
      <ClassCoveragePanel
        report={undefined}
        isLoading={false}
        isError={true}
        onRetry={onRetry}
      />
    );

    expect(
      screen.getByText('Could not load the execution class report.')
    ).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: /retry/i }));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});

describe('worker budget and slot views', () => {
  it('lists each budget with its capacity, rows held and free capacity', () => {
    const budgets = report.workers[0]?.budgets ?? [];
    render(<BudgetTable budgets={budgets} />);

    expect(screen.getByText('40')).toBeTruthy();
    expect(screen.getByText('37')).toBeTruthy();
    expect(screen.getByText('3')).toBeTruthy();
  });

  it('says so when a worker serves no class', () => {
    render(<BudgetTable budgets={[]} />);

    expect(
      screen.getByText('This worker serves no execution class.')
    ).toBeTruthy();
  });

  it('lists each down slot with its cause and failed starts', () => {
    render(
      <DownSlotList
        slots={[
          {
            slot: 2,
            cause: 'the OS ran short of a resource (EMFILE)',
            detail: 'OSError: [Errno 24] Too many open files',
            down_since: '2026-11-10T12:00:00Z',
            failed_boots: 4,
          },
        ]}
      />
    );

    expect(screen.getByText('slot 2')).toBeTruthy();
    expect(
      screen.getByText('the OS ran short of a resource (EMFILE)')
    ).toBeTruthy();
    expect(screen.getByText(/4 failed start\(s\) since/)).toBeTruthy();
  });
});
