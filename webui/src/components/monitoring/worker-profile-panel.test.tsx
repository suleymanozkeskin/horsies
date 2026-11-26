import { render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import {
  profileLabel,
  WorkerProfilePanel,
  type ClassReportRead,
} from '@/components/monitoring/worker-profile-panel';
import {
  groupByProfile,
  type WorkerGroup,
} from '@/components/monitoring/worker-profiles';
import { statusColorVar } from '@/lib/status-utils';
import {
  ALL_CLASSES,
  budgetLoad,
  classLoad,
  classReport,
  downSlot,
  named,
  NOT_RECORDED,
  workerState,
} from '@/test/worker-fixtures';
import type { BudgetLoad, WorkerState } from '@/types/workers';

/**
 * The one group of `workers`. `budgets` has one entry for each worker in
 * order: its budgets, or `'not in the report'`.
 */
function oneGroup(
  workers: WorkerState[],
  budgets: (BudgetLoad[] | 'not in the report')[]
): WorkerGroup {
  const groups = groupByProfile(
    workers,
    classReport(
      workers.flatMap((worker, index) => {
        const entry = budgets[index];
        return entry === undefined || entry === 'not in the report'
          ? []
          : [classLoad(worker.worker_id, entry)];
      })
    )
  );
  const group = groups[0];
  if (groups.length !== 1 || group === undefined) {
    throw new Error(`expected one group, got ${groups.length}`);
  }
  return group;
}

function renderPanel(
  group: WorkerGroup,
  reportRead: ClassReportRead = 'loaded'
): HTMLElement {
  const { container } = render(
    <WorkerProfilePanel group={group} reportRead={reportRead}>
      <span>worker cards</span>
    </WorkerProfilePanel>
  );
  return container;
}

/** The width of the fill of a meter, as the style has it. */
const fillWidth = (meter: HTMLElement): string =>
  meter.querySelector<HTMLElement>('div')?.style.width ?? '';

const isolated = (inFlight: number, capacity = 12): BudgetLoad[] => [
  budgetLoad('ISOLATED', capacity, ALL_CLASSES, inFlight),
];

const defaultGroup = oneGroup([workerState('w1', 12)], [isolated(3)]);

describe('WorkerProfilePanel for a named profile', () => {
  it('names the profile, and counts its workers and its seats in service', () => {
    const container = renderPanel(defaultGroup);

    expect(screen.getByRole('group', { name: 'Profile default' })).toBeTruthy();
    expect(screen.getByText('default')).toBeTruthy();
    expect(container.textContent).toContain('1 worker · 12 seats in service');
    expect(container.textContent).not.toContain('down');
    expect(container.textContent).not.toContain('schema migration');
  });

  it('says what the profile gives a worker', () => {
    const container = renderPanel(defaultGroup);

    expect(
      screen.getByText('12 children per worker · ISOLATED budget')
    ).toBeTruthy();
    expect(container.textContent).not.toContain('shapes');
  });

  it('shows the load of a budget as a meter and in words', () => {
    const container = renderPanel(defaultGroup);

    const meter = screen.getByRole('meter', { name: 'ISOLATED budget' });
    expect(meter.getAttribute('aria-valuemin')).toBe('0');
    expect(meter.getAttribute('aria-valuemax')).toBe('12');
    expect(meter.getAttribute('aria-valuenow')).toBe('3');
    expect(meter.getAttribute('aria-valuetext')).toBe(
      '3 in flight of 12 children in service'
    );
    expect(fillWidth(meter)).toBe('25%');
    expect(container.textContent).toContain('3 in flight · 9 free');
    expect(container.textContent).toContain(
      '12 children in service · serves ISOLATED, SHARED_ASYNC, SHARED_THREAD'
    );
  });

  it('fills the meter with the color of the RUNNING status', () => {
    renderPanel(defaultGroup);

    const fill = screen
      .getByRole('meter', { name: 'ISOLATED budget' })
      .querySelector<HTMLElement>('div');
    expect(fill?.style.background).toBe(statusColorVar('RUNNING'));
  });

  it('sums the budgets over the workers and gives one meter for each budget', () => {
    const io = { profile: named('io') };
    const group = oneGroup(
      [workerState('io-1', 6, io), workerState('io-2', 6, io)],
      [
        [
          budgetLoad('ISOLATED', 2, ['ISOLATED', 'SHARED_THREAD'], 1),
          budgetLoad('SHARED_ASYNC', 100, ['SHARED_ASYNC'], 50),
        ],
        [
          budgetLoad('ISOLATED', 2, ['ISOLATED', 'SHARED_THREAD'], 2),
          budgetLoad('SHARED_ASYNC', 100, ['SHARED_ASYNC'], 100),
        ],
      ]
    );

    const container = renderPanel(group);

    expect(screen.getByRole('group', { name: 'Profile io' })).toBeTruthy();
    expect(container.textContent).toContain('2 workers · 204 seats in service');
    expect(container.textContent).toContain(
      '6 children per worker · ISOLATED + SHARED_ASYNC budgets'
    );
    expect(screen.getAllByRole('meter')).toHaveLength(2);
    const isolatedMeter = screen.getByRole('meter', { name: 'ISOLATED budget' });
    const shared = screen.getByRole('meter', { name: 'SHARED_ASYNC budget' });
    expect(fillWidth(isolatedMeter)).toBe('75%');
    expect(fillWidth(shared)).toBe('75%');
    expect(shared.getAttribute('aria-valuetext')).toBe(
      '150 in flight of 200 seats in service'
    );
    expect(container.textContent).toContain('3 in flight · 1 free');
    expect(container.textContent).toContain('150 in flight · 50 free');
    expect(container.textContent).toContain(
      '4 children in service · serves ISOLATED, SHARED_THREAD'
    );
    expect(container.textContent).toContain(
      '200 seats in service · serves SHARED_ASYNC'
    );
  });

  it('keeps the meter full and counts the rows above the capacity', () => {
    const group = oneGroup(
      [workerState('io-2', 6)],
      [[budgetLoad('SHARED_ASYNC', 87, ['SHARED_ASYNC'], 92)]]
    );

    const container = renderPanel(group);

    const meter = screen.getByRole('meter', { name: 'SHARED_ASYNC budget' });
    expect(fillWidth(meter)).toBe('100%');
    expect(meter.getAttribute('aria-valuenow')).toBe('87');
    expect(container.textContent).toContain('92 in flight · 0 free');
    expect(container.textContent).toContain('5 rows above capacity');
  });

  it('does not write rows above the capacity while each worker has room', () => {
    const container = renderPanel(defaultGroup);

    expect(container.textContent).not.toContain('above capacity');
  });

  it('shows a budget with no capacity in service with an empty meter', () => {
    const group = oneGroup(
      [
        workerState('w1', 1, {
          slot_health: { kind: 'slots_down', slots: [downSlot(0)] },
        }),
      ],
      [isolated(0, 0)]
    );

    const container = renderPanel(group);

    const meter = screen.getByRole('meter', { name: 'ISOLATED budget' });
    expect(fillWidth(meter)).toBe('0%');
    expect(container.textContent).toContain('0 children in service');
    expect(container.textContent).toContain('1 worker · 0 seats in service');
  });

  it('counts the down slots of the group in its header', () => {
    const group = oneGroup(
      [
        workerState('w1', 12),
        workerState('w2', 12, {
          slot_health: { kind: 'slots_down', slots: [downSlot(3), downSlot(7)] },
        }),
      ],
      [isolated(5), isolated(4, 10)]
    );

    const container = renderPanel(group);

    expect(container.textContent).toContain(
      '2 workers · 22 seats in service · 2 slots down'
    );
  });

  it('names the classes that the workers of the profile do not claim', () => {
    const group = oneGroup(
      [workerState('files', 4, { profile: named('files') })],
      [[budgetLoad('SHARED_THREAD', 32, ['SHARED_THREAD'], 9)]]
    );

    const container = renderPanel(group);

    expect(container.textContent).toContain(
      'Claims no ISOLATED or SHARED_ASYNC rows: these workers run no children that can run them.'
    );
  });

  it('does not write a line of classes that are not claimed when every class is served', () => {
    const container = renderPanel(defaultGroup);

    expect(container.textContent).not.toContain('Claims no');
  });

  it('names all three classes for a worker that serves no class', () => {
    const container = renderPanel(oneGroup([workerState('idle', 2)], [[]]));

    expect(screen.queryAllByRole('meter')).toHaveLength(0);
    expect(container.textContent).toContain('2 children per worker · no budget');
    expect(container.textContent).toContain(
      'Claims no ISOLATED, SHARED_ASYNC or SHARED_THREAD rows'
    );
  });

  it('lists the shapes of a profile whose workers do not have one shape', () => {
    const group = oneGroup(
      [workerState('old', 8), workerState('new-1', 12), workerState('new-2', 12)],
      [isolated(3, 8), isolated(5), isolated(6)]
    );

    const container = renderPanel(group);

    const count = screen.getByText('2 shapes in one profile');
    expect(count.getAttribute('title')).toBe(
      'The workers of this profile do not have one shape: they do not run the same definition of the profile.'
    );
    expect(count.style.color).toBe('var(--warning-dark)');
    expect(
      screen.getAllByRole('listitem').map(item => item.textContent)
    ).toEqual(
      expect.arrayContaining([
        '12 children per worker · ISOLATED budget · 2 workers',
        '8 children per worker · ISOLATED budget · 1 worker',
      ])
    );
    expect(container.textContent).toContain('3 workers · 32 seats in service');
  });

  it('counts the workers that the class report does not hold, apart from the sums', () => {
    const group = oneGroup(
      [workerState('w1', 12), workerState('late', 12)],
      [isolated(5), 'not in the report']
    );

    const container = renderPanel(group);

    expect(container.textContent).toContain('2 workers · 12 seats in service');
    expect(container.textContent).toContain(
      '1 worker has no budgets in the execution class report, so the sums leave it out.'
    );
  });

  it('writes the plural for more than one worker that the report does not hold', () => {
    const group = oneGroup(
      [workerState('w1', 12), workerState('late-1', 12), workerState('late-2', 12)],
      [isolated(5), 'not in the report', 'not in the report']
    );

    const container = renderPanel(group);

    expect(container.textContent).toContain(
      '2 workers have no budgets in the execution class report, so the sums leave them out.'
    );
  });

  it('does not write that line when the report holds every worker', () => {
    const container = renderPanel(defaultGroup);

    expect(container.textContent).not.toContain('no budgets in the execution');
  });

  it('holds the cards of its workers below the budgets', () => {
    const container = renderPanel(defaultGroup);

    const cards = screen.getByText('worker cards');
    const meter = screen.getByRole('meter', { name: 'ISOLATED budget' });
    expect(container.contains(cards)).toBe(true);
    expect(
      meter.compareDocumentPosition(cards) & Node.DOCUMENT_POSITION_FOLLOWING
    ).toBeTruthy();
  });
});

describe('WorkerProfilePanel for a profile with an unknown load', () => {
  const unknown = oneGroup(
    [workerState('late-1', 12), workerState('late-2', 12)],
    ['not in the report', 'not in the report']
  );

  it('names the profile and counts its workers, with no seats and no meter', () => {
    const container = renderPanel(unknown);

    expect(screen.getByRole('group', { name: 'Profile default' })).toBeTruthy();
    expect(container.textContent).toContain('2 workers');
    expect(container.textContent).not.toContain('seats in service');
    expect(container.textContent).not.toContain('per worker');
    expect(container.textContent).not.toContain('Claims no');
    expect(screen.queryAllByRole('meter')).toHaveLength(0);
    expect(within(container).getByText('worker cards')).toBeTruthy();
  });

  it.each<[ClassReportRead, string]>([
    ['loading', 'Reading the budgets of these workers…'],
    [
      'failed',
      'The execution class report could not be read, so the budgets of these workers are not known.',
    ],
    [
      'loaded',
      'The execution class report has no live snapshot of these workers.',
    ],
  ])('gives the cause when the report is %s', (reportRead, text) => {
    const container = renderPanel(unknown, reportRead);

    expect(container.textContent).toContain(text);
  });
});

describe('WorkerProfilePanel for workers with no profile name', () => {
  const unnamed = oneGroup(
    [
      workerState('old-1', 8, { profile: NOT_RECORDED }),
      workerState('old-2', 4, { profile: NOT_RECORDED }),
    ],
    [isolated(1, 8), isolated(1, 4)]
  );

  it('says that the profile is not recorded, and why', () => {
    const container = renderPanel(unnamed);

    expect(
      screen.getByRole('group', { name: 'Profile not recorded' })
    ).toBeTruthy();
    expect(screen.getByText('Profile not recorded')).toBeTruthy();
    expect(container.textContent).toContain(
      'A worker of a build before schema migration 1004 wrote these snapshots, so they hold no profile name.'
    );
  });

  it('lists the shapes with no warning: the workers can be of more than one profile', () => {
    renderPanel(unnamed);

    const count = screen.getByText('2 shapes');
    expect(count.style.color).toBe('');
    expect(count.getAttribute('title')).toBeNull();
    expect(screen.queryByText('2 shapes in one profile')).toBeNull();
  });

  it('shows the load of its workers like a named profile', () => {
    const container = renderPanel(unnamed);

    expect(container.textContent).toContain('2 workers · 12 seats in service');
    expect(container.textContent).toContain('2 in flight · 10 free');
    expect(screen.getAllByRole('meter')).toHaveLength(1);
  });
});

describe('profileLabel', () => {
  it('names a profile for a screen reader', () => {
    expect(profileLabel(named('batch-high-memory'))).toBe(
      'Profile batch-high-memory'
    );
    expect(profileLabel(NOT_RECORDED)).toBe('Profile not recorded');
  });
});
