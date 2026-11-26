import { describe, expect, it } from 'vitest';

import {
  capacityText,
  countText,
  downSlotCount,
  EXECUTION_CLASSES,
  groupByProfile,
  rowsAboveCapacity,
  seatsInService,
  shapeText,
  usedFraction,
  type GroupBudget,
  type KnownLoad,
  type WorkerGroup,
} from '@/components/monitoring/worker-profiles';
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

const ids = (group: WorkerGroup): string[] =>
  group.workers.map(worker => worker.worker_id);

function groupAt(groups: WorkerGroup[], index: number): WorkerGroup {
  const group = groups[index];
  if (group === undefined) {
    throw new Error(`no group at ${index}`);
  }
  return group;
}

function knownLoad(group: WorkerGroup): KnownLoad {
  if (group.load.kind !== 'known') {
    throw new Error(`the load of group ${group.key} is not known`);
  }
  return group.load;
}

/** 12 isolated children: every class goes to the ISOLATED budget. */
const isolatedOnly = (inFlight: number, capacity = 12) => [
  budgetLoad('ISOLATED', capacity, ALL_CLASSES, inFlight),
];

/** 2 isolated children and 4 shared async children of 25 seats. */
const isolatedAndAsync = (isolated: number, shared: number, seats = 100) => [
  budgetLoad('ISOLATED', 2, ['ISOLATED', 'SHARED_THREAD'], isolated),
  budgetLoad('SHARED_ASYNC', seats, ['SHARED_ASYNC'], shared),
];

const io = { profile: named('io') };

describe('groupByProfile', () => {
  it('puts one worker in the group of its profile, with its budgets and its shape', () => {
    const groups = groupByProfile(
      [workerState('w1', 12)],
      classReport([classLoad('w1', isolatedOnly(5))])
    );

    expect(groups).toHaveLength(1);
    const group = groupAt(groups, 0);
    expect(group.profile).toEqual(named('default'));
    expect(group.key).toBe('named:default');
    expect(ids(group)).toEqual(['w1']);
    expect(group.load).toEqual({
      kind: 'known',
      budgets: [
        {
          budget: 'ISOLATED',
          capacity: 12,
          inFlight: 5,
          free: 7,
          serves: ALL_CLASSES,
        },
      ],
      notServed: [],
      shapes: [{ shape: { children: 12, classes: ['ISOLATED'] }, workers: 1 }],
      withoutBudgets: 0,
    });
  });

  it('sums the budgets of the workers of one profile', () => {
    const groups = groupByProfile(
      [workerState('w1', 12), workerState('w2', 12)],
      classReport([
        classLoad('w1', isolatedOnly(5)),
        classLoad('w2', isolatedOnly(12)),
      ])
    );

    expect(groups).toHaveLength(1);
    expect(knownLoad(groupAt(groups, 0)).budgets).toEqual([
      {
        budget: 'ISOLATED',
        capacity: 24,
        inFlight: 17,
        free: 7,
        serves: ALL_CLASSES,
      },
    ]);
  });

  it('gives one group for each profile name, in name order', () => {
    const report = classReport([
      classLoad('a', isolatedOnly(1)),
      classLoad('b', isolatedAndAsync(1, 64)),
      classLoad('c', isolatedAndAsync(1, 10)),
      classLoad('d', isolatedOnly(2)),
    ]);
    const workers = [
      workerState('b', 6, { profile: named('io-10') }),
      workerState('a', 12, { profile: named('default') }),
      workerState('c', 6, { profile: named('io-2') }),
      workerState('d', 12, { profile: named('default') }),
    ];

    const groups = groupByProfile(workers, report);
    const reversed = groupByProfile([...workers].reverse(), report);

    expect(groups.map(group => group.key)).toEqual([
      'named:default',
      'named:io-2',
      'named:io-10',
    ]);
    expect(groups.map(ids)).toEqual([['a', 'd'], ['c'], ['b']]);
    expect(reversed.map(group => group.key)).toEqual(
      groups.map(group => group.key)
    );
  });

  it('keeps a profile at one shape while a slot is down or a shared child closes', () => {
    // The capacity in service is lower on w2 and on w4. The children and the
    // budget classes are the same, so the shape is the same.
    const groups = groupByProfile(
      [
        workerState('w1', 12),
        workerState('w2', 12, {
          slot_health: { kind: 'slots_down', slots: [downSlot(7)] },
        }),
        workerState('w3', 6, io),
        workerState('w4', 6, io),
      ],
      classReport([
        classLoad('w1', isolatedOnly(5)),
        classLoad('w2', isolatedOnly(4, 11)),
        classLoad('w3', isolatedAndAsync(1, 64)),
        classLoad('w4', isolatedAndAsync(2, 92, 87)),
      ])
    );

    const batch = knownLoad(groupAt(groups, 0));
    const shared = knownLoad(groupAt(groups, 1));
    expect(batch.shapes).toEqual([
      { shape: { children: 12, classes: ['ISOLATED'] }, workers: 2 },
    ]);
    expect(batch.budgets[0]?.capacity).toBe(23);
    expect(shared.shapes).toEqual([
      {
        shape: { children: 6, classes: ['ISOLATED', 'SHARED_ASYNC'] },
        workers: 2,
      },
    ]);
    expect(shared.budgets).toEqual([
      {
        budget: 'ISOLATED',
        capacity: 4,
        inFlight: 3,
        free: 1,
        serves: ['ISOLATED', 'SHARED_THREAD'],
      },
      {
        budget: 'SHARED_ASYNC',
        capacity: 187,
        inFlight: 156,
        free: 36,
        serves: ['SHARED_ASYNC'],
      },
    ]);
  });

  it('lists each shape of a profile whose workers do not have one shape, most workers first', () => {
    // One worker still runs the profile as it was: 8 children.
    const groups = groupByProfile(
      [workerState('old', 8), workerState('new-1', 12), workerState('new-2', 12)],
      classReport([
        classLoad('old', isolatedOnly(3, 8)),
        classLoad('new-1', isolatedOnly(5)),
        classLoad('new-2', isolatedOnly(6)),
      ])
    );

    expect(groups).toHaveLength(1);
    const load = knownLoad(groupAt(groups, 0));
    expect(load.shapes).toEqual([
      { shape: { children: 12, classes: ['ISOLATED'] }, workers: 2 },
      { shape: { children: 8, classes: ['ISOLATED'] }, workers: 1 },
    ]);
    expect(load.budgets[0]?.capacity).toBe(32);
    expect(load.budgets[0]?.inFlight).toBe(14);
  });

  it('orders shapes with the same number of workers by children, then by budget classes', () => {
    const groups = groupByProfile(
      [workerState('a', 6), workerState('b', 12), workerState('c', 6)],
      classReport([
        classLoad('a', isolatedAndAsync(0, 0)),
        classLoad('b', isolatedOnly(0)),
        classLoad('c', isolatedOnly(0, 6)),
      ])
    );

    expect(
      knownLoad(groupAt(groups, 0)).shapes.map(entry => shapeText(entry.shape))
    ).toEqual([
      '12 children per worker · ISOLATED budget',
      '6 children per worker · ISOLATED budget',
      '6 children per worker · ISOLATED + SHARED_ASYNC budgets',
    ]);
  });

  it('puts the workers with no profile name in one last group', () => {
    const groups = groupByProfile(
      [
        workerState('old-1', 8, { profile: NOT_RECORDED }),
        workerState('new', 12, { profile: named('zeta') }),
        workerState('old-2', 4, { profile: NOT_RECORDED }),
      ],
      classReport([
        classLoad('old-1', isolatedOnly(1, 8)),
        classLoad('old-2', isolatedOnly(1, 4)),
        classLoad('new', isolatedOnly(1)),
      ])
    );

    expect(groups.map(group => group.key)).toEqual([
      'named:zeta',
      'not-recorded',
    ]);
    const unnamed = groupAt(groups, 1);
    expect(unnamed.profile).toEqual(NOT_RECORDED);
    expect(ids(unnamed)).toEqual(['old-1', 'old-2']);
    expect(knownLoad(unnamed).shapes).toHaveLength(2);
    expect(knownLoad(unnamed).budgets[0]?.capacity).toBe(12);
  });

  it('keeps a worker that the report does not hold in the group of its profile and counts it', () => {
    const groups = groupByProfile(
      [workerState('late', 12), workerState('w1', 12)],
      classReport([classLoad('w1', isolatedOnly(5))])
    );

    expect(groups).toHaveLength(1);
    const group = groupAt(groups, 0);
    expect(ids(group)).toEqual(['late', 'w1']);
    const load = knownLoad(group);
    expect(load.withoutBudgets).toBe(1);
    expect(load.budgets[0]?.capacity).toBe(12);
    expect(load.shapes).toEqual([
      { shape: { children: 12, classes: ['ISOLATED'] }, workers: 1 },
    ]);
  });

  it('gives a group an unknown load when the report holds none of its workers', () => {
    const groups = groupByProfile(
      [workerState('w1', 12), workerState('late', 6, io)],
      classReport([classLoad('w1', isolatedOnly(5))])
    );

    expect(groups.map(group => [group.key, group.load.kind])).toEqual([
      ['named:default', 'known'],
      ['named:io', 'unknown'],
    ]);
  });

  it('groups by profile with an unknown load while no report is read', () => {
    const groups = groupByProfile(
      [workerState('w1', 12), workerState('w2', 6, io), workerState('w3', 12)],
      undefined
    );

    expect(groups.map(ids)).toEqual([['w1', 'w3'], ['w2']]);
    expect(groups.map(group => group.load)).toEqual([
      { kind: 'unknown' },
      { kind: 'unknown' },
    ]);
  });

  it('keeps the order of the workers inside a group', () => {
    const groups = groupByProfile(
      [workerState('b', 12), workerState('c', 12), workerState('a', 12)],
      classReport([
        classLoad('a', isolatedOnly(1)),
        classLoad('b', isolatedOnly(1)),
        classLoad('c', isolatedOnly(1)),
      ])
    );

    expect(groups.map(ids)).toEqual([['b', 'c', 'a']]);
  });

  it('names the classes that no budget of a group claims', () => {
    const groups = groupByProfile(
      [workerState('files', 4, { profile: named('files') })],
      classReport([
        classLoad('files', [
          budgetLoad('SHARED_THREAD', 32, ['SHARED_THREAD'], 9),
        ]),
      ])
    );

    expect(knownLoad(groupAt(groups, 0)).notServed).toEqual([
      'ISOLATED',
      'SHARED_ASYNC',
    ]);
  });

  it('keeps a worker that serves no class as a shape with no budget', () => {
    const groups = groupByProfile(
      [workerState('idle', 2)],
      classReport([classLoad('idle', [])])
    );

    const load = knownLoad(groupAt(groups, 0));
    expect(load.budgets).toEqual([]);
    expect(load.notServed).toEqual(EXECUTION_CLASSES);
    expect(load.shapes.map(entry => shapeText(entry.shape))).toEqual([
      '2 children per worker · no budget',
    ]);
  });

  it('gives no group for no worker', () => {
    expect(groupByProfile([], classReport([]))).toEqual([]);
    expect(groupByProfile([], undefined)).toEqual([]);
  });

  it('does not count the budgets of a report entry that has no worker in the list', () => {
    const groups = groupByProfile(
      [workerState('w1', 12)],
      classReport([
        classLoad('w1', isolatedOnly(5)),
        classLoad('gone', isolatedOnly(12)),
      ])
    );

    expect(groups.map(ids)).toEqual([['w1']]);
    expect(knownLoad(groupAt(groups, 0)).budgets[0]?.capacity).toBe(12);
  });
});

describe('group texts', () => {
  const group = groupAt(
    groupByProfile(
      [
        workerState('w3', 6, io),
        workerState('w4', 6, {
          ...io,
          slot_health: { kind: 'slots_down', slots: [downSlot(1), downSlot(4)] },
        }),
      ],
      classReport([
        classLoad('w3', isolatedAndAsync(1, 64)),
        classLoad('w4', isolatedAndAsync(2, 92, 87)),
      ])
    ),
    0
  );

  it('writes the children of one worker and the budget classes of a shape', () => {
    expect(shapeText({ children: 6, classes: ['ISOLATED', 'SHARED_ASYNC'] })).toBe(
      '6 children per worker · ISOLATED + SHARED_ASYNC budgets'
    );
    expect(shapeText({ children: 1, classes: ['ISOLATED'] })).toBe(
      '1 child per worker · ISOLATED budget'
    );
    expect(shapeText({ children: 2, classes: [] })).toBe(
      '2 children per worker · no budget'
    );
  });

  it('counts the seats in service over all budgets', () => {
    expect(seatsInService(knownLoad(group))).toBe(191);
  });

  it('counts the down slots of the workers of a group', () => {
    expect(downSlotCount(group)).toBe(2);
  });

  it('counts children for an isolated budget and seats for a shared budget', () => {
    expect(capacityText('ISOLATED', 12)).toBe('12 children');
    expect(capacityText('ISOLATED', 1)).toBe('1 child');
    expect(capacityText('SHARED_ASYNC', 100)).toBe('100 seats');
    expect(capacityText('SHARED_THREAD', 1)).toBe('1 seat');
    expect(capacityText('SHARED_THREAD', 0)).toBe('0 seats');
  });

  it('writes one and many', () => {
    expect(countText(1, 'worker', 'workers')).toBe('1 worker');
    expect(countText(0, 'worker', 'workers')).toBe('0 workers');
    expect(countText(3, 'worker', 'workers')).toBe('3 workers');
  });
});

describe('budget load', () => {
  const budget = (
    capacity: number,
    inFlight: number,
    free: number
  ): GroupBudget => ({
    budget: 'ISOLATED',
    capacity,
    inFlight,
    free,
    serves: ['ISOLATED'],
  });

  it('gives the part of the capacity that holds a row', () => {
    expect(usedFraction(budget(12, 3, 9))).toBe(0.25);
    expect(usedFraction(budget(12, 0, 12))).toBe(0);
    expect(usedFraction(budget(12, 12, 0))).toBe(1);
  });

  it('stops at the full capacity when the workers hold more rows', () => {
    expect(usedFraction(budget(12, 14, 0))).toBe(1);
  });

  it('gives 0 for a budget with no capacity in service', () => {
    expect(usedFraction(budget(0, 0, 0))).toBe(0);
    expect(usedFraction(budget(0, 3, 0))).toBe(0);
  });

  it('counts no row above the capacity while each worker has room', () => {
    expect(rowsAboveCapacity(budget(12, 3, 9))).toBe(0);
    expect(rowsAboveCapacity(budget(12, 12, 0))).toBe(0);
  });

  it('counts the rows that no capacity in service holds', () => {
    // One worker holds 14 rows on 12 children, another holds 2 on 12: the
    // group has 10 free children and 2 rows above the capacity.
    expect(rowsAboveCapacity(budget(24, 16, 10))).toBe(2);
    expect(rowsAboveCapacity(budget(0, 3, 0))).toBe(3);
  });
});
