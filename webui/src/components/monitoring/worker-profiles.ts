// The live workers in groups of one worker profile, with the load of each
// group. Pure: no I/O, no clock.
//
// A worker snapshot names the worker profile that the worker runs
// (`profile`). The class report gives the claim budgets of each live worker.
// From the two, a group has:
//
// - its load: each budget summed over the workers of the group;
// - its shapes: what the profile gives a worker, as the worker recorded it.
//   A shape is the number of children of a worker (`processes`) and its
//   budget classes. These two facts do not change while a worker runs. The
//   capacity of a budget is not a part of the shape: it counts the children
//   or seats in service, so it goes down while a slot is down or a shared
//   child closes.
//
// The workers of one profile have one shape, unless they do not run the same
// definition of the profile: for example, the profile changed and only a part
// of the workers started again.

import { assertNever } from '@/lib/exhaustive';
import type {
  BudgetLoad,
  ClassReport,
  ExecutionClass,
  SnapshotProfile,
  WorkerState,
} from '@/types/workers';

/** The execution classes, in the order of the monitoring API. */
export const EXECUTION_CLASSES: readonly ExecutionClass[] = [
  'ISOLATED',
  'SHARED_ASYNC',
  'SHARED_THREAD',
];

/** A list with at least one entry. */
export type NonEmpty<Entry> = readonly [Entry, ...Entry[]];

/** One claim budget of a group: the sum over the workers of the group. */
export interface GroupBudget {
  readonly budget: ExecutionClass;
  /** Children (`ISOLATED`) or seats (a shared class) in service. */
  readonly capacity: number;
  /** RUNNING rows and CLAIMED rows with a live lease, counted at the read. */
  readonly inFlight: number;
  /** The sum of the free capacity of each worker, each at least 0. */
  readonly free: number;
  /** The classes whose rows the budget claims, in class order. */
  readonly serves: readonly ExecutionClass[];
}

/** What a profile gives a worker, as the worker recorded it. */
export interface WorkerShape {
  /** The children of the worker, of all execution modes. */
  readonly children: number;
  /** The budget classes of the worker, in class order. */
  readonly classes: readonly ExecutionClass[];
}

/** One shape of a group, and the number of its workers that have it. */
export interface ShapeCount {
  readonly shape: WorkerShape;
  readonly workers: number;
}

/** The load of a group of which the class report holds at least one worker. */
export interface KnownLoad {
  readonly kind: 'known';
  /** One entry for each budget class of the group, in class order. */
  readonly budgets: readonly GroupBudget[];
  /** The classes that no budget of the group claims, in class order. */
  readonly notServed: readonly ExecutionClass[];
  /** The shapes of the workers that the report holds: the shape of the most
   * workers first. More than one shape in a named profile: its workers do
   * not run the same definition of the profile. */
  readonly shapes: NonEmpty<ShapeCount>;
  /** The workers of the group that the report does not hold. Their budgets
   * are not in `budgets`. */
  readonly withoutBudgets: number;
}

/** The class report holds no worker of the group: the report is not read, or
 * it has no live snapshot of these workers. */
export interface UnknownLoad {
  readonly kind: 'unknown';
}

export type GroupLoad = KnownLoad | UnknownLoad;

/** The live workers that have one profile in common: the workers of one
 * named profile, or the workers whose snapshot holds no profile name. */
export interface WorkerGroup {
  readonly key: string;
  readonly profile: SnapshotProfile;
  /** The workers of the group, in the order of the worker list. */
  readonly workers: NonEmpty<WorkerState>;
  readonly load: GroupLoad;
}

const classOrder = (execution: ExecutionClass): number =>
  EXECUTION_CLASSES.indexOf(execution);

function inClassOrder(classes: Iterable<ExecutionClass>): ExecutionClass[] {
  return [...new Set(classes)].sort(
    (left, right) => classOrder(left) - classOrder(right)
  );
}

const compareText = (left: string, right: string): number =>
  left < right ? -1 : left > right ? 1 : 0;

/** Profile names with numbers in number order (`io-2` before `io-10`), with
 * a fixed locale so the order does not depend on the browser. */
const nameOrder = new Intl.Collator('en', { numeric: true });

function profileKey(profile: SnapshotProfile): string {
  switch (profile.kind) {
    case 'named':
      return `named:${profile.name}`;
    case 'not_recorded':
      return 'not-recorded';
    default:
      return assertNever(profile, 'snapshot profile');
  }
}

/** Named profiles in name order, then the workers with no profile name. */
function compareProfiles(left: SnapshotProfile, right: SnapshotProfile): number {
  if (left.kind === 'named' && right.kind === 'named') {
    return (
      nameOrder.compare(left.name, right.name) ||
      compareText(left.name, right.name)
    );
  }
  return Number(left.kind === 'not_recorded') - Number(right.kind === 'not_recorded');
}

const shapeKey = (shape: WorkerShape): string =>
  `${shape.children}:${shape.classes.join('+')}`;

function sumBudgets(loads: readonly BudgetLoad[]): GroupBudget[] {
  return inClassOrder(loads.map(load => load.budget)).map(budget => {
    const ofClass = loads.filter(load => load.budget === budget);
    return {
      budget,
      capacity: ofClass.reduce((sum, load) => sum + load.capacity, 0),
      inFlight: ofClass.reduce((sum, load) => sum + load.in_flight, 0),
      free: ofClass.reduce((sum, load) => sum + load.free, 0),
      serves: inClassOrder(ofClass.flatMap(load => load.serves)),
    };
  });
}

/** The budgets of one worker that the class report holds. */
interface WorkerBudgets {
  readonly worker: WorkerState;
  readonly budgets: readonly BudgetLoad[];
}

/** The distinct shapes of `reported`: the shape of the most workers first,
 * then more children first, then by budget classes. */
function countShapes(reported: NonEmpty<WorkerBudgets>): NonEmpty<ShapeCount> {
  const counts = new Map<string, ShapeCount>();
  for (const { worker, budgets } of reported) {
    const shape: WorkerShape = {
      children: worker.processes,
      classes: inClassOrder(budgets.map(budget => budget.budget)),
    };
    const key = shapeKey(shape);
    counts.set(key, { shape, workers: (counts.get(key)?.workers ?? 0) + 1 });
  }
  const [first, ...rest] = [...counts.values()].sort(
    (left, right) =>
      right.workers - left.workers ||
      right.shape.children - left.shape.children ||
      compareText(shapeKey(left.shape), shapeKey(right.shape))
  );
  if (first === undefined) {
    // `reported` has at least one worker, so at least one shape is counted.
    throw new Error('worker profile group: workers with budgets gave no shape');
  }
  return [first, ...rest];
}

function groupLoad(
  workers: NonEmpty<WorkerState>,
  budgetsById: ReadonlyMap<string, readonly BudgetLoad[]>
): GroupLoad {
  const reported = workers.flatMap((worker): WorkerBudgets[] => {
    const budgets = budgetsById.get(worker.worker_id);
    return budgets === undefined ? [] : [{ worker, budgets }];
  });
  const [first, ...rest] = reported;
  if (first === undefined) {
    return { kind: 'unknown' };
  }
  const budgets = sumBudgets(reported.flatMap(member => member.budgets));
  const served = new Set(budgets.flatMap(budget => budget.serves));
  return {
    kind: 'known',
    budgets,
    notServed: EXECUTION_CLASSES.filter(execution => !served.has(execution)),
    shapes: countShapes([first, ...rest]),
    withoutBudgets: workers.length - reported.length,
  };
}

/**
 * The workers in groups of one profile. A worker keeps its place in
 * `workers` inside its group. The groups are in a fixed order: the named
 * profiles in name order, then the workers whose snapshot holds no profile
 * name. A group is never empty.
 *
 * `report` gives the budgets of the workers. It is `undefined` while the
 * class report is not read: the load of each group is then unknown.
 */
export function groupByProfile(
  workers: readonly WorkerState[],
  report: ClassReport | undefined
): WorkerGroup[] {
  const budgetsById = new Map(
    (report?.workers ?? []).map(load => [load.worker_id, load.budgets])
  );
  const byKey = new Map<string, NonEmpty<WorkerState>>();
  for (const worker of workers) {
    const key = profileKey(worker.profile);
    const members = byKey.get(key);
    byKey.set(key, members === undefined ? [worker] : [...members, worker]);
  }
  return [...byKey.entries()]
    .map(([key, members]) => ({
      key,
      profile: members[0].profile,
      workers: members,
      load: groupLoad(members, budgetsById),
    }))
    .sort((left, right) => compareProfiles(left.profile, right.profile));
}

/** `1 worker`, `3 workers`. */
export function countText(count: number, one: string, many: string): string {
  return `${count} ${count === 1 ? one : many}`;
}

/** The capacity of a budget with its unit: an `ISOLATED` budget counts
 * children, and a shared budget counts seats. */
export function capacityText(budget: ExecutionClass, capacity: number): string {
  switch (budget) {
    case 'ISOLATED':
      return countText(capacity, 'child', 'children');
    case 'SHARED_ASYNC':
    case 'SHARED_THREAD':
      return countText(capacity, 'seat', 'seats');
    default:
      return assertNever(budget, 'execution class');
  }
}

/** A shape in words: `12 children per worker · ISOLATED budget`. */
export function shapeText(shape: WorkerShape): string {
  const budgets =
    shape.classes.length === 0
      ? 'no budget'
      : `${shape.classes.join(' + ')} ${shape.classes.length === 1 ? 'budget' : 'budgets'}`;
  return `${countText(shape.children, 'child', 'children')} per worker · ${budgets}`;
}

/** The seats in service of a load: the sum of its budget capacities. One
 * isolated child is one seat. */
export function seatsInService(load: KnownLoad): number {
  return load.budgets.reduce((sum, budget) => sum + budget.capacity, 0);
}

/** The down slots of the workers of a group. */
export function downSlotCount(group: WorkerGroup): number {
  return group.workers.reduce(
    (sum, worker) => sum + worker.down_slots.length,
    0
  );
}

/**
 * The rows in flight that no capacity in service holds. Above 0 when a
 * worker holds more rows than its capacity: with a prefetch buffer, or after
 * a slot went down or a shared child started to close.
 */
export function rowsAboveCapacity(budget: GroupBudget): number {
  return Math.max(0, budget.inFlight + budget.free - budget.capacity);
}

/** The part of the capacity that holds a row, from 0 to 1. A budget with no
 * capacity in service is 0. */
export function usedFraction(budget: GroupBudget): number {
  if (budget.capacity <= 0) {
    return 0;
  }
  return Math.min(1, Math.max(0, budget.inFlight / budget.capacity));
}
