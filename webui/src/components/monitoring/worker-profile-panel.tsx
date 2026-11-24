import type { ReactNode } from 'react';

import { assertNever } from '@/lib/exhaustive';
import { statusColorVar } from '@/lib/status-utils';
import type { SnapshotProfile } from '@/types/workers';

import {
  capacityText,
  countText,
  downSlotCount,
  rowsAboveCapacity,
  seatsInService,
  shapeText,
  usedFraction,
  type GroupBudget,
  type KnownLoad,
  type WorkerGroup,
} from './worker-profiles';

/** Whether the class report, which holds the budgets of the live workers,
 * was read. */
export type ClassReportRead = 'loading' | 'failed' | 'loaded';

/** `A`, `A or B`, `A, B or C`. */
function orList(items: readonly string[]): string {
  const last = items[items.length - 1];
  if (last === undefined) {
    return '';
  }
  const before = items.slice(0, -1);
  return before.length === 0 ? last : `${before.join(', ')} or ${last}`;
}

/** The name of a group for a screen reader: `Profile default`. */
export function profileLabel(profile: SnapshotProfile): string {
  switch (profile.kind) {
    case 'named':
      return `Profile ${profile.name}`;
    case 'not_recorded':
      return 'Profile not recorded';
    default:
      return assertNever(profile, 'snapshot profile');
  }
}

/** The title of a group: the profile name as the app config writes it, or
 * the words that say that the snapshots hold no name. A name has no space
 * and up to 64 characters, so it can break at any character. */
function ProfileTitle({ profile }: { profile: SnapshotProfile }) {
  switch (profile.kind) {
    case 'named':
      return (
        <span className="min-w-0 break-all font-mono text-sm font-medium">
          {profile.name}
        </span>
      );
    case 'not_recorded':
      return <span className="text-sm font-medium">Profile not recorded</span>;
    default:
      return assertNever(profile, 'snapshot profile');
  }
}

/** Why the snapshots of a group hold no profile name. A named profile needs
 * no note. */
function ProfileNote({ profile }: { profile: SnapshotProfile }) {
  switch (profile.kind) {
    case 'named':
      return null;
    case 'not_recorded':
      return (
        <p className="text-11 text-muted-foreground">
          A worker of a build before schema migration 1004 wrote these
          snapshots, so they hold no profile name.
        </p>
      );
    default:
      return assertNever(profile, 'snapshot profile');
  }
}

/**
 * One budget of a group. First line: the class of the budget, a meter of the
 * capacity in service that holds a row, the rows in flight and the free
 * capacity. Second line: the capacity with its unit, the classes that the
 * budget claims, and the rows above the capacity when there are some.
 *
 * The meter column has the same width in each row and in each group, so the
 * bars of two budgets compare. On a narrow screen the meter is on a line of
 * its own, below the two texts. The fill has the color of the RUNNING status:
 * the rows in flight are the RUNNING rows and the CLAIMED rows with a live
 * lease.
 */
function BudgetMeterRow({ budget }: { budget: GroupBudget }) {
  const capacity = capacityText(budget.budget, budget.capacity);
  const above = rowsAboveCapacity(budget);
  return (
    <li className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-1 sm:grid-cols-[7.5rem_minmax(3rem,1fr)_10.5rem]">
      <span className="order-1 font-mono text-xs sm:order-none">
        {budget.budget}
      </span>
      <div
        role="meter"
        aria-label={`${budget.budget} budget`}
        aria-valuemin={0}
        aria-valuemax={budget.capacity}
        aria-valuenow={Math.min(budget.inFlight, budget.capacity)}
        aria-valuetext={`${budget.inFlight} in flight of ${capacity} in service`}
        className="order-3 col-span-2 h-2 overflow-hidden rounded-full bg-muted sm:order-none sm:col-span-1"
      >
        <div
          className="h-full rounded-full"
          style={{
            width: `${usedFraction(budget) * 100}%`,
            background: statusColorVar('RUNNING'),
          }}
        />
      </div>
      <span
        className="order-2 whitespace-nowrap text-right text-xs tabular-nums sm:order-none"
        title="Counted at the read of the execution class report. The running and claimed counts of a worker are from its last snapshot."
      >
        <span className="font-semibold">{budget.inFlight}</span> in flight ·{' '}
        <span className="font-semibold">{budget.free}</span> free
      </span>
      <span className="order-4 col-span-2 text-11 text-muted-foreground sm:order-none sm:col-span-3">
        <span
          style={
            budget.capacity === 0 ? { color: 'var(--warning-dark)' } : undefined
          }
        >
          {capacity} in service
        </span>
        {' · serves '}
        <span className="font-mono text-10">{budget.serves.join(', ')}</span>
        {above > 0 && (
          <span
            style={{ color: 'var(--warning-dark)' }}
            title="A worker holds more rows than its capacity in service: it has a prefetch buffer, or a slot went down or a shared child closes."
          >
            {' · '}
            {countText(above, 'row', 'rows')} above capacity
          </span>
        )}
      </span>
    </li>
  );
}

/** The line above a list of more than one shape. The workers of one named
 * profile with more than one shape do not run the same definition of the
 * profile, so the line has the warning color. Workers with no profile name
 * can be of more than one profile, so more than one shape says nothing. */
function ShapeCountLine({
  profile,
  shapes,
}: {
  profile: SnapshotProfile;
  shapes: number;
}) {
  switch (profile.kind) {
    case 'named':
      return (
        <span
          style={{ color: 'var(--warning-dark)' }}
          title="The workers of this profile do not have one shape: they do not run the same definition of the profile."
        >
          {shapes} shapes in one profile
        </span>
      );
    case 'not_recorded':
      return <span>{shapes} shapes</span>;
    default:
      return assertNever(profile, 'snapshot profile');
  }
}

/**
 * What the profile gives its workers. One shape is one line. More than one
 * shape is a list with the number of workers of each.
 */
function ShapeLines({
  profile,
  load,
}: {
  profile: SnapshotProfile;
  load: KnownLoad;
}) {
  const [first, ...others] = load.shapes;
  if (others.length === 0) {
    return (
      <p className="text-11 text-muted-foreground">{shapeText(first.shape)}</p>
    );
  }
  return (
    <div className="flex flex-col gap-0.5 text-11 text-muted-foreground">
      <ShapeCountLine profile={profile} shapes={load.shapes.length} />
      <ul className="flex flex-col gap-0.5">
        {load.shapes.map(entry => (
          <li key={shapeText(entry.shape)}>
            {shapeText(entry.shape)} ·{' '}
            {countText(entry.workers, 'worker', 'workers')}
          </li>
        ))}
      </ul>
    </div>
  );
}

/** The body of a group whose budgets are known: its shapes, the load of each
 * budget summed over its workers, the classes that its workers do not claim,
 * and the workers that the class report does not hold. */
function KnownLoadBody({
  profile,
  load,
}: {
  profile: SnapshotProfile;
  load: KnownLoad;
}) {
  return (
    <>
      <ShapeLines profile={profile} load={load} />
      {load.budgets.length > 0 && (
        <ul className="flex flex-col gap-3">
          {load.budgets.map(budget => (
            <BudgetMeterRow key={budget.budget} budget={budget} />
          ))}
        </ul>
      )}
      {load.notServed.length > 0 && (
        <p className="text-11 text-muted-foreground">
          Claims no{' '}
          <span className="font-mono text-10">{orList(load.notServed)}</span>{' '}
          rows: these workers run no children that can run them.
        </p>
      )}
      {load.withoutBudgets > 0 && (
        <p className="text-11 text-muted-foreground">
          {countText(load.withoutBudgets, 'worker has', 'workers have')} no
          budgets in the execution class report, so the sums leave{' '}
          {load.withoutBudgets === 1 ? 'it' : 'them'} out.
        </p>
      )}
    </>
  );
}

function unknownLoadText(reportRead: ClassReportRead): string {
  switch (reportRead) {
    case 'loading':
      return 'Reading the budgets of these workers…';
    case 'failed':
      return 'The execution class report could not be read, so the budgets of these workers are not known.';
    case 'loaded':
      return 'The execution class report has no live snapshot of these workers.';
    default:
      return assertNever(reportRead, 'class report read');
  }
}

function GroupBody({
  group,
  reportRead,
}: {
  group: WorkerGroup;
  reportRead: ClassReportRead;
}) {
  switch (group.load.kind) {
    case 'known':
      return <KnownLoadBody profile={group.profile} load={group.load} />;
    case 'unknown':
      return (
        <p className="text-11 text-muted-foreground">
          {unknownLoadText(reportRead)}
        </p>
      );
    default:
      return assertNever(group.load, 'group load');
  }
}

/** The counts at the end of the header of a group: its workers, and its seats
 * in service when its budgets are known. */
function groupCounts(group: WorkerGroup): string {
  const workers = countText(group.workers.length, 'worker', 'workers');
  switch (group.load.kind) {
    case 'known':
      return `${workers} · ${countText(seatsInService(group.load), 'seat', 'seats')} in service`;
    case 'unknown':
      return workers;
    default:
      return assertNever(group.load, 'group load');
  }
}

/**
 * The live workers of one profile as a flat group below a hairline: the
 * profile name and the counts in the header, then what the profile gives a
 * worker and the load of each budget summed over the workers, then the
 * workers (`children`). Only the worker cards are boxes.
 */
export function WorkerProfilePanel({
  group,
  reportRead,
  children,
}: {
  group: WorkerGroup;
  reportRead: ClassReportRead;
  /** The cards of the workers of the group. */
  children: ReactNode;
}) {
  const down = downSlotCount(group);
  return (
    <div
      role="group"
      aria-label={profileLabel(group.profile)}
      className="flex flex-col gap-3 border-t border-hairline pt-3"
    >
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
        <ProfileTitle profile={group.profile} />
        <span className="text-11 tabular-nums text-muted-foreground">
          {groupCounts(group)}
          {down > 0 && (
            <span style={{ color: 'var(--warning-dark)' }}>
              {' · '}
              {countText(down, 'slot', 'slots')} down
            </span>
          )}
        </span>
      </div>
      <div className="flex flex-col gap-3">
        <ProfileNote profile={group.profile} />
        <GroupBody group={group} reportRead={reportRead} />
      </div>
      {children}
    </div>
  );
}
