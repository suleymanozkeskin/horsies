import { useMemo, useState } from 'react';

import { ArrowDown, ArrowUp, ChevronsUpDown } from 'lucide-react';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useSchedules } from '@/hooks/use-workers';
import { assertNever } from '@/lib/exhaustive';
import { formatRelative, formatTime } from '@/lib/format-duration';
import { cn } from '@/lib/utils';
import type { ScheduleState } from '@/types/workers';

import {
  ariaSortOf,
  DEFAULT_SCHEDULE_SORT,
  SCHEDULE_SORT_COLUMNS,
  sortAfterClick,
  sortSchedules,
  type ScheduleSort,
  type ScheduleSortColumn,
  type SortDirection,
} from './schedule-sort';
import { ErrorState } from './states';

const COLUMN_LABEL: Record<ScheduleSortColumn, string> = {
  schedule_name: 'Schedule',
  last_run_at: 'Last run',
  next_run_at: 'Next run',
  run_count: 'Runs',
};

/** Which side of its cell the text of a column is on. A column of numbers is
 * on the end side, so the digits are in line. */
type ColumnSide = 'start' | 'end';

const COLUMN_SIDE: Record<ScheduleSortColumn, ColumnSide> = {
  schedule_name: 'start',
  last_run_at: 'start',
  next_run_at: 'start',
  run_count: 'end',
};

/** The arrow of a header: up or down on the sorted column, and a faint double
 * arrow on each other column, which shows that it can be sorted. */
function SortArrow({ state }: { state: SortDirection | 'none' }) {
  switch (state) {
    case 'ascending':
      return <ArrowUp className="size-3.5 shrink-0" aria-hidden />;
    case 'descending':
      return <ArrowDown className="size-3.5 shrink-0" aria-hidden />;
    case 'none':
      return (
        <ChevronsUpDown className="size-3.5 shrink-0 opacity-50" aria-hidden />
      );
    default:
      return assertNever(state, 'sort state of a header');
  }
}

/** A column header that sorts the table on a click, and on Enter or Space
 * when it has the focus. `aria-sort` on the cell gives the state to a screen
 * reader. */
function SortableHead({
  column,
  sort,
  onSort,
}: {
  column: ScheduleSortColumn;
  sort: ScheduleSort;
  onSort: (column: ScheduleSortColumn) => void;
}) {
  const state = ariaSortOf(sort, column);
  const label = COLUMN_LABEL[column];
  const side = COLUMN_SIDE[column];
  return (
    <TableHead
      aria-sort={state}
      className={side === 'end' ? 'text-right' : undefined}
    >
      <button
        type="button"
        onClick={() => onSort(column)}
        title={`Sort by ${label.toLowerCase()}, ${sortAfterClick(sort, column).direction}`}
        className={cn(
          '-mx-1.5 inline-flex h-8 items-center gap-1 whitespace-nowrap rounded-sm px-1.5 font-medium transition-colors hover:text-foreground focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring',
          // On an end column the arrow is before the label, so the label
          // stays in line with the numbers below it.
          side === 'end' && 'flex-row-reverse',
          state !== 'none' && 'text-foreground'
        )}
      >
        {label}
        <SortArrow state={state} />
      </button>
    </TableHead>
  );
}

/** The schedule rows in the order of `sort`, below a sortable header. */
export function ScheduleTableView({
  schedules,
  sort,
  onSort,
}: {
  schedules: readonly ScheduleState[];
  sort: ScheduleSort;
  onSort: (column: ScheduleSortColumn) => void;
}) {
  const sorted = useMemo(() => sortSchedules(schedules, sort), [schedules, sort]);
  return (
    <Table>
      <TableHeader>
        <TableRow>
          {SCHEDULE_SORT_COLUMNS.map(column => (
            <SortableHead
              key={column}
              column={column}
              sort={sort}
              onSort={onSort}
            />
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {sorted.map(schedule => (
          <TableRow key={schedule.schedule_name}>
            <TableCell className="font-mono text-xs">
              {schedule.schedule_name}
            </TableCell>
            {schedule.last_run_at === null ? (
              // No last run is a state of the schedule (its run count is 0),
              // not a missing value.
              <TableCell
                className="text-muted-foreground"
                title="This schedule has not run."
              >
                never
              </TableCell>
            ) : (
              <TableCell
                className="text-muted-foreground"
                title={formatTime(schedule.last_run_at)}
              >
                {formatRelative(schedule.last_run_at)}
              </TableCell>
            )}
            <TableCell title={formatTime(schedule.next_run_at)}>
              {formatRelative(schedule.next_run_at)}
            </TableCell>
            <TableCell className="text-right tabular-nums">
              {schedule.run_count}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

/** What the schedule panel shows below its title. */
type SchedulePanelState =
  | { readonly kind: 'failed' }
  | { readonly kind: 'loading' }
  | { readonly kind: 'none_registered' }
  | { readonly kind: 'listed'; readonly schedules: readonly ScheduleState[] };

/** The state of the panel from the schedule read. A failed read is `failed`
 * also when an earlier read left rows. */
export function schedulePanelState(read: {
  schedules: readonly ScheduleState[];
  isLoading: boolean;
  isError: boolean;
}): SchedulePanelState {
  if (read.isError) {
    return { kind: 'failed' };
  }
  if (read.schedules.length > 0) {
    return { kind: 'listed', schedules: read.schedules };
  }
  return read.isLoading ? { kind: 'loading' } : { kind: 'none_registered' };
}

/** The schedule panel: each registered schedule with its last run, its next
 * run and its run count. A click on a column header sorts by that column.
 * The sort stays while the table polls. */
export function SchedulesTable() {
  const state = schedulePanelState(useSchedules());
  const [sort, setSort] = useState<ScheduleSort>(DEFAULT_SCHEDULE_SORT);
  return (
    <div className="glass overflow-hidden rounded-xl border border-border">
      <div className="border-b border-border px-4 py-3 text-sm font-medium">
        Schedules
      </div>
      <SchedulePanelBody
        state={state}
        sort={sort}
        onSort={column => setSort(current => sortAfterClick(current, column))}
      />
    </div>
  );
}

function SchedulePanelBody({
  state,
  sort,
  onSort,
}: {
  state: SchedulePanelState;
  sort: ScheduleSort;
  onSort: (column: ScheduleSortColumn) => void;
}) {
  switch (state.kind) {
    case 'failed':
      return (
        <div className="p-4">
          <ErrorState compact message="Could not load schedules." />
        </div>
      );
    case 'loading':
      return (
        <p className="p-4 text-sm text-muted-foreground">Loading schedules…</p>
      );
    case 'none_registered':
      return (
        <p className="p-4 text-sm text-muted-foreground">
          No schedules registered.
        </p>
      );
    case 'listed':
      return (
        <>
          {/* The sort in words, which a screen reader says when it changes. */}
          <span className="sr-only" aria-live="polite">
            Sorted by {COLUMN_LABEL[sort.column].toLowerCase()}, {sort.direction}.
          </span>
          <ScheduleTableView
            schedules={state.schedules}
            sort={sort}
            onSort={onSort}
          />
        </>
      );
    default:
      return assertNever(state, 'schedule panel state');
  }
}
