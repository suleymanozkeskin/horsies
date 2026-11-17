import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  statusColorVar,
  statusLabel,
  TASK_STATUS_ORDER,
  type LifecycleStatus,
} from '@/lib/status-utils';
import { cn } from '@/lib/utils';
import type {
  Breakdown,
  BreakdownCounts,
  BreakdownGroup,
  GroupBy,
} from '@/types/tasks';

/** The count field each status occupies on a group row. Keyed by the closed
 * status union, so a new status cannot be added without a column for it. */
const STATUS_FIELD: Record<LifecycleStatus, keyof BreakdownCounts> = {
  PENDING: 'pending',
  CLAIMED: 'claimed',
  RUNNING: 'running',
  COMPLETED: 'completed',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
  EXPIRED: 'expired',
};

const STATUS_COLS: {
  key: keyof BreakdownCounts;
  label: string;
  status: string;
}[] =
  TASK_STATUS_ORDER.map(status => ({
    key: STATUS_FIELD[status],
    label: statusLabel(status),
    status,
  }));

const GROUP_HEADING: Record<GroupBy, string> = {
  worker: 'Worker',
  task_name: 'Task',
  queue: 'Queue',
};

/** A single count cell — dim when zero so the eye lands on non-empty buckets. */
function CountCell({ value, color }: { value: number; color?: string }) {
  return (
    <TableCell className="text-right tabular-nums">
      <span
        className={cn(value === 0 && 'text-muted-foreground')}
        style={value > 0 && color ? { color } : undefined}
      >
        {value}
      </span>
    </TableCell>
  );
}

/** The text a group row shows. The unclaimed group never reads as a worker id. */
function groupLabel(group: BreakdownGroup): string {
  switch (group.kind) {
    case 'value':
      return group.value;
    case 'unclaimed':
      return 'unclaimed';
  }
}

/** The value a click on the group drills into: only a value group names one. */
function groupDrillValue(group: BreakdownGroup): string | undefined {
  switch (group.kind) {
    case 'value':
      return group.value;
    case 'unclaimed':
      return undefined;
  }
}

/** A React key that keeps the unclaimed group apart from a value 'unknown'. */
function groupKey(group: BreakdownGroup): string {
  switch (group.kind) {
    case 'value':
      return `value:${group.value}`;
    case 'unclaimed':
      return 'unclaimed';
  }
}

function Row({
  label,
  counts,
  onClick,
  isTotal,
}: {
  label: string;
  counts: BreakdownCounts;
  onClick?: () => void;
  isTotal?: boolean;
}) {
  return (
    <TableRow
      className={cn(
        isTotal && 'border-t-2 font-medium',
        onClick !== undefined && 'cursor-pointer'
      )}
      onClick={onClick}
    >
      <TableCell className={cn('font-mono text-xs', isTotal && 'font-semibold')}>
        {label}
      </TableCell>
      <TableCell className="text-right font-semibold tabular-nums">
        {counts.total}
      </TableCell>
      {STATUS_COLS.map(col => (
        <CountCell
          key={col.key}
          value={counts[col.key]}
          color={statusColorVar(col.status)}
        />
      ))}
      <CountCell value={counts.retried} />
    </TableRow>
  );
}

/** Per-group status pivot with a TOTAL footer. Clicking a group drills into the
 * flat list filtered by that group. */
export function TaskBreakdown({
  breakdown,
  isLoading,
  onDrill,
}: {
  breakdown: Breakdown | undefined;
  isLoading: boolean;
  onDrill: (group: string) => void;
}) {
  if (isLoading && !breakdown) {
    return (
      <p className="glass rounded-xl border border-border p-4 text-sm text-muted-foreground">
        Loading breakdown…
      </p>
    );
  }
  if (!breakdown || breakdown.groups.length === 0) {
    return (
      <p className="glass rounded-xl border border-border p-4 text-sm text-muted-foreground">
        No tasks found.
      </p>
    );
  }

  const heading = GROUP_HEADING[breakdown.group_by];
  const truncated = breakdown.group_count > breakdown.groups.length;

  return (
    <div className="glass overflow-hidden rounded-xl border border-border">
      {truncated && (
        <div className="border-b border-border px-3 py-2 text-xs text-muted-foreground">
          Showing top {breakdown.groups.length} of {breakdown.group_count} groups
          by task count.
        </div>
      )}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{heading}</TableHead>
            <TableHead className="text-right">total</TableHead>
            {STATUS_COLS.map(col => (
              <TableHead key={col.key} className="text-right">
                {col.label}
              </TableHead>
            ))}
            <TableHead className="text-right">retried</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {breakdown.groups.map(row => {
            const key = groupKey(row.group);
            const label = groupLabel(row.group);
            const value = groupDrillValue(row.group);
            // Only a group with a drill value gets a click handler; the prop
            // is left out, not set to undefined (exactOptionalPropertyTypes).
            return value === undefined ? (
              <Row key={key} label={label} counts={row} />
            ) : (
              <Row
                key={key}
                label={label}
                counts={row}
                onClick={() => onDrill(value)}
              />
            );
          })}
        </TableBody>
        <TableFooter>
          <Row label="TOTAL" counts={breakdown.total} isTotal />
        </TableFooter>
      </Table>
    </div>
  );
}
