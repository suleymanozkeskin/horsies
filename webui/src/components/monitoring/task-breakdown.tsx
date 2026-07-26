import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { statusColorVar } from '@/lib/status-utils';
import { cn } from '@/lib/utils';
import type { Breakdown, GroupBy, GroupRow } from '@/types/tasks';

const STATUS_COLS: { key: keyof GroupRow; label: string; status: string }[] = [
  { key: 'pending', label: 'pending', status: 'PENDING' },
  { key: 'claimed', label: 'claimed', status: 'CLAIMED' },
  { key: 'running', label: 'running', status: 'RUNNING' },
  { key: 'completed', label: 'completed', status: 'COMPLETED' },
  { key: 'failed', label: 'failed', status: 'FAILED' },
  { key: 'cancelled', label: 'cancelled', status: 'CANCELLED' },
  { key: 'expired', label: 'expired', status: 'EXPIRED' },
];

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

function Row({
  row,
  onDrill,
  isTotal,
}: {
  row: GroupRow;
  onDrill?: (group: string) => void;
  isTotal?: boolean;
}) {
  const clickable = !isTotal && row.group !== 'unknown' && onDrill !== undefined;
  return (
    <TableRow
      className={cn(
        isTotal && 'border-t-2 font-medium',
        clickable && 'cursor-pointer'
      )}
      onClick={clickable ? () => onDrill(row.group) : undefined}
    >
      <TableCell className={cn('font-mono text-xs', isTotal && 'font-semibold')}>
        {row.group}
      </TableCell>
      <TableCell className="text-right font-semibold tabular-nums">
        {row.total}
      </TableCell>
      {STATUS_COLS.map(col => (
        <CountCell
          key={col.key}
          value={row[col.key] as number}
          color={statusColorVar(col.status)}
        />
      ))}
      <CountCell value={row.retried} />
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
          {breakdown.groups.map(group => (
            <Row key={group.group} row={group} onDrill={onDrill} />
          ))}
        </TableBody>
        <TableFooter>
          <Row row={breakdown.total} isTotal />
        </TableFooter>
      </Table>
    </div>
  );
}
