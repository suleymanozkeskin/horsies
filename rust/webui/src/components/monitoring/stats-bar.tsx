import { useTaskStats } from '@/hooks/use-tasks';
import {
  statusColorVar,
  statusLabel,
  TASK_STATUS_ORDER,
} from '@/lib/status-utils';
import { cn } from '@/lib/utils';
import type { TaskFilters } from '@/types/tasks';

import { ErrorState } from './states';

/** The strip's shape before its first counts arrive: one inert card per
 * lifecycle status, sized like the real ones. The row is always these seven, so
 * holding its height costs nothing and spares the page a reflow when the counts
 * land. */
function StatsBarPlaceholder() {
  return (
    <div className="flex flex-wrap gap-2" aria-hidden>
      {TASK_STATUS_ORDER.map(status => (
        <div
          key={status}
          className="flex items-center gap-2 rounded-lg border border-border px-3 py-2 opacity-50"
          style={{
            background: `color-mix(in oklab, ${statusColorVar(status)} 6%, var(--card))`,
          }}
        >
          <span
            className="size-2.5 rounded-full"
            style={{ background: statusColorVar(status) }}
          />
          <span className="text-sm font-semibold tabular-nums">—</span>
          <span className="text-xs text-muted-foreground">
            {statusLabel(status)}
          </span>
        </div>
      ))}
    </div>
  );
}

/**
 * Clickable status overview cards — each toggles membership in the (multi-
 * select) status filter. Counts are scoped by every other active filter so they
 * stay coherent with the table.
 *
 * The strip occupies its row unconditionally: the filters are part of the query
 * key, so every filter change starts a new query, and a strip that rendered
 * nothing until counts arrived would evict its own row from the page on each
 * one. `useTaskStats` supplies the previous scope's counts in the meantime.
 */
export function StatsBar({
  filters,
  onToggle,
}: {
  filters: TaskFilters;
  onToggle: (status: string) => void;
}) {
  const { stats, isError, refetch } = useTaskStats(filters);
  // Report the failure only when it leaves nothing on screen: a poll that fails
  // after a good one must not blank counts the operator is already reading.
  if (isError && stats.length === 0) {
    return (
      <ErrorState compact message="Could not load status counts." onRetry={refetch} />
    );
  }
  if (stats.length === 0) {
    return <StatsBarPlaceholder />;
  }
  const active = new Set(filters.status ?? []);
  return (
    <div className="flex flex-wrap gap-2">
      {stats.map(stat => {
        const isActive = active.has(stat.status);
        return (
          <button
            key={stat.status}
            type="button"
            aria-pressed={isActive}
            onClick={() => onToggle(stat.status)}
            className={cn(
              'flex items-center gap-2 rounded-lg border px-3 py-2 transition-colors',
              isActive
                ? 'border-primary ring-1 ring-primary'
                : 'border-border hover:bg-muted'
            )}
            style={{
              background: `color-mix(in oklab, ${statusColorVar(stat.status)} 6%, var(--card))`,
            }}
          >
            <span
              className="size-2.5 rounded-full"
              style={{ background: statusColorVar(stat.status) }}
              aria-hidden
            />
            <span className="text-sm font-semibold tabular-nums">{stat.count}</span>
            <span className="text-xs text-muted-foreground">
              {statusLabel(stat.status)}
            </span>
          </button>
        );
      })}
    </div>
  );
}
