import { statusColorVar, statusLabel } from '@/lib/status-utils';
import type { WorkflowStatus } from '@/types/workflows';

import { LEGEND_STATUSES } from './status';

interface LegendProps {
  /** Statuses currently used as a visibility filter (empty = show all). */
  active: ReadonlySet<string>;
  onToggle: (status: WorkflowStatus) => void;
}

/** Clickable status key: each chip toggles a filter that fades nodes whose
 * status is not selected. Also documents the dashed-border convention. */
export function Legend({ active, onToggle }: LegendProps) {
  const filtering = active.size > 0;
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
      {LEGEND_STATUSES.map(status => {
        const on = active.has(status);
        return (
          <button
            key={status}
            type="button"
            onClick={() => onToggle(status)}
            aria-pressed={on}
            title={
              on
                ? `Showing ${statusLabel(status)} — click to clear`
                : `Show only ${statusLabel(status)}`
            }
            className="inline-flex items-center gap-1.5 rounded px-1 hover:text-foreground"
            style={{ opacity: filtering && !on ? 0.4 : 1 }}
          >
            <span
              className="size-2 rounded-full"
              style={{ background: statusColorVar(status) }}
              aria-hidden
            />
            {statusLabel(status)}
          </button>
        );
      })}
      <span className="border-l border-border pl-3">dashed = tolerant join</span>
    </div>
  );
}
