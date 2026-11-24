import { statusColorVar, statusLabel } from '@/lib/status-utils';
import type { WorkflowStatus } from '@/types/workflows';

import { PHASE_ORDER, phaseFill, phaseLabel, phaseMeaning } from './phase';
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

/** Read-only phase key of the timeline and the graph phase strip. Each
 * swatch states what its phase means; a pause is a band, not a phase. */
export function PhaseLegend() {
  return (
    <ul
      aria-label="Phase legend"
      className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground"
    >
      {PHASE_ORDER.map(key => (
        <li
          key={key}
          className="inline-flex items-center gap-1.5"
          title={phaseMeaning(key)}
        >
          <span
            className="inline-block h-2 w-3 rounded-sm"
            style={{ background: phaseFill(key) }}
            aria-hidden
          />
          <span>{phaseLabel(key)}</span>
          <span className="sr-only">: {phaseMeaning(key)}</span>
        </li>
      ))}
      <li className="inline-flex items-center gap-1.5" title="The run was paused.">
        <span
          className="inline-block h-2 w-3 rounded-sm border border-border"
          style={{ background: 'var(--pause-band)' }}
          aria-hidden
        />
        <span>paused</span>
        <span className="sr-only">: the run was paused (a band over every row).</span>
      </li>
      <li
        className="inline-flex items-center gap-1.5"
        title="The node reached its terminal status: a marker, not a duration."
      >
        <span
          className="inline-block size-2 rotate-45 border border-foreground"
          style={{ background: 'var(--card)' }}
          aria-hidden
        />
        <span>end</span>
        <span className="sr-only">
          : the node reached its terminal status (a marker, not a duration).
        </span>
      </li>
    </ul>
  );
}
