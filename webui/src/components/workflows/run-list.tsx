import { useState } from 'react';

import { StatusChip } from '@/components/ui/status-chip';
import { useWorkflowNames, useWorkflowRuns } from '@/hooks/use-workflow-runs';
import { formatElapsed } from '@/lib/format-duration';
import { cn } from '@/lib/utils';
import type { WorkflowRunSummary } from '@/types/workflows';

import { ErrorState } from '@/components/monitoring/states';
import { RUN_STATUS_FILTERS } from './status';

interface RunListProps {
  selectedRunId: string | null;
  onSelect: (run: WorkflowRunSummary) => void;
}

/** Left-rail run picker: filter by workflow name and status, newest-first. */
export function RunList({ selectedRunId, onSelect }: RunListProps) {
  const [name, setName] = useState<string>('');
  const [status, setStatus] = useState<string>('');
  const { names } = useWorkflowNames();
  const { runs, isLoading, isError } = useWorkflowRuns(
    name === '' ? null : name,
    status === '' ? null : status
  );

  return (
    <div className="flex h-full flex-col border-r border-border">
      <div className="flex flex-col gap-3 border-b border-border p-3">
        <div>
          <label
            htmlFor="workflow-name-filter"
            className="mb-1.5 block text-xs font-medium text-muted-foreground"
          >
            Workflow
          </label>
          <select
            id="workflow-name-filter"
            value={name}
            onChange={event => setName(event.target.value)}
            className="w-full rounded-md border border-input bg-glass-field px-2.5 py-1.5 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-ring"
          >
            <option value="">All workflows</option>
            {names.map(candidate => (
              <option key={candidate} value={candidate}>
                {candidate}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label
            htmlFor="workflow-status-filter"
            className="mb-1.5 block text-xs font-medium text-muted-foreground"
          >
            Status
          </label>
          <select
            id="workflow-status-filter"
            value={status}
            onChange={event => setStatus(event.target.value)}
            className="w-full rounded-md border border-input bg-glass-field px-2.5 py-1.5 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-ring"
          >
            <option value="">Any status</option>
            {RUN_STATUS_FILTERS.map(candidate => (
              <option key={candidate} value={candidate}>
                {candidate.toLowerCase()}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {isError && runs.length === 0 ? (
          <div className="p-3">
            <ErrorState compact message="Could not load runs." />
          </div>
        ) : isLoading && runs.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">Loading runs…</p>
        ) : runs.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">No runs found.</p>
        ) : (
          <ul>
            {runs.map(run => (
              <li key={run.id}>
                <button
                  type="button"
                  onClick={() => onSelect(run)}
                  className={cn(
                    'flex w-full flex-col gap-1.5 border-b border-border px-3 py-2.5 text-left transition-colors hover:bg-glass-surface-strong',
                    run.id === selectedRunId && 'bg-accent-surface'
                  )}
                >
                  <span className="truncate text-sm font-medium" title={run.name}>
                    {run.name}
                  </span>
                  <div className="flex items-center justify-between gap-2">
                    <StatusChip status={run.status} />
                    <span className="font-mono text-xs text-muted-foreground">
                      {formatElapsed(run.wall_s)}
                    </span>
                  </div>
                  <span className="truncate font-mono text-10 text-muted-foreground">
                    {run.definition_key
                      ? `${run.definition_key} · ${run.id.slice(0, 8)}`
                      : run.id.slice(0, 8)}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
