import { useMemo, useState } from 'react';

import {
  SelectCombobox,
  type SelectOption,
} from '@/components/ui/select-combobox';
import { StatusChip } from '@/components/ui/status-chip';
import { useWorkflowNames, useWorkflowRuns } from '@/hooks/use-workflow-runs';
import { formatElapsed } from '@/lib/format-duration';
import { statusColorVar } from '@/lib/status-utils';
import { cn } from '@/lib/utils';
import type { WorkflowRunSummary } from '@/types/workflows';

import { ErrorState } from '@/components/monitoring/states';
import { RUN_STATUS_FILTERS } from './status';

const STATUS_OPTIONS: SelectOption[] = RUN_STATUS_FILTERS.map(status => ({
  value: status,
  label: status.toLowerCase(),
  adornment: (
    <span
      className="size-2 shrink-0 rounded-full"
      style={{ background: statusColorVar(status) }}
      aria-hidden
    />
  ),
}));

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

  const nameOptions = useMemo<SelectOption[]>(
    () => names.map(candidate => ({ value: candidate })),
    [names]
  );

  return (
    <div className="flex h-full flex-col border-r border-hairline">
      <div className="flex flex-col gap-3 border-b border-hairline pr-4 pb-3">
        <SelectCombobox
          id="workflow-name-filter"
          label="Workflow"
          emptyLabel="All workflows"
          placeholder="Search workflows…"
          options={nameOptions}
          value={name === '' ? null : name}
          onChange={next => setName(next ?? '')}
        />
        <SelectCombobox
          id="workflow-status-filter"
          label="Status"
          emptyLabel="Any status"
          placeholder="Search statuses…"
          options={STATUS_OPTIONS}
          value={status === '' ? null : status}
          onChange={next => setStatus(next ?? '')}
        />
      </div>

      <div className="flex-1 overflow-y-auto pt-2 pr-4">
        {isError && runs.length === 0 ? (
          <div className="py-1">
            <ErrorState compact message="Could not load runs." />
          </div>
        ) : isLoading && runs.length === 0 ? (
          <p className="px-3 py-2 text-sm text-muted-foreground">Loading runs…</p>
        ) : runs.length === 0 ? (
          <p className="px-3 py-2 text-sm text-muted-foreground">No runs found.</p>
        ) : (
          <ul className="flex flex-col gap-0.5 pb-2">
            {runs.map(run => (
              <li key={run.id}>
                <button
                  type="button"
                  onClick={() => onSelect(run)}
                  className={cn(
                    'flex w-full flex-col gap-1.5 rounded-lg px-3 py-2.5 text-left transition-colors',
                    run.id === selectedRunId
                      ? 'bg-accent-surface'
                      : 'hover:bg-surface-hover'
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
