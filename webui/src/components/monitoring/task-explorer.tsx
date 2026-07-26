import { useMemo } from 'react';

import { ChevronLeft, ChevronRight, X } from 'lucide-react';
import {
  ShadStackTable,
  useShadStackTable,
  type SST_ColumnDef,
  type SST_PaginationState,
  type SST_SortingState,
} from 'shadstack-table';

import { TaskActionBar } from '@/actions/action-bar';
import { useTaskActions } from '@/actions/use-task-actions';
import { StatusChip } from '@/components/ui/status-chip';
import { Button } from '@/components/ui/button';
import { Toggle } from '@/components/ui/toggle';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { useBreakdown, useFacets, useTasks, useTaskStats } from '@/hooks/use-tasks';
import { formatDuration, formatRelative, formatTime } from '@/lib/format-duration';
import { statusColorVar } from '@/lib/status-utils';
import { cn } from '@/lib/utils';
import {
  DEFAULT_DIR,
  DEFAULT_PAGE_SIZE,
  DEFAULT_SORT,
  filtersFromSearch,
  hasActiveFilters,
  PAGE_SIZES,
  type TaskSearch,
  type TaskSearchPatch,
  type TaskView,
} from '@/routes/search';
import type { ErrorCategory, TaskFilters, TaskSummary } from '@/types/tasks';

import { AttemptHistory, DetailRow } from './detail';
import { CategoryTag, ErrorChip } from './error-chip';
import { ERROR_CATEGORIES, errorCategoryColorVar } from './error-taxonomy';
import { FacetCombobox, type FacetOption } from './facet-combobox';
import { ErrorState } from './states';
import { TaskBreakdown } from './task-breakdown';

/** URL dimensions the filter controls write to. */
type ValueDimension = 'status' | 'task_name' | 'queue' | 'worker' | 'error_code';

export interface TaskExplorerProps {
  search: TaskSearch;
  /** Merge patch. A key set to `undefined` is removed from the URL. */
  onSearchChange: (patch: TaskSearchPatch) => void;
}

/** Clickable status overview cards — each toggles membership in the (multi-
 * select) status filter. Counts are scoped by every other active filter so they
 * stay coherent with the table. */
function StatsBar({
  filters,
  onToggle,
}: {
  filters: TaskFilters;
  onToggle: (status: string) => void;
}) {
  const { stats, isError, refetch } = useTaskStats(filters);
  if (isError) {
    return (
      <ErrorState compact message="Could not load status counts." onRetry={refetch} />
    );
  }
  if (stats.length === 0) {
    return null;
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
                ? 'border-accent-high ring-1 ring-accent-high'
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
              {stat.status.toLowerCase()}
            </span>
          </button>
        );
      })}
    </div>
  );
}

/** Error-code counts rolled up by taxonomy category. Counts every task carrying
 * an error code (including completed tasks that returned a domain error), so it
 * is deliberately NOT labelled "failures". */
function TaxonomyStrip({
  totals,
}: {
  totals: Partial<Record<ErrorCategory, number>>;
}) {
  const present = ERROR_CATEGORIES.filter(category => (totals[category] ?? 0) > 0);
  if (present.length === 0) {
    return null;
  }
  return (
    <div className="glass flex flex-wrap items-center gap-4 rounded-lg border border-border px-3 py-2">
      <span
        className="text-xs font-medium uppercase tracking-wide text-foreground"
        title="Tasks carrying an error code, grouped by family. Includes domain errors returned by completed tasks, so this is not the same as the failed count."
      >
        Error codes by category
      </span>
      {present.map(category => (
        <span key={category} className="flex items-center gap-1.5">
          <CategoryTag category={category} />
          <span className="font-mono text-11 tabular-nums text-muted-foreground">
            {totals[category]}
          </span>
        </span>
      ))}
    </div>
  );
}

function TaskDetailPanel({
  taskId,
  onClose,
}: {
  taskId: string;
  onClose: () => void;
}) {
  const view = useTaskActions(taskId, onClose);
  const { detail, isLoading, isError } = view;
  const leaf = detail?.leaf;
  return (
    <aside className="glass flex w-full shrink-0 flex-col overflow-hidden rounded-lg border border-border lg:w-[420px]">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <span className="truncate font-mono text-sm" title={detail?.task_name}>
          {detail?.task_name ?? 'Task'}
        </span>
        <button
          type="button"
          onClick={onClose}
          className="rounded p-1 text-muted-foreground hover:bg-muted"
          aria-label="Close detail"
        >
          <X className="size-4" />
        </button>
      </div>
      <div className="flex flex-col gap-4 overflow-y-auto p-4">
        {isError && detail === undefined ? (
          <ErrorState compact message="Could not load this task." />
        ) : !detail && isLoading ? (
          <p className="text-sm text-muted-foreground">Loading…</p>
        ) : !detail || !leaf ? (
          <p className="text-sm text-muted-foreground">Task not found.</p>
        ) : (
          <>
            <TaskActionBar view={view} />
            <DetailRow label="Status" value={<StatusChip status={leaf.status} />} />
            {leaf.error_code && (
              <DetailRow
                label="Error"
                value={
                  <ErrorChip
                    code={leaf.error_code}
                    category={detail.error_category}
                  />
                }
              />
            )}
            <div className="flex gap-8">
              <DetailRow label="Queue" value={detail.queue_name} />
              <DetailRow label="Priority" value={detail.priority} />
            </div>
            <div className="flex gap-8">
              <DetailRow label="Execution" value={formatDuration(leaf.exec_s)} />
              <DetailRow
                label="Queued"
                value={
                  <span title="Time waiting in the queue after dispatch">
                    {formatDuration(leaf.queue_s)}
                  </span>
                }
              />
            </div>
            <DetailRow
              label="Attempts"
              value={`${leaf.retry_count + 1} of ${leaf.max_retries + 1}`}
            />
            <DetailRow
              label="Worker"
              value={
                <span className="font-mono text-xs">
                  {leaf.worker_hostname ?? '—'}
                </span>
              }
            />
            <div className="flex gap-8">
              <DetailRow label="Enqueued" value={formatTime(leaf.enqueued_at)} />
              <DetailRow
                label={leaf.failed_at ? 'Failed' : 'Completed'}
                value={formatTime(leaf.failed_at ?? leaf.completed_at)}
              />
            </div>
            {leaf.good_until && (
              <DetailRow label="Good until" value={formatTime(leaf.good_until)} />
            )}
            {leaf.failed_reason && (
              <div>
                <span className="text-xs uppercase tracking-wide text-muted-foreground">
                  Failure
                </span>
                <pre className="glass mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-words rounded p-2 font-mono text-11 leading-snug">
                  {leaf.failed_reason}
                </pre>
              </div>
            )}
            <AttemptHistory attempts={detail.attempts} isLoading={isLoading} />
            <span className="font-mono text-10 text-muted-foreground">
              {leaf.task_id}
            </span>
          </>
        )}
      </div>
    </aside>
  );
}

/** Task monitor: status cards, scoped facet filters, a grouping pivot or a
 * server-paginated grid, plus a per-task detail panel. Filters, sort and paging
 * live in the URL, so a view is shareable and survives a reload. */
export function TaskExplorer({ search, onSearchChange }: TaskExplorerProps) {
  const view: TaskView = search.view ?? 'flat';
  const sortBy = search.sort ?? DEFAULT_SORT;
  const sortDir = search.dir ?? DEFAULT_DIR;
  const pageSize = search.size ?? DEFAULT_PAGE_SIZE;
  const pageIndex = (search.page ?? 1) - 1;
  const selectedId = search.task ?? null;

  const filters = useMemo(() => filtersFromSearch(search), [search]);

  const { facets } = useFacets({
    ...(search.status === undefined ? {} : { status: search.status }),
    ...(search.retried === undefined ? {} : { retried_only: search.retried }),
  });

  const { page, isLoading, isError, refetch } = useTasks({
    filters,
    sortBy,
    sortDir,
    offset: pageIndex * pageSize,
    limit: pageSize,
    enabled: view === 'flat',
  });
  const {
    breakdown,
    isLoading: breakdownLoading,
    isError: breakdownError,
    refetch: refetchBreakdown,
  } = useBreakdown(view === 'flat' ? 'worker' : view, filters, view !== 'flat');

  const rows = useMemo(() => page?.rows ?? [], [page]);
  const total = page?.total ?? 0;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));

  // Any filter or grouping change resets to the first page: the old offset
  // would point into a different result set.
  const setDimension = (key: ValueDimension, values: string[]): void => {
    const patch: TaskSearchPatch = { page: undefined };
    patch[key] = values.length === 0 ? undefined : values;
    onSearchChange(patch);
  };

  const toggleValue = (key: ValueDimension, value: string): void => {
    const current = new Set(search[key] ?? []);
    if (current.has(value)) {
      current.delete(value);
    } else {
      current.add(value);
    }
    setDimension(key, [...current]);
  };

  const clearFilters = (): void =>
    onSearchChange({
      status: undefined,
      task_name: undefined,
      queue: undefined,
      worker: undefined,
      error_code: undefined,
      retried: undefined,
      page: undefined,
    });

  const toOptions = (items: { value: string; count: number }[] = []): FacetOption[] =>
    items.map(item => ({ value: item.value, count: item.count }));

  const errorOptions: FacetOption[] = (facets?.error_codes ?? []).map(facet => ({
    value: facet.value,
    count: facet.count,
    adornment: (
      <span
        className="mr-1.5 size-2 shrink-0 rounded-full"
        style={{ background: errorCategoryColorVar(facet.category) }}
        aria-hidden
      />
    ),
  }));

  const columns = useMemo<SST_ColumnDef<TaskSummary>[]>(
    () => [
      {
        accessorKey: 'status',
        header: 'Status',
        Cell: ({ row }) => <StatusChip status={row.original.status} />,
        size: 120,
      },
      {
        accessorKey: 'task_name',
        header: 'Task',
        grow: true,
        Cell: ({ row }) => (
          <span className="flex items-center gap-1.5 font-mono text-xs">
            <span className="truncate" title={row.original.task_name}>
              {row.original.task_name}
            </span>
            {row.original.is_workflow_task && (
              <span
                className="shrink-0 rounded bg-muted px-1 text-9 uppercase text-muted-foreground"
                title="Part of a workflow"
              >
                wf
              </span>
            )}
          </span>
        ),
        minSize: 180,
      },
      {
        accessorKey: 'queue_name',
        header: 'Queue',
        Cell: ({ row }) => (
          <span className="text-xs text-muted-foreground">
            {row.original.queue_name}
          </span>
        ),
        size: 120,
      },
      {
        id: 'error',
        accessorKey: 'error_code',
        header: 'Error',
        enableSorting: false,
        Cell: ({ row }) => (
          <ErrorChip
            code={row.original.error_code}
            category={row.original.error_category}
          />
        ),
        minSize: 160,
      },
      {
        accessorKey: 'retry_count',
        header: 'Retries',
        Cell: ({ row }) => (
          <span className="tabular-nums">
            {row.original.retry_count}/{row.original.max_retries}
          </span>
        ),
        size: 90,
      },
      {
        accessorKey: 'queue_s',
        header: 'Queued',
        Cell: ({ row }) => (
          <span className="tabular-nums text-muted-foreground">
            {formatDuration(row.original.queue_s)}
          </span>
        ),
        size: 90,
      },
      {
        accessorKey: 'exec_s',
        header: 'Exec',
        Cell: ({ row }) => (
          <span className="tabular-nums">{formatDuration(row.original.exec_s)}</span>
        ),
        size: 90,
      },
      {
        id: 'worker',
        header: 'Worker',
        enableSorting: false,
        grow: true,
        Cell: ({ row }) => (
          <span
            className="truncate font-mono text-11 text-muted-foreground"
            title={row.original.worker_hostname ?? undefined}
          >
            {row.original.worker_hostname ?? '—'}
          </span>
        ),
        minSize: 120,
      },
      {
        accessorKey: 'enqueued_at',
        header: 'Enqueued',
        Cell: ({ row }) => (
          <span
            className="whitespace-nowrap text-xs text-muted-foreground"
            title={formatTime(row.original.enqueued_at)}
          >
            {formatRelative(row.original.enqueued_at)}
          </span>
        ),
        size: 110,
      },
    ],
    []
  );

  const pagination: SST_PaginationState = { pageIndex, pageSize };
  const sorting: SST_SortingState = [{ id: sortBy, desc: sortDir === 'desc' }];

  const table = useShadStackTable<TaskSummary>({
    columns,
    data: rows,
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
    layoutMode: 'grid',
    enableColumnFilters: false,
    enableColumnActions: false,
    enableTopToolbar: false,
    enableBottomToolbar: false,
    enableStickyHeader: true,
    enableMultiSort: false,
    pageCount,
    // Drive the loading state from the initial load only. `isFetching` is true
    // on every background poll; folding it in would flash the whole grid each
    // cycle. Background polls swap rows in place (keepPreviousData).
    state: { pagination, sorting, isLoading },
    onPaginationChange: updater => {
      const next =
        typeof updater === 'function' ? updater(pagination) : updater;
      onSearchChange({
        page: next.pageIndex === 0 ? undefined : next.pageIndex + 1,
        size: next.pageSize === DEFAULT_PAGE_SIZE ? undefined : next.pageSize,
      });
    },
    onSortingChange: updater => {
      const next = typeof updater === 'function' ? updater(sorting) : updater;
      const first = next[0];
      if (first === undefined) {
        onSearchChange({ sort: undefined, dir: undefined });
        return;
      }
      const dir = first.desc ? 'desc' : 'asc';
      onSearchChange({
        sort: first.id === DEFAULT_SORT ? undefined : (first.id as never),
        dir: dir === DEFAULT_DIR ? undefined : dir,
      });
    },
    theme: { baseBackgroundColor: 'var(--glass-table)' },
    slotProps: {
      tableContainer: {
        className: 'max-h-[calc(100vh-26rem)] overscroll-contain',
      },
      tablePaper: { className: 'glass rounded-lg overflow-hidden' },
      tableBodyRow: ({ row }) => ({
        onClick: () => onSearchChange({ task: row.original.id }),
        className: cn(
          'cursor-pointer',
          row.original.id === selectedId && 'bg-accent-surface'
        ),
      }),
    },
  });

  const startRow = total === 0 ? 0 : pageIndex * pageSize + 1;
  const endRow = Math.min(total, (pageIndex + 1) * pageSize);

  return (
    <div className="flex flex-col gap-4">
      <StatsBar
        filters={filters}
        onToggle={status => toggleValue('status', status)}
      />
      <TaxonomyStrip totals={facets?.error_category_totals ?? {}} />

      <div className="glass flex flex-wrap items-end gap-3 rounded-lg border border-border px-3 py-2">
        <div className="flex flex-col gap-1">
          <span className="text-xs font-medium text-muted-foreground">View</span>
          <ToggleGroup
            type="single"
            value={view}
            onValueChange={next =>
              next &&
              onSearchChange({
                view: next === 'flat' ? undefined : (next as TaskView),
                page: undefined,
              })
            }
            variant="outline"
            size="sm"
          >
            <ToggleGroupItem value="flat">Flat</ToggleGroupItem>
            <ToggleGroupItem value="worker">By worker</ToggleGroupItem>
            <ToggleGroupItem value="task_name">By task</ToggleGroupItem>
            <ToggleGroupItem value="queue">By queue</ToggleGroupItem>
          </ToggleGroup>
        </div>

        <FacetCombobox
          label="Worker"
          options={toOptions(facets?.workers)}
          value={search.worker ?? []}
          onChange={values => setDimension('worker', values)}
        />
        <FacetCombobox
          label="Task"
          options={toOptions(facets?.task_names)}
          value={search.task_name ?? []}
          onChange={values => setDimension('task_name', values)}
        />
        <FacetCombobox
          label="Queue"
          options={toOptions(facets?.queues)}
          value={search.queue ?? []}
          onChange={values => setDimension('queue', values)}
        />
        <FacetCombobox
          label="Error code"
          options={errorOptions}
          value={search.error_code ?? []}
          onChange={values => setDimension('error_code', values)}
        />
        <div className="flex flex-col gap-1">
          <span className="text-xs font-medium text-muted-foreground">Retried</span>
          <Toggle
            pressed={search.retried === true}
            onPressedChange={pressed =>
              onSearchChange({
                retried: pressed ? true : undefined,
                page: undefined,
              })
            }
            variant="outline"
            size="sm"
            className="h-8"
          >
            retried only
          </Toggle>
        </div>
        {hasActiveFilters(search) && (
          <Button
            variant="ghost"
            size="sm"
            className="h-8 text-muted-foreground"
            onClick={clearFilters}
          >
            <X className="size-3.5" />
            Clear filters
          </Button>
        )}
      </div>

      <div className="flex flex-col gap-4 lg:flex-row">
        <div className="min-w-0 flex-1">
          {view === 'flat' ? (
            isError ? (
              <ErrorState message="Could not load tasks." onRetry={refetch} />
            ) : (
              <>
                <ShadStackTable table={table} />
                <div className="glass mt-3 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border px-3 py-2 text-xs text-muted-foreground">
                  <span className="flex items-center gap-2">
                    <span className="tabular-nums">
                      {startRow}–{endRow} of {total}
                    </span>
                    <span className="text-border">·</span>
                    <span>per page</span>
                    <ToggleGroup
                      type="single"
                      value={String(pageSize)}
                      onValueChange={next =>
                        next &&
                        onSearchChange({
                          page: undefined,
                          size:
                            Number(next) === DEFAULT_PAGE_SIZE
                              ? undefined
                              : Number(next),
                        })
                      }
                      variant="outline"
                      size="sm"
                    >
                      {PAGE_SIZES.map(size => (
                        <ToggleGroupItem key={size} value={String(size)}>
                          {size}
                        </ToggleGroupItem>
                      ))}
                    </ToggleGroup>
                  </span>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={pageIndex === 0}
                      onClick={() =>
                        onSearchChange({
                          page: pageIndex === 1 ? undefined : pageIndex,
                        })
                      }
                    >
                      <ChevronLeft className="size-4" /> Prev
                    </Button>
                    <span className="tabular-nums">
                      {pageIndex + 1} / {pageCount}
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={pageIndex + 1 >= pageCount}
                      onClick={() => onSearchChange({ page: pageIndex + 2 })}
                    >
                      Next <ChevronRight className="size-4" />
                    </Button>
                  </div>
                </div>
              </>
            )
          ) : breakdownError ? (
            <ErrorState
              message="Could not load the breakdown."
              onRetry={refetchBreakdown}
            />
          ) : (
            <TaskBreakdown
              breakdown={breakdown}
              isLoading={breakdownLoading}
              onDrill={group => {
                const key: ValueDimension =
                  view === 'worker'
                    ? 'worker'
                    : view === 'task_name'
                      ? 'task_name'
                      : 'queue';
                const patch: TaskSearchPatch = {
                  view: undefined,
                  page: undefined,
                };
                patch[key] = [group];
                onSearchChange(patch);
              }}
            />
          )}
        </div>
        {selectedId !== null && view === 'flat' && !isError && (
          <TaskDetailPanel
            taskId={selectedId}
            onClose={() => onSearchChange({ task: undefined })}
          />
        )}
      </div>
    </div>
  );
}
