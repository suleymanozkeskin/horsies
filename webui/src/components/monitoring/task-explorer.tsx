import { useMemo, type ReactNode } from 'react';

import { ChevronLeft, ChevronRight, RotateCcw, X } from 'lucide-react';
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
import { Capsule, CapsuleSeparator } from '@/components/ui/capsule';
import { PageHeader } from '@/components/ui/page-header';
import { Toggle } from '@/components/ui/toggle';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { useMeta } from '@/hooks/use-meta';
import { useBreakdown, useFacets, useTasks } from '@/hooks/use-tasks';
import { formatDuration, formatRelative, formatTime } from '@/lib/format-duration';
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
import type { TaskSummary } from '@/types/tasks';

import { AttemptPanel } from './attempt-panel';
import { ErrorChip } from './error-chip';
import { errorCategoryColorVar } from './error-taxonomy';
import { FacetCombobox, type FacetOption } from './facet-combobox';
import { ErrorState } from './states';
import { StatsBar } from './stats-bar';
import { TaskBreakdown } from './task-breakdown';
import { TaxonomyStrip } from './taxonomy-strip';

/** URL dimensions the filter controls write to. */
type ValueDimension =
  | 'status'
  | 'task_name'
  | 'queue'
  | 'worker'
  | 'error_code'
  | 'error_category';

export interface TaskExplorerProps {
  search: TaskSearch;
  /** Merge patch. A key set to `undefined` is removed from the URL. */
  onSearchChange: (patch: TaskSearchPatch) => void;
}

/** The page header count: `1 task`, `12,345 tasks`. */
function taskCountText(count: number): string {
  return `${count.toLocaleString()} ${count === 1 ? 'task' : 'tasks'}`;
}

/** The close button of the task detail. */
function CloseDetailButton({ onClose }: { onClose: () => void }) {
  return (
    <button
      type="button"
      onClick={onClose}
      className="shrink-0 rounded-full p-1.5 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring"
      aria-label="Close detail"
    >
      <X className="size-4" />
    </button>
  );
}

/** The task detail while it has no task to show: a state line with the close
 * button. */
function DetailState({ text, onClose }: { text: ReactNode; onClose: () => void }) {
  return (
    <div className="flex items-start justify-between gap-3">
      <div className="min-w-0 flex-1 text-sm text-muted-foreground">{text}</div>
      <CloseDetailButton onClose={onClose} />
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
  const { meta } = useMeta();
  const { detail, isLoading, isError } = view;
  return (
    <aside
      aria-label="Task detail"
      className="flex w-full shrink-0 flex-col border-hairline max-lg:border-t max-lg:pt-4 lg:sticky lg:top-[calc(var(--capsule-height)_+_1.5rem)] lg:max-h-[calc(100dvh_-_var(--shell-block)_-_var(--capsule-height)_-_1.5rem)] lg:w-[420px] lg:border-l lg:pl-6"
    >
      <div className="-mx-1 flex flex-col overflow-y-auto px-1 pb-1">
        {isError && detail === undefined ? (
          <DetailState text={<ErrorState compact message="Could not load this task." />} onClose={onClose} />
        ) : detail === undefined && isLoading ? (
          <DetailState text="Loading…" onClose={onClose} />
        ) : detail === undefined ? (
          <DetailState text="Task not found." onClose={onClose} />
        ) : (
          <AttemptPanel
            detail={detail}
            traceTemplate={meta?.trace_link_template ?? null}
            titleEnd={<CloseDetailButton onClose={onClose} />}
            actions={<TaskActionBar view={view} />}
          />
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
    ...(search.error_category === undefined
      ? {}
      : { error_category: search.error_category }),
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
  // The task count of the current filter, from whichever query the view runs.
  const filterTotal = view === 'flat' ? page?.total : breakdown?.total.total;

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
      error_category: undefined,
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
    // The window is the scroll container. A sticky header makes the grid cap
    // its own height near the viewport and scroll inside the window.
    enableStickyHeader: false,
    enableMultiSort: false,
    initialState: { density: 'compact' },
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
    // The grid sits flat on the window: the paper's border, radius and shadow,
    // the head's card fill and the head row's shadow are switched off. The
    // grid paints an inline background on every row and `inherit` on every
    // cell, so the row states take the important modifier to win over it, and
    // the hover paints the cells only, so the translucent token is painted
    // once.
    slotProps: {
      tablePaper: { className: 'rounded-none border-0 shadow-none' },
      tableHead: { className: 'bg-transparent' },
      tableHeadRow: { style: { boxShadow: 'none' } },
      tableHeadCell: {
        className: 'border-hairline text-xs font-semibold text-muted-foreground',
      },
      tableBodyRow: ({ row }) => ({
        onClick: () => onSearchChange({ task: row.original.id }),
        className: cn(
          'cursor-pointer border-hairline',
          row.original.id === selectedId
            ? 'bg-accent-surface!'
            : '[&:hover>td]:bg-surface-hover!'
        ),
      }),
    },
  });

  const startRow = total === 0 ? 0 : pageIndex * pageSize + 1;
  const endRow = Math.min(total, (pageIndex + 1) * pageSize);

  return (
    <div className="flex flex-col gap-4">
      <PageHeader title="Tasks">
        {filterTotal === undefined ? undefined : taskCountText(filterTotal)}
      </PageHeader>
      <StatsBar
        filters={filters}
        onToggle={status => toggleValue('status', status)}
      />
      <TaxonomyStrip
        totals={facets?.error_category_totals ?? {}}
        active={search.error_category ?? []}
        onToggle={category => toggleValue('error_category', category)}
      />

      {/* Sticks to the top of the window, which is the scroll container. The
          wrapper reaches the window edges, so its fade covers the rows that
          pass under the capsule. */}
      <div className="sticky top-0 z-20 -my-2.5 py-2.5">
        <Capsule className="max-w-full overflow-x-auto whitespace-nowrap [scrollbar-width:none]">
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
            variant="segment"
            size="capsule"
            className="gap-0.5"
            aria-label="View"
          >
            <ToggleGroupItem value="flat">Flat</ToggleGroupItem>
            <ToggleGroupItem value="worker">By worker</ToggleGroupItem>
            <ToggleGroupItem value="task_name">By task</ToggleGroupItem>
            <ToggleGroupItem value="queue">By queue</ToggleGroupItem>
          </ToggleGroup>
          <CapsuleSeparator />
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
          <CapsuleSeparator />
          <Toggle
            pressed={search.retried === true}
            onPressedChange={pressed =>
              onSearchChange({
                retried: pressed ? true : undefined,
                page: undefined,
              })
            }
            variant="segment"
            size="capsule"
          >
            <RotateCcw />
            retried only
          </Toggle>
          {hasActiveFilters(search) && (
            <>
              <CapsuleSeparator />
              <Button
                variant="capsule"
                size="capsule"
                className="text-muted-foreground"
                onClick={clearFilters}
              >
                <X />
                Clear filters
              </Button>
            </>
          )}
        </Capsule>
      </div>

      <div className="flex flex-col gap-6 lg:flex-row lg:items-start">
        <div className="min-w-0 flex-1">
          {view === 'flat' ? (
            isError ? (
              <ErrorState message="Could not load tasks." onRetry={refetch} />
            ) : (
              <>
                <ShadStackTable table={table} />
                <div className="mt-3 flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
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
                      variant="segment"
                      size="segment"
                      className="segment-track gap-0.5 rounded-full p-0.75"
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
                      className="h-8 rounded-full"
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
                      className="h-8 rounded-full"
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
