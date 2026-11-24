import { useMemo, useState } from 'react';

import { Activity, Cpu, Database, MemoryStick, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Capsule } from '@/components/ui/capsule';
import { PageHeader } from '@/components/ui/page-header';
import { useNow } from '@/hooks/use-now';
import {
  useClassReport,
  useWorkerLiveness,
  useWorkers,
} from '@/hooks/use-workers';
import { assertNever } from '@/lib/exhaustive';
import { formatElapsed, formatTime } from '@/lib/format-duration';
import { cn } from '@/lib/utils';
import type {
  BudgetLoad,
  ClassReport,
  SnapshotProfile,
  WorkerState,
} from '@/types/workers';

import { budgetsOf } from './class-coverage';
import {
  BudgetChips,
  BudgetTable,
  ClassCoveragePanel,
  DownSlotList,
} from './class-coverage-panel';
import { SchedulesTable } from './schedules-table';
import { ErrorState } from './states';
import { WorkerHistoryChart } from './worker-history-chart';
import {
  WorkerProfilePanel,
  type ClassReportRead,
} from './worker-profile-panel';
import { groupByProfile } from './worker-profiles';

interface WorkerStatus {
  label: string;
  color: string;
}

/** Status is liveness-first: a worker proven responsive by the active ping is
 * "online" regardless of snapshot age; a non-responsive worker is "stale" once
 * its snapshot is old (likely dead), else "no ping" (recent snapshot, silent). */
function workerStatus(worker: WorkerState, responsive: boolean): WorkerStatus {
  if (responsive) {
    return { label: 'online', color: 'var(--success)' };
  }
  if (worker.stale) {
    return { label: 'stale', color: 'var(--muted-foreground)' };
  }
  return { label: 'no ping', color: 'var(--warning-dark)' };
}

/** The page header count: live workers, plus the stale ones when any exist. */
function workerCountText(liveCount: number, staleCount: number): string {
  const live = `${liveCount} live`;
  return staleCount === 0 ? live : `${live} · ${staleCount} stale`;
}

/** The worker profile of a snapshot in a line of worker facts. */
function profileText(profile: SnapshotProfile): string {
  switch (profile.kind) {
    case 'named':
      return `profile ${profile.name}`;
    case 'not_recorded':
      return 'profile not recorded';
    default:
      return assertNever(profile, 'snapshot profile');
  }
}

/** A thin labelled bar for a 0–100 percentage metric. */
function MetricBar({
  label,
  percent,
  detail,
  icon: Icon,
}: {
  label: string;
  percent: number | null;
  detail?: string;
  icon: typeof Cpu;
}) {
  const pct = percent ?? 0;
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center justify-between text-11 text-muted-foreground">
        <span className="flex items-center gap-1">
          <Icon className="size-3" />
          {label}
        </span>
        <span className="tabular-nums">
          {percent === null ? '—' : `${pct.toFixed(0)}%`}
          {detail ? ` · ${detail}` : ''}
        </span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-muted">
        <div
          className="h-full rounded-full"
          style={{
            width: `${Math.min(100, Math.max(0, pct))}%`,
            background:
              pct > 85
                ? 'var(--error)'
                : pct > 60
                  ? 'var(--warning-dark)'
                  : 'var(--chart-1)',
          }}
        />
      </div>
    </div>
  );
}

/** Compact, clickable worker card for the left rail. */
function WorkerCard({
  worker,
  budgets,
  responsive,
  focused,
  showsProfile,
  onSelect,
}: {
  worker: WorkerState;
  /** `undefined` when the class report has no live snapshot of the worker. */
  budgets: BudgetLoad[] | undefined;
  responsive: boolean;
  focused: boolean;
  /** Whether the card names the worker profile. A card inside the panel of
   * its profile does not: the panel names it. */
  showsProfile: boolean;
  onSelect: () => void;
}) {
  const status = workerStatus(worker, responsive);
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'content-surface flex w-full flex-col gap-3 rounded-xl p-4 text-left transition-colors',
        focused ? 'bg-accent-surface' : 'hover:bg-card-hover',
        !responsive && worker.stale && 'opacity-70'
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span
              className="size-2.5 shrink-0 rounded-full"
              style={{ background: status.color }}
              aria-hidden
            />
            <span className="truncate font-mono text-sm" title={worker.hostname}>
              {worker.hostname}
            </span>
          </div>
          <span className="font-mono text-11 text-muted-foreground">
            pid {worker.pid} · {worker.processes} proc
            {showsProfile && ` · ${profileText(worker.profile)}`}
          </span>
        </div>
        <span
          className="rounded-full border border-hairline-strong px-2 py-0.5 text-10 font-medium"
          style={{ color: status.color }}
        >
          {status.label}
        </span>
      </div>

      <div className="flex items-center gap-4 text-xs text-muted-foreground">
        <span className="tabular-nums">
          <span className="font-semibold text-foreground">
            {worker.tasks_running}
          </span>{' '}
          running
        </span>
        <span className="tabular-nums">
          <span className="font-semibold text-foreground">
            {worker.tasks_claimed}
          </span>{' '}
          claimed
        </span>
        <span className="ml-auto" title={formatTime(worker.worker_started_at)}>
          up {formatElapsed(worker.uptime_s)}
        </span>
      </div>

      {(budgets !== undefined || worker.down_slots.length > 0) && (
        <div className="flex flex-wrap items-center gap-1.5">
          {budgets !== undefined && <BudgetChips budgets={budgets} />}
          {worker.down_slots.length > 0 && (
            <span
              className="rounded border px-1.5 py-0.5 text-10 font-medium"
              style={{
                borderColor: 'var(--warning-dark)',
                color: 'var(--warning-dark)',
              }}
            >
              {worker.down_slots.length} slot
              {worker.down_slots.length === 1 ? '' : 's'} down
            </span>
          )}
        </div>
      )}

      <MetricBar label="CPU" percent={worker.cpu_percent} icon={Cpu} />
      <MetricBar
        label="Memory"
        percent={worker.memory_percent}
        {...(worker.memory_usage_mb === null
          ? {}
          : { detail: `${worker.memory_usage_mb.toFixed(0)} MB` })}
        icon={MemoryStick}
      />
    </button>
  );
}

/** The focused worker, shown on the right half with history charts expanded. */
function WorkerFocus({
  worker,
  budgets,
  responsive,
}: {
  worker: WorkerState;
  budgets: BudgetLoad[] | undefined;
  responsive: boolean;
}) {
  const status = workerStatus(worker, responsive);
  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span
              className="size-3 shrink-0 rounded-full"
              style={{ background: status.color }}
              aria-hidden
            />
            <span className="truncate font-mono text-base" title={worker.hostname}>
              {worker.hostname}
            </span>
          </div>
          <span className="font-mono text-xs text-muted-foreground">
            {profileText(worker.profile)} · pid {worker.pid} ·{' '}
            {worker.processes} proc · up {formatElapsed(worker.uptime_s)}
          </span>
        </div>
        <span
          className="rounded-full border border-hairline-strong px-2.5 py-1 text-xs font-medium"
          style={{ color: status.color }}
        >
          {status.label}
        </span>
      </div>

      <div className="flex flex-wrap gap-1.5">
        {worker.queues.map(queue => (
          <span
            key={queue}
            className="rounded bg-glass-inset px-1.5 py-0.5 font-mono text-10 text-muted-foreground"
          >
            {queue}
            {worker.queue_max_concurrency[queue] === undefined
              ? ''
              : `·${worker.queue_max_concurrency[queue]}`}
          </span>
        ))}
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="flex flex-col">
          <span className="text-2xl font-semibold tabular-nums">
            {worker.tasks_running}
          </span>
          <span className="text-11 text-muted-foreground">running</span>
        </div>
        <div className="flex flex-col">
          <span className="text-2xl font-semibold tabular-nums">
            {worker.tasks_claimed}
          </span>
          <span className="text-11 text-muted-foreground">claimed</span>
        </div>
      </div>

      <MetricBar label="CPU" percent={worker.cpu_percent} icon={Cpu} />
      <MetricBar
        label="Memory"
        percent={worker.memory_percent}
        {...(worker.memory_usage_mb === null
          ? {}
          : { detail: `${worker.memory_usage_mb.toFixed(0)} MB` })}
        icon={MemoryStick}
      />

      <div className="flex flex-col gap-2 border-t border-hairline pt-3">
        <span className="text-11 font-medium text-muted-foreground">
          Execution classes
        </span>
        {budgets === undefined ? (
          <p className="text-xs text-muted-foreground">
            No live snapshot of this worker in the class report.
          </p>
        ) : (
          <BudgetTable budgets={budgets} />
        )}
      </div>

      {worker.slot_health === 'slots_down' && (
        <div className="flex flex-col gap-2 border-t border-hairline pt-3">
          <span className="text-11 font-medium text-muted-foreground">
            Down slots
          </span>
          <DownSlotList slots={worker.down_slots} />
        </div>
      )}

      <div className="border-t border-hairline pt-3">
        <WorkerHistoryChart workerId={worker.worker_id} />
      </div>
    </div>
  );
}

function LivenessBanner({
  responsiveCount,
  isFetching,
  isError,
  lastPingedAt,
  onPing,
}: {
  responsiveCount: number;
  isFetching: boolean;
  isError: boolean;
  lastPingedAt: number;
  onPing: () => void;
}) {
  const { liveness } = useWorkerLiveness();
  const now = useNow(5_000);
  // The ping is on-demand, so the "online" badges are only as fresh as the last
  // ping — surface that age so a green dot isn't mistaken for live truth.
  const ageS = lastPingedAt ? Math.round((now - lastPingedAt) / 1000) : null;
  const freshness =
    ageS === null
      ? 'not pinged yet'
      : ageS < 5
        ? 'just now'
        : `pinged ${formatElapsed(ageS)} ago`;

  return (
    <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
      <span className="flex items-center gap-2">
        <Database className="size-4 text-muted-foreground" />
        <span className="text-muted-foreground">Database</span>
        <span
          className="font-medium"
          style={{
            color:
              isError || liveness?.db_reachable === false
                ? 'var(--error)'
                : 'var(--success)',
          }}
        >
          {isError
            ? 'ping failed'
            : liveness === undefined
              ? '…'
              : liveness.db_reachable
                ? `reachable · ${liveness.db_latency_ms?.toFixed(1)} ms`
                : 'unreachable'}
        </span>
      </span>
      <span className="flex items-center gap-2">
        <Activity className="size-4 text-muted-foreground" />
        <span className="text-muted-foreground">Responsive workers</span>
        <span className="font-medium tabular-nums">{responsiveCount}</span>
      </span>
      <span className="text-xs text-muted-foreground">{freshness}</span>
      <Capsule className="ml-auto">
        <Button
          variant="capsule"
          size="capsule"
          onClick={onPing}
          disabled={isFetching}
          title="Re-ping workers (not polled automatically)"
        >
          <RefreshCw className={cn('size-3.5', isFetching && 'animate-spin')} />
          {isFetching ? 'Pinging…' : 'Ping'}
        </Button>
      </Capsule>
    </div>
  );
}

/** Whether the class report was read. A report from an earlier read counts
 * as read when a later read failed: its budgets stay on the page. */
function classReportRead(
  report: ClassReport | undefined,
  isError: boolean
): ClassReportRead {
  if (report !== undefined) {
    return 'loaded';
  }
  return isError ? 'failed' : 'loading';
}

/** Worker + scheduler overview: page header and liveness row, the pending
 * rows per execution class with fleet warnings, the live workers in groups of
 * one worker profile with the budget load of each group, a focused active
 * worker with its budgets, down slots and history charts on the right, and the
 * schedules table. Sections sit flat on the window; only the worker cards are
 * boxes. */
export function WorkerOverview() {
  const { workers, isLoading, isError, refetch } = useWorkers();
  const {
    liveness,
    isFetching,
    isError: pingError,
    lastPingedAt,
    refresh,
  } = useWorkerLiveness();
  const {
    report,
    isLoading: reportLoading,
    isError: reportError,
    refetch: refetchReport,
  } = useClassReport();
  const [focusId, setFocusId] = useState<string | null>(null);
  const [showStale, setShowStale] = useState(false);

  const responsiveIds = useMemo(
    () => new Set(liveness?.workers.map(worker => worker.worker_id) ?? []),
    [liveness]
  );

  const isResponsive = (worker: WorkerState): boolean =>
    responsiveIds.has(worker.worker_id);

  // Active (responsive) first, then live snapshots, then by recency.
  const sorted = useMemo(() => {
    const rank = (worker: WorkerState): number =>
      responsiveIds.has(worker.worker_id) ? 0 : worker.stale ? 2 : 1;
    return [...workers].sort(
      (a, b) =>
        rank(a) - rank(b) ||
        (a.snapshot_age_s ?? 1e9) - (b.snapshot_age_s ?? 1e9)
    );
  }, [workers, responsiveIds]);

  // Dead/stale historical workers accumulate (retention); collapse them so the
  // grid isn't dominated by them. A responsive worker is never "stale".
  const liveWorkers = sorted.filter(w => isResponsive(w) || !w.stale);
  const staleWorkers = sorted.filter(w => !isResponsive(w) && w.stale);

  // Default focus to an active worker, else the most-recent live one, so the
  // focus panel appears immediately rather than after the on-demand ping.
  const focusWorker =
    (focusId === null
      ? undefined
      : workers.find(worker => worker.worker_id === focusId)) ??
    sorted.find(isResponsive) ??
    liveWorkers[0] ??
    sorted[0] ??
    null;

  // The count is shown only once a worker read has answered.
  const countText =
    workers.length > 0 || (!isLoading && !isError)
      ? workerCountText(liveWorkers.length, staleWorkers.length)
      : undefined;

  const banner = (
    <>
      <div className="flex flex-col gap-3 border-b border-hairline pb-4">
        <PageHeader title="Workers">{countText}</PageHeader>
        <LivenessBanner
          responsiveCount={responsiveIds.size}
          isFetching={isFetching}
          isError={pingError}
          lastPingedAt={lastPingedAt}
          onPing={refresh}
        />
      </div>
      <ClassCoveragePanel
        report={report}
        isLoading={reportLoading}
        isError={reportError}
        onRetry={refetchReport}
      />
    </>
  );

  if (isError && workers.length === 0) {
    return (
      <div className="flex flex-col gap-6">
        {banner}
        <ErrorState message="Could not load workers." onRetry={refetch} />
      </div>
    );
  }

  const gridClass = focusWorker
    ? 'grid-cols-1 2xl:grid-cols-2'
    : 'grid-cols-1 md:grid-cols-2 xl:grid-cols-3';

  return (
    <div className="flex flex-col gap-6">
      {banner}
      {/* A failed poll never blanks the grid: stale rows stay visible beside
          the error banner so the operator is not left blind. */}
      {isError && workers.length > 0 && (
        <ErrorState compact message="Could not refresh workers." onRetry={refetch} />
      )}

      {isLoading && workers.length === 0 ? (
        <p className="text-sm text-muted-foreground">Loading workers…</p>
      ) : workers.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No workers have reported state. Is a horsies worker running?
        </p>
      ) : (
        <div className="flex flex-col gap-6 lg:flex-row">
          <div
            className={cn(
              'flex min-w-0 flex-col gap-6',
              focusWorker ? 'lg:flex-1' : 'w-full'
            )}
          >
            <section
              aria-label="Workers by profile"
              className="flex flex-col gap-3"
            >
              <span className="text-sm font-semibold">Workers by profile</span>
              {liveWorkers.length === 0 && (
                <p className="text-sm text-muted-foreground">No live worker.</p>
              )}
              {groupByProfile(liveWorkers, report).map(group => (
                <WorkerProfilePanel
                  key={group.key}
                  group={group}
                  reportRead={classReportRead(report, reportError)}
                >
                  <div className={cn('grid gap-3', gridClass)}>
                    {group.workers.map(worker => (
                      <WorkerCard
                        key={worker.worker_id}
                        worker={worker}
                        budgets={budgetsOf(report, worker.worker_id)}
                        responsive={isResponsive(worker)}
                        focused={focusWorker?.worker_id === worker.worker_id}
                        showsProfile={false}
                        onSelect={() => setFocusId(worker.worker_id)}
                      />
                    ))}
                  </div>
                </WorkerProfilePanel>
              ))}
            </section>

            {staleWorkers.length > 0 && (
              <div className="flex flex-col gap-3">
                <Button
                  variant="ghost"
                  size="sm"
                  className="self-start text-muted-foreground"
                  onClick={() => setShowStale(shown => !shown)}
                >
                  {showStale ? 'Hide' : 'Show'} {staleWorkers.length} stale
                  worker{staleWorkers.length === 1 ? '' : 's'}
                </Button>
                {showStale && (
                  <div className={cn('grid gap-3', gridClass)}>
                    {staleWorkers.map(worker => (
                      <WorkerCard
                        key={worker.worker_id}
                        worker={worker}
                        budgets={budgetsOf(report, worker.worker_id)}
                        responsive={false}
                        focused={focusWorker?.worker_id === worker.worker_id}
                        showsProfile
                        onSelect={() => setFocusId(worker.worker_id)}
                      />
                    ))}
                  </div>
                )}
              </div>
            )}

            <SchedulesTable />
          </div>

          {focusWorker && (
            <aside className="shrink-0 border-hairline max-lg:border-t max-lg:pt-6 lg:sticky lg:top-4 lg:w-1/2 lg:self-start lg:border-l lg:pl-6">
              <WorkerFocus
                worker={focusWorker}
                budgets={budgetsOf(report, focusWorker.worker_id)}
                responsive={isResponsive(focusWorker)}
              />
            </aside>
          )}
        </div>
      )}
    </div>
  );
}
