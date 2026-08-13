import { useMemo, useState } from 'react';

import { Activity, Cpu, Database, MemoryStick, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useNow } from '@/hooks/use-now';
import { useSchedules, useWorkerLiveness, useWorkers } from '@/hooks/use-workers';
import { formatElapsed, formatRelative, formatTime } from '@/lib/format-duration';
import { cn } from '@/lib/utils';
import type { WorkerState } from '@/types/workers';

import { ErrorState } from './states';
import { WorkerHistoryChart } from './worker-history-chart';

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
  responsive,
  focused,
  onSelect,
}: {
  worker: WorkerState;
  responsive: boolean;
  focused: boolean;
  onSelect: () => void;
}) {
  const status = workerStatus(worker, responsive);
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'glass flex w-full flex-col gap-3 rounded-xl border border-border p-4 text-left transition-colors hover:bg-glass-surface-strong',
        focused && 'ring-1 ring-primary',
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
          </span>
        </div>
        <span
          className="rounded-full border border-border px-2 py-0.5 text-10 font-medium"
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
  responsive,
}: {
  worker: WorkerState;
  responsive: boolean;
}) {
  const status = workerStatus(worker, responsive);
  return (
    <div className="glass flex flex-col gap-4 rounded-xl border border-border p-4">
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
            pid {worker.pid} · {worker.processes} proc · up{' '}
            {formatElapsed(worker.uptime_s)}
          </span>
        </div>
        <span
          className="rounded-full border border-border px-2.5 py-1 text-xs font-medium"
          style={{ color: status.color }}
        >
          {status.label}
        </span>
      </div>

      <div className="flex flex-wrap gap-1.5">
        {worker.queues.map(queue => (
          <span
            key={queue}
            className="rounded bg-muted px-1.5 py-0.5 font-mono text-10 text-muted-foreground"
          >
            {queue}
            {worker.queue_max_concurrency?.[queue] === undefined
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

      <div className="border-t border-border pt-3">
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
    <div className="glass flex flex-wrap items-center gap-x-6 gap-y-2 rounded-xl border border-border px-4 py-3 text-sm">
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
      <Button
        variant="outline"
        size="sm"
        className="ml-auto h-8"
        onClick={onPing}
        disabled={isFetching}
        title="Re-ping workers (not polled automatically)"
      >
        <RefreshCw className={cn('size-3.5', isFetching && 'animate-spin')} />
        {isFetching ? 'Pinging…' : 'Ping'}
      </Button>
    </div>
  );
}

function SchedulesTable() {
  const { schedules, isLoading, isError } = useSchedules();
  return (
    <div className="glass overflow-hidden rounded-xl border border-border">
      <div className="border-b border-border px-4 py-3 text-sm font-medium">
        Schedules
      </div>
      {isError ? (
        <div className="p-4">
          <ErrorState compact message="Could not load schedules." />
        </div>
      ) : isLoading && schedules.length === 0 ? (
        <p className="p-4 text-sm text-muted-foreground">Loading schedules…</p>
      ) : schedules.length === 0 ? (
        <p className="p-4 text-sm text-muted-foreground">
          No schedules registered.
        </p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Schedule</TableHead>
              <TableHead>Last run</TableHead>
              <TableHead>Next run</TableHead>
              <TableHead className="text-right">Runs</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {schedules.map(schedule => (
              <TableRow key={schedule.schedule_name}>
                <TableCell className="font-mono text-xs">
                  {schedule.schedule_name}
                </TableCell>
                <TableCell
                  className="text-muted-foreground"
                  title={formatTime(schedule.last_run_at)}
                >
                  {formatRelative(schedule.last_run_at)}
                </TableCell>
                <TableCell title={formatTime(schedule.next_run_at)}>
                  {formatRelative(schedule.next_run_at)}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {schedule.run_count}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}

/** Worker + scheduler overview: liveness banner, a worker list, a focused
 * active worker with history charts on the right, and the schedules table. */
export function WorkerOverview() {
  const { workers, isLoading, isError, refetch } = useWorkers();
  const {
    liveness,
    isFetching,
    isError: pingError,
    lastPingedAt,
    refresh,
  } = useWorkerLiveness();
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

  const banner = (
    <LivenessBanner
      responsiveCount={responsiveIds.size}
      isFetching={isFetching}
      isError={pingError}
      lastPingedAt={lastPingedAt}
      onPing={refresh}
    />
  );

  if (isError && workers.length === 0) {
    return (
      <div className="flex flex-col gap-4">
        {banner}
        <ErrorState message="Could not load workers." onRetry={refetch} />
      </div>
    );
  }

  const gridClass = focusWorker
    ? 'grid-cols-1 2xl:grid-cols-2'
    : 'grid-cols-1 md:grid-cols-2 xl:grid-cols-3';

  return (
    <div className="flex flex-col gap-4">
      {banner}
      {/* A failed poll never blanks the grid: stale rows stay visible beside
          the error banner so the operator is not left blind. */}
      {isError && workers.length > 0 && (
        <ErrorState compact message="Could not refresh workers." onRetry={refetch} />
      )}

      {isLoading && workers.length === 0 ? (
        <p className="glass rounded-xl border border-border p-4 text-sm text-muted-foreground">
          Loading workers…
        </p>
      ) : workers.length === 0 ? (
        <p className="glass rounded-xl border border-border p-6 text-sm text-muted-foreground">
          No workers have reported state. Is a horsies worker running?
        </p>
      ) : (
        <div className="flex flex-col gap-4 lg:flex-row">
          <div
            className={cn(
              'flex min-w-0 flex-col gap-4',
              focusWorker ? 'lg:flex-1' : 'w-full'
            )}
          >
            <div className={cn('grid gap-3', gridClass)}>
              {liveWorkers.map(worker => (
                <WorkerCard
                  key={worker.worker_id}
                  worker={worker}
                  responsive={isResponsive(worker)}
                  focused={focusWorker?.worker_id === worker.worker_id}
                  onSelect={() => setFocusId(worker.worker_id)}
                />
              ))}
            </div>

            {staleWorkers.length > 0 && (
              <div className="flex flex-col gap-3">
                <Button
                  variant="ghost"
                  size="sm"
                  className="glass self-start text-muted-foreground"
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
                        responsive={false}
                        focused={focusWorker?.worker_id === worker.worker_id}
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
            <aside className="shrink-0 self-start lg:sticky lg:top-4 lg:w-1/2">
              <WorkerFocus
                worker={focusWorker}
                responsive={isResponsive(focusWorker)}
              />
            </aside>
          )}
        </div>
      )}
    </div>
  );
}
