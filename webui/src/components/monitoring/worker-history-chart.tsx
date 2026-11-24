import { useMemo, useState, type ReactNode } from 'react';

import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  type TooltipContentProps,
} from 'recharts';

import { useWorkerHistory } from '@/hooks/use-workers';
import { assertNever } from '@/lib/exhaustive';
import { statusColorVar } from '@/lib/status-utils';
import type { WorkerHistoryPoint } from '@/types/workers';

import { SeriesLegend, type LegendSeries } from './series-legend';
import {
  ALL_SERIES,
  isSeriesShown,
  selectionAfterClick,
  type SeriesSelection,
} from './series-selection';

const CHART_HEIGHT = 140;
const CHART_MARGIN = { top: 8, right: 8, bottom: 0, left: -12 };

const axisStyle = { fontSize: 10, fill: 'var(--muted-foreground)' };
/** The axis lines, the tick marks and the line that follows the pointer. */
const AXIS_STROKE = 'var(--hairline-strong)';
/** The grid: solid hairlines that are weaker than the axis in the two themes,
 * so the series stay the strongest marks of a chart. */
const GRID_STROKE = 'var(--hairline)';
/** The dot of the snapshot under the pointer, with a ring in the color of the
 * window that the chart sits on, so it stays clear where it is on a line. */
const ACTIVE_DOT = { r: 4, stroke: 'var(--background)', strokeWidth: 2 };

/** The series of the load chart. `running` and `claimed` are task statuses,
 * so they have the status colors of the task views. Each series is drawn from
 * the zero line, not stacked: `claimed` is 0 in most snapshots of a worker
 * with no prefetch buffer, and the edge of an empty band on top of a stack
 * would cover the `running` line. */
const LOAD_SERIES = [
  {
    key: 'running',
    label: 'running',
    color: statusColorVar('RUNNING'),
    mark: 'area',
  },
  {
    key: 'claimed',
    label: 'claimed',
    color: statusColorVar('CLAIMED'),
    mark: 'area',
  },
] as const satisfies readonly LegendSeries<string>[];

/** The series of the worker process chart. The two lines differ in lightness
 * and in hue, so a reader who does not see the hue can tell them apart. */
const PROCESS_SERIES = [
  { key: 'cpu', label: 'CPU', color: 'var(--foreground)', mark: 'line' },
  { key: 'memory', label: 'memory', color: 'var(--chart-2)', mark: 'line' },
] as const satisfies readonly LegendSeries<string>[];

type LoadSeriesKey = (typeof LOAD_SERIES)[number]['key'];
type ProcessSeriesKey = (typeof PROCESS_SERIES)[number]['key'];
type ChartSeriesKey = LoadSeriesKey | ProcessSeriesKey;

const LOAD_TITLE = 'Load';
const PROCESS_TITLE = 'Worker process';
const PROCESS_NOTE =
  'The worker process only. Its task children are not in these numbers.';

/** One snapshot as the two charts read it. */
export interface ChartPoint {
  /** The x value: the local time with minutes. */
  readonly t: string;
  readonly snapshotAt: string;
  readonly running: number;
  readonly claimed: number;
  readonly cpu: number | null;
  readonly memory: number | null;
  readonly memoryMb: number | null;
}

const timeLabel = (iso: string): string =>
  new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

/** The snapshots in time order: the API returns the newest first, and a chart
 * reads from left to right. */
export function chartPoints(history: readonly WorkerHistoryPoint[]): ChartPoint[] {
  return [...history].reverse().map(point => ({
    t: timeLabel(point.snapshot_at),
    snapshotAt: point.snapshot_at,
    running: point.tasks_running,
    claimed: point.tasks_claimed,
    cpu: point.cpu_percent,
    memory: point.memory_percent,
    memoryMb: point.memory_usage_mb,
  }));
}

/** Whether a value that the chart library hands to the tooltip is one of the
 * points that this file gave to the chart. */
function isChartPoint(value: unknown): value is ChartPoint {
  return (
    typeof value === 'object' &&
    value !== null &&
    'snapshotAt' in value &&
    typeof value.snapshotAt === 'string' &&
    'running' in value &&
    'claimed' in value
  );
}

/** A percent with at most two decimals and no trailing zero: `0.2%`. */
const percentText = (value: number): string => `${Number(value.toFixed(2))}%`;

/** The value of one series at one snapshot, as the tooltip writes it. A CPU
 * or memory value that the worker did not read is `not read`, never 0. */
export function seriesValueText(
  series: ChartSeriesKey,
  point: ChartPoint
): string {
  switch (series) {
    case 'running':
      return String(point.running);
    case 'claimed':
      return String(point.claimed);
    case 'cpu':
      return point.cpu === null ? 'not read' : percentText(point.cpu);
    case 'memory':
      if (point.memory === null) {
        return 'not read';
      }
      return point.memoryMb === null
        ? percentText(point.memory)
        : `${percentText(point.memory)} · ${point.memoryMb.toFixed(1)} MB`;
    default:
      return assertNever(series, 'chart series');
  }
}

/** The readout of one snapshot: its time, then one row for each series that
 * is drawn. The value is the strong text; the series name is secondary. */
function SnapshotTooltip<Key extends ChartSeriesKey>({
  active,
  payload,
  series,
}: Pick<TooltipContentProps, 'active' | 'payload'> & {
  series: readonly LegendSeries<Key>[];
}) {
  const point: unknown = payload[0]?.payload;
  if (!active || !isChartPoint(point)) {
    return null;
  }
  return (
    <div className="glass-menu rounded-lg px-2.5 py-1.5 text-xs text-popover-foreground">
      <div className="pb-1 text-11 text-muted-foreground">
        {new Date(point.snapshotAt).toLocaleTimeString()}
      </div>
      {series.map(entry => (
        <div key={entry.key} className="flex items-center gap-2">
          <span
            className="h-0.5 w-3 shrink-0 rounded-full"
            style={{ background: entry.color }}
            aria-hidden
          />
          <span className="text-muted-foreground">{entry.label}</span>
          <span className="ml-auto pl-3 font-semibold tabular-nums">
            {seriesValueText(entry.key, point)}
          </span>
        </div>
      ))}
    </div>
  );
}

/**
 * A titled chart slot of fixed height, with the legend of the chart beside
 * the title. The body holds that height whether it carries a chart or a
 * message, so focusing another worker — which starts a fresh query with
 * nothing cached — does not collapse the panel around it. Keeping the
 * previous worker's series would be the wrong repair: it would caption one
 * worker's data with another's name.
 */
function ChartSlot({
  title,
  legend,
  note,
  children,
}: {
  title: string;
  legend: ReactNode;
  /** One line below the chart that says what the chart measures. */
  note?: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
        <span className="text-xs uppercase tracking-wide text-foreground">
          {title}
        </span>
        {legend}
      </div>
      <div style={{ height: CHART_HEIGHT }}>{children}</div>
      {note !== undefined && (
        <p className="text-10 text-muted-foreground">{note}</p>
      )}
    </div>
  );
}

/** Fills a slot while it has no series to draw. */
function SlotMessage({ text }: { text: string }) {
  return (
    <p className="flex h-full items-center justify-center text-xs text-muted-foreground">
      {text}
    </p>
  );
}

/** The RUNNING and the CLAIMED tasks that the worker held at each snapshot.
 * The count axis starts at 0 and fits the series that are drawn. */
function LoadChart({
  points,
  series,
}: {
  points: ChartPoint[];
  series: readonly LegendSeries<LoadSeriesKey>[];
}) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={points} margin={CHART_MARGIN}>
        <CartesianGrid vertical={false} stroke={GRID_STROKE} />
        <XAxis
          dataKey="t"
          tick={axisStyle}
          stroke={AXIS_STROKE}
          minTickGap={32}
        />
        <YAxis allowDecimals={false} tick={axisStyle} stroke={AXIS_STROKE} />
        <Tooltip
          cursor={{ stroke: AXIS_STROKE }}
          content={({ active, payload }) => (
            <SnapshotTooltip active={active} payload={payload} series={series} />
          )}
        />
        {series.map(entry => (
          <Area
            key={entry.key}
            type="monotone"
            dataKey={entry.key}
            name={entry.label}
            stroke={entry.color}
            strokeWidth={1.5}
            fill={entry.color}
            fillOpacity={0.2}
            activeDot={ACTIVE_DOT}
            isAnimationActive={false}
          />
        ))}
      </AreaChart>
    </ResponsiveContainer>
  );
}

/** The CPU and the memory of the worker process at each snapshot, in percent.
 * The percent axis starts at 0 and fits the series that are drawn: the values
 * of a worker process are far below 100, and a fixed axis to 100 would draw
 * them as a flat line on zero. */
function ProcessChart({
  points,
  series,
}: {
  points: ChartPoint[];
  series: readonly LegendSeries<ProcessSeriesKey>[];
}) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={points} margin={CHART_MARGIN}>
        <CartesianGrid vertical={false} stroke={GRID_STROKE} />
        <XAxis
          dataKey="t"
          tick={axisStyle}
          stroke={AXIS_STROKE}
          minTickGap={32}
        />
        <YAxis
          tick={axisStyle}
          stroke={AXIS_STROKE}
          tickFormatter={percentText}
        />
        <Tooltip
          cursor={{ stroke: AXIS_STROKE }}
          content={({ active, payload }) => (
            <SnapshotTooltip active={active} payload={payload} series={series} />
          )}
        />
        {series.map(entry => (
          <Line
            key={entry.key}
            type="monotone"
            dataKey={entry.key}
            name={entry.label}
            stroke={entry.color}
            strokeWidth={2}
            dot={false}
            activeDot={ACTIVE_DOT}
            connectNulls
            isAnimationActive={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

/**
 * Two charts of one worker's snapshots, over the same time axis.
 *
 * - Load: the RUNNING and the CLAIMED tasks that the worker held.
 * - Worker process: the CPU and the memory of the worker process, in
 *   percent. The task children of the worker are not in these numbers.
 *
 * A click on a legend item draws that series alone, and the value axis then
 * fits that series. The selection stays when the focus moves to another
 * worker.
 */
export function WorkerHistoryChart({ workerId }: { workerId: string }) {
  const { history, isLoading } = useWorkerHistory(workerId);
  const [loadSelection, setLoadSelection] =
    useState<SeriesSelection<LoadSeriesKey>>(ALL_SERIES);
  const [processSelection, setProcessSelection] =
    useState<SeriesSelection<ProcessSeriesKey>>(ALL_SERIES);
  const points = useMemo(() => chartPoints(history), [history]);

  const empty =
    points.length === 0 ? (
      <SlotMessage
        text={isLoading ? 'Loading history…' : 'No history recorded yet.'}
      />
    ) : null;

  return (
    <div className="flex flex-col gap-4">
      <ChartSlot
        title={LOAD_TITLE}
        legend={
          <SeriesLegend
            chart={LOAD_TITLE}
            series={LOAD_SERIES}
            selection={loadSelection}
            onClick={series =>
              setLoadSelection(current => selectionAfterClick(current, series))
            }
          />
        }
      >
        {empty ?? (
          <LoadChart
            points={points}
            series={LOAD_SERIES.filter(entry =>
              isSeriesShown(loadSelection, entry.key)
            )}
          />
        )}
      </ChartSlot>
      <ChartSlot
        title={PROCESS_TITLE}
        note={PROCESS_NOTE}
        legend={
          <SeriesLegend
            chart={PROCESS_TITLE}
            series={PROCESS_SERIES}
            selection={processSelection}
            onClick={series =>
              setProcessSelection(current =>
                selectionAfterClick(current, series)
              )
            }
          />
        }
      >
        {empty ?? (
          <ProcessChart
            points={points}
            series={PROCESS_SERIES.filter(entry =>
              isSeriesShown(processSelection, entry.key)
            )}
          />
        )}
      </ChartSlot>
    </div>
  );
}
