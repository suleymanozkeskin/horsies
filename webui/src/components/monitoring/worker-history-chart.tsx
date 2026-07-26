import { useMemo } from 'react';

import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { useWorkerHistory } from '@/hooks/use-workers';

const axisStyle = { fontSize: 10, fill: 'var(--muted-foreground)' };

const tooltipStyle = {
  background: 'var(--card)',
  border: '1px solid var(--border)',
  borderRadius: 8,
  fontSize: 12,
};

const timeLabel = (iso: string): string =>
  new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

/** Two stacked timeseries charts (load + CPU/mem) for one worker. */
export function WorkerHistoryChart({ workerId }: { workerId: string }) {
  const { history, isLoading } = useWorkerHistory(workerId);

  // The API returns newest-first; charts read left-to-right chronological.
  const points = useMemo(
    () =>
      [...history].reverse().map(point => ({
        t: timeLabel(point.snapshot_at),
        running: point.tasks_running,
        claimed: point.tasks_claimed,
        cpu: point.cpu_percent,
        mem: point.memory_percent,
      })),
    [history]
  );

  if (isLoading && points.length === 0) {
    return <p className="text-xs text-muted-foreground">Loading history…</p>;
  }
  if (points.length === 0) {
    return (
      <p className="text-xs text-muted-foreground">No history recorded yet.</p>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      <div>
        <span className="text-xs uppercase tracking-wide text-foreground">
          Load (running / claimed)
        </span>
        <ResponsiveContainer width="100%" height={140}>
          <AreaChart
            data={points}
            margin={{ top: 8, right: 8, bottom: 0, left: -20 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis dataKey="t" tick={axisStyle} minTickGap={32} />
            <YAxis allowDecimals={false} tick={axisStyle} />
            <Tooltip contentStyle={tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Area
              type="monotone"
              dataKey="running"
              name="running"
              stroke="var(--chart-1)"
              fill="var(--chart-1)"
              fillOpacity={0.2}
              isAnimationActive={false}
            />
            <Area
              type="monotone"
              dataKey="claimed"
              name="claimed"
              stroke="var(--chart-3)"
              fill="var(--chart-3)"
              fillOpacity={0.15}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      <div>
        <span className="text-xs uppercase tracking-wide text-foreground">
          CPU % / Memory %
        </span>
        <ResponsiveContainer width="100%" height={140}>
          <LineChart
            data={points}
            margin={{ top: 8, right: 8, bottom: 0, left: -20 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
            <XAxis dataKey="t" tick={axisStyle} minTickGap={32} />
            <YAxis domain={[0, 100]} tick={axisStyle} />
            <Tooltip contentStyle={tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Line
              type="monotone"
              dataKey="cpu"
              name="CPU %"
              stroke="var(--chart-5)"
              dot={false}
              connectNulls
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="mem"
              name="Memory %"
              stroke="var(--chart-2)"
              dot={false}
              connectNulls
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
