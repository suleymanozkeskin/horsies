import { useMemo, type ReactNode } from 'react';

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

const CHART_HEIGHT = 140;

/**
 * A titled chart slot of fixed height. The body holds that height whether it
 * carries a chart or a message, so focusing another worker — which starts a
 * fresh query with nothing cached — does not collapse the panel around it.
 * Keeping the previous worker's series would be the wrong repair: it would
 * caption one worker's data with another's name.
 */
function ChartSlot({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div>
      <span className="text-xs uppercase tracking-wide text-foreground">
        {title}
      </span>
      <div style={{ height: CHART_HEIGHT }}>{children}</div>
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

  const empty =
    points.length === 0 ? (
      <SlotMessage
        text={isLoading ? 'Loading history…' : 'No history recorded yet.'}
      />
    ) : null;

  return (
    <div className="flex flex-col gap-4">
      <ChartSlot title="Load (running / claimed)">
        {empty ?? (
          <ResponsiveContainer width="100%" height="100%">
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
        )}
      </ChartSlot>
      <ChartSlot title="CPU % / Memory %">
        {empty ?? (
          <ResponsiveContainer width="100%" height="100%">
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
        )}
      </ChartSlot>
    </div>
  );
}
