import { AlertTriangle } from 'lucide-react';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { formatTime } from '@/lib/format-duration';
import type { BudgetLoad, ClassReport, DownSlot } from '@/types/workers';

import {
  budgetFreeText,
  coverageText,
  fleetWarningText,
  uncoveredStreams,
  waitText,
} from './class-coverage';
import { ErrorState } from './states';

/** One alert row per pending stream that no live worker can run. An unserved
 * stream is an error; a stream whose workers have no capacity is a warning. */
export function FleetWarnings({ report }: { report: ClassReport }) {
  const uncovered = uncoveredStreams(report);
  if (uncovered.length === 0) {
    return null;
  }
  return (
    <div className="flex flex-col gap-2" role="alert">
      {uncovered.map(stream => {
        const color =
          stream.coverage.kind === 'unserved'
            ? 'var(--error)'
            : 'var(--warning-dark)';
        return (
          <div
            key={`${stream.queue_name}/${stream.execution}`}
            className="flex items-start gap-3 rounded-lg border px-3 py-2 text-sm"
            style={{ borderColor: color }}
          >
            <AlertTriangle
              className="mt-0.5 size-4 shrink-0"
              style={{ color }}
              aria-hidden
            />
            <span style={{ color }}>{fleetWarningText(stream)}</span>
          </div>
        );
      })}
    </div>
  );
}

/** Pending rows per (queue, execution class): the oldest wait and which live
 * workers serve them, with the fleet warnings above. */
export function ClassCoveragePanel({
  report,
  isLoading,
  isError,
  onRetry,
}: {
  report: ClassReport | undefined;
  isLoading: boolean;
  isError: boolean;
  onRetry: () => void;
}) {
  return (
    <div className="flex flex-col gap-3">
      {report !== undefined && <FleetWarnings report={report} />}
      <div className="glass overflow-hidden rounded-xl border border-border">
        <div className="flex items-baseline justify-between border-b border-border px-4 py-3">
          <span className="text-sm font-medium">Pending by execution class</span>
          {report !== undefined && (
            <span className="text-11 text-muted-foreground">
              live window {report.live_window_s}s · checked{' '}
              {formatTime(report.checked_at)}
            </span>
          )}
        </div>
        {/* A failed poll never blanks the table: the last report stays
            visible below the error so the operator is not left blind. */}
        {isError && (
          <div className="p-4">
            <ErrorState
              compact
              message="Could not load the execution class report."
              onRetry={onRetry}
            />
          </div>
        )}
        {report === undefined ? (
          isError ? null : (
            <p className="p-4 text-sm text-muted-foreground">
              {isLoading ? 'Loading execution classes…' : 'No report yet.'}
            </p>
          )
        ) : report.streams.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">No pending rows.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Queue</TableHead>
                <TableHead>Class</TableHead>
                <TableHead>Oldest wait</TableHead>
                <TableHead>Served by</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {report.streams.map(stream => (
                <TableRow key={`${stream.queue_name}/${stream.execution}`}>
                  <TableCell className="font-mono text-xs">
                    {stream.queue_name}
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {stream.execution}
                  </TableCell>
                  <TableCell className="tabular-nums">
                    {waitText(stream.wait)}
                  </TableCell>
                  <TableCell
                    style={
                      stream.coverage.kind === 'served'
                        ? undefined
                        : {
                            color:
                              stream.coverage.kind === 'unserved'
                                ? 'var(--error)'
                                : 'var(--warning-dark)',
                          }
                    }
                  >
                    {coverageText(stream)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>
    </div>
  );
}

/** One compact chip per budget for a worker card: `ISOLATED 3/8 free`. */
export function BudgetChips({ budgets }: { budgets: BudgetLoad[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {budgets.map(budget => (
        <span
          key={budget.budget}
          className="rounded bg-muted px-1.5 py-0.5 font-mono text-10 text-muted-foreground"
          title={`serves ${budget.serves.join(', ')} · ${budget.in_flight} in flight`}
        >
          {budget.budget} {budgetFreeText(budget)}
        </span>
      ))}
    </div>
  );
}

/** The budgets of the focused worker: capacity, rows held, free, classes served. */
export function BudgetTable({ budgets }: { budgets: BudgetLoad[] }) {
  if (budgets.length === 0) {
    return (
      <p className="text-xs text-muted-foreground">
        This worker serves no execution class.
      </p>
    );
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Budget</TableHead>
          <TableHead className="text-right">In service</TableHead>
          <TableHead className="text-right">In flight</TableHead>
          <TableHead className="text-right">Free</TableHead>
          <TableHead>Serves</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {budgets.map(budget => (
          <TableRow key={budget.budget}>
            <TableCell className="font-mono text-xs">{budget.budget}</TableCell>
            <TableCell className="text-right tabular-nums">
              {budget.capacity}
            </TableCell>
            <TableCell className="text-right tabular-nums">
              {budget.in_flight}
            </TableCell>
            <TableCell className="text-right tabular-nums">{budget.free}</TableCell>
            <TableCell className="font-mono text-10 text-muted-foreground">
              {budget.serves.join(', ')}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

/** The down slots of the focused worker, in slot order. */
export function DownSlotList({ slots }: { slots: DownSlot[] }) {
  return (
    <ul className="flex flex-col gap-2 text-xs">
      {slots.map(slot => (
        <li
          key={slot.slot}
          className="rounded-lg border px-3 py-2"
          style={{ borderColor: 'var(--warning-dark)' }}
        >
          <div className="flex items-center justify-between gap-2">
            <span className="font-mono">slot {slot.slot}</span>
            <span className="text-muted-foreground" title={formatTime(slot.down_since)}>
              {slot.failed_boots} failed start(s) since{' '}
              {formatTime(slot.down_since)}
            </span>
          </div>
          <div style={{ color: 'var(--warning-dark)' }}>{slot.cause}</div>
          <div className="truncate font-mono text-10 text-muted-foreground" title={slot.detail}>
            {slot.detail}
          </div>
        </li>
      ))}
    </ul>
  );
}
