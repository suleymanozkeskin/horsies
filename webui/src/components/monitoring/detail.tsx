import type { ReactNode } from 'react';

import { statusColorVar, statusLabel } from '@/lib/status-utils';
import { formatTime } from '@/lib/format-duration';
import type { TaskAttempt } from '@/types/workflows';

/** Label-over-value field used across the monitoring detail panels. */
export function DetailRow({
  label,
  value,
}: {
  label: string;
  value: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs uppercase tracking-wide text-muted-foreground">
        {label}
      </span>
      <span className="break-words text-sm">{value}</span>
    </div>
  );
}

/** One attempt: outcome chip, error message, timing, worker. */
export function AttemptCard({ attempt }: { attempt: TaskAttempt }) {
  const color = statusColorVar(attempt.outcome);
  const message = attempt.error_message ?? attempt.failed_reason;
  return (
    <div
      className="rounded-md border border-border p-2.5"
      style={{ background: `color-mix(in oklab, ${color} 6%, var(--card))` }}
    >
      <div className="flex items-center gap-2 text-xs">
        <span
          className="inline-flex items-center gap-1.5 rounded-full border border-border px-2 py-0.5 font-medium"
          title="outcome"
        >
          <span
            className="size-2 rounded-full"
            style={{ background: color }}
            aria-hidden
          />
          #{attempt.attempt} · {statusLabel(attempt.outcome)}
        </span>
        {attempt.error_code && (
          <span className="font-mono text-10 text-muted-foreground">
            {attempt.error_code}
          </span>
        )}
        {attempt.will_retry && (
          <span className="ml-auto text-10 text-muted-foreground">
            will retry
          </span>
        )}
      </div>
      {message && (
        <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-words rounded bg-glass-field p-2 font-mono text-11 leading-snug text-foreground">
          {message}
        </pre>
      )}
      <div className="mt-2 flex items-center justify-between gap-2 text-10 text-muted-foreground">
        <span>{formatTime(attempt.finished_at ?? attempt.started_at)}</span>
        {attempt.worker_hostname && (
          <span className="truncate font-mono" title={attempt.worker_hostname}>
            {attempt.worker_hostname}
          </span>
        )}
      </div>
    </div>
  );
}

/** Attempt history list with loading/empty states. */
export function AttemptHistory({
  attempts,
  isLoading,
}: {
  attempts: TaskAttempt[];
  isLoading: boolean;
}) {
  return (
    <div className="flex flex-col gap-2">
      <span className="text-xs uppercase tracking-wide text-muted-foreground">
        Attempt history{attempts.length ? ` (${attempts.length})` : ''}
      </span>
      {isLoading && attempts.length === 0 ? (
        <p className="text-xs text-muted-foreground">Loading…</p>
      ) : attempts.length === 0 ? (
        <p className="text-xs text-muted-foreground">No attempts recorded yet.</p>
      ) : (
        attempts.map(attempt => (
          <AttemptCard key={attempt.attempt} attempt={attempt} />
        ))
      )}
    </div>
  );
}
