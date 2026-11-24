import type { ReactNode } from 'react';

import { traceLinkOf } from '@/lib/trace-link';

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

/** The trace id of a task: a link under the server's template, or the id as
 * text with no template. Nothing for a task sent with no trace. */
export function TraceRow({
  traceId,
  template,
}: {
  traceId: string | null;
  template: string | null;
}) {
  if (traceId === null) {
    return null;
  }
  const text = <span className="font-mono text-xs">{traceId}</span>;
  return (
    <DetailRow
      label="Trace"
      value={
        template === null ? (
          text
        ) : (
          <a
            href={traceLinkOf(template, traceId)}
            target="_blank"
            rel="noopener noreferrer"
            className="underline underline-offset-2 hover:text-foreground"
          >
            {text}
          </a>
        )
      }
    />
  );
}
