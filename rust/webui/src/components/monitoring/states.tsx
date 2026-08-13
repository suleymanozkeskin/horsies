import { AlertTriangle, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';

/**
 * Failure state for a monitoring panel. A monitoring tool must never render an
 * API failure as "no data" — that reads as "all clear" when the truth is "we're
 * blind". This makes the failure explicit and offers a retry.
 */
export function ErrorState({
  message = 'Could not load monitoring data.',
  onRetry,
  compact = false,
}: {
  message?: string;
  onRetry?: () => void;
  compact?: boolean;
}) {
  return (
    <div
      className={
        compact
          ? 'flex items-center gap-3 rounded-lg border px-3 py-2 text-sm'
          : 'flex flex-col items-center gap-3 rounded-xl border p-6 text-center'
      }
      style={{ borderColor: 'var(--error)' }}
      role="alert"
    >
      <AlertTriangle
        className="size-4 shrink-0"
        style={{ color: 'var(--error)' }}
        aria-hidden
      />
      <span className="text-sm" style={{ color: 'var(--error)' }}>
        {message}
      </span>
      {onRetry && (
        <Button
          variant="outline"
          size="sm"
          className={compact ? 'ml-auto h-7' : 'h-8'}
          onClick={onRetry}
        >
          <RefreshCw className="size-3.5" />
          Retry
        </Button>
      )}
    </div>
  );
}
