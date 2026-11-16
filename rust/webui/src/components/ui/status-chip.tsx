import { statusColorVar, statusLabel } from '@/lib/status-utils';

/** Small status pill: a colored dot plus the lowercased status label. */
export function StatusChip({ status }: { status: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-border px-2 py-0.5 text-xs font-medium">
      <span
        className="size-2 rounded-full"
        style={{ background: statusColorVar(status) }}
        aria-hidden
      />
      {statusLabel(status)}
    </span>
  );
}
