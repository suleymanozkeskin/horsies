import { cn } from '@/lib/utils';
import type { ErrorCategory } from '@/types/tasks';

import { CategoryTag } from './error-chip';
import { ERROR_CATEGORIES } from './error-taxonomy';

/**
 * The categories the strip lists: those carrying tasks, plus any the filter is
 * currently engaged on.
 *
 * An engaged category stays listed even when its total is absent, because the
 * chip that produced the filter is also the only way to switch it off — a
 * control may not disappear while it is doing something.
 */
export function visibleCategories(
  totals: Partial<Record<ErrorCategory, number>>,
  active: readonly string[]
): ErrorCategory[] {
  const engaged = new Set(active);
  return ERROR_CATEGORIES.filter(
    category => (totals[category] ?? 0) > 0 || engaged.has(category)
  );
}

interface TaxonomyStripProps {
  /** Per-category task counts, whole — never scoped by `active`. */
  totals: Partial<Record<ErrorCategory, number>>;
  /** Categories the filter is engaged on. */
  active: string[];
  onToggle: (category: ErrorCategory) => void;
}

/**
 * Error-code counts rolled up by taxonomy family, each chip a filter toggle.
 * Counts every task carrying an error code (including completed tasks that
 * returned a domain error), so it is deliberately NOT labelled "failures".
 */
export function TaxonomyStrip({
  totals,
  active,
  onToggle,
}: TaxonomyStripProps) {
  const visible = visibleCategories(totals, active);
  if (visible.length === 0) {
    return null;
  }
  const engaged = new Set(active);
  return (
    <div className="glass flex flex-wrap items-center gap-2 rounded-lg border border-border px-3 py-2">
      <span
        className="mr-2 text-xs font-medium uppercase tracking-wide text-foreground"
        title="Tasks carrying an error code, grouped by family. Includes domain errors returned by completed tasks, so this is not the same as the failed count."
      >
        Error codes by category
      </span>
      {visible.map(category => {
        const isActive = engaged.has(category);
        return (
          <button
            key={category}
            type="button"
            aria-pressed={isActive}
            onClick={() => onToggle(category)}
            className={cn(
              'flex items-center gap-1.5 rounded-md border px-2 py-1 transition-colors',
              isActive
                ? 'border-primary ring-1 ring-primary'
                : 'border-border hover:bg-muted'
            )}
          >
            <CategoryTag category={category} />
            <span className="font-mono text-11 tabular-nums text-muted-foreground">
              {totals[category] ?? 0}
            </span>
          </button>
        );
      })}
    </div>
  );
}
