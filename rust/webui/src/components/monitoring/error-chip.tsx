import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import type { ErrorCategory } from '@/types/tasks';

import {
  errorCategoryBlurb,
  errorCategoryColorVar,
  errorCategoryLabel,
} from './error-taxonomy';

/**
 * Error code rendered with its taxonomy category: a category-colored left bar,
 * the code in mono, and a tooltip explaining the family. `category` may be null
 * for codes that predate the taxonomy; we fall back to DOMAIN styling.
 */
export function ErrorChip({
  code,
  category,
}: {
  code: string | null;
  category: ErrorCategory | null;
}) {
  if (!code) {
    return <span className="text-muted-foreground">—</span>;
  }
  const cat: ErrorCategory = category ?? 'DOMAIN';
  const color = errorCategoryColorVar(cat);
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            className="inline-flex max-w-full items-center gap-1.5 rounded border border-border py-0.5 pl-1.5 pr-2 text-xs"
            style={{ borderLeftColor: color, borderLeftWidth: 3 }}
          >
            <span className="truncate font-mono text-11">{code}</span>
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs">
          <p className="font-medium" style={{ color }}>
            {errorCategoryLabel(cat)}
          </p>
          <p className="text-xs text-muted-foreground">
            {errorCategoryBlurb(cat)}
          </p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** Just the category label as a colored dot + word (for the taxonomy legend). */
export function CategoryTag({ category }: { category: ErrorCategory }) {
  const color = errorCategoryColorVar(category);
  return (
    <span className="inline-flex items-center gap-1.5 text-xs">
      <span
        className="size-2 rounded-full"
        style={{ background: color }}
        aria-hidden
      />
      {errorCategoryLabel(category)}
    </span>
  );
}
