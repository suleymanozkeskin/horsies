import type { ReactNode } from 'react';

import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';

/** The wait before a hint opens: short, so a mark explains itself at once. */
const HINT_DELAY_MS = 120;

/**
 * Text that explains an element on hover, with no wait of a native title.
 * The same text is the accessible description of the element, so a screen
 * reader reads it without the pointer.
 */
export function Hint({
  text,
  className,
  children,
}: {
  text: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <TooltipProvider delayDuration={HINT_DELAY_MS}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className={className} aria-description={text}>
            {children}
          </span>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs text-xs leading-snug">{text}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
