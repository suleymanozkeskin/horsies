import * as React from 'react';
import type { ReactNode } from 'react';

import { cn } from '@/lib/utils';

/**
 * Navigation glass: a pill that groups controls in the top bar or in a
 * toolbar. It is `--capsule-height` tall; a control inside it is
 * `--capsule-item` tall (`Button` and `Toggle` size `capsule`). Content never
 * goes in a capsule.
 */
const Capsule = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement>
>(({ className, ...props }, ref) => (
  <div
    ref={ref}
    className={cn(
      'liquid-glass inline-flex h-(--capsule-height) shrink-0 items-center gap-0.5 rounded-full p-1',
      className
    )}
    {...props}
  />
));
Capsule.displayName = 'Capsule';

/** A thin vertical rule between two control groups in one capsule. */
function CapsuleSeparator({ className }: { className?: string }): ReactNode {
  return (
    <span
      aria-hidden
      className={cn('mx-1 h-4.5 w-px shrink-0 bg-hairline-strong', className)}
    />
  );
}

export { Capsule, CapsuleSeparator };
