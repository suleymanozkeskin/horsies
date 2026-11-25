import * as React from 'react';

import * as TogglePrimitive from '@radix-ui/react-toggle';
import { cva, type VariantProps } from 'class-variance-authority';

import { cn } from '@/lib/utils';

const toggleVariants = cva(
  'inline-flex items-center justify-center gap-2 rounded-md text-sm font-medium transition-colors hover:bg-surface-hover hover:text-foreground focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-accent-surface data-[state=on]:text-foreground [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0',
  {
    variants: {
      variant: {
        default: 'bg-transparent',
        outline:
          'border border-input bg-transparent hover:bg-accent-surface hover:text-foreground',
        // A segment in a `segment-track` group, or a control in a `Capsule`.
        // The pressed item is a raised pill.
        segment:
          'rounded-full bg-transparent text-foreground data-[state=on]:bg-segment-on data-[state=on]:shadow-(--segment-on-shadow)',
      },
      size: {
        default: 'h-10 min-w-10 px-3',
        sm: 'h-9 min-w-9 px-2.5',
        lg: 'h-11 min-w-11 px-5',
        // Inside a `segment-track` group.
        segment:
          'h-(--segment-item) min-w-(--segment-item) px-2 text-xs [&_svg]:size-3.5',
        // Directly inside a `Capsule`.
        capsule: 'h-(--capsule-item) min-w-(--capsule-item) px-3 text-13',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  }
);

const Toggle = React.forwardRef<
  React.ComponentRef<typeof TogglePrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof TogglePrimitive.Root> &
    VariantProps<typeof toggleVariants>
>(({ className, variant, size, ...props }, ref) => (
  <TogglePrimitive.Root
    ref={ref}
    className={cn(toggleVariants({ variant, size, className }))}
    {...props}
  />
));
Toggle.displayName = TogglePrimitive.Root.displayName;

export { Toggle, toggleVariants };
