import type { ReactNode } from 'react';

/**
 * The title row at the top of a page. It sits flat on the window, with no
 * box. `children` is a short muted line after the title, such as a count.
 */
export function PageHeader({
  title,
  children,
}: {
  title: string;
  children?: ReactNode;
}): ReactNode {
  return (
    <header className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
      <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
      {children !== undefined && (
        <div className="text-sm text-muted-foreground">{children}</div>
      )}
    </header>
  );
}
