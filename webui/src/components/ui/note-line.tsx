import { Info, TriangleAlert } from 'lucide-react';

import { assertNever } from '@/lib/exhaustive';

/** How loud a note is: `warning` for a fact the reader must see, `info` for a
 * fact that explains a number. */
export type NoteTone = 'warning' | 'info';

/** What a note says, and how loud it is. */
export interface NoteView {
  tone: NoteTone;
  text: string;
}

/** One fact in words, with an icon for its tone. */
export function NoteLine({ view }: { view: NoteView }) {
  switch (view.tone) {
    case 'warning':
      return (
        <p className="flex items-start gap-1.5 text-xs" role="note" data-tone="warning">
          <TriangleAlert
            className="mt-px size-3.5 shrink-0"
            style={{ color: 'var(--marked-ink)' }}
            aria-hidden
          />
          <span>{view.text}</span>
        </p>
      );
    case 'info':
      return (
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground" role="note" data-tone="info">
          <Info className="mt-px size-3.5 shrink-0" aria-hidden />
          <span>{view.text}</span>
        </p>
      );
    default:
      return assertNever(view.tone, 'note tone');
  }
}
