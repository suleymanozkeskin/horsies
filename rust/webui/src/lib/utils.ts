import { clsx, type ClassValue } from 'clsx';
import { extendTailwindMerge } from 'tailwind-merge';

/**
 * The token set defines numeric font-size utilities (`text-9` … `text-13`, via
 * `--text-*` in app.css). Unextended tailwind-merge classifies `text-<number>`
 * as a text COLOR, where it collides with and evicts real color classes such as
 * `text-muted-foreground`. Registering the numeric sizes as font-sizes keeps
 * size and color in separate conflict groups. The validator matches any bare
 * integer, so new `--text-<n>` tokens are covered without editing this list.
 */
const twMerge = extendTailwindMerge({
  extend: {
    classGroups: {
      'font-size': [{ text: [(value: string) => /^\d+$/.test(value)] }],
    },
  },
});

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}
