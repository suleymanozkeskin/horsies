// Maps horsies error-code families to tokens + human labels, so the monitor can
// differentiate *why* a task failed: infra vs a code/contract bug vs a transient
// retrieval state vs a terminal lifecycle outcome vs a business-domain error the
// task itself returned. Categories come from the backend; this is display only.

import type { ErrorCategory } from '@/types/tasks';

interface CategoryMeta {
  /** CSS variable reference for the category's representative color. */
  colorVar: string;
  /** Short label for chips/legends. */
  label: string;
  /** One-line explanation for tooltips and the taxonomy legend. */
  blurb: string;
}

const META: Record<ErrorCategory, CategoryMeta> = {
  OPERATIONAL: {
    colorVar: 'var(--error)',
    label: 'Infrastructure',
    blurb:
      'Worker / broker / serialization failure — the platform, not your code.',
  },
  CONTRACT: {
    colorVar: 'var(--warning-dark)',
    label: 'Contract',
    blurb: 'Return-type or workflow-context violation — a task definition bug.',
  },
  RETRIEVAL: {
    colorVar: 'var(--info)',
    label: 'Retrieval',
    blurb: 'Result not ready / not found / wait timed out — often transient.',
  },
  OUTCOME: {
    colorVar: 'var(--cancelled)',
    label: 'Lifecycle',
    blurb: 'Terminal lifecycle outcome — cancelled, expired, timed out, paused.',
  },
  DOMAIN: {
    colorVar: 'var(--muted-foreground)',
    label: 'Domain',
    blurb: 'A business error the task itself returned (user-defined code).',
  },
};

/** Categories in display order (most operationally urgent first). */
export const ERROR_CATEGORIES: ErrorCategory[] = [
  'OPERATIONAL',
  'CONTRACT',
  'RETRIEVAL',
  'OUTCOME',
  'DOMAIN',
];

export const errorCategoryColorVar = (category: ErrorCategory): string =>
  META[category].colorVar;

export const errorCategoryLabel = (category: ErrorCategory): string =>
  META[category].label;

export const errorCategoryBlurb = (category: ErrorCategory): string =>
  META[category].blurb;
