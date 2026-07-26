import { describe, expect, it } from 'vitest';

import {
  visibleOptions,
  type SelectOption,
} from '@/components/ui/select-combobox';

const named = (...values: string[]): SelectOption[] =>
  values.map(value => ({ value }));

const generated = (count: number, prefix = 'wf'): SelectOption[] =>
  Array.from({ length: count }, (_, index) => ({
    value: `${prefix}-${index}`,
  }));

describe('visibleOptions', () => {
  it('returns every option when the query is empty and the cap allows it', () => {
    const options = named('alpha', 'beta', 'gamma');
    expect(visibleOptions(options, '', 50)).toEqual({
      options,
      overflow: 0,
    });
  });

  it('treats a whitespace-only query as empty', () => {
    const options = named('alpha', 'beta');
    expect(visibleOptions(options, '   ', 50).options).toEqual(options);
  });

  it('narrows on a case-insensitive substring of the display text', () => {
    const options = named('billing.invoice', 'billing.refund', 'reports.daily');
    const result = visibleOptions(options, 'BILL', 50);
    expect(result.options.map(option => option.value)).toEqual([
      'billing.invoice',
      'billing.refund',
    ]);
    expect(result.overflow).toBe(0);
  });

  it('matches the label rather than the value when a label is given', () => {
    const options: SelectOption[] = [
      { value: 'RUNNING', label: 'running' },
      { value: 'FAILED', label: 'failed' },
    ];
    expect(visibleOptions(options, 'fail', 50).options).toEqual([options[1]]);
  });

  it('reports no overflow when the match count equals the cap exactly', () => {
    const result = visibleOptions(generated(50), '', 50);
    expect(result.options).toHaveLength(50);
    expect(result.overflow).toBe(0);
  });

  it('caps the rendered list and reports the withheld remainder', () => {
    const result = visibleOptions(generated(4000), '', 50);
    expect(result.options).toHaveLength(50);
    expect(result.options[0]?.value).toBe('wf-0');
    expect(result.overflow).toBe(3950);
  });

  it('applies the cap after filtering, not before', () => {
    const options = [...generated(4000, 'other'), ...generated(3, 'billing')];
    const result = visibleOptions(options, 'billing', 50);
    expect(result.options.map(option => option.value)).toEqual([
      'billing-0',
      'billing-1',
      'billing-2',
    ]);
    expect(result.overflow).toBe(0);
  });

  it('reports zero matches as an empty list with no overflow', () => {
    expect(visibleOptions(named('alpha', 'beta'), 'zeta', 50)).toEqual({
      options: [],
      overflow: 0,
    });
  });

  it('renders nothing for a non-positive cap instead of slicing from the end', () => {
    expect(visibleOptions(named('alpha', 'beta'), '', 0)).toEqual({
      options: [],
      overflow: 2,
    });
    expect(visibleOptions(named('alpha', 'beta'), '', -5)).toEqual({
      options: [],
      overflow: 2,
    });
  });

  it('does not alias the caller-supplied option array', () => {
    const options = named('alpha');
    const result = visibleOptions(options, '', 50);
    expect(result.options).not.toBe(options);
  });
});
