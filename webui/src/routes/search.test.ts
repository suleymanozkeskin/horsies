import { describe, expect, it } from 'vitest';

import {
  filtersFromSearch,
  hasActiveFilters,
  validateTaskSearch,
  validateWorkflowSearch,
} from '@/routes/search';

describe('task search validation', () => {
  it('accepts a single repeatable param as a one-element array', () => {
    expect(validateTaskSearch({ status: 'FAILED' })).toEqual({
      status: ['FAILED'],
    });
  });

  it('accepts repeated params as an array', () => {
    expect(validateTaskSearch({ status: ['FAILED', 'EXPIRED'] })).toEqual({
      status: ['FAILED', 'EXPIRED'],
    });
  });

  it('drops unknown sort columns instead of forwarding them to the server', () => {
    expect(validateTaskSearch({ sort: 'DROP TABLE' })).toEqual({});
  });

  it('keeps an allowlisted sort column and direction', () => {
    expect(validateTaskSearch({ sort: 'exec_s', dir: 'asc' })).toEqual({
      sort: 'exec_s',
      dir: 'asc',
    });
  });

  it('drops a direction that is neither asc nor desc', () => {
    expect(validateTaskSearch({ dir: 'sideways' })).toEqual({});
  });

  it('coerces numeric params from strings and rejects non-positive pages', () => {
    expect(validateTaskSearch({ page: '3', size: '100' })).toEqual({
      page: 3,
      size: 100,
    });
    expect(validateTaskSearch({ page: '0' })).toEqual({});
    expect(validateTaskSearch({ page: '-1' })).toEqual({});
  });

  it('rejects a page size outside the offered set', () => {
    expect(validateTaskSearch({ size: '73' })).toEqual({});
  });

  it('accepts the boolean retried flag in its URL forms', () => {
    expect(validateTaskSearch({ retried: 'true' })).toEqual({ retried: true });
    expect(validateTaskSearch({ retried: '1' })).toEqual({ retried: true });
    expect(validateTaskSearch({ retried: 'false' })).toEqual({});
  });

  it('drops an unknown view', () => {
    expect(validateTaskSearch({ view: 'kanban' })).toEqual({});
    expect(validateTaskSearch({ view: 'queue' })).toEqual({ view: 'queue' });
  });

  it('ignores empty strings so a blank param never becomes a filter', () => {
    expect(validateTaskSearch({ task: '', status: '' })).toEqual({});
  });
});

describe('filters extracted from the URL', () => {
  it('maps only the dimensions the API takes', () => {
    const search = validateTaskSearch({
      status: ['FAILED'],
      worker: 'box-1',
      retried: 'true',
      sort: 'exec_s',
      page: '2',
    });
    expect(filtersFromSearch(search)).toEqual({
      status: ['FAILED'],
      worker: ['box-1'],
      retried_only: true,
    });
  });

  it('reports no active filters for a sort-only URL', () => {
    expect(hasActiveFilters(validateTaskSearch({ sort: 'exec_s' }))).toBe(false);
  });

  it('reports active filters once any dimension is set', () => {
    expect(hasActiveFilters(validateTaskSearch({ queue: 'default' }))).toBe(true);
  });
});

describe('workflow search validation', () => {
  it('keeps a run id and a node index', () => {
    expect(validateWorkflowSearch({ run: 'w1', node: '4' })).toEqual({
      run: 'w1',
      node: 4,
    });
  });

  it('accepts node index zero — task_index is zero-based', () => {
    expect(validateWorkflowSearch({ run: 'w1', node: 0 })).toEqual({
      run: 'w1',
      node: 0,
    });
  });

  it('drops a negative or non-numeric node index', () => {
    expect(validateWorkflowSearch({ run: 'w1', node: '-2' })).toEqual({
      run: 'w1',
    });
    expect(validateWorkflowSearch({ run: 'w1', node: 'first' })).toEqual({
      run: 'w1',
    });
  });
});
