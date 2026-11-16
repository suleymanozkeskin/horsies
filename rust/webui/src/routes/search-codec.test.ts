import { describe, expect, it } from 'vitest';

import { parseSearch, stringifySearch } from '@/routes/search-codec';
import { validateTaskSearch, validateWorkflowSearch } from '@/routes/search';

describe('stringifySearch', () => {
  it('writes the URL a person would write', () => {
    expect(
      stringifySearch({
        status: ['PENDING', 'COMPLETED', 'FAILED', 'RUNNING'],
        view: 'flat',
        page: 2,
      })
    ).toBe('?status=PENDING,COMPLETED,FAILED,RUNNING&view=flat&page=2');
  });

  it('leaves scalars bare rather than JSON-quoting them', () => {
    expect(stringifySearch({ run: 'nightly-report', node: 4 })).toBe(
      '?run=nightly-report&node=4'
    );
  });

  it('writes booleans bare', () => {
    expect(stringifySearch({ retried: true })).toBe('?retried=true');
  });

  it('omits absent and empty values entirely', () => {
    expect(
      stringifySearch({
        status: [],
        task: '',
        page: undefined,
        view: null,
      })
    ).toBe('');
  });

  it('drops only the empty dimensions, keeping the rest', () => {
    expect(stringifySearch({ status: [], view: 'queue' })).toBe('?view=queue');
  });

  it('percent-encodes a comma inside a value so separators stay unambiguous', () => {
    expect(stringifySearch({ task_name: ['a,b', 'plain'] })).toBe(
      '?task_name=a%2Cb,plain'
    );
  });

  it('percent-encodes a percent sign', () => {
    expect(stringifySearch({ error_code: ['50%,off'] })).toBe(
      '?error_code=50%25%2Coff'
    );
  });

  it('encodes spaces and ampersands rather than breaking the query', () => {
    expect(stringifySearch({ task_name: ['a b&c=d'] })).toBe(
      '?task_name=a%20b%26c%3Dd'
    );
  });
});

describe('parseSearch', () => {
  it('reads a comma-joined list back as its values', () => {
    expect(parseSearch('?status=PENDING,COMPLETED')).toEqual({
      status: ['PENDING', 'COMPLETED'],
    });
  });

  it('reads a single value as a scalar', () => {
    expect(parseSearch('?view=queue')).toEqual({ view: 'queue' });
  });

  it('tolerates a missing leading question mark', () => {
    expect(parseSearch('view=queue')).toEqual({ view: 'queue' });
  });

  it('returns nothing for an empty query', () => {
    expect(parseSearch('')).toEqual({});
    expect(parseSearch('?')).toEqual({});
  });

  it('accepts the legacy JSON array so old bookmarks still resolve', () => {
    expect(
      parseSearch(
        '?status=%5B%22PENDING%22%2C%22COMPLETED%22%2C%22FAILED%22%2C%22RUNNING%22%5D'
      )
    ).toEqual({ status: ['PENDING', 'COMPLETED', 'FAILED', 'RUNNING'] });
  });

  it('accepts a legacy JSON-quoted scalar, which is how run ids were written', () => {
    expect(parseSearch('?run=%22w-123%22')).toEqual({ run: 'w-123' });
  });

  it('keeps text that merely opens with a bracket or quote', () => {
    expect(parseSearch('?task_name=%5Bdraft%5D')).toEqual({
      task_name: '[draft]',
    });
    expect(parseSearch('?task_name=%22loose')).toEqual({
      task_name: '"loose',
    });
  });

  it('keeps a numeric-looking value as text for the validator to coerce', () => {
    // Coercing here would turn a run named "2024" into a number, which the
    // validator then drops as the wrong type.
    expect(parseSearch('?run=2024&page=2')).toEqual({
      run: '2024',
      page: '2',
    });
  });

  it('merges a repeated key instead of letting the last one win', () => {
    expect(parseSearch('?status=FAILED&status=EXPIRED')).toEqual({
      status: ['FAILED', 'EXPIRED'],
    });
    expect(parseSearch('?status=FAILED,EXPIRED&status=PENDING')).toEqual({
      status: ['FAILED', 'EXPIRED', 'PENDING'],
    });
  });

  it('reads a valueless key as empty text', () => {
    expect(parseSearch('?status=')).toEqual({ status: '' });
    expect(parseSearch('?status')).toEqual({ status: '' });
  });

  it('leaves a malformed escape as written rather than throwing', () => {
    expect(parseSearch('?task_name=%ZZ')).toEqual({ task_name: '%ZZ' });
  });
});

describe('round trip', () => {
  it('survives every task dimension, including the error category', () => {
    const search = {
      status: ['FAILED', 'EXPIRED'],
      error_category: ['OPERATIONAL', 'DOMAIN'],
      error_code: ['TASK_EXCEPTION'],
      task_name: ['billing,report'],
      retried: true,
      view: 'queue' as const,
      page: 3,
      size: 100,
      task: 'abc-123',
    };

    const url = stringifySearch(search);

    expect(url).toBe(
      '?status=FAILED,EXPIRED' +
        '&error_category=OPERATIONAL,DOMAIN' +
        '&error_code=TASK_EXCEPTION' +
        '&task_name=billing%2Creport' +
        '&retried=true' +
        '&view=queue' +
        '&page=3' +
        '&size=100' +
        '&task=abc-123'
    );
    expect(validateTaskSearch(parseSearch(url))).toEqual(search);
  });

  it('survives a workflow deep link', () => {
    const search = { run: 'w-2024', node: 0 };

    const url = stringifySearch(search);

    expect(url).toBe('?run=w-2024&node=0');
    expect(validateWorkflowSearch(parseSearch(url))).toEqual(search);
  });

  it('feeds the validators a legacy URL unchanged in meaning', () => {
    const legacy = '?status=%5B%22FAILED%22%5D&view=%22queue%22&page=2';

    expect(validateTaskSearch(parseSearch(legacy))).toEqual({
      status: ['FAILED'],
      view: 'queue',
      page: 2,
    });
  });
});
