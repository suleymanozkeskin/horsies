import { afterEach, describe, expect, it, vi } from 'vitest';

import { ApiError, apiGet, apiPost, NetworkError, serializeParams } from '@/lib/http';

/** The RequestInit the wrapper handed to fetch on its first (only) call. */
function requestInit(mock: { mock: { calls: unknown[][] } }): RequestInit {
  const call = mock.mock.calls[0];
  if (call === undefined) {
    throw new Error('fetch was never called');
  }
  return call[1] as RequestInit;
}

const jsonResponse = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('query serialization', () => {
  it('emits repeated keys for arrays', () => {
    expect(serializeParams({ status: ['FAILED', 'EXPIRED'] })).toBe(
      '?status=FAILED&status=EXPIRED'
    );
  });

  it('drops null, undefined and empty arrays', () => {
    expect(
      serializeParams({ a: null, b: undefined, c: [], d: 'kept' })
    ).toBe('?d=kept');
  });

  it('stringifies scalars', () => {
    expect(serializeParams({ limit: 50, retried_only: true })).toBe(
      '?limit=50&retried_only=true'
    );
  });

  it('returns an empty string when nothing survives', () => {
    expect(serializeParams({ a: undefined })).toBe('');
  });
});

describe('error mapping', () => {
  it('preserves 404 and its detail', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => jsonResponse(404, { detail: 'Task not found.' }))
    );
    const error = await apiGet('/tasks/x').catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(404);
    expect((error as ApiError).detail).toBe('Task not found.');
    expect((error as ApiError).code).toBeNull();
  });

  it('preserves 409 with the code and the re-read status', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse(409, {
          code: 'TASK_NOT_CANCELLABLE',
          current_status: 'COMPLETED',
        })
      )
    );
    const error = (await apiPost('/tasks/x/cancel').catch(
      (caught: unknown) => caught
    )) as ApiError;
    expect(error.status).toBe(409);
    expect(error.code).toBe('TASK_NOT_CANCELLABLE');
    expect(error.currentStatus).toBe('COMPLETED');
  });

  it('preserves 503 distinctly from a client error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => jsonResponse(503, { detail: 'tasks query failed: down' }))
    );
    const error = (await apiGet('/tasks').catch(
      (caught: unknown) => caught
    )) as ApiError;
    expect(error.status).toBe(503);
    expect(error.detail).toBe('tasks query failed: down');
  });

  it('keeps a non-JSON error body as the detail', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('<html>502</html>', { status: 502 }))
    );
    const error = (await apiGet('/tasks').catch(
      (caught: unknown) => caught
    )) as ApiError;
    expect(error.status).toBe(502);
    expect(error.detail).toBe('<html>502</html>');
  });

  it('raises NetworkError, not ApiError, when the request never lands', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new TypeError('Failed to fetch');
      })
    );
    const error = await apiPost('/tasks/x/cancel').catch(
      (caught: unknown) => caught
    );
    expect(error).toBeInstanceOf(NetworkError);
    expect(error).not.toBeInstanceOf(ApiError);
  });
});

describe('mutating requests', () => {
  it('sends the intent header and a JSON body', async () => {
    const fetchMock = vi.fn(async () => jsonResponse(200, { outcome: 'cancelled' }));
    vi.stubGlobal('fetch', fetchMock);

    await apiPost('/tasks/abc/cancel', { include_running: true });

    const init = requestInit(fetchMock);
    const headers = init.headers as Record<string, string>;
    expect(headers['X-Horsies-Intent']).toBe('action');
    expect(init.method).toBe('POST');
    expect(init.body).toBe(JSON.stringify({ include_running: true }));
  });

  it('does not send the intent header on reads', async () => {
    const fetchMock = vi.fn(async () => jsonResponse(200, []));
    vi.stubGlobal('fetch', fetchMock);

    await apiGet('/tasks');

    expect(requestInit(fetchMock).headers).not.toHaveProperty(
      'X-Horsies-Intent'
    );
  });
});
