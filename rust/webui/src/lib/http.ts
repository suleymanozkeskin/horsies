// Typed fetch wrapper for the monitoring API.
//
// Two deliberate properties:
//   1. HTTP status is preserved on the thrown error. 404 (retention removed the
//      row), 409 (lost a CAS race) and 503 (broker unreachable) drive different
//      UX, so collapsing them into a boolean would erase the distinction.
//   2. Transport failures are a *different* error type from HTTP responses. A
//      POST that never got a response is not proof the action had no effect, so
//      the action layer must be able to tell the two apart.

import { uiConfig } from '@/lib/config';

/** A query value. Arrays serialize as repeated keys; null/undefined are dropped. */
export type QueryValue = string | number | boolean | string[] | null | undefined;

export type QueryParams = Record<string, QueryValue>;

/** The server answered with a non-2xx status. */
export class ApiError extends Error {
  readonly status: number;
  /** Machine-readable code from a 400/409 body, else null. */
  readonly code: string | null;
  /** Freshly re-read entity status from a 409 body, else null. */
  readonly currentStatus: string | null;
  /** Human-readable `detail` from the body, else null. */
  readonly detail: string | null;

  constructor(args: {
    status: number;
    code: string | null;
    currentStatus: string | null;
    detail: string | null;
  }) {
    super(args.detail ?? args.code ?? `HTTP ${args.status}`);
    this.name = 'ApiError';
    this.status = args.status;
    this.code = args.code;
    this.currentStatus = args.currentStatus;
    this.detail = args.detail;
  }
}

/** The request never produced a response (offline, DNS, abort, timeout). */
export class NetworkError extends Error {
  override readonly cause: unknown;

  constructor(cause: unknown) {
    super('The request could not be sent.');
    this.name = 'NetworkError';
    this.cause = cause;
  }
}

/**
 * Repeated-key serialization: `?status=A&status=B`, which is what FastAPI's
 * `list[str] = Query()` reads. Empty arrays, null and undefined are omitted so
 * an unset dimension never reaches the server as an empty filter.
 */
export function serializeParams(params: QueryParams): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        search.append(key, item);
      }
      continue;
    }
    search.append(key, String(value));
  }
  const query = search.toString();
  return query === '' ? '' : `?${query}`;
}

/** Pull `{code, current_status, detail}` out of an error body, tolerating any shape. */
function parseErrorBody(body: unknown): {
  code: string | null;
  currentStatus: string | null;
  detail: string | null;
} {
  if (body === null || typeof body !== 'object') {
    return { code: null, currentStatus: null, detail: null };
  }
  const record = body as Record<string, unknown>;
  const detail = record.detail;
  const code = record.code;
  const currentStatus = record.current_status;
  return {
    code: typeof code === 'string' ? code : null,
    currentStatus: typeof currentStatus === 'string' ? currentStatus : null,
    detail: typeof detail === 'string' ? detail : null,
  };
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text();
  if (text === '') {
    return null;
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    // A proxy or gateway can answer with HTML; keep it as the detail string.
    return { detail: text };
  }
}

async function request<T>(
  path: string,
  init: RequestInit,
  params?: QueryParams
): Promise<T> {
  const url = `${uiConfig.apiBase}${path}${params ? serializeParams(params) : ''}`;
  let response: Response;
  try {
    response = await fetch(url, init);
  } catch (cause) {
    throw new NetworkError(cause);
  }

  const body = await readJson(response);
  if (!response.ok) {
    const parsed = parseErrorBody(body);
    throw new ApiError({ status: response.status, ...parsed });
  }
  return body as T;
}

export function apiGet<T>(path: string, params?: QueryParams): Promise<T> {
  return request<T>(path, { method: 'GET', headers: { Accept: 'application/json' } }, params);
}

/**
 * Every mutating call carries `X-Horsies-Intent: action`. The server rejects a
 * POST without it with 403, which stops a cross-site form post from reaching an
 * action endpoint.
 */
export function apiPost<T>(path: string, body: unknown = {}): Promise<T> {
  return request<T>(path, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      'X-Horsies-Intent': 'action',
    },
    body: JSON.stringify(body),
  });
}
