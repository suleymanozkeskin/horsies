// Server-sent invalidation signals. Payloads are ids, never data: an event says
// "this surface changed", and the client refetches through the normal queries.

export type DataTopic = 'tasks' | 'workflows' | 'workers';

/**
 * `ids` is deduplicated per coalescing window and capped by the server; an
 * EMPTY array means "the cap was exceeded — invalidate the whole topic", not
 * "nothing changed". `degraded` means the server's listener died: it closes the
 * stream and the client falls back to interval polling.
 */
export type MonitoringEvent =
  | { topic: DataTopic; ids: string[] }
  | { topic: 'degraded' };

const isDataTopic = (value: unknown): value is DataTopic =>
  value === 'tasks' || value === 'workflows' || value === 'workers';

/** Parse one SSE `data:` payload. Returns null for anything unrecognised so a
 * future topic cannot crash an older client. */
export function parseMonitoringEvent(raw: string): MonitoringEvent | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== 'object') {
    return null;
  }
  const topic = (parsed as Record<string, unknown>).topic;
  if (topic === 'degraded') {
    return { topic: 'degraded' };
  }
  if (!isDataTopic(topic)) {
    return null;
  }
  const rawIds = (parsed as Record<string, unknown>).ids;
  const ids = Array.isArray(rawIds)
    ? rawIds.filter((id): id is string => typeof id === 'string')
    : [];
  return { topic, ids };
}
