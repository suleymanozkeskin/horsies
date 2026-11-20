/** The placeholder of a trace link template; the server checks that a
 * template holds it once. */
export const TRACE_ID_PLACEHOLDER = '{trace_id}';

/** The link of `traceId` under `template`: the template with its one
 * placeholder replaced. A trace id is 32 hex digits, so it needs no
 * escaping. */
export function traceLinkOf(template: string, traceId: string): string {
  return template.replace(TRACE_ID_PLACEHOLDER, traceId);
}
