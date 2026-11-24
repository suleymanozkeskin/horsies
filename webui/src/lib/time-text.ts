/**
 * Epoch milliseconds of a UTC time text of the monitoring API
 * (`2026-11-17T13:59:14.500000Z`). The server writes every time in this form,
 * so a text that does not parse is a defect of the server contract: it throws
 * with the text. Microseconds below one millisecond are dropped.
 */
export function instantMs(text: string): number {
  const ms = Date.parse(text);
  if (Number.isNaN(ms)) {
    throw new Error(`time text does not parse: ${JSON.stringify(text)}`);
  }
  return ms;
}
