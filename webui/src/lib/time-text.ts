const MS_PER_S = 1_000;

/** The fraction of the seconds of a time text with a zone (`.409945` in
 * `…:08.409945Z`). */
const SECOND_FRACTION = /\.(\d+)(?=Z$|[+-]\d{2}:\d{2}$)/;

/**
 * Epoch milliseconds of a UTC time text of the monitoring API
 * (`2026-11-17T13:59:14.500123Z`). The server writes every time in this form,
 * so a text that does not parse is a defect of the server contract: it throws
 * with the text. The microseconds stay, as a fraction of a millisecond: a
 * phase of 0.812ms keeps its 3 significant digits.
 */
export function instantMs(text: string): number {
  const fraction = SECOND_FRACTION.exec(text)?.[1];
  const seconds = Date.parse(fraction === undefined ? text : text.replace(SECOND_FRACTION, ''));
  if (Number.isNaN(seconds)) {
    throw new Error(`time text does not parse: ${JSON.stringify(text)}`);
  }
  return fraction === undefined ? seconds : seconds + Number(`0.${fraction}`) * MS_PER_S;
}
