// The lines that a scenario writes for the operator: a heading, a line, an indented point. All go to stdout.

/** The base URL of the monitoring page: `ACME_WEB_URL`, else the address of the `web` command's defaults. */
export const WEB_URL_VARIABLE = "ACME_WEB_URL";
export const DEFAULT_WEB_URL = "http://127.0.0.1:8600";

/** The base URL of the monitoring page. Effects: reads the environment. */
export function webBaseUrl(): string {
  const configured = process.env[WEB_URL_VARIABLE] ?? "";

  return configured === "" ? DEFAULT_WEB_URL : configured;
}

/** Write one line. */
export function say(text: string): void {
  process.stdout.write(`${text}\n`);
}

/** Write a heading: an empty line, the text, and a rule under it. */
export function heading(text: string): void {
  say(`\n${text}\n${"-".repeat(text.length)}`);
}

/** Write one indented point. */
export function bullet(text: string): void {
  say(`  ${text}`);
}
