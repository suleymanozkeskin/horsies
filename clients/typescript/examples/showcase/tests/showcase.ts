// The showcase app of a test: the app of the source as it is, with every task module.

import { appRefusedText, buildShowcase, type ShowcaseApp } from "../src/app.ts";
import type { DatabaseSettings } from "../src/settings.ts";

/** The showcase app of `settings`. A refusal throws: the tests build the app of the source as it is. */
export function testShowcase(settings: DatabaseSettings): ShowcaseApp {
  const built = buildShowcase(settings);

  if (!built.ok) {
    throw new Error(`the showcase app: ${appRefusedText(built.error)}`);
  }

  return built.value;
}
