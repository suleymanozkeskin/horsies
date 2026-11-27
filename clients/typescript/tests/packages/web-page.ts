// The checks of the monitoring page that the installed package serves: the facts of the two responses and the
// problems that they show. The install check (`consumer-check.ts`) reads the responses; these functions read their
// facts. The page and its script are in the addon (`core/webui-dist`), so a missing page shows in a package that
// installs cleanly and then fails every dashboard.

/** What the check reads of one response. */
export type ResponseFacts = { readonly status: number; readonly text: string };

/** The marker of the page's injected configuration. */
export const PAGE_CONFIG_MARKER = "__HORSIES_UI__";

/** The path of the first script asset that the page names, or why it names none. */
export type ScriptAsset = { readonly kind: "named"; readonly path: string } | { readonly kind: "notNamed" };

/** A script asset path in the page text: `assets/<name>.js`, ended by a quote. */
const SCRIPT_ASSET = /(assets\/[^"]+[.]js)/u;

/** The first script asset that the page text names. Pure. */
export function scriptAssetOf(pageText: string): ScriptAsset {
  const found = SCRIPT_ASSET.exec(pageText)?.[1];

  return found === undefined ? { kind: "notNamed" } : { kind: "named", path: found };
}

/** The problems of the page response: status 200, the injected configuration, a named script. Pure. */
export function pageProblems(page: ResponseFacts): readonly string[] {
  return [
    ...(page.status === 200 ? [] : [`GET / answered ${page.status}`]),
    ...(page.text.includes(PAGE_CONFIG_MARKER) ? [] : [`the page has no ${PAGE_CONFIG_MARKER}`]),
    ...(scriptAssetOf(page.text).kind === "named" ? [] : ["the page names no script asset"]),
  ];
}

/** The problems of the script response: status 200. Pure. */
export function scriptProblems(path: string, script: ResponseFacts): readonly string[] {
  return script.status === 200 ? [] : [`the script ${path} answered ${script.status}`];
}
