// Runtime deployment config. The server injects `window.__HORSIES_UI__` into the
// served index so the same bundle works at any mount path.

export interface HorsiesUiConfig {
  /** Mount path the SPA is served under, e.g. `/` or `/monitoring`. */
  basePath: string;
  /** Absolute prefix for every API call, e.g. `/api` or `/monitoring/api`. */
  apiBase: string;
}

declare global {
  interface Window {
    __HORSIES_UI__?: Partial<HorsiesUiConfig>;
  }
}

/** Used by the Vite dev server, which serves the SPA without the FastAPI shell
 * and proxies `/api` to a locally running `horsies web`. */
const DEV_FALLBACK: HorsiesUiConfig = { basePath: '/', apiBase: '/api' };

/** Trailing slashes make `${apiBase}/tasks` double-slash; strip them once. */
const stripTrailingSlash = (value: string): string =>
  value.length > 1 && value.endsWith('/') ? value.slice(0, -1) : value;

export function readUiConfig(): HorsiesUiConfig {
  const injected = typeof window === 'undefined' ? undefined : window.__HORSIES_UI__;
  if (injected === undefined) {
    return DEV_FALLBACK;
  }
  const basePath = injected.basePath ?? DEV_FALLBACK.basePath;
  const apiBase = injected.apiBase ?? DEV_FALLBACK.apiBase;
  return {
    basePath: basePath === '' ? '/' : basePath,
    apiBase: stripTrailingSlash(apiBase),
  };
}

/** Resolved once at module load; the injected config never changes at runtime. */
export const uiConfig: HorsiesUiConfig = readUiConfig();
