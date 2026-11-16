// Recovery from a chunk that no longer exists.
//
// A dashboard tab outlives deploys. Chunk filenames carry a content hash, so a
// deploy replaces them and the old files stop being served; a tab left open
// across one then fails the dynamic import for any route it has not already
// loaded. That is worse than showing stale UI — the route does not render at
// all. Vite reports it as `vite:preloadError`, and reloading fixes it: the new
// index.html carries the new chunk names.
//
// Reloading on every such failure would be its own trap. When the asset is
// genuinely missing — a bad deploy, a half-populated CDN — the reloaded tab
// lands on the same route, fails the same import, and reloads again, forever.
// So a recovery reload leaves a timestamped marker in sessionStorage (per tab,
// gone when the tab closes), and a second failure while that marker is fresh
// surfaces the error instead of reloading.

/** sessionStorage key holding the epoch-ms of the last recovery reload. */
export const RELOAD_MARKER_KEY = 'horsies:chunk-reload-at';

/** How long after a recovery reload a repeat failure counts as a loop. */
export const RECOVERY_WINDOW_MS = 10_000;

/** Reload to pick up the new chunk names, or let the import error through. */
export type RecoveryDecision = 'reload' | 'surface';

/**
 * Decide what a failed chunk import deserves.
 *
 * The timestamp is the authority rather than a flag cleared at mount: it
 * expires on its own, so a boot that never completes cannot leave recovery
 * disabled for the life of the tab. A marker that is unreadable, or dated in
 * the future by a clock change, is treated as absent — one reload is the
 * healing move and it rewrites the marker correctly.
 */
export function decideRecovery(
  marker: string | null,
  now: number,
  windowMs: number
): RecoveryDecision {
  if (marker === null) {
    return 'reload';
  }
  const reloadedAt = Number.parseInt(marker, 10);
  if (!Number.isFinite(reloadedAt)) {
    return 'reload';
  }
  const elapsed = now - reloadedAt;
  return elapsed >= 0 && elapsed < windowMs ? 'surface' : 'reload';
}

/** The platform this module touches, so a test can supply its own. */
export interface RecoveryEnvironment {
  storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;
  reload: () => void;
  now: () => number;
  windowMs?: number;
}

/** Act on one preload failure, marking the reload it triggers. */
export function recoverFromPreloadError(
  environment: RecoveryEnvironment
): RecoveryDecision {
  const windowMs = environment.windowMs ?? RECOVERY_WINDOW_MS;
  const now = environment.now();
  const decision = decideRecovery(
    environment.storage.getItem(RELOAD_MARKER_KEY),
    now,
    windowMs
  );
  if (decision === 'reload') {
    environment.storage.setItem(RELOAD_MARKER_KEY, String(now));
    environment.reload();
  }
  return decision;
}

/**
 * Forget the marker once the tab has stayed up past the guard window.
 *
 * Clearing it at mount would re-arm the reload before the session had proven
 * anything, and a route whose chunk is truly gone would boot, fail, reload, and
 * repeat. Waiting the window out means the next failure is a later deploy
 * rather than the same one.
 */
export function clearMarkerAfterHealthyBoot(
  environment: RecoveryEnvironment,
  schedule: (callback: () => void, delayMs: number) => void
): void {
  schedule(
    () => environment.storage.removeItem(RELOAD_MARKER_KEY),
    environment.windowMs ?? RECOVERY_WINDOW_MS
  );
}

/**
 * Read `sessionStorage`, which throws outright when storage is blocked.
 *
 * There is no predicate for "blocked" short of touching it, and recovery is a
 * nicety: without storage this returns null and chunk errors are left to
 * surface, rather than taking the app down at boot over a missing convenience.
 */
function sessionStorageOrNull(): Storage | null {
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

/** Listen for chunk-preload failures for the rest of this tab's life. */
export function installChunkRecovery(): void {
  const storage = sessionStorageOrNull();
  if (storage === null) {
    return;
  }
  const environment: RecoveryEnvironment = {
    storage,
    reload: () => {
      window.location.reload();
    },
    now: () => Date.now(),
  };
  window.addEventListener('vite:preloadError', event => {
    if (recoverFromPreloadError(environment) === 'reload') {
      // Suppress the throw only when a reload is already on its way to
      // replace this document.
      event.preventDefault();
    }
  });
  clearMarkerAfterHealthyBoot(environment, (callback, delayMs) => {
    window.setTimeout(callback, delayMs);
  });
}
