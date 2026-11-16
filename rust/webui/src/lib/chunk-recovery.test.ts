import { describe, expect, it, vi } from 'vitest';

import {
  clearMarkerAfterHealthyBoot,
  decideRecovery,
  recoverFromPreloadError,
  RELOAD_MARKER_KEY,
  RECOVERY_WINDOW_MS,
  type RecoveryEnvironment,
} from '@/lib/chunk-recovery';

class FakeStorage {
  private readonly items = new Map<string, string>();

  getItem(key: string): string | null {
    return this.items.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.items.set(key, value);
  }

  removeItem(key: string): void {
    this.items.delete(key);
  }
}

function environment(marker: string | null, now: number) {
  const storage = new FakeStorage();
  if (marker !== null) {
    storage.setItem(RELOAD_MARKER_KEY, marker);
  }
  return {
    storage,
    reload: vi.fn<() => void>(),
    now: () => now,
  } satisfies RecoveryEnvironment;
}

describe('decideRecovery', () => {
  it('reloads on the first failure', () => {
    expect(decideRecovery(null, 1_000, RECOVERY_WINDOW_MS)).toBe('reload');
  });

  it('surfaces a second failure while the marker is fresh', () => {
    // The reload already happened and did not help: reloading again would spin.
    expect(decideRecovery('1000', 3_000, RECOVERY_WINDOW_MS)).toBe('surface');
  });

  it('surfaces a failure at the very start of the window', () => {
    expect(decideRecovery('1000', 1_000, RECOVERY_WINDOW_MS)).toBe('surface');
  });

  it('reloads again once the window has elapsed — that is a later deploy', () => {
    expect(decideRecovery('1000', 1_000 + RECOVERY_WINDOW_MS, RECOVERY_WINDOW_MS)).toBe(
      'reload'
    );
    expect(decideRecovery('1000', 900_000, RECOVERY_WINDOW_MS)).toBe('reload');
  });

  it('treats an unreadable marker as absent', () => {
    expect(decideRecovery('not-a-time', 1_000, RECOVERY_WINDOW_MS)).toBe('reload');
    expect(decideRecovery('', 1_000, RECOVERY_WINDOW_MS)).toBe('reload');
  });

  it('treats a future-dated marker as absent rather than trusting it', () => {
    // A clock change must not disable recovery until the clock catches up.
    expect(decideRecovery('9000', 1_000, RECOVERY_WINDOW_MS)).toBe('reload');
  });
});

describe('recoverFromPreloadError', () => {
  it('reloads and records when it did so', () => {
    const env = environment(null, 5_000);

    expect(recoverFromPreloadError(env)).toBe('reload');
    expect(env.reload).toHaveBeenCalledTimes(1);
    expect(env.storage.getItem(RELOAD_MARKER_KEY)).toBe('5000');
  });

  it('does not reload a second time inside the window', () => {
    const env = environment('5000', 6_000);

    expect(recoverFromPreloadError(env)).toBe('surface');
    expect(env.reload).not.toHaveBeenCalled();
    // The original marker stands: the window runs from the reload, not the
    // failures that follow it.
    expect(env.storage.getItem(RELOAD_MARKER_KEY)).toBe('5000');
  });

  it('heals once, then surfaces, for a chunk that is genuinely gone', () => {
    const storage = new FakeStorage();
    const reload = vi.fn();
    let clock = 1_000;
    const env: RecoveryEnvironment = { storage, reload, now: () => clock };

    expect(recoverFromPreloadError(env)).toBe('reload');
    clock += 500; // the reloaded tab lands on the same broken route
    expect(recoverFromPreloadError(env)).toBe('surface');
    clock += 200;
    expect(recoverFromPreloadError(env)).toBe('surface');

    expect(reload).toHaveBeenCalledTimes(1);
  });
});

describe('clearMarkerAfterHealthyBoot', () => {
  it('clears the marker after the guard window', () => {
    const env = environment('5000', 5_000);
    const scheduled: { callback: () => void; delayMs: number }[] = [];

    clearMarkerAfterHealthyBoot(env, (callback, delayMs) => {
      scheduled.push({ callback, delayMs });
    });

    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]?.delayMs).toBe(RECOVERY_WINDOW_MS);
    // Still armed while the boot is unproven.
    expect(env.storage.getItem(RELOAD_MARKER_KEY)).toBe('5000');

    scheduled[0]?.callback();

    expect(env.storage.getItem(RELOAD_MARKER_KEY)).toBeNull();
  });

  it('re-arms recovery for the next deploy once cleared', () => {
    const env = environment('5000', 5_000);
    clearMarkerAfterHealthyBoot(env, callback => {
      callback();
    });

    expect(recoverFromPreloadError(env)).toBe('reload');
    expect(env.reload).toHaveBeenCalledTimes(1);
  });
});
