// Class-strategy dark mode: `.dark` on <html>, three-state preference persisted
// in localStorage. No theme library — the whole contract is one class and one
// stored string.

import { useCallback, useEffect, useState } from 'react';

export type ThemePreference = 'system' | 'light' | 'dark';

const STORAGE_KEY = 'horsies-theme';
const DARK_QUERY = '(prefers-color-scheme: dark)';

const isPreference = (value: string | null): value is ThemePreference =>
  value === 'system' || value === 'light' || value === 'dark';

function readStoredPreference(): ThemePreference {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    return isPreference(stored) ? stored : 'system';
  } catch {
    // localStorage is unavailable in private browsing / sandboxed frames.
    return 'system';
  }
}

function resolve(preference: ThemePreference): 'light' | 'dark' {
  switch (preference) {
    case 'light':
      return 'light';
    case 'dark':
      return 'dark';
    case 'system':
      return window.matchMedia(DARK_QUERY).matches ? 'dark' : 'light';
  }
}

function applyToDocument(resolved: 'light' | 'dark'): void {
  document.documentElement.classList.toggle('dark', resolved === 'dark');
  document.documentElement.style.colorScheme = resolved;
}

/** Reads the stored preference and applies the class before first paint. */
export function initTheme(): void {
  applyToDocument(resolve(readStoredPreference()));
}

export interface ThemeControls {
  preference: ThemePreference;
  resolved: 'light' | 'dark';
  setPreference: (next: ThemePreference) => void;
}

export function useTheme(): ThemeControls {
  const [preference, setPreferenceState] =
    useState<ThemePreference>(readStoredPreference);
  const [resolved, setResolved] = useState<'light' | 'dark'>(() =>
    resolve(preference)
  );

  useEffect(() => {
    const next = resolve(preference);
    setResolved(next);
    applyToDocument(next);
    if (preference !== 'system') {
      return;
    }
    // Only 'system' tracks the OS; an explicit choice must not be overridden.
    const media = window.matchMedia(DARK_QUERY);
    const onChange = (): void => {
      const followed = media.matches ? 'dark' : 'light';
      setResolved(followed);
      applyToDocument(followed);
    };
    media.addEventListener('change', onChange);
    return () => media.removeEventListener('change', onChange);
  }, [preference]);

  const setPreference = useCallback((next: ThemePreference): void => {
    setPreferenceState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // Preference stays in-memory for this session.
    }
  }, []);

  return { preference, resolved, setPreference };
}
