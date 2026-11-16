import { useCallback, useState } from 'react';

type SetValue<T> = (value: T | ((prev: T) => T)) => void;

/**
 * State mirrored into localStorage as JSON. Falls back to `defaultValue` on
 * parse errors or a missing key, and tolerates storage being unavailable
 * (private browsing, quota) by keeping the value in memory for the session.
 */
export function useLocalStorage<T>(
  key: string,
  defaultValue: T
): [T, SetValue<T>] {
  const [storedValue, setStoredValue] = useState<T>(() => {
    try {
      const item = window.localStorage.getItem(key);
      return item !== null ? (JSON.parse(item) as T) : defaultValue;
    } catch {
      return defaultValue;
    }
  });

  const setValue = useCallback<SetValue<T>>(
    value => {
      setStoredValue(prev => {
        const nextValue =
          typeof value === 'function' ? (value as (prev: T) => T)(prev) : value;
        try {
          window.localStorage.setItem(key, JSON.stringify(nextValue));
        } catch {
          // Storage unavailable; the value still applies for this session.
        }
        return nextValue;
      });
    },
    [key]
  );

  return [storedValue, setValue];
}
