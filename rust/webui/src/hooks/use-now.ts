import { useEffect, useState } from 'react';

/**
 * Current epoch ms, re-rendering every `intervalMs`. Keeps relative-time labels
 * ("3m ago", "pinged 12s ago") advancing on screens that don't otherwise poll,
 * instead of freezing at render time.
 */
export function useNow(intervalMs = 15_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs]);
  return now;
}
