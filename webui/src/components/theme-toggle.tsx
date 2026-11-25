import type { ReactNode } from 'react';

import { Monitor, Moon, Sun } from 'lucide-react';

import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { useTheme, type ThemePreference } from '@/lib/theme';

const OPTIONS: { value: ThemePreference; label: string; icon: typeof Sun }[] = [
  { value: 'system', label: 'System theme', icon: Monitor },
  { value: 'light', label: 'Light theme', icon: Sun },
  { value: 'dark', label: 'Dark theme', icon: Moon },
];

/** Three-state theme control: follow the OS, or pin light/dark. A segmented
 * track, made to sit inside a top bar capsule. */
export function ThemeToggle(): ReactNode {
  const { preference, setPreference } = useTheme();
  return (
    <ToggleGroup
      type="single"
      value={preference}
      onValueChange={next => next && setPreference(next as ThemePreference)}
      variant="segment"
      size="segment"
      className="segment-track gap-0.5 rounded-full p-0.75"
      aria-label="Theme"
    >
      {OPTIONS.map(option => (
        <ToggleGroupItem
          key={option.value}
          value={option.value}
          aria-label={option.label}
          title={option.label}
        >
          <option.icon />
        </ToggleGroupItem>
      ))}
    </ToggleGroup>
  );
}
