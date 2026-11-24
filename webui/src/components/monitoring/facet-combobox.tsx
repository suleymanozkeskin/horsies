import * as React from 'react';

import { Check, ChevronDown } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from '@/components/ui/command';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';
import { cn } from '@/lib/utils';

export interface FacetOption {
  value: string;
  count: number;
  /** Optional decoration rendered before the label (e.g. a category dot). */
  adornment?: React.ReactNode;
}

interface FacetComboboxProps {
  /** Field name, shown first in the trigger. */
  label: string;
  options: FacetOption[];
  /** Selected values; empty means no filter on this field. */
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
  className?: string;
}

/** The trigger text: the field name alone with no selection, then the value
 * with one selected, or the count with more. */
function triggerText(label: string, value: readonly string[]): string {
  const [first, ...rest] = value;
  if (first === undefined) {
    return label;
  }
  return rest.length === 0
    ? `${label}: ${first}`
    : `${label}: ${value.length} selected`;
}

/**
 * A multi-select scoped filter combobox: searchable, shows each value's task
 * count, toggles values without closing, and clears via "Clear". Values within
 * a dimension are OR-combined (the backend uses `IN`). The trigger is a
 * capsule control: it goes directly inside a `Capsule`.
 */
export function FacetCombobox({
  label,
  options,
  value,
  onChange,
  placeholder = 'Search…',
  className,
}: FacetComboboxProps) {
  const [open, setOpen] = React.useState(false);
  const selected = new Set(value);

  const toggle = (candidate: string): void => {
    const next = new Set(selected);
    if (next.has(candidate)) {
      next.delete(candidate);
    } else {
      next.add(candidate);
    }
    onChange([...next]);
  };

  return (
    <Popover open={open} onOpenChange={setOpen} modal>
      <PopoverTrigger asChild>
        <Button
          variant="capsule"
          aria-haspopup="listbox"
          aria-expanded={open}
          size="capsule"
          className={cn(
            'max-w-64 shrink-0 gap-1.5',
            value.length > 0 && 'bg-accent-surface hover:bg-accent-surface',
            className
          )}
        >
          <span className="truncate">{triggerText(label, value)}</span>
          <ChevronDown className="text-muted-foreground" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-64 p-0" align="start">
        <Command>
          <CommandInput placeholder={placeholder} />
          <CommandList>
            <CommandEmpty>No matches.</CommandEmpty>
            {value.length > 0 && (
              <>
                <CommandGroup>
                  <CommandItem value="__all__" onSelect={() => onChange([])}>
                    <span className="mr-2 size-4" />
                    Clear ({value.length})
                  </CommandItem>
                </CommandGroup>
                <CommandSeparator />
              </>
            )}
            <CommandGroup>
              {options.map(option => (
                <CommandItem
                  key={option.value}
                  value={option.value}
                  onSelect={() => toggle(option.value)}
                >
                  <Check
                    className={cn(
                      'mr-2 size-4 shrink-0',
                      selected.has(option.value) ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  {option.adornment}
                  <span className="truncate">{option.value}</span>
                  <span className="ml-auto pl-2 font-mono text-11 tabular-nums text-muted-foreground">
                    {option.count}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
