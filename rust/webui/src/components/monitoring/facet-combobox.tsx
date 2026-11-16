import * as React from 'react';

import { Check, ChevronsUpDown } from 'lucide-react';

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
  /** Field label shown above the trigger. */
  label: string;
  options: FacetOption[];
  /** Selected values ("All" when empty). */
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
  className?: string;
}

/**
 * A multi-select scoped filter combobox: searchable, shows each value's task
 * count, toggles values without closing, and clears via "Clear". Values within
 * a dimension are OR-combined (the backend uses `IN`).
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

  const displayLabel =
    value.length === 0
      ? 'All'
      : value.length === 1
        ? value[0]
        : `${value.length} selected`;

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
    <div className="flex flex-col gap-1">
      <span className="text-xs font-medium text-muted-foreground">{label}</span>
      <Popover open={open} onOpenChange={setOpen} modal>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            aria-haspopup="listbox"
            aria-expanded={open}
            size="sm"
            className={cn(
              'h-8 justify-between gap-2 font-normal',
              value.length > 0 && 'border-primary',
              className
            )}
          >
            <span className="truncate">{displayLabel}</span>
            <ChevronsUpDown className="size-3.5 shrink-0 opacity-50" />
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
    </div>
  );
}
