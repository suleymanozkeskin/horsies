import * as React from 'react';

import { Check, ChevronsUpDown } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Command,
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

export interface SelectOption {
  value: string;
  /** Display text; defaults to `value`. */
  label?: string;
  /** Rendered in a trailing column when present. */
  count?: number;
  /** Decoration before the label (e.g. a status dot). */
  adornment?: React.ReactNode;
}

/** The options to render plus how many matches the cap withheld. */
export interface VisibleOptions {
  options: SelectOption[];
  overflow: number;
}

/** Display text for an option. */
function optionLabel(option: SelectOption): string {
  return option.label ?? option.value;
}

/** Options rendered at once before the query must narrow further. */
const DEFAULT_CAP = 50;

/**
 * Case-insensitive substring match on the display text, truncated to `cap`.
 * Option sets are unbounded (workflow names grow with the deployment), so the
 * list is never rendered whole: `overflow` is what the cap withheld, and the
 * caller surfaces it as a "keep typing" hint.
 */
export function visibleOptions(
  options: readonly SelectOption[],
  query: string,
  cap: number
): VisibleOptions {
  const needle = query.trim().toLowerCase();
  const matched =
    needle === ''
      ? [...options]
      : options.filter(option =>
          optionLabel(option).toLowerCase().includes(needle)
        );
  const limit = Math.max(0, cap);
  return {
    options: matched.slice(0, limit),
    overflow: Math.max(0, matched.length - limit),
  };
}

interface SelectComboboxProps {
  /** Field label shown above the trigger. */
  label: string;
  /** Text of the top row that clears the selection (e.g. "All workflows"). */
  emptyLabel: string;
  options: SelectOption[];
  /** Selected value; `null` is the cleared state. */
  value: string | null;
  onChange: (value: string | null) => void;
  /** Search-box placeholder. */
  placeholder?: string;
  /** Max options rendered at once. */
  cap?: number;
  /** Trigger id; generated when omitted. */
  id?: string;
  className?: string;
}

/**
 * A single-select filter combobox: searchable, keyboard-navigable, and capped
 * so a large option set stays renderable. Picking an option — or the clear row
 * — closes the popover.
 */
export function SelectCombobox({
  label,
  emptyLabel,
  options,
  value,
  onChange,
  placeholder = 'Search…',
  cap = DEFAULT_CAP,
  id,
  className,
}: SelectComboboxProps) {
  const [open, setOpen] = React.useState(false);
  const [query, setQuery] = React.useState('');
  const generatedId = React.useId();
  const triggerId = id ?? `${generatedId}-trigger`;
  const labelId = `${generatedId}-label`;

  const { options: visible, overflow } = React.useMemo(
    () => visibleOptions(options, query, cap),
    [options, query, cap]
  );

  const selected =
    value === null ? undefined : options.find(option => option.value === value);
  const displayLabel =
    value === null ? emptyLabel : selected ? optionLabel(selected) : value;

  const changeOpen = (next: boolean): void => {
    setOpen(next);
    if (!next) {
      setQuery('');
    }
  };

  const pick = (next: string | null): void => {
    onChange(next);
    changeOpen(false);
  };

  return (
    <div className="flex flex-col gap-1">
      <span
        id={labelId}
        className="text-xs font-medium text-muted-foreground"
      >
        {label}
      </span>
      <Popover open={open} onOpenChange={changeOpen} modal>
        <PopoverTrigger asChild>
          <Button
            id={triggerId}
            variant="outline"
            // The trigger's own text is part of its name, so it reads as
            // "Workflow, All workflows" rather than just the field label.
            aria-labelledby={`${labelId} ${triggerId}`}
            aria-haspopup="listbox"
            aria-expanded={open}
            size="sm"
            className={cn(
              'h-8 w-full justify-between gap-2 font-normal',
              value !== null && 'border-primary',
              className
            )}
          >
            <span className="flex min-w-0 items-center gap-1.5">
              {selected?.adornment}
              <span className="min-w-0 truncate" title={displayLabel}>
                {displayLabel}
              </span>
            </span>
            <ChevronsUpDown className="size-3.5 shrink-0 opacity-50" />
          </Button>
        </PopoverTrigger>
        <PopoverContent
          className="w-[min(16rem,calc(100vw-2rem))] p-0"
          align="start"
        >
          {/* Filtering and capping are ours (`visibleOptions`), so cmdk must
              not filter the already-narrowed list a second time. */}
          <Command shouldFilter={false}>
            <CommandInput
              value={query}
              onValueChange={setQuery}
              placeholder={placeholder}
            />
            <CommandList className="max-h-72">
              <CommandGroup>
                <CommandItem value="__clear__" onSelect={() => pick(null)}>
                  <Check
                    className={cn(
                      'mr-2 size-4 shrink-0',
                      value === null ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  <span className="min-w-0 truncate">{emptyLabel}</span>
                </CommandItem>
              </CommandGroup>
              <CommandSeparator />
              {visible.length === 0 ? (
                <div
                  role="presentation"
                  className="py-6 text-center text-sm text-muted-foreground"
                >
                  No matches.
                </div>
              ) : (
                <CommandGroup>
                  {visible.map(option => (
                    <CommandItem
                      key={option.value}
                      // Prefixed so no option can collide with the clear row.
                      value={`option:${option.value}`}
                      onSelect={() => pick(option.value)}
                    >
                      <Check
                        className={cn(
                          'mr-2 size-4 shrink-0',
                          option.value === value ? 'opacity-100' : 'opacity-0'
                        )}
                      />
                      {option.adornment}
                      <span
                        className="min-w-0 truncate"
                        title={optionLabel(option)}
                      >
                        {optionLabel(option)}
                      </span>
                      {option.count !== undefined && (
                        <span className="ml-auto pl-2 font-mono text-11 tabular-nums text-muted-foreground">
                          {option.count}
                        </span>
                      )}
                    </CommandItem>
                  ))}
                </CommandGroup>
              )}
            </CommandList>
            {overflow > 0 && (
              <div className="border-t border-border px-3 py-2 text-11 text-muted-foreground">
                {overflow} more — keep typing to narrow
              </div>
            )}
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  );
}
