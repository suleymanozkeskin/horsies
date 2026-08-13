// Minimal toast surface. Actions need a notification channel that can carry a
// single affordance (Retry on a 503), and nothing more — so this is a provider,
// a queue, and a fixed-position list rather than a dependency.

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';

import { X } from 'lucide-react';

import { cn } from '@/lib/utils';

export type ToastTone = 'success' | 'error' | 'warning' | 'info';

export interface ToastAction {
  label: string;
  onSelect: () => void;
}

export interface ToastInput {
  tone: ToastTone;
  message: string;
  action?: ToastAction;
  /** Milliseconds before auto-dismiss. Omit for a sticky toast. */
  durationMs?: number;
}

interface Toast extends ToastInput {
  id: number;
}

interface ToastControls {
  notify: (input: ToastInput) => number;
  dismiss: (id: number) => void;
}

const ToastContext = createContext<ToastControls | null>(null);

export function useToast(): ToastControls {
  const controls = useContext(ToastContext);
  if (controls === null) {
    throw new Error('useToast must be used inside <ToastProvider>.');
  }
  return controls;
}

const TONE_COLOR: Record<ToastTone, string> = {
  success: 'var(--success)',
  error: 'var(--error)',
  warning: 'var(--warning-dark)',
  info: 'var(--info)',
};

const DEFAULT_DURATION_MS = 6_000;

export function ToastProvider({ children }: { children: ReactNode }): ReactNode {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const nextId = useRef(1);

  const dismiss = useCallback((id: number): void => {
    setToasts(current => current.filter(toast => toast.id !== id));
  }, []);

  const notify = useCallback(
    (input: ToastInput): number => {
      const id = nextId.current++;
      setToasts(current => [...current, { ...input, id }]);
      const duration = input.durationMs ?? DEFAULT_DURATION_MS;
      if (duration > 0) {
        window.setTimeout(() => dismiss(id), duration);
      }
      return id;
    },
    [dismiss]
  );

  const controls = useMemo<ToastControls>(
    () => ({ notify, dismiss }),
    [notify, dismiss]
  );

  return (
    <ToastContext.Provider value={controls}>
      {children}
      <div
        className="pointer-events-none fixed bottom-4 right-4 z-100 flex w-full max-w-sm flex-col gap-2"
        role="region"
        aria-label="Notifications"
      >
        {toasts.map(toast => (
          <div
            key={toast.id}
            role="status"
            className={cn(
              'glass pointer-events-auto flex items-start gap-3 rounded-lg border border-border p-3 text-sm shadow-lg'
            )}
            style={{ borderLeftColor: TONE_COLOR[toast.tone], borderLeftWidth: 3 }}
          >
            <span className="min-w-0 flex-1 break-words">{toast.message}</span>
            {toast.action && (
              <button
                type="button"
                onClick={() => {
                  dismiss(toast.id);
                  toast.action?.onSelect();
                }}
                className="shrink-0 rounded border border-border px-2 py-0.5 text-xs font-medium hover:bg-muted"
              >
                {toast.action.label}
              </button>
            )}
            <button
              type="button"
              onClick={() => dismiss(toast.id)}
              aria-label="Dismiss notification"
              className="shrink-0 rounded p-0.5 text-muted-foreground hover:text-foreground"
            >
              <X className="size-3.5" />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}
