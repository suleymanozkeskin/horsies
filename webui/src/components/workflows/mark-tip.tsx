// The tooltips of the timing views. Every mark of a strip, a bar chart, a
// timeline or a key has one: what the mark is, its duration, and its start →
// end. One floating box shows the tooltip of the mark under the pointer or
// under the keyboard focus.

import {
  useCallback,
  useEffect,
  useId,
  useState,
  type FocusEvent,
  type PointerEvent,
  type ReactNode,
} from 'react';

import { createPortal } from 'react-dom';

import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';

import { phaseFill, phaseLabel, type TimingKind } from './phase';

/** Offset of a floating tooltip from the pointer, and its widest box. */
const TOOLTIP_OFFSET_PX = 12;
const TOOLTIP_MAX_WIDTH_PX = 320;
/** Below this distance to the bottom of the window the tooltip opens above
 * the pointer. */
const TOOLTIP_FLIP_HEIGHT_PX = 240;
/** A tooltip for the keyboard focus opens under the focused mark, at most
 * this far right of its left edge. */
const FOCUS_ANCHOR_MAX_PX = 120;

/** A box that follows the pointer. It stays in the window: it moves left at
 * the right edge and opens above the pointer near the bottom. It is a child
 * of the document body: a view inside a blurred panel is the containing
 * block of its fixed children, which would shift the box by the offset of
 * the panel. */
export function FloatingTooltip({
  id,
  xPx,
  yPx,
  children,
}: {
  id: string;
  xPx: number;
  yPx: number;
  children: ReactNode;
}) {
  const maxLeft = window.innerWidth - TOOLTIP_MAX_WIDTH_PX - TOOLTIP_OFFSET_PX;
  const left = Math.max(Math.min(xPx + TOOLTIP_OFFSET_PX, maxLeft), 0);
  const above = yPx > window.innerHeight - TOOLTIP_FLIP_HEIGHT_PX;
  return createPortal(
    <div
      id={id}
      role="tooltip"
      className="pointer-events-none fixed z-50 flex flex-col gap-1 rounded-md border border-hairline-strong bg-popover px-2.5 py-2 text-xs text-popover-foreground shadow-(--menu-shadow)"
      style={{
        left,
        maxWidth: TOOLTIP_MAX_WIDTH_PX,
        ...(above
          ? { bottom: window.innerHeight - yPx + TOOLTIP_OFFSET_PX }
          : { top: yPx + TOOLTIP_OFFSET_PX }),
      }}
    >
      {children}
    </div>,
    document.body
  );
}

/** One mark with a tooltip: its id in its view, what its tooltip says, and
 * what it lights in its view while the tooltip shows. */
export interface MarkTip<H> {
  id: string;
  content: ReactNode;
  highlight: H;
}

/** The tooltip of a view: none, or the tooltip of one mark at a point. */
export type ShownTip<H> =
  | { kind: 'none' }
  | { kind: 'shown'; tip: MarkTip<H>; xPx: number; yPx: number };

/** The props that a mark spreads to show its tooltip on the pointer and on
 * the keyboard focus. */
export interface MarkTipProps {
  'aria-describedby': string | undefined;
  onPointerMove: (event: PointerEvent<HTMLElement>) => void;
  onPointerLeave: () => void;
  onFocus: (event: FocusEvent<HTMLElement>) => void;
  onBlur: () => void;
}

export interface MarkTips<H> {
  /** The id of the floating box, which the shown mark names as its
   * description. */
  id: string;
  shown: ShownTip<H>;
  bind: (tip: MarkTip<H>) => MarkTipProps;
  hide: () => void;
}

/** The tooltip state of one view. A scroll anywhere hides the tooltip: the
 * mark moves, and the box would point at nothing. */
export function useMarkTips<H>(): MarkTips<H> {
  const id = useId();
  const [shown, setShown] = useState<ShownTip<H>>({ kind: 'none' });
  const hide = useCallback(() => setShown({ kind: 'none' }), []);
  const showing = shown.kind === 'shown';
  useEffect(() => {
    if (!showing) {
      return undefined;
    }
    window.addEventListener('scroll', hide, { capture: true, passive: true });
    return () => window.removeEventListener('scroll', hide, { capture: true });
  }, [showing, hide]);
  const bind = (tip: MarkTip<H>): MarkTipProps => ({
    'aria-describedby': shown.kind === 'shown' && shown.tip.id === tip.id ? id : undefined,
    onPointerMove: event => setShown({ kind: 'shown', tip, xPx: event.clientX, yPx: event.clientY }),
    onPointerLeave: hide,
    onFocus: event => {
      const box = event.currentTarget.getBoundingClientRect();
      setShown({
        kind: 'shown',
        tip,
        xPx: box.left + Math.min(box.width / 2, FOCUS_ANCHOR_MAX_PX) - TOOLTIP_OFFSET_PX,
        yPx: box.bottom - TOOLTIP_OFFSET_PX / 2,
      });
    },
    onBlur: hide,
  });
  return { id, shown, bind, hide };
}

/** The floating box of a view, with the tooltip of its shown mark. */
export function ShownTipBox<H>({ tips }: { tips: MarkTips<H> }) {
  const shown = tips.shown;
  switch (shown.kind) {
    case 'none':
      return null;
    case 'shown':
      return (
        <FloatingTooltip id={tips.id} xPx={shown.xPx} yPx={shown.yPx}>
          {shown.tip.content}
        </FloatingTooltip>
      );
    default:
      return assertNever(shown, 'shown tooltip');
  }
}

/** The fill of a kind as a small swatch; dependency wait as a thin line. */
export function PhaseSwatch({ kind, thin = false }: { kind: TimingKind; thin?: boolean }) {
  return (
    <span
      className={thin ? 'inline-block h-0.5 w-3 shrink-0 rounded-full' : 'inline-block h-[9px] w-3 shrink-0 rounded-[2px]'}
      style={{ background: phaseFill(kind) }}
      aria-hidden
    />
  );
}

/** The first line of a tooltip: the mark, the name of what it is, where it
 * belongs, and its duration. */
export function TipHead({
  mark,
  name,
  context,
  value,
}: {
  mark?: ReactNode;
  name: string;
  context?: string;
  value?: string;
}) {
  return (
    <div className="flex items-center gap-1.5">
      {mark}
      <b className="shrink-0 font-semibold whitespace-nowrap">{name}</b>
      {context !== undefined && <span className="min-w-0 truncate text-muted-foreground">{context}</span>}
      {value !== undefined && (
        <span className="ml-auto shrink-0 pl-3.5 font-mono font-semibold whitespace-nowrap tabular-nums">{value}</span>
      )}
    </div>
  );
}

/** Times of day in a tooltip: `09:14:26.244 → 09:14:31.020`. */
export function TipTimes({ text }: { text: string }) {
  return <div className="font-mono text-11 leading-snug text-muted-foreground">{text}</div>;
}

/** A short phrase in a tooltip: what the mark is, or a fact of it. */
export function TipNote({ text }: { text: string }) {
  return <div className="text-11 text-muted-foreground">{text}</div>;
}

/** One time of a list of kinds. */
export interface TipPhaseItem {
  kind: TimingKind;
  ms: number;
}

/** The time of each kind of an attempt, a node or a run, as a list: fill,
 * name, time. */
export function TipPhaseList({ items }: { items: readonly TipPhaseItem[] }) {
  return (
    <div className="my-0.5 grid grid-cols-[auto_auto_minmax(0,1fr)] items-center gap-x-1.75 gap-y-0.5 text-11">
      {items.map(item => (
        <span key={item.kind} className="contents">
          <PhaseSwatch kind={item.kind} />
          <span className="whitespace-nowrap">{phaseLabel(item.kind)}</span>
          <span className="pl-3 text-right font-mono whitespace-nowrap tabular-nums">{formatMilliseconds(item.ms)}</span>
        </span>
      ))}
    </div>
  );
}
