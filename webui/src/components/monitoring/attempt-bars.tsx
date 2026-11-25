// The bars of the attempt panel: one bar for each interval on a log axis of
// duration, so 1ms, 3ms and 7ms get different lengths beside a running time
// of seconds. The bars are grouped by phase or by attempt; both groupings
// show the same bars, in a different order.

import type { ReactNode } from 'react';

import { TipHead, TipNote, type MarkTip, type MarkTipProps } from '@/components/workflows/mark-tip';
import { OutcomeCap } from '@/components/workflows/outcome-sign';
import { phaseFill, phaseLabel, type TimingKind } from '@/components/workflows/phase';
import { LogAxisLabels, LogLines } from '@/components/workflows/timing-axes';
import { logDomain, logPct, type LogDomain } from '@/components/workflows/time-scale';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import { cn } from '@/lib/utils';

import {
  attemptLit,
  intervalLit,
  intervalMs,
  longestIntervalMs,
  ownerText,
  panelKinds,
  type BarGrouping,
  type PanelHighlight,
  type PanelInterval,
  type PanelModel,
} from './attempt-panel-model';
import { attemptTip, intervalName, intervalTip, litOpacity } from './attempt-strip';

const GROUPINGS: readonly (readonly [BarGrouping, string])[] = [
  ['phase', 'by phase'],
  ['attempt', 'by attempt'],
];

/** The widths of the columns: the group label, the attempt number, the
 * duration text after the bars, and the attempt label of a group by
 * attempt. A narrow panel (a node panel at its smallest) takes the narrow
 * widths. */
const COLUMNS_MULTI =
  '[--lab-w:68px] [--num-w:26px] [--val-w:62px] [--att-w:28px] @max-[350px]:[--lab-w:66px] @max-[350px]:[--num-w:20px] @max-[350px]:[--val-w:56px] @max-[350px]:[--att-w:26px]';
const COLUMNS_SINGLE =
  '[--lab-w:68px] [--num-w:0px] [--val-w:62px] [--att-w:28px] @max-[350px]:[--lab-w:66px] @max-[350px]:[--val-w:56px] @max-[350px]:[--att-w:26px]';
const TRACK_START = 'calc(var(--lab-w) + var(--num-w))';

/** How the rows of the bars are laid out. One attempt has one layout: its
 * phases in time order. */
type BarsLayout = 'by_phase_multi' | 'by_attempt' | 'single';

/** The layout of the bars of `attemptCount` attempts in `grouping`. */
function barsLayout(attemptCount: number, grouping: BarGrouping): BarsLayout {
  if (attemptCount <= 1) {
    return 'single';
  }
  switch (grouping) {
    case 'phase':
      return 'by_phase_multi';
    case 'attempt':
      return 'by_attempt';
    default:
      return assertNever(grouping, 'bar grouping');
  }
}

function rowLook(layout: BarsLayout): { heightPx: number; barPx: number; leadColumns: string } {
  switch (layout) {
    case 'by_phase_multi':
      return { heightPx: 13, barPx: 8, leadColumns: 'var(--num-w) minmax(0, 1fr)' };
    case 'by_attempt':
      return { heightPx: 15, barPx: 8, leadColumns: 'calc(var(--lab-w) + var(--num-w) - var(--att-w)) minmax(0, 1fr)' };
    case 'single':
      return { heightPx: 17, barPx: 9, leadColumns: 'minmax(0, 1fr)' };
    default:
      return assertNever(layout, 'bars layout');
  }
}

interface BarsContext {
  domain: LogDomain;
  layout: BarsLayout;
  multi: boolean;
  baseMs: number;
  highlight: PanelHighlight;
  bind: (tip: MarkTip<PanelHighlight>) => MarkTipProps;
}

/** One bar row. `lead`: the cell before the bar (the attempt number, the
 * phase name, or nothing). */
function BarRow({ interval, lead, context }: { interval: PanelInterval; lead: ReactNode; context: BarsContext }) {
  const look = rowLook(context.layout);
  const ms = intervalMs(interval);
  const widthPct = logPct(Math.max(ms, 0), context.domain);
  return (
    <div
      tabIndex={0}
      role="img"
      data-bar={interval.kind}
      data-bar-owner={ownerText(interval.owner)}
      aria-label={intervalName(interval, context.multi)}
      className="grid cursor-default items-center rounded-[3px] transition-opacity hover:bg-surface-hover focus-visible:outline-2 focus-visible:outline-ring motion-reduce:transition-none"
      style={{
        gridTemplateColumns: look.leadColumns,
        height: look.heightPx,
        opacity: litOpacity(intervalLit(context.highlight, interval)),
      }}
      {...context.bind(intervalTip(interval, context.multi, context.baseMs))}
    >
      {lead}
      <div className="relative h-full" style={{ marginRight: 'var(--val-w)' }}>
        <span
          className="absolute top-1/2 left-0 z-[1] min-w-0.5 -translate-y-1/2 rounded-r-[2px]"
          style={{ width: `${widthPct}%`, height: look.barPx, background: phaseFill(interval.kind) }}
        />
        <span
          data-bar-value
          className="absolute top-1/2 z-[3] ml-[5px] -translate-y-1/2 font-mono text-11 whitespace-nowrap text-foreground tabular-nums"
          style={{ left: `${widthPct}%` }}
        >
          {formatMilliseconds(ms)}
        </span>
      </div>
    </div>
  );
}

const numberCell = (interval: PanelInterval): ReactNode => (
  <span className="pr-1.5 text-right font-mono text-10 text-muted-foreground">{ownerText(interval.owner)}</span>
);

const phaseCell = (kind: TimingKind): ReactNode => (
  <span className="truncate pr-1 text-11 leading-[15px] @max-[350px]:pr-0.5">{phaseLabel(kind)}</span>
);

/** The bars by phase: one group for each kind, with a row for each interval
 * (each attempt, each retry wait). */
function PhaseGroups({ intervals, context }: { intervals: readonly PanelInterval[]; context: BarsContext }) {
  return (
    <>
      {panelKinds(intervals).map(kind => (
        <div key={kind} data-bar-group={kind} className="grid items-start" style={{ gridTemplateColumns: 'var(--lab-w) minmax(0, 1fr)' }}>
          <div className={cn('pr-1.5 text-11 @max-[350px]:pr-0.5', context.multi ? 'leading-[13px]' : 'leading-[17px]')}>
            {phaseLabel(kind)}
          </div>
          <div>
            {intervals
              .filter(interval => interval.kind === kind)
              .map(interval => (
                <BarRow key={interval.id} interval={interval} lead={context.multi ? numberCell(interval) : null} context={context} />
              ))}
          </div>
        </div>
      ))}
    </>
  );
}

/** The bars by attempt: one group for each attempt with its phases in time
 * order, then the retry wait before the next attempt. */
function AttemptGroups({ model, context }: { model: PanelModel; context: BarsContext }) {
  const groupColumns = { gridTemplateColumns: 'var(--att-w) minmax(0, 1fr)' };
  return (
    <>
      {model.attempts.map(attempt => {
        const own = model.intervals.filter(
          interval => interval.owner.kind === 'attempt' && interval.owner.attempt === attempt.number
        );
        const waits = model.intervals.filter(
          interval => interval.owner.kind === 'retry_wait' && interval.owner.fromAttempt === attempt.number
        );
        return (
          <div key={attempt.number} className="contents">
            {own.length > 0 && (
              <div data-bar-group={`attempt ${attempt.number}`} className="grid items-start" style={groupColumns}>
                <span
                  tabIndex={0}
                  role="img"
                  aria-label={`attempt ${attempt.number}, ${statusLabel(attempt.outcome)}`}
                  className="inline-flex h-[15px] cursor-default items-center gap-[3px] font-mono text-11 font-semibold whitespace-nowrap transition-opacity motion-reduce:transition-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
                  style={{ opacity: litOpacity(attemptLit(context.highlight, attempt.number)) }}
                  {...context.bind(attemptTip(attempt, model.intervals, context.baseMs))}
                >
                  {attempt.number}
                  <OutcomeCap status={attempt.outcome} sizePx={11} />
                </span>
                <div>
                  {own.map(interval => (
                    <BarRow key={interval.id} interval={interval} lead={phaseCell(interval.kind)} context={context} />
                  ))}
                </div>
              </div>
            )}
            {waits.map(interval => (
              <div key={interval.id} data-bar-group={ownerText(interval.owner)} className="grid items-start" style={groupColumns}>
                <span
                  className="inline-flex h-[15px] items-center font-mono text-10 whitespace-nowrap text-muted-foreground transition-opacity motion-reduce:transition-none"
                  style={{ opacity: litOpacity(intervalLit(context.highlight, interval)) }}
                >
                  {ownerText(interval.owner)}
                </span>
                <div>
                  <BarRow interval={interval} lead={phaseCell(interval.kind)} context={context} />
                </div>
              </div>
            ))}
          </div>
        );
      })}
    </>
  );
}

function BarGroups({ model, context }: { model: PanelModel; context: BarsContext }) {
  switch (context.layout) {
    case 'single':
    case 'by_phase_multi':
      return <PhaseGroups intervals={model.intervals} context={context} />;
    case 'by_attempt':
      return <AttemptGroups model={model} context={context} />;
    default:
      return assertNever(context.layout, 'bars layout');
  }
}

function switchButtonClass(pressed: boolean, enabled: boolean): string {
  return cn(
    'rounded-md px-1.75 py-1 text-11 leading-none font-medium text-foreground',
    pressed && 'bg-segment-on shadow-(--segment-on-shadow)',
    enabled ? 'cursor-pointer' : 'cursor-default opacity-45',
    enabled && !pressed && 'hover:bg-surface-hover'
  );
}

/** The switch of the grouping. With one attempt both groupings show the same
 * bars: the switch stays in its place, off (`aria-disabled`), and its
 * tooltip says why. The choice of the reader stays pressed. */
function GroupingSwitch({
  grouping,
  multi,
  onChange,
  bind,
}: {
  grouping: BarGrouping;
  multi: boolean;
  onChange: (grouping: BarGrouping) => void;
  bind: (tip: MarkTip<PanelHighlight>) => MarkTipProps;
}) {
  const offTip = multi
    ? {}
    : bind({
        id: 'switch:off',
        content: (
          <>
            <TipHead name="one attempt" />
            <TipNote text="both groupings show the same bars" />
          </>
        ),
        highlight: { kind: 'none' },
      });
  return (
    <div role="group" aria-label="Grouping of the bars" className="segment-track inline-flex shrink-0 gap-0.5 rounded-lg p-0.5" {...offTip}>
      {GROUPINGS.map(([key, label]) => (
        <button
          key={key}
          type="button"
          aria-pressed={key === grouping}
          {...(multi ? {} : { 'aria-disabled': true })}
          className={switchButtonClass(key === grouping, multi)}
          onClick={() => {
            if (multi && key !== grouping) {
              onChange(key);
            }
          }}
        >
          {label}
        </button>
      ))}
    </div>
  );
}

const LOG_AXIS_TIP: MarkTip<PanelHighlight> = {
  id: 'axis:log',
  content: (
    <>
      <TipHead name="duration" context="log scale" />
      <TipNote text="the axis starts at 0.1ms; each labeled line is 10 times the line before it, up to 10s; then 1m, 10m, 1h" />
    </>
  ),
  highlight: { kind: 'none' },
};

/**
 * The bars on the log axis, with the switch of their grouping. A task with
 * one attempt draws its phases in time order, and its switch is off.
 */
export function AttemptBars({
  model,
  grouping,
  onGrouping,
  baseMs,
  highlight,
  bind,
}: {
  model: PanelModel;
  grouping: BarGrouping;
  onGrouping: (grouping: BarGrouping) => void;
  baseMs: number;
  highlight: PanelHighlight;
  bind: (tip: MarkTip<PanelHighlight>) => MarkTipProps;
}) {
  const multi = model.attempts.length > 1;
  const layout = barsLayout(model.attempts.length, grouping);
  const domain = logDomain(longestIntervalMs(model.intervals));
  const context: BarsContext = { domain, layout, multi, baseMs, highlight, bind };
  return (
    <div role="group" aria-label="Duration of each phase, log scale" className={cn('mt-1', multi ? COLUMNS_MULTI : COLUMNS_SINGLE)}>
      <div className="flex min-h-[23px] flex-wrap items-end gap-x-2 gap-y-1 text-10 text-muted-foreground">
        <span
          className={cn('shrink-0 pb-1.5', layout === 'by_attempt' ? 'text-left' : 'pr-1.5 text-right')}
          style={{ width: TRACK_START }}
        >
          {multi ? 'attempt' : ''}
        </span>
        <span className="ml-auto flex flex-wrap items-end justify-end gap-x-2 gap-y-1">
          <span tabIndex={0} className="cursor-default pb-1.5 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring" {...bind(LOG_AXIS_TIP)}>
            duration · log scale
          </span>
          <GroupingSwitch grouping={grouping} multi={multi} onChange={onGrouping} bind={bind} />
        </span>
      </div>
      <div className="relative h-[21px] @max-[350px]:[&_span]:text-9">
        <div className="absolute inset-y-0" style={{ left: TRACK_START, right: 'var(--val-w)' }}>
          <LogAxisLabels domain={domain} />
        </div>
      </div>
      <div className="relative isolate flex flex-col gap-[7px] border-t border-hairline-strong pt-[7px] pb-[3px]">
        <div className="pointer-events-none absolute inset-y-0" style={{ left: TRACK_START, right: 'var(--val-w)' }} aria-hidden>
          <LogLines domain={domain} tone="grid" />
        </div>
        <BarGroups model={model} context={context} />
        <div className="pointer-events-none absolute inset-y-0 z-[2]" style={{ left: TRACK_START, right: 'var(--val-w)' }} aria-hidden>
          <LogLines domain={domain} tone="notch" />
        </div>
      </div>
    </div>
  );
}
