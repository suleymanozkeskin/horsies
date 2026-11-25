// The strip of the attempt panel: the task from its first due time to its
// last record on a linear axis. Width is the share of the total; order is
// left to right. Each attempt has its number with its outcome above its
// part of the strip.

import { TipHead, TipNote, type MarkTip, type MarkTipProps } from '@/components/workflows/mark-tip';
import { OutcomeCap } from '@/components/workflows/outcome-sign';
import { phaseFill, phaseLabel } from '@/components/workflows/phase';
import { LinearAxisLabels } from '@/components/workflows/timing-axes';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';

import {
  attemptLit,
  intervalLit,
  intervalMs,
  ownerAttempt,
  ownerContext,
  type PanelHighlight,
  type PanelInterval,
  type PanelModel,
} from './attempt-panel-model';
import type { RecordedAttempt } from './attempt-story';
import { AttemptTip, IntervalTip } from './attempt-tips';

/** A segment below this share of the strip is less than half a pixel wide
 * on a strip of the panel: the bars show its time. */
const MIN_SEGMENT_PCT = 0.14;
/** The opacity of a mark that the mark under the cursor does not light. */
export const DIMMED_OPACITY = 0.22;

export const litOpacity = (lit: boolean): number => (lit ? 1 : DIMMED_OPACITY);

/** The tooltip of an attempt mark: the number above the strip, its bracket,
 * or its group label of the bars. */
export function attemptTip(
  attempt: RecordedAttempt,
  intervals: readonly PanelInterval[],
  baseMs: number
): MarkTip<PanelHighlight> {
  return {
    id: `attempt:${attempt.number}`,
    content: <AttemptTip attempt={attempt} intervals={intervals} baseMs={baseMs} />,
    highlight: { kind: 'attempt', attempt: attempt.number },
  };
}

/** The tooltip of an interval mark: a segment of the strip or a bar row. */
export function intervalTip(interval: PanelInterval, multi: boolean, baseMs: number): MarkTip<PanelHighlight> {
  return {
    id: interval.id,
    content: <IntervalTip interval={interval} multi={multi} baseMs={baseMs} />,
    highlight: {
      kind: 'interval',
      id: interval.id,
      phase: interval.kind,
      attempt: ownerAttempt(interval.owner),
    },
  };
}

/** The accessible name of an interval mark: `queue wait, attempt 2, 7.05ms`. */
export const intervalName = (interval: PanelInterval, multi: boolean): string =>
  [phaseLabel(interval.kind), ...(multi ? [ownerContext(interval.owner)] : []), formatMilliseconds(intervalMs(interval))].join(
    ', '
  );

interface StripProps {
  model: PanelModel;
  fromMs: number;
  spanMs: number;
  highlight: PanelHighlight;
  bind: (tip: MarkTip<PanelHighlight>) => MarkTipProps;
}

function AttemptMarks({ model, fromMs, spanMs, highlight, bind }: StripProps) {
  const pct = (ms: number): number => ((ms - fromMs) / spanMs) * 100;
  return (
    <>
      {model.attempts.map(attempt => {
        const span = attempt.span;
        if (span.kind === 'no_times') {
          return null;
        }
        const left = pct(span.startMs);
        const width = pct(span.endMs) - left;
        const mid = left + width / 2;
        const opacity = litOpacity(attemptLit(highlight, attempt.number));
        const tip = attemptTip(attempt, model.intervals, fromMs);
        return (
          <span key={attempt.number} className="contents">
            <span
              data-attempt-span={attempt.number}
              className="absolute top-[17px] h-1 min-w-[3px] rounded-t-[2px] border border-b-0 border-tick transition-opacity motion-reduce:transition-none"
              style={{ left: `${left}%`, width: `${width}%`, opacity }}
              {...bind(tip)}
            />
            <span
              tabIndex={0}
              role="img"
              data-attempt-tag={attempt.number}
              aria-label={`attempt ${attempt.number}, ${statusLabel(attempt.outcome)}`}
              className="absolute top-0 inline-flex h-[15px] cursor-default items-center gap-[3px] rounded px-0.5 font-mono text-11 font-semibold whitespace-nowrap transition-opacity motion-reduce:transition-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
              style={{ left: `${mid}%`, transform: `translateX(-${mid}%)`, opacity }}
              {...bind(tip)}
            >
              {attempt.number}
              <OutcomeCap status={attempt.outcome} sizePx={11} />
            </span>
          </span>
        );
      })}
    </>
  );
}

/** The strip: the attempt marks, the segments of every interval in time
 * order, and the linear axis of elapsed time. */
export function AttemptStrip(props: StripProps) {
  const { model, fromMs, spanMs, highlight, bind } = props;
  const multi = model.attempts.length > 1;
  return (
    <div className="relative pt-[23px]">
      <AttemptMarks {...props} />
      <div data-strip className="relative h-3.5 overflow-hidden rounded-sm bg-glass-inset">
        {model.intervals.map(interval => {
          const width = (intervalMs(interval) / spanMs) * 100;
          if (width < MIN_SEGMENT_PCT) {
            return null;
          }
          return (
            <span
              key={interval.id}
              tabIndex={0}
              role="img"
              data-segment={interval.kind}
              aria-label={intervalName(interval, multi)}
              className="absolute inset-y-0 transition-opacity focus-visible:outline-2 focus-visible:outline-offset-[-2px] focus-visible:outline-ring motion-reduce:transition-none"
              style={{
                left: `${((interval.startMs - fromMs) / spanMs) * 100}%`,
                width: `${width}%`,
                background: phaseFill(interval.kind),
                opacity: litOpacity(intervalLit(highlight, interval)),
              }}
              {...bind(intervalTip(interval, multi, fromMs))}
            />
          );
        })}
      </div>
      <div
        className="relative mt-[3px] h-[15px] font-mono text-10 text-muted-foreground"
        {...bind({
          id: 'axis:linear',
          content: (
            <>
              <TipHead name="time" context="linear" />
              <TipNote text="elapsed time from the first due time" />
            </>
          ),
          highlight: { kind: 'none' },
        })}
      >
        <LinearAxisLabels spanMs={spanMs} placement="under_strip" />
      </div>
    </div>
  );
}
