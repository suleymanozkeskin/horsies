// The tooltips of the attempt panel: an interval (a phase of an attempt, or
// a retry wait) and an attempt.

import { PhaseSwatch, TipHead, TipNote, TipPhaseList, TipTimes } from '@/components/workflows/mark-tip';
import { OutcomeCap } from '@/components/workflows/outcome-sign';
import { phaseLabel, phaseMeaning } from '@/components/workflows/phase';
import { clockTextFrom } from '@/lib/clock-text';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';

import { intervalMs, ownerContext, type PanelInterval } from './attempt-panel-model';
import type { AttemptWriter, RecordedAttempt } from './attempt-story';

/** Start → end in a tooltip, with the day offset from the first time of the
 * task. */
export const spanText = (baseMs: number, startMs: number, endMs: number): string =>
  `${clockTextFrom(baseMs, startMs)} → ${clockTextFrom(baseMs, endMs)}`;

/** The tooltip of an interval: its kind, its attempt (when the task has more
 * than one), its time, its start → end, what it is, and what it holds beyond
 * its kind. */
export function IntervalTip({
  interval,
  multi,
  baseMs,
}: {
  interval: PanelInterval;
  multi: boolean;
  baseMs: number;
}) {
  return (
    <>
      <TipHead
        mark={<PhaseSwatch kind={interval.kind} />}
        name={phaseLabel(interval.kind)}
        {...(multi ? { context: ownerContext(interval.owner) } : {})}
        value={formatMilliseconds(intervalMs(interval))}
      />
      <TipTimes text={spanText(baseMs, interval.startMs, interval.endMs)} />
      <TipNote text={phaseMeaning(interval.kind)} />
      {interval.detail !== null && <TipNote text={interval.detail} />}
    </>
  );
}

/** Who wrote the row of an attempt, when it is not the worker. */
function writerText(writer: AttemptWriter): string | null {
  switch (writer.kind) {
    case 'worker':
    case 'not_recorded':
      return null;
    case 'recovery':
      return 'recorded by recovery';
    case 'cancel':
      return 'ended by a cancel';
    default:
      return assertNever(writer, 'attempt writer');
  }
}

/** `failed · PSP_UNAVAILABLE`, `completed`. */
export const outcomeText = (attempt: RecordedAttempt): string =>
  attempt.errorCode === null ? statusLabel(attempt.outcome) : `${statusLabel(attempt.outcome)} · ${attempt.errorCode}`;

/** The tooltip of an attempt: its outcome, its time, its start → end, the
 * time of each of its phases, its worker, and who wrote its row. */
export function AttemptTip({
  attempt,
  intervals,
  baseMs,
}: {
  attempt: RecordedAttempt;
  intervals: readonly PanelInterval[];
  baseMs: number;
}) {
  const writer = writerText(attempt.writer);
  const own = intervals.filter(
    interval => interval.owner.kind === 'attempt' && interval.owner.attempt === attempt.number
  );
  const span = attempt.span;
  return (
    <>
      <TipHead
        mark={<OutcomeCap status={attempt.outcome} sizePx={12} />}
        name={`attempt ${attempt.number}`}
        context={outcomeText(attempt)}
        {...(span.kind === 'span' ? { value: formatMilliseconds(span.endMs - span.startMs) } : {})}
      />
      {span.kind === 'span' && <TipTimes text={spanText(baseMs, span.startMs, span.endMs)} />}
      {own.length > 0 && <TipPhaseList items={own.map(interval => ({ kind: interval.kind, ms: intervalMs(interval) }))} />}
      {attempt.host !== null && <TipNote text={attempt.host} />}
      {writer !== null && <TipNote text={writer} />}
    </>
  );
}
