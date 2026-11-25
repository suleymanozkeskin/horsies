// The key of a timing view: it names every fill and every mark of the view,
// with the total of each kind when the view has one. Each entry has a
// tooltip, and an entry of a kind lights the marks of that kind.

import type { ReactNode } from 'react';

import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import { cn } from '@/lib/utils';

import { PhaseSwatch, TipHead, TipNote, type MarkTipProps } from './mark-tip';
import { OutcomeCap } from './outcome-sign';
import { phaseLabel, phaseMeaning, phaseTrack, type TimingKind } from './phase';

/** The total of a kind in a key, with where it is counted (`total of 3`,
 * `on the longest chain`), or no total: the view shows each time itself. */
export type KeyTotal = { kind: 'total'; ms: number; context: string } | { kind: 'no_total' };

export interface KeyPhase {
  phase: TimingKind;
  total: KeyTotal;
}

/** A mark of a view that is not the fill of a kind. */
export type KeyMark =
  /** The sign of an outcome at the end of an attempt. */
  | { kind: 'outcome'; status: string }
  /** The diamond at the end of a node. */
  | { kind: 'end' }
  /** The line of the longest chain, with its nodes. */
  | { kind: 'chain'; nodes: string }
  /** The band of a pause of the run. */
  | { kind: 'paused' }
  /** What a striped fill and a solid fill mean. */
  | { kind: 'pattern'; pattern: KeyPattern };

/** The two patterns of the fills: a wait is striped, work is solid. */
export type KeyPattern = 'wait' | 'work';

function patternFill(pattern: KeyPattern): string {
  switch (pattern) {
    case 'wait':
      return 'var(--pattern-wait-fill)';
    case 'work':
      return 'var(--pattern-work-fill)';
    default:
      return assertNever(pattern, 'key pattern');
  }
}

function patternText(pattern: KeyPattern): { label: string; meaning: string } {
  switch (pattern) {
    case 'wait':
      return { label: 'wait', meaning: 'a striped fill is a wait' };
    case 'work':
      return { label: 'work', meaning: 'a solid fill is work' };
    default:
      return assertNever(pattern, 'key pattern');
  }
}

export type KeyEntry = { kind: 'phase'; entry: KeyPhase } | { kind: 'mark'; mark: KeyMark };

/** Which kind entries are lit: all of them, or the entry of one kind while
 * its marks are lit. */
export type KeyLight = { kind: 'all' } | { kind: 'phase'; phase: TimingKind };

/** The id of a key entry, unique in its key. */
export function keyEntryId(entry: KeyEntry): string {
  switch (entry.kind) {
    case 'phase':
      return `key:phase:${entry.entry.phase}`;
    case 'mark':
      return `key:mark:${markId(entry.mark)}`;
    default:
      return assertNever(entry, 'key entry');
  }
}

function markId(mark: KeyMark): string {
  switch (mark.kind) {
    case 'outcome':
      return `outcome:${mark.status}`;
    case 'pattern':
      return `pattern:${mark.pattern}`;
    case 'end':
    case 'chain':
    case 'paused':
      return mark.kind;
    default:
      return assertNever(mark, 'key mark');
  }
}

/** The icon of a mark: the sign, the diamond, the line, the band, the
 * pattern. */
export function KeyMarkIcon({ mark }: { mark: KeyMark }) {
  switch (mark.kind) {
    case 'outcome':
      return <OutcomeCap status={mark.status} sizePx={11} />;
    case 'end':
      return <span className="mx-0.5 inline-block size-2 shrink-0 rotate-45 border border-foreground bg-card" aria-hidden />;
    case 'chain':
      return (
        <span
          className="mx-[5px] inline-block h-[11px] w-[1.5px] shrink-0 rounded-[1px]"
          style={{ background: 'var(--longest-chain)', opacity: 0.75 }}
          aria-hidden
        />
      );
    case 'paused':
      return (
        <span
          className="inline-block h-[9px] w-3 shrink-0 rounded-[2px] border border-border"
          style={{ background: 'var(--pause-band)' }}
          aria-hidden
        />
      );
    case 'pattern':
      return (
        <span
          className="inline-block h-[9px] w-3 shrink-0 rounded-[2px]"
          style={{ background: patternFill(mark.pattern) }}
          aria-hidden
        />
      );
    default:
      return assertNever(mark, 'key mark');
  }
}

/** The label of a mark and what it means. */
export function keyMarkText(mark: KeyMark): { label: string; meaning: string } {
  switch (mark.kind) {
    case 'outcome':
      return { label: statusLabel(mark.status), meaning: 'the end of an attempt, with its outcome' };
    case 'end':
      return {
        label: 'end of a node',
        meaning:
          'The time at which a node got its terminal status, in the color of that status. It is a point in time, not a duration.',
      };
    case 'chain':
      return { label: 'longest chain', meaning: `the nodes that set the end of the run: ${mark.nodes}` };
    case 'paused':
      return { label: 'run paused', meaning: 'a band over the rows, from the pause to the resume, to the end of the run, or to now' };
    case 'pattern':
      return patternText(mark.pattern);
    default:
      return assertNever(mark, 'key mark');
  }
}

/** The tooltip of a kind entry: its fill, its name, where its total is
 * counted, its total, and what it is. */
export function KeyPhaseTip({ entry }: { entry: KeyPhase }) {
  const thin = phaseTrack(entry.phase) === 'thin';
  const total = entry.total;
  switch (total.kind) {
    case 'total':
      return (
        <>
          <TipHead
            mark={<PhaseSwatch kind={entry.phase} thin={thin} />}
            name={phaseLabel(entry.phase)}
            context={total.context}
            value={formatMilliseconds(total.ms)}
          />
          <TipNote text={phaseMeaning(entry.phase)} />
        </>
      );
    case 'no_total':
      return (
        <>
          <TipHead mark={<PhaseSwatch kind={entry.phase} thin={thin} />} name={phaseLabel(entry.phase)} />
          <TipNote text={phaseMeaning(entry.phase)} />
        </>
      );
    default:
      return assertNever(total, 'key total');
  }
}

function KeyMarkTip({ mark }: { mark: KeyMark }) {
  const text = keyMarkText(mark);
  return (
    <>
      <TipHead mark={<KeyMarkIcon mark={mark} />} name={text.label} />
      <TipNote text={text.meaning} />
    </>
  );
}

const lit = (light: KeyLight, phase: TimingKind): boolean => light.kind === 'all' || light.phase === phase;

function PhaseItem({
  entry,
  light,
  bind,
}: {
  entry: KeyPhase;
  light: KeyLight;
  bind: (entry: KeyEntry, content: ReactNode) => MarkTipProps;
}) {
  const total = entry.total;
  return (
    <li
      tabIndex={0}
      data-key-phase={entry.phase}
      className="inline-flex cursor-default items-center gap-[5px] whitespace-nowrap rounded-[3px] transition-opacity motion-reduce:transition-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      style={{ opacity: lit(light, entry.phase) ? 1 : 0.22 }}
      {...bind({ kind: 'phase', entry }, <KeyPhaseTip entry={entry} />)}
    >
      <PhaseSwatch kind={entry.phase} thin={phaseTrack(entry.phase) === 'thin'} />
      <span>{phaseLabel(entry.phase)}</span>
      {total.kind === 'total' && (
        <span className="font-mono text-11 text-muted-foreground tabular-nums">{formatMilliseconds(total.ms)}</span>
      )}
    </li>
  );
}

function MarkItem({ mark, bind }: { mark: KeyMark; bind: (entry: KeyEntry, content: ReactNode) => MarkTipProps }) {
  const quiet = mark.kind === 'pattern';
  return (
    <li
      tabIndex={0}
      data-key-mark={mark.kind}
      className={cn(
        'inline-flex cursor-default items-center gap-[5px] whitespace-nowrap rounded-[3px] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring',
        quiet && 'text-muted-foreground'
      )}
      {...bind({ kind: 'mark', mark }, <KeyMarkTip mark={mark} />)}
    >
      <KeyMarkIcon mark={mark} />
      <span>{keyMarkText(mark).label}</span>
    </li>
  );
}

/**
 * The key of a view, in one wrapped list: the lead item of the view (a
 * total, when it has one), the full-height kinds and the trail item of the
 * view, then on a line of its own the thin kinds and the marks.
 */
export function TimingKey({
  lead,
  trail,
  phases,
  marks,
  light,
  bind,
}: {
  lead?: ReactNode;
  trail?: ReactNode;
  phases: readonly KeyPhase[];
  marks: readonly KeyMark[];
  light: KeyLight;
  bind: (entry: KeyEntry, content: ReactNode) => MarkTipProps;
}) {
  const full = phases.filter(entry => phaseTrack(entry.phase) === 'full');
  const thin = phases.filter(entry => phaseTrack(entry.phase) === 'thin');
  const secondLine = thin.length > 0 || marks.length > 0;
  return (
    <ul aria-label="Key" className="m-0 flex list-none flex-wrap items-center gap-x-3 gap-y-1.25 p-0 text-11 leading-tight">
      {lead}
      {full.map(entry => (
        <PhaseItem key={entry.phase} entry={entry} light={light} bind={bind} />
      ))}
      {trail}
      {secondLine && full.length > 0 && <li className="h-0 basis-full" aria-hidden />}
      {thin.map(entry => (
        <PhaseItem key={entry.phase} entry={entry} light={light} bind={bind} />
      ))}
      {marks.map(mark => (
        <MarkItem key={markId(mark)} mark={mark} bind={bind} />
      ))}
    </ul>
  );
}
