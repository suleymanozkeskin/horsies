// Run-level timing from the segments of every node: where the time of the
// run went, the critical path, the node with the longest wall time, and the
// split of one node into its own phases and its task. Pure: no React, no DOM.
// Every function takes the draw time (`drawAtMs`), to which an open segment
// runs.

import { assertNever } from '@/lib/exhaustive';
import { instantMs } from '@/lib/time-text';
import type { NodeTiming, TimingSegment } from '@/types/timing';
import type { WorkflowEdge, WorkflowNode } from '@/types/workflows';

import type { PhaseKey } from './phase';
import {
  nodeWallTime,
  phaseComposition,
  segmentSpan,
  type PhaseShare,
} from './timeline-geometry';

/** The segments of a node whose timing has them. */
export function timedSegments(timing: NodeTiming): readonly TimingSegment[] {
  switch (timing.kind) {
    case 'exact':
    case 'partial':
      return timing.segments;
    case 'unavailable':
    case 'inconsistent':
      return [];
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The end of a node: the latest of its segment ends and its endpoint, the
 * same end as its wall time. */
export type NodeEnd = { kind: 'measured'; endMs: number } | { kind: 'no_timing' };

export function nodeEnd(timing: NodeTiming, drawAtMs: number): NodeEnd {
  switch (timing.kind) {
    case 'unavailable':
    case 'inconsistent':
      return { kind: 'no_timing' };
    case 'exact':
    case 'partial': {
      let endMs = Number.NEGATIVE_INFINITY;
      for (const segment of timing.segments) {
        endMs = Math.max(endMs, segmentSpan(segment, drawAtMs).endMs);
      }
      switch (timing.endpoint.kind) {
        case 'terminal':
          endMs = Math.max(endMs, instantMs(timing.endpoint.at));
          break;
        case 'not_terminal':
          break;
        default:
          return assertNever(timing.endpoint, 'node endpoint');
      }
      return Number.isFinite(endMs) ? { kind: 'measured', endMs } : { kind: 'no_timing' };
    }
    default:
      return assertNever(timing, 'node timing');
  }
}

/** The time of a node that is not dependency wait. */
export function activeMs(segments: readonly TimingSegment[], drawAtMs: number): number {
  let total = 0;
  for (const segment of segments) {
    if (segment.phase.kind !== 'pending') {
      const span = segmentSpan(segment, drawAtMs);
      total += Math.max(span.endMs - span.startMs, 0);
    }
  }
  return total;
}

/** The time of each phase summed over every node of the run (node time, so
 * parallel nodes add up), in lifecycle order. */
export function runPhaseTotals(nodes: readonly WorkflowNode[], drawAtMs: number): PhaseShare[] {
  return phaseComposition(
    nodes.flatMap(node => timedSegments(node.timing)),
    drawAtMs
  );
}

/**
 * The critical path of a run: the chain of nodes whose end times decided
 * the end of the run. It ends at the node that ended last; each step goes to
 * the dependency that ended last, the one that released the node. `lengthMs`
 * is the time of the chain without its dependency wait; `phases` splits it
 * by phase.
 */
export type CriticalPath =
  | { kind: 'path'; taskIndices: number[]; lengthMs: number; phases: PhaseShare[] }
  | { kind: 'no_timing' };

interface TimedNode {
  node: WorkflowNode;
  endMs: number;
  activeMs: number;
}

/** The later end wins; on a tie, the node with more time that is not
 * dependency wait, then the lower task index. A skip cascade ends many nodes
 * at one instant, and the node that ran is the cause. */
function decides(candidate: TimedNode, best: TimedNode | undefined): boolean {
  if (best === undefined) {
    return true;
  }
  if (candidate.endMs !== best.endMs) {
    return candidate.endMs > best.endMs;
  }
  if (candidate.activeMs !== best.activeMs) {
    return candidate.activeMs > best.activeMs;
  }
  return candidate.node.task_index < best.node.task_index;
}

function timedNodes(nodes: readonly WorkflowNode[], drawAtMs: number): Map<number, TimedNode> {
  const timed = new Map<number, TimedNode>();
  for (const node of nodes) {
    const end = nodeEnd(node.timing, drawAtMs);
    switch (end.kind) {
      case 'measured':
        timed.set(node.task_index, {
          node,
          endMs: end.endMs,
          activeMs: activeMs(timedSegments(node.timing), drawAtMs),
        });
        break;
      case 'no_timing':
        break;
      default:
        assertNever(end, 'node end');
    }
  }
  return timed;
}

function dependenciesByNode(edges: readonly WorkflowEdge[]): Map<number, number[]> {
  const dependencies = new Map<number, number[]>();
  for (const edge of edges) {
    const list = dependencies.get(edge.to_index) ?? [];
    list.push(edge.from_index);
    dependencies.set(edge.to_index, list);
  }
  return dependencies;
}

export function criticalPath(
  nodes: readonly WorkflowNode[],
  edges: readonly WorkflowEdge[],
  drawAtMs: number
): CriticalPath {
  const timed = timedNodes(nodes, drawAtMs);
  let last: TimedNode | undefined;
  for (const candidate of timed.values()) {
    last = decides(candidate, last) ? candidate : last;
  }
  if (last === undefined) {
    return { kind: 'no_timing' };
  }
  const dependencies = dependenciesByNode(edges);
  const chain: TimedNode[] = [last];
  const visited = new Set<number>([last.node.task_index]);
  // A walk in a DAG visits each node once at most: the node count bounds it.
  let current = last;
  for (let step = 0; step < nodes.length; step += 1) {
    let release: TimedNode | undefined;
    for (const index of dependencies.get(current.node.task_index) ?? []) {
      const candidate = timed.get(index);
      if (candidate !== undefined && !visited.has(index) && decides(candidate, release)) {
        release = candidate;
      }
    }
    if (release === undefined) {
      break;
    }
    visited.add(release.node.task_index);
    chain.unshift(release);
    current = release;
  }
  const segments = chain
    .flatMap(entry => timedSegments(entry.node.timing))
    .filter(segment => segment.phase.kind !== 'pending');
  const phases = phaseComposition(segments, drawAtMs);
  return {
    kind: 'path',
    taskIndices: chain.map(entry => entry.node.task_index),
    lengthMs: phases.reduce((sum, share) => sum + share.ms, 0),
    phases,
  };
}

/** Whether a node is on the critical path of its run. */
export type CriticalMembership = 'on_path' | 'off_path';

export function criticalMembership(path: CriticalPath, taskIndex: number): CriticalMembership {
  switch (path.kind) {
    case 'path':
      return path.taskIndices.includes(taskIndex) ? 'on_path' : 'off_path';
    case 'no_timing':
      return 'off_path';
    default:
      return assertNever(path, 'critical path');
  }
}

/** The active span of a node: from the end of its dependency wait (its
 * first segment that is not `pending`) to its end. */
export type ActiveSpan = { kind: 'span'; startMs: number; endMs: number } | { kind: 'no_active_time' };

export function activeSpan(timing: NodeTiming, drawAtMs: number): ActiveSpan {
  const first = timedSegments(timing).find(segment => segment.phase.kind !== 'pending');
  const end = nodeEnd(timing, drawAtMs);
  if (first === undefined || end.kind === 'no_timing') {
    return { kind: 'no_active_time' };
  }
  return { kind: 'span', startMs: segmentSpan(first, drawAtMs).startMs, endMs: end.endMs };
}

/** The node with the longest wall time, the time the timeline draws for it. */
export type LongestWall =
  | { kind: 'node'; node: WorkflowNode; ms: number }
  | { kind: 'no_timing' };

export function longestWall(nodes: readonly WorkflowNode[], drawAtMs: number): LongestWall {
  let longest: LongestWall = { kind: 'no_timing' };
  for (const node of nodes) {
    const segments = timedSegments(node.timing);
    switch (node.timing.kind) {
      case 'exact':
      case 'partial': {
        const wall = nodeWallTime(segments, node.timing.endpoint, drawAtMs);
        if (wall.kind === 'measured' && (longest.kind === 'no_timing' || wall.ms > longest.ms)) {
          longest = { kind: 'node', node, ms: wall.ms };
        }
        break;
      }
      case 'unavailable':
      case 'inconsistent':
        break;
      default:
        assertNever(node.timing, 'node timing');
    }
  }
  return longest;
}

/** One part of the time of a node: a phase of the node itself (dependency
 * wait, ready, a child run, advancing), or its task: every segment of its
 * tasks and attempts, retry waits included. */
export type NodeTimePart =
  | { kind: 'phase'; key: PhaseKey; ms: number }
  | { kind: 'task'; ms: number; tasks: number; attempts: number };

/** The parts of a node in the order they first appear; each node phase is
 * summed over its segments. */
export function nodeTimeParts(
  segments: readonly TimingSegment[],
  drawAtMs: number
): NodeTimePart[] {
  const parts: NodeTimePart[] = [];
  const tasks = new Set<number>();
  const attempts = new Set<string>();
  for (const segment of segments) {
    const span = segmentSpan(segment, drawAtMs);
    const ms = Math.max(span.endMs - span.startMs, 0);
    const source = segment.source;
    switch (source.kind) {
      case 'node': {
        const existing = parts.find(
          part => part.kind === 'phase' && part.key === segment.phase.kind
        );
        if (existing === undefined) {
          parts.push({ kind: 'phase', key: segment.phase.kind, ms });
        } else {
          existing.ms += ms;
        }
        break;
      }
      case 'task':
      case 'attempt': {
        tasks.add(source.generation);
        attempts.add(
          source.kind === 'attempt' ? `${source.generation}:${source.attempt}` : `${source.generation}:task`
        );
        const existing = parts.find(part => part.kind === 'task');
        if (existing === undefined) {
          parts.push({ kind: 'task', ms, tasks: 0, attempts: 0 });
        } else {
          existing.ms += ms;
        }
        break;
      }
      default:
        assertNever(source, 'segment source');
    }
  }
  return parts.map(part =>
    part.kind === 'task' ? { ...part, tasks: tasks.size, attempts: attempts.size } : part
  );
}
