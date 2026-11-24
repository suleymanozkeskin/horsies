import { useEffect, useId, useMemo, useRef, useState, type MouseEvent } from 'react';

import {
  Background,
  Controls,
  type Edge,
  type NodeMouseHandler,
  Panel,
  ReactFlow,
  type ReactFlowInstance,
} from '@xyflow/react';
import { ChevronLeft, ChevronRight, Search, TriangleAlert } from 'lucide-react';

import '@xyflow/react/dist/style.css';

import { RESIDUAL_COPY } from '@/actions/copy';
import { Button } from '@/components/ui/button';
import { Capsule } from '@/components/ui/capsule';
import { Hint } from '@/components/ui/hint';
import { useDrawTime, type ReadClock } from '@/hooks/use-draw-time';
import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { assertNever } from '@/lib/exhaustive';
import { formatMilliseconds } from '@/lib/format-duration';
import { statusLabel } from '@/lib/status-utils';
import type { WorkflowNode, WorkflowRunDetail } from '@/types/workflows';

import { layoutWorkflow, NODE_H, NODE_W, type NodePosition } from './layout';
import { TimingLegend, type LegendMark } from './legend';
import { FloatingTooltip, NodeTooltip } from './node-tooltip';
import { residualState } from './residual';
import {
  criticalMembership,
  criticalPath,
  longestNodeTime,
  type CriticalPath,
  type LongestNodeTime,
} from './run-timing';
import { nodeTimingText } from './timing-text';
import {
  childRollupBadge,
  WorkflowNodeCard,
  type WorkflowFlowNode,
  type WorkflowNodeData,
} from './workflow-node';

const nodeTypes = { workflow: WorkflowNodeCard };

/** Stable empty default so an absent statusFilter prop keeps a constant reference. */
const NO_STATUS_FILTER: ReadonlySet<string> = new Set();

/** Aggregate run metrics shown over the canvas. `longest` is the node with
 * the longest time without dependency wait. */
interface RunMetrics {
  total: number;
  completed: number;
  running: number;
  failed: number;
  longest: LongestNodeTime;
  /** The marks that the graph draws: the key under the counts. */
  marks: LegendMark[];
}

/** What the pointer is on in the graph, where the shared tooltip shows it:
 * a node card, or the edge from one node to another. */
type GraphHover =
  | { kind: 'none' }
  | { kind: 'node'; taskIndex: number; xPx: number; yPx: number }
  | { kind: 'edge'; fromIndex: number; toIndex: number; xPx: number; yPx: number };

/** Where the status counts of the graph show: on the graph card (a
 * sub-workflow graph in the side panel), or in the run summary over the
 * graph (the main graph), which then needs no copy on the card. */
export type CountsPlacement = 'in_graph' | 'in_run_summary';

/** The edges of the critical path: each joins two consecutive nodes of it. */
function criticalEdgeIds(taskIndices: readonly number[]): Set<string> {
  const ids = new Set<string>();
  for (let index = 1; index < taskIndices.length; index += 1) {
    const from = taskIndices[index - 1];
    const to = taskIndices[index];
    if (from !== undefined && to !== undefined) {
      ids.add(edgeId(from, to));
    }
  }
  return ids;
}

const nodeLabel = (node: WorkflowNode): string =>
  node.node_id ?? node.task_name ?? `#${node.task_index}`;

const drillableId = (node: WorkflowNode): string | null =>
  node.is_subworkflow && node.sub_workflow_id ? node.sub_workflow_id : null;

/** The facts of a node that the graph draws with a mark: the dashed border
 * of a tolerant join, the layers icon and the count of a sub-workflow, and
 * the badge of a node that runs on under a cancelled run. One line each, for
 * the tooltip of the card. */
export function graphNodeFacts(node: WorkflowNode, runStatus: string): string[] {
  const facts: string[] = [];
  if (node.node_id !== null && node.node_id !== node.task_name) {
    facts.push(`Task: ${node.task_name}.`);
  }
  if (node.allow_failed_deps) {
    facts.push('Tolerant join (dashed border): the node runs even when a dependency failed.');
  }
  const rollup = childRollupBadge(node.child_total, node.child_failed);
  if (node.is_subworkflow) {
    facts.push(
      `Sub-workflow${rollup === null ? '' : `: ${rollup.title}`}.` +
        (drillableId(node) === null ? '' : ' Double-click to open it.')
    );
  }
  const residual = residualState(runStatus, node.node_status, null);
  if (residual !== null) {
    facts.push(RESIDUAL_COPY[residual].tooltip);
  }
  return facts;
}

/** The id of the edge from one node to another. */
const edgeId = (fromIndex: number, toIndex: number): string => `${fromIndex}-${toIndex}`;

/** The two task indexes of an edge id; an id of another form is a defect of
 * this file. */
function edgeEnds(id: string): { fromIndex: number; toIndex: number } {
  const [from, to] = id.split('-').map(Number);
  if (from === undefined || to === undefined || !Number.isInteger(from) || !Number.isInteger(to)) {
    throw new Error(`edge id is not "<from>-<to>": ${JSON.stringify(id)}`);
  }
  return { fromIndex: from, toIndex: to };
}

interface WorkflowGraphProps {
  workflowId: string;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  onDrillInto: (subWorkflowId: string, label: string) => void;
  /** Open a node's detail from failure navigation (main graph only). */
  onFocusNode?: (node: WorkflowNode) => void;
  /** Statuses to keep visible; nodes of other statuses are faded (empty = all). */
  statusFilter?: ReadonlySet<string>;
  countsPlacement: CountsPlacement;
}

/** Renders one run's DAG. Click selects a node; double-click drills into a
 * subworkflow. */
export function WorkflowGraph({
  workflowId,
  selectedIndex,
  onSelectNode,
  onDrillInto,
  onFocusNode,
  statusFilter = NO_STATUS_FILTER,
  countsPlacement,
}: WorkflowGraphProps) {
  const { detail, readClock, isLoading, isError } = useWorkflowRun(workflowId);

  // Failure navigation: a cursor into detail.failed_indices. `focusedIndex` is
  // the node the cursor last centered on, highlighted like a selection without
  // touching the parent's selection prop.
  const instanceRef = useRef<ReactFlowInstance<WorkflowFlowNode> | null>(null);
  const [cursor, setCursor] = useState(0);
  const [focusedIndex, setFocusedIndex] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  const [searchCursor, setSearchCursor] = useState(0);
  const [hover, setHover] = useState<GraphHover>({ kind: 'none' });
  const tooltipId = useId();
  const failedIndices = useMemo(
    () => detail?.failed_indices ?? [],
    [detail?.failed_indices]
  );

  const { rfNodes, rfEdges, byIndex, positions, metrics, matchIndices } = useMemo(() => {
    if (!detail || !readClock) {
      return {
        rfNodes: [] as WorkflowFlowNode[],
        rfEdges: [] as Edge[],
        byIndex: new Map<number, WorkflowNode>(),
        positions: new Map<number, NodePosition>(),
        metrics: null as RunMetrics | null,
        matchIndices: [] as number[],
      };
    }
    const query = search.trim().toLowerCase();
    const matched = new Set<number>();
    const matches: number[] = [];
    const statusFiltering = statusFilter.size > 0;
    const layout = layoutWorkflow(detail.nodes, detail.edges);
    const index = new Map<number, WorkflowNode>();
    const statusByIndex = new Map<number, string>();
    let completed = 0;
    let running = 0;
    for (const node of detail.nodes) {
      index.set(node.task_index, node);
      statusByIndex.set(node.task_index, node.node_status);
      if (node.node_status === 'COMPLETED') {
        completed += 1;
      }
      if (node.node_status === 'RUNNING') {
        running += 1;
      }
      if (
        query &&
        (nodeLabel(node).toLowerCase().includes(query) ||
          node.task_name.toLowerCase().includes(query))
      ) {
        matched.add(node.task_index);
        matches.push(node.task_index);
      }
    }

    const nodes: WorkflowFlowNode[] = detail.nodes.map(node => {
      const data: WorkflowNodeData = {
        label: nodeLabel(node),
        taskName: node.task_name,
        status: node.node_status,
        isSubworkflow: node.is_subworkflow,
        allowFailedDeps: node.allow_failed_deps,
        execS: node.exec_s,
        drillable: drillableId(node) !== null,
        childTotal: node.child_total,
        childFailed: node.child_failed,
        dimmed:
          (query !== '' && !matched.has(node.task_index)) ||
          (statusFiltering && !statusFilter.has(node.node_status)),
        // The run payload has no backing-task status, so only the cancelled-run
        // "draining" case is detectable here; "finishing" surfaces in the node
        // detail, which loads the leaf.
        residual: residualState(detail.run.status, node.node_status, null),
        timing: node.timing,
        readClock,
      };
      return {
        id: String(node.task_index),
        type: 'workflow',
        position: layout.positions.get(node.task_index) ?? { x: 0, y: 0 },
        data,
        // The phase times in the name are those of the read, without the
        // one-second clock of the card.
        ariaLabel: `${nodeLabel(node)}, ${statusLabel(node.node_status)}, ${nodeTimingText(node.timing, readClock.asOfMs)}`,
        selected:
          node.task_index === selectedIndex || node.task_index === focusedIndex,
        draggable: false,
      };
    });

    const path = criticalPath(detail.nodes, detail.edges, readClock.asOfMs);
    const critical = path.kind === 'path' ? criticalEdgeIds(path.taskIndices) : new Set<string>();
    const edges: Edge[] = detail.edges.map(edge => {
      const id = edgeId(edge.from_index, edge.to_index);
      const onPath = critical.has(id);
      return {
        id,
        source: String(edge.from_index),
        target: String(edge.to_index),
        animated: statusByIndex.get(edge.to_index) === 'RUNNING',
        style: onPath
          ? { stroke: 'var(--critical-path)', strokeWidth: 2.5 }
          : { stroke: 'var(--border)', strokeWidth: 1.5 },
        ...(onPath ? { ariaLabel: 'critical path' } : {}),
      };
    });

    return {
      rfNodes: nodes,
      rfEdges: edges,
      byIndex: index,
      positions: layout.positions,
      metrics: {
        total: detail.nodes.length,
        completed,
        running,
        failed: detail.failed_count,
        longest: longestNodeTime(detail.nodes, readClock.asOfMs),
        marks: [
          ...(critical.size > 0 ? (['critical_edge'] as const) : []),
          ...(detail.nodes.some(node => node.allow_failed_deps) ? (['tolerant_join'] as const) : []),
        ],
      } satisfies RunMetrics,
      matchIndices: matches,
    };
  }, [detail, readClock, selectedIndex, focusedIndex, search, statusFilter]);

  // Reset failure/search navigation when switching runs, and clamp cursors when
  // their target sets shrink while a run is live or the query changes.
  useEffect(() => {
    setCursor(0);
    setFocusedIndex(null);
    setSearch('');
    setHover({ kind: 'none' });
  }, [workflowId]);
  useEffect(() => {
    setCursor(current =>
      failedIndices.length === 0
        ? 0
        : Math.min(current, failedIndices.length - 1)
    );
  }, [failedIndices.length]);
  useEffect(() => {
    setSearchCursor(0);
  }, [search]);

  /** Pan the viewport to center a node by task_index. */
  const centerOn = (taskIndex: number): void => {
    const position = positions.get(taskIndex);
    if (position && instanceRef.current) {
      instanceRef.current.setCenter(
        position.x + NODE_W / 2,
        position.y + NODE_H / 2,
        { zoom: 1.2, duration: 500 }
      );
    }
  };

  const jumpToFailure = (nextCursor: number): void => {
    const count = failedIndices.length;
    if (count === 0) {
      return;
    }
    const wrapped = ((nextCursor % count) + count) % count;
    const target = failedIndices[wrapped];
    if (target === undefined) {
      return;
    }
    setCursor(wrapped);
    setFocusedIndex(target);
    centerOn(target);
    const node = byIndex.get(target);
    if (node) {
      onFocusNode?.(node);
    }
  };

  const jumpToMatch = (nextCursor: number): void => {
    const count = matchIndices.length;
    if (count === 0) {
      return;
    }
    const wrapped = ((nextCursor % count) + count) % count;
    const target = matchIndices[wrapped];
    if (target === undefined) {
      return;
    }
    setSearchCursor(wrapped);
    setFocusedIndex(target);
    centerOn(target);
  };

  const hoverNode: NodeMouseHandler = (event, node) =>
    setHover({ kind: 'node', taskIndex: Number(node.id), xPx: event.clientX, yPx: event.clientY });
  const hoverEdge = (event: MouseEvent, edge: Edge): void =>
    setHover({ kind: 'edge', ...edgeEnds(edge.id), xPx: event.clientX, yPx: event.clientY });
  const endHover = (): void => setHover({ kind: 'none' });

  const handleNodeClick: NodeMouseHandler = (_event, node) => {
    const source = byIndex.get(Number(node.id));
    if (source) {
      // Drop any failure-jump highlight so the click's selection is the only one.
      setFocusedIndex(null);
      onSelectNode(source);
    }
  };

  const handleNodeDoubleClick: NodeMouseHandler = (_event, node) => {
    const source = byIndex.get(Number(node.id));
    if (!source) {
      return;
    }
    const subId = drillableId(source);
    if (subId) {
      onDrillInto(subId, nodeLabel(source));
    }
  };

  if (isError) {
    return (
      <div
        className="flex h-full items-center justify-center text-sm"
        style={{ color: 'var(--error)' }}
        role="alert"
      >
        Could not load this workflow run.
      </div>
    );
  }

  if (isLoading && !detail) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Loading workflow…
      </div>
    );
  }

  return (
    <ReactFlow<WorkflowFlowNode>
      key={workflowId}
      nodes={rfNodes}
      edges={rfEdges}
      nodeTypes={nodeTypes}
      onInit={instance => {
        instanceRef.current = instance;
      }}
      onNodeClick={handleNodeClick}
      onNodeDoubleClick={handleNodeDoubleClick}
      onNodeMouseEnter={hoverNode}
      onNodeMouseMove={hoverNode}
      onNodeMouseLeave={endHover}
      onEdgeMouseEnter={hoverEdge}
      onEdgeMouseMove={hoverEdge}
      onEdgeMouseLeave={endHover}
      onMoveStart={endHover}
      nodesDraggable={false}
      nodesConnectable={false}
      edgesFocusable={false}
      fitView
      fitViewOptions={{ padding: 0.2 }}
      minZoom={0.1}
      proOptions={{ hideAttribution: true }}
    >
      {/* One row across the canvas top, so the overlays wrap instead of
          overlapping on a narrow canvas. The row itself lets pointer events
          through to the canvas; only the overlays take them. */}
      <Panel position="top-left" style={{ right: 0, pointerEvents: 'none' }}>
        <div className="flex flex-wrap items-start gap-2">
          {metrics && metrics.total > 0 && (
            <div className="liquid-glass pointer-events-auto flex min-h-(--capsule-height) flex-col justify-center gap-1.5 rounded-2xl px-4 py-1.5 text-xs">
              <GraphCounts metrics={metrics} placement={countsPlacement} />
              <LongestNodeLine longest={metrics.longest} />
              <TimingLegend phases={[]} marks={metrics.marks} />
            </div>
          )}
          {failedIndices.length > 0 && (
            <Capsule className="pointer-events-auto gap-1 pl-3 text-xs">
              <span
                className="inline-flex items-center gap-1.5 font-medium"
                style={{ color: 'var(--error)' }}
              >
                <TriangleAlert className="size-3.5" aria-hidden />
                {failedIndices.length} failed
              </span>
              <Button
                type="button"
                variant="capsule"
                size="capsule-icon"
                onClick={() => jumpToFailure(cursor - 1)}
                aria-label="Previous failure"
                className="text-muted-foreground hover:text-foreground"
              >
                <ChevronLeft />
              </Button>
              <span className="tabular-nums text-muted-foreground">
                {Math.min(cursor, failedIndices.length - 1) + 1}/
                {failedIndices.length}
              </span>
              <Button
                type="button"
                variant="capsule"
                size="capsule-icon"
                onClick={() => jumpToFailure(cursor + 1)}
                aria-label="Next failure"
                className="text-muted-foreground hover:text-foreground"
              >
                <ChevronRight />
              </Button>
            </Capsule>
          )}
          <Capsule className="pointer-events-auto ml-auto gap-1.5 pl-3 text-xs">
            <Search className="size-3.5 shrink-0 text-muted-foreground" aria-hidden />
            <input
              type="text"
              value={search}
              onChange={event => setSearch(event.target.value)}
              onKeyDown={event => {
                if (event.key === 'Enter') {
                  jumpToMatch(searchCursor + (event.shiftKey ? -1 : 1));
                }
              }}
              placeholder="Find node…"
              aria-label="Find node"
              className="w-32 bg-transparent pr-2 text-foreground placeholder:text-muted-foreground focus:outline-none"
            />
            {search.trim() !== '' && (
              <>
                <span className="tabular-nums text-muted-foreground">
                  {matchIndices.length === 0
                    ? '0/0'
                    : `${Math.min(searchCursor, matchIndices.length - 1) + 1}/${matchIndices.length}`}
                </span>
                <Button
                  type="button"
                  variant="capsule"
                  size="capsule-icon"
                  onClick={() => jumpToMatch(searchCursor - 1)}
                  aria-label="Previous match"
                  disabled={matchIndices.length === 0}
                  className="text-muted-foreground hover:text-foreground"
                >
                  <ChevronLeft />
                </Button>
                <Button
                  type="button"
                  variant="capsule"
                  size="capsule-icon"
                  onClick={() => jumpToMatch(searchCursor + 1)}
                  aria-label="Next match"
                  disabled={matchIndices.length === 0}
                  className="text-muted-foreground hover:text-foreground"
                >
                  <ChevronRight />
                </Button>
              </>
            )}
          </Capsule>
        </div>
      </Panel>
      <Background color="var(--border)" gap={26} />
      <Controls showInteractive={false} />
      {detail !== undefined && readClock !== undefined && (
        <GraphTooltip id={tooltipId} hover={hover} detail={detail} readClock={readClock} />
      )}
    </ReactFlow>
  );
}

/** The shared tooltip of the graph: the node under the pointer with each of
 * its phases as a number and the meaning of its marks, or the edge under the
 * pointer. It follows the one-second clock of an open run only while it
 * shows. */
function GraphTooltip({
  id,
  hover,
  detail,
  readClock,
}: {
  id: string;
  hover: GraphHover;
  detail: WorkflowRunDetail;
  readClock: ReadClock;
}) {
  const drawAtMs = useDrawTime(readClock, readClock.open && hover.kind !== 'none');
  switch (hover.kind) {
    case 'none':
      return null;
    case 'node': {
      const node = detail.nodes.find(candidate => candidate.task_index === hover.taskIndex);
      // A refetch can remove the node under the pointer.
      return node === undefined ? null : (
        <FloatingTooltip id={id} xPx={hover.xPx} yPx={hover.yPx}>
          <NodeTooltip
            workflowId={detail.run.id}
            node={node}
            drawAtMs={drawAtMs}
            membership={criticalMembership(
              criticalPath(detail.nodes, detail.edges, drawAtMs),
              node.task_index
            )}
            focus={{ kind: 'node' }}
            pause={{ kind: 'not_in_pause' }}
            facts={graphNodeFacts(node, detail.run.status)}
          />
        </FloatingTooltip>
      );
    }
    case 'edge': {
      const from = detail.nodes.find(candidate => candidate.task_index === hover.fromIndex);
      const to = detail.nodes.find(candidate => candidate.task_index === hover.toIndex);
      return from === undefined || to === undefined ? null : (
        <FloatingTooltip id={id} xPx={hover.xPx} yPx={hover.yPx}>
          <EdgeText
            from={nodeLabel(from)}
            to={nodeLabel(to)}
            onPath={edgeOnPath(
              criticalPath(detail.nodes, detail.edges, drawAtMs),
              hover.fromIndex,
              hover.toIndex
            )}
          />
        </FloatingTooltip>
      );
    }
    default:
      return assertNever(hover, 'graph hover');
  }
}

/** Whether the edge joins two consecutive nodes of the critical path. */
function edgeOnPath(path: CriticalPath, fromIndex: number, toIndex: number): boolean {
  switch (path.kind) {
    case 'no_timing':
      return false;
    case 'path':
      return criticalEdgeIds(path.taskIndices).has(edgeId(fromIndex, toIndex));
    default:
      return assertNever(path, 'critical path');
  }
}

/** What an edge is: a dependency, and for a bold edge its part in the
 * critical path. */
function EdgeText({ from, to, onPath }: { from: string; to: string; onPath: boolean }) {
  return (
    <>
      <p className="font-semibold">
        {from} → {to}
      </p>
      <p>
        {to} waits for {from}.
      </p>
      {onPath && (
        <p className="text-muted-foreground">
          Bold edge: on the critical path. {from} ended last of the dependencies of {to}, so its
          end decided when {to} could start.
        </p>
      )}
    </>
  );
}

function GraphCounts({ metrics, placement }: { metrics: RunMetrics; placement: CountsPlacement }) {
  switch (placement) {
    case 'in_run_summary':
      return <span className="font-medium">{metrics.total} nodes</span>;
    case 'in_graph':
      return (
        <div className="flex items-center gap-2">
          <span className="font-medium">{metrics.total} nodes</span>
          <span style={{ color: 'var(--success)' }}>{metrics.completed} done</span>
          {metrics.running > 0 && (
            <span style={{ color: 'var(--info)' }}>{metrics.running} running</span>
          )}
          {metrics.failed > 0 && (
            <span style={{ color: 'var(--error)' }}>{metrics.failed} failed</span>
          )}
        </div>
      );
    default:
      return assertNever(placement, 'counts placement');
  }
}

/** The node with the longest time without dependency wait: the node that
 * cost the most time. A wall time would name the last node of each run. */
function LongestNodeLine({ longest }: { longest: LongestNodeTime }) {
  switch (longest.kind) {
    case 'no_timing':
      return null;
    case 'node':
      return (
        <Hint text="The node with the longest time without its dependency wait: from the time it could start to its end.">
          <span className="text-muted-foreground">
            Longest node:{' '}
            <span className="font-mono text-foreground">{nodeLabel(longest.node)}</span>{' '}
            <span className="font-mono">{formatMilliseconds(longest.ms)}</span>
          </span>
        </Hint>
      );
    default:
      return assertNever(longest, 'longest node time');
  }
}
