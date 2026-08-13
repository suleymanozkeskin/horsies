import { useEffect, useMemo, useRef, useState } from 'react';

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

import { useWorkflowRun } from '@/hooks/use-workflow-run';
import { formatElapsed } from '@/lib/format-duration';
import type { WorkflowNode } from '@/types/workflows';

import { layoutWorkflow, NODE_H, NODE_W, type NodePosition } from './layout';
import { residualState } from './residual';
import {
  WorkflowNodeCard,
  type WorkflowFlowNode,
  type WorkflowNodeData,
} from './workflow-node';

const nodeTypes = { workflow: WorkflowNodeCard };

/** Stable empty default so an absent statusFilter prop keeps a constant reference. */
const NO_STATUS_FILTER: ReadonlySet<string> = new Set();

/** Aggregate run metrics shown over the canvas. */
interface RunMetrics {
  total: number;
  completed: number;
  running: number;
  failed: number;
  slowest: WorkflowNode | null;
}

const nodeLabel = (node: WorkflowNode): string =>
  node.node_id ?? node.task_name ?? `#${node.task_index}`;

const drillableId = (node: WorkflowNode): string | null =>
  node.is_subworkflow && node.sub_workflow_id ? node.sub_workflow_id : null;

interface WorkflowGraphProps {
  workflowId: string;
  selectedIndex: number | null;
  onSelectNode: (node: WorkflowNode) => void;
  onDrillInto: (subWorkflowId: string, label: string) => void;
  /** Open a node's detail from failure navigation (main graph only). */
  onFocusNode?: (node: WorkflowNode) => void;
  /** Statuses to keep visible; nodes of other statuses are faded (empty = all). */
  statusFilter?: ReadonlySet<string>;
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
}: WorkflowGraphProps) {
  const { detail, isLoading, isError } = useWorkflowRun(workflowId);

  // Failure navigation: a cursor into detail.failed_indices. `focusedIndex` is
  // the node the cursor last centered on, highlighted like a selection without
  // touching the parent's selection prop.
  const instanceRef = useRef<ReactFlowInstance<WorkflowFlowNode> | null>(null);
  const [cursor, setCursor] = useState(0);
  const [focusedIndex, setFocusedIndex] = useState<number | null>(null);
  const [search, setSearch] = useState('');
  const [searchCursor, setSearchCursor] = useState(0);
  const failedIndices = useMemo(
    () => detail?.failed_indices ?? [],
    [detail?.failed_indices]
  );

  const { rfNodes, rfEdges, byIndex, positions, metrics, matchIndices } = useMemo(() => {
    if (!detail) {
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
    let slowest: WorkflowNode | null = null;
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
        node.exec_s !== null &&
        (slowest === null || node.exec_s > (slowest.exec_s ?? 0))
      ) {
        slowest = node;
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
      };
      return {
        id: String(node.task_index),
        type: 'workflow',
        position: layout.positions.get(node.task_index) ?? { x: 0, y: 0 },
        data,
        selected:
          node.task_index === selectedIndex || node.task_index === focusedIndex,
        draggable: false,
      };
    });

    const edges: Edge[] = detail.edges.map(edge => ({
      id: `${edge.from_index}-${edge.to_index}`,
      source: String(edge.from_index),
      target: String(edge.to_index),
      animated: statusByIndex.get(edge.to_index) === 'RUNNING',
      style: { stroke: 'var(--border)', strokeWidth: 1.5 },
    }));

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
        slowest,
      } satisfies RunMetrics,
      matchIndices: matches,
    };
  }, [detail, selectedIndex, focusedIndex, search, statusFilter]);

  // Reset failure/search navigation when switching runs, and clamp cursors when
  // their target sets shrink while a run is live or the query changes.
  useEffect(() => {
    setCursor(0);
    setFocusedIndex(null);
    setSearch('');
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
      nodesDraggable={false}
      nodesConnectable={false}
      edgesFocusable={false}
      fitView
      fitViewOptions={{ padding: 0.2 }}
      minZoom={0.1}
      proOptions={{ hideAttribution: true }}
    >
      {metrics && metrics.total > 0 && (
        <Panel position="top-left">
          <div className="glass flex flex-col gap-1.5 rounded-lg px-2.5 py-1.5 text-xs">
            <div className="flex items-center gap-2">
              <span className="font-medium">{metrics.total} nodes</span>
              <span style={{ color: 'var(--success)' }}>
                {metrics.completed} done
              </span>
              {metrics.running > 0 && (
                <span style={{ color: 'var(--info)' }}>
                  {metrics.running} running
                </span>
              )}
              {metrics.failed > 0 && (
                <span style={{ color: 'var(--error)' }}>
                  {metrics.failed} failed
                </span>
              )}
            </div>
            {metrics.slowest && metrics.slowest.exec_s !== null && (
              <div className="text-muted-foreground">
                slowest{' '}
                <span className="font-mono" title={metrics.slowest.task_name}>
                  {metrics.slowest.node_id ?? metrics.slowest.task_name}
                </span>{' '}
                {formatElapsed(metrics.slowest.exec_s)}
              </div>
            )}
          </div>
        </Panel>
      )}
      <Panel position="top-right">
        <div className="glass flex items-center gap-1.5 rounded-lg px-2 py-1 text-xs">
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
            className="w-32 bg-transparent text-foreground placeholder:text-muted-foreground focus:outline-none"
          />
          {search.trim() !== '' && (
            <>
              <span className="tabular-nums text-muted-foreground">
                {matchIndices.length === 0
                  ? '0/0'
                  : `${Math.min(searchCursor, matchIndices.length - 1) + 1}/${matchIndices.length}`}
              </span>
              <button
                type="button"
                onClick={() => jumpToMatch(searchCursor - 1)}
                aria-label="Previous match"
                disabled={matchIndices.length === 0}
                className="rounded p-0.5 text-muted-foreground hover:text-foreground disabled:opacity-40"
              >
                <ChevronLeft className="size-4" />
              </button>
              <button
                type="button"
                onClick={() => jumpToMatch(searchCursor + 1)}
                aria-label="Next match"
                disabled={matchIndices.length === 0}
                className="rounded p-0.5 text-muted-foreground hover:text-foreground disabled:opacity-40"
              >
                <ChevronRight className="size-4" />
              </button>
            </>
          )}
        </div>
      </Panel>
      {failedIndices.length > 0 && (
        <Panel position="top-center">
          <div className="glass flex items-center gap-1.5 rounded-lg px-2 py-1 text-xs">
            <span
              className="inline-flex items-center gap-1.5 font-medium"
              style={{ color: 'var(--error)' }}
            >
              <TriangleAlert className="size-3.5" aria-hidden />
              {failedIndices.length} failed
            </span>
            <button
              type="button"
              onClick={() => jumpToFailure(cursor - 1)}
              aria-label="Previous failure"
              className="rounded p-0.5 text-muted-foreground hover:text-foreground"
            >
              <ChevronLeft className="size-4" />
            </button>
            <span className="tabular-nums text-muted-foreground">
              {Math.min(cursor, failedIndices.length - 1) + 1}/
              {failedIndices.length}
            </span>
            <button
              type="button"
              onClick={() => jumpToFailure(cursor + 1)}
              aria-label="Next failure"
              className="rounded p-0.5 text-muted-foreground hover:text-foreground"
            >
              <ChevronRight className="size-4" />
            </button>
          </div>
        </Panel>
      )}
      <Background color="var(--border)" gap={26} />
      <Controls showInteractive={false} />
    </ReactFlow>
  );
}
