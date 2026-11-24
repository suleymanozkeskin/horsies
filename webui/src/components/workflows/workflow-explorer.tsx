import { useEffect, useRef, useState, type KeyboardEvent, type PointerEvent } from 'react';

import { ChevronLeft, PanelLeftClose, PanelLeftOpen, X } from 'lucide-react';

import { WorkflowActionBar } from '@/actions/action-bar';
import { useWorkflowActions } from '@/actions/use-workflow-actions';
import { Button } from '@/components/ui/button';
import { Capsule, CapsuleSeparator } from '@/components/ui/capsule';
import { PageHeader } from '@/components/ui/page-header';
import { Toggle } from '@/components/ui/toggle';
import { useLocalStorage } from '@/hooks/use-local-storage';
import { cn } from '@/lib/utils';
import type { WorkflowSearch, WorkflowSearchPatch } from '@/routes/search';
import type { WorkflowNode, WorkflowStatus } from '@/types/workflows';

import { NodeDetailContent } from './node-detail';
import { RunList } from './run-list';
import { RunTimingSummary } from './run-summary';
import { WorkflowGraph } from './workflow-graph';
import { WorkflowTimeline } from './workflow-timeline';

type ViewMode = 'graph' | 'timeline';

/** A view shown in the side panel. The panel is a small navigable stack: a node
 * detail can open a subworkflow graph, whose nodes open their own detail. */
type PanelView =
  | { kind: 'detail'; workflowId: string; node: WorkflowNode }
  | { kind: 'graph'; workflowId: string; label: string };

const MIN_PANEL_WIDTH = 360;
const MIN_MAIN_WIDTH = 400;
const DEFAULT_PANEL_WIDTH = 440;
const PANEL_RESIZE_STEP = 24; // px moved per arrow-key press on the separator

const RAIL_OPEN_KEY = 'workflow-rail-open';
const PANEL_WIDTH_KEY = 'workflow-panel-width';

function viewTitle(view: PanelView): string {
  return view.kind === 'graph'
    ? view.label
    : (view.node.node_id ?? view.node.task_name ?? `#${view.node.task_index}`);
}

export interface WorkflowExplorerProps {
  search: WorkflowSearch;
  onSearchChange: (patch: WorkflowSearchPatch) => void;
}

/**
 * Workflow-run explorer: pick a run and view its DAG. Selecting a node opens a
 * resizable side panel; opening a subworkflow renders the child graph inside
 * that same panel, so the parent is never navigated away from. The selected run
 * and the open root node live in the URL.
 */
export function WorkflowExplorer({
  search,
  onSearchChange,
}: WorkflowExplorerProps) {
  const [railOpen, setRailOpen] = useLocalStorage(RAIL_OPEN_KEY, true);
  const [panelWidth, setPanelWidth] = useLocalStorage(
    PANEL_WIDTH_KEY,
    DEFAULT_PANEL_WIDTH
  );
  const [views, setViews] = useState<PanelView[]>([]);
  const [viewMode, setViewMode] = useState<ViewMode>('graph');
  const [statusFilter, setStatusFilter] = useState<Set<string>>(new Set());
  const containerRef = useRef<HTMLDivElement>(null);

  const runId = search.run ?? null;
  const workflow = useWorkflowActions(runId);
  const runDetail = workflow.detail;

  // Status filter of the run summary: toggling a status fades nodes of other
  // statuses (empty set = show everything).
  const toggleStatusFilter = (status: WorkflowStatus): void =>
    setStatusFilter(previous => {
      const next = new Set(previous);
      if (next.has(status)) {
        next.delete(status);
      } else {
        next.add(status);
      }
      return next;
    });

  const top = views.length > 0 ? views[views.length - 1] : undefined;

  // Highlight the main-graph node whose detail is on top of the panel stack
  // (null while viewing a subworkflow graph or a child detail).
  const rootSelectedIndex =
    top?.kind === 'detail' && runId !== null && top.workflowId === runId
      ? top.node.task_index
      : null;

  // Deep link: restore the open node once the run's nodes are known. Only while
  // the stack is empty, so a drilled-in stack is never clobbered by a refetch.
  const hydrated = useRef(false);
  useEffect(() => {
    if (hydrated.current || runDetail === undefined || runId === null) {
      return;
    }
    const target = search.node;
    if (target !== undefined) {
      const node = runDetail.nodes.find(
        candidate => candidate.task_index === target
      );
      if (node !== undefined) {
        setViews([{ kind: 'detail', workflowId: runId, node }]);
      }
    }
    hydrated.current = true;
  }, [runDetail, runId, search.node]);

  // Reflect the open root node back into the URL, once hydration has had its
  // chance — otherwise the initial empty stack would erase the deep link.
  useEffect(() => {
    if (!hydrated.current) {
      return;
    }
    onSearchChange({ node: rootSelectedIndex ?? undefined });
    // `onSearchChange` is recreated per render by the route; depending on it
    // would rewrite the URL on every render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rootSelectedIndex]);

  const pushDetail = (workflowId: string, node: WorkflowNode): void =>
    setViews(previous => [...previous, { kind: 'detail', workflowId, node }]);
  const pushGraph = (workflowId: string, label: string): void =>
    setViews(previous => [...previous, { kind: 'graph', workflowId, label }]);
  // Failure navigation: show the focused node's detail, replacing the top view
  // when it is already a detail at the same run so repeated jumps don't stack.
  const focusDetail = (workflowId: string, node: WorkflowNode): void =>
    setViews(previous => {
      const last = previous[previous.length - 1];
      if (last && last.kind === 'detail' && last.workflowId === workflowId) {
        return [...previous.slice(0, -1), { kind: 'detail', workflowId, node }];
      }
      return [...previous, { kind: 'detail', workflowId, node }];
    });
  const back = (): void => setViews(previous => previous.slice(0, -1));
  const closePanel = (): void => setViews([]);

  /** Clamp a candidate panel width so the main graph keeps a usable width. */
  const clampPanelWidth = (next: number): number => {
    const rect = containerRef.current?.getBoundingClientRect();
    const max = rect ? rect.width - MIN_MAIN_WIDTH : next;
    return Math.max(MIN_PANEL_WIDTH, Math.min(next, max));
  };

  // The side panel sits on the container's right edge; width = distance from
  // the pointer to that edge.
  const startResize = (event: PointerEvent<HTMLDivElement>): void => {
    event.preventDefault();
    const onMove = (move: globalThis.PointerEvent): void => {
      const rect = containerRef.current?.getBoundingClientRect();
      if (!rect) {
        return;
      }
      setPanelWidth(clampPanelWidth(rect.right - move.clientX));
    };
    const onUp = (): void => {
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    };
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
  };

  // Arrow keys widen/narrow the panel; the panel grows leftward, so Left widens.
  const onSeparatorKeyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    const stepByKey: Record<string, number> = {
      ArrowLeft: PANEL_RESIZE_STEP,
      ArrowRight: -PANEL_RESIZE_STEP,
    };
    const delta = stepByKey[event.key] ?? 0;
    if (delta !== 0) {
      event.preventDefault();
      setPanelWidth(width => clampPanelWidth(width + delta));
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col gap-4">
      <PageHeader title="Workflows" />
      <div ref={containerRef} className="flex min-h-0 flex-1">
        {railOpen && (
          <div className="w-72 shrink-0">
            <RunList
              selectedRunId={runId}
              onSelect={run => {
                setViews([]);
                onSearchChange({ run: run.id, node: undefined });
              }}
            />
          </div>
        )}

        <div
          className={cn(
            'flex min-w-0 flex-1 flex-col',
            railOpen && 'pl-4',
            top !== undefined && 'pr-4'
          )}
        >
          <div className="flex flex-wrap items-center gap-3 pb-3 text-sm">
            <Capsule>
              <Button
                type="button"
                variant="capsule"
                size="capsule-icon"
                onClick={() => setRailOpen(open => !open)}
                aria-label={railOpen ? 'Collapse run list' : 'Expand run list'}
                title={railOpen ? 'Collapse run list' : 'Expand run list'}
                className="text-muted-foreground hover:text-foreground"
              >
                {railOpen ? <PanelLeftClose /> : <PanelLeftOpen />}
              </Button>
              {runId !== null && runDetail !== undefined && (
                <>
                  <CapsuleSeparator />
                  <Toggle
                    variant="segment"
                    size="capsule"
                    pressed={viewMode === 'graph'}
                    onClick={() => setViewMode('graph')}
                  >
                    Graph
                  </Toggle>
                  <Toggle
                    variant="segment"
                    size="capsule"
                    pressed={viewMode === 'timeline'}
                    onClick={() => setViewMode('timeline')}
                  >
                    Timeline
                  </Toggle>
                  {/* Hidden when the action bar renders nothing, or renders
                      an empty bar because no action is shown. */}
                  <CapsuleSeparator className="last:hidden [&:has(+:empty)]:hidden" />
                  <WorkflowActionBar view={workflow} />
                </>
              )}
            </Capsule>
            {runId !== null && runDetail !== undefined ? (
              <span
                className="min-w-0 flex-1 truncate font-medium"
                title={runDetail.run.name}
              >
                {runDetail.run.name}
              </span>
            ) : (
              <span className="text-muted-foreground">
                Select a workflow run to view its graph.
              </span>
            )}
          </div>
          {runId !== null && runDetail !== undefined && (
            <RunTimingSummary
              workflowId={runId}
              view={viewMode}
              statusFilter={statusFilter}
              onToggleStatus={toggleStatusFilter}
            />
          )}
          <div className="min-h-0 flex-1">
            {runId !== null && viewMode === 'graph' ? (
              <WorkflowGraph
                workflowId={runId}
                selectedIndex={rootSelectedIndex}
                onSelectNode={node => pushDetail(runId, node)}
                onDrillInto={(subId, label) => pushGraph(subId, label)}
                onFocusNode={node => focusDetail(runId, node)}
                statusFilter={statusFilter}
                countsPlacement="in_run_summary"
              />
            ) : runId !== null && viewMode === 'timeline' ? (
              <WorkflowTimeline
                workflowId={runId}
                selectedIndex={rootSelectedIndex}
                onSelectNode={node => focusDetail(runId, node)}
                statusFilter={statusFilter}
              />
            ) : (
              <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
                {railOpen
                  ? 'Select a workflow run to view its graph.'
                  : 'Expand the run list to pick a workflow.'}
              </div>
            )}
          </div>
        </div>

        {top !== undefined && (
          <>
            <div
              role="separator"
              aria-orientation="vertical"
              aria-label="Resize panel"
              aria-valuemin={MIN_PANEL_WIDTH}
              aria-valuenow={Math.round(panelWidth)}
              tabIndex={0}
              onPointerDown={startResize}
              onKeyDown={onSeparatorKeyDown}
              className="w-1 shrink-0 cursor-col-resize transition-colors hover:bg-primary focus:outline-none focus-visible:bg-primary"
            />
            <div
              className="flex shrink-0 flex-col border-l border-hairline"
              style={{ width: panelWidth }}
            >
              <div className="box-content flex h-(--capsule-height) shrink-0 items-center gap-2 border-b border-hairline pb-3 pl-4">
                {views.length > 1 && (
                  <button
                    type="button"
                    onClick={back}
                    aria-label="Back"
                    className="inline-flex size-7 shrink-0 items-center justify-center rounded-full bg-glass-inset text-muted-foreground transition-colors hover:text-foreground"
                  >
                    <ChevronLeft className="size-4" />
                  </button>
                )}
                <span
                  className="min-w-0 flex-1 truncate text-sm font-semibold"
                  title={viewTitle(top)}
                >
                  {viewTitle(top)}
                </span>
                <button
                  type="button"
                  onClick={closePanel}
                  aria-label="Close panel"
                  className="inline-flex size-7 shrink-0 items-center justify-center rounded-full bg-glass-inset text-muted-foreground transition-colors hover:text-foreground"
                >
                  <X className="size-4" />
                </button>
              </div>

              <div className="min-h-0 flex-1 overflow-hidden pl-4">
                {top.kind === 'detail' ? (
                  <div className="h-full overflow-y-auto">
                    <NodeDetailContent
                      workflowId={top.workflowId}
                      runStatus={
                        top.workflowId === runId
                          ? (runDetail?.run.status ?? '')
                          : ''
                      }
                      node={top.node}
                      onOpenSubworkflow={(subId, label) => pushGraph(subId, label)}
                    />
                  </div>
                ) : (
                  <WorkflowGraph
                    workflowId={top.workflowId}
                    selectedIndex={null}
                    onSelectNode={node => pushDetail(top.workflowId, node)}
                    onDrillInto={(subId, label) => pushGraph(subId, label)}
                    statusFilter={statusFilter}
                    countsPlacement="in_graph"
                  />
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
