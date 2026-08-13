// Longest-path layered DAG layout: a layer with many nodes (a wide fan-out)
// wraps into several sub-columns instead of one tall column, so any workflow
// shape lays out readably. Pure — returns absolute positions by task_index.

import type { WorkflowEdge, WorkflowNode } from '@/types/workflows';

export const NODE_W = 200;
export const NODE_H = 48;

const GAP_X = 64; // horizontal gap between layers
const GAP_Y = 14; // vertical gap between stacked nodes
const SUBGAP_X = 26; // gap between sub-columns inside one wide layer
const MAX_PER_COL = 16; // wrap a layer into sub-columns past this height

export interface NodePosition {
  x: number;
  y: number;
}

export interface LayoutResult {
  positions: Map<number, NodePosition>;
  width: number;
  height: number;
}

/** Assign each node a layer = longest path from a root (no incoming edges).
 * Returns the layer map and the predecessor adjacency (reused for ordering). */
function computeLayers(
  nodes: WorkflowNode[],
  edges: WorkflowEdge[]
): { layer: Map<number, number>; preds: Map<number, number[]> } {
  const preds = new Map<number, number[]>();
  for (const node of nodes) {
    preds.set(node.task_index, []);
  }
  for (const edge of edges) {
    const target = preds.get(edge.to_index);
    if (target !== undefined && preds.has(edge.from_index)) {
      target.push(edge.from_index);
    }
  }

  const layer = new Map<number, number>();
  const visiting = new Set<number>();
  const depth = (index: number): number => {
    const cached = layer.get(index);
    if (cached !== undefined) {
      return cached;
    }
    if (visiting.has(index)) {
      return 0; // defensive: break any accidental cycle
    }
    visiting.add(index);
    const parents = preds.get(index) ?? [];
    const value = parents.length ? 1 + Math.max(...parents.map(depth)) : 0;
    layer.set(index, value);
    return value;
  };
  for (const node of nodes) {
    depth(node.task_index);
  }
  return { layer, preds };
}

/** Order nodes within each layer by the mean position of their predecessors in
 * the previous layer (barycenter heuristic) to reduce edge crossings. Roots
 * keep a stable task_index order. */
function orderWithinLayers(
  orderedLayers: number[],
  byLayer: Map<number, WorkflowNode[]>,
  preds: Map<number, number[]>
): void {
  const position = new Map<number, number>();
  const barycenter = (node: WorkflowNode): number => {
    const positions = (preds.get(node.task_index) ?? [])
      .map(parent => position.get(parent))
      .filter((value): value is number => value !== undefined);
    return positions.length
      ? positions.reduce((sum, value) => sum + value, 0) / positions.length
      : node.task_index;
  };
  orderedLayers.forEach((layerKey, layerIdx) => {
    const members = byLayer.get(layerKey) ?? [];
    if (layerIdx === 0) {
      members.sort((a, b) => a.task_index - b.task_index);
    } else {
      members.sort(
        (a, b) => barycenter(a) - barycenter(b) || a.task_index - b.task_index
      );
    }
    members.forEach((node, row) => position.set(node.task_index, row));
  });
}

/** Lay nodes out left-to-right by layer, wrapping wide layers into sub-columns. */
export function layoutWorkflow(
  nodes: WorkflowNode[],
  edges: WorkflowEdge[]
): LayoutResult {
  const positions = new Map<number, NodePosition>();
  if (nodes.length === 0) {
    return { positions, width: 0, height: 0 };
  }

  const { layer, preds } = computeLayers(nodes, edges);
  const byLayer = new Map<number, WorkflowNode[]>();
  for (const node of nodes) {
    const key = layer.get(node.task_index) ?? 0;
    const bucket = byLayer.get(key);
    if (bucket === undefined) {
      byLayer.set(key, [node]);
    } else {
      bucket.push(node);
    }
  }

  const orderedLayers = [...byLayer.keys()].sort((a, b) => a - b);
  orderWithinLayers(orderedLayers, byLayer, preds);

  // First pass: per-layer geometry (sub-column count, layer width/height).
  const geom = orderedLayers.map(key => {
    const members = byLayer.get(key) ?? [];
    const subCols = Math.max(1, Math.ceil(members.length / MAX_PER_COL));
    const rowsPerCol = Math.ceil(members.length / subCols);
    const width = subCols * (NODE_W + SUBGAP_X) - SUBGAP_X;
    const height = rowsPerCol * (NODE_H + GAP_Y) - GAP_Y;
    return { members, rowsPerCol, width, height };
  });

  const maxHeight = Math.max(...geom.map(entry => entry.height));

  // Second pass: absolute placement, each layer vertically centered.
  let layerX = 0;
  for (const entry of geom) {
    const topPad = (maxHeight - entry.height) / 2;
    entry.members.forEach((node, index) => {
      const subColumn = Math.floor(index / entry.rowsPerCol);
      const row = index % entry.rowsPerCol;
      positions.set(node.task_index, {
        x: layerX + subColumn * (NODE_W + SUBGAP_X),
        y: topPad + row * (NODE_H + GAP_Y),
      });
    });
    layerX += entry.width + GAP_X;
  }

  return { positions, width: layerX - GAP_X, height: maxHeight };
}
