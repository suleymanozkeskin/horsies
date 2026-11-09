"""Bounds on the size of a workflow tree.

Both bounds fail closed:

- A ``WorkflowSpec`` with more than ``MAX_WORKFLOW_NODES`` nodes is refused
  when it is built, with ``HRS-038``.
- A child workflow deeper than ``MAX_SUBWORKFLOW_DEPTH`` is not started. Its
  ``SubWorkflowNode`` fails with ``SUBWORKFLOW_DEPTH_LIMIT_REACHED``, and the
  ``on_error`` policy of the parent workflow applies. ``horsies check`` reports
  a static nesting deeper than the bound with ``HRS-036``.
"""

from __future__ import annotations

MAX_SUBWORKFLOW_DEPTH = 32
"""The greatest depth of a child workflow.

A top-level workflow has depth 0. A child workflow has the depth of its
parent plus 1.
"""

MAX_WORKFLOW_NODES = 10_000
"""The most nodes that one ``WorkflowSpec`` holds."""
