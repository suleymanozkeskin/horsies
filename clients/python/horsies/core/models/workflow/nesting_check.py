"""How ``horsies check`` decides on a subworkflow nesting.

The check builds each static child spec of a registered builder and walks it.
At each ``SubWorkflowNode`` the walk does one of three things:

- it walks the child spec;
- it stops with no finding, when results at run time decide how deep the
  nesting goes; the engine applies ``MAX_SUBWORKFLOW_DEPTH`` there;
- it stops and reports a finding, when the nesting reaches the depth limit for
  certain.

The walk of one root spec builds at most ``MAX_CHECK_SUBWORKFLOW_BUILDS``
children (``ChildBuildBudget``); at the bound it stops and reports ``HRS-039``.

A check error stops a worker at boot. So the check reports only a nesting that
fails for certain, and never one that results at run time can end.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from horsies._catalog.workflow_bounds import MAX_CHECK_SUBWORKFLOW_BUILDS
from horsies.core.errors import HorsiesErrorCode, WorkflowValidationError
from horsies.core.models.workflow.limits import MAX_SUBWORKFLOW_DEPTH
from horsies.core.models.workflow.nodes import SubWorkflowNode


class ChildStart(Enum):
    """When the engine starts the child workflow of a ``SubWorkflowNode``."""

    WITH_ITS_WORKFLOW = 'with_its_workflow'
    """The node has no ``waits_for``. The engine starts its child when it
    starts the workflow that holds the node."""

    AFTER_DEPENDENCIES = 'after_dependencies'
    """The node waits for other nodes. Their results decide whether the child
    starts."""


@dataclass(frozen=True, slots=True)
class NestingStep:
    """One ``SubWorkflowNode`` on the path from the root spec of a builder."""

    definition_key: str
    kwargs_signature: str
    """A stable text form of the stored kwargs of the node."""
    start: ChildStart


NestingPath = tuple[NestingStep, ...]
"""The ``SubWorkflowNode``s from the root spec to one spec, root first.

Its length is the depth of that spec. The walk keeps it at most
``MAX_SUBWORKFLOW_DEPTH``.
"""


@dataclass(frozen=True, slots=True)
class WalkChild:
    """Build the child spec and walk it with ``path``."""

    path: NestingPath


@dataclass(frozen=True, slots=True)
class StopWithoutFinding:
    """Do not build the child. Results at run time decide whether the nesting
    reaches the depth limit, and the engine applies the limit there."""


@dataclass(frozen=True, slots=True)
class StopWithFinding:
    """Do not build the child. The nesting reaches the depth limit for
    certain."""

    finding: WorkflowValidationError


NestingDecision = WalkChild | StopWithoutFinding | StopWithFinding


@dataclass(frozen=True, slots=True)
class BuildGranted:
    """The walk may build one more child."""


@dataclass(frozen=True, slots=True)
class BuildRefusedFirst:
    """The bound is reached, and this is the first refusal. The walk stops and
    reports ``finding``."""

    finding: WorkflowValidationError


@dataclass(frozen=True, slots=True)
class BuildRefusedAgain:
    """The bound is reached, and the walk reported it already. The walk stops
    with no further finding."""


BuildDecision = BuildGranted | BuildRefusedFirst | BuildRefusedAgain


class ChildBuildBudget:
    """The builds of child specs in the walk of one root spec.

    One budget belongs to one root spec. ``take`` is called before each build.
    ``bound`` is the most builds; the check gives ``MAX_CHECK_SUBWORKFLOW_BUILDS``.
    """

    def __init__(self, bound: int = MAX_CHECK_SUBWORKFLOW_BUILDS) -> None:
        self._bound = bound
        self._builds = 0
        self._reported = False

    def take(self, node_name: str) -> BuildDecision:
        """Count one build of the child of ``node_name`` and say whether it may
        go on. Changes only the counts of this budget."""
        if self._builds < self._bound:
            self._builds += 1
            return BuildGranted()
        if self._reported:
            return BuildRefusedAgain()
        self._reported = True
        return BuildRefusedFirst(_build_limit_finding(node_name, self._bound))


def child_start(node: SubWorkflowNode[Any]) -> ChildStart:
    """Return when the engine starts the child workflow of ``node``."""
    if node.waits_for:
        return ChildStart.AFTER_DEPENDENCIES
    return ChildStart.WITH_ITS_WORKFLOW


def decide_nesting(
    path: NestingPath,
    node_name: str,
    step: NestingStep,
) -> NestingDecision:
    """Decide what the check does with the child of the node that ``step`` describes.

    ``path`` leads to the spec that holds the node. The rules, in this order:

    - The same definition with the same kwargs is already on ``path``.
      ``build_with()`` builds the same spec from the same arguments, so the
      walk stops. It reports ``HRS-037`` when no node from the earlier
      occurrence to this one waits for other nodes: once that definition
      starts, its nesting never ends.
    - The child would be deeper than ``MAX_SUBWORKFLOW_DEPTH``. The walk
      stops. It reports ``HRS-036`` when no node on the path waits for other
      nodes: the engine then starts the whole chain with the root workflow.
    - In all other cases, the walk goes on into the child.

    Reads its arguments only.
    """
    assert len(path) <= MAX_SUBWORKFLOW_DEPTH, 'the walk stops at the limit'
    walked = (*path, step)
    for index, earlier in enumerate(path):
        if _same_child(earlier, step):
            repeating = walked[index + 1:]
            if _starts_for_certain(repeating):
                return StopWithFinding(
                    _self_nesting_finding(node_name, step.definition_key),
                )
            return StopWithoutFinding()
    if len(path) >= MAX_SUBWORKFLOW_DEPTH:
        if _starts_for_certain(walked):
            return StopWithFinding(_depth_limit_finding(node_name, len(walked)))
        return StopWithoutFinding()
    return WalkChild(walked)


def _same_child(earlier: NestingStep, step: NestingStep) -> bool:
    """Return whether two steps build the same child spec."""
    return (
        earlier.definition_key == step.definition_key
        and earlier.kwargs_signature == step.kwargs_signature
    )


def _starts_for_certain(steps: NestingPath) -> bool:
    """Return whether the engine starts the child of each step with no wait."""
    return all(step.start is ChildStart.WITH_ITS_WORKFLOW for step in steps)


def _self_nesting_finding(
    node_name: str,
    definition_key: str,
) -> WorkflowValidationError:
    return WorkflowValidationError(
        message='a subworkflow includes itself with the same kwargs',
        code=HorsiesErrorCode.WORKFLOW_SUBWORKFLOW_SELF_NESTING,
        notes=[
            f"node '{node_name}': definition '{definition_key}' with these "
            'kwargs is already on the path from the root, and no node between '
            'the two waits for other nodes',
        ],
        help_text='change the kwargs at each level, so that the nesting ends',
    )


def _depth_limit_finding(node_name: str, child_depth: int) -> WorkflowValidationError:
    return WorkflowValidationError(
        message='subworkflow nesting passes the depth limit',
        code=HorsiesErrorCode.WORKFLOW_SUBWORKFLOW_DEPTH_LIMIT,
        notes=[
            f"node '{node_name}': its child workflow would have depth "
            f'{child_depth}; the limit is {MAX_SUBWORKFLOW_DEPTH}, and no node '
            'on the path waits for other nodes',
        ],
        help_text=(
            'nest fewer levels; at run time this node fails with '
            'SUBWORKFLOW_DEPTH_LIMIT_REACHED'
        ),
    )


def _build_limit_finding(node_name: str, bound: int) -> WorkflowValidationError:
    return WorkflowValidationError(
        message='the subworkflow nesting walk reached its bound of builds',
        code=HorsiesErrorCode.WORKFLOW_CHECK_BUILD_LIMIT,
        notes=[
            f"node '{node_name}': the walk of this workflow built "
            f'{bound} child workflows and stopped; '
            'the rest of its nesting is not checked',
        ],
        help_text='nest fewer child workflows in this workflow',
    )
