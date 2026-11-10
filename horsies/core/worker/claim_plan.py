"""The claim plan of a worker: which budget claims the rows of each class.

A worker runs children for some execution classes. Each such class has a
budget of its own: the children (``ISOLATED``) or seats (a shared class) of
that class that are in service. The plan routes the rows of every
``Execution`` class:

- A class that the worker runs children for is claimed into its own budget.
- A shared class that the worker runs no children for is claimed into the
  ``ISOLATED`` budget, when the worker runs isolated children. An isolated
  child runs a task of any class: a worker may give a task more isolation
  than its row asks, never less. So the isolated children of a worker that
  runs children of a shared class never take rows of that class.
- Any other class is not served: the worker claims none of its rows.

Priority holds within one class only. Each (queue, class) is its own stream
in (priority, enqueued_at, id) order, and a budget that serves more than one
class reads the merge of their streams in that order.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeAlias, assert_never

from horsies.core.models.tasks import Execution


@dataclass(frozen=True, slots=True)
class ServedBy:
    """The rows of the class are claimed into the budget of ``budget``."""

    budget: Execution


@dataclass(frozen=True, slots=True)
class NotServed:
    """The worker claims no row of the class."""


ClaimRoute: TypeAlias = ServedBy | NotServed


@dataclass(frozen=True, slots=True)
class ClaimPlan:
    """The routes of a worker, from the classes it runs children for.

    ``budget_classes`` are the classes that this worker runs children for.
    Each one has a budget of its own. Every other route follows from this
    set (see the module docstring), so a plan cannot hold a route that the
    rules refuse. An empty set serves no class.
    """

    budget_classes: frozenset[Execution]

    def route(self, execution: Execution) -> ClaimRoute:
        """The budget that claims rows of ``execution``, or ``NotServed``."""
        if execution in self.budget_classes:
            return ServedBy(budget=execution)
        match execution:
            case Execution.ISOLATED:
                return NotServed()
            case Execution.SHARED_ASYNC | Execution.SHARED_THREAD:
                return self._isolated_route()
            case _ as unreachable:
                assert_never(unreachable)

    def _isolated_route(self) -> ClaimRoute:
        """The route of a shared class that has no children of its own."""
        if Execution.ISOLATED in self.budget_classes:
            return ServedBy(budget=Execution.ISOLATED)
        return NotServed()

    def budgets(self) -> tuple[Execution, ...]:
        """The budget classes, in ``Execution`` order."""
        return tuple(member for member in Execution if member in self.budget_classes)

    def served_by(self, budget: Execution) -> tuple[Execution, ...]:
        """The classes whose rows the budget of ``budget`` claims."""
        return tuple(
            member
            for member in Execution
            if self.route(member) == ServedBy(budget=budget)
        )

    def unserved(self) -> tuple[Execution, ...]:
        """The classes of which this worker claims no row, in ``Execution`` order."""
        return tuple(
            member for member in Execution if self.route(member) == NotServed()
        )


def isolated_children_plan() -> ClaimPlan:
    """The plan of a worker that runs isolated children and no shared ones.

    It claims the rows of every class into its ``ISOLATED`` budget.
    """
    return ClaimPlan(budget_classes=frozenset({Execution.ISOLATED}))


@dataclass(frozen=True, slots=True)
class BudgetCapacity:
    """The capacity of one budget in a claim pass.

    ``in_service`` counts the children (``ISOLATED``) or seats (a shared
    class) of ``budget`` that are not down. It is 0 or more; the claim
    function takes it as a Postgres ``integer``.
    """

    budget: Execution
    in_service: int


def routes_json(plan: ClaimPlan) -> dict[str, str]:
    """``p_routes`` of ``horsies_claim``: each served class to its budget class.

    A class that is not served is not a key.
    """
    routes: dict[str, str] = {}
    for member in Execution:
        match plan.route(member):
            case ServedBy(budget=budget):
                routes[member.value] = budget.value
            case NotServed():
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return routes


def budgets_json(capacity: Iterable[BudgetCapacity]) -> dict[str, int]:
    """``p_budgets`` of ``horsies_claim``: each budget class to its capacity."""
    return {entry.budget.value: entry.in_service for entry in capacity}


def class_capacity_json(
    plan: ClaimPlan,
    capacity: Iterable[BudgetCapacity],
) -> dict[str, dict[str, object]]:
    """The ``class_capacity`` of a worker state snapshot.

    Each budget class maps to ``capacity`` (its capacity in service) and
    ``serves`` (the classes whose rows it claims, in ``Execution`` order). A
    class that no ``serves`` list names is not served by the worker.
    """
    return {
        entry.budget.value: {
            'capacity': entry.in_service,
            'serves': [member.value for member in plan.served_by(entry.budget)],
        }
        for entry in capacity
    }


def describe_claim_plan(plan: ClaimPlan) -> str:
    """One line for logs, for example ``ISOLATED -> ISOLATED budget, ...``."""
    parts: list[str] = []
    for member in Execution:
        match plan.route(member):
            case ServedBy(budget=budget):
                parts.append(f'{member.value} -> {budget.value} budget')
            case NotServed():
                parts.append(f'{member.value} -> not served')
            case _ as unreachable:
                assert_never(unreachable)
    return ', '.join(parts)
