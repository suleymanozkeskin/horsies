"""The child spec source that a broker registers on its core handle.

When a workflow step of the core needs the child of a sub-workflow node, the
core calls the registered source on its callback thread with a
``ChildSpecRequest``. The source takes the steps of ``child_build`` after
the depth check, which the core made: the load of the child definition, the
node input decoded against ``build_with``, the ``args_from`` members from
the dependency results, ``build_with``, the check of its output type, and the
node rows of the child. It answers:

- ``ChildSpecBuilt``: the child's spec as workflow spec data;
- ``ChildSpecRefused``: the node fails with the stored error, as the Python
  build fails it; also ``WORKFLOW_ENQUEUE_FAILED`` when the child's spec
  data does not encode (a text with a lone surrogate).

An exception of the source, such as ``SerializationError`` for task options
of a child root that do not parse, is not a defect of the core: the step
rolls back. A start, and a resume that failed with it, raise the same object
again to their caller, as Python's start and resume let it propagate. Phase 2,
the parent advance, the recovery passes, the worker and the web handler keep
its type and text in their record, as Python's finalize and recovery catch
it. ``KeyboardInterrupt`` and ``SystemExit`` pass through; the step rolls
back and the operation raises them again.
"""

from __future__ import annotations

import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, assert_never

from horsies._core import ChildSpecBuilt, ChildSpecRefused, ChildSpecRequest
from horsies.core.models.outcome import TaskOutcome
from horsies.core.models.tasks import OperationalErrorCode, TaskError
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.child_build import (
    ChildBuildFailed,
    ChildBuilt,
    ChildOfNode,
    build_child_within_depth,
    node_error,
    node_failure_result,
)
from horsies.core.workflows.spec_data import workflow_spec_json

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

type ChildSpecSource = Callable[[ChildSpecRequest], ChildSpecBuilt | ChildSpecRefused]


def _dependency_results(
    broker: PostgresBroker, request: ChildSpecRequest,
) -> dict[int, TaskOutcome[Any, TaskError]]:
    """The outcome of each terminal dependency of the node, by index. Does no I/O."""
    from horsies.core.workflows.engine import terminal_dependency_result

    workflow_id = str(request.parent_workflow_id)
    return {
        source.task_index: terminal_dependency_result(
            broker.app,
            workflow_id=workflow_id,
            task_index=source.task_index,
            node_id=source.node_id,
            status=source.status,
            stored_result=source.result,
            task_name=source.task_name,
            sub_definition_key=source.definition_key,
        )
        for source in request.sources
    }


def _refused(error: TaskError) -> ChildSpecRefused:
    message = error.message
    assert message is not None, (
        'a child build failure always carries a message (child_build._failed, _load_failed)'
    )
    return ChildSpecRefused(message=message, failure_result=node_failure_result(error))


def build_child_spec(
    broker: PostgresBroker, request: ChildSpecRequest,
) -> ChildSpecBuilt | ChildSpecRefused:
    """The child of the node of ``request``, built with ``broker``'s app.

    Does no I/O; it runs the child's ``build_with``.

    Raises:
        SerializationError: a value of a child node row does not serialize,
            or task options do not parse (a defect of the task function, as
            in ``build_child_within_depth``). The operation that asked rolls its step
            back; see the module documentation for where the exception goes.
    """
    node = ChildOfNode(
        workflow_id=str(request.parent_workflow_id),
        workflow_name=request.parent_workflow_name,
        task_index=request.task_index,
        parent_depth=request.child_depth - 1,
        sub_definition_key=request.definition_key,
        node_input=request.node_input,
        args_from=request.args_from,
    )
    built = build_child_within_depth(
        broker, node, _dependency_results(broker, request), request.child_depth,
    )
    match built:
        case ChildBuilt(spec=spec, nodes=rows):
            match workflow_spec_json(spec, rows):
                case Ok(ok_value=spec_json):
                    return ChildSpecBuilt(spec_json=spec_json)
                case Err(err_value=refused):
                    return _refused(node_error(
                        node.workflow_id,
                        node.task_index,
                        f'Child workflow {spec.name} does not encode as spec data: {refused}',
                        OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED,
                    ))
                case _ as unreachable:
                    assert_never(unreachable)
        case ChildBuildFailed(error=error):
            return _refused(error)


def child_spec_source_of(broker: PostgresBroker) -> ChildSpecSource:
    """The source to register on ``broker``'s core handle.

    It holds the broker weakly: the handle lives in the broker, so a strong
    reference would keep both alive. A call after the broker is gone is a
    defect: no operation of a gone broker runs.
    """
    broker_ref = weakref.ref(broker)

    def source(request: ChildSpecRequest) -> ChildSpecBuilt | ChildSpecRefused:
        live_broker = broker_ref()
        assert live_broker is not None, (
            'the core asked for a child spec after its broker was collected'
        )
        return build_child_spec(live_broker, request)

    return source
