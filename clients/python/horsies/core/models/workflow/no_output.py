"""The value of a sub-workflow node whose child workflow has no output node."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, final

from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema

__all__ = ['NoWorkflowOutput']


@final
@dataclass(frozen=True, slots=True)
class NoWorkflowOutput:
    """The success value of a sub-workflow node whose child has no output node.

    The core stores that node's result as ``Ok(null)``. A read gives
    ``Succeeded(NoWorkflowOutput())``, so no reader takes ``None`` for a value
    that the child returned. Every instance is equal to every other.

    A ``WorkflowDefinition`` with no ``config.output`` declares
    ``WorkflowDefinition[NoWorkflowOutput]``. Its ``SubWorkflowNode`` is a
    ``SubWorkflowNode[NoWorkflowOutput]``, and a task parameter that receives
    its outcome is ``TaskOutcome[NoWorkflowOutput, TaskError]``.

    The stored form is ``null``. A value other than ``null`` or
    ``NoWorkflowOutput`` does not validate.
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        _ = (source, handler)
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(cls),
                core_schema.no_info_after_validator_function(
                    _value_of_null, core_schema.none_schema(),
                ),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                _stored_form, return_schema=core_schema.none_schema(),
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, schema: CoreSchema, handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        _ = (schema, handler)
        return {'type': 'null'}


def _value_of_null(stored: None) -> NoWorkflowOutput:
    """The ``NoWorkflowOutput`` of a stored ``null``."""
    _ = stored
    return NoWorkflowOutput()


def _stored_form(value: NoWorkflowOutput) -> None:
    """The stored form of ``NoWorkflowOutput``: ``null``."""
    _ = value
