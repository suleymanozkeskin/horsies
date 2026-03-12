"""Unit tests for horsies.core.models.workflow.typing_utils."""

from __future__ import annotations

import inspect
from typing import Any, Generic, TypeVar, Union
from types import UnionType
from unittest.mock import MagicMock, patch

import pytest

from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow.nodes import TaskNode, SubWorkflowNode
from horsies.core.models.workflow.typing_utils import (
    _get_inspect_target,
    _task_accepts_workflow_ctx,
    _get_signature,
    _signature_accepts_kwargs,
    _valid_kwarg_names,
    _resolved_type_hints,
    _extract_taskresult_ok_type,
    _normalize_resolved_ok_type,
    _resolve_task_fn_ok_type,
    _resolve_workflow_def_ok_type,
    _resolve_source_node_ok_type,
    validate_workflow_generic_output_match,
    _format_type_name,
    _is_ok_type_compatible,
)

T = TypeVar("T")


# =============================================================================
# Helpers
# =============================================================================


class _Uninspectable:
    """Callable whose signature cannot be inspected."""

    @property
    def __signature__(self) -> inspect.Signature:
        raise TypeError("no signature")

    def __call__(self) -> None:
        pass  # pragma: no cover


def _plain_fn(x: int, y: str) -> str:
    return f"{x}{y}"  # pragma: no cover


def _fn_with_workflow_ctx(workflow_ctx: Any, x: int) -> None:
    pass  # pragma: no cover


def _fn_no_workflow_ctx(x: int) -> None:
    pass  # pragma: no cover


def _fn_returns_task_result(x: int) -> TaskResult[str, TaskError]:
    ...  # pragma: no cover


def _fn_returns_optional_task_result(
    x: int,
) -> TaskResult[int, TaskError] | None:
    ...  # pragma: no cover


def _fn_no_return_annotation(x: int):
    pass  # pragma: no cover


# =============================================================================
# TestGetInspectTarget
# =============================================================================


@pytest.mark.unit
class TestGetInspectTarget:
    """Tests for _get_inspect_target()."""

    def test_plain_function_returns_itself(self) -> None:
        # Arrange / Act
        result = _get_inspect_target(_plain_fn)

        # Assert
        assert result is _plain_fn

    def test_function_with_callable_original_fn_returns_original(self) -> None:
        # Arrange
        def wrapper() -> None:
            pass  # pragma: no cover

        def original() -> None:
            pass  # pragma: no cover

        wrapper._original_fn = original  # type: ignore[attr-defined]

        # Act
        result = _get_inspect_target(wrapper)

        # Assert
        assert result is original

    def test_function_with_non_callable_original_fn_returns_itself(self) -> None:
        # Arrange
        def wrapper() -> None:
            pass  # pragma: no cover

        wrapper._original_fn = "not_callable"  # type: ignore[attr-defined]

        # Act
        result = _get_inspect_target(wrapper)

        # Assert
        assert result is wrapper


# =============================================================================
# TestTaskAcceptsWorkflowCtx
# =============================================================================


@pytest.mark.unit
class TestTaskAcceptsWorkflowCtx:
    """Tests for _task_accepts_workflow_ctx()."""

    def test_returns_true_when_param_present(self) -> None:
        result = _task_accepts_workflow_ctx(_fn_with_workflow_ctx)
        assert result is True

    def test_returns_false_when_param_absent(self) -> None:
        result = _task_accepts_workflow_ctx(_fn_no_workflow_ctx)
        assert result is False

    def test_returns_false_for_uninspectable_callable(self) -> None:
        # Arrange
        uninspectable = _Uninspectable()

        # Act
        result = _task_accepts_workflow_ctx(uninspectable)

        # Assert
        assert result is False


# =============================================================================
# TestGetSignature
# =============================================================================


@pytest.mark.unit
class TestGetSignature:
    """Tests for _get_signature()."""

    def test_normal_function_returns_signature(self) -> None:
        sig = _get_signature(_plain_fn)

        assert sig is not None
        assert isinstance(sig, inspect.Signature)
        assert "x" in sig.parameters
        assert "y" in sig.parameters

    def test_uninspectable_callable_returns_none(self) -> None:
        uninspectable = _Uninspectable()

        result = _get_signature(uninspectable)

        assert result is None

    def test_callable_with_original_fn_uses_original(self) -> None:
        # Arrange
        def original(a: int, b: str) -> None:
            pass  # pragma: no cover

        def wrapper(*args: Any, **kwargs: Any) -> None:
            pass  # pragma: no cover

        wrapper._original_fn = original  # type: ignore[attr-defined]

        # Act
        sig = _get_signature(wrapper)

        # Assert
        assert sig is not None
        assert "a" in sig.parameters
        assert "b" in sig.parameters


# =============================================================================
# TestSignatureAcceptsKwargs
# =============================================================================


@pytest.mark.unit
class TestSignatureAcceptsKwargs:
    """Tests for _signature_accepts_kwargs()."""

    def test_function_with_kwargs_returns_true(self) -> None:
        def fn(x: int, **kwargs: Any) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        assert _signature_accepts_kwargs(sig) is True

    def test_function_without_kwargs_returns_false(self) -> None:
        def fn(x: int, y: str) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        assert _signature_accepts_kwargs(sig) is False

    def test_empty_signature_returns_false(self) -> None:
        def fn() -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        assert _signature_accepts_kwargs(sig) is False


# =============================================================================
# TestValidKwargNames
# =============================================================================


@pytest.mark.unit
class TestValidKwargNames:
    """Tests for _valid_kwarg_names()."""

    def test_positional_or_keyword_params_included(self) -> None:
        def fn(a: int, b: str) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig)
        assert result == {"a", "b"}

    def test_keyword_only_params_included(self) -> None:
        def fn(*, a: int, b: str) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig)
        assert result == {"a", "b"}

    def test_var_keyword_excluded(self) -> None:
        def fn(a: int, **kwargs: Any) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig)
        assert "kwargs" not in result
        assert result == {"a"}

    def test_var_positional_excluded(self) -> None:
        def fn(*args: Any, kw: int) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig)
        assert "args" not in result
        assert result == {"kw"}

    def test_exclude_parameter(self) -> None:
        def fn(a: int, b: str, c: float) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig, exclude={"a", "c"})
        assert result == {"b"}

    def test_exclude_none_includes_all(self) -> None:
        def fn(a: int, b: str) -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig, exclude=None)
        assert result == {"a", "b"}

    def test_empty_signature(self) -> None:
        def fn() -> None:
            pass  # pragma: no cover

        sig = inspect.signature(fn)
        result = _valid_kwarg_names(sig)
        assert result == set()


# =============================================================================
# TestResolvedTypeHints
# =============================================================================


@pytest.mark.unit
class TestResolvedTypeHints:
    """Tests for _resolved_type_hints()."""

    def test_normal_function_returns_hints(self) -> None:
        hints = _resolved_type_hints(_plain_fn)

        assert hints["x"] is int
        assert hints["y"] is str
        assert hints["return"] is str

    def test_class_with_call_fallback(self) -> None:
        # Arrange: a class where get_type_hints on the instance fails
        # but __call__ succeeds
        class CallableClass:
            def __call__(self, x: int) -> str:
                ...  # pragma: no cover

        instance = CallableClass()

        # Act
        hints = _resolved_type_hints(instance)

        # Assert — should get hints from either the instance or __call__
        assert "x" in hints or "return" in hints

    def test_all_targets_fail_returns_empty_dict(self) -> None:
        # Arrange: patch get_type_hints to always raise
        with patch(
            "horsies.core.models.workflow.typing_utils.get_type_hints",
            autospec=True,
            side_effect=Exception("boom"),
        ):
            result = _resolved_type_hints(_plain_fn)

        assert result == {}

    def test_function_with_original_fn_gets_hints_from_original(self) -> None:
        # Arrange
        def original(x: int) -> str:
            ...  # pragma: no cover

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            ...  # pragma: no cover

        wrapper._original_fn = original  # type: ignore[attr-defined]

        # Act
        hints = _resolved_type_hints(wrapper)

        # Assert
        assert hints.get("x") is int
        assert hints.get("return") is str


# =============================================================================
# TestExtractTaskResultOkType
# =============================================================================


@pytest.mark.unit
class TestExtractTaskResultOkType:
    """Tests for _extract_taskresult_ok_type()."""

    def test_none_annotation_returns_none(self) -> None:
        assert _extract_taskresult_ok_type(None) is None

    def test_task_result_with_two_args_extracts_ok_type(self) -> None:
        annotation = TaskResult[int, TaskError]
        result = _extract_taskresult_ok_type(annotation)
        assert result is int

    def test_task_result_wrong_arg_count_returns_none(self) -> None:
        # Arrange: manually construct a generic alias with only 1 arg
        # TaskResult normally requires 2 args, but we can test the guard
        # by mocking get_args to return a tuple of length != 2
        with patch(
            "horsies.core.models.workflow.typing_utils.get_args",
            autospec=True,
            return_value=(int,),
        ), patch(
            "horsies.core.models.workflow.typing_utils.get_origin",
            autospec=True,
            return_value=TaskResult,
        ):
            result = _extract_taskresult_ok_type(TaskResult)

        assert result is None

    def test_optional_task_result_extracts_ok_type(self) -> None:
        # Union[TaskResult[int, TaskError], None]
        annotation = Union[TaskResult[int, TaskError], None]
        result = _extract_taskresult_ok_type(annotation)
        assert result is int

    def test_union_with_none_extracts_ok_type(self) -> None:
        annotation = Union[None, TaskResult[str, TaskError]]
        result = _extract_taskresult_ok_type(annotation)
        assert result is str

    def test_pipe_syntax_union_extracts_ok_type(self) -> None:
        # Python 3.10+ pipe syntax: TaskResult[int, TaskError] | None
        annotation = TaskResult[int, TaskError] | None
        result = _extract_taskresult_ok_type(annotation)
        assert result is int

    def test_ambiguous_union_returns_none(self) -> None:
        # Two different TaskResult extractions → ambiguous
        annotation = Union[TaskResult[int, TaskError], TaskResult[str, TaskError]]
        result = _extract_taskresult_ok_type(annotation)
        assert result is None

    def test_unrelated_type_returns_none(self) -> None:
        assert _extract_taskresult_ok_type(int) is None
        assert _extract_taskresult_ok_type(str) is None
        assert _extract_taskresult_ok_type(list[int]) is None


# =============================================================================
# TestNormalizeResolvedOkType
# =============================================================================


@pytest.mark.unit
class TestNormalizeResolvedOkType:
    """Tests for _normalize_resolved_ok_type()."""

    def test_none_returns_none(self) -> None:
        assert _normalize_resolved_ok_type(None) is None

    def test_typevar_returns_none(self) -> None:
        tv = TypeVar("T")
        assert _normalize_resolved_ok_type(tv) is None

    def test_concrete_type_returns_itself(self) -> None:
        assert _normalize_resolved_ok_type(int) is int
        assert _normalize_resolved_ok_type(str) is str


# =============================================================================
# TestResolveTaskFnOkType
# =============================================================================


@pytest.mark.unit
class TestResolveTaskFnOkType:
    """Tests for _resolve_task_fn_ok_type()."""

    def test_explicit_task_ok_type_attr(self) -> None:
        # Arrange
        def fn() -> None:
            pass  # pragma: no cover

        fn.task_ok_type = int  # type: ignore[attr-defined]

        # Act / Assert
        assert _resolve_task_fn_ok_type(fn) is int

    def test_return_annotation_fallback_via_hints(self) -> None:
        # Arrange: function with TaskResult return annotation
        # Act / Assert
        result = _resolve_task_fn_ok_type(_fn_returns_task_result)
        assert result is str

    def test_signature_fallback_when_hints_fail(self) -> None:
        # Arrange: when get_type_hints fails, the code falls back to
        # sig.return_annotation. Because `from __future__ import annotations`
        # makes annotations strings, we need to mock _get_signature to return
        # a signature with the real type object as return_annotation.
        real_annotation = TaskResult[float, TaskError]
        mock_sig = inspect.Signature(
            parameters=[
                inspect.Parameter("x", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ],
            return_annotation=real_annotation,
        )

        def fn(x: int) -> None:
            ...  # pragma: no cover

        with patch(
            "horsies.core.models.workflow.typing_utils.get_type_hints",
            autospec=True,
            side_effect=Exception("hints failed"),
        ), patch(
            "horsies.core.models.workflow.typing_utils._get_signature",
            autospec=True,
            return_value=mock_sig,
        ):
            result = _resolve_task_fn_ok_type(fn)

        assert result is float

    def test_no_inspectable_signature_returns_none(self) -> None:
        # Arrange
        uninspectable = _Uninspectable()

        # Hints will also fail for uninspectable
        with patch(
            "horsies.core.models.workflow.typing_utils.get_type_hints",
            autospec=True,
            side_effect=Exception("no hints"),
        ):
            result = _resolve_task_fn_ok_type(uninspectable)

        assert result is None

    def test_signature_with_no_return_annotation_returns_none(self) -> None:
        # Arrange: function with no return annotation and no task_ok_type
        result = _resolve_task_fn_ok_type(_fn_no_return_annotation)
        assert result is None

    def test_typevar_task_ok_type_normalized_to_none(self) -> None:
        # Arrange
        def fn() -> None:
            pass  # pragma: no cover

        fn.task_ok_type = TypeVar("T")  # type: ignore[attr-defined]

        # Act / Assert
        assert _resolve_task_fn_ok_type(fn) is None

    def test_nothing_resolvable_returns_none(self) -> None:
        # Arrange: function with no attr, no hints, no sig
        uninspectable = _Uninspectable()

        with patch(
            "horsies.core.models.workflow.typing_utils.get_type_hints",
            autospec=True,
            side_effect=Exception("no hints"),
        ):
            result = _resolve_task_fn_ok_type(uninspectable)

        assert result is None

    def test_explicit_task_ok_type_none_falls_through_to_hints(self) -> None:
        # Arrange: task_ok_type is None (not set to a useful type)
        # so it should fall through to hints
        result = _resolve_task_fn_ok_type(_fn_returns_task_result)
        assert result is str

    def test_signature_return_empty_returns_none(self) -> None:
        # Arrange: signature exists but return_annotation is empty
        # _fn_no_return_annotation has no type hints and signature return is empty
        with patch(
            "horsies.core.models.workflow.typing_utils.get_type_hints",
            autospec=True,
            side_effect=Exception("hints fail"),
        ):
            result = _resolve_task_fn_ok_type(_fn_no_return_annotation)

        assert result is None


# =============================================================================
# TestResolveWorkflowDefOkType
# =============================================================================


@pytest.mark.unit
class TestResolveWorkflowDefOkType:
    """Tests for _resolve_workflow_def_ok_type()."""

    def test_generic_subclass_resolves_ok_type(self) -> None:
        # Arrange: avoid triggering WorkflowDefinitionMeta by using a
        # simulated class hierarchy with __orig_bases__
        from horsies.core.models.workflow.definition import WorkflowDefinition

        class MyWorkflow(WorkflowDefinition[int]):
            name = "test_wf"

        # Act
        result = _resolve_workflow_def_ok_type(MyWorkflow)

        # Assert
        assert result is int

    def test_bare_workflow_definition_returns_none(self) -> None:
        # Arrange: a class without generic parameter
        # We simulate this with a mock that has no __orig_bases__ containing
        # WorkflowDefinition
        mock_cls = type("BareWorkflow", (), {})

        result = _resolve_workflow_def_ok_type(mock_cls)  # type: ignore[arg-type]

        assert result is None

    def test_typevar_generic_returns_none(self) -> None:
        # Arrange: WorkflowDefinition[T] where T is a TypeVar
        from horsies.core.models.workflow.definition import WorkflowDefinition

        OkT = TypeVar("OkT")

        # Create a class whose __orig_bases__ contains WorkflowDefinition[TypeVar]
        mock_cls = type(
            "GenericWorkflow",
            (),
            {"__orig_bases__": (WorkflowDefinition[OkT],)},  # type: ignore[name-defined]
        )

        result = _resolve_workflow_def_ok_type(mock_cls)  # type: ignore[arg-type]

        assert result is None

    def test_wrong_arg_count_returns_none(self) -> None:
        # Arrange: simulate __orig_bases__ with wrong arg count
        mock_base = MagicMock()
        mock_origin = MagicMock()
        mock_origin.__name__ = "WorkflowDefinition"

        with patch(
            "horsies.core.models.workflow.typing_utils.get_origin",
            autospec=True,
            return_value=mock_origin,
        ), patch(
            "horsies.core.models.workflow.typing_utils.get_args",
            autospec=True,
            return_value=(),  # 0 args, not 1
        ):
            mock_cls = type(
                "BadWorkflow",
                (),
                {"__orig_bases__": (mock_base,)},
            )

            result = _resolve_workflow_def_ok_type(mock_cls)  # type: ignore[arg-type]

        assert result is None

    def test_non_workflow_definition_bases_skipped(self) -> None:
        # Arrange: class whose __orig_bases__ has a non-WorkflowDefinition
        # generic BEFORE the WorkflowDefinition one. This exercises the
        # `continue` on line 188 where origin.__name__ != 'WorkflowDefinition'.
        from horsies.core.models.workflow.definition import WorkflowDefinition

        WfBase = WorkflowDefinition[str]

        # Build a class with __orig_bases__ containing Generic[T] first,
        # then WorkflowDefinition[str]. The function iterates __orig_bases__
        # and should skip Generic[T] via `continue`, then find WorkflowDefinition[str].
        mock_cls = type(
            "MixedBases",
            (),
            {"__orig_bases__": (Generic[T], WfBase)},
        )

        # Act
        result = _resolve_workflow_def_ok_type(mock_cls)  # type: ignore[arg-type]

        # Assert
        assert result is str


# =============================================================================
# TestResolveSourceNodeOkType
# =============================================================================


@pytest.mark.unit
class TestResolveSourceNodeOkType:
    """Tests for _resolve_source_node_ok_type()."""

    def test_task_node_delegates_to_resolve_task_fn(self) -> None:
        # Arrange
        def fn() -> TaskResult[int, TaskError]:
            ...  # pragma: no cover

        fn.task_ok_type = int  # type: ignore[attr-defined]

        mock_fn = MagicMock()
        mock_fn.task_ok_type = int

        node = MagicMock(spec=TaskNode)
        node.fn = mock_fn

        # Act
        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_task_fn_ok_type",
            autospec=True,
            return_value=int,
        ) as mock_resolve:
            result = _resolve_source_node_ok_type(node)

        # Assert
        mock_resolve.assert_called_once_with(mock_fn)
        assert result is int

    def test_sub_workflow_node_delegates_to_resolve_workflow_def(self) -> None:
        # Arrange
        mock_def = MagicMock()
        node = MagicMock(spec=SubWorkflowNode)
        node.workflow_def = mock_def

        # Act
        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=str,
        ) as mock_resolve:
            result = _resolve_source_node_ok_type(node)

        # Assert
        mock_resolve.assert_called_once_with(mock_def)
        assert result is str


# =============================================================================
# TestValidateWorkflowGenericOutputMatch
# =============================================================================


@pytest.mark.unit
class TestValidateWorkflowGenericOutputMatch:
    """Tests for validate_workflow_generic_output_match()."""

    def test_no_output_skips_validation(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = None

        # Act / Assert — should not raise
        validate_workflow_generic_output_match(mock_cls, mock_spec)

    def test_unresolvable_declared_type_skips_validation(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=None,
        ):
            # Act / Assert — should not raise
            validate_workflow_generic_output_match(mock_cls, mock_spec)

    def test_declared_any_skips_validation(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=Any,
        ):
            validate_workflow_generic_output_match(mock_cls, mock_spec)

    def test_actual_unresolvable_skips_validation(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)
        mock_spec.name = "test_wf"

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=int,
        ), patch(
            "horsies.core.models.workflow.typing_utils._resolve_source_node_ok_type",
            autospec=True,
            return_value=None,
        ):
            validate_workflow_generic_output_match(mock_cls, mock_spec)

    def test_actual_any_skips_validation(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)
        mock_spec.name = "test_wf"

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=int,
        ), patch(
            "horsies.core.models.workflow.typing_utils._resolve_source_node_ok_type",
            autospec=True,
            return_value=Any,
        ):
            validate_workflow_generic_output_match(mock_cls, mock_spec)

    def test_mismatch_raises_e025(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)
        mock_spec.name = "my_workflow"

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=int,
        ), patch(
            "horsies.core.models.workflow.typing_utils._resolve_source_node_ok_type",
            autospec=True,
            return_value=str,
        ):
            with pytest.raises(WorkflowValidationError) as exc_info:
                validate_workflow_generic_output_match(mock_cls, mock_spec)

        assert exc_info.value.code == ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH

    def test_compatible_types_no_error(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)
        mock_spec.name = "test_wf"

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=int,
        ), patch(
            "horsies.core.models.workflow.typing_utils._resolve_source_node_ok_type",
            autospec=True,
            return_value=int,
        ):
            # Act / Assert — should not raise
            validate_workflow_generic_output_match(mock_cls, mock_spec)

    def test_mismatch_error_contains_type_names(self) -> None:
        # Arrange
        mock_cls = MagicMock()
        mock_spec = MagicMock()
        mock_spec.output = MagicMock(spec=TaskNode)
        mock_spec.name = "bad_workflow"

        with patch(
            "horsies.core.models.workflow.typing_utils._resolve_workflow_def_ok_type",
            autospec=True,
            return_value=int,
        ), patch(
            "horsies.core.models.workflow.typing_utils._resolve_source_node_ok_type",
            autospec=True,
            return_value=str,
        ):
            with pytest.raises(WorkflowValidationError) as exc_info:
                validate_workflow_generic_output_match(mock_cls, mock_spec)

        error = exc_info.value
        assert "int" in error.message
        assert "str" in error.message
        assert "bad_workflow" in error.message


# =============================================================================
# TestFormatTypeName
# =============================================================================


@pytest.mark.unit
class TestFormatTypeName:
    """Tests for _format_type_name()."""

    def test_none_returns_unknown(self) -> None:
        assert _format_type_name(None) == "unknown"

    def test_any_returns_any(self) -> None:
        assert _format_type_name(Any) == "Any"

    def test_int_returns_int(self) -> None:
        assert _format_type_name(int) == "int"

    def test_custom_class_returns_qualname(self) -> None:
        class MyCustomClass:
            pass

        result = _format_type_name(MyCustomClass)
        assert "MyCustomClass" in result

    def test_generic_alias_returns_repr(self) -> None:
        annotation = list[int]
        result = _format_type_name(annotation)
        assert "list" in result
        assert "int" in result


# =============================================================================
# TestIsOkTypeCompatible
# =============================================================================


@pytest.mark.unit
class TestIsOkTypeCompatible:
    """Tests for _is_ok_type_compatible()."""

    def test_source_any_always_true(self) -> None:
        assert _is_ok_type_compatible(Any, int) is True
        assert _is_ok_type_compatible(Any, str) is True

    def test_expected_any_always_true(self) -> None:
        assert _is_ok_type_compatible(int, Any) is True
        assert _is_ok_type_compatible(str, Any) is True

    def test_exact_match(self) -> None:
        assert _is_ok_type_compatible(int, int) is True
        assert _is_ok_type_compatible(str, str) is True

    def test_subclass_compatible(self) -> None:
        assert _is_ok_type_compatible(bool, int) is True

    def test_not_subclass_incompatible(self) -> None:
        assert _is_ok_type_compatible(str, int) is False

    def test_expected_union_match(self) -> None:
        expected = Union[int, str]
        assert _is_ok_type_compatible(int, expected) is True
        assert _is_ok_type_compatible(str, expected) is True

    def test_expected_union_no_match(self) -> None:
        expected = Union[int, str]
        assert _is_ok_type_compatible(float, expected) is False

    def test_source_union_all_match(self) -> None:
        # int and bool are both subclasses of int
        source = Union[int, bool]
        assert _is_ok_type_compatible(source, int) is True

    def test_source_union_partial_match_is_false(self) -> None:
        source = Union[int, str]
        assert _is_ok_type_compatible(source, int) is False

    def test_issubclass_raises_type_error_returns_false(self) -> None:
        # Arrange: types where issubclass raises TypeError
        # Use generic aliases that are types but issubclass fails
        class BadMeta(type):
            def __subclasscheck__(cls, subclass: type) -> bool:
                raise TypeError("bad subclass check")

        class BadType(metaclass=BadMeta):
            pass

        # Act / Assert
        assert _is_ok_type_compatible(BadType, BadType) is True  # exact match first
        # For non-exact match where issubclass would be called:
        class AnotherBadType(metaclass=BadMeta):
            pass

        assert _is_ok_type_compatible(AnotherBadType, BadType) is False

    def test_non_type_non_union_mismatch(self) -> None:
        # Non-type, non-union values that don't match
        assert _is_ok_type_compatible(list[int], list[str]) is False

    def test_pipe_syntax_union_expected(self) -> None:
        expected = int | str
        assert _is_ok_type_compatible(int, expected) is True
        assert _is_ok_type_compatible(float, expected) is False

    def test_pipe_syntax_union_source(self) -> None:
        source = int | bool
        assert _is_ok_type_compatible(source, int) is True

    def test_both_any(self) -> None:
        assert _is_ok_type_compatible(Any, Any) is True
