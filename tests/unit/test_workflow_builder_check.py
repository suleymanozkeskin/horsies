"""Unit tests for workflow builder check contract (Workstream A)."""

from __future__ import annotations

import sys
import types
from typing import Any
from unittest import mock

import pytest

from horsies.core.app import Horsies, _BUILDER_ATTR, _NO_CHECK_ATTR
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    WorkflowValidationError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.workflow import WorkflowSpec

pytestmark = pytest.mark.unit


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://user:pass@localhost/db',
            ),
        ),
    )


# =============================================================================
# Registration Tests
# =============================================================================


class TestWorkflowBuilderRegistration:
    """Tests for @app.workflow_builder() decorator registration."""

    def test_decorator_registers_builder(self) -> None:
        """Decorated function is stored in the builder registry."""
        app = _make_app()

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        builders = app.get_workflow_builders()
        assert len(builders) == 1
        assert builders[0].fn is my_builder

    def test_decorator_stamps_builder_attr(self) -> None:
        """Decorated function carries the sentinel attribute."""
        app = _make_app()

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        assert getattr(my_builder, _BUILDER_ATTR, False) is True

    def test_decorator_preserves_function(self) -> None:
        """Decorator returns the original function unchanged."""
        app = _make_app()

        def my_builder() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        decorated = app.workflow_builder()(my_builder)
        assert decorated is my_builder

    def test_decorator_stores_cases(self) -> None:
        """Cases passed to decorator are stored in metadata."""
        app = _make_app()

        @app.workflow_builder(cases=[{'x': 1}, {'x': 2}])
        def my_builder(x: int) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        builders = app.get_workflow_builders()
        assert builders[0].cases == [{'x': 1}, {'x': 2}]

    def test_decorator_stores_source_location(self) -> None:
        """Source location is captured from the decorated function."""
        app = _make_app()

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        builders = app.get_workflow_builders()
        loc = builders[0].location
        assert loc is not None
        assert loc.file.endswith('test_workflow_builder_check.py')

    def test_multiple_builders_registered(self) -> None:
        """Multiple decorated builders accumulate in the registry."""
        app = _make_app()

        @app.workflow_builder()
        def builder_a() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        @app.workflow_builder()
        def builder_b() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        assert len(app.get_workflow_builders()) == 2


# =============================================================================
# _check_workflow_builders Tests
# =============================================================================


class TestCheckWorkflowBuilders:
    """Tests for _check_workflow_builders phase."""

    def test_zero_arg_builder_auto_invoked(self) -> None:
        """Builder with no params is auto-invoked once."""
        app = _make_app()
        call_count = 0

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            nonlocal call_count
            call_count += 1
            # Return a mock spec to avoid real validation
            return mock.MagicMock(spec=WorkflowSpec)

        errors = app._check_workflow_builders()
        assert errors == []
        assert call_count == 1

    def test_all_defaults_builder_auto_invoked(self) -> None:
        """Builder where all params have defaults is auto-invoked once."""
        app = _make_app()
        call_count = 0

        @app.workflow_builder()
        def my_builder(x: int = 5) -> WorkflowSpec[Any]:
            nonlocal call_count
            call_count += 1
            return mock.MagicMock(spec=WorkflowSpec)

        errors = app._check_workflow_builders()
        assert errors == []
        assert call_count == 1

    def test_zero_arg_builder_exception_wrapped_as_e029(self) -> None:
        """Unexpected exception from builder is wrapped with E029."""
        app = _make_app()

        @app.workflow_builder()
        def bad_builder() -> WorkflowSpec[Any]:
            raise RuntimeError('kaboom')

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION
        assert 'kaboom' in str(errors[0])

    def test_builder_returning_non_workflow_spec_wrapped_as_e029(self) -> None:
        """Builder returning non-WorkflowSpec is reported as E029."""
        app = _make_app()

        @app.workflow_builder()
        def bad_builder() -> WorkflowSpec[Any]:
            return None  # type: ignore[return-value]

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION
        assert 'returned non-WorkflowSpec' in str(errors[0])

    def test_builder_validation_error_collected(self) -> None:
        """WorkflowValidationError from builder is collected directly."""
        app = _make_app()

        @app.workflow_builder()
        def bad_workflow_builder() -> WorkflowSpec[Any]:
            raise WorkflowValidationError(
                message='bad graph',
                code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
            )

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CYCLE_DETECTED

    def test_parameterized_builder_without_cases_e027(self) -> None:
        """Parameterized builder missing cases= fails with E027."""
        app = _make_app()

        @app.workflow_builder()
        def my_builder(urls: list[str]) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_CASES_REQUIRED
        assert 'urls' in str(errors[0])

    def test_parameterized_builder_with_valid_cases(self) -> None:
        """Each case is invoked when cases are valid."""
        app = _make_app()
        invocations: list[dict[str, Any]] = []

        @app.workflow_builder(cases=[{'x': 1}, {'x': 2}])
        def my_builder(x: int) -> WorkflowSpec[Any]:
            invocations.append({'x': x})
            return mock.MagicMock(spec=WorkflowSpec)

        errors = app._check_workflow_builders()
        assert errors == []
        assert invocations == [{'x': 1}, {'x': 2}]

    def test_case_with_unknown_keys_e028(self) -> None:
        """Case with keys not in builder signature fails with E028."""
        app = _make_app()

        @app.workflow_builder(cases=[{'x': 1, 'bogus': 2}])
        def my_builder(x: int) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_CASE_INVALID
        assert 'bogus' in str(errors[0])

    def test_case_missing_required_keys_e028(self) -> None:
        """Case missing required keys fails with E028."""
        app = _make_app()

        @app.workflow_builder(cases=[{'x': 1}])
        def my_builder(x: int, y: str) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_CASE_INVALID
        assert 'y' in str(errors[0])

    def test_case_not_a_dict_e028(self) -> None:
        """Non-dict case fails with E028."""
        app = _make_app()

        @app.workflow_builder(cases=["not_a_dict"])  # type: ignore[list-item]
        def my_builder(x: int) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_CASE_INVALID

    def test_case_exception_wrapped_as_e029(self) -> None:
        """Exception during case execution is wrapped with E029."""
        app = _make_app()

        @app.workflow_builder(cases=[{'x': 1}])
        def my_builder(x: int) -> WorkflowSpec[Any]:
            raise ValueError(f'bad value: {x}')

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION
        assert 'bad value: 1' in str(errors[0])

    def test_send_suppression_active_during_builder_execution(self) -> None:
        """Sends are suppressed while builders execute."""
        app = _make_app()
        suppressed_during_call = False

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            nonlocal suppressed_during_call
            suppressed_during_call = app.are_sends_suppressed()
            return mock.MagicMock(spec=WorkflowSpec)

        # Ensure sends are NOT suppressed before
        assert not app.are_sends_suppressed()
        app._check_workflow_builders()
        assert suppressed_during_call is True
        # Ensure sends are NOT suppressed after
        assert not app.are_sends_suppressed()

    def test_multiple_builder_errors_aggregated(self) -> None:
        """Errors from multiple builders are all collected."""
        app = _make_app()

        @app.workflow_builder()
        def builder_a(x: int) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        @app.workflow_builder()
        def builder_b(y: str) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        errors = app._check_workflow_builders()
        assert len(errors) == 2
        codes = {e.code for e in errors}
        assert codes == {ErrorCode.WORKFLOW_CHECK_CASES_REQUIRED}

    def test_builder_horsies_error_collected_directly(self) -> None:
        """HorsiesError from builder is collected as-is (not wrapped)."""
        app = _make_app()

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            raise ConfigurationError(
                message='bad config in builder',
                code=ErrorCode.BROKER_INVALID_URL,
            )

        errors = app._check_workflow_builders()
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.BROKER_INVALID_URL


# =============================================================================
# _check_undecorated_builders Tests
# =============================================================================


class TestCheckUndecoratedBuilders:
    """Tests for undecorated builder detection."""

    def _make_module(
        self,
        name: str,
        functions: dict[str, Any],
    ) -> types.ModuleType:
        """Create a synthetic module with given functions."""
        module = types.ModuleType(name)
        module.__name__ = name
        for fn_name, fn in functions.items():
            fn.__module__ = name
            fn.__qualname__ = fn_name
            setattr(module, fn_name, fn)
        sys.modules[name] = module
        return module

    def _cleanup_module(self, name: str) -> None:
        sys.modules.pop(name, None)

    def test_undecorated_workflow_spec_function_detected(self) -> None:
        """Top-level function returning WorkflowSpec triggers E030."""
        app = _make_app()

        def build_wf() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        mod_name = '_test_undecorated_mod_1'
        self._make_module(mod_name, {'build_wf': build_wf})
        app.discover_tasks([mod_name])
        # Mark as "imported" by adding to sys.modules (already done)
        try:
            errors = app._check_undecorated_builders()
            assert len(errors) == 1
            assert errors[0].code == ErrorCode.WORKFLOW_CHECK_UNDECORATED_BUILDER
            assert 'build_wf' in str(errors[0])
        finally:
            self._cleanup_module(mod_name)

    def test_decorated_builder_not_flagged(self) -> None:
        """Function with @app.workflow_builder is NOT flagged."""
        app = _make_app()

        @app.workflow_builder()
        def build_wf() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        mod_name = '_test_decorated_mod_1'
        self._make_module(mod_name, {'build_wf': build_wf})
        app.discover_tasks([mod_name])
        try:
            errors = app._check_undecorated_builders()
            assert errors == []
        finally:
            self._cleanup_module(mod_name)

    def test_no_check_opt_out_suppresses_detection(self) -> None:
        """__horsies_no_check__ = True suppresses E030."""
        app = _make_app()

        def build_wf() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        setattr(build_wf, _NO_CHECK_ATTR, True)

        mod_name = '_test_optout_mod_1'
        self._make_module(mod_name, {'build_wf': build_wf})
        app.discover_tasks([mod_name])
        try:
            errors = app._check_undecorated_builders()
            assert errors == []
        finally:
            self._cleanup_module(mod_name)

    def test_non_workflow_spec_return_not_flagged(self) -> None:
        """Function returning int is not flagged."""
        app = _make_app()

        def regular_fn() -> int:
            return 42

        mod_name = '_test_non_wf_mod_1'
        self._make_module(mod_name, {'regular_fn': regular_fn})
        app.discover_tasks([mod_name])
        try:
            errors = app._check_undecorated_builders()
            assert errors == []
        finally:
            self._cleanup_module(mod_name)

    def test_no_return_annotation_not_flagged(self) -> None:
        """Function without return annotation is not flagged."""
        app = _make_app()

        def no_annotation():  # type: ignore[no-untyped-def]
            pass

        mod_name = '_test_no_annot_mod_1'
        self._make_module(mod_name, {'no_annotation': no_annotation})
        app.discover_tasks([mod_name])
        try:
            errors = app._check_undecorated_builders()
            assert errors == []
        finally:
            self._cleanup_module(mod_name)

    def test_imported_function_not_flagged(self) -> None:
        """Function defined in another module (re-exported) is not flagged."""
        app = _make_app()

        def foreign_fn() -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        # Set __module__ to a different module
        foreign_fn.__module__ = 'some_other_module'

        mod_name = '_test_foreign_mod_1'
        self._make_module(mod_name, {'foreign_fn': foreign_fn})
        # Manually fix __module__ back since _make_module overwrites it
        foreign_fn.__module__ = 'some_other_module'
        app.discover_tasks([mod_name])
        try:
            errors = app._check_undecorated_builders()
            assert errors == []
        finally:
            self._cleanup_module(mod_name)


# =============================================================================
# Integration: check() wiring tests
# =============================================================================


class TestCheckIntegration:
    """Tests that builder check phases are wired into Horsies.check()."""

    def test_builder_errors_surface_in_check(self) -> None:
        """Builder check errors appear in check() output."""
        app = _make_app()

        @app.workflow_builder()
        def missing_cases_builder(x: int) -> WorkflowSpec[Any]:
            raise NotImplementedError  # pragma: no cover

        # No modules to import, so import phase passes
        errors = app.check(live=False)
        assert len(errors) == 1
        assert errors[0].code == ErrorCode.WORKFLOW_CHECK_CASES_REQUIRED

    def test_check_passes_with_valid_builder(self) -> None:
        """check() passes when builder is valid and produces no errors."""
        app = _make_app()

        @app.workflow_builder()
        def good_builder() -> WorkflowSpec[Any]:
            return mock.MagicMock(spec=WorkflowSpec)

        errors = app.check(live=False)
        assert errors == []

    def test_import_errors_block_builder_phase(self) -> None:
        """If imports fail, builder phase is never reached."""
        app = _make_app()
        call_count = 0

        @app.workflow_builder()
        def my_builder() -> WorkflowSpec[Any]:
            nonlocal call_count
            call_count += 1
            return mock.MagicMock(spec=WorkflowSpec)

        # Register a non-existent module
        app.discover_tasks(['/nonexistent/module.py'])
        errors = app.check(live=False)
        # Import error should be returned
        assert len(errors) >= 1
        # Builder should NOT have been called
        assert call_count == 0
