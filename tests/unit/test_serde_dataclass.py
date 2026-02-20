"""Unit tests for dataclass serialization/rehydration edge cases."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest

from horsies.core.codec.serde import rehydrate_value, to_jsonable
from horsies.core.types.result import is_err
from horsies.core.worker.worker import import_by_path


# ---------------------------------------------------------------------------
# Serialization: to_jsonable produces the expected tagged dict shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataclassSerializationShape:
    """Verify to_jsonable produces correct tagged dicts for dataclass types."""

    def test_simple_dataclass_produces_tagged_dict(self, tmp_path: Path) -> None:
        """Serialized dataclass must have __dataclass__, module, qualname, data keys."""
        pkg = tmp_path / 'serdeshape'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class Simple:
    x: int
    name: str
"""
        )

        _cleanup_modules('serdeshape')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            result = to_jsonable(mod.Simple(x=42, name='hello')).unwrap()

            assert isinstance(result, dict)
            assert set(result.keys()) == {'__dataclass__', 'module', 'qualname', 'data'}
            assert result['__dataclass__'] is True
            assert result['qualname'] == 'Simple'
            assert isinstance(result['data'], dict)
            assert result['data'] == {'x': 42, 'name': 'hello'}
        finally:
            sys.path[:] = original_path

    def test_nested_dataclass_preserves_inner_metadata(self, tmp_path: Path) -> None:
        """Inner dataclass fields also carry __dataclass__ tags."""
        pkg = tmp_path / 'serdenested_shape'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass
from pydantic import BaseModel

class InnerModel(BaseModel):
    label: str

@dataclass
class InnerDC:
    value: int

@dataclass
class OuterDC:
    dc_field: InnerDC
    model_field: InnerModel
"""
        )

        _cleanup_modules('serdenested_shape')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            outer = mod.OuterDC(
                dc_field=mod.InnerDC(value=7),
                model_field=mod.InnerModel(label='ok'),
            )

            result = to_jsonable(outer).unwrap()

            assert isinstance(result, dict)
            assert result['__dataclass__'] is True

            # Inner dataclass field must also be tagged
            inner_dc = result['data']['dc_field']
            assert isinstance(inner_dc, dict)
            assert inner_dc['__dataclass__'] is True

            # Inner pydantic field must carry pydantic tag
            inner_model = result['data']['model_field']
            assert isinstance(inner_model, dict)
            assert inner_model['__pydantic_model__'] is True
        finally:
            sys.path[:] = original_path


# ---------------------------------------------------------------------------
# Serialization: error paths in to_jsonable / _qualified_class_path
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataclassSerializationErrors:
    """Error paths during dataclass serialization."""

    def test_main_module_rejected(self, tmp_path: Path) -> None:
        """Class with __module__ == '__main__' returns Err(SerializationError)."""
        pkg = tmp_path / 'serdemain'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class MainPayload:
    value: int
"""
        )

        _cleanup_modules('serdemain')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            instance = mod.MainPayload(value=1)

            with patch.object(type(instance), '__module__', '__main__'):
                result = to_jsonable(instance)
                assert is_err(result)
                assert '__main__' in str(result.err_value)
        finally:
            sys.path[:] = original_path

    def test_local_class_serialization_rejected(self) -> None:
        """Local classes fail serialization due to non-importable qualname."""

        @dataclass
        class LocalPayload:
            value: int

        result = to_jsonable(LocalPayload(value=1))
        assert is_err(result)
        assert 'local class' in str(result.err_value)

    def test_unsupported_type_raises(self) -> None:
        """Non-serializable type returns Err(SerializationError) with type name."""
        result = to_jsonable(object())
        assert is_err(result)
        assert 'object' in str(result.err_value)


# ---------------------------------------------------------------------------
# Round-trip: serialize then rehydrate
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataclassRoundTrip:
    """Happy-path round-trip tests for dataclass serde."""

    def test_simple_dataclass_roundtrip(self, tmp_path: Path) -> None:
        """Plain fields (int, str) survive a serialize-then-rehydrate cycle."""
        pkg = tmp_path / 'serdert'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class SimpleDC:
    x: int
    name: str
"""
        )

        _cleanup_modules('serdert')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            original = mod.SimpleDC(x=99, name='round-trip')

            json_data = to_jsonable(original).unwrap()
            rehydrated = rehydrate_value(json_data).unwrap()

            assert type(rehydrated).__name__ == 'SimpleDC'
            assert rehydrated.x == 99
            assert rehydrated.name == 'round-trip'
        finally:
            sys.path[:] = original_path

    def test_init_false_fields_roundtrip(self, tmp_path: Path) -> None:
        """init=False fields round-trip via non-init assignment."""
        pkg = tmp_path / 'serdedc'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'datatypes.py'
        mod_file.write_text(
            """
from dataclasses import dataclass, field

@dataclass
class InitFalseDC:
    x: int
    y: int = field(init=False, default=5)
"""
        )

        _cleanup_modules('serdedc', 'datatypes')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            instance = mod.InitFalseDC(3)
            instance.y = 9

            json_data = to_jsonable(instance).unwrap()
            rehydrated = rehydrate_value(json_data).unwrap()

            assert rehydrated.x == 3
            assert rehydrated.y == 9
            assert type(rehydrated).__name__ == 'InitFalseDC'
        finally:
            sys.path[:] = original_path

    def test_nested_dataclass_and_pydantic_roundtrip(self, tmp_path: Path) -> None:
        """Nested dataclass/Pydantic values preserve types on roundtrip."""
        pkg = tmp_path / 'serdenested'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass
from pydantic import BaseModel

class InnerModel(BaseModel):
    name: str

@dataclass
class InnerDC:
    value: int

@dataclass
class OuterDC:
    inner: InnerDC
    model: InnerModel
"""
        )

        _cleanup_modules('serdenested')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            outer = mod.OuterDC(
                inner=mod.InnerDC(7),
                model=mod.InnerModel(name='ok'),
            )

            json_data = to_jsonable(outer).unwrap()
            rehydrated = rehydrate_value(json_data).unwrap()

            assert isinstance(rehydrated, mod.OuterDC)
            assert isinstance(rehydrated.inner, mod.InnerDC)
            assert isinstance(rehydrated.model, mod.InnerModel)
            assert rehydrated.inner.value == 7
            assert rehydrated.model.name == 'ok'
        finally:
            sys.path[:] = original_path

    def test_dataclass_with_collection_fields(self, tmp_path: Path) -> None:
        """List/dict of nested models round-trip correctly."""
        pkg = tmp_path / 'serdecoll'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class Item:
    label: str

@dataclass
class Container:
    items: list
    lookup: dict
"""
        )

        _cleanup_modules('serdecoll')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            container = mod.Container(
                items=[mod.Item(label='a'), mod.Item(label='b')],
                lookup={'key': mod.Item(label='c')},
            )

            json_data = to_jsonable(container).unwrap()
            rehydrated = rehydrate_value(json_data).unwrap()

            assert type(rehydrated).__name__ == 'Container'
            assert len(rehydrated.items) == 2
            assert rehydrated.items[0].label == 'a'
            assert rehydrated.items[1].label == 'b'
            assert rehydrated.lookup['key'].label == 'c'
        finally:
            sys.path[:] = original_path


# ---------------------------------------------------------------------------
# Rehydration: error paths in rehydrate_value for __dataclass__ dicts
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataclassRehydrationErrors:
    """Error paths during dataclass rehydration from raw tagged dicts."""

    def test_rehydrate_import_error(self) -> None:
        """Bad module name returns Err(SerializationError) matching 'Could not import'."""
        raw = {
            '__dataclass__': True,
            'module': 'nonexistent.module.xyz',
            'qualname': 'SomeClass',
            'data': {},
        }
        result = rehydrate_value(raw)
        assert is_err(result)
        assert 'Could not import' in str(result.err_value)

    def test_rehydrate_not_a_dataclass(self) -> None:
        """Module exists but name is not a dataclass returns Err(SerializationError)."""
        raw = {
            '__dataclass__': True,
            'module': 'os.path',
            'qualname': 'join',
            'data': {},
        }
        result = rehydrate_value(raw)
        assert is_err(result)
        assert 'is not a dataclass' in str(result.err_value)

    def test_rehydrate_data_not_dict(self, tmp_path: Path) -> None:
        """data field as a list raises SerializationError matching 'must be a dict'."""
        pkg = tmp_path / 'serdenotdict'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class Payload:
    x: int
"""
        )

        _cleanup_modules('serdenotdict')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            module_name = type(mod.Payload(x=1)).__module__

            raw = {
                '__dataclass__': True,
                'module': module_name,
                'qualname': 'Payload',
                'data': [1, 2, 3],
            }
            result = rehydrate_value(raw)
            assert is_err(result)
            assert 'must be a dict' in str(result.err_value)
        finally:
            sys.path[:] = original_path

    def test_rehydrate_unknown_field_skipped(self, tmp_path: Path) -> None:
        """Extra field in serialized data not on dataclass is silently ignored."""
        pkg = tmp_path / 'serdeextra'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class Minimal:
    x: int
"""
        )

        _cleanup_modules('serdeextra')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            # Serialize normally first
            json_data = to_jsonable(mod.Minimal(x=10)).unwrap()

            # Inject an extra field that doesn't exist on the dataclass
            assert isinstance(json_data, dict)
            json_data['data']['nonexistent_field'] = 'should be ignored'

            rehydrated = rehydrate_value(json_data).unwrap()

            assert rehydrated.x == 10
            assert not hasattr(rehydrated, 'nonexistent_field')
        finally:
            sys.path[:] = original_path

    def test_rehydrate_generic_error(self, tmp_path: Path) -> None:
        """Constructor TypeError wraps into SerializationError."""
        pkg = tmp_path / 'serdeerr'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
from dataclasses import dataclass

@dataclass
class Strict:
    x: int

    def __post_init__(self):
        if not isinstance(self.x, int):
            raise TypeError("x must be int")
"""
        )

        _cleanup_modules('serdeerr')
        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            module_name = type(mod.Strict(x=1)).__module__

            raw = {
                '__dataclass__': True,
                'module': module_name,
                'qualname': 'Strict',
                'data': {'x': 'not_an_int'},
            }
            result = rehydrate_value(raw)
            assert is_err(result)
            assert 'Failed to rehydrate dataclass' in str(result.err_value)
        finally:
            sys.path[:] = original_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cleanup_modules(*prefixes: str) -> None:
    """Remove cached modules matching any of the given prefixes."""
    for name in list(sys.modules.keys()):
        for prefix in prefixes:
            if prefix in name:
                sys.modules.pop(name, None)
                break
