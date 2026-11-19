"""The generated ``_core.pyi`` names exactly the functions, classes,
exceptions, methods and properties of the built ``horsies._core``.

The stub is read with ``ast`` in this process; the module is read in a fresh
interpreter (``programs/module_surface.py``). A constructor is not compared:
the stub writes it as ``__init__``, and names that start with ``_`` are left
out on both sides.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from tests.extension.fresh_interpreter import (
    REPOSITORY_ROOT,
    NoDatabase,
    report_at,
    run_case,
    texts_of,
)
from tests.extension.programs.report import Report

STUB_PATH: Final = REPOSITORY_ROOT / 'python' / 'horsies' / '_core.pyi'


@dataclass(frozen=True)
class ClassMembers:
    """The public properties and methods of one class, and any other member."""

    properties: frozenset[str]
    methods: frozenset[str]
    others: frozenset[str]


@dataclass(frozen=True)
class Surface:
    """The public names of the module by kind. An exception maps to the
    names of its bases."""

    functions: frozenset[str]
    classes: Mapping[str, ClassMembers]
    exceptions: Mapping[str, tuple[str, ...]]
    others: frozenset[str]


def stub_surface(text: str) -> Surface:
    """The surface that the stub text declares. Pure.

    A class with no base is a class; a class with bases is an exception (the
    generator writes no other form). An import and the module docstring are
    not members; any other top-level statement goes under ``others``.
    """
    functions: set[str] = set()
    classes: dict[str, ClassMembers] = {}
    exceptions: dict[str, tuple[str, ...]] = {}
    others: set[str] = set()
    for statement in ast.parse(text).body:
        match statement:
            case ast.Import() | ast.ImportFrom():
                pass
            case ast.Expr(value=ast.Constant(value=str())):
                pass
            case ast.FunctionDef(name=name) if not name.startswith('_'):
                functions.add(name)
            case ast.ClassDef(name=name, bases=[]) if not name.startswith('_'):
                classes[name] = _stub_members(statement)
            case ast.ClassDef(name=name, bases=bases) if not name.startswith('_'):
                exceptions[name] = tuple(_base_name(base) for base in bases)
            case _:
                others.add(ast.unparse(statement))
    return Surface(
        functions=frozenset(functions),
        classes=classes,
        exceptions=exceptions,
        others=frozenset(others),
    )


def _stub_members(class_def: ast.ClassDef) -> ClassMembers:
    properties: set[str] = set()
    methods: set[str] = set()
    others: set[str] = set()
    for statement in class_def.body:
        match statement:
            case ast.Expr():
                pass
            case ast.FunctionDef(name=name) if name.startswith('_'):
                pass
            case ast.FunctionDef(name=name, decorator_list=decorators) if _is_property(decorators):
                properties.add(name)
            case ast.FunctionDef(name=name):
                methods.add(name)
            case _:
                others.add(ast.unparse(statement))
    return ClassMembers(
        properties=frozenset(properties),
        methods=frozenset(methods),
        others=frozenset(others),
    )


def _is_property(decorators: list[ast.expr]) -> bool:
    return any(
        isinstance(decorator, ast.Name) and decorator.id == 'property' for decorator in decorators
    )


def _base_name(base: ast.expr) -> str:
    match base:
        case ast.Name(id=identifier):
            return identifier
        case _:
            return ast.unparse(base)


def module_surface(report: Report) -> Surface:
    """The surface of the built module from the report of ``module_surface``."""
    classes = report_at(report, 'classes')
    exceptions = report_at(report, 'exceptions')
    return Surface(
        functions=frozenset(texts_of(report, 'functions')),
        classes={
            name: ClassMembers(
                properties=frozenset(texts_of(report_at(classes, name), 'properties')),
                methods=frozenset(texts_of(report_at(classes, name), 'methods')),
                others=frozenset(texts_of(report_at(classes, name), 'others')),
            )
            for name in classes
        },
        exceptions={name: tuple(texts_of(exceptions, name)) for name in exceptions},
        others=frozenset(texts_of(report, 'others')),
    )


def differences(stub: Surface, module: Surface) -> list[str]:
    """Every difference between the two surfaces, one line each; empty when
    they name the same members. Pure."""
    found = [
        *_set_differences('function', stub.functions, module.functions),
        *_set_differences('class', frozenset(stub.classes), frozenset(module.classes)),
        *_set_differences('exception', frozenset(stub.exceptions), frozenset(module.exceptions)),
        *(f'other member of the stub: {other}' for other in sorted(stub.others)),
        *(f'other member of the module: {other}' for other in sorted(module.others)),
    ]
    for name in sorted(set(stub.classes) & set(module.classes)):
        stub_members = stub.classes[name]
        module_members = module.classes[name]
        found.extend(
            [
                *_set_differences(
                    f'{name} property', stub_members.properties, module_members.properties,
                ),
                *_set_differences(f'{name} method', stub_members.methods, module_members.methods),
                *(f'{name}: other member of the stub: {o}' for o in sorted(stub_members.others)),
                *(
                    f'{name}: other member of the module: {o}'
                    for o in sorted(module_members.others)
                ),
            ],
        )
    for name in sorted(set(stub.exceptions) & set(module.exceptions)):
        if stub.exceptions[name] != module.exceptions[name]:
            found.append(
                f'{name}: bases {stub.exceptions[name]} in the stub, '
                f'{module.exceptions[name]} in the module',
            )
    return found


def _set_differences(kind: str, stub: frozenset[str], module: frozenset[str]) -> list[str]:
    return [
        *(f'{kind} only in the stub: {name}' for name in sorted(stub - module)),
        *(f'{kind} only in the module: {name}' for name in sorted(module - stub)),
    ]


def test_the_stub_names_exactly_the_built_module(tmp_path: Path) -> None:
    report = run_case('module_surface', 'surface', tmp_path, NoDatabase()).report
    stub = stub_surface(STUB_PATH.read_text(encoding='utf-8'))
    module = module_surface(report)

    # The comparison holds only when both sides found members.
    assert stub.classes and stub.functions and stub.exceptions
    assert differences(stub, module) == []
