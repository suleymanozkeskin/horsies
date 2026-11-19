"""The public surface of the built ``horsies._core``, read from the module.

The report names each public module member by kind: a function, a class
with its public properties and methods, or an exception with the names of
its bases. A member of no such kind goes under ``others``. Names that start
with ``_`` are left out, as in the stub comparison.
"""

from __future__ import annotations

import types

from tests.extension.programs.report import JsonValue, Report, run_case


def surface() -> Report:
    """The public members of ``horsies._core`` by kind."""
    import horsies._core as core

    functions: list[JsonValue] = []
    classes: dict[str, JsonValue] = {}
    exceptions: dict[str, JsonValue] = {}
    others: list[JsonValue] = []
    for name, value in sorted(vars(core).items()):
        if name.startswith('_'):
            continue
        match value:
            case type() if issubclass(value, BaseException):
                exceptions[name] = [base.__name__ for base in value.__bases__]
            case type():
                classes[name] = _class_members(value)
            case types.BuiltinFunctionType():
                functions.append(name)
            case _:
                others.append(f'{name}: {type(value).__name__}')
    return {
        'functions': functions,
        'classes': classes,
        'exceptions': exceptions,
        'others': others,
    }


def _class_members(cls: type) -> Report:
    """The public properties and methods that ``cls`` defines itself."""
    properties: list[JsonValue] = []
    methods: list[JsonValue] = []
    others: list[JsonValue] = []
    for name, member in sorted(vars(cls).items()):
        if name.startswith('_'):
            continue
        match member:
            case property() | types.GetSetDescriptorType() | types.MemberDescriptorType():
                properties.append(name)
            case (
                types.MethodDescriptorType()
                | types.BuiltinFunctionType()
                | types.ClassMethodDescriptorType()
                | staticmethod()
                | classmethod()
            ):
                methods.append(name)
            case _:
                others.append(f'{name}: {type(member).__name__}')
    return {'properties': properties, 'methods': methods, 'others': others}


if __name__ == '__main__':
    run_case({'surface': surface})
