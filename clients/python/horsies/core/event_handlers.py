"""The event handlers of an app: `@app.on_event(EventType)` and its checks.

An event handler is a `def` or an `async def` function that takes the events
of one type (`horsies.telemetry`). A worker runs the handlers of its app in
its handler child, never in the worker process
(`horsies.core.worker.handler_child`). This module checks one registration
and builds the record of the handler. It holds no state and does no I/O.

The checks, each with its own code:

- `HRS-222`: the argument of `on_event` is not one of `EVENT_TYPES`.
- `HRS-223`: the handler is not a function or a method, is a generator
  function, does not take a call with one event, or annotates the parameter
  that takes the event with another event type.
- `HRS-224`: an earlier handler of the app has its name.
- `HRS-225`: the function has no module name, or its name is longer than
  `MAX_HANDLER_NAME_CHARS`.
- `HRS-226`: the app has `MAX_EVENT_HANDLERS` handlers already.

The name of a handler is `module.qualname` of its function. It is the
`handler` label value of `horsies.telemetry.handler.errors` and
`horsies.telemetry.handler.dropped`. The worker and its handler child import
the app and its task modules in the same way, so both give a handler the same
name.
"""

from __future__ import annotations

import inspect
import types
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final, TypeIs, assert_never, get_args, overload

from horsies._catalog import events
from horsies._catalog import worker_manifest as wire
from horsies._catalog.events import AnyEvent
from horsies.core.errors import ConfigurationError, HorsiesErrorCode
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_frames import MAX_EVENT_HANDLERS, MAX_HANDLER_NAME_CHARS

type SyncEventHandler[E] = Callable[[E], None]
"""A `def` handler of the events of type `E`."""

type AsyncEventHandler[E] = Callable[[E], Coroutine[object, object, None]]
"""An `async def` handler of the events of type `E`."""

type HandlerFunction = types.FunctionType | types.MethodType
"""What a handler is: a function, or a method bound to its object."""

type HandlerChildStartHook = Callable[[], None]
"""A sync function with no arguments that runs once at the boot of the
handler child (`Horsies.on_handler_child_start`)."""


class HandlerKind(Enum):
    """How the handler child calls a handler."""

    DEF = 'def'
    """A `def` function: called on the thread of the handler."""

    ASYNC_DEF = 'async def'
    """An `async def` function: awaited on the event loop that the thread of
    the handler owns."""


@dataclass(frozen=True, slots=True)
class KnownEventType:
    """An event type of `EVENT_TYPES`, and its name in the worker manifest
    (the tag of its wire form)."""

    event_class: type[AnyEvent]
    name: wire.EventTypeName


@dataclass(frozen=True, slots=True)
class EventHandler:
    """A registered handler: its name (`module.qualname`), the event type
    that it takes, how the handler child calls it, and its function."""

    name: str
    event_type: KnownEventType
    handler_kind: HandlerKind
    function: HandlerFunction


@dataclass(frozen=True, slots=True)
class NotAnEventType:
    """The argument of `on_event` is not an event type. `given` describes
    it."""

    given: str


@dataclass(frozen=True, slots=True)
class NotAFunction:
    """The handler is not a function or a bound method. `given` is the name
    of its type."""

    given: str


@dataclass(frozen=True, slots=True)
class GeneratorHandler:
    """The handler is a generator function or an async generator function:
    its call gives a generator, and its body does not run."""


@dataclass(frozen=True, slots=True)
class SignatureUnreadable:
    """`inspect.signature` refused the handler. `detail` is its text."""

    detail: str


@dataclass(frozen=True, slots=True)
class OneEventDoesNotBind:
    """A call of the handler with one event does not bind to its
    parameters. `detail` is the text of the `TypeError` of the bind."""

    detail: str


@dataclass(frozen=True, slots=True)
class OtherEventAnnotated:
    """The parameter that takes the event is annotated with another event
    type, `annotated`."""

    parameter: str
    annotated: str


@dataclass(frozen=True, slots=True)
class NoModuleName:
    """The function has no module name, so it has no handler name."""


@dataclass(frozen=True, slots=True)
class NameOverBound:
    """The handler name has `chars` characters, more than
    `MAX_HANDLER_NAME_CHARS`."""

    name: str
    chars: int


@dataclass(frozen=True, slots=True)
class NameTaken:
    """An earlier handler of the app has the name `name`."""

    name: str


@dataclass(frozen=True, slots=True)
class HandlerLimitReached:
    """The app has `MAX_EVENT_HANDLERS` handlers already."""


type SignatureRefusal = (
    NotAFunction
    | GeneratorHandler
    | SignatureUnreadable
    | OneEventDoesNotBind
    | OtherEventAnnotated
)
"""Why a handler cannot take the events of its type."""

type NameRefusal = NoModuleName | NameOverBound
"""Why a handler has no valid name."""

type HandlerRefusal = SignatureRefusal | NameRefusal | NameTaken | HandlerLimitReached
"""Why `check_event_handler` refused a handler."""


@dataclass(frozen=True, slots=True)
class AnnotatedWith:
    """The parameter that takes the event is annotated with this event
    type."""

    event_class: type[AnyEvent]


@dataclass(frozen=True, slots=True)
class NoEventAnnotation:
    """The parameter that takes the event has no annotation, an annotation
    that does not evaluate, or an annotation that is not an event type."""


type EventAnnotation = AnnotatedWith | NoEventAnnotation


_EVENT_TYPE_NAMES: Final[frozenset[str]] = frozenset(
    value for value in get_args(wire.EventTypeName.__value__) if isinstance(value, str)
)
"""The event type names of the worker manifest."""

_ONE_EVENT: Final = object()
"""The stand-in event of the bind check of a handler."""

_MAX_GIVEN_CHARS: Final = 200
"""The most characters of the description of a refused value."""


def _is_event_type_name(name: str) -> TypeIs[wire.EventTypeName]:
    return name in _EVENT_TYPE_NAMES


def _described(value: object) -> str:
    """A short description of `value` for a refusal. Pure."""
    match value:
        case type():
            text = f'the class {value.__module__}.{value.__qualname__}'
        case _:
            text = f'a value of type {type(value).__name__}'
    return text[:_MAX_GIVEN_CHARS]


def known_event_type(value: object) -> Result[KnownEventType, NotAnEventType]:
    """The event type of `value` when it is one of `EVENT_TYPES`; `Err`
    otherwise. Pure.

    The class name of an event type is its name in the worker manifest: both
    come from the one list of event types of the core. A class name that the
    manifest does not have is a defect of the generated modules:
    `AssertionError`.
    """
    for event_class in events.EVENT_TYPES:
        if value is event_class:
            name = event_class.__name__
            assert _is_event_type_name(name), (
                f'the event type {name} has no name in the worker manifest'
            )
            return Ok(KnownEventType(event_class=event_class, name=name))
    return Err(NotAnEventType(given=_described(value)))


def _event_annotation(function: HandlerFunction, parameter: str) -> EventAnnotation:
    """The event type that annotates `parameter` of `function`. Evaluates the
    annotations of the function: an annotation that raises when it is
    evaluated counts as no event annotation."""
    try:
        resolved = inspect.signature(function, eval_str=True)
    except Exception:
        # The annotations are user expressions: evaluating them can raise
        # any exception. Such an annotation is not checked.
        return NoEventAnnotation()
    annotation: object = resolved.parameters[parameter].annotation
    for event_class in events.EVENT_TYPES:
        if annotation is event_class:
            return AnnotatedWith(event_class=event_class)
    return NoEventAnnotation()


def _function_and_handler_kind(
    handler: object,
    event_type: KnownEventType,
) -> Result[tuple[HandlerFunction, HandlerKind], SignatureRefusal]:
    """The function of `handler` and how the handler child calls it, when it
    can take the events of `event_type`. Reads the function; calls
    nothing but the evaluation of its annotations."""
    match handler:
        case types.FunctionType() | types.MethodType():
            function: HandlerFunction = handler
        case _:
            return Err(NotAFunction(given=type(handler).__name__))
    if inspect.isgeneratorfunction(function) or inspect.isasyncgenfunction(function):
        return Err(GeneratorHandler())
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError) as exc:
        return Err(SignatureUnreadable(detail=str(exc)))
    try:
        bound = signature.bind(_ONE_EVENT)
    except TypeError as exc:
        return Err(OneEventDoesNotBind(detail=str(exc)))
    (parameter,) = bound.arguments
    match _event_annotation(function, parameter):
        case AnnotatedWith(event_class=annotated) if (
            annotated is not event_type.event_class
        ):
            return Err(
                OtherEventAnnotated(parameter=parameter, annotated=annotated.__name__)
            )
        case AnnotatedWith() | NoEventAnnotation():
            pass
    handler_kind = (
        HandlerKind.ASYNC_DEF
        if inspect.iscoroutinefunction(function)
        else HandlerKind.DEF
    )
    return Ok((function, handler_kind))


def handler_name(function: HandlerFunction) -> Result[str, NameRefusal]:
    """The name of a handler: `module.qualname` of `function`, at most
    `MAX_HANDLER_NAME_CHARS` characters. It holds a dot, so it is never
    empty and never the `(unknown)` label value of the core. Pure."""
    module: object = getattr(function, '__module__', None)
    match module:
        case str():
            name = f'{module}.{function.__qualname__}'
        case _:
            return Err(NoModuleName())
    chars = len(name)
    if chars > MAX_HANDLER_NAME_CHARS:
        return Err(NameOverBound(name=name, chars=chars))
    return Ok(name)


def check_event_handler(
    event_type: KnownEventType,
    handler: object,
    registered: Sequence[EventHandler],
) -> Result[EventHandler, HandlerRefusal]:
    """The record of `handler` as a handler of `event_type`, after the
    handlers of `registered`. Pure apart from the evaluation of the
    annotations of the handler.

    `Err`, the first rule that does not hold, in this order: the signature
    (`SignatureRefusal`), the name (`NameRefusal`), a name of `registered`
    (`NameTaken`), the bound of `MAX_EVENT_HANDLERS` (`HandlerLimitReached`).
    """
    match _function_and_handler_kind(handler, event_type):
        case Ok((function, handler_kind)):
            pass
        case Err(signature_refused):
            return Err(signature_refused)
    match handler_name(function):
        case Ok(name):
            pass
        case Err(name_refused):
            return Err(name_refused)
    if any(earlier.name == name for earlier in registered):
        return Err(NameTaken(name=name))
    if len(registered) >= MAX_EVENT_HANDLERS:
        return Err(HandlerLimitReached())
    return Ok(
        EventHandler(
            name=name,
            event_type=event_type,
            handler_kind=handler_kind,
            function=function,
        ),
    )


def _event_type_names_text() -> str:
    return ', '.join(event_class.__name__ for event_class in events.EVENT_TYPES)


def event_type_refusal_error(refused: NotAnEventType) -> ConfigurationError:
    """The startup error of an argument of `on_event` that is not an event
    type (`HRS-222`). Pure."""
    return ConfigurationError(
        message='on_event takes an event type',
        code=HorsiesErrorCode.EVENT_HANDLER_UNKNOWN_EVENT_TYPE,
        notes=[f'got {refused.given}', f'the event types: {_event_type_names_text()}'],
        help_text=(
            'pass one event class of horsies.telemetry, for example '
            '@app.on_event(AttemptEnded); a handler takes one event type, '
            'never the union AnyEvent'
        ),
    )


def _handler_label(handler: object) -> str:
    qualname: object = getattr(handler, '__qualname__', None)
    match qualname:
        case str():
            return qualname[:_MAX_GIVEN_CHARS]
        case _:
            return _described(handler)


def handler_refusal_error(
    refused: HandlerRefusal,
    *,
    handler: object,
    event_type: KnownEventType,
) -> ConfigurationError:
    """The startup error of a handler that `check_event_handler` refused:
    `HRS-223` for a signature, `HRS-224` for a name in use, `HRS-225` for a
    name, `HRS-226` for the bound. Pure."""
    subject = f"handler '{_handler_label(handler)}' of {event_type.name}"
    match refused:
        case NotAFunction(given=given):
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_SIGNATURE
            message = 'an event handler must be a function or a method'
            notes = [subject, f'got a {given}']
            help_text = 'decorate a def or an async def function'
        case GeneratorHandler():
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_SIGNATURE
            message = 'an event handler cannot be a generator function'
            notes = [subject, 'the call of a generator function does not run its body']
            help_text = 'use a def or an async def function that does not yield'
        case SignatureUnreadable(detail=detail):
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_SIGNATURE
            message = 'the signature of the event handler cannot be read'
            notes = [subject, detail]
            help_text = 'decorate a def or an async def function'
        case OneEventDoesNotBind(detail=detail):
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_SIGNATURE
            message = 'an event handler takes one event'
            notes = [subject, f'a call with one event does not bind: {detail}']
            help_text = (
                f'give the handler one parameter for the event, for example '
                f'def handle(event: {event_type.name}) -> None'
            )
        case OtherEventAnnotated(parameter=parameter, annotated=annotated):
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_SIGNATURE
            message = 'the event parameter of the handler names another event type'
            notes = [subject, f"parameter '{parameter}' is annotated {annotated}"]
            help_text = f'annotate it {event_type.name}, the event type of on_event'
        case NoModuleName():
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_NAME
            message = 'the event handler has no module name'
            notes = [
                subject,
                'the name of a handler is module.qualname of its function',
            ]
            help_text = (
                'define the handler in a module that the app or a task module imports'
            )
        case NameOverBound(name=name, chars=chars):
            code = HorsiesErrorCode.EVENT_HANDLER_INVALID_NAME
            message = (
                f'the name of the event handler has {chars} characters; the most is '
                f'{MAX_HANDLER_NAME_CHARS}'
            )
            notes = [subject, f'name: {name[:_MAX_GIVEN_CHARS]}...']
            help_text = 'define the handler in a module with a shorter path, or with a shorter function name'
        case NameTaken(name=name):
            code = HorsiesErrorCode.EVENT_HANDLER_DUPLICATE_NAME
            message = f"an event handler named '{name}' is registered already"
            notes = [
                subject,
                'the name is the handler label of the handler metrics, so each name is once',
            ]
            help_text = 'register each function once; a second handler needs a function name of its own'
        case HandlerLimitReached():
            code = HorsiesErrorCode.EVENT_HANDLER_LIMIT
            message = f'the app has {MAX_EVENT_HANDLERS} event handlers, the most'
            notes = [subject]
            help_text = 'join handlers of one event type into one function'
        case _ as unreachable:
            assert_never(unreachable)
    return ConfigurationError(
        message=message, code=code, notes=notes, help_text=help_text
    )


class EventHandlerDecorator[E: AnyEvent]:
    """The decorator that `Horsies.on_event(event_type)` returns. It
    registers the function that it decorates as a handler of the events of
    `E`, and gives the function back unchanged.

    A type checker checks the parameter of the handler against `E`: a `def`
    function takes `E` and returns `None`; an `async def` function takes `E`
    and its coroutine returns `None`.
    """

    __slots__ = ('_event_type', '_register')

    def __init__(
        self,
        event_type: KnownEventType,
        register: Callable[[KnownEventType, object], None],
    ) -> None:
        self._event_type = event_type
        self._register = register

    @overload
    def __call__(self, handler: AsyncEventHandler[E]) -> AsyncEventHandler[E]: ...

    @overload
    def __call__(self, handler: SyncEventHandler[E]) -> SyncEventHandler[E]: ...

    def __call__(
        self,
        handler: AsyncEventHandler[E] | SyncEventHandler[E],
    ) -> AsyncEventHandler[E] | SyncEventHandler[E]:
        """Register `handler`.

        Raises:
            ConfigurationError: `HRS-223` to `HRS-226`
                (`handler_refusal_error`). Nothing is registered then.
        """
        self._register(self._event_type, handler)
        return handler
