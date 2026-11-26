"""`@app.on_event(EventType)`, `@app.on_handler_child_start`, and the event
handlers of the worker manifest.

One test per refusal code of a registration (`HRS-222` to `HRS-226`, and
`HRS-214` for the hook), the record of an accepted handler, the manifest
field `handlers` and the public module `horsies.telemetry`.
"""

from __future__ import annotations

import dataclasses
import functools
import json
import logging
from collections.abc import Callable, Coroutine
from typing import Any, Final, cast, get_args

import pytest
from pydantic import SecretStr

import horsies.telemetry as telemetry
from horsies._catalog import events
from horsies._catalog import worker_manifest as wire
from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.event_handlers import (
    EventHandler,
    HandlerKind,
    check_event_handler,
    known_event_type,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_frames import MAX_EVENT_HANDLERS, MAX_HANDLER_NAME_CHARS
from horsies.core.worker.core_run import WorkerLaunch, worker_manifest_json
from horsies.core.worker_manifest import HandlerFacts, handlers_manifest
from horsies.telemetry import AttemptEnded, TaskClaimed

pytestmark = [pytest.mark.unit]

_MODULE: Final = __name__


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:pass@localhost/db')
            ),
        ),
    )


def _refusal(raised: pytest.ExceptionInfo[ConfigurationError]) -> ErrorCode | None:
    return raised.value.code


def _on_end(event: AttemptEnded) -> None:
    del event


async def _on_end_async(event: AttemptEnded) -> None:
    del event


class TestAcceptedHandlers:
    def test_a_def_handler_is_registered_with_its_name_and_type(self) -> None:
        app = _make_app()

        returned = app.on_event(AttemptEnded)(_on_end)

        assert returned is _on_end
        (handler,) = app.get_event_handlers()
        assert handler.name == f'{_MODULE}._on_end'
        assert handler.event_type.event_class is AttemptEnded
        assert handler.event_type.name == 'AttemptEnded'
        assert handler.handler_kind is HandlerKind.DEF
        assert handler.function is _on_end

    def test_an_async_def_handler_is_awaited(self) -> None:
        app = _make_app()

        app.on_event(AttemptEnded)(_on_end_async)

        (handler,) = app.get_event_handlers()
        assert handler.handler_kind is HandlerKind.ASYNC_DEF

    def test_a_bound_method_is_named_by_its_class(self) -> None:
        class Sink:
            def on_claim(self, event: TaskClaimed) -> None:
                del event

        app = _make_app()
        sink = Sink()

        app.on_event(TaskClaimed)(sink.on_claim)

        (handler,) = app.get_event_handlers()
        assert handler.name == f'{_MODULE}.{Sink.on_claim.__qualname__}'
        assert handler.handler_kind is HandlerKind.DEF

    @pytest.mark.parametrize('event_class', events.EVENT_TYPES)
    def test_every_event_type_is_known_by_its_manifest_name(
        self, event_class: type[Any]
    ) -> None:
        match known_event_type(event_class):
            case Ok(known):
                assert known.event_class is event_class
                assert known.name == event_class.__name__
            case Err(refused):
                pytest.fail(f'{event_class.__name__} is an event type, got {refused}')

    def test_the_event_type_names_are_the_manifest_names(self) -> None:
        """The class names of the generated events and the names of the
        generated manifest come from the one list of the core."""
        names = {event_class.__name__ for event_class in events.EVENT_TYPES}
        assert names == set(get_args(wire.EventTypeName.__value__))

    @pytest.mark.parametrize(
        'handler_source',
        [
            'def handler(event): pass',
            'def handler(event: "NotDefinedAnywhere") -> None: pass',
            'def handler(*events_in): pass',
            'def handler(event, verbose=False): pass',
            'def handler(event, /, *, verbose=False): pass',
        ],
    )
    def test_a_handler_that_takes_one_event_is_accepted(
        self, handler_source: str
    ) -> None:
        namespace: dict[str, object] = {'__name__': _MODULE}
        exec(handler_source, namespace)
        app = _make_app()

        app.on_event(AttemptEnded)(
            cast('Callable[[AttemptEnded], None]', namespace['handler'])
        )

        assert [handler.name for handler in app.get_event_handlers()] == [
            f'{_MODULE}.handler'
        ]

    def test_the_order_of_registration_is_kept_and_the_snapshot_is_a_copy(self) -> None:
        app = _make_app()

        def first(event: TaskClaimed) -> None:
            del event

        def second(event: AttemptEnded) -> None:
            del event

        app.on_event(TaskClaimed)(first)
        app.on_event(AttemptEnded)(second)

        snapshot = app.get_event_handlers()
        assert [handler.function for handler in snapshot] == [first, second]
        assert isinstance(snapshot, tuple)


class TestUnknownEventType:
    @pytest.mark.parametrize(
        'value',
        [int, 'AttemptEnded', events.AnyEvent, events.AttemptEndReportedByChild, None],
        ids=['class', 'name', 'union', 'variant', 'none'],
    )
    def test_a_value_that_is_not_an_event_type_is_refused_at_once(
        self, value: object
    ) -> None:
        app = _make_app()

        with pytest.raises(ConfigurationError) as raised:
            app.on_event(value)  # pyright: ignore[reportArgumentType]

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_UNKNOWN_EVENT_TYPE
        assert raised.value.code is not None
        assert raised.value.code.value == 'HRS-222'
        assert app.get_event_handlers() == ()

    def test_an_instance_of_an_event_type_is_refused(self) -> None:
        match known_event_type(object.__new__(AttemptEnded)):
            case Err(refused):
                assert refused.given == 'a value of type AttemptEnded'
            case Ok(known):
                pytest.fail(f'an event instance is not an event type, got {known}')


class _CallableSink:
    def __call__(self, event: AttemptEnded) -> None:
        del event


def _generator_handler(event: AttemptEnded) -> Any:
    yield event


async def _async_generator_handler(event: AttemptEnded) -> Any:
    yield event


def _no_parameter() -> None:
    return None


def _two_events(first: AttemptEnded, second: AttemptEnded) -> None:
    del first, second


def _keyword_only(*, event: AttemptEnded) -> None:
    del event


def _claim_handler(event: TaskClaimed) -> None:
    del event


class TestInvalidSignature:
    @pytest.mark.parametrize(
        ('handler', 'note'),
        [
            (_CallableSink(), 'got a _CallableSink'),
            (functools.partial(_on_end), 'got a partial'),
            (print, 'got a builtin_function_or_method'),
            (_generator_handler, 'does not run its body'),
            (_async_generator_handler, 'does not run its body'),
            (_no_parameter, 'a call with one event does not bind'),
            (_two_events, 'a call with one event does not bind'),
            (_keyword_only, 'a call with one event does not bind'),
            (_claim_handler, "parameter 'event' is annotated TaskClaimed"),
        ],
        ids=[
            'callable_object',
            'partial',
            'builtin',
            'generator',
            'async_generator',
            'no_parameter',
            'two_parameters',
            'keyword_only',
            'other_event_type',
        ],
    )
    def test_a_handler_that_cannot_take_the_events_is_refused(
        self,
        handler: object,
        note: str,
    ) -> None:
        app = _make_app()
        decorator = app.on_event(AttemptEnded)

        with pytest.raises(ConfigurationError) as raised:
            decorator(handler)  # pyright: ignore[reportArgumentType, reportCallIssue]

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_INVALID_SIGNATURE
        assert raised.value.code is not None
        assert raised.value.code.value == 'HRS-223'
        assert any(note in text for text in raised.value.notes), raised.value.notes
        assert app.get_event_handlers() == ()


class TestNames:
    def test_a_name_that_an_earlier_handler_has_is_refused(self) -> None:
        app = _make_app()
        app.on_event(AttemptEnded)(_on_end)

        with pytest.raises(ConfigurationError) as raised:
            app.on_event(AttemptEnded)(_on_end)

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_DUPLICATE_NAME
        assert raised.value.code is not None
        assert raised.value.code.value == 'HRS-224'
        assert f'{_MODULE}._on_end' in raised.value.message
        assert len(app.get_event_handlers()) == 1

    def test_two_lambdas_of_one_module_have_one_name(self) -> None:
        app = _make_app()
        app.on_event(AttemptEnded)(lambda event: None)

        with pytest.raises(ConfigurationError) as raised:
            app.on_event(TaskClaimed)(lambda event: None)

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_DUPLICATE_NAME

    def test_a_name_over_its_bound_is_refused(self) -> None:
        def handler(event: AttemptEnded) -> None:
            del event

        handler.__qualname__ = 'h' * MAX_HANDLER_NAME_CHARS
        app = _make_app()

        with pytest.raises(ConfigurationError) as raised:
            app.on_event(AttemptEnded)(handler)

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_INVALID_NAME
        assert raised.value.code is not None
        assert raised.value.code.value == 'HRS-225'
        assert (
            f'{len(_MODULE) + 1 + MAX_HANDLER_NAME_CHARS} characters'
            in raised.value.message
        )

    def test_a_name_at_its_bound_is_accepted(self) -> None:
        def handler(event: AttemptEnded) -> None:
            del event

        handler.__qualname__ = 'h' * (MAX_HANDLER_NAME_CHARS - len(_MODULE) - 1)
        app = _make_app()

        app.on_event(AttemptEnded)(handler)

        (registered,) = app.get_event_handlers()
        assert len(registered.name) == MAX_HANDLER_NAME_CHARS

    def test_a_function_with_no_module_name_is_refused(self) -> None:
        def handler(event: AttemptEnded) -> None:
            del event

        handler.__module__ = None  # pyright: ignore[reportFunctionMemberAccess]
        app = _make_app()

        with pytest.raises(ConfigurationError) as raised:
            app.on_event(AttemptEnded)(handler)

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_INVALID_NAME


def _numbered_handler(number: int) -> Callable[[AttemptEnded], None]:
    def handler(event: AttemptEnded) -> None:
        del event

    handler.__qualname__ = f'handler_{number}'
    return handler


class TestLimit:
    def test_the_handler_past_the_most_is_refused(self) -> None:
        app = _make_app()
        for number in range(MAX_EVENT_HANDLERS):
            app.on_event(AttemptEnded)(_numbered_handler(number))

        with pytest.raises(ConfigurationError) as raised:
            app.on_event(AttemptEnded)(_numbered_handler(MAX_EVENT_HANDLERS))

        assert _refusal(raised) is ErrorCode.EVENT_HANDLER_LIMIT
        assert raised.value.code is not None
        assert raised.value.code.value == 'HRS-226'
        assert len(app.get_event_handlers()) == MAX_EVENT_HANDLERS

    def test_the_checks_name_the_signature_before_the_bound(self) -> None:
        """`check_event_handler` refuses a handler that is wrong by itself
        first, so the fix that it names is the one of that handler."""
        match known_event_type(AttemptEnded):
            case Ok(known):
                pass
            case Err(refused):
                pytest.fail(f'AttemptEnded is an event type, got {refused}')
        full: tuple[EventHandler, ...] = tuple(
            EventHandler(
                name=f'm.h{number}',
                event_type=known,
                handler_kind=HandlerKind.DEF,
                function=_on_end,
            )
            for number in range(MAX_EVENT_HANDLERS)
        )
        match check_event_handler(known, _no_parameter, full):
            case Err(refused):
                assert type(refused).__name__ == 'OneEventDoesNotBind'
            case Ok(handler):
                pytest.fail(f'a handler with no parameter is refused, got {handler}')


class TestHandlerChildStartHook:
    def test_a_hook_is_registered_once_in_order(self) -> None:
        app = _make_app()

        def first() -> None:
            return None

        def second() -> None:
            return None

        app.on_handler_child_start(first)
        app.on_handler_child_start(second)
        app.on_handler_child_start(first)

        assert app.get_handler_child_start_hooks() == [first, second]
        assert app.get_child_process_start_hooks() == []

    def test_an_async_hook_is_refused(self) -> None:
        app = _make_app()

        async def hook() -> None:
            return None

        with pytest.raises(ConfigurationError) as raised:
            app.on_handler_child_start(hook)  # pyright: ignore[reportArgumentType]

        assert _refusal(raised) is ErrorCode.CONFIG_INVALID_CHILD_HOOK
        assert 'on_handler_child_start' in raised.value.message
        assert app.get_handler_child_start_hooks() == []

    def test_a_value_that_is_not_callable_is_refused(self) -> None:
        app = _make_app()

        with pytest.raises(ConfigurationError) as raised:
            app.on_handler_child_start('reset')  # pyright: ignore[reportArgumentType]

        assert _refusal(raised) is ErrorCode.CONFIG_INVALID_CHILD_HOOK


class TestWorkerManifest:
    def test_no_handler_is_no_handlers(self) -> None:
        assert handlers_manifest(()) == wire.EventHandlersNoHandlers()

    def test_the_handlers_are_in_their_order(self) -> None:
        manifest = handlers_manifest(
            (
                HandlerFacts(name='app.hooks.on_end', event_type='AttemptEnded'),
                HandlerFacts(name='app.hooks.on_claim', event_type='TaskClaimed'),
            ),
        )

        assert manifest == wire.EventHandlersHandlers(
            handlers=(
                wire.HandlerManifest(
                    name='app.hooks.on_end', event_type='AttemptEnded'
                ),
                wire.HandlerManifest(
                    name='app.hooks.on_claim', event_type='TaskClaimed'
                ),
            ),
        )

    def test_a_repeated_name_is_a_defect(self) -> None:
        facts = HandlerFacts(name='app.hooks.on_end', event_type='AttemptEnded')
        with pytest.raises(AssertionError):
            handlers_manifest((facts, facts))

    def test_the_worker_manifest_names_the_handlers_of_the_app(self) -> None:
        app = _make_app()
        app.on_event(AttemptEnded)(_on_end)
        app.on_event(AttemptEnded)(_on_end_async)
        launch = WorkerLaunch(
            app_locator='app.main:app',
            imports=(),
            sys_path_roots=(),
            profile='default',
            loglevel='INFO',
            metrics_endpoint=wire.MetricsEndpointNoMetricsEndpoint(),
        )

        match worker_manifest_json(app, launch, logging.getLogger('test')):
            case Ok(text):
                document = cast('dict[str, Any]', json.loads(text))
            case Err(refused):
                pytest.fail(f'the manifest builds, got {refused}')

        assert document['handlers'] == {
            'kind': 'HANDLERS',
            'handlers': [
                {'name': f'{_MODULE}._on_end', 'event_type': 'AttemptEnded'},
                {'name': f'{_MODULE}._on_end_async', 'event_type': 'AttemptEnded'},
            ],
        }
        assert document['worker_ended_spans'] == 'NO_SPANS'

    def test_an_app_with_no_handler_sends_no_handlers(self) -> None:
        launch = WorkerLaunch(
            app_locator='app.main:app',
            imports=(),
            sys_path_roots=(),
            profile='default',
            loglevel='INFO',
            metrics_endpoint=wire.MetricsEndpointNoMetricsEndpoint(),
        )

        match worker_manifest_json(_make_app(), launch, logging.getLogger('test')):
            case Ok(text):
                document = cast('dict[str, Any]', json.loads(text))
            case Err(refused):
                pytest.fail(f'the manifest builds, got {refused}')

        assert document['handlers'] == {'kind': 'NO_HANDLERS'}


_DECODER_NAMES: Final = frozenset(
    {
        'NotAnEvent',
        'UnknownEventType',
        'MalformedEvent',
        'EventDecodeError',
        'decode_event',
    },
)
_HANDLER_TYPE_NAMES: Final = frozenset(
    {'AsyncEventHandler', 'EventHandlerDecorator', 'SyncEventHandler'},
)


class TestPublicModule:
    def test_horsies_telemetry_exports_every_event_name_but_the_decoder(self) -> None:
        assert (
            set(telemetry.__all__)
            == (set(events.__all__) - _DECODER_NAMES) | _HANDLER_TYPE_NAMES
        )
        assert len(telemetry.__all__) == len(set(telemetry.__all__))

    def test_each_event_name_is_the_generated_object(self) -> None:
        for name in set(events.__all__) - _DECODER_NAMES:
            assert getattr(telemetry, name) is getattr(events, name), name


class TestDecoratorTypes:
    def test_the_decorator_gives_the_function_back(self) -> None:
        app = _make_app()

        async def handler(event: AttemptEnded) -> None:
            del event

        decorated: Callable[[AttemptEnded], Coroutine[object, object, None]] = (
            app.on_event(
                AttemptEnded,
            )(handler)
        )

        assert decorated is handler

    def test_a_handler_record_is_frozen(self) -> None:
        app = _make_app()
        app.on_event(AttemptEnded)(_on_end)
        (handler,) = app.get_event_handlers()

        with pytest.raises(dataclasses.FrozenInstanceError):
            handler.name = 'other'  # pyright: ignore[reportAttributeAccessIssue]
