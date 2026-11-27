"""Unit tests of `horsies metrics` (clients/python/horsies/core/fleet_metrics.py).

The command refuses its arguments before it discovers the app and takes its
label names from the app config. A name that cannot be a label value of its
own is not refused: the core counts its samples in the `(unknown)` series.
Serving itself is the core's (`core/src/monitoring/fleet/serve.rs`); no
database is reached here.
"""

from __future__ import annotations

import argparse
import sys
from datetime import time as datetime_time
from typing import Any, Final

import pytest
from pydantic import SecretStr

from horsies._core import FleetMetricsServer, ListenAddress, prepare_fleet_metrics
from horsies.core import cli
from horsies.core.app import Horsies
from horsies.core.fleet_metrics import (
    FleetLabelNames,
    fleet_label_names,
    listen_address,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.schedule import DailySchedule, ScheduleConfig, TaskSchedule

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL: Final = 'postgresql://postgres:none@127.0.0.1:1/none'
"""Nothing listens here; no test below reaches the database."""


def broker_config() -> PostgresConfig:
    return PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))


def daily(name: str) -> TaskSchedule:
    return TaskSchedule(
        name=name,
        task_name='cleanup',
        pattern=DailySchedule(time=datetime_time(3, 0, 0)),
    )


def parsed_metrics_args(
    monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> dict[str, Any]:
    """The namespace that `horsies <argv>` hands `metrics_command`."""
    captured: dict[str, Any] = {}

    def capture(args: argparse.Namespace) -> None:
        captured.update(args.__dict__)

    monkeypatch.setattr(cli, 'metrics_command', capture)
    monkeypatch.setattr(sys, 'argv', ['horsies', *argv])
    cli.main()
    return captured


class TestListenAddress:
    """`--metrics-listen` is HOST:PORT or [IPV6]:PORT, the port 1 to 65535."""

    @pytest.mark.parametrize(
        ('text', 'host', 'port'),
        [
            ('127.0.0.1:9100', '127.0.0.1', 9100),
            ('localhost:1', 'localhost', 1),
            ('[::1]:65535', '::1', 65535),
        ],
    )
    def test_an_address_parses(self, text: str, host: str, port: int) -> None:
        address = listen_address(text)

        assert (address.host, address.port) == (host, port)

    @pytest.mark.parametrize(
        ('text', 'refusal'),
        [
            ('9100', 'is not HOST:PORT'),
            (':9100', 'has no host before the port'),
            ('localhost:0', 'is not a number from 1 to 65535'),
            ('localhost:65536', 'is not a number from 1 to 65535'),
            ('::1:9100', 'write an IPv6 address in brackets'),
        ],
    )
    def test_a_text_that_is_not_an_address_is_refused(
        self, text: str, refusal: str
    ) -> None:
        with pytest.raises(argparse.ArgumentTypeError, match=refusal):
            listen_address(text)


class TestParser:
    """The parser refuses the arguments before the app is discovered."""

    def test_metrics_listen_and_the_app_reach_the_command(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = parsed_metrics_args(
            monkeypatch, ['metrics', 'myapp.tasks:app', '--metrics-listen', '0.0.0.0:9100']
        )

        listen = captured['metrics_listen']
        assert isinstance(listen, ListenAddress)
        assert (listen.host, listen.port) == ('0.0.0.0', 9100)
        assert captured['module_positional'] == 'myapp.tasks:app'
        assert captured['log_level'] == 'INFO'

    def test_no_metrics_listen_is_a_usage_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as raised:
            parsed_metrics_args(monkeypatch, ['metrics', 'myapp.tasks:app'])

        assert raised.value.code == 2
        assert '--metrics-listen' in capsys.readouterr().err

    def test_the_old_listen_flag_is_a_usage_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(SystemExit) as raised:
            parsed_metrics_args(
                monkeypatch, ['metrics', 'myapp.tasks:app', '--listen', '0.0.0.0:9100']
            )

        assert raised.value.code == 2

    def test_an_address_that_does_not_parse_is_a_usage_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as raised:
            parsed_metrics_args(
                monkeypatch, ['metrics', 'myapp.tasks:app', '--metrics-listen', 'localhost:0']
            )

        assert raised.value.code == 2
        assert 'is not a number from 1 to 65535' in capsys.readouterr().err


class TestLabelNames:
    """The label names are the queues and the schedules of the app config."""

    def test_a_default_config_has_the_default_queue_and_no_schedule(self) -> None:
        app = Horsies(AppConfig(database=broker_config()))

        assert fleet_label_names(app) == FleetLabelNames(
            queues=('default',), schedules=()
        )

    def test_a_custom_config_has_its_queues_and_its_schedules(self) -> None:
        app = Horsies(
            AppConfig(
                database=broker_config(),
                queue_mode=QueueMode.CUSTOM,
                custom_queues=[
                    CustomQueueConfig(name='fast'),
                    CustomQueueConfig(name='slow'),
                ],
                schedule=ScheduleConfig(schedules=[daily('nightly')]),
            )
        )

        assert fleet_label_names(app) == FleetLabelNames(
            queues=('fast', 'slow'), schedules=('nightly',)
        )

    def test_a_name_that_cannot_be_a_label_value_is_not_refused(self) -> None:
        app = Horsies(
            AppConfig(
                database=broker_config(),
                schedule=ScheduleConfig(
                    schedules=[daily('(unknown)'), daily('nightly')]
                ),
            )
        )
        names = fleet_label_names(app)

        server = prepare_fleet_metrics(
            app.get_broker()._core_handle,  # pyright: ignore[reportPrivateUsage]
            listen=listen_address('127.0.0.1:9100'),
            queue_names=[*names.queues, ''],
            schedule_names=list(names.schedules),
        )

        assert isinstance(server, FleetMetricsServer)
