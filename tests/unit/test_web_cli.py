# pyright: reportPrivateUsage=false
# These tests deliberately exercise module-private rules and seams.
"""Unit tests for the ``horsies web`` command's fail-closed argument rules.

The command decides two things before anything is served: which app to read,
and who is allowed to read it. Both refuse ambiguous or exposed invocations
rather than guessing, so both are tested here. Serving itself is not exercised
— uvicorn ships with the web extra and is not a test dependency.
"""

from __future__ import annotations

import argparse
import importlib
import sys
import types
from typing import Any

import pytest
from pydantic import SecretStr

from horsies import web as horsies_web
from horsies.core import cli
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.web.auth import AllowAll, TrustedHeader, ViewOnly

pytestmark = [pytest.mark.unit]

DB_URL = 'postgresql+psycopg://user:pw@localhost:5432/horsies'


def web_args(**overrides: Any) -> argparse.Namespace:
    """A namespace shaped like the web subparser's output."""
    values: dict[str, Any] = {
        'app_path': None,
        'database_url': None,
        'session_database_url': None,
        'pgbouncer_transaction_mode': False,
        'host': '127.0.0.1',
        'port': 8600,
        'auth': 'none',
        'trusted_header': 'X-Forwarded-User',
        'enable_actions': False,
        'loglevel': 'INFO',
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class TestLoopbackDetection:
    """Anything not provably loopback is treated as exposed."""

    @pytest.mark.parametrize('host', ['127.0.0.1', '127.0.0.2', '::1', 'localhost'])
    def test_loopback_hosts(self, host: str) -> None:
        assert cli._is_loopback(host) is True

    @pytest.mark.parametrize(
        'host', ['0.0.0.0', '192.168.1.5', '10.0.0.1', 'example.com', '']
    )
    def test_exposed_or_unparseable_hosts(self, host: str) -> None:
        assert cli._is_loopback(host) is False


class TestAppSourceIsExclusive:
    """Exactly one app source, never two and never zero."""

    def test_both_sources_is_a_usage_error(self) -> None:
        with pytest.raises(SystemExit) as raised:
            cli.resolve_web_app(
                web_args(app_path='myapp.tasks:app', database_url=DB_URL)
            )

        assert raised.value.code == 2

    def test_no_source_is_a_usage_error(self) -> None:
        with pytest.raises(SystemExit) as raised:
            cli.resolve_web_app(web_args())

        assert raised.value.code == 2

    def test_database_url_builds_a_registryless_app(self) -> None:
        app = cli.resolve_web_app(web_args(database_url=DB_URL))

        assert isinstance(app, Horsies)
        config = app.get_broker().config
        assert config.database_url.get_secret_value() == DB_URL
        assert config.session_database_url is None
        assert config.pgbouncer_transaction_mode is False

    def test_pooling_flags_reach_the_broker_config(self) -> None:
        session_url = 'postgresql+psycopg://user:pw@localhost:5432/direct'

        app = cli.resolve_web_app(
            web_args(
                database_url=DB_URL,
                session_database_url=session_url,
                pgbouncer_transaction_mode=True,
            )
        )

        config = app.get_broker().config
        assert config.session_database_url is not None
        assert config.session_database_url.get_secret_value() == session_url
        assert config.pgbouncer_transaction_mode is True
        assert config.effective_session_database_url == session_url


class TestAuthPolicySelection:
    """A port reachable from the network requires a proxy-supplied identity."""

    def test_loopback_without_actions_is_view_only(self) -> None:
        policy = cli.resolve_web_auth_policy(web_args())

        assert isinstance(policy, ViewOnly)

    def test_loopback_with_actions_allows_everything(self) -> None:
        policy = cli.resolve_web_auth_policy(web_args(enable_actions=True))

        assert isinstance(policy, AllowAll)

    @pytest.mark.parametrize('host', ['0.0.0.0', '192.168.1.5'])
    def test_exposed_host_refuses_auth_none(self, host: str) -> None:
        with pytest.raises(SystemExit) as raised:
            cli.resolve_web_auth_policy(web_args(host=host))

        assert raised.value.code == 2

    def test_exposed_host_is_allowed_with_a_trusted_header(self) -> None:
        policy = cli.resolve_web_auth_policy(
            web_args(host='0.0.0.0', auth='trusted-header', enable_actions=True)
        )

        assert isinstance(policy, TrustedHeader)
        assert policy.header_name == 'X-Forwarded-User'
        assert policy.allow_actions is True

    def test_trusted_header_defaults_to_view_only_actions(self) -> None:
        policy = cli.resolve_web_auth_policy(
            web_args(host='0.0.0.0', auth='trusted-header')
        )

        assert isinstance(policy, TrustedHeader)
        assert policy.allow_actions is False

    def test_blank_header_name_is_a_usage_error(self) -> None:
        with pytest.raises(SystemExit) as raised:
            cli.resolve_web_auth_policy(
                web_args(auth='trusted-header', trusted_header='   ')
            )

        assert raised.value.code == 2

    def test_trusted_header_mode_warns_about_proxy_stripping(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The spoofing risk is the deployment's to close, so it is stated."""
        cli.resolve_web_auth_policy(web_args(host='0.0.0.0', auth='trusted-header'))

        stderr = capsys.readouterr().err
        assert 'MUST strip or overwrite' in stderr
        assert 'X-Forwarded-User' in stderr


class TestAppPathBootLoadsTheRegistry:
    """An app path is served with its tasks registered, or not served at all.

    Actions that re-enqueue work need the registry: resuming a run encodes each
    ready node's upstream results into fresh task rows, and that encoding reads
    the source task's registered OkT. Serving an app path without importing its
    task modules leaves those actions to fail at use.
    """

    @pytest.fixture
    def sentinel_app(self, monkeypatch: pytest.MonkeyPatch) -> Horsies:
        """A freshly imported sentinel app, so its registry starts empty."""
        for name in list(sys.modules):
            if name.startswith('tests.unit.web_cli_sentinel'):
                monkeypatch.delitem(sys.modules, name, raising=False)
        package = importlib.import_module('tests.unit.web_cli_sentinel')
        app: Horsies = package.app
        return app

    @pytest.fixture
    def served(self, monkeypatch: pytest.MonkeyPatch) -> list[Horsies]:
        """Capture the apps handed to the server, without starting one."""
        captured: list[Horsies] = []

        def fake_create(app: Horsies, **_kwargs: Any) -> object:
            captured.append(app)
            return object()

        def fake_run(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(horsies_web, 'create_monitoring_app', fake_create)
        fake_uvicorn = types.ModuleType('uvicorn')
        setattr(fake_uvicorn, 'run', fake_run)
        monkeypatch.setitem(sys.modules, 'uvicorn', fake_uvicorn)
        return captured

    def test_app_path_boot_imports_the_discovered_task_modules(
        self,
        monkeypatch: pytest.MonkeyPatch,
        sentinel_app: Horsies,
        served: list[Horsies],
    ) -> None:
        assert 'web_cli_sentinel_task' not in sentinel_app.list_tasks()

        def fake_discover(_locator: str) -> tuple[Horsies, str, str, None]:
            return (sentinel_app, 'app', 'sentinel', None)

        monkeypatch.setattr(cli, 'discover_app', fake_discover)

        cli.web_command(web_args(app_path='tests.unit.web_cli_sentinel:app'))

        assert 'web_cli_sentinel_task' in sentinel_app.list_tasks()
        assert served == [sentinel_app]

    def test_a_failing_check_refuses_to_serve(
        self,
        monkeypatch: pytest.MonkeyPatch,
        served: list[Horsies],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        broken = Horsies(
            config=AppConfig(
                broker=PostgresConfig(
                    database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
                ),
            ),
        )
        broken.discover_tasks(['tests/unit/no_such_task_module.py'])

        def fake_discover(_locator: str) -> tuple[Horsies, str, str, None]:
            return (broken, 'app', 'broken', None)

        monkeypatch.setattr(cli, 'discover_app', fake_discover)

        with pytest.raises(SystemExit) as raised:
            cli.web_command(web_args(app_path='broken:app'))

        assert raised.value.code == 1
        assert 'no_such_task_module.py' in capsys.readouterr().err
        assert served == []

    def test_database_url_boot_stays_registryless(
        self, served: list[Horsies]
    ) -> None:
        # No app to import, so no registry and no startup validation — the
        # read-only form is served as-is.
        cli.web_command(web_args(database_url=DB_URL))

        assert len(served) == 1
        assert served[0].list_tasks() == []


class TestParserDefaults:
    """The documented defaults come from the parser, not from a caller."""

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def capture(args: argparse.Namespace) -> None:
            captured.update(args.__dict__)

        monkeypatch.setattr(cli, 'web_command', capture)
        monkeypatch.setattr(sys, 'argv', ['horsies', 'web', 'myapp.tasks:app'])

        cli.main()

        assert captured['host'] == '127.0.0.1'
        assert captured['port'] == 8600
        assert captured['auth'] == 'none'
        assert captured['enable_actions'] is False
        assert captured['trusted_header'] == 'X-Forwarded-User'
        assert captured['app_path'] == 'myapp.tasks:app'

    def test_flags_are_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def capture(args: argparse.Namespace) -> None:
            captured.update(args.__dict__)

        monkeypatch.setattr(cli, 'web_command', capture)
        monkeypatch.setattr(
            sys,
            'argv',
            [
                'horsies',
                'web',
                '--database-url',
                DB_URL,
                '--host',
                '0.0.0.0',
                '--port',
                '9000',
                '--auth',
                'trusted-header',
                '--trusted-header',
                'X-User',
                '--enable-actions',
                '--pgbouncer-transaction-mode',
            ],
        )

        cli.main()

        assert captured['database_url'] == DB_URL
        assert captured['host'] == '0.0.0.0'
        assert captured['port'] == 9000
        assert captured['auth'] == 'trusted-header'
        assert captured['trusted_header'] == 'X-User'
        assert captured['enable_actions'] is True
        assert captured['pgbouncer_transaction_mode'] is True
