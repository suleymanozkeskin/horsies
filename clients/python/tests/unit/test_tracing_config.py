"""The tracing setting of `AppConfig`: `OtelTracing` or `NoTracing`.

`NoTracing` is the default, does not look for the OpenTelemetry API and
imports nothing of it. `OtelTracing` with the API not installed is refused
when the config is built (HRS-220, which names the extra `horsies[otel]`).
The cases that need the API absent run in a child interpreter whose import
system refuses every `opentelemetry` module, so they hold whether or not the
API is installed in the test environment.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest
from pydantic import SecretStr, ValidationError

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models import tracing as tracing_module
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tracing import (
    ChildOfSend,
    LinkedToSend,
    NoTracing,
    OtelTracing,
    tracing_label,
)
from horsies.core.telemetry.otel_api import (
    OTEL_API_MODULES,
    OtelApiInstalled,
    OtelApiMissing,
    OtelApiPresence,
)

pytestmark = [pytest.mark.unit]

_BROKER = PostgresConfig(database_url=SecretStr('postgresql://user:pass@127.0.0.1:1/none'))

_BLOCK_OPENTELEMETRY = textwrap.dedent(
    '''
    import sys

    class BlockOpenTelemetry:
        """Refuses every opentelemetry module, as an environment without
        the API does."""

        def find_spec(self, name, path=None, target=None):
            if name == 'opentelemetry' or name.startswith('opentelemetry.'):
                raise ModuleNotFoundError(f'No module named {name!r}', name=name)
            return None

    sys.meta_path.insert(0, BlockOpenTelemetry())
    ''',
)


def _run_without_opentelemetry(body: str) -> subprocess.CompletedProcess[str]:
    """Run `body` in a child interpreter in which no `opentelemetry` module
    can be imported."""
    code = _BLOCK_OPENTELEMETRY + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True,
        text=True,
        check=False,
    )


def _presence_is(
    monkeypatch: pytest.MonkeyPatch,
    presence: OtelApiPresence,
) -> None:
    def fixed_presence() -> OtelApiPresence:
        return presence

    monkeypatch.setattr(tracing_module, 'otel_api_presence', fixed_presence)


class TestSetting:
    def test_an_app_config_traces_nothing_by_default(self) -> None:
        assert AppConfig(database=_BROKER).tracing == NoTracing()

    def test_otel_tracing_with_the_api_installed_is_accepted(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _presence_is(monkeypatch, OtelApiInstalled())
        assert AppConfig(database=_BROKER, tracing=OtelTracing()).tracing == OtelTracing()

    def test_otel_tracing_without_the_api_is_hrs_220_and_names_the_extra(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _presence_is(monkeypatch, OtelApiMissing(module='opentelemetry.trace'))
        with pytest.raises(ConfigurationError) as raised:
            AppConfig(database=_BROKER, tracing=OtelTracing())
        assert raised.value.code is ErrorCode.CONFIG_OTEL_API_MISSING
        assert raised.value.code.value == 'HRS-220'
        assert 'horsies[otel]' in raised.value.message
        assert raised.value.notes == ['the module opentelemetry.trace cannot be imported']
        assert raised.value.help_text is not None
        assert "pip install 'horsies[otel]'" in raised.value.help_text

    def test_no_tracing_does_not_look_for_the_api(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def presence_not_wanted() -> OtelApiPresence:
            raise AssertionError('NoTracing looked for the OpenTelemetry API')

        monkeypatch.setattr(tracing_module, 'otel_api_presence', presence_not_wanted)
        assert AppConfig(database=_BROKER, tracing=NoTracing()).tracing == NoTracing()

    def test_the_setting_is_read_from_its_kind(self) -> None:
        config = AppConfig.model_validate(
            {'database': _BROKER, 'tracing': {'kind': 'no_tracing'}},
        )
        assert config.tracing == NoTracing()

    def test_a_setting_of_another_kind_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            AppConfig.model_validate(
                {'database': _BROKER, 'tracing': {'kind': 'zipkin'}},
            )

    def test_an_attempt_span_is_a_child_of_the_send_span_by_default(self) -> None:
        assert OtelTracing().parentage == ChildOfSend()

    def test_the_parentage_is_read_from_its_kind(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _presence_is(monkeypatch, OtelApiInstalled())
        config = AppConfig.model_validate(
            {
                'database': _BROKER,
                'tracing': {'kind': 'otel_tracing', 'parentage': {'kind': 'linked_to_send'}},
            },
        )
        assert config.tracing == OtelTracing(parentage=LinkedToSend())

    def test_a_parentage_of_another_kind_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            OtelTracing.model_validate({'parentage': {'kind': 'follows_from'}})

    def test_the_label_names_the_setting(self) -> None:
        assert tracing_label(OtelTracing()) == 'OpenTelemetry'
        assert tracing_label(NoTracing()) == 'off'


class TestWithoutTheApi:
    def test_the_presence_names_the_first_api_module(self) -> None:
        result = _run_without_opentelemetry(
            '''
            from horsies.core.telemetry.otel_api import otel_api_presence
            print(otel_api_presence())
            ''',
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == repr(OtelApiMissing(module=OTEL_API_MODULES[0]))

    def test_horsies_imports_and_builds_a_config_with_no_opentelemetry_module(
        self,
    ) -> None:
        result = _run_without_opentelemetry(
            '''
            import sys
            from pydantic import SecretStr
            import horsies
            from horsies import AppConfig, NoTracing, PostgresConfig

            config = AppConfig(
                database=PostgresConfig(
                    database_url=SecretStr('postgresql://u:p@127.0.0.1:1/none'),
                ),
            )
            assert config.tracing == NoTracing()
            loaded = sorted(
                name for name in sys.modules
                if name == 'opentelemetry' or name.startswith('opentelemetry.')
            )
            print(loaded)
            ''',
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == '[]'

    def test_an_app_with_no_tracing_has_no_send_telemetry_and_no_opentelemetry(
        self,
    ) -> None:
        result = _run_without_opentelemetry(
            '''
            import sys
            from pydantic import SecretStr
            from horsies import AppConfig, Horsies, PostgresConfig
            from horsies.core.telemetry.send_telemetry import NoSendTelemetry

            app = Horsies(
                AppConfig(
                    database=PostgresConfig(
                        database_url=SecretStr('postgresql://u:p@127.0.0.1:1/none'),
                    ),
                ),
            )
            assert isinstance(app.send_telemetry(), NoSendTelemetry)
            loaded = sorted(
                name for name in sys.modules
                if name == 'opentelemetry' or name.startswith('opentelemetry.')
            )
            print(loaded)
            ''',
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == '[]'

    def test_otel_tracing_is_refused_with_hrs_220(self) -> None:
        result = _run_without_opentelemetry(
            '''
            from pydantic import SecretStr
            from horsies import AppConfig, OtelTracing, PostgresConfig
            from horsies.core.errors import ConfigurationError

            try:
                AppConfig(
                    database=PostgresConfig(
                        database_url=SecretStr('postgresql://u:p@127.0.0.1:1/none'),
                    ),
                    tracing=OtelTracing(),
                )
            except ConfigurationError as refused:
                print(refused.code.value)
            ''',
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == 'HRS-220'


class TestWithTheApi:
    def test_the_presence_is_installed(self) -> None:
        pytest.importorskip('opentelemetry.trace')
        from horsies.core.telemetry.otel_api import otel_api_presence

        assert otel_api_presence() == OtelApiInstalled()
