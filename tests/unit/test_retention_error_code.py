"""Every retention-config refusal answers with one code.

Retention configuration refused under `CONFIG_INVALID_RECOVERY` while the
fields still lived on `RecoveryConfig`. They moved, and the code follows
them here rather than drifting from its meaning across releases.

The hazard in a change like this is a missed raise site: two codes then
mean one thing, which is worse than one code meaning the wrong thing,
because a handler written against either is right some of the time. The
sweep is therefore by MEANING — what refuses a retention config — and not
by the code a site happens to carry today. That distinction is load
bearing: the queue cross-check in `AppConfig` answered with
`CONFIG_INVALID_QUEUE_MODE`, so a sweep driven by grepping the old code
would have found every site but that one and looked complete.

Completeness is owned by the STRUCTURAL test below, which reads every
error code `models/retention.py` constructs rather than a list someone
must remember to extend. The behavioural cases prove those sites are
reachable and produce the code; they are not the completeness guarantee.

KNOWN LIMIT: the structural test covers `models/retention.py` only.
`AppConfig` raises many unrelated config codes, so its single retention
refusal is pinned behaviourally instead. A retention refusal added to
`models/app.py` needs its own case here — nothing detects that
automatically.
"""

from __future__ import annotations

import inspect
import re
from datetime import timedelta

import pytest

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    MultipleValidationErrors,
)
from horsies.core.models import retention as retention_module
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from pydantic import SecretStr
from horsies.core.models.retention import (
    RetentionClassConfig,
    RetentionConfig,
)

pytestmark = [pytest.mark.unit]

_DB_URL = 'postgresql+psycopg://u:p@localhost:5432/db'


def _raised_codes(build: object) -> list[ErrorCode | None]:
    """Every error code one invalid configuration reports.

    `ConfigurationError.code` is optional, so the None stays in the
    result rather than being filtered: a refusal that carries no code at
    all is a defect these assertions should fail on, not skip.
    """
    with pytest.raises(
        (ConfigurationError, MultipleValidationErrors)
    ) as raised:
        build()  # type: ignore[operator]
    error = raised.value
    if isinstance(error, MultipleValidationErrors):
        return [item.code for item in error.report.errors]
    return [error.code]


class TestRetentionModuleUsesOneCode:
    """Completeness, read off the module rather than a maintained list."""

    def test_every_code_in_the_retention_module_is_the_retention_code(
        self,
    ) -> None:
        source = inspect.getsource(retention_module)
        referenced = re.findall(r'ErrorCode\.([A-Z_]+)', source)
        assert referenced, 'no error codes found; the pattern stopped matching'
        assert set(referenced) == {'CONFIG_INVALID_RETENTION'}, (
            f'models/retention.py refuses with {sorted(set(referenced))}; '
            f'every retention-config refusal must carry '
            f'CONFIG_INVALID_RETENTION'
        )
        # The set assertion alone cannot see a site this pattern MISSES.
        # A refusal reaching the enum another way -- an aliased import, a
        # getattr -- would contribute no match, leave the set unchanged,
        # and pass while carrying the wrong code. Counting against the
        # constructions closes that: every ConfigurationError built here
        # must name its code the way this test can read.
        constructions = len(re.findall(r'ConfigurationError\(', source))
        assert len(referenced) == constructions, (
            f'{constructions} ConfigurationError constructions in '
            f'models/retention.py but {len(referenced)} readable code '
            f'references; a refusal reaches ErrorCode by a route this '
            f'test cannot audit'
        )

    def test_the_code_is_distinct_and_in_the_config_band(self) -> None:
        code = ErrorCode.CONFIG_INVALID_RETENTION
        assert code is not ErrorCode.CONFIG_INVALID_RECOVERY
        assert 200 <= int(code.value[4:]) <= 299
        values = [member.value for member in ErrorCode]
        assert values.count(code.value) == 1, 'error code values are unique'

    def test_the_moved_field_refusal_stays_a_recovery_code(self) -> None:
        """A moved-field error belongs to the object that lost the field.

        `RecoveryConfig` refusing a field that moved to retention is a
        recovery refusal: the adopter configured `RecoveryConfig`, and it
        is the only object positioned to notice. The remedy living in
        retention does not make the refusal a retention one. Pinned so a
        later sweep does not "finish the job" by mistake.
        """
        from horsies.core.models.recovery import (
            MOVED_TO_RETENTION,
            RecoveryConfig,
        )

        # Driven off the module's own list of moved fields, so a field
        # added there is covered without editing this test.
        for name in sorted(MOVED_TO_RETENTION):
            codes = _raised_codes(
                lambda field=name: RecoveryConfig.model_validate({field: 1})
            )
            assert codes == [ErrorCode.CONFIG_INVALID_RECOVERY], name


class TestEveryRefusalCarriesTheCode:
    """Reachability: each refusal really is raised, and with this code."""

    @pytest.mark.parametrize(
        'build',
        [
            pytest.param(
                lambda: RetentionConfig(
                    queue_retention={'not a queue': timedelta(days=1)}
                ),
                id='queue-name-not-an-identifier',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    queue_retention={'emails': timedelta(0)}
                ),
                id='queue-maps-non-positive',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    queue_retention={'emails': timedelta(milliseconds=1500)}
                ),
                id='queue-maps-fractional-seconds',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    queue_retention={'a' * 40: timedelta(days=1)}
                ),
                id='derived-key-too-long',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    retention_classes=(
                        RetentionClassConfig(
                            key='standard_30d', duration=timedelta(days=1)
                        ),
                    )
                ),
                id='declared-key-reserved',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    retention_classes=(
                        RetentionClassConfig(
                            key='q_emails_7d', duration=timedelta(days=1)
                        ),
                    )
                ),
                id='declared-key-uses-queue-prefix',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    retention_classes=(
                        RetentionClassConfig(
                            key='not-an-identifier', duration=timedelta(days=1)
                        ),
                    )
                ),
                id='declared-key-not-an-identifier',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    retention_classes=(
                        RetentionClassConfig(
                            key='a' * 40, duration=timedelta(days=1)
                        ),
                    )
                ),
                id='declared-key-too-long',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    retention_classes=(
                        RetentionClassConfig(
                            key='audit_1y', duration=timedelta(days=365)
                        ),
                        RetentionClassConfig(
                            key='audit_1y', duration=timedelta(days=365)
                        ),
                    )
                ),
                id='declared-key-twice',
            ),
            pytest.param(
                lambda: RetentionConfig(
                    retention_classes=(
                        RetentionClassConfig(
                            key='audit_1y', duration=timedelta(0)
                        ),
                    )
                ),
                id='declared-duration-non-positive',
            ),
        ],
    )
    def test_refusal_carries_the_retention_code(self, build: object) -> None:
        codes = _raised_codes(build)
        assert codes, 'the configuration was accepted; the case is vacuous'
        assert set(codes) == {ErrorCode.CONFIG_INVALID_RETENTION}, (
            'refused with '
            f'{sorted(str(code) for code in codes)}'
        )


    def test_the_queue_cross_check_carries_the_code(self) -> None:
        """The site a sweep by old code would have missed.

        `AppConfig`'s cross-check answered with CONFIG_INVALID_QUEUE_MODE,
        so it appears in no grep for the code the other sites carried.
        The structural test cannot cover it either — `AppConfig` raises
        many unrelated config codes — so this is the only thing pinning
        it, and it asserts the MESSAGE as well as the code.

        The message assertion is not decoration. The first version of
        this case used a queue name long enough that the derived class
        key blew the 18-character limit, so `RetentionConfig` refused
        first and the case passed while never reaching the cross-check
        at all — green against a deliberately reverted code.
        """
        codes = _raised_codes(
            lambda: AppConfig(
                broker=PostgresConfig(database_url=SecretStr(_DB_URL)),
                retention=RetentionConfig(
                    queue_retention={'absent': timedelta(days=1)}
                ),
            )
        )
        assert codes == [ErrorCode.CONFIG_INVALID_RETENTION]

    def test_the_queue_cross_check_is_the_refusal_being_measured(
        self,
    ) -> None:
        """Proves the case above reaches the cross-check, not a neighbour."""
        with pytest.raises(ConfigurationError) as raised:
            AppConfig(
                broker=PostgresConfig(database_url=SecretStr(_DB_URL)),
                retention=RetentionConfig(
                    queue_retention={'absent': timedelta(days=1)}
                ),
            )
        assert 'not a queue this deployment has' in str(raised.value)


class TestPublishedCodeTable:
    def test_the_code_is_documented(self) -> None:
        """The published table is the adopter's index of what a code means."""
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[2]
        table = (
            repo_root
            / 'website'
            / 'src'
            / 'content'
            / 'docs'
            / 'tasks'
            / 'errors.md'
        ).read_text()
        assert ErrorCode.CONFIG_INVALID_RETENTION.value in table
        assert 'CONFIG_INVALID_RETENTION' in table
