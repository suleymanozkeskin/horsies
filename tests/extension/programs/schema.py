"""The schema install of the test database, through ``CoreHandle.install_schema``."""

from __future__ import annotations

from tests.extension.programs.report import Report, run_case


def install() -> Report:
    """Install this build's schema in the case's database: the outcome class
    and its facts."""
    import horsies._core as core
    from tests.extension.programs.support import database_url, manifest_json

    handle = core.CoreHandle(
        manifest_json(database_url(), schema='MIGRATE_AND_VALIDATE', echo=False),
        pool_profile='PRODUCER',
    )
    match handle.install_schema():
        case core.SchemaInstalled(version=version):
            return {'outcome': 'SchemaInstalled', 'version': version}
        case core.SchemaInstallNotRun():
            return {'outcome': 'SchemaInstallNotRun'}
        case core.BrokerFailure(code=code, message=message):
            return {'outcome': 'BrokerFailure', 'code': code, 'message': message}


if __name__ == '__main__':
    run_case({'install': install})
