"""Actual SQL compatibility with already-applied additive005; no executor backport."""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from drover.scripts.migrate import MigrationLedgerError, check_runtime_compatibility, load_manifest


class AsyncConnection:
    def __init__(self, connection):
        self.connection = connection

    async def execute(self, statement):
        return self.connection.execute(statement)


@pytest.fixture
def database():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE schema_migrations (logical_id TEXT, relative_path TEXT, sha256 TEXT)"))
        for migration in load_manifest():
            connection.execute(text("INSERT INTO schema_migrations VALUES (:logical_id, :relative_path, :sha256)"),
                               vars(migration))
        for table in ("drover_delegations", "drover_cluster_credentials"):
            connection.execute(text(f"CREATE TABLE {table} (id TEXT, requester_user_id TEXT)"))
            connection.execute(text(f"INSERT INTO {table} VALUES ('original', NULL)"))
        connection.execute(text("CREATE TABLE drover_credential_cleanup (id TEXT)"))
        yield connection
    engine.dispose()


@pytest.mark.asyncio
async def test_existing005_nullable_rows_preserved(database):
    before = {table: database.execute(text(f"SELECT * FROM {table}")).all()
              for table in ("drover_delegations", "drover_cluster_credentials")}
    await check_runtime_compatibility(AsyncConnection(database))
    await check_runtime_compatibility(AsyncConnection(database))
    assert before == {table: database.execute(text(f"SELECT * FROM {table}")).all() for table in before}


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["unknown", "drift", "pending", "delegation", "credential", "cleanup"])
async def test_unsafe_schema_or_executor_data_fails_closed(database, boundary):
    if boundary == "unknown":
        database.execute(text("INSERT INTO schema_migrations VALUES ('006_unknown', 'unknown.sql', 'unknown')"))
    elif boundary == "drift":
        database.execute(text("UPDATE schema_migrations SET sha256='drift' WHERE logical_id='005_admin_executor'"))
    elif boundary == "pending":
        database.execute(text("DELETE FROM schema_migrations WHERE logical_id='005_admin_executor'"))
    elif boundary == "cleanup":
        database.execute(text("INSERT INTO drover_credential_cleanup VALUES ('retained')"))
    else:
        table = "drover_delegations" if boundary == "delegation" else "drover_cluster_credentials"
        database.execute(text(f"UPDATE {table} SET requester_user_id='operator'"))
    with pytest.raises(MigrationLedgerError):
        await check_runtime_compatibility(AsyncConnection(database))


@pytest.mark.asyncio
async def test_api_readiness_rejects_unsupported_executor_rows(database, monkeypatch):
    from drover import main

    class Session:
        async def __aenter__(self):
            return AsyncConnection(database)

        async def __aexit__(self, *_):
            pass

    monkeypatch.setattr(main, "get_session_factory", lambda: Session)
    assert await main._check_migration_ledger() is True
    database.execute(text("UPDATE drover_delegations SET requester_user_id='operator'"))
    assert await main._check_migration_ledger() is False
