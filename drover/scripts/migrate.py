"""Checksum-verified Drover schema migration runner."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from drover.config import get_settings

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations"
MANIFEST = MIGRATIONS / "manifest.txt"


class MigrationLedgerError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    logical_id: str
    relative_path: str
    sha256: str


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_manifest(path: Path = MANIFEST) -> list[Migration]:
    if not path.is_file():
        raise MigrationLedgerError(f"migration manifest is missing: {path}")
    migrations: list[Migration] = []
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = [field.strip() for field in line.split("|")]
        if len(fields) != 3:
            raise MigrationLedgerError(f"manifest line {line_number} must contain logical_id|path|sha256")
        logical_id, relative_path, checksum = fields
        if not logical_id or not relative_path or len(checksum) != 64:
            raise MigrationLedgerError(f"manifest line {line_number} is malformed")
        if logical_id in seen_ids or relative_path in seen_paths:
            raise MigrationLedgerError(f"manifest line {line_number} duplicates an immutable identity")
        migration_path = MIGRATIONS / relative_path
        if migration_path.parent != MIGRATIONS or not migration_path.is_file():
            raise MigrationLedgerError(f"manifest line {line_number} references an invalid migration path")
        actual = _sha256(migration_path)
        if actual != checksum:
            raise MigrationLedgerError(f"checksum drift for {logical_id}: manifest={checksum} actual={actual}")
        seen_ids.add(logical_id)
        seen_paths.add(relative_path)
        migrations.append(Migration(logical_id, relative_path, checksum))
    unlisted = {path.name for path in MIGRATIONS.glob("*.sql")} - seen_paths
    if unlisted:
        raise MigrationLedgerError(f"migration files absent from manifest: {', '.join(sorted(unlisted))}")
    if not migrations:
        raise MigrationLedgerError("migration manifest is empty")
    return migrations


def _statements(path: Path) -> list[str]:
    sql = "\n".join(line for line in path.read_text(encoding="utf-8").splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


async def check_runtime_compatibility(connection) -> None:
    """Recognize the full ledger and reject unpublished executor-owned authority."""
    expected = {migration.logical_id: migration for migration in load_manifest()}
    rows = (await connection.execute(text("SELECT logical_id, relative_path, sha256 FROM schema_migrations"))).all()
    for row in rows:
        migration = expected.get(row.logical_id)
        if migration is None:
            raise MigrationLedgerError(f"unknown applied migration: {row.logical_id}")
        if row.relative_path != migration.relative_path or row.sha256 != migration.sha256:
            raise MigrationLedgerError(f"applied migration identity drift: {row.logical_id}")
    if {row.logical_id for row in rows} != set(expected):
        raise MigrationLedgerError("pending Drover migrations")
    for table in ("drover_delegations", "drover_cluster_credentials"):
        count = (await connection.execute(text(
            f"SELECT COUNT(*) FROM {table} WHERE requester_user_id IS NOT NULL"
        ))).scalar_one()
        if count:
            raise MigrationLedgerError("executor-owned authority requires matching executor-capable API and workers")
    count = (await connection.execute(text("SELECT COUNT(*) FROM drover_credential_cleanup"))).scalar_one()
    if count:
        raise MigrationLedgerError("credential cleanup references require matching cleanup-capable API and workers")


async def migrate(database_url: str, *, apply: bool) -> list[str]:
    if not database_url:
        raise MigrationLedgerError("database URL is required")
    migrations = load_manifest()
    engine = create_async_engine(database_url, pool_pre_ping=True)
    pending: list[str] = []
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                  logical_id VARCHAR(100) NOT NULL PRIMARY KEY,
                  relative_path VARCHAR(255) NOT NULL,
                  sha256 CHAR(64) NOT NULL,
                  applied_at DATETIME(6) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            rows = (await connection.execute(text("SELECT logical_id FROM schema_migrations"))).all()
            unknown = {row.logical_id for row in rows} - {migration.logical_id for migration in migrations}
            if unknown:
                raise MigrationLedgerError(f"unknown applied migrations: {', '.join(sorted(unknown))}")
        for migration in migrations:
            async with engine.begin() as connection:
                row = (
                    await connection.execute(
                        text("SELECT relative_path, sha256 FROM schema_migrations WHERE logical_id = :logical_id"),
                        {"logical_id": migration.logical_id},
                    )
                ).first()
                if row is not None:
                    if row.relative_path != migration.relative_path or row.sha256 != migration.sha256:
                        raise MigrationLedgerError(f"applied migration identity drift: {migration.logical_id}")
                    continue
                pending.append(migration.logical_id)
                if not apply:
                    continue
                for statement in _statements(MIGRATIONS / migration.relative_path):
                    await connection.exec_driver_sql(statement)
                await connection.execute(
                    text(
                        "INSERT INTO schema_migrations (logical_id, relative_path, sha256, applied_at) "
                        "VALUES (:logical_id, :relative_path, :sha256, NOW(6))"
                    ),
                    {
                        "logical_id": migration.logical_id,
                        "relative_path": migration.relative_path,
                        "sha256": migration.sha256,
                    },
                )
        if not pending or apply:
            async with engine.connect() as connection:
                await check_runtime_compatibility(connection)
        return pending
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    database_url = args.database_url or get_settings().database_url
    pending = asyncio.run(migrate(database_url, apply=args.apply))
    if pending and not args.apply:
        raise SystemExit(f"pending Drover migrations: {', '.join(pending)}")


if __name__ == "__main__":
    main()
