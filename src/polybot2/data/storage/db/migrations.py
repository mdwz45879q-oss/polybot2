"""Schema bootstrap for polybot2 (fresh DB only)."""

from __future__ import annotations

import sqlite3

from polybot2.data.storage.db.schema import SCHEMA_SQL, SCHEMA_VERSION


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in cols)


def run_migrations(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    row = conn.execute("SELECT version FROM _schema_version LIMIT 1").fetchone()
    if row is None:
        conn.execute("INSERT INTO _schema_version(version) VALUES (?)", (int(SCHEMA_VERSION),))
    else:
        version = int(row[0] or 0)
        if version < int(SCHEMA_VERSION):
            # v7 → v8: add minimum_tick_size to pm_markets
            if version <= 7 and not _column_exists(conn, "pm_markets", "minimum_tick_size"):
                conn.execute("ALTER TABLE pm_markets ADD COLUMN minimum_tick_size REAL")
            # v8 → v9: add stream_exists to provider_games
            if version <= 8 and not _column_exists(conn, "provider_games", "stream_exists"):
                conn.execute("ALTER TABLE provider_games ADD COLUMN stream_exists INTEGER")
            conn.execute("UPDATE _schema_version SET version = ?", (int(SCHEMA_VERSION),))
        elif version > int(SCHEMA_VERSION):
            raise RuntimeError(
                f"polybot2 DB schema version={version} is newer than expected={SCHEMA_VERSION}."
            )
    conn.commit()
