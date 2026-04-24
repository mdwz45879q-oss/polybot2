"""Schema bootstrap for polybot2 (fresh DB only)."""

from __future__ import annotations

import sqlite3

from polybot2.data.storage.db.schema import SCHEMA_SQL, SCHEMA_VERSION


def run_migrations(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    row = conn.execute("SELECT version FROM _schema_version LIMIT 1").fetchone()
    if row is None:
        conn.execute("INSERT INTO _schema_version(version) VALUES (?)", (int(SCHEMA_VERSION),))
    else:
        version = int(row[0] or 0)
        if version != int(SCHEMA_VERSION):
            raise RuntimeError(
                f"polybot2 expects fresh DB schema version={SCHEMA_VERSION}; found {version}. "
                "Delete DB and re-bootstrap."
            )
    conn.commit()
