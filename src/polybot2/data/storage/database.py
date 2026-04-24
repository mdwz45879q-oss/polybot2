"""SQLite database composition root for polybot2."""

from __future__ import annotations

import logging
from pathlib import Path
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Generator, Sequence, TypeVar

import polars as pl

from polybot2.data.storage.config import DataRuntimeConfig
from polybot2.data.storage.db.linking import LinkingAdapter
from polybot2.data.storage.db.markets import MarketsAdapter
from polybot2.data.storage.db.migrations import run_migrations

log = logging.getLogger(__name__)

_T = TypeVar("_T")
_SQLITE_LOCK_RETRY_ATTEMPTS = 8
_SQLITE_LOCK_RETRY_BASE_DELAY_S = 0.05
_SQLITE_LOCK_RETRY_MAX_DELAY_S = 0.5


class Database:
    def __init__(self, cfg: DataRuntimeConfig | Any):
        infra = getattr(cfg, "infra", None)
        self._infra = infra if infra is not None else cfg
        self._conn: sqlite3.Connection | None = None
        self._conn_lock = threading.RLock()

        self.markets = MarketsAdapter(self)
        self.linking = LinkingAdapter(self)

    def open(self) -> None:
        with self._conn_lock:
            if self._conn is not None:
                return
            db_path = str(self._infra.db_path)
            if db_path not in {"", ":memory:"}:
                db_parent = Path(db_path).expanduser().resolve().parent
                db_parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            if bool(self._infra.db_wal_mode):
                conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA temp_store=MEMORY")
            run_migrations(conn)
            self._conn = conn

    def close(self) -> None:
        with self._conn_lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        with self._conn_lock:
            if self._conn is None:
                self.open()
            if self._conn is None:
                raise RuntimeError("Database connection unavailable.")
            return self._conn

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self._run_with_retry(lambda: self.conn.execute(sql, params), op_name="execute")

    def executemany(self, sql: str, seq: Sequence[tuple]) -> None:
        self._run_with_retry(lambda: self.conn.executemany(sql, seq), op_name="executemany")

    def commit(self) -> None:
        self._run_with_retry(lambda: self.conn.commit(), op_name="commit")

    def rollback(self) -> None:
        self._run_with_retry(lambda: self.conn.rollback(), op_name="rollback")

    def read_pl(self, sql: str, params: tuple = ()) -> pl.DataFrame:
        def _read() -> tuple[list[str], list[tuple[Any, ...]]]:
            cur = self.conn.execute(sql, params)
            cols = [str(c[0]) for c in (cur.description or [])]
            rows = [tuple(r) for r in cur.fetchall()]
            return cols, rows

        cols, vals = self._run_with_retry(_read, op_name="read_pl")
        if not cols:
            return pl.DataFrame()
        if not vals:
            return pl.DataFrame({c: [] for c in cols})
        return pl.DataFrame(vals, schema=cols, orient="row")

    @staticmethod
    def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
        msg = str(exc).strip().lower()
        return (
            "database is locked" in msg
            or "database schema is locked" in msg
            or "database table is locked" in msg
        )

    def _run_with_retry(self, fn: Callable[[], _T], *, op_name: str) -> _T:
        delay = float(_SQLITE_LOCK_RETRY_BASE_DELAY_S)
        attempts = max(1, int(_SQLITE_LOCK_RETRY_ATTEMPTS))
        for attempt in range(1, attempts + 1):
            try:
                with self._conn_lock:
                    return fn()
            except sqlite3.OperationalError as exc:
                if not self._is_lock_error(exc) or attempt >= attempts:
                    raise
                log.warning("SQLite lock during %s (attempt %d/%d): %s", op_name, attempt, attempts, exc)
                time.sleep(delay)
                delay = min(delay * 2.0, float(_SQLITE_LOCK_RETRY_MAX_DELAY_S))
        raise RuntimeError(f"SQLite retry loop failed unexpectedly for {op_name}")


@contextmanager
def open_database(cfg: DataRuntimeConfig | Any) -> Generator[Database, None, None]:
    db = Database(cfg)
    try:
        yield db
    finally:
        db.close()
