"""Linking persistence adapter for polybot2 deterministic mappings."""

from __future__ import annotations

import json
from typing import Any


class LinkingAdapter:
    def __init__(self, db: Any):
        self._db = db

    def _batched_executemany(self, sql: str, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        if not rows:
            return
        bs = max(1, int(getattr(self._db._infra, "db_batch_size", 500) or 500))
        for i in range(0, len(rows), bs):
            self._db.executemany(sql, rows[i : i + bs])
        if bool(commit):
            self._db.commit()

    def upsert_provider_games(self, rows: list[tuple[Any, ...]]) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO provider_games
            (provider, provider_game_id, game_label, orig_teams, sport_raw, league_raw,
             category_name, category_country_code,
             when_raw, start_ts_utc, game_date_et, home_raw, away_raw, parse_status,
             parse_reason, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )

    def replace_provider_games_snapshot(self, *, provider: str, rows: list[tuple[Any, ...]]) -> None:
        p = str(provider or "").strip().lower()
        try:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute("DELETE FROM provider_games WHERE provider = ?", (p,))
            if rows:
                bs = max(1, int(getattr(self._db._infra, "db_batch_size", 500) or 500))
                filtered = [r for r in rows if str((r[0] if len(r) > 0 else "") or "").strip().lower() == p]
                for i in range(0, len(filtered), bs):
                    self._db.executemany(
                        """
                        INSERT OR REPLACE INTO provider_games
                        (provider, provider_game_id, game_label, orig_teams, sport_raw, league_raw,
                         category_name, category_country_code,
                         when_raw, start_ts_utc, game_date_et, home_raw, away_raw, parse_status,
                         parse_reason, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        filtered[i : i + bs],
                    )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def load_provider_games(self, *, provider: str) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """
            SELECT * FROM provider_games
            WHERE provider = ?
            ORDER BY provider_game_id ASC
            """,
            (str(provider or "").strip().lower(),),
        ).fetchall()
        return [dict(r) for r in rows]

    def clear_provider_bindings(self, *, provider: str, commit: bool = True) -> None:
        p = str(provider or "").strip().lower()
        self._db.execute("DELETE FROM link_market_bindings WHERE provider = ?", (p,))
        self._db.execute("DELETE FROM link_event_bindings WHERE provider = ?", (p,))
        self._db.execute("DELETE FROM link_game_bindings WHERE provider = ?", (p,))
        if bool(commit):
            self._db.commit()

    def upsert_game_bindings(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_game_bindings
            (provider, provider_game_id, canonical_league, canonical_home_team, canonical_away_team,
             event_slug_prefix, binding_status, reason_code, is_tradeable,
             mapping_version, mapping_hash, run_id, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def upsert_event_bindings(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_event_bindings
            (provider, provider_game_id, event_id, event_slug_prefix, updated_at)
            VALUES (?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def upsert_market_bindings(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_market_bindings
            (provider, provider_game_id, condition_id, outcome_index, token_id, market_slug,
             sports_market_type, binding_status, reason_code, is_tradeable,
             mapping_version, mapping_hash, run_id, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def insert_link_run(
        self,
        *,
        provider: str,
        league: str = "",
        league_scope: str,
        mapping_version: str,
        mapping_hash: str,
        n_games_seen: int,
        n_games_linked: int,
        n_games_tradeable: int,
        n_targets: int,
        n_targets_tradeable: int,
        gate_result: str,
        report: dict[str, Any],
        run_ts: int,
        commit: bool = True,
    ) -> int:
        cur = self._db.execute(
            """
            INSERT INTO link_runs
            (run_ts, provider, league, league_scope, mapping_version, mapping_hash,
             n_games_seen, n_games_linked, n_games_tradeable, n_targets, n_targets_tradeable,
             gate_result, report_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(run_ts),
                str(provider),
                str(league or ""),
                str(league_scope),
                str(mapping_version),
                str(mapping_hash),
                int(n_games_seen),
                int(n_games_linked),
                int(n_games_tradeable),
                int(n_targets),
                int(n_targets_tradeable),
                str(gate_result),
                json.dumps(report, separators=(",", ":"), sort_keys=True, default=str),
            ),
        )
        if bool(commit):
            self._db.commit()
        return int(cur.lastrowid)

    def load_latest_link_run(self, *, provider: str, league: str = "") -> dict[str, Any] | None:
        lk = str(league or "").strip().lower()
        p = str(provider or "").strip().lower()
        if lk:
            row = self._db.execute(
                """
                SELECT * FROM link_runs
                WHERE provider = ? AND league = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (p, lk),
            ).fetchone()
        else:
            row = self._db.execute(
                """
                SELECT * FROM link_runs
                WHERE provider = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (p,),
            ).fetchone()
        return dict(row) if row is not None else None

    def load_link_report_rows(self, *, provider: str) -> dict[str, Any]:
        p = str(provider or "").strip().lower()
        parent_status = self._db.execute(
            """
            SELECT binding_status, COUNT(*) AS n
            FROM link_game_bindings
            WHERE provider = ?
            GROUP BY binding_status
            ORDER BY binding_status
            """,
            (p,),
        ).fetchall()
        target_status = self._db.execute(
            """
            SELECT binding_status, is_tradeable, COUNT(*) AS n
            FROM link_market_bindings
            WHERE provider = ?
            GROUP BY binding_status, is_tradeable
            ORDER BY binding_status, is_tradeable
            """,
            (p,),
        ).fetchall()
        unresolved = self._db.execute(
            """
            SELECT reason_code, COUNT(*) AS n
            FROM link_game_bindings
            WHERE provider = ? AND is_tradeable = 0
            GROUP BY reason_code
            ORDER BY n DESC, reason_code
            """,
            (p,),
        ).fetchall()
        return {
            "parent_status_counts": {str(r["binding_status"]): int(r["n"] or 0) for r in parent_status},
            "target_status_tradeable_counts": {
                f"{str(r['binding_status'])}|{int(r['is_tradeable'] or 0)}": int(r["n"] or 0)
                for r in target_status
            },
            "unresolved_reason_counts": {str(r["reason_code"]): int(r["n"] or 0) for r in unresolved},
        }

    def upsert_run_provider_games(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_run_provider_games
            (run_id, provider, provider_game_id, parse_status, parse_reason, game_label, sport_raw, league_raw,
             when_raw, start_ts_utc, game_date_et, home_raw, away_raw,
             canonical_league, canonical_home_team, canonical_away_team, event_slug_prefix,
             binding_status, reason_code, is_tradeable, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def upsert_run_game_reviews(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_run_game_reviews
            (run_id, provider, provider_game_id, resolution_state, reason_code,
             selected_event_id, selected_event_slug, used_slug_fallback,
             kickoff_tolerance_minutes, kickoff_delta_sec, score_tuple, trace_json, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def upsert_run_event_candidates(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_run_event_candidates
            (run_id, provider, provider_game_id, candidate_rank, event_id, event_slug, kickoff_ts_utc,
             team_set_match, kickoff_within_tolerance, slug_hint_match, ordering_bonus, kickoff_delta_sec,
             score_tuple, is_selected, reject_reason, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def upsert_run_market_targets(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        self._batched_executemany(
            """
            INSERT OR REPLACE INTO link_run_market_targets
            (run_id, provider, provider_game_id, condition_id, outcome_index, token_id, market_slug,
             sports_market_type, binding_status, reason_code, is_tradeable, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
            commit=commit,
        )

    def load_latest_link_run_for_league(self, *, league: str) -> dict[str, Any] | None:
        """Find the latest link run that contains games for the given league."""
        lk = str(league or "").strip().lower()
        if not lk:
            return None
        row = self._db.execute(
            """
            SELECT lr.* FROM link_runs lr
            INNER JOIN link_run_provider_games pg ON pg.run_id = lr.run_id
            WHERE pg.canonical_league = ?
            ORDER BY lr.run_id DESC
            LIMIT 1
            """,
            (lk,),
        ).fetchone()
        return dict(row) if row is not None else None

    def load_link_run(self, *, provider: str, run_id: int | None = None) -> dict[str, Any] | None:
        p = str(provider or "").strip().lower()
        rid = None
        try:
            rid = None if run_id is None else int(run_id)
        except Exception:
            rid = None
        if rid is None:
            return self.load_latest_link_run(provider=p)
        row = self._db.execute(
            "SELECT * FROM link_runs WHERE run_id = ? LIMIT 1",
            (int(rid),),
        ).fetchone()
        return dict(row) if row is not None else None

    def insert_review_decision(
        self,
        *,
        run_id: int,
        provider: str,
        provider_game_id: str,
        decision: str,
        note: str,
        actor: str,
        decided_at: int,
        commit: bool = True,
    ) -> int:
        cur = self._db.execute(
            """
            INSERT INTO link_review_decisions
            (run_id, provider, provider_game_id, decision, note, actor, decided_at)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                int(run_id),
                str(provider or "").strip().lower(),
                str(provider_game_id or "").strip(),
                str(decision or "").strip().lower(),
                str(note or ""),
                str(actor or ""),
                int(decided_at),
            ),
        )
        if bool(commit):
            self._db.commit()
        return int(cur.lastrowid)

