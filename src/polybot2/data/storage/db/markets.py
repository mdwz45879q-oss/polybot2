"""Polymarket market/event persistence adapter for polybot2."""

from __future__ import annotations

import hashlib
import json
from typing import Any

_SQLITE_SAFE_DELETE_BATCH_SIZE = 500


class MarketsAdapter:
    def __init__(self, db: Any):
        self._db = db

    @staticmethod
    def _json_list(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str) and value:
            try:
                decoded = json.loads(value)
                return decoded if isinstance(decoded, list) else []
            except Exception:
                return []
        return []

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _to_optional_float(value: Any) -> float | None:
        try:
            if value is None or str(value).strip() == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_optional_int(value: Any) -> int | None:
        try:
            if value is None or str(value).strip() == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_iso_ts(value: str) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                from datetime import datetime

                return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
            from datetime import datetime

            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                from datetime import timezone

                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

    @staticmethod
    def _event_id_from_event(event: dict[str, Any]) -> str:
        for key in ("id", "eventId", "event_id"):
            text = str(event.get(key) or "").strip()
            if text:
                return text
        slug = str(event.get("slug") or event.get("eventSlug") or "").strip()
        if slug:
            return f"slug:{slug}"
        digest = hashlib.sha1(json.dumps(event, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
        return f"evt_{digest}"

    @staticmethod
    def _slug_prefix(slug_raw: str) -> str:
        text = str(slug_raw or "").strip().lower()
        if not text:
            return ""
        parts = text.split("-")
        if len(parts) >= 6:
            return "-".join(parts[:6])
        return text

    @staticmethod
    def _league_key_from_slug(slug_raw: str) -> str:
        text = str(slug_raw or "").strip().lower()
        if not text:
            return ""
        return text.split("-", 1)[0]

    @staticmethod
    def _game_date_from_slug(slug_raw: str) -> str:
        text = str(slug_raw or "").strip().lower()
        parts = text.split("-")
        if len(parts) >= 6:
            y, m, d = parts[3], parts[4], parts[5]
            if len(y) == 4 and len(m) == 2 and len(d) == 2:
                return f"{y}-{m}-{d}"
        return ""

    def _delete_where_in_chunks(
        self,
        *,
        table: str,
        column: str,
        ids: list[str],
    ) -> None:
        if not ids:
            return
        chunk_size = max(1, int(_SQLITE_SAFE_DELETE_BATCH_SIZE))
        for i in range(0, len(ids), chunk_size):
            batch = ids[i : i + chunk_size]
            placeholders = ",".join("?" for _ in batch)
            self._db.execute(
                f"DELETE FROM {table} WHERE {column} IN ({placeholders})",
                tuple(batch),
            )

    def upsert_from_gamma_events(
        self,
        *,
        events_data: list[dict[str, Any]],
        updated_ts: int,
        commit: bool = True,
    ) -> tuple[int, int, int, int]:
        if not events_data:
            return (0, 0, 0, 0)

        event_rows: list[tuple[Any, ...]] = []
        market_rows: list[tuple[Any, ...]] = []
        token_rows: list[tuple[Any, ...]] = []
        tag_rows: list[tuple[Any, ...]] = []
        team_rows: list[tuple[Any, ...]] = []
        touched_event_ids: set[str] = set()

        for event in events_data:
            if not isinstance(event, dict):
                continue
            event_id = self._event_id_from_event(event)
            touched_event_ids.add(event_id)
            slug_raw = str(event.get("slug") or event.get("eventSlug") or "").strip().lower()
            slug = self._slug_prefix(slug_raw) or slug_raw
            title = str(event.get("title") or event.get("name") or event.get("question") or "")
            ticker = str(event.get("ticker") or "").strip().lower()
            kickoff_ts = self._parse_iso_ts(str(event.get("startTime") or event.get("start_time") or ""))
            start_ts = self._parse_iso_ts(str(event.get("startDate") or event.get("start_date") or ""))
            end_ts = self._parse_iso_ts(str(event.get("endDate") or event.get("end_date") or ""))
            game_id = self._to_optional_int(event.get("gameId") or event.get("game_id"))
            status = "closed" if bool(event.get("closed")) else "open"
            event_rows.append(
                (
                    event_id,
                    title,
                    ticker,
                    slug,
                    slug_raw,
                    "",
                    self._league_key_from_slug(slug_raw),
                    game_id,
                    self._game_date_from_slug(slug_raw),
                    kickoff_ts,
                    start_ts,
                    end_ts,
                    status,
                    int(updated_ts),
                )
            )

            teams = event.get("teams") if isinstance(event.get("teams"), list) else []
            for idx, team in enumerate(teams):
                if not isinstance(team, dict):
                    continue
                team_rows.append(
                    (
                        event_id,
                        int(idx),
                        self._to_optional_int(team.get("id")),
                        self._to_optional_int(team.get("providerId") or team.get("provider_id")),
                        str(team.get("name") or ""),
                        str(team.get("league") or "").strip().lower(),
                        str(team.get("abbreviation") or "").strip().lower(),
                        str(team.get("alias") or ""),
                        str(team.get("record") or ""),
                        str(team.get("logo") or ""),
                        str(team.get("color") or ""),
                        int(updated_ts),
                    )
                )

            tags = event.get("tags") if isinstance(event.get("tags"), list) else []
            markets = event.get("markets") if isinstance(event.get("markets"), list) else []
            for market in markets:
                if not isinstance(market, dict):
                    continue
                condition_id = str(market.get("conditionId") or market.get("condition_id") or "").strip()
                if not condition_id:
                    continue
                market_slug = str(market.get("slug") or "").strip().lower()
                end_date = str(market.get("endDate") or market.get("end_date") or "")
                event_start_ts = self._parse_iso_ts(
                    str(market.get("eventStartTime") or market.get("event_start_time") or "")
                )
                game_start_ts = self._parse_iso_ts(
                    str(market.get("gameStartTime") or market.get("game_start_time") or "")
                )
                market_rows.append(
                    (
                        condition_id,
                        str(market.get("id") or "").strip(),
                        event_id,
                        str(market.get("question") or market.get("title") or ""),
                        str(market.get("questionID") or market.get("questionId") or "").strip(),
                        market_slug,
                        str(market.get("sportsMarketType") or "").strip().lower(),
                        self._to_optional_float(market.get("line")),
                        event_start_ts,
                        game_start_ts,
                        1 if bool(market.get("closed") or market.get("resolved")) else 0,
                        None,
                        self._to_float(market.get("volume")),
                        end_date,
                        self._parse_iso_ts(end_date),
                        int(updated_ts),
                    )
                )

                outcomes = self._json_list(market.get("outcomes"))
                token_ids = self._json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
                for idx in range(max(len(outcomes), len(token_ids))):
                    token_id = str(token_ids[idx] if idx < len(token_ids) else "")
                    outcome_label = str(outcomes[idx] if idx < len(outcomes) else "")
                    if not token_id and not outcome_label:
                        continue
                    if token_id:
                        token_rows.append(
                            (
                                token_id,
                                condition_id,
                                int(idx),
                                outcome_label,
                                int(updated_ts),
                            )
                        )

                for tag in tags:
                    if not isinstance(tag, dict):
                        continue
                    tag_slug = str(tag.get("slug") or tag.get("label") or "").strip().lower().replace(" ", "-")
                    if not tag_slug:
                        continue
                    try:
                        tag_id = int(tag.get("id") or 0)
                    except (TypeError, ValueError):
                        tag_id = 0
                    tag_rows.append((condition_id, tag_id, str(tag.get("label") or ""), tag_slug))

        try:
            if bool(commit):
                self._db.execute("BEGIN IMMEDIATE")
            self.upsert_pm_events(event_rows, commit=False)
            self.upsert_pm_markets(market_rows, commit=False)
            self.upsert_pm_market_tokens(token_rows, commit=False)
            self.upsert_pm_market_tags(tag_rows, commit=False)
            self.upsert_pm_event_teams(team_rows, touched_event_ids=sorted(touched_event_ids), commit=False)
            if bool(commit):
                self._db.commit()
        except Exception:
            if bool(commit):
                self._db.rollback()
            raise
        return (len(event_rows), len(market_rows), len(token_rows), len(tag_rows))

    def upsert_pm_events(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        if not rows:
            return
        normalized_rows: list[tuple[Any, ...]] = []
        for row in rows:
            vals = tuple(row)
            if len(vals) == 14:
                # Disambiguate: new format has status at position 12 (str like "open"/"closed")
                # Old legacy format has status at position 9 and payload_size at position 12 (int 0)
                if isinstance(vals[12], str) and vals[12] in ("open", "closed", ""):
                    normalized_rows.append(vals)
                else:
                    # Old 14-element legacy format with payload cols:
                    # (eid, title, slug, slug_raw, sport, league, gdate, start, end, status, sha, ref, size, ts)
                    normalized_rows.append(
                        (vals[0], vals[1], "", vals[2], vals[3], vals[4], vals[5], None, vals[6], None, vals[7], vals[8], vals[9], vals[13])
                    )
                continue
            if len(vals) == 17:
                # Old format with all fields including payload columns — strip payload
                normalized_rows.append(vals[:13] + (vals[16],))
                continue
            if len(vals) == 11:
                # Legacy test format: (eid, title, slug, slug_raw, sport, league, gdate, start, end, status, ts)
                normalized_rows.append(
                    (vals[0], vals[1], "", vals[2], vals[3], vals[4], vals[5], None, vals[6], None, vals[7], vals[8], vals[9], vals[10])
                )
                continue
            raise ValueError(f"pm_events row has unsupported length: {len(vals)}")
        bs = max(1, int(self._db._infra.db_batch_size))
        for i in range(0, len(normalized_rows), bs):
            self._db.executemany(
                """
                INSERT OR REPLACE INTO pm_events
                (event_id, title, ticker, slug, slug_raw, sport_key, league_key, game_id, game_date_et,
                 kickoff_ts_utc, start_ts_utc, end_ts_utc, status, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                normalized_rows[i : i + bs],
            )
        if bool(commit):
            self._db.commit()

    def upsert_pm_markets(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        if not rows:
            return
        normalized_rows: list[tuple[Any, ...]] = []
        for row in rows:
            vals = tuple(row)
            if len(vals) == 16:
                normalized_rows.append(vals)
                continue
            if len(vals) == 19:
                # Old format with payload columns — strip them
                normalized_rows.append(vals[:15] + (vals[18],))
                continue
            if len(vals) == 17:
                # Old test format without event_start/game_start but with payload cols:
                # (cid, mid, eid, q, qid, slug, smt, line, resolved, res_val, vol, end_date, end_ts, sha, ref, size, ts)
                normalized_rows.append(
                    (vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7],
                     None, None, vals[8], vals[9], vals[10], vals[11], vals[12], vals[16])
                )
                continue
            if len(vals) == 14:
                # Minimal test format: (cid, mid, eid, q, qid, slug, smt, line, resolved, res_val, vol, end_date, end_ts, ts)
                normalized_rows.append(
                    (vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7],
                     None, None, vals[8], vals[9], vals[10], vals[11], vals[12], vals[13])
                )
                continue
            raise ValueError(f"pm_markets row has unsupported length: {len(vals)}")
        bs = max(1, int(self._db._infra.db_batch_size))
        for i in range(0, len(normalized_rows), bs):
            self._db.executemany(
                """
                INSERT OR REPLACE INTO pm_markets
                (condition_id, market_id, event_id, question, question_id, slug, sports_market_type, line,
                 event_start_ts_utc, game_start_ts_utc, resolved, resolution_value,
                 volume, end_date, end_ts_utc, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                normalized_rows[i : i + bs],
            )
        if bool(commit):
            self._db.commit()

    def upsert_pm_market_tokens(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        if not rows:
            return
        touched_condition_ids = sorted(
            {
                str(r[1]).strip()
                for r in rows
                if len(r) > 1 and str(r[1]).strip()
            }
        )
        if touched_condition_ids:
            self._delete_where_in_chunks(
                table="pm_market_tokens",
                column="condition_id",
                ids=touched_condition_ids,
            )
        bs = max(1, int(self._db._infra.db_batch_size))
        for i in range(0, len(rows), bs):
            self._db.executemany(
                """
                INSERT OR REPLACE INTO pm_market_tokens
                (token_id, condition_id, outcome_index, outcome_label, updated_at)
                VALUES (?,?,?,?,?)
                """,
                rows[i : i + bs],
            )
        if bool(commit):
            self._db.commit()

    def upsert_pm_market_tags(self, rows: list[tuple[Any, ...]], *, commit: bool = True) -> None:
        if not rows:
            return
        bs = max(1, int(self._db._infra.db_batch_size))
        for i in range(0, len(rows), bs):
            self._db.executemany(
                """
                INSERT OR REPLACE INTO pm_market_tags
                (condition_id, tag_id, label, slug)
                VALUES (?,?,?,?)
                """,
                rows[i : i + bs],
            )
        if bool(commit):
            self._db.commit()

    def upsert_pm_event_teams(
        self,
        rows: list[tuple[Any, ...]],
        *,
        touched_event_ids: list[str] | None = None,
        commit: bool = True,
    ) -> None:
        touched = sorted({str(x).strip() for x in (touched_event_ids or []) if str(x).strip()})
        if not touched and not rows:
            return
        if touched:
            self._delete_where_in_chunks(
                table="pm_event_teams",
                column="event_id",
                ids=touched,
            )
        if rows:
            bs = max(1, int(self._db._infra.db_batch_size))
            for i in range(0, len(rows), bs):
                self._db.executemany(
                    """
                    INSERT OR REPLACE INTO pm_event_teams
                    (event_id, team_index, team_id, provider_team_id, name, league,
                     abbreviation, alias, record, logo, color, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows[i : i + bs],
                )
        if bool(commit):
            self._db.commit()

    def load_pm_events_by_league_and_date_range(
        self,
        *,
        league_key: str,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        lk = str(league_key or "").strip().lower()
        df = str(date_from or "").strip()
        dt = str(date_to or "").strip()
        if not lk or not df or not dt:
            return []
        rows = self._db.execute(
            """
            SELECT *
            FROM pm_events
            WHERE league_key = ?
              AND game_date_et >= ?
              AND game_date_et <= ?
            ORDER BY game_date_et, kickoff_ts_utc, event_id
            """,
            (lk, df, dt),
        ).fetchall()
        return [dict(r) for r in rows]

    def load_pm_events_by_league_and_kickoff_range(
        self,
        *,
        league_key: str,
        kickoff_from_utc: int,
        kickoff_to_utc: int,
    ) -> list[dict[str, Any]]:
        """Load PM events by kickoff timestamp range.

        Catches postponed games whose game_date_et is stale but
        kickoff_ts_utc has been updated to the rescheduled time.
        """
        lk = str(league_key or "").strip().lower()
        if not lk:
            return []
        rows = self._db.execute(
            """
            SELECT *
            FROM pm_events
            WHERE league_key = ?
              AND kickoff_ts_utc >= ?
              AND kickoff_ts_utc <= ?
            ORDER BY kickoff_ts_utc, event_id
            """,
            (lk, kickoff_from_utc, kickoff_to_utc),
        ).fetchall()
        return [dict(r) for r in rows]

    def load_markets_for_event_ids(self, event_ids: list[str]) -> list[dict[str, Any]]:
        if not event_ids:
            return []
        placeholders = ",".join("?" for _ in event_ids)
        rows = self._db.execute(
            f"""
            SELECT * FROM pm_markets WHERE event_id IN ({placeholders}) ORDER BY condition_id
            """,
            tuple(event_ids),
        ).fetchall()
        return [dict(r) for r in rows]

    def load_tokens_for_condition_ids(self, condition_ids: list[str]) -> list[dict[str, Any]]:
        if not condition_ids:
            return []
        placeholders = ",".join("?" for _ in condition_ids)
        rows = self._db.execute(
            f"""
            SELECT * FROM pm_market_tokens WHERE condition_id IN ({placeholders}) ORDER BY condition_id, outcome_index
            """,
            tuple(condition_ids),
        ).fetchall()
        return [dict(r) for r in rows]

    def load_event_teams_for_event_ids(self, event_ids: list[str]) -> list[dict[str, Any]]:
        if not event_ids:
            return []
        placeholders = ",".join("?" for _ in event_ids)
        rows = self._db.execute(
            f"""
            SELECT *
            FROM pm_event_teams
            WHERE event_id IN ({placeholders})
            ORDER BY event_id, team_index
            """,
            tuple(event_ids),
        ).fetchall()
        return [dict(r) for r in rows]

    def load_valid_sports_market_types(self) -> list[str]:
        rows = self._db.execute(
            """
            SELECT market_type
            FROM pm_sports_market_types_ref
            ORDER BY market_type
            """
        ).fetchall()
        return [str(r["market_type"] or "") for r in rows if str(r["market_type"] or "").strip()]

    def load_market_tokens_for_condition_pl(self, condition_id: str):
        cid = str(condition_id or "").strip()
        if not cid:
            return self._db.read_pl("SELECT token_id, condition_id, outcome_index, outcome_label FROM pm_market_tokens WHERE 1=0")
        return self._db.read_pl(
            """
            SELECT token_id, condition_id, outcome_index, outcome_label AS title
            FROM pm_market_tokens
            WHERE condition_id = ?
            ORDER BY outcome_index ASC, token_id ASC
            """,
            (cid,),
        )
