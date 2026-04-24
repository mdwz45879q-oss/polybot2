"""Polymarket market/event persistence adapter for polybot2."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from polybot2.data.payload_artifacts import PayloadArtifactWriter

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
    def _lineage(
        *,
        payload: dict[str, Any] | list[Any] | str | int | float | bool | None,
        payload_writer: PayloadArtifactWriter | None,
        stream_name: str,
        entity_key: str,
        compute_hash: bool = True,
    ) -> tuple[str, str, int]:
        if payload_writer is not None:
            record = payload_writer.write_payload(stream_name=stream_name, entity_key=entity_key, payload=payload)
            return (str(record.payload_sha256), str(record.payload_ref), int(record.payload_size_bytes))
        if not bool(compute_hash):
            return ("", "", 0)
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
        digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        return (digest, "", int(len(payload_json.encode("utf-8"))))

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
        payload_writer: PayloadArtifactWriter | None = None,
        compute_lineage_hash: bool = True,
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
            event_sha, event_ref, event_size = self._lineage(
                payload=event,
                payload_writer=payload_writer,
                stream_name="pm_events",
                entity_key=event_id,
                compute_hash=compute_lineage_hash,
            )
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
                    event_sha,
                    event_ref,
                    event_size,
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
                market_sha, market_ref, market_size = self._lineage(
                    payload=market,
                    payload_writer=payload_writer,
                    stream_name="pm_markets",
                    entity_key=condition_id,
                    compute_hash=compute_lineage_hash,
                )
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
                        market_sha,
                        market_ref,
                        market_size,
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
            if len(vals) == 17:
                normalized_rows.append(vals)
                continue
            if len(vals) == 16:
                normalized_rows.append(
                    (
                        vals[0],  # event_id
                        vals[1],  # title
                        vals[2],  # ticker
                        vals[3],  # slug
                        vals[4],  # slug_raw
                        vals[5],  # sport_key
                        vals[6],  # league_key
                        vals[7],  # game_id
                        vals[8],  # game_date_et
                        None,  # kickoff_ts_utc
                        vals[9],  # start_ts_utc
                        vals[10],  # end_ts_utc
                        vals[11],  # status
                        vals[12],  # payload_sha256
                        vals[13],  # payload_ref
                        vals[14],  # payload_size_bytes
                        vals[15],  # updated_at
                    )
                )
                continue
            if len(vals) == 14:
                normalized_rows.append(
                    (
                        vals[0],  # event_id
                        vals[1],  # title
                        "",  # ticker
                        vals[2],  # slug
                        vals[3],  # slug_raw
                        vals[4],  # sport_key
                        vals[5],  # league_key
                        None,  # game_id
                        vals[6],  # game_date_et
                        None,  # kickoff_ts_utc
                        vals[7],  # start_ts_utc
                        vals[8],  # end_ts_utc
                        vals[9],  # status
                        vals[10],  # payload_sha256
                        vals[11],  # payload_ref
                        vals[12],  # payload_size_bytes
                        vals[13],  # updated_at
                    )
                )
                continue
            raise ValueError(f"pm_events row has unsupported length: {len(vals)}")
        bs = max(1, int(self._db._infra.db_batch_size))
        for i in range(0, len(normalized_rows), bs):
            self._db.executemany(
                """
                INSERT OR REPLACE INTO pm_events
                (event_id, title, ticker, slug, slug_raw, sport_key, league_key, game_id, game_date_et,
                 kickoff_ts_utc, start_ts_utc, end_ts_utc, status, payload_sha256, payload_ref,
                 payload_size_bytes, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            if len(vals) == 19:
                normalized_rows.append(vals)
                continue
            if len(vals) == 17:
                normalized_rows.append(
                    (
                        vals[0],  # condition_id
                        vals[1],  # market_id
                        vals[2],  # event_id
                        vals[3],  # question
                        vals[4],  # question_id
                        vals[5],  # slug
                        vals[6],  # sports_market_type
                        vals[7],  # line
                        None,  # event_start_ts_utc
                        None,  # game_start_ts_utc
                        vals[8],  # resolved
                        vals[9],  # resolution_value
                        vals[10],  # volume
                        vals[11],  # end_date
                        vals[12],  # end_ts_utc
                        vals[13],  # payload_sha256
                        vals[14],  # payload_ref
                        vals[15],  # payload_size_bytes
                        vals[16],  # updated_at
                    )
                )
                continue
            if len(vals) == 13:
                normalized_rows.append(
                    (
                        vals[0],  # condition_id
                        "",  # market_id
                        vals[1],  # event_id
                        vals[2],  # question
                        "",  # question_id
                        vals[3],  # slug
                        "",  # sports_market_type
                        None,  # line
                        None,  # event_start_ts_utc
                        None,  # game_start_ts_utc
                        vals[4],  # resolved
                        vals[5],  # resolution_value
                        vals[6],  # volume
                        vals[7],  # end_date
                        vals[8],  # end_ts_utc
                        vals[9],  # payload_sha256
                        vals[10],  # payload_ref
                        vals[11],  # payload_size_bytes
                        vals[12],  # updated_at
                    )
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
                 volume, end_date, end_ts_utc, payload_sha256, payload_ref,
                 payload_size_bytes, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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

    def replace_pm_sports_ref(self, rows: list[tuple[Any, ...]]) -> None:
        try:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute("DELETE FROM pm_sports_ref")
            if rows:
                bs = max(1, int(self._db._infra.db_batch_size))
                for i in range(0, len(rows), bs):
                    self._db.executemany(
                        """
                        INSERT INTO pm_sports_ref
                        (sport, sport_id, image, resolution, ordering, tags_csv, series, created_at_raw, synced_at)
                        VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        rows[i : i + bs],
                    )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def replace_pm_sports_market_types_ref(self, rows: list[tuple[Any, ...]]) -> None:
        try:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute("DELETE FROM pm_sports_market_types_ref")
            if rows:
                bs = max(1, int(self._db._infra.db_batch_size))
                for i in range(0, len(rows), bs):
                    self._db.executemany(
                        """
                        INSERT INTO pm_sports_market_types_ref
                        (market_type, synced_at)
                        VALUES (?,?)
                        """,
                        rows[i : i + bs],
                    )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def replace_pm_teams_ref(self, rows: list[tuple[Any, ...]]) -> None:
        try:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute("DELETE FROM pm_teams_ref")
            if rows:
                bs = max(1, int(self._db._infra.db_batch_size))
                for i in range(0, len(rows), bs):
                    self._db.executemany(
                        """
                        INSERT INTO pm_teams_ref
                        (team_id, name, league, abbreviation, alias, provider_team_id,
                         record, logo, color, created_at_raw, updated_at_raw, synced_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        rows[i : i + bs],
                    )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def market_tags_filter_sql(self, market_tags: str | None, *, alias: str = "") -> tuple[str, tuple[Any, ...]]:
        text = str(market_tags or "").strip().lower()
        if not text:
            return "1=1", tuple()
        tags = [t for t in text.replace(",", " ").split() if t]
        if not tags:
            return "1=1", tuple()
        col = f"{alias}." if alias else ""
        placeholders = ",".join("?" for _ in tags)
        sql = (
            f"{col}condition_id IN ("
            f"SELECT condition_id FROM pm_market_tags "
            f"WHERE LOWER(slug) IN ({placeholders}) "
            f"GROUP BY condition_id HAVING COUNT(DISTINCT LOWER(slug)) = ?"
            f")"
        )
        return sql, tuple(tags) + (len(tags),)

    def load_pm_events_by_slug_raw(self, slug_raw: str) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """
            SELECT * FROM pm_events WHERE slug_raw = ? ORDER BY event_id
            """,
            (str(slug_raw).strip().lower(),),
        ).fetchall()
        return [dict(r) for r in rows]

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

    def load_sports_ref(self) -> list[dict[str, Any]]:
        rows = self._db.execute(
            """
            SELECT *
            FROM pm_sports_ref
            ORDER BY sport
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def load_market_end_ts_for_condition(self, condition_id: str) -> int | None:
        row = self._db.execute(
            "SELECT end_ts_utc FROM pm_markets WHERE condition_id = ? LIMIT 1",
            (str(condition_id or "").strip(),),
        ).fetchone()
        if row is None:
            return None
        val = row["end_ts_utc"]
        if val is None:
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

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
