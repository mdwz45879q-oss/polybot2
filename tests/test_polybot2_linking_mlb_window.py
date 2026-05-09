from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkService, load_mapping

_ET = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")
_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "linking_mlb_window_2026_04_17_19.json"


def _et_ts(date_text: str, hour24: int, minute: int = 0) -> int:
    dt = datetime.strptime(f"{date_text} {hour24:02d}:{minute:02d}", "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
    return int(dt.astimezone(_UTC).timestamp())


def _canon_key(home: str, away: str) -> str:
    return "|".join(sorted((home, away)))


def test_linking_mlb_window_matches_all_games(tmp_path: Path) -> None:
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    rows = list(fixture.get("provider_games") or [])
    assert len(rows) == 45

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000

    provider_alias_idx: dict[str, str] = {}
    for canonical_team, tmeta in mapping.team_map.get("mlb", {}).items():
        ck = str(canonical_team)
        provider_alias_idx[" ".join(ck.strip().lower().split())] = ck
        aliases = (tmeta or {}).get("provider_aliases", {}) if isinstance(tmeta, dict) else {}
        for alias in aliases.get("kalstrop_v1", []) if isinstance(aliases, dict) else []:
            provider_alias_idx[" ".join(str(alias).strip().lower().split())] = ck

    team_to_code = {
        str(team): str((meta or {}).get("polymarket_code") or "")
        for team, meta in mapping.team_map.get("mlb", {}).items()
        if isinstance(meta, dict)
    }

    with open_database(runtime) as db:
        provider_db_rows = []
        event_rows = []
        event_team_rows = []
        market_rows = []
        token_rows = []
        event_id = 0
        market_id = 0

        for rec in rows:
            gid = str(rec["provider_game_id"])
            d = str(rec["game_date_et"])
            home_raw = str(rec["home_raw"])
            away_raw = str(rec["away_raw"])
            hr = int(rec["hour_et"])
            provider_ts = _et_ts(d, hr, 0)
            home_key = " ".join(home_raw.strip().lower().split())
            away_key = " ".join(away_raw.strip().lower().split())
            canonical_home = provider_alias_idx.get(home_key, "")
            canonical_away = provider_alias_idx.get(away_key, "")
            assert canonical_home, f"unmapped provider home alias: {home_raw}"
            assert canonical_away, f"unmapped provider away alias: {away_raw}"
            code_home = team_to_code.get(canonical_home, "")
            code_away = team_to_code.get(canonical_away, "")
            assert code_home and code_away

            provider_db_rows.append(
                (
                    "kalstrop_v1",
                    gid,
                    f"{home_raw} vs {away_raw}, {d}",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    f"{d}, {hr:02d}:00 PM",
                    provider_ts,
                    d,
                    home_raw,
                    away_raw,
                    "ok",
                    "",
                    now_ts,
                )
            )

            event_id += 1
            eid = f"mlb_evt_{event_id:03d}"
            # MLB Polymarket is configured as away-first ordering.
            slug = f"mlb-{code_away}-{code_home}-{d}"
            kickoff = provider_ts + 8 * 60
            event_rows.append(
                (
                    eid,
                    f"{canonical_away} vs. {canonical_home}",
                    slug,
                    slug,
                    slug,
                    "",
                    "mlb",
                    10_000_000 + event_id,
                    d,
                    kickoff,
                    kickoff - 12 * 3600,
                    kickoff + 7 * 24 * 3600,
                    "open",
                    "",
                    "",
                    0,
                    now_ts,
                )
            )
            event_team_rows.extend(
                [
                    (eid, 0, 200_000 + event_id * 2, None, canonical_away, "mlb", code_away, "", "", "", "", now_ts),
                    (
                        eid,
                        1,
                        200_001 + event_id * 2,
                        None,
                        canonical_home,
                        "mlb",
                        code_home,
                        "",
                        "",
                        "",
                        "",
                        now_ts,
                    ),
                ]
            )

            market_id += 1
            cid = f"mlb_cond_{market_id:03d}"
            mid = f"mlb_mkt_{market_id:03d}"
            market_rows.append(
                (
                    cid,
                    mid,
                    eid,
                    f"{canonical_away} vs. {canonical_home}",
                    "",
                    f"{slug}-moneyline",
                    "moneyline",
                    None,
                    kickoff,
                    kickoff,
                    0,
                    None,
                    0.0,
                    "",
                    kickoff + 7 * 24 * 3600,
                    "",
                    "",
                    0,
                    now_ts,
                )
            )
            token_rows.extend(
                [
                    (f"{cid}_yes", cid, 0, "Yes", now_ts),
                    (f"{cid}_no", cid, 1, "No", now_ts),
                ]
            )

        db.markets.upsert_pm_events(event_rows)
        db.markets.upsert_pm_event_teams(event_team_rows, touched_event_ids=[r[0] for r in event_rows])
        db.markets.upsert_pm_markets(market_rows)
        db.markets.upsert_pm_market_tokens(token_rows)
        db.linking.upsert_provider_games(provider_db_rows)

        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")

        unresolved = db.execute(
            """
            SELECT reason_code, COUNT(*) AS n
            FROM link_game_bindings
            WHERE provider = ? AND binding_status = 'unresolved'
            GROUP BY reason_code
            ORDER BY reason_code
            """,
            ("kalstrop_v1",),
        ).fetchall()
        event_bindings = db.execute(
            """
            SELECT provider_game_id, event_id
            FROM link_event_bindings
            WHERE provider = ?
            ORDER BY provider_game_id
            """,
            ("kalstrop_v1",),
        ).fetchall()

    assert res.n_games_seen == len(rows)
    assert res.n_games_linked == len(rows)
    assert len(event_bindings) == len(rows)
    # Event-match reasons must be absent for the required MLB window.
    bad_reasons = {
        "no_event_candidates",
        "team_set_not_found",
        "ambiguous_event_match",
        "kickoff_out_of_tolerance",
        "event_slug_not_found",
    }
    unresolved_reasons = {str(r["reason_code"]): int(r["n"]) for r in unresolved}
    assert not (set(unresolved_reasons) & bad_reasons), unresolved_reasons

