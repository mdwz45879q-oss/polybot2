from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkService, load_mapping

_ET = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")


def _et_ts(date_text: str, hour24: int, minute: int = 0) -> int:
    dt = datetime.strptime(f"{date_text} {hour24:02d}:{minute:02d}", "%Y-%m-%d %H:%M").replace(tzinfo=_ET)
    return int(dt.astimezone(_UTC).timestamp())


def _seed_event(
    db,
    *,
    event_id: str,
    slug: str,
    game_date_et: str,
    kickoff_ts_utc: int,
    team_a: str,
    team_b: str,
    now_ts: int,
    market_type: str = "moneyline",
) -> None:
    db.markets.upsert_pm_events(
        [
            (
                event_id,
                f"{team_a} vs {team_b}",
                slug,
                slug,
                slug,
                "",
                "mlb",
                None,
                game_date_et,
                kickoff_ts_utc,
                kickoff_ts_utc - 1000,
                kickoff_ts_utc + 86_400,
                "open",
                now_ts,
            )
        ]
    )
    db.markets.upsert_pm_event_teams(
        [
            (event_id, 0, None, None, team_a, "mlb", "", "", "", "", "", now_ts),
            (event_id, 1, None, None, team_b, "mlb", "", "", "", "", "", now_ts),
        ],
        touched_event_ids=[event_id],
    )
    db.markets.upsert_pm_markets(
        [
            (
                f"cond_{event_id}",
                f"mkt_{event_id}",
                event_id,
                f"{team_a} vs {team_b}",
                "",
                f"{slug}-{market_type}",
                market_type,
                None,
                kickoff_ts_utc,
                kickoff_ts_utc,
                0,
                None,
                0.0,
                "",
                kickoff_ts_utc + 86_400,
                now_ts,
            )
        ]
    )
    db.markets.upsert_pm_market_tokens(
        [
            (f"tok_yes_{event_id}", f"cond_{event_id}", 0, "Yes", now_ts),
            (f"tok_no_{event_id}", f"cond_{event_id}", 1, "No", now_ts),
        ]
    )


def _seed_provider_game(
    db,
    *,
    provider_game_id: str,
    game_date_et: str,
    home_raw: str,
    away_raw: str,
    start_ts_utc: int,
    now_ts: int,
) -> None:
    db.linking.upsert_provider_games(
        [
            (
                "kalstrop_v1",
                provider_game_id,
                f"{home_raw} vs {away_raw}",
                "",
                "baseball",
                "Major League Baseball",
                "", "",
                f"{game_date_et}, 09:00 PM",
                start_ts_utc,
                game_date_et,
                home_raw,
                away_raw,
                "ok",
                "",
                now_ts,
            )
        ]
    )


def test_team_order_invariance_provider_swap_still_matches(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    game_date = "2026-04-17"
    with open_database(runtime) as db:
        _seed_event(
            db,
            event_id="e1",
            slug="mlb-sd-laa-2026-04-17",
            game_date_et=game_date,
            kickoff_ts_utc=_et_ts(game_date, 21, 38),
            team_a="san diego padres",
            team_b="los angeles angels",
            now_ts=now_ts,
        )
        _seed_provider_game(
            db,
            provider_game_id="gid_swap",
            game_date_et=game_date,
            home_raw="San Diego Padres",
            away_raw="Los Angeles Angels",
            start_ts_utc=_et_ts(game_date, 21, 30),
            now_ts=now_ts,
        )
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        row = db.execute(
            "SELECT binding_status, reason_code FROM link_game_bindings WHERE provider = ? AND provider_game_id = ?",
            ("kalstrop_v1", "gid_swap"),
        ).fetchone()
    assert res.n_games_seen == 1
    assert res.n_games_linked == 1
    assert row is not None
    assert row["binding_status"] == "exact"
    assert row["reason_code"] == "ok"


def test_pm_away_first_ordering_does_not_block_match(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    game_date = "2026-04-18"
    with open_database(runtime) as db:
        _seed_event(
            db,
            event_id="e2",
            slug="mlb-sd-laa-2026-04-18",
            game_date_et=game_date,
            kickoff_ts_utc=_et_ts(game_date, 21, 38),
            team_a="san diego padres",
            team_b="los angeles angels",
            now_ts=now_ts,
        )
        _seed_provider_game(
            db,
            provider_game_id="gid_ordering",
            game_date_et=game_date,
            home_raw="Los Angeles Angels",
            away_raw="San Diego Padres",
            start_ts_utc=_et_ts(game_date, 21, 35),
            now_ts=now_ts,
        )
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        ev = db.execute(
            "SELECT event_id FROM link_event_bindings WHERE provider = ? AND provider_game_id = ?",
            ("kalstrop_v1", "gid_ordering"),
        ).fetchone()
    assert res.n_games_linked == 1
    assert ev is not None
    assert ev["event_id"] == "e2"


def test_same_cluster_sibling_events_resolve_as_multi_event(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    game_date = "2026-04-19"
    kickoff = _et_ts(game_date, 19, 0)
    with open_database(runtime) as db:
        _seed_event(
            db,
            event_id="e3a",
            slug="mlb-ari-tor-2026-04-19",
            game_date_et=game_date,
            kickoff_ts_utc=kickoff,
            team_a="arizona diamondbacks",
            team_b="toronto blue jays",
            now_ts=now_ts,
        )
        _seed_event(
            db,
            event_id="e3b",
            slug="mlb-ari-tor-2026-04-19-alt",
            game_date_et=game_date,
            kickoff_ts_utc=kickoff,
            team_a="arizona diamondbacks",
            team_b="toronto blue jays",
            now_ts=now_ts,
        )
        _seed_provider_game(
            db,
            provider_game_id="gid_amb",
            game_date_et=game_date,
            home_raw="Arizona Diamondbacks",
            away_raw="Toronto Blue Jays",
            start_ts_utc=kickoff,
            now_ts=now_ts,
        )
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        bindings = db.execute(
            """
            SELECT event_id
            FROM link_event_bindings
            WHERE provider = ? AND provider_game_id = ?
            ORDER BY event_id
            """,
            ("kalstrop_v1", "gid_amb"),
        ).fetchall()
        selected_count = int(
            db.execute(
                """
                SELECT COUNT(*) AS n
                FROM link_run_event_candidates
                WHERE run_id = ? AND provider = ? AND provider_game_id = ? AND is_selected = 1
                """,
                (int(res.run_id), "kalstrop_v1", "gid_amb"),
            ).fetchone()["n"]
        )
    assert res.n_games_seen == 1
    assert res.n_games_linked == 1
    assert [str(r["event_id"]) for r in bindings] == ["e3a", "e3b"]
    assert selected_count == 2


def test_ambiguous_event_match_when_top_tie_spans_multiple_clusters(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    game_date = "2026-04-19"
    kickoff = _et_ts(game_date, 19, 0)
    with open_database(runtime) as db:
        _seed_event(
            db,
            event_id="e3a",
            slug="mlb-ari-tor-2026-04-19",
            game_date_et=game_date,
            kickoff_ts_utc=kickoff,
            team_a="arizona diamondbacks",
            team_b="toronto blue jays",
            now_ts=now_ts,
        )
        _seed_event(
            db,
            event_id="e3b",
            slug="mlb-tor-ari-2026-04-19",
            game_date_et=game_date,
            kickoff_ts_utc=kickoff,
            team_a="arizona diamondbacks",
            team_b="toronto blue jays",
            now_ts=now_ts,
        )
        _seed_provider_game(
            db,
            provider_game_id="gid_amb",
            game_date_et=game_date,
            home_raw="Arizona Diamondbacks",
            away_raw="Toronto Blue Jays",
            start_ts_utc=kickoff,
            now_ts=now_ts,
        )
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        row = db.execute(
            "SELECT binding_status, reason_code FROM link_game_bindings WHERE provider = ? AND provider_game_id = ?",
            ("kalstrop_v1", "gid_amb"),
        ).fetchone()
    assert res.n_games_seen == 1
    assert res.n_games_linked == 0
    assert row is not None
    assert row["binding_status"] == "unresolved"
    assert row["reason_code"] == "ambiguous_event_match"


def test_market_type_filter_applies_after_multi_event_selection(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    game_date = "2026-04-19"
    kickoff = _et_ts(game_date, 19, 0)
    with open_database(runtime) as db:
        _seed_event(
            db,
            event_id="e5a",
            slug="mlb-ari-tor-2026-04-19",
            game_date_et=game_date,
            kickoff_ts_utc=kickoff,
            team_a="arizona diamondbacks",
            team_b="toronto blue jays",
            now_ts=now_ts,
            market_type="moneyline",
        )
        _seed_event(
            db,
            event_id="e5b",
            slug="mlb-ari-tor-2026-04-19-exact-score",
            game_date_et=game_date,
            kickoff_ts_utc=kickoff,
            team_a="arizona diamondbacks",
            team_b="toronto blue jays",
            now_ts=now_ts,
            market_type="exact_score",
        )
        _seed_provider_game(
            db,
            provider_game_id="gid_market_filter",
            game_date_et=game_date,
            home_raw="Arizona Diamondbacks",
            away_raw="Toronto Blue Jays",
            start_ts_utc=kickoff,
            now_ts=now_ts,
        )
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        bindings = db.execute(
            """
            SELECT event_id
            FROM link_event_bindings
            WHERE provider = ? AND provider_game_id = ?
            ORDER BY event_id
            """,
            ("kalstrop_v1", "gid_market_filter"),
        ).fetchall()
        targets = db.execute(
            """
            SELECT condition_id
            FROM link_market_bindings
            WHERE provider = ? AND provider_game_id = ?
            ORDER BY condition_id, outcome_index
            """,
            ("kalstrop_v1", "gid_market_filter"),
        ).fetchall()

    assert res.n_games_seen == 1
    assert res.n_games_linked == 1
    assert [str(r["event_id"]) for r in bindings] == ["e5a", "e5b"]
    assert {str(r["condition_id"]) for r in targets} == {"cond_e5a"}


def test_kickoff_out_of_tolerance_is_unresolved(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    game_date = "2026-04-19"
    with open_database(runtime) as db:
        # 6 hours apart from provider kickoff; MLB default tolerance is 180 minutes.
        _seed_event(
            db,
            event_id="e4",
            slug="mlb-phi-atl-2026-04-19",
            game_date_et=game_date,
            kickoff_ts_utc=_et_ts(game_date, 19, 0),
            team_a="philadelphia phillies",
            team_b="atlanta braves",
            now_ts=now_ts,
        )
        _seed_provider_game(
            db,
            provider_game_id="gid_kickoff",
            game_date_et=game_date,
            home_raw="Philadelphia Phillies",
            away_raw="Atlanta Braves",
            start_ts_utc=_et_ts(game_date, 1, 0),
            now_ts=now_ts,
        )
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        row = db.execute(
            "SELECT binding_status, reason_code FROM link_game_bindings WHERE provider = ? AND provider_game_id = ?",
            ("kalstrop_v1", "gid_kickoff"),
        ).fetchone()
    assert res.n_games_seen == 1
    assert res.n_games_linked == 0
    assert row is not None
    assert row["binding_status"] == "unresolved"
    assert row["reason_code"] == "kickoff_out_of_tolerance"
