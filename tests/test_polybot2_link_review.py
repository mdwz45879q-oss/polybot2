from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from polybot2._cli.router import dispatch
from polybot2._cli.link_review_ui import _build_session_renderable
from polybot2._cli.link_review_ui import _build_card_document_lines
from polybot2._cli.link_review_ui import _derive_game_state
from polybot2._cli.link_review_ui import _SESSION_STYLE_MAP
from polybot2._cli.link_review_ui import _decode_session_key
from polybot2._cli.link_review_ui import _kv_value_style
from polybot2._cli.link_review_ui import _RICH_AVAILABLE
from polybot2._cli.link_review_ui import _styled_card_line
from polybot2._cli.parser import build_parser
from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkReviewService, LinkService, load_mapping
from polybot2.linking.review import _market_sort_key
from polybot2.linking.review import _normalize_market_type_info

if _RICH_AVAILABLE:
    from rich.console import Console


def _seed_link_data(*, runtime: DataRuntimeConfig, include_unresolved: bool = True) -> int:
    mapping = load_mapping()
    now_ts = 1_777_000_000
    provider_rows = [
        (
            "kalstrop_v1",
            "gid_ok",
            "Atlanta Braves vs Philadelphia Phillies, 2026-04-18",
            "",
            "baseball",
            "Major League Baseball",
            "", "",
            "2026-04-18, 07:00 PM",
            1_776_553_200,
            "2026-04-18",
            "Atlanta Braves",
            "Philadelphia Phillies",
            "ok",
            "",
            now_ts,
        )
    ]
    if include_unresolved:
        provider_rows.append(
            (
                "kalstrop_v1",
                "gid_bad",
                "Atlanta Braves vs Unknown Team, 2026-04-18",
                "",
                "baseball",
                "Major League Baseball",
                "", "",
                "2026-04-18, 07:15 PM",
                1_776_553_300,
                "2026-04-18",
                "Atlanta Braves",
                "Unknown Team",
                "ok",
                "",
                now_ts,
            )
        )

    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[
                {
                    "id": "evt_1",
                    "title": "Philadelphia Phillies vs Atlanta Braves",
                    "slug": "mlb-phi-atl-2026-04-18",
                    "startTime": "2026-04-18T23:00:00Z",
                    "tags": [{"id": 1, "label": "MLB", "slug": "mlb"}],
                    "teams": [
                        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_1",
                            "conditionId": "cond_1",
                            "question": "Philadelphia Phillies vs Atlanta Braves",
                            "slug": "mlb-phi-atl-2026-04-18-moneyline",
                            "sportsMarketType": "moneyline",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Yes", "No"],
                            "clobTokenIds": ["tok_yes", "tok_no"],
                        },
                        {
                            "id": "mkt_2",
                            "conditionId": "cond_2",
                            "question": "Any player to hit a home run",
                            "slug": "mlb-phi-atl-2026-04-18-player-props",
                            "sportsMarketType": "player_props",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 500,
                            "outcomes": ["Yes", "No"],
                            "clobTokenIds": ["tok_prop_yes", "tok_prop_no"],
                        }
                    ],
                }
            ],
            updated_ts=now_ts,
        )
        db.linking.upsert_provider_games(provider_rows)
        result = LinkService(db=db).build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
    return int(result.run_id)


def _seed_link_data_multi_event(*, runtime: DataRuntimeConfig) -> int:
    mapping = load_mapping()
    now_ts = 1_777_000_000
    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[
                {
                    "id": "evt_multi_a",
                    "title": "Philadelphia Phillies vs Atlanta Braves",
                    "slug": "mlb-phi-atl-2026-04-18",
                    "startTime": "2026-04-18T23:00:00Z",
                    "tags": [{"id": 1, "label": "MLB", "slug": "mlb"}],
                    "teams": [
                        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_multi_a",
                            "conditionId": "cond_multi_a",
                            "question": "Philadelphia Phillies vs Atlanta Braves",
                            "slug": "mlb-phi-atl-2026-04-18-moneyline",
                            "sportsMarketType": "moneyline",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Yes", "No"],
                            "clobTokenIds": ["tok_multi_a_yes", "tok_multi_a_no"],
                        }
                    ],
                },
                {
                    "id": "evt_multi_b",
                    "title": "Philadelphia Phillies vs Atlanta Braves - More Markets",
                    "slug": "mlb-phi-atl-2026-04-18-alt",
                    "startTime": "2026-04-18T23:00:00Z",
                    "tags": [{"id": 1, "label": "MLB", "slug": "mlb"}],
                    "teams": [
                        {"id": 20, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 21, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_multi_b",
                            "conditionId": "cond_multi_b",
                            "question": "Philadelphia Phillies vs Atlanta Braves - totals",
                            "slug": "mlb-phi-atl-2026-04-18-alt-totals",
                            "sportsMarketType": "totals",
                            "line": 8.5,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Over 8.5", "Under 8.5"],
                            "clobTokenIds": ["tok_multi_b_yes", "tok_multi_b_no"],
                        }
                    ],
                },
            ],
            updated_ts=now_ts,
        )
        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid_multi_style",
                    "Atlanta Braves vs Philadelphia Phillies, 2026-04-18",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    "2026-04-18, 07:00 PM",
                    1_776_553_200,
                    "2026-04-18",
                    "Atlanta Braves",
                    "Philadelphia Phillies",
                    "ok",
                    "",
                    now_ts,
                )
            ]
        )
        result = LinkService(db=db).build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
    return int(result.run_id)


def test_review_queries_summary_unresolved_and_reason_filter(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)

    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        summary = review.get_run_status(provider="kalstrop_v1", include_inactive=True)
        assert summary["run_found"] is True
        assert int(summary["run_id"]) == run_id
        assert int(summary["n_games_seen"]) == 2
        assert int(summary["n_unresolved_games"]) == 1
        unresolved = review.unresolved_games(provider="kalstrop_v1", include_inactive=True)
        assert len(unresolved) == 1
        assert unresolved[0]["provider_game_id"] == "gid_bad"
        assert unresolved[0]["reason_code"] == "team_alias_unmapped"
        filtered = review.unresolved_games(provider="kalstrop_v1", reason="team_alias_unmapped", include_inactive=True)
        assert len(filtered) == 1
        none_rows = review.unresolved_games(provider="kalstrop_v1", reason="not_a_reason", include_inactive=True)
        assert none_rows == []


def test_review_queries_matched_compact_and_game_drilldown(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    _seed_link_data(runtime=runtime, include_unresolved=True)

    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        rows = review.matched_games(provider="kalstrop_v1", include_inactive=True)
        assert len(rows) == 1
        row = rows[0]
        assert row["provider_game_id"] == "gid_ok"
        assert int(row["n_markets"]) == 1
        assert int(row["n_tradeable_targets"]) == 2
        assert row["event_id"] == "evt_1"
        assert row["market_types_csv"] == "moneyline"
        assert isinstance(row["kickoff_delta_min"], int)

        drill = review.game_drilldown(provider="kalstrop_v1", provider_game_id="gid_ok")
        assert drill["game"] is not None
        assert drill["event"] is not None
        assert drill["event"]["event_id"] == "evt_1"
        markets = drill["markets"]
        assert len(markets) == 2
        assert [int(m["outcome_index"]) for m in markets] == [0, 1]
        selected_events = drill["event"].get("selected_events") if isinstance(drill["event"], dict) else None
        assert isinstance(selected_events, list)
        assert len(selected_events) == 1


def test_review_queue_hides_inactive_by_default_and_can_include_it(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    with open_database(runtime) as db:
        db.execute("UPDATE pm_events SET status = 'closed' WHERE event_id = ?", ("evt_1",))
        db.commit()
        review = LinkReviewService(db=db)
        default_rows = review.get_queue(provider="kalstrop_v1", run_id=run_id, parse_status="ok", limit=20)
        full_rows = review.get_queue(
            provider="kalstrop_v1",
            run_id=run_id,
            parse_status="ok",
            limit=20,
            include_inactive=True,
        )
    assert default_rows == []
    assert len(full_rows) == 1
    assert str(full_rows[0].get("provider_game_id") or "") == "gid_ok"


def test_review_card_includes_semantic_market_blocks(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        card_payload = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_ok")
        assert bool(card_payload.get("found"))
        card = card_payload.get("card") if isinstance(card_payload.get("card"), dict) else {}
        market_bindings = card.get("market_bindings") if isinstance(card.get("market_bindings"), dict) else {}
        markets = market_bindings.get("markets") if isinstance(market_bindings.get("markets"), list) else []
        unselected_markets = (
            market_bindings.get("unselected_markets")
            if isinstance(market_bindings.get("unselected_markets"), list)
            else []
        )
        assert len(markets) == 1
        market = markets[0]
        assert str(market.get("condition_id") or "") == "cond_1"
        assert str(market.get("display_market_type") or "") == "MONEYLINE"
        assert str(market.get("market_type_key") or "") == "moneyline"
        assert int(market.get("market_type_inferred") or 0) == 0
        outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
        assert len(outcomes) == 2
        # Token labels are "Yes/No" in this fixture, so GAME-family fallback should use event team names.
        labels = [str(o.get("outcome_label") or "").lower() for o in outcomes]
        assert "philadelphia phillies" in labels
        assert "atlanta braves" in labels
        assert int(market_bindings.get("n_selected_markets") or 0) == 1
        assert int(market_bindings.get("n_total_markets") or 0) == 2
        assert len(unselected_markets) == 1
        assert str(unselected_markets[0].get("condition_id") or "") == "cond_2"
        assert str(unselected_markets[0].get("display_market_type") or "") == "OTHER"
        assert int(unselected_markets[0].get("is_selected") or 0) == 0


def test_review_card_supports_multi_event_hierarchy(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    now_ts = 1_777_000_000
    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[
                {
                    "id": "evt_multi_1",
                    "title": "Philadelphia Phillies vs Atlanta Braves",
                    "slug": "mlb-phi-atl-2026-04-18",
                    "startTime": "2026-04-18T23:00:00Z",
                    "tags": [{"id": 1, "label": "MLB", "slug": "mlb"}],
                    "teams": [
                        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_multi_1",
                            "conditionId": "cond_multi_1",
                            "question": "Philadelphia Phillies vs Atlanta Braves",
                            "slug": "mlb-phi-atl-2026-04-18-moneyline",
                            "sportsMarketType": "moneyline",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Yes", "No"],
                            "clobTokenIds": ["tok_multi_1_yes", "tok_multi_1_no"],
                        }
                    ],
                },
                {
                    "id": "evt_multi_2",
                    "title": "Philadelphia Phillies vs Atlanta Braves - More Markets",
                    "slug": "mlb-phi-atl-2026-04-18-alt",
                    "startTime": "2026-04-18T23:00:00Z",
                    "tags": [{"id": 1, "label": "MLB", "slug": "mlb"}],
                    "teams": [
                        {"id": 20, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 21, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_multi_2",
                            "conditionId": "cond_multi_2",
                            "question": "Philadelphia Phillies vs Atlanta Braves - totals",
                            "slug": "mlb-phi-atl-2026-04-18-alt-totals",
                            "sportsMarketType": "totals",
                            "line": 8.5,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Over 8.5", "Under 8.5"],
                            "clobTokenIds": ["tok_multi_2_yes", "tok_multi_2_no"],
                        }
                    ],
                },
            ],
            updated_ts=now_ts,
        )
        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid_multi",
                    "Atlanta Braves vs Philadelphia Phillies, 2026-04-18",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    "2026-04-18, 07:00 PM",
                    1_776_553_200,
                    "2026-04-18",
                    "Atlanta Braves",
                    "Philadelphia Phillies",
                    "ok",
                    "",
                    now_ts,
                )
            ]
        )
        run_id = int(LinkService(db=db).build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all").run_id)
        review = LinkReviewService(db=db)
        card_payload = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_multi")
        assert bool(card_payload.get("found"))
        card = card_payload.get("card") if isinstance(card_payload.get("card"), dict) else {}
        event_resolution = card.get("event_resolution") if isinstance(card.get("event_resolution"), dict) else {}
        selected_events = event_resolution.get("selected_events") if isinstance(event_resolution.get("selected_events"), list) else []
        market_bindings = card.get("market_bindings") if isinstance(card.get("market_bindings"), dict) else {}
        markets = market_bindings.get("markets") if isinstance(market_bindings.get("markets"), list) else []
        selected_ids = {str(ev.get("event_id") or "") for ev in selected_events}
        market_event_ids = {str(m.get("event_id") or "") for m in markets}

    assert selected_ids == {"evt_multi_1", "evt_multi_2"}
    assert market_event_ids == {"evt_multi_1", "evt_multi_2"}


def test_review_v2_decisions_and_status_progress(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)

    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        status = review.get_run_status(provider="kalstrop_v1", run_id=run_id, include_inactive=True)
        assert status["run_found"] is True
        assert int(status["run_id"]) == run_id
        assert bool(status["decision_progress"]["all_approved"]) is False

        queue = review.get_queue(provider="kalstrop_v1", run_id=run_id, parse_status="ok", limit=20, include_inactive=True)
        assert len(queue) == 2
        gid_ok = "gid_ok"
        card = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id=gid_ok)
        assert card["found"] is True
        assert card["card"]["event_resolution"]["selected_event_id"] == "evt_1"
        assert str(card["card"]["event_resolution"]["selected_event_status"] or "") == "open"
        assert [str(r.get("event_id") or "") for r in card["card"]["event_resolution"]["selected_events"]] == ["evt_1"]
        assert str(card["card"]["event_resolution"]["selected_events"][0].get("status") or "") == "open"
        candidates = review.get_candidate_comparison(provider="kalstrop_v1", run_id=run_id, provider_game_id=gid_ok)
        assert isinstance(candidates, list)

        review.record_decision(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_ok", decision="approve", actor="test")
        review.record_decision(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_bad", decision="approve", actor="test")
        progress = review.get_decision_progress(provider="kalstrop_v1", run_id=run_id, include_inactive=True)
        assert bool(progress["all_approved"]) is True

        # Latest-decision semantics (append-only log): newest action overrides prior action for progress.
        review.record_decision(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_bad", decision="reject", actor="test")
        progress2 = review.get_decision_progress(provider="kalstrop_v1", run_id=run_id, include_inactive=True)
        assert int(progress2["n_approved"]) == 1
        assert int(progress2["n_rejected"]) == 1
        assert int(progress2["n_pending"]) == 0
        assert bool(progress2["all_approved"]) is False


def test_review_queue_scope_filters(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        all_rows = review.get_queue(provider="kalstrop_v1", run_id=run_id, scope="all", parse_status="ok", limit=50, include_inactive=True)
        assert {str(r["provider_game_id"]) for r in all_rows} == {"gid_ok", "gid_bad"}

        mapped_rows = review.get_queue(provider="kalstrop_v1", run_id=run_id, scope="mapped", parse_status="ok", limit=50, include_inactive=True)
        assert [str(r["provider_game_id"]) for r in mapped_rows] == ["gid_ok"]

        unresolved_rows = review.get_queue(provider="kalstrop_v1", run_id=run_id, scope="unresolved", parse_status="ok", limit=50, include_inactive=True)
        assert [str(r["provider_game_id"]) for r in unresolved_rows] == ["gid_bad"]

        mapped_pending = review.get_queue(provider="kalstrop_v1", run_id=run_id, scope="mapped_pending", parse_status="ok", limit=50, include_inactive=True)
        assert [str(r["provider_game_id"]) for r in mapped_pending] == ["gid_ok"]

        review.record_decision(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_ok", decision="approve", actor="test")
        mapped_pending_after = review.get_queue(provider="kalstrop_v1", run_id=run_id, scope="mapped_pending", parse_status="ok", limit=50, include_inactive=True)
        assert mapped_pending_after == []


def test_review_cli_session_defaults_and_key_decode(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    _seed_link_data(runtime=runtime, include_unresolved=True)
    parser = build_parser()

    session_args = parser.parse_args(
        [
            "link",
            "review",
            "--db",
            runtime.db_path,
            "--run-id",
            "1",
        ]
    )
    assert str(session_args.scope) == "mapped_pending"

    assert _decode_session_key(b"\x1b[D") == "left"
    assert _decode_session_key(b"\x1b[C") == "right"
    assert _decode_session_key(b"\x1b[A") == "up"
    assert _decode_session_key(b"\x1b[B") == "down"
    assert _decode_session_key(b"\x1b[5~") == "pageup"
    assert _decode_session_key(b"\x1b[6~") == "pagedown"
    assert _decode_session_key(b"\x1b[H") == "home"
    assert _decode_session_key(b"\x1b[F") == "end"
    assert _decode_session_key(b"a") == "a"
    assert _decode_session_key(b"r") == "r"
    assert _decode_session_key(b"s") == "s"
    assert _decode_session_key(b"o") == "o"
    assert _decode_session_key(b"x") == "x"
    assert _decode_session_key(b"u") == "u"
    assert _decode_session_key(b"1") == "1"
    assert _decode_session_key(b"q") == "q"


def test_scrollable_renderer_clamps_offset_and_renders_multi_event_sections(tmp_path: Path) -> None:
    if not _RICH_AVAILABLE:
        return
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        payload = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_ok")
    lines = _build_card_document_lines(
        payload,
        view_mode="card",
        expanded_event_ids={"evt_1"},
        show_full_ids=False,
        candidates=None,
    )
    assert any("Matched Events" in line for line in lines)
    # Force tiny terminal size so scroll is required.
    console = Console(width=80, height=14, force_terminal=True, color_system="standard")
    renderable, clamped, max_scroll = _build_session_renderable(
        payload,
        queue_position="Game 1/1",
        scope="mapped_pending",
        filters_text="parse_status=ok",
        decision_progress={"n_pending": 1, "n_approved": 0, "n_rejected": 0, "n_skipped": 0},
        view_mode="card",
        scroll_offset=9999,
        expanded_event_ids={"evt_1"},
        show_full_ids=False,
        show_unselected_markets=False,
        last_action_note="",
        candidates=None,
        console=console,
    )
    assert renderable is not None
    assert max_scroll >= 0
    assert clamped == max_scroll


def test_compact_card_height_guard_single_event(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        payload = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_ok")
    lines = _build_card_document_lines(
        payload,
        view_mode="card",
        expanded_event_ids={"evt_1"},
        show_full_ids=False,
        candidates=None,
    )
    assert len(lines) <= 38


def test_compact_card_height_guard_multi_event(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data_multi_event(runtime=runtime)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        payload = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_multi_style")
    lines = _build_card_document_lines(
        payload,
        view_mode="card",
        expanded_event_ids={"evt_multi_a"},
        show_full_ids=False,
        candidates=None,
    )
    assert len(lines) <= 41


def test_compact_card_drilldown_regression_markets_mode_shows_summary_table(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        payload = review.get_game_card(provider="kalstrop_v1", run_id=run_id, provider_game_id="gid_ok")
    card_lines = _build_card_document_lines(
        payload,
        view_mode="card",
        expanded_event_ids=set(),
        show_full_ids=False,
        candidates=None,
    )
    market_lines = _build_card_document_lines(
        payload,
        view_mode="markets",
        expanded_event_ids={"evt_1"},
        show_full_ids=False,
        candidates=None,
    )
    card_strs = [str(l) for l in card_lines]
    market_strs = [str(l) for l in market_lines]
    assert any("Matched Events" in l for l in card_strs)
    assert not any("Matched Events" in l for l in market_strs)
    has_targets = bool(payload.get("card", {}).get("market_bindings", {}).get("targets"))
    if has_targets:
        assert any("Market Targets" in l for l in market_strs)


def test_market_type_normalization_and_inference() -> None:
    key, display, inferred = _normalize_market_type_info(
        sports_market_type="moneyline",
        market_question="",
        market_slug="",
    )
    assert (key, display, inferred) == ("moneyline", "MONEYLINE", False)

    key, display, inferred = _normalize_market_type_info(
        sports_market_type="nrfi",
        market_question="",
        market_slug="",
    )
    assert (key, display, inferred) == ("nrfi", "NRFI", False)

    key, display, inferred = _normalize_market_type_info(
        sports_market_type="",
        market_question="Team A vs Team B",
        market_slug="mlb-team-a-team-b",
    )
    assert key == "other"
    assert display == "OTHER (inf)"
    assert inferred is True


def test_market_sort_key_semantic_order_and_line_sort() -> None:
    markets = [
        {"market_type_key": "spread", "line": -1.5, "market_question": "Spread 1", "condition_id": "c5"},
        {"market_type_key": "totals", "line": 8.5, "market_question": "Totals 8.5", "condition_id": "c3"},
        {"market_type_key": "moneyline", "line": None, "market_question": "Moneyline", "condition_id": "c1"},
        {"market_type_key": "totals", "line": 7.5, "market_question": "Totals 7.5", "condition_id": "c2"},
        {"market_type_key": "nrfi", "line": None, "market_question": "NRFI", "condition_id": "c4"},
        {"market_type_key": "player_props", "line": None, "market_question": "Prop", "condition_id": "c6"},
    ]
    ordered = sorted(markets, key=_market_sort_key)
    assert [str(m.get("market_type_key") or "") for m in ordered] == [
        "moneyline",
        "totals",
        "totals",
        "nrfi",
        "spread",
        "player_props",
    ]
    assert [float(m.get("line")) for m in ordered if str(m.get("market_type_key") or "") == "totals"] == [7.5, 8.5]


def test_markets_view_uses_real_types_line_display_and_unselected_toggle() -> None:
    payload = {
        "found": True,
        "provider": "kalstrop_v1",
        "run_id": 1,
        "provider_game_id": "gid_demo",
        "card": {
            "provider_game": {
                "provider_game_id": "gid_demo",
                "game_date_et": "2026-04-18",
                "kickoff_ts_utc": 1_776_553_200,
                "away_raw": "A",
                "home_raw": "B",
                "parse_status": "ok",
            },
            "canonicalization": {
                "canonical_league": "mlb",
                "canonical_home_team": "team b",
                "canonical_away_team": "team a",
                "event_slug_prefix": "mlb-a-b-2026-04-18",
            },
            "event_resolution": {
                "resolution_state": "MATCHED_CLEAN",
                "reason_code": "",
                "selected_event_id": "evt_demo",
                "selected_event_status": "in_progress",
                "selected_events": [
                    {
                        "event_id": "evt_demo",
                        "event_title": "Team A vs Team B",
                        "event_slug": "mlb-a-b-2026-04-18",
                        "kickoff_ts_utc": 1_776_553_200,
                        "status": "in_progress",
                        "is_primary": True,
                    }
                ],
                "score_tuple": [1, 1, 0, 0],
                "kickoff_delta_sec": 0,
            },
            "market_bindings": {
                "markets": [
                    {
                        "event_id": "evt_demo",
                        "condition_id": "cond_moneyline",
                        "market_question": "Team A vs Team B",
                        "sports_market_type": "moneyline",
                        "display_market_type": "MONEYLINE",
                        "market_type_key": "moneyline",
                        "market_type_inferred": 0,
                        "line_display": "",
                        "line": None,
                        "binding_status": "exact",
                        "is_tradeable": True,
                        "outcomes": [
                            {"outcome_index": 0, "outcome_label": "Team A", "token_id": "tok_m_0", "is_tradeable": 1},
                            {"outcome_index": 1, "outcome_label": "Team B", "token_id": "tok_m_1", "is_tradeable": 1},
                        ],
                    },
                    {
                        "event_id": "evt_demo",
                        "condition_id": "cond_total_85",
                        "market_question": "Total Runs",
                        "sports_market_type": "totals",
                        "display_market_type": "TOTALS",
                        "market_type_key": "totals",
                        "market_type_inferred": 0,
                        "line_display": "O/U 8.5",
                        "line": 8.5,
                        "binding_status": "exact",
                        "is_tradeable": True,
                        "outcomes": [
                            {"outcome_index": 0, "outcome_label": "Over 8.5", "token_id": "tok_t_0", "is_tradeable": 1},
                            {"outcome_index": 1, "outcome_label": "Under 8.5", "token_id": "tok_t_1", "is_tradeable": 1},
                        ],
                    },
                    {
                        "event_id": "evt_demo",
                        "condition_id": "cond_total_75",
                        "market_question": "Total Runs",
                        "sports_market_type": "totals",
                        "display_market_type": "TOTALS",
                        "market_type_key": "totals",
                        "market_type_inferred": 0,
                        "line_display": "O/U 7.5",
                        "line": 7.5,
                        "binding_status": "exact",
                        "is_tradeable": True,
                        "outcomes": [
                            {"outcome_index": 0, "outcome_label": "Over 7.5", "token_id": "tok_t2_0", "is_tradeable": 1},
                            {"outcome_index": 1, "outcome_label": "Under 7.5", "token_id": "tok_t2_1", "is_tradeable": 1},
                        ],
                    },
                    {
                        "event_id": "evt_demo",
                        "condition_id": "cond_nrfi",
                        "market_question": "No runs first inning",
                        "sports_market_type": "nrfi",
                        "display_market_type": "NRFI",
                        "market_type_key": "nrfi",
                        "market_type_inferred": 0,
                        "line_display": "",
                        "line": None,
                        "binding_status": "exact",
                        "is_tradeable": True,
                        "outcomes": [
                            {"outcome_index": 0, "outcome_label": "Yes", "token_id": "tok_n_0", "is_tradeable": 1},
                            {"outcome_index": 1, "outcome_label": "No", "token_id": "tok_n_1", "is_tradeable": 1},
                        ],
                    },
                    {
                        "event_id": "evt_demo",
                        "condition_id": "cond_spread",
                        "market_question": "Run line",
                        "sports_market_type": "spread",
                        "display_market_type": "SPREAD",
                        "market_type_key": "spread",
                        "market_type_inferred": 0,
                        "line_display": "line -1.5",
                        "line": -1.5,
                        "binding_status": "exact",
                        "is_tradeable": True,
                        "outcomes": [
                            {"outcome_index": 0, "outcome_label": "Team A -1.5", "token_id": "tok_s_0", "is_tradeable": 1},
                            {"outcome_index": 1, "outcome_label": "Team B +1.5", "token_id": "tok_s_1", "is_tradeable": 1},
                        ],
                    },
                ],
                "unselected_markets": [
                    {
                        "event_id": "evt_demo",
                        "condition_id": "cond_unselected",
                        "market_question": "Alternate total",
                        "sports_market_type": "totals",
                        "display_market_type": "TOTALS",
                        "market_type_key": "totals",
                        "market_type_inferred": 0,
                        "line_display": "O/U 9.5",
                        "line": 9.5,
                        "binding_status": "not_selected",
                        "is_tradeable": False,
                        "is_selected": 0,
                        "outcomes": [],
                    }
                ],
                "targets": [
                    {"condition_id": "cond_moneyline", "sports_market_type": "moneyline", "is_tradeable": 1, "line": None},
                    {"condition_id": "cond_moneyline", "sports_market_type": "moneyline", "is_tradeable": 1, "line": None},
                    {"condition_id": "cond_total_85", "sports_market_type": "totals", "is_tradeable": 1, "line": 8.5},
                    {"condition_id": "cond_total_85", "sports_market_type": "totals", "is_tradeable": 1, "line": 8.5},
                    {"condition_id": "cond_total_75", "sports_market_type": "totals", "is_tradeable": 1, "line": 7.5},
                    {"condition_id": "cond_total_75", "sports_market_type": "totals", "is_tradeable": 1, "line": 7.5},
                    {"condition_id": "cond_nrfi", "sports_market_type": "nrfi", "is_tradeable": 1, "line": None},
                    {"condition_id": "cond_nrfi", "sports_market_type": "nrfi", "is_tradeable": 1, "line": None},
                    {"condition_id": "cond_spread", "sports_market_type": "spread", "is_tradeable": 1, "line": -1.5},
                    {"condition_id": "cond_spread", "sports_market_type": "spread", "is_tradeable": 1, "line": -1.5},
                ],
                "n_targets": 10,
                "n_tradeable_targets": 10,
                "n_selected_markets": 5,
                "n_total_markets": 6,
                "is_tradeable": True,
            },
            "notes": {"reason_notes": [], "trace": {}},
            "latest_decision": {"decision": "", "note": "", "actor": "", "decided_at": None},
        },
    }
    ordered_markets = sorted(
        list(payload["card"]["market_bindings"]["markets"]),
        key=_market_sort_key,
    )
    payload["card"]["market_bindings"]["markets"] = ordered_markets
    ordered_unselected = sorted(
        list(payload["card"]["market_bindings"]["unselected_markets"]),
        key=_market_sort_key,
    )
    payload["card"]["market_bindings"]["unselected_markets"] = ordered_unselected

    market_lines = _build_card_document_lines(
        payload,
        view_mode="markets",
        expanded_event_ids={"evt_demo"},
        show_full_ids=False,
        show_unselected_markets=False,
        candidates=None,
    )
    market_strs = [str(l) for l in market_lines]
    assert any("Market Targets" in l for l in market_strs)
    assert any("moneyline" in l for l in market_strs)
    assert any("totals" in l for l in market_strs)
    assert any("nrfi" in l for l in market_strs)
    assert any("spread" in l for l in market_strs)
    assert any("Total" in l for l in market_strs)


def test_game_state_derivation_prefers_status_then_kickoff_fallback() -> None:
    assert (
        _derive_game_state(
            provider_game={"kickoff_ts_utc": 2_000_000_000},
            event_resolution={"selected_event_status": "closed"},
            now_ts_utc=1_900_000_000,
        )
        == "FINAL"
    )
    assert (
        _derive_game_state(
            provider_game={"kickoff_ts_utc": 2_000_000_000},
            event_resolution={"selected_event_status": "in_progress"},
            now_ts_utc=1_900_000_000,
        )
        == "LIVE"
    )
    assert (
        _derive_game_state(
            provider_game={"kickoff_ts_utc": 2_000_000_000},
            event_resolution={"selected_event_status": "scheduled"},
            now_ts_utc=2_100_000_000,
        )
        == "NOT STARTED"
    )
    assert (
        _derive_game_state(
            provider_game={"kickoff_ts_utc": 2_000_000_000},
            event_resolution={},
            now_ts_utc=1_900_000_000,
        )
        == "NOT STARTED"
    )
    assert (
        _derive_game_state(
            provider_game={"kickoff_ts_utc": 2_000_000_000},
            event_resolution={},
            now_ts_utc=2_100_000_000,
        )
        == "LIVE"
    )
    assert _derive_game_state(provider_game={}, event_resolution={}, now_ts_utc=2_100_000_000) == "UNKNOWN"


def test_style_map_has_semantic_roles() -> None:
    assert "status_success" in _SESSION_STYLE_MAP
    assert "status_warn" in _SESSION_STYLE_MAP
    assert "status_error" in _SESSION_STYLE_MAP
    assert "section_title" in _SESSION_STYLE_MAP
    assert "selected_primary" in _SESSION_STYLE_MAP
    assert "label_dim" in _SESSION_STYLE_MAP
    assert "meta_dim" in _SESSION_STYLE_MAP
    assert "meta_accent" in _SESSION_STYLE_MAP


def test_kv_style_hierarchy_and_metadata_deemphasis() -> None:
    assert "green" in _kv_value_style("state", "MATCHED_CLEAN")
    assert "yellow" in _kv_value_style("reason", "fallback_used")
    assert "bold bright_white" == _kv_value_style("raw", "A @ B")
    assert "grey58" == _kv_value_style("slug", "mlb-phi-atl-2026-04-18")
    assert "bright_blue" == _kv_value_style("condition", "0xabc")
    assert "bright_magenta" in _kv_value_style("primary_event", "evt_1")


def test_styled_line_emphasizes_primary_and_resolution() -> None:
    primary_line = _styled_card_line("  raw=ATL Braves @ PHI Phillies  parse=ok")
    resolution_line = _styled_card_line("  state=MATCHED_CLEAN  reason=  selected_events=1  tradeable_targets=2/2")
    assert len(primary_line.spans) > 0
    assert len(resolution_line.spans) > 0
    assert any("bold bright_white" in str(span.style) for span in primary_line.spans if span.style is not None)
    assert any("green" in str(span.style) for span in resolution_line.spans if span.style is not None)


def test_link_review_cli_v2_session_smoke(tmp_path: Path, caplog, monkeypatch) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)
    parser = build_parser()
    logger = logging.getLogger("polybot2.test.link_review_cli_v2")
    caplog.set_level(logging.INFO, logger=logger.name)

    monkeypatch.setattr("builtins.input", lambda _prompt: "q")
    args = parser.parse_args(
        [
            "link",
            "review",
            "--db",
            runtime.db_path,
            "--run-id",
            str(run_id),
        ]
    )
    code = asyncio.run(dispatch(args, logger=logger))
    assert code == 0
