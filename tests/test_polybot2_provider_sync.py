from __future__ import annotations

from pathlib import Path

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.providers.sync import (
    _derive_start_ts_and_game_date_et,
    _parse_boltodds_when_et,
    _resolve_kalstrop_catalog_sport_codes,
)


def test_parse_boltodds_when_et() -> None:
    ts, date_et = _parse_boltodds_when_et("2026-04-18, 01:30 PM")
    assert ts is not None
    assert date_et == "2026-04-18"


def test_derive_start_ts_prefers_provider_parsed_timestamp() -> None:
    ts, date_et = _derive_start_ts_and_game_date_et(
        provider_start_ts_utc=1_776_624_000,
        when_raw="not-a-time",
    )
    assert ts == 1_776_624_000
    assert date_et == "2026-04-19"


def test_derive_start_ts_falls_back_to_raw_when_needed() -> None:
    ts, date_et = _derive_start_ts_and_game_date_et(
        provider_start_ts_utc=None,
        when_raw="2026-04-19T16:00:00Z",
    )
    assert ts is not None
    assert date_et == "2026-04-19"


def test_replace_provider_games_snapshot_prunes_removed_rows(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        db.linking.upsert_provider_games(
            [
                ("kalstrop", "gid_a", "", "", "", "", "", "", "", None, "", "", "", "ok", "", 100),
                ("kalstrop", "gid_b", "", "", "", "", "", "", "", None, "", "", "", "ok", "", 100),
            ]
        )
        db.linking.replace_provider_games_snapshot(
            provider="kalstrop",
            rows=[
                ("kalstrop", "gid_b", "", "", "", "", "", "", "", None, "", "", "", "ok", "", 200),
            ],
        )
        rows = db.linking.load_provider_games(provider="kalstrop")
    ids = sorted(str(r.get("provider_game_id") or "") for r in rows)
    assert ids == ["gid_b"]


def test_resolve_kalstrop_catalog_sport_codes_default() -> None:
    out = _resolve_kalstrop_catalog_sport_codes()
    assert out == ("baseball", "soccer")


def test_resolve_kalstrop_catalog_sport_codes_env_override(monkeypatch) -> None:
    monkeypatch.setenv("KALSTROP_CATALOG_SPORT_CODES", "tennis, soccer ,american-football")
    out = _resolve_kalstrop_catalog_sport_codes()
    assert out == ("american_football", "soccer", "tennis")
