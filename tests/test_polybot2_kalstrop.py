from __future__ import annotations

import hashlib
import hmac

import httpx

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.providers import sync as sync_module
from polybot2.providers.sync import sync_provider_games
from polybot2.sports import ProviderGameRecord
from polybot2.sports.factory import resolve_kalstrop_credentials_from_env
from polybot2.sports.kalstrop_v1 import KalstropV1Provider, KalstropV1ProviderConfig


def test_kalstrop_signature_matches_spec() -> None:
    client_id = "abc"
    secret = "raw-shared-secret"
    timestamp = "1713566400"
    hashed_secret = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    expected = hmac.new(
        hashed_secret.encode("utf-8"),
        f"{client_id}:{timestamp}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    got = KalstropV1Provider.build_signature(
        client_id=client_id,
        shared_secret_raw=secret,
        timestamp=timestamp,
    )
    assert got == expected


def test_resolve_kalstrop_credentials_prefers_prefixed_env(monkeypatch) -> None:
    monkeypatch.setenv("KALSTROP_CLIENT_ID", "kid")
    monkeypatch.setenv("KALSTROP_SHARED_SECRET_RAW", "ksecret")
    monkeypatch.setenv("CLIENT_ID", "legacy_id")
    monkeypatch.setenv("SHARED_SECRET_RAW", "legacy_secret")

    client_id, shared_secret_raw, source = resolve_kalstrop_credentials_from_env()
    assert (client_id, shared_secret_raw, source) == ("kid", "ksecret", "kalstrop_prefixed")


def test_resolve_kalstrop_credentials_uses_legacy_fallback(monkeypatch) -> None:
    monkeypatch.delenv("KALSTROP_CLIENT_ID", raising=False)
    monkeypatch.delenv("KALSTROP_SHARED_SECRET_RAW", raising=False)
    monkeypatch.setenv("CLIENT_ID", "legacy_id")
    monkeypatch.setenv("SHARED_SECRET_RAW", "legacy_secret")

    client_id, shared_secret_raw, source = resolve_kalstrop_credentials_from_env()
    assert (client_id, shared_secret_raw, source) == ("legacy_id", "legacy_secret", "legacy_generic")


def test_kalstrop_catalog_handles_outer_and_inner_cursors() -> None:
    provider = KalstropV1Provider(
        config=KalstropV1ProviderConfig(
            client_id="cid",
            shared_secret_raw="secret",
            catalog_sport_codes=("baseball",),
            catalog_types=("live",),
            catalog_max_outer_pages=4,
            catalog_max_inner_pages=4,
        )
    )

    def _fake_get(endpoint: str, *, params: dict[str, object] | None = None):
        params = params or {}
        cursor = str(params.get("cursor") or "")
        if endpoint == "sports/baseball/live" and not cursor:
            return {
                "sportsCompetitions": {
                    "nextCursor": "outer_1",
                    "nodes": [
                        {
                            "name": "Major League Baseball",
                            "slug": "major-league-baseball",
                            "category": {"sports": "baseball"},
                            "fixtures": {
                                "nextCursor": "inner_1",
                                "nodes": [
                                    {
                                        "id": "fixture_1",
                                        "name": "Chicago White Sox vs Athletics",
                                        "startTime": "2026-04-18T23:10:00Z",
                                        "competitors": [
                                            {"displayName": "Chicago White Sox", "isHome": True},
                                            {"displayName": "Athletics", "isHome": False},
                                        ],
                                    }
                                ],
                            },
                        }
                    ],
                }
            }
        if endpoint == "sports/baseball/live" and cursor == "outer_1":
            return {"sportsCompetitions": {"nextCursor": "", "nodes": []}}
        if endpoint == "competition/major-league-baseball/fixtures" and cursor == "inner_1":
            return {
                "fixtures": {
                    "nextCursor": "",
                    "nodes": [
                        {
                            "id": "fixture_2",
                            "name": "Seattle Mariners vs Houston Astros",
                            "startTime": "2026-04-19T00:10:00Z",
                            "competitors": [
                                {"displayName": "Seattle Mariners", "isHome": True},
                                {"displayName": "Houston Astros", "isHome": False},
                            ],
                        }
                    ],
                }
            }
        raise AssertionError(f"unexpected request endpoint={endpoint} params={params}")

    provider._http_get_json = _fake_get  # type: ignore[method-assign]
    rows = provider.load_game_catalog()
    provider.close()

    assert {r.provider_game_id for r in rows} == {"fixture_1", "fixture_2"}
    fixture_1 = [r for r in rows if r.provider_game_id == "fixture_1"][0]
    assert fixture_1.home_team_raw == "Chicago White Sox"
    assert fixture_1.away_team_raw == "Athletics"
    assert fixture_1.parse_status == "ok"


def test_kalstrop_catalog_supports_sports_fixtures_upcoming_shape() -> None:
    provider = KalstropV1Provider(
        config=KalstropV1ProviderConfig(
            client_id="cid",
            shared_secret_raw="secret",
            catalog_sport_codes=("soccer",),
            catalog_types=("upcoming",),
            catalog_max_outer_pages=4,
            catalog_max_inner_pages=4,
        )
    )

    def _fake_get(endpoint: str, *, params: dict[str, object] | None = None):
        params = params or {}
        cursor = str(params.get("cursor") or "")
        if endpoint == "sports/soccer/upcoming" and not cursor:
            return {
                "sportsFixtures": {
                    "nodes": [
                        {
                            "id": "fixture_a",
                            "name": "Yupanqui vs CA Puerto Nuevo",
                            "startTime": "2026-04-19T16:00:00.000Z",
                            "competitors": [
                                {"displayName": "CA Puerto Nuevo", "isHome": False},
                                {"displayName": "Yupanqui", "isHome": True},
                            ],
                            "competition": {
                                "name": "Primera C",
                                "slug": "primera-c",
                                "category": {"sports": "soccer"},
                            },
                        }
                    ],
                    "nextCursor": "next_1",
                }
            }
        if endpoint == "sports/soccer/upcoming" and cursor == "next_1":
            return {
                "sportsFixtures": {
                    "nodes": [
                        {
                            "id": "fixture_b",
                            "name": "Team One vs Team Two",
                            "startTime": "2026-04-19T20:00:00.000Z",
                            "competitors": [
                                {"displayName": "Team One", "isHome": True},
                                {"displayName": "Team Two", "isHome": False},
                            ],
                            "competition": {
                                "name": "Primera C",
                                "slug": "primera-c",
                                "category": {"sports": "soccer"},
                            },
                        }
                    ],
                    "nextCursor": "",
                }
            }
        raise AssertionError(f"unexpected request endpoint={endpoint} params={params}")

    provider._http_get_json = _fake_get  # type: ignore[method-assign]
    rows = provider.load_game_catalog()
    provider.close()

    assert {r.provider_game_id for r in rows} == {"fixture_a", "fixture_b"}
    a = [r for r in rows if r.provider_game_id == "fixture_a"][0]
    assert a.home_team_raw == "Yupanqui"
    assert a.away_team_raw == "CA Puerto Nuevo"


def test_kalstrop_fetch_sports_page_upcoming_omits_fixture_first_on_primary_request() -> None:
    provider = KalstropV1Provider(
        config=KalstropV1ProviderConfig(
            client_id="cid",
            shared_secret_raw="secret",
            catalog_sport_codes=("baseball",),
            catalog_types=("upcoming",),
        )
    )
    calls: list[tuple[str, dict[str, object]]] = []

    def _fake_get(endpoint: str, *, params: dict[str, object] | None = None):
        query = dict(params or {})
        calls.append((endpoint, query))
        return {"sportsFixtures": {"nodes": [], "nextCursor": ""}}

    provider._http_get_json = _fake_get  # type: ignore[method-assign]
    payload = provider._fetch_sports_page(sport_code="baseball", feed_type="upcoming")
    provider.close()

    assert isinstance(payload, dict)
    assert len(calls) == 1
    assert "fixtureFirst" not in calls[0][1]


def test_kalstrop_fetch_sports_page_retries_without_fixture_first_on_bad_request() -> None:
    provider = KalstropV1Provider(
        config=KalstropV1ProviderConfig(
            client_id="cid",
            shared_secret_raw="secret",
            catalog_sport_codes=("baseball",),
            catalog_types=("live",),
        )
    )
    calls: list[tuple[str, dict[str, object]]] = []

    def _fake_get(endpoint: str, *, params: dict[str, object] | None = None):
        query = dict(params or {})
        calls.append((endpoint, query))
        if "fixtureFirst" in query:
            req = httpx.Request("GET", "https://example.test/sports/baseball/live")
            resp = httpx.Response(400, request=req, text='{"detail":"invalid query"}')
            raise httpx.HTTPStatusError("bad request", request=req, response=resp)
        return {"sportsCompetitions": {"nodes": [], "nextCursor": ""}}

    provider._http_get_json = _fake_get  # type: ignore[method-assign]
    payload = provider._fetch_sports_page(sport_code="baseball", feed_type="live")
    provider.close()

    assert isinstance(payload, dict)
    assert len(calls) >= 2
    first_params = calls[0][1]
    second_params = calls[1][1]
    assert "fixtureFirst" in first_params
    assert "fixtureFirst" not in second_params


def test_kalstrop_catalog_skips_unsupported_sport_branch() -> None:
    provider = KalstropV1Provider(
        config=KalstropV1ProviderConfig(
            client_id="cid",
            shared_secret_raw="secret",
            catalog_sport_codes=("unsupported_sport", "baseball"),
            catalog_types=("live",),
            catalog_max_outer_pages=2,
            catalog_max_inner_pages=2,
        )
    )

    def _fake_get(endpoint: str, *, params: dict[str, object] | None = None):
        params = params or {}
        cursor = str(params.get("cursor") or "")
        if endpoint == "sports/unsupported_sport/live" and not cursor:
            req = httpx.Request("GET", "https://example.test/sports/unsupported_sport/live")
            resp = httpx.Response(400, request=req, text='{"detail":"Invalid sport"}')
            raise httpx.HTTPStatusError("invalid sport", request=req, response=resp)
        if endpoint == "sports/baseball/live" and not cursor:
            return {
                "sportsFixtures": {
                    "nodes": [
                        {
                            "id": "fixture_ok",
                            "name": "Seattle Mariners vs Houston Astros",
                            "startTime": "2026-04-19T00:10:00.000Z",
                            "competitors": [
                                {"displayName": "Seattle Mariners", "isHome": True},
                                {"displayName": "Houston Astros", "isHome": False},
                            ],
                            "competition": {
                                "name": "Major League Baseball",
                                "slug": "major-league-baseball",
                                "category": {"sports": "baseball"},
                            },
                        }
                    ],
                    "nextCursor": "",
                }
            }
        raise AssertionError(f"unexpected request endpoint={endpoint} params={params}")

    provider._http_get_json = _fake_get  # type: ignore[method-assign]
    rows = provider.load_game_catalog()
    provider.close()

    assert [r.provider_game_id for r in rows] == ["fixture_ok"]



def test_sync_provider_games_kalstrop_inserts_rows(tmp_path, monkeypatch) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    seen_config = {}

    class _FakeMap:
        leagues = {
            "mlb": {"sport_family": "baseball"},
            "bundesliga": {"sport_family": "soccer"},
        }

    class _FakeKalstropProvider:
        def __init__(self, *, config):
            self.config = config
            seen_config["catalog_sport_codes"] = tuple(getattr(config, "catalog_sport_codes", ()))

        def load_game_catalog(self):
            return [
                ProviderGameRecord(
                    provider="kalstrop",
                    provider_game_id="fixture_123",
                    game_label="Hoffenheim vs Dortmund",
                    sport_raw="soccer",
                    league_raw="bundesliga",
                    when_raw="2026-04-18T19:30:00Z",
                    home_team_raw="Hoffenheim",
                    away_team_raw="Dortmund",
                    start_ts_utc=1776540600,
                    parse_status="ok",
                )
            ]

        def close(self):
            return None

    monkeypatch.setattr(sync_module, "resolve_kalstrop_credentials_from_env", lambda: ("cid", "secret", "kalstrop_prefixed"))
    monkeypatch.setattr(sync_module, "KalstropV1Provider", _FakeKalstropProvider)

    with open_database(runtime) as db:
        res = sync_provider_games(db=db, provider="kalstrop_v1")
        row = db.execute(
            """
            SELECT provider, provider_game_id, league_raw, home_raw, away_raw, parse_status
            FROM provider_games
            WHERE provider='kalstrop_v1' AND provider_game_id='fixture_123'
            """
        ).fetchone()

    assert res.status == "ok"
    assert int(res.n_rows) == 1
    assert row is not None
    assert row["provider"] == "kalstrop_v1"
    assert row["league_raw"] == "bundesliga"
    assert row["home_raw"] == "Hoffenheim"
    assert row["away_raw"] == "Dortmund"
    assert row["parse_status"] == "ok"
    assert tuple(seen_config.get("catalog_sport_codes") or ()) == ("baseball", "soccer")


def test_sync_provider_games_kalstrop_missing_credentials_returns_error(tmp_path, monkeypatch) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    monkeypatch.setattr(sync_module, "resolve_kalstrop_credentials_from_env", lambda: ("", "", ""))

    with open_database(runtime) as db:
        res = sync_provider_games(db=db, provider="kalstrop")

    assert res.status == "error"
    assert res.reason == "missing_kalstrop_credentials"
