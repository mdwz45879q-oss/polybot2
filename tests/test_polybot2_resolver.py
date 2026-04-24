from __future__ import annotations

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking.snapshot import BindingResolver


def test_binding_resolver_loads_tradeable_targets(tmp_path):
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        db.linking.upsert_game_bindings(
            [
                (
                    "boltodds",
                    "u1",
                    "mlb",
                    "arizona diamondbacks",
                    "atlanta braves",
                    "mlb-ari-atl-2026-04-18",
                    "exact",
                    "ok",
                    1,
                    "v1",
                    "h1",
                    None,
                    1,
                )
            ]
        )
        db.linking.upsert_market_bindings(
            [
                (
                    "boltodds",
                    "u1",
                    "c1",
                    0,
                    "t1",
                    "mlb-ari-atl-2026-04-18-ari",
                    "GAME",
                    "exact",
                    "ok",
                    1,
                    "v1",
                    "h1",
                    None,
                    1,
                )
            ]
        )
        db.linking.insert_link_run(
            provider="boltodds",
            league_scope="live",
            mapping_version="v1",
            mapping_hash="h1",
            n_games_seen=1,
            n_games_linked=1,
            n_games_tradeable=1,
            n_targets=1,
            n_targets_tradeable=1,
            gate_result="pass",
            report={},
            run_ts=1,
        )
        resolver = BindingResolver(db=db)
        resolver.reload()
        view = resolver.resolve_game_binding("boltodds", "u1")
        assert view is not None
        assert view.is_tradeable is True
        assert len(view.targets) == 1
