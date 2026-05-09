from __future__ import annotations

from pathlib import Path

import pytest

from polybot2.linking.mapping_loader import MappingValidationError, load_live_trading_policy, load_mapping


def test_load_mapping_repo_file() -> None:
    loaded = load_mapping()
    assert loaded.mapping_version
    assert loaded.mapping_hash
    assert "mlb" in loaded.leagues


def test_load_live_trading_repo_file() -> None:
    loaded = load_live_trading_policy()
    assert loaded.policy_version
    assert loaded.policy_hash
    assert loaded.default_provider == "kalstrop_v1"
    assert "mlb" in loaded.live_betting_leagues
    assert "mlb" in loaded.live_betting_market_types_by_league
    assert "moneyline" in loaded.live_betting_market_types_by_league["mlb"]
    assert "moneyline" in loaded.live_betting_market_types
    assert "mlb" in loaded.hotpath_execution_by_league
    assert float(loaded.hotpath_execution_by_league["mlb"]["amount_usdc"]) > 0
    assert "mlb" in loaded.hotpath_runtime_by_league
    assert int(loaded.hotpath_runtime_by_league["mlb"]["plan_horizon_hours"]) > 0
    assert int(loaded.hotpath_runtime_by_league["mlb"]["reload_interval_seconds"]) > 0
    assert int(loaded.hotpath_runtime_by_league["mlb"]["provider_catalog_max_age_seconds"]) > 0


def test_mapping_conflicting_alias_fails(tmp_path: Path) -> None:
    mapping_file = tmp_path / "bad_mapping.py"
    mapping_file.write_text(
        "\n".join(
            [
                "MAPPING_VERSION='v1'",
                "LEAGUES={'mlb': {'polymarket_league_code': 'mlb', 'sport_family': 'baseball'}}",
                "PROVIDER_LEAGUE_ALIASES={'boltodds': {'mlb': 'mlb'}}",
                "TEAM_MAP={'mlb': {",
                "  'team a': {'polymarket_code': 'aaa', 'provider_aliases': {'boltodds': ['same alias']}},",
                "  'team b': {'polymarket_code': 'bbb', 'provider_aliases': {'boltodds': ['same alias']}},",
                "}}",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(MappingValidationError):
        load_mapping(str(mapping_file))


def test_live_trading_runtime_policy_validation_fails_on_invalid_values(tmp_path: Path) -> None:
    policy_file = tmp_path / "bad_live_trading.py"
    policy_file.write_text(
        "\n".join(
            [
                "LIVE_TRADING_VERSION='v1'",
                "LIVE_BETTING_LEAGUES={'mlb'}",
                "LIVE_BETTING_MARKET_TYPES={'mlb': ['totals']}",
                "HOTPATH_EXECUTION_POLICY={'mlb': {'amount_usdc': 5.0, 'limit_price': 0.52}}",
                "HOTPATH_RUNTIME_POLICY={'mlb': {'plan_horizon_hours': 0, 'subscribe_lead_minutes': 90, 'unsubscribe_grace_minutes': 15, 'reload_interval_seconds': 120}}",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(MappingValidationError):
        load_live_trading_policy(str(policy_file))


def test_live_trading_execution_policy_rejects_legacy_keys(tmp_path: Path) -> None:
    policy_file = tmp_path / "bad_execution_policy_legacy_keys.py"
    policy_file.write_text(
        "\n".join(
            [
                "LIVE_TRADING_VERSION='v1'",
                "LIVE_BETTING_LEAGUES={'mlb'}",
                "LIVE_BETTING_MARKET_TYPES={'mlb': ['totals']}",
                "HOTPATH_EXECUTION_POLICY={'mlb': {'amount_usdc': 5.0, 'buy_yes_limit_price': 0.52}}",
                "HOTPATH_RUNTIME_POLICY={'mlb': {'plan_horizon_hours': 24, 'subscribe_lead_minutes': 90, 'unsubscribe_grace_minutes': 15, 'reload_interval_seconds': 120, 'provider_catalog_max_age_seconds': 600}}",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(MappingValidationError, match="removed"):
        load_live_trading_policy(str(policy_file))


def test_live_trading_default_provider_validation_fails_on_invalid_value(tmp_path: Path) -> None:
    policy_file = tmp_path / "bad_default_provider.py"
    policy_file.write_text(
        "\n".join(
            [
                "LIVE_TRADING_VERSION='v1'",
                "DEFAULT_PROVIDER='not_real'",
                "LIVE_BETTING_LEAGUES={'mlb'}",
                "LIVE_BETTING_MARKET_TYPES={'mlb': ['totals']}",
                "HOTPATH_RUNTIME_POLICY={'mlb': {'plan_horizon_hours': 24, 'subscribe_lead_minutes': 90, 'unsubscribe_grace_minutes': 15, 'reload_interval_seconds': 120}}",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(MappingValidationError, match="DEFAULT_PROVIDER"):
        load_live_trading_policy(str(policy_file))


def test_live_trading_default_provider_backfills_to_kalstrop_when_missing(tmp_path: Path) -> None:
    policy_file = tmp_path / "no_default_provider.py"
    policy_file.write_text(
        "\n".join(
            [
                "LIVE_TRADING_VERSION='v1'",
                "LIVE_BETTING_LEAGUES={'mlb'}",
                "LIVE_BETTING_MARKET_TYPES={'mlb': ['totals']}",
                "HOTPATH_RUNTIME_POLICY={'mlb': {'plan_horizon_hours': 24, 'subscribe_lead_minutes': 90, 'unsubscribe_grace_minutes': 15, 'reload_interval_seconds': 120}}",
            ]
        ),
        encoding="utf-8",
    )
    loaded = load_live_trading_policy(str(policy_file))
    assert loaded.default_provider == "kalstrop_v1"
