"""Mapping and policy loaders for deterministic linking."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


@dataclass(frozen=True)
class LoadedMapping:
    path: str
    mapping_version: str
    mapping_hash: str
    leagues: dict[str, Any]
    provider_league_aliases: dict[str, Any]
    provider_league_country: dict[str, Any]
    team_map: dict[str, Any]
    pm_league_orderings: dict[str, str]
    market_mappings: dict[str, Any]
    league_match_rules: dict[str, Any]


@dataclass(frozen=True)
class LoadedLiveTradingPolicy:
    path: str
    policy_version: str
    policy_hash: str
    default_provider: str
    live_betting_leagues: set[str]
    live_betting_market_types_by_league: dict[str, set[str]]
    live_betting_market_types: set[str]
    hotpath_execution_by_league: dict[str, dict[str, Any]] = field(default_factory=dict)
    hotpath_runtime_by_league: dict[str, dict[str, int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "hotpath_execution_by_league", dict(self.hotpath_execution_by_league or {}))
        object.__setattr__(self, "hotpath_runtime_by_league", dict(self.hotpath_runtime_by_league or {}))


class MappingValidationError(ValueError):
    pass


_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"
_DEFAULT_MAPPING_FILE = _CONFIG_DIR / "mappings.py"
_DEFAULT_LIVE_TRADING_FILE = _CONFIG_DIR / "live_trading.py"


def default_mapping_file() -> str:
    return str(_DEFAULT_MAPPING_FILE)


def default_live_trading_file() -> str:
    return str(_DEFAULT_LIVE_TRADING_FILE)


def _load_module(*, path_like: str, module_name: str) -> tuple[Any, str]:
    path = Path(str(path_like)).expanduser().resolve()
    if not path.exists():
        raise MappingValidationError(f"config file not found: {path}")
    # Add the config file's directory to sys.path so sibling imports
    # (e.g., `from baseball_mappings import ...` in mappings.py) resolve.
    config_dir = str(path.parent)
    if config_dir not in sys.path:
        sys.path.insert(0, config_dir)
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise MappingValidationError(f"unable to load config module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return (module, str(path))


def load_mapping(mapping_file: str | None = None) -> LoadedMapping:
    module, path = _load_module(
        path_like=str(mapping_file or default_mapping_file()),
        module_name="polybot2_mapping_module",
    )

    mapping_version = str(getattr(module, "MAPPING_VERSION", "")).strip()
    if not mapping_version:
        raise MappingValidationError("MAPPING_VERSION must be non-empty")

    leagues = getattr(module, "LEAGUES", None)
    provider_league_aliases = getattr(module, "PROVIDER_LEAGUE_ALIASES", None)
    provider_league_country = getattr(module, "PROVIDER_LEAGUE_COUNTRY", None)
    team_map = getattr(module, "TEAM_MAP", None)
    pm_league_orderings = getattr(module, "PM_LEAGUE_ORDERINGS", None)
    market_mappings = getattr(module, "MARKET_MAPPINGS", None)
    league_match_rules = getattr(module, "LEAGUE_MATCH_RULES", None)

    if not isinstance(leagues, dict) or not leagues:
        raise MappingValidationError("LEAGUES must be a non-empty dict")
    if not isinstance(provider_league_aliases, dict):
        raise MappingValidationError("PROVIDER_LEAGUE_ALIASES must be a dict")
    if provider_league_country is None:
        provider_league_country = {}
    if not isinstance(provider_league_country, dict):
        raise MappingValidationError("PROVIDER_LEAGUE_COUNTRY must be a dict")
    if not isinstance(team_map, dict):
        raise MappingValidationError("TEAM_MAP must be a dict")
    if pm_league_orderings is None:
        pm_league_orderings = {}
    if market_mappings is None:
        market_mappings = {}
    if league_match_rules is None:
        league_match_rules = {}
    if not isinstance(pm_league_orderings, dict):
        raise MappingValidationError("PM_LEAGUE_ORDERINGS must be a dict")
    if not isinstance(market_mappings, dict):
        raise MappingValidationError("MARKET_MAPPINGS must be a dict")
    if not isinstance(league_match_rules, dict):
        raise MappingValidationError("LEAGUE_MATCH_RULES must be a dict")

    norm_orderings = {_norm(str(k)): _norm(str(v)) for k, v in pm_league_orderings.items() if _norm(str(k))}
    norm_market_mappings = {_norm(str(k)): v for k, v in market_mappings.items() if _norm(str(k))}
    norm_match_rules = {_norm(str(k)): v for k, v in league_match_rules.items() if _norm(str(k))}

    canonical_repr = {
        "MAPPING_VERSION": mapping_version,
        "LEAGUES": leagues,
        "PROVIDER_LEAGUE_ALIASES": provider_league_aliases,
        "PROVIDER_LEAGUE_COUNTRY": provider_league_country,
        "TEAM_MAP": team_map,
        "PM_LEAGUE_ORDERINGS": norm_orderings,
        "MARKET_MAPPINGS": norm_market_mappings,
        "LEAGUE_MATCH_RULES": norm_match_rules,
    }
    mapping_hash = hashlib.sha256(
        json.dumps(canonical_repr, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()

    loaded = LoadedMapping(
        path=str(path),
        mapping_version=mapping_version,
        mapping_hash=mapping_hash,
        leagues=leagues,
        provider_league_aliases=provider_league_aliases,
        provider_league_country=provider_league_country,
        team_map=team_map,
        pm_league_orderings=norm_orderings,
        market_mappings=norm_market_mappings,
        league_match_rules=norm_match_rules,
    )
    validate_loaded_mapping(loaded)
    return loaded


def load_live_trading_policy(policy_file: str | None = None) -> LoadedLiveTradingPolicy:
    module, path = _load_module(
        path_like=str(policy_file or default_live_trading_file()),
        module_name="polybot2_live_trading_module",
    )
    policy_version = str(
        getattr(module, "LIVE_TRADING_VERSION", "") or getattr(module, "POLICY_VERSION", "") or "v1"
    ).strip()
    if not policy_version:
        raise MappingValidationError("LIVE_TRADING_VERSION must be non-empty")
    default_provider = _norm(str(getattr(module, "DEFAULT_PROVIDER", "") or ""))
    if not default_provider:
        default_provider = "kalstrop_v1"

    live_betting_leagues = getattr(module, "LIVE_BETTING_LEAGUES", None)
    if not isinstance(live_betting_leagues, (set, list, tuple)):
        raise MappingValidationError("LIVE_BETTING_LEAGUES must be a set/list/tuple")
    live_leagues = {_norm(x) for x in live_betting_leagues if _norm(str(x))}

    live_betting_market_types = getattr(module, "LIVE_BETTING_MARKET_TYPES", None)
    if not isinstance(live_betting_market_types, dict):
        raise MappingValidationError("LIVE_BETTING_MARKET_TYPES must be a dict: <league> -> [market_type,...]")
    live_market_types_by_league: dict[str, set[str]] = {}
    live_market_types: set[str] = set()
    for league, values in live_betting_market_types.items():
        lk = _norm(str(league))
        if not lk:
            continue
        if not isinstance(values, (set, list, tuple)):
            raise MappingValidationError(
                f"LIVE_BETTING_MARKET_TYPES[{league!r}] must be set/list/tuple of market types"
            )
        norm_values = {_norm(x) for x in values if _norm(str(x))}
        live_market_types_by_league[lk] = norm_values
        live_market_types.update(norm_values)

    hotpath_execution_policy = getattr(module, "HOTPATH_EXECUTION_POLICY", None)
    if hotpath_execution_policy is None:
        hotpath_execution_policy = {}
    if not isinstance(hotpath_execution_policy, dict):
        raise MappingValidationError("HOTPATH_EXECUTION_POLICY must be a dict: <league> -> {...}")
    hotpath_execution_by_league: dict[str, dict[str, Any]] = {}
    for league, cfg in hotpath_execution_policy.items():
        lk = _norm(str(league))
        if not lk:
            continue
        if not isinstance(cfg, dict):
            raise MappingValidationError(f"HOTPATH_EXECUTION_POLICY[{league!r}] must be dict")
        hotpath_execution_by_league[lk] = dict(cfg)

    hotpath_runtime_policy = getattr(module, "HOTPATH_RUNTIME_POLICY", None)
    if hotpath_runtime_policy is None:
        hotpath_runtime_policy = {}
    if not isinstance(hotpath_runtime_policy, dict):
        raise MappingValidationError("HOTPATH_RUNTIME_POLICY must be a dict: <league> -> {...}")
    hotpath_runtime_by_league: dict[str, dict[str, int]] = {}
    for league, cfg in hotpath_runtime_policy.items():
        lk = _norm(str(league))
        if not lk:
            continue
        if not isinstance(cfg, dict):
            raise MappingValidationError(f"HOTPATH_RUNTIME_POLICY[{league!r}] must be dict")
        hotpath_runtime_by_league[lk] = {
            "plan_horizon_hours": int(cfg.get("plan_horizon_hours", 24)),
            "subscribe_lead_minutes": int(cfg.get("subscribe_lead_minutes", 90)),
            "reload_interval_seconds": int(cfg.get("reload_interval_seconds", 120)),
            "provider_catalog_max_age_seconds": int(cfg.get("provider_catalog_max_age_seconds", 600)),
            "refresh_interval_seconds": int(cfg.get("refresh_interval_seconds", 300)),
        }

    canonical_repr = {
        "LIVE_TRADING_VERSION": policy_version,
        "DEFAULT_PROVIDER": default_provider,
        "LIVE_BETTING_LEAGUES": sorted(live_leagues),
        "LIVE_BETTING_MARKET_TYPES_BY_LEAGUE": {
            k: sorted(v) for k, v in sorted(live_market_types_by_league.items(), key=lambda x: x[0])
        },
        "LIVE_BETTING_MARKET_TYPES": sorted(live_market_types),
        "HOTPATH_EXECUTION_POLICY_BY_LEAGUE": {
            k: dict(v) for k, v in sorted(hotpath_execution_by_league.items(), key=lambda x: x[0])
        },
        "HOTPATH_RUNTIME_POLICY_BY_LEAGUE": {
            k: dict(v) for k, v in sorted(hotpath_runtime_by_league.items(), key=lambda x: x[0])
        },
    }
    policy_hash = hashlib.sha256(
        json.dumps(canonical_repr, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()

    loaded = LoadedLiveTradingPolicy(
        path=path,
        policy_version=policy_version,
        policy_hash=policy_hash,
        default_provider=default_provider,
        live_betting_leagues=live_leagues,
        live_betting_market_types_by_league=live_market_types_by_league,
        live_betting_market_types=live_market_types,
        hotpath_execution_by_league=hotpath_execution_by_league,
        hotpath_runtime_by_league=hotpath_runtime_by_league,
    )
    validate_loaded_live_trading_policy(loaded)
    return loaded


def validate_loaded_mapping(mapping: LoadedMapping) -> None:
    leagues = mapping.leagues
    provider_aliases = mapping.provider_league_aliases
    team_map = mapping.team_map
    pm_league_orderings = mapping.pm_league_orderings
    market_mappings = mapping.market_mappings
    league_match_rules = mapping.league_match_rules

    for league, meta in leagues.items():
        lk = _norm(league)
        if lk != str(league):
            raise MappingValidationError(f"league key must be normalized: {league!r}")
        if not isinstance(meta, dict):
            raise MappingValidationError(f"LEAGUES[{league!r}] must be dict")
        code = _norm(str(meta.get("polymarket_league_code") or ""))
        sport = _norm(str(meta.get("sport_family") or ""))
        if not code:
            raise MappingValidationError(f"LEAGUES[{league!r}] missing polymarket_league_code")
        if not sport:
            raise MappingValidationError(f"LEAGUES[{league!r}] missing sport_family")

    for provider, pmap in provider_aliases.items():
        if not isinstance(pmap, dict):
            raise MappingValidationError(f"PROVIDER_LEAGUE_ALIASES[{provider!r}] must be dict")
        seen_alias: set[str] = set()
        for alias, league in pmap.items():
            an = _norm(alias)
            if an != str(alias):
                raise MappingValidationError(f"provider league alias must be normalized: {provider}:{alias!r}")
            if an in seen_alias:
                raise MappingValidationError(f"duplicate provider alias: {provider}:{alias!r}")
            seen_alias.add(an)
            if _norm(str(league)) not in leagues:
                raise MappingValidationError(f"provider alias points to unknown league: {provider}:{alias!r}->{league!r}")

    for league, teams in team_map.items():
        lk = _norm(league)
        if lk not in leagues:
            raise MappingValidationError(f"TEAM_MAP league not in LEAGUES: {league!r}")
        if not isinstance(teams, dict):
            raise MappingValidationError(f"TEAM_MAP[{league!r}] must be dict")

        provider_alias_owners: dict[tuple[str, str], str] = {}
        for canonical_team, tmeta in teams.items():
            ct = _norm(canonical_team)
            if ct != str(canonical_team):
                raise MappingValidationError(f"team key must be normalized: {league}:{canonical_team!r}")
            if not isinstance(tmeta, dict):
                raise MappingValidationError(f"TEAM_MAP[{league!r}][{canonical_team!r}] must be dict")
            pm_code = _norm(str(tmeta.get("polymarket_code") or ""))
            if not pm_code:
                raise MappingValidationError(f"TEAM_MAP[{league!r}][{canonical_team!r}] missing polymarket_code")
            pa = tmeta.get("provider_aliases")
            if not isinstance(pa, dict):
                raise MappingValidationError(
                    f"TEAM_MAP[{league!r}][{canonical_team!r}].provider_aliases must be dict"
                )
            for provider, aliases in pa.items():
                if not isinstance(aliases, list):
                    raise MappingValidationError(
                        f"provider_aliases list expected at {league}:{canonical_team}:{provider}"
                    )
                for alias in aliases:
                    an = _norm(alias)
                    if not an:
                        continue
                    key = (_norm(provider), an)
                    owner = provider_alias_owners.get(key)
                    if owner is None:
                        provider_alias_owners[key] = ct
                    elif owner != ct:
                        raise MappingValidationError(
                            f"conflicting team/provider alias {provider}:{an!r} owned by both {owner!r} and {ct!r} in league {league!r}"
                        )

    for league, ordering in pm_league_orderings.items():
        lk = _norm(league)
        if lk not in leagues:
            raise MappingValidationError(f"PM_LEAGUE_ORDERINGS league not in LEAGUES: {league!r}")
        ov = _norm(ordering)
        if ov not in {"home", "away"}:
            raise MappingValidationError(f"PM_LEAGUE_ORDERINGS[{league!r}] must be home|away")

    for league, cfg in market_mappings.items():
        lk = _norm(league)
        if lk not in leagues:
            raise MappingValidationError(f"MARKET_MAPPINGS league not in LEAGUES: {league!r}")
        if not isinstance(cfg, dict):
            raise MappingValidationError(f"MARKET_MAPPINGS[{league!r}] must be dict")

    for league, cfg in league_match_rules.items():
        lk = _norm(league)
        if lk == "default":
            pass
        elif lk not in leagues:
            raise MappingValidationError(f"LEAGUE_MATCH_RULES league not in LEAGUES: {league!r}")
        if not isinstance(cfg, dict):
            raise MappingValidationError(f"LEAGUE_MATCH_RULES[{league!r}] must be dict")
        if "date_tolerance_days" in cfg:
            try:
                dv = int(cfg.get("date_tolerance_days"))
            except (TypeError, ValueError):
                raise MappingValidationError(f"LEAGUE_MATCH_RULES[{league!r}].date_tolerance_days must be int") from None
            if dv < 0:
                raise MappingValidationError(f"LEAGUE_MATCH_RULES[{league!r}].date_tolerance_days must be >= 0")
        if "kickoff_tolerance_minutes" in cfg:
            try:
                kv = int(cfg.get("kickoff_tolerance_minutes"))
            except (TypeError, ValueError):
                raise MappingValidationError(
                    f"LEAGUE_MATCH_RULES[{league!r}].kickoff_tolerance_minutes must be int"
                ) from None
            if kv < 0:
                raise MappingValidationError(f"LEAGUE_MATCH_RULES[{league!r}].kickoff_tolerance_minutes must be >= 0")
        for bk in ("provider_order_reliable", "pm_order_reliable"):
            if bk in cfg and not isinstance(cfg.get(bk), bool):
                raise MappingValidationError(f"LEAGUE_MATCH_RULES[{league!r}].{bk} must be bool")


def validate_loaded_live_trading_policy(policy: LoadedLiveTradingPolicy) -> None:
    if str(policy.default_provider) not in {"boltodds", "kalstrop", "kalstrop_v1", "kalstrop_v2"}:
        raise MappingValidationError("DEFAULT_PROVIDER must be one of {'boltodds','kalstrop_v1','kalstrop_v2'}")
    if not policy.live_betting_leagues:
        raise MappingValidationError("LIVE_BETTING_LEAGUES must not be empty")
    if not policy.live_betting_market_types_by_league:
        raise MappingValidationError("LIVE_BETTING_MARKET_TYPES must not be empty")
    for league, market_types in policy.live_betting_market_types_by_league.items():
        lk = _norm(league)
        if lk != str(league):
            raise MappingValidationError(f"LIVE_BETTING_MARKET_TYPES league key must be normalized: {league!r}")
        if not market_types:
            raise MappingValidationError(f"LIVE_BETTING_MARKET_TYPES[{league!r}] must not be empty")
    missing = sorted(x for x in policy.live_betting_leagues if x not in policy.live_betting_market_types_by_league)
    if missing:
        raise MappingValidationError(
            f"LIVE_BETTING_MARKET_TYPES missing league keys for LIVE_BETTING_LEAGUES: {','.join(missing)}"
        )
    if not policy.live_betting_market_types:
        raise MappingValidationError("LIVE_BETTING_MARKET_TYPES must not be empty")
    for league, cfg in policy.hotpath_execution_by_league.items():
        lk = _norm(str(league))
        if lk != str(league):
            raise MappingValidationError(f"HOTPATH_EXECUTION_POLICY league key must be normalized: {league!r}")
        if not isinstance(cfg, dict):
            raise MappingValidationError(f"HOTPATH_EXECUTION_POLICY[{league!r}] must be dict")
        if "buy_yes_limit_price" in cfg:
            raise MappingValidationError(
                f"HOTPATH_EXECUTION_POLICY[{league!r}].buy_yes_limit_price is removed; use limit_price"
            )
        if "amount_usdc" in cfg:
            try:
                nv = float(cfg.get("amount_usdc"))
            except (TypeError, ValueError):
                raise MappingValidationError(
                    f"HOTPATH_EXECUTION_POLICY[{league!r}].amount_usdc must be float"
                ) from None
            if nv <= 0:
                raise MappingValidationError(f"HOTPATH_EXECUTION_POLICY[{league!r}].amount_usdc must be > 0")
        if "limit_price" in cfg:
            try:
                pv = float(cfg.get("limit_price"))
            except (TypeError, ValueError):
                raise MappingValidationError(
                    f"HOTPATH_EXECUTION_POLICY[{league!r}].limit_price must be float"
                ) from None
            if pv <= 0.0 or pv >= 1.0:
                raise MappingValidationError(
                    f"HOTPATH_EXECUTION_POLICY[{league!r}].limit_price must be in (0,1)"
                )
        if "time_in_force" in cfg and not str(cfg.get("time_in_force") or "").strip():
            raise MappingValidationError(f"HOTPATH_EXECUTION_POLICY[{league!r}].time_in_force must be non-empty")
        for bk in ("require_presign", "presign_fallback_on_miss"):
            if bk in cfg and not isinstance(cfg.get(bk), bool):
                raise MappingValidationError(f"HOTPATH_EXECUTION_POLICY[{league!r}].{bk} must be bool")
    for league, cfg in policy.hotpath_runtime_by_league.items():
        lk = _norm(str(league))
        if lk != str(league):
            raise MappingValidationError(f"HOTPATH_RUNTIME_POLICY league key must be normalized: {league!r}")
        if not isinstance(cfg, dict):
            raise MappingValidationError(f"HOTPATH_RUNTIME_POLICY[{league!r}] must be dict")
        for key in (
            "plan_horizon_hours",
            "subscribe_lead_minutes",
            "reload_interval_seconds",
            "provider_catalog_max_age_seconds",
            "refresh_interval_seconds",
        ):
            if key not in cfg:
                raise MappingValidationError(f"HOTPATH_RUNTIME_POLICY[{league!r}] missing {key}")
            try:
                value = int(cfg.get(key))
            except (TypeError, ValueError):
                raise MappingValidationError(f"HOTPATH_RUNTIME_POLICY[{league!r}].{key} must be int") from None
            if value <= 0:
                raise MappingValidationError(f"HOTPATH_RUNTIME_POLICY[{league!r}].{key} must be > 0")
