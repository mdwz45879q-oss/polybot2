#!/usr/bin/env python3
"""
Build a games.json capture plan from provider_games DB.

Usage:
    python scripts/build_capture_plan.py --league mlb --date 2026-05-11
    python scripts/build_capture_plan.py --league epl bundesliga --date 2026-05-11
    python scripts/build_capture_plan.py --league mlb --date 2026-05-11 --out ./captures/2026_05_11/games.json

Prerequisites:
    polybot2 provider sync   (populates provider_games table)
"""

import argparse
import importlib.util
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
PROVIDERS = ["kalstrop_v1", "kalstrop_v2", "boltodds"]
TIME_TOLERANCE_SECONDS = 900  # 15 minutes


def load_config(config_dir: str) -> dict:
    path = str(Path(config_dir) / "mappings.py")
    # Add config dir to path so relative imports (baseball_mappings, soccer_mappings) work
    config_abs = str(Path(config_dir).resolve())
    if config_abs not in sys.path:
        sys.path.insert(0, config_abs)
    spec = importlib.util.spec_from_file_location("_mappings", path)
    if not spec or not spec.loader:
        print(f"ERROR: cannot load {path}", file=sys.stderr)
        sys.exit(1)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return {
        "LEAGUES": getattr(mod, "LEAGUES", {}),
        "PROVIDER_LEAGUE_ALIASES": getattr(mod, "PROVIDER_LEAGUE_ALIASES", {}),
        "PROVIDER_LEAGUE_COUNTRY": getattr(mod, "PROVIDER_LEAGUE_COUNTRY", {}),
        "TEAM_MAP": getattr(mod, "TEAM_MAP", {}),
    }


def resolve_league(
    provider: str,
    sport_raw: str,
    league_raw: str,
    category_name: str,
    aliases: dict,
    country_map: dict,
) -> str | None:
    provider_aliases = aliases.get(provider, {})
    raw_country = country_map.get(provider, {})
    # Parse "country|league" string keys and normalize to lowercase
    provider_country: dict[tuple[str, str], str] = {}
    for k, v in raw_country.items():
        if isinstance(k, str) and "|" in k:
            parts = k.split("|", 1)
            provider_country[(parts[0].strip().lower(), parts[1].strip().lower())] = v
        elif isinstance(k, tuple) and len(k) == 2:
            provider_country[(str(k[0]).strip().lower(), str(k[1]).strip().lower())] = v

    if provider == "boltodds":
        key = sport_raw.strip().lower()
    else:
        key = league_raw.strip().lower()

    if key in provider_aliases:
        return provider_aliases[key]

    country_key = (category_name.strip().lower(), key)
    if country_key in provider_country:
        return provider_country[country_key]

    return None


def build_alias_index(team_map: dict, league: str) -> dict:
    """Build reverse index: (provider, alias_lower) → canonical_name."""
    league_map = team_map.get(league, {})
    index: dict[tuple[str, str], str] = {}
    for canonical, entry in league_map.items():
        for provider, alias_list in entry.get("provider_aliases", {}).items():
            for alias in alias_list:
                index[(provider, alias.strip().lower())] = canonical
    return index


def resolve_team(
    team_raw: str,
    provider: str,
    alias_index: dict,
) -> str:
    key = (provider, team_raw.strip().lower())
    if key in alias_index:
        return alias_index[key]
    return team_raw.strip().lower()


def find_games(
    db: sqlite3.Connection,
    league: str,
    date_start_utc: int,
    date_end_utc: int,
    aliases: dict,
    country_map: dict,
) -> dict[str, list[dict]]:
    """Find games for each provider that belong to the target league + date."""
    rows = db.execute(
        """
        SELECT provider, provider_game_id, game_label, sport_raw, league_raw,
               category_name, home_raw, away_raw, when_raw, start_ts_utc
        FROM provider_games
        WHERE start_ts_utc IS NOT NULL
          AND start_ts_utc BETWEEN ? AND ?
        ORDER BY start_ts_utc
        """,
        (date_start_utc, date_end_utc),
    ).fetchall()

    by_provider: dict[str, list[dict]] = {}
    for r in rows:
        provider = r[0]
        resolved = resolve_league(
            provider, r[3], r[4], r[5], aliases, country_map,
        )
        if resolved != league:
            continue
        game = {
            "provider": provider,
            "provider_game_id": r[1],
            "game_label": r[2],
            "sport_raw": r[3],
            "league_raw": r[4],
            "category_name": r[5],
            "home_raw": r[6],
            "away_raw": r[7],
            "when_raw": r[8],
            "start_ts_utc": r[9],
        }
        by_provider.setdefault(provider, []).append(game)
    return by_provider


def match_games(
    by_provider: dict[str, list[dict]],
    league: str,
    alias_index: dict,
) -> list[dict]:
    """Match games across providers by canonical team names + start time."""
    # Flatten all games with canonical names
    entries: list[dict] = []
    for provider, games in by_provider.items():
        for g in games:
            canon_home = resolve_team(g["home_raw"], provider, alias_index)
            canon_away = resolve_team(g["away_raw"], provider, alias_index)
            entries.append({
                **g,
                "canon_home": canon_home,
                "canon_away": canon_away,
            })

    def _teams_match(a_home: str, a_away: str, b_home: str, b_away: str) -> bool:
        """Check if two team pairs refer to the same game (exact or substring, either order)."""
        def _pair_match(h1: str, a1: str, h2: str, a2: str) -> bool:
            return _name_match(h1, h2) and _name_match(a1, a2)
        return _pair_match(a_home, a_away, b_home, b_away) or _pair_match(a_home, a_away, b_away, b_home)

    def _name_match(a: str, b: str) -> bool:
        if a == b:
            return True
        if a in b or b in a:
            return True
        return False

    # Group by team match with time tolerance — allows same-provider duplicates
    groups: list[list[dict]] = []
    used: set[int] = set()

    for i, e in enumerate(entries):
        if i in used:
            continue
        group = [e]
        used.add(i)
        for j, other in enumerate(entries):
            if j in used:
                continue
            if (
                _teams_match(e["canon_home"], e["canon_away"], other["canon_home"], other["canon_away"])
                and abs((other["start_ts_utc"] or 0) - (e["start_ts_utc"] or 0)) <= TIME_TOLERANCE_SECONDS
            ):
                group.append(other)
                used.add(j)
        groups.append(group)

    # Build matched result, resolving same-provider duplicates (BoltOdds flipped entries)
    matched: list[dict] = []
    for group in groups:
        providers: dict[str, list[dict]] = {}
        for g in group:
            providers.setdefault(g["provider"], []).append(g)

        # Resolve duplicates: if a provider has 2 entries (flipped home/away),
        # pick the one matching a reference provider's home/away order.
        # If no reference, keep both (user resolves manually).
        ref_home = None
        for p in ["kalstrop_v1", "kalstrop_v2"]:
            if p in providers and len(providers[p]) == 1:
                ref_home = providers[p][0]["canon_home"]
                break

        resolved: dict[str, dict] = {}
        for p, entries_list in providers.items():
            if len(entries_list) == 1:
                resolved[p] = entries_list[0]
            elif ref_home is not None:
                # Pick the entry whose canon_home matches the reference
                for e in entries_list:
                    if e["canon_home"] == ref_home:
                        resolved[p] = e
                        break
                if p not in resolved:
                    resolved[p] = entries_list[0]
            else:
                # No reference — keep both as separate entries
                for idx, e in enumerate(entries_list):
                    key = f"{p}_{idx}" if idx > 0 else p
                    resolved[key] = e

        canon_home = group[0]["canon_home"]
        canon_away = group[0]["canon_away"]
        if ref_home and ref_home != canon_home:
            canon_home, canon_away = canon_away, canon_home

        entry = {
            "canon_home": canon_home,
            "canon_away": canon_away,
            "start_ts_utc": group[0]["start_ts_utc"],
            "providers": resolved,
        }
        matched.append(entry)

    matched.sort(key=lambda x: x["start_ts_utc"] or 0)
    return matched


def make_name(canon_home: str, canon_away: str) -> str:
    def clean(s: str) -> str:
        s = re.sub(r"[^a-z0-9]+", "_", s.strip().lower())
        return s.strip("_")
    return f"{clean(canon_home)}_vs_{clean(canon_away)}"


def ts_to_et(ts: int | None) -> str:
    if not ts:
        return ""
    dt = datetime.fromtimestamp(ts, tz=ET)
    return dt.strftime("%H:%M")


def ts_to_date(ts: int | None) -> str:
    if not ts:
        return ""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def emit_json(matched: list[dict], league: str, sport: str) -> list[dict]:
    result: list[dict] = []
    for m in matched:
        entry: dict = {
            "name": make_name(m["canon_home"], m["canon_away"]),
            "sport": sport,
            "league": league,
            "kickoff_et": ts_to_et(m["start_ts_utc"]),
        }

        providers = m["providers"]

        if "kalstrop_v1" in providers:
            entry["v1_fixture_id"] = providers["kalstrop_v1"]["provider_game_id"]

        if "kalstrop_v2" in providers:
            v2 = providers["kalstrop_v2"]
            entry["v2_event_id"] = v2["provider_game_id"]
            entry["v2_category_slug"] = v2["category_name"]
            entry["v2_tournament_slug"] = v2["league_raw"]
            entry["v2_home_team"] = v2["home_raw"]
            entry["v2_away_team"] = v2["away_raw"]
            when_raw = v2.get("when_raw", "")
            if when_raw:
                try:
                    entry["v2_scheduled_date"] = when_raw[:10]
                except Exception:
                    pass

        bo_entries = [v for k, v in providers.items() if k.startswith("boltodds")]
        if len(bo_entries) == 1:
            entry["boltodds_game_label"] = bo_entries[0]["game_label"]
        elif len(bo_entries) > 1:
            # Multiple unresolved BoltOdds entries — include both for manual review
            for i, bo in enumerate(bo_entries):
                suffix = f"_{i+1}" if i > 0 else ""
                entry[f"boltodds_game_label{suffix}"] = bo["game_label"]

        result.append(entry)
    return result


def print_summary(matched: list[dict], league: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {league.upper()} — {len(matched)} game(s)")
    print(f"{'='*60}")
    for i, m in enumerate(matched, 1):
        providers = m["providers"]
        unique_providers = set(k.split("_")[0] for k in providers)
        tag = "" if len(unique_providers) > 1 else " (single provider)"
        kickoff = ts_to_et(m["start_ts_utc"])
        date_str = ts_to_date(m["start_ts_utc"])
        print(f"\n[{i}] {make_name(m['canon_home'], m['canon_away'])} ({date_str} {kickoff} ET){tag}")

        if "kalstrop_v1" in providers:
            v1 = providers["kalstrop_v1"]
            print(f"    V1:       {v1['provider_game_id']} ({v1['home_raw']} vs {v1['away_raw']})")

        if "kalstrop_v2" in providers:
            v2 = providers["kalstrop_v2"]
            print(f"    V2:       {v2['provider_game_id']} ({v2['home_raw']} vs {v2['away_raw']})")

        bo_keys = sorted(k for k in providers if k.startswith("boltodds"))
        for k in bo_keys:
            bo = providers[k]
            suffix = " ⚠️ UNRESOLVED DUPLICATE" if len(bo_keys) > 1 else ""
            print(f"    BoltOdds: {bo['game_label']}{suffix}")


def main():
    ap = argparse.ArgumentParser(description="Build capture plan from provider_games DB")
    ap.add_argument("--league", nargs="+", required=True, help="Canonical league key(s)")
    ap.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    ap.add_argument("--db", default="data/prediction_markets.db", help="DB path")
    ap.add_argument("--out", default="", help="Output path for games.json")
    ap.add_argument("--config-dir", default="", help="Config directory (default: auto-detect)")
    ap.add_argument("--tz", default="utc", choices=["utc", "et"],
                    help="Timezone for date range (default: utc)")
    args = ap.parse_args()

    config_dir = args.config_dir or str(Path(__file__).resolve().parents[1] / "config")
    cfg = load_config(config_dir)

    try:
        if args.tz == "et":
            dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=ET)
        else:
            dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"ERROR: invalid date format: {args.date}", file=sys.stderr)
        return 1

    date_start = int(dt.timestamp())
    date_end = date_start + 86400

    db_path = args.db
    if not Path(db_path).exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = None  # tuple rows

    all_entries: list[dict] = []

    for league in args.league:
        league = league.strip().lower()
        league_cfg = cfg["LEAGUES"].get(league, {})
        sport = league_cfg.get("sport_family", "soccer")

        alias_index = build_alias_index(cfg["TEAM_MAP"], league)

        by_provider = find_games(
            conn, league, date_start, date_end,
            cfg["PROVIDER_LEAGUE_ALIASES"], cfg["PROVIDER_LEAGUE_COUNTRY"],
        )

        total = sum(len(v) for v in by_provider.values())
        provider_summary = ", ".join(f"{k}={len(v)}" for k, v in sorted(by_provider.items()))
        print(f"[{league}] Found {total} games across providers: {provider_summary}")

        matched = match_games(by_provider, league, alias_index)
        print_summary(matched, league)

        entries = emit_json(matched, league, sport)
        all_entries.extend(entries)

    conn.close()

    if not all_entries:
        print("\nNo games found.")
        return 0

    out_path = args.out
    if not out_path:
        out_path = f"captures/{args.date.replace('-', '_')}/games.json"

    out_dir = Path(out_path).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w") as f:
        json.dump(all_entries, f, indent=2)
    print(f"\nWrote {len(all_entries)} game(s) to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
