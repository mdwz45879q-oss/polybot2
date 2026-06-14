#!/usr/bin/env python3
"""Fetch PandaScore CS2 teams and match against TEAM_MAP_CS2.

Outputs proposed pandascore aliases for teams that don't match by
canonical name. Run from the repo root:

    PANDASCORE_API_TOKEN=... python scripts/build_pandascore_cs2_aliases.py
"""

import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "config"))
from cs2_mappings import TEAM_MAP_CS2

TOKEN = os.environ.get("PANDASCORE_API_TOKEN", "").strip()
if not TOKEN:
    print("ERROR: set PANDASCORE_API_TOKEN", file=sys.stderr)
    sys.exit(1)

BASE = "https://api.pandascore.co"


def fetch_all_cs2_teams():
    teams = []
    for page in range(1, 200):
        url = f"{BASE}/csgo/teams?token={TOKEN}&per_page=100&page={page}"
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())
        if not data:
            break
        teams.extend(data)
        if len(data) < 100:
            break
        time.sleep(0.15)
    return teams


def norm(s):
    return s.strip().lower()


def main():
    print("Fetching PandaScore CS2 teams...", file=sys.stderr)
    ps_teams = fetch_all_cs2_teams()
    print(f"  {len(ps_teams)} teams fetched", file=sys.stderr)

    ps_by_norm = {}
    for t in ps_teams:
        n = norm(t["name"])
        ps_by_norm[n] = t

    exact = []
    alias_needed = []
    already_aliased = []
    no_match = []

    for canonical, info in sorted(TEAM_MAP_CS2.items()):
        cn = norm(canonical)
        existing_ps_aliases = info.get("provider_aliases", {}).get("pandascore", [])

        # Check exact match on canonical name
        if cn in ps_by_norm:
            exact.append((canonical, ps_by_norm[cn]["name"]))
            continue

        # Check existing pandascore aliases
        found_via_alias = None
        for alias in existing_ps_aliases:
            if norm(alias) in ps_by_norm:
                found_via_alias = alias
                break

        if found_via_alias:
            already_aliased.append((canonical, found_via_alias))
            continue

        # Fuzzy search: substring match in both directions
        candidates = []
        for t in ps_teams:
            tn = norm(t["name"])
            if cn in tn or tn in cn:
                candidates.append((t["name"], t["id"], "substring"))
            elif cn.replace(" ", "") == tn.replace(" ", ""):
                candidates.append((t["name"], t["id"], "nospace"))

        # Also check V1 aliases — PandaScore might use the same name
        v1_aliases = info.get("provider_aliases", {}).get("kalstrop_v1", [])
        for v1a in v1_aliases:
            v1n = norm(v1a)
            if v1n in ps_by_norm:
                candidates.append((ps_by_norm[v1n]["name"], ps_by_norm[v1n]["id"], "v1_alias"))

        if candidates:
            # Pick best: prefer exact v1_alias match, then shortest substring match
            best = None
            for name, tid, match_type in candidates:
                if match_type == "v1_alias":
                    best = (name, tid, match_type)
                    break
            if not best:
                candidates.sort(key=lambda x: len(x[0]))
                best = candidates[0]
            alias_needed.append((canonical, best[0], best[2]))
        else:
            no_match.append(canonical)

    # Output
    print(f"\n=== ALIASES TO ADD ({len(alias_needed)} teams) ===")
    for canonical, ps_name, match_type in sorted(alias_needed):
        print(f'  "{canonical}": add pandascore alias ["{ps_name}"]  ({match_type})')

    print(f"\n=== NO MATCH FOUND ({len(no_match)} teams) ===")
    for canonical in sorted(no_match):
        print(f'  "{canonical}": no PandaScore team found')

    print(f"\n=== ALREADY ALIASED ({len(already_aliased)} teams) ===")
    for canonical, alias in sorted(already_aliased):
        print(f'  "{canonical}": existing alias "{alias}"')

    print(f"\n=== EXACT MATCH ({len(exact)} teams) ===")
    for canonical, ps_name in sorted(exact):
        print(f'  "{canonical}": "{ps_name}"')

    print(f"\nSummary: {len(exact)} exact, {len(already_aliased)} aliased, "
          f"{len(alias_needed)} need alias, {len(no_match)} no match", file=sys.stderr)


if __name__ == "__main__":
    main()
