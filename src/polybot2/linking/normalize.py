"""Normalization helpers for provider parsing and deterministic linking."""

from __future__ import annotations


def normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_league_key(raw: str) -> str:
    text = normalize_text(raw)
    aliases = {
        "champions league": "ucl",
        "uefa champions league": "ucl",
        "nba": "nba",
        "mlb": "mlb",
        "major league baseball": "mlb",
        "nhl": "nhl",
        "epl": "epl",
        "premier league": "epl",
        "bundesliga": "bundesliga",
        "la liga": "laliga",
        "laliga": "laliga",
        "world cup": "fifa-world-cup",
        "fifa world cup": "fifa-world-cup",
    }
    return aliases.get(text, text.replace(" ", "-"))


def sport_key_for_league(league_key: str) -> str:
    lk = normalize_text(league_key)
    mapping = {
        "nba": "basketball",
        "mlb": "baseball",
        "nhl": "hockey",
        "ucl": "soccer",
        "fifa-world-cup": "soccer",
        "epl": "soccer",
        "bundesliga": "soccer",
        "laliga": "soccer",
    }
    return mapping.get(lk, "")
