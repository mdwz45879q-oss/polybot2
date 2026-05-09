"""Normalization helpers for provider parsing and deterministic linking."""

from __future__ import annotations


def normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_league_key(raw: str) -> str:
    text = normalize_text(raw)
    aliases = {
        "mlb": "mlb",
        "major league baseball": "mlb",
        "epl": "epl",
        "english premier league": "epl",
        "champions league": "ucl",
        "uefa champions league": "ucl",
        "bundesliga": "bundesliga",
    }
    return aliases.get(text, text.replace(" ", "-"))


def sport_key_for_league(league_key: str) -> str:
    lk = normalize_text(league_key)
    mapping = {
        "mlb": "baseball",
        "ucl": "soccer",
        "epl": "soccer",
        "bundesliga": "soccer",
    }
    return mapping.get(lk, "")
