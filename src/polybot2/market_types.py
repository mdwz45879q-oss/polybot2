"""Canonical Polymarket sports market-type normalization."""

from __future__ import annotations

from typing import Any


CANONICAL_MARKET_TYPES = {"totals", "nrfi", "moneyline", "spread", "other"}


def _norm(text: Any) -> str:
    return " ".join(str(text or "").strip().lower().split())


def normalize_sports_market_type(value: Any) -> str:
    raw = _norm(value).replace("-", "_").replace(" ", "_")
    if not raw:
        return "other"
    if raw in {"total", "totals", "ou", "o_u"}:
        return "totals"
    if raw in {"nrfi", "nfri"}:
        return "nrfi"
    if raw in {"spread", "spreads"}:
        return "spread"
    if raw in {"moneyline", "game", "child_moneyline", "first_half_moneyline"}:
        return "moneyline"
    return raw if raw in CANONICAL_MARKET_TYPES else "other"


def is_totals_market_type(value: Any) -> bool:
    return normalize_sports_market_type(value) == "totals"


__all__ = [
    "CANONICAL_MARKET_TYPES",
    "is_totals_market_type",
    "normalize_sports_market_type",
]
