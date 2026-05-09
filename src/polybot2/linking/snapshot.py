"""In-memory resolver over deterministic link tables."""

from __future__ import annotations

import threading
from typing import Any

from polybot2.linking.contracts import BindingTarget, GameBindingView
from polybot2.market_types import normalize_sports_market_type


class BindingResolver:
    def __init__(self, *, db: Any):
        self._db = db
        self._lock = threading.RLock()
        self._snapshot_id: int | None = None
        self._by_game: dict[tuple[str, str], GameBindingView] = {}

    def reload(self) -> int | None:
        with self._lock:
            self._by_game = {}

        rows = self._db.execute(
            """
            SELECT g.provider, g.provider_game_id, g.event_slug_prefix, g.binding_status,
                   g.reason_code, g.is_tradeable,
                   m.condition_id, m.outcome_index, m.token_id, m.market_slug,
                   m.sports_market_type, m.binding_status AS target_binding_status,
                   m.reason_code AS target_reason_code, m.is_tradeable AS target_is_tradeable
            FROM link_game_bindings g
            LEFT JOIN link_market_bindings m
              ON m.provider = g.provider
             AND m.provider_game_id = g.provider_game_id
            ORDER BY g.provider, g.provider_game_id, m.condition_id, m.outcome_index
            """
        ).fetchall()

        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            key = (str(r["provider"] or "").strip().lower(), str(r["provider_game_id"] or "").strip())
            row = grouped.setdefault(
                key,
                {
                    "event_slug_prefix": str(r["event_slug_prefix"] or ""),
                    "binding_status": str(r["binding_status"] or ""),
                    "reason_code": str(r["reason_code"] or ""),
                    "is_tradeable": bool(int(r["is_tradeable"] or 0)),
                    "targets": [],
                },
            )
            cid = str(r["condition_id"] or "")
            if not cid:
                continue
            row["targets"].append(
                BindingTarget(
                    provider=key[0],
                    provider_game_id=key[1],
                    condition_id=cid,
                    outcome_index=int(r["outcome_index"] or 0),
                    token_id=str(r["token_id"] or ""),
                    market_slug=str(r["market_slug"] or ""),
                    sports_market_type=str(r["sports_market_type"] or ""),
                    binding_status=str(r["target_binding_status"] or ""),
                    reason_code=str(r["target_reason_code"] or ""),
                    is_tradeable=bool(int(r["target_is_tradeable"] or 0)),
                )
            )

        latest = self._db.execute("SELECT MAX(run_id) AS max_run_id FROM link_runs").fetchone()
        snapshot_id = None if latest is None or latest["max_run_id"] is None else int(latest["max_run_id"])

        by_game: dict[tuple[str, str], GameBindingView] = {}
        for key, row in grouped.items():
            by_game[key] = GameBindingView(
                provider=key[0],
                provider_game_id=key[1],
                event_slug_prefix=str(row["event_slug_prefix"]),
                binding_status=str(row["binding_status"]),
                reason_code=str(row["reason_code"]),
                is_tradeable=bool(row["is_tradeable"]),
                targets=tuple(row["targets"]),
            )

        with self._lock:
            self._snapshot_id = snapshot_id
            self._by_game = by_game
        return snapshot_id

    def current_snapshot_id(self) -> int | None:
        with self._lock:
            return self._snapshot_id

    def resolve_game_binding(self, provider: str, provider_game_id: str, venue: str = "polymarket") -> GameBindingView | None:
        del venue
        key = (str(provider or "").strip().lower(), str(provider_game_id or "").strip())
        with self._lock:
            return self._by_game.get(key)

    def resolve_market_tokens(
        self,
        provider: str,
        provider_game_id: str,
        market_type_filter: str | None = None,
        venue: str = "polymarket",
    ) -> list[BindingTarget]:
        del venue
        view = self.resolve_game_binding(provider, provider_game_id)
        if view is None:
            return []
        if market_type_filter is None:
            return list(view.targets)
        mt = normalize_sports_market_type(market_type_filter)
        return [t for t in view.targets if normalize_sports_market_type(t.sports_market_type) == mt]


