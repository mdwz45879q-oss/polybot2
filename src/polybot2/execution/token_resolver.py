"""Optional token resolver helper (non-hot path)."""

from __future__ import annotations

from dataclasses import dataclass

from polybot2.data.storage import DataRuntimeConfig, open_database


@dataclass(frozen=True)
class ResolvedToken:
    condition_id: str
    outcome_index: int
    token_id: str


def resolve_token_id(
    *,
    runtime: DataRuntimeConfig,
    condition_id: str,
    outcome_index: int,
) -> ResolvedToken:
    cid = str(condition_id or "").strip()
    if not cid:
        raise ValueError("condition_id must be non-empty")
    idx = int(outcome_index)
    if idx < 0:
        raise ValueError("outcome_index must be >= 0")
    with open_database(runtime) as db:
        row = db.execute(
            """
            SELECT token_id
            FROM pm_market_tokens
            WHERE condition_id = ? AND outcome_index = ?
            ORDER BY updated_at DESC, token_id DESC
            LIMIT 1
            """,
            (cid, idx),
        ).fetchone()
    if row is None:
        raise ValueError(
            f"Could not resolve token_id for condition_id={cid!r}, outcome_index={idx}"
        )
    return ResolvedToken(
        condition_id=cid,
        outcome_index=idx,
        token_id=str(row["token_id"] or ""),
    )


__all__ = ["ResolvedToken", "resolve_token_id"]
