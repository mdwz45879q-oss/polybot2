"""Sport-generic order execution policy."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OrderPolicy:
    """Execution profile injected from live trading policy."""

    amount_usdc: float = 5.0
    size_shares: float = 5.0
    limit_price: float = 0.52
    time_in_force: str = "FAK"


__all__ = ["OrderPolicy"]
