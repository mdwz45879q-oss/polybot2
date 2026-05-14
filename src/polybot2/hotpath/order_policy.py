"""Sport-generic order execution policy."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class OrderPolicy:
    """Execution profile injected from live trading policy.

    ``market_overrides`` maps ``sports_market_type`` → dict of fields to
    override.  Unspecified fields inherit from the league default.

    Secondary order fields (``secondary_*``) are optional.  When
    ``secondary_time_in_force`` is non-empty and ``secondary_amount_usdc > 0``,
    each intent fires two pre-signed orders: the primary and the secondary.
    """

    amount_usdc: float = 5.0
    size_shares: float = 5.0
    limit_price: float = 0.52
    time_in_force: str = "FAK"
    # Secondary order (optional — fires alongside primary when configured)
    secondary_amount_usdc: float = 0.0
    secondary_size_shares: float = 0.0
    secondary_limit_price: float = 0.0
    secondary_time_in_force: str = ""
    market_overrides: dict[str, dict[str, float | str]] = field(default_factory=dict)

    @property
    def has_secondary(self) -> bool:
        return bool(self.secondary_time_in_force) and self.secondary_amount_usdc > 0

    def for_market_type(self, sports_market_type: str) -> OrderPolicy:
        """Return a policy with overrides applied for this market type."""
        overrides = self.market_overrides.get(sports_market_type, {})
        if not overrides:
            return self
        return OrderPolicy(
            amount_usdc=float(overrides.get("amount_usdc", self.amount_usdc)),
            size_shares=float(overrides.get("size_shares", self.size_shares)),
            limit_price=float(overrides.get("limit_price", self.limit_price)),
            time_in_force=str(overrides.get("time_in_force", self.time_in_force)),
            secondary_amount_usdc=float(overrides.get("secondary_amount_usdc", self.secondary_amount_usdc)),
            secondary_size_shares=float(overrides.get("secondary_size_shares", self.secondary_size_shares)),
            secondary_limit_price=float(overrides.get("secondary_limit_price", self.secondary_limit_price)),
            secondary_time_in_force=str(overrides.get("secondary_time_in_force", self.secondary_time_in_force)),
        )


__all__ = ["OrderPolicy"]
