"""Sports provider framework (provider-agnostic + BoltOdds/Kalstrop)."""

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.boltodds import BoltOddsProvider, BoltOddsProviderConfig
from polybot2.sports.factory import build_sports_provider, resolve_kalstrop_credentials_from_env
from polybot2.sports.kalstrop_v1 import KalstropV1Provider, KalstropV1ProviderConfig
from polybot2.sports.contracts import (
    ProviderGameRecord,
    SportsProviderConfig,
)

__all__ = [
    "BoltOddsProvider",
    "BoltOddsProviderConfig",
    "KalstropV1Provider",
    "KalstropV1ProviderConfig",
    "build_sports_provider",
    "resolve_kalstrop_credentials_from_env",
    "ProviderGameRecord",
    "SportsDataProviderBase",
    "SportsProviderConfig",
]
