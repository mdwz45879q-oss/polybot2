"""Sports provider framework (provider-agnostic + BoltOdds/Kalstrop)."""

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.boltodds import BoltOddsProvider, BoltOddsProviderConfig
from polybot2.sports.factory import build_sports_provider, capture_stream_profile, resolve_kalstrop_credentials_from_env
from polybot2.sports.kalstrop import KalstropProvider, KalstropProviderConfig
from polybot2.sports.contracts import (
    OddsOutcome,
    OddsUpdateEvent,
    PlayByPlayUpdateEvent,
    ProviderGameRecord,
    ScoreUpdateEvent,
    SportsProviderConfig,
    StreamEnvelope,
    StreamType,
)
from polybot2.sports.recorder import (
    JsonlRawFrameRecorder,
    JsonlUpdateRecorder,
    NullRawFrameRecorder,
    NullRecorder,
    RawFrameRecorder,
    UpdateRecorder,
)

__all__ = [
    "BoltOddsProvider",
    "BoltOddsProviderConfig",
    "KalstropProvider",
    "KalstropProviderConfig",
    "build_sports_provider",
    "capture_stream_profile",
    "resolve_kalstrop_credentials_from_env",
    "JsonlUpdateRecorder",
    "JsonlRawFrameRecorder",
    "NullRawFrameRecorder",
    "NullRecorder",
    "OddsOutcome",
    "OddsUpdateEvent",
    "PlayByPlayUpdateEvent",
    "ProviderGameRecord",
    "RawFrameRecorder",
    "ScoreUpdateEvent",
    "SportsDataProviderBase",
    "SportsProviderConfig",
    "StreamEnvelope",
    "StreamType",
    "UpdateRecorder",
]
