from __future__ import annotations

from polybot2.sports.boltodds import BoltOddsProvider, BoltOddsProviderConfig


class _FakeWs:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def _provider() -> BoltOddsProvider:
    provider = BoltOddsProvider(config=BoltOddsProviderConfig(api_key="test_key"))
    provider.resolve_universal_ids = lambda universal_ids: sorted(  # type: ignore[method-assign]
        {str(x or "").strip() for x in (universal_ids or ()) if str(x or "").strip()}
    )
    return provider


def test_subscribe_scores_shrink_closes_ws_for_clean_resubscribe() -> None:
    provider = _provider()
    ws = _FakeWs()
    provider._scores_ws = ws
    provider._subscribed_scores_uids = {"gid1", "gid2"}
    sent: list[list[str]] = []
    provider._send_subscribe = lambda *, ws, stream, universal_ids: sent.append(list(universal_ids))  # type: ignore[method-assign]

    provider.subscribe_scores(["gid1"])

    assert ws.closed == 1
    assert provider._scores_ws is None
    assert sent == []


def test_subscribe_scores_growth_sends_incremental_subscribe_without_close() -> None:
    provider = _provider()
    ws = _FakeWs()
    provider._scores_ws = ws
    provider._subscribed_scores_uids = {"gid1"}
    sent: list[list[str]] = []
    provider._send_subscribe = lambda *, ws, stream, universal_ids: sent.append(list(universal_ids))  # type: ignore[method-assign]

    provider.subscribe_scores(["gid1", "gid2"])

    assert ws.closed == 0
    assert provider._scores_ws is ws
    assert sent == [["gid1", "gid2"]]


def test_subscribe_odds_empty_set_closes_ws() -> None:
    provider = _provider()
    ws = _FakeWs()
    provider._odds_ws = ws
    provider._subscribed_odds_uids = {"gid1"}
    sent: list[list[str]] = []
    provider._send_subscribe = lambda *, ws, stream, universal_ids: sent.append(list(universal_ids))  # type: ignore[method-assign]

    provider.subscribe_odds([])

    assert ws.closed == 1
    assert provider._odds_ws is None
    assert sent == []
