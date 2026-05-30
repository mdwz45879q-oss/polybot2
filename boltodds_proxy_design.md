# BoltOdds Shared Proxy Design

## Motivation

BoltOdds limits WebSocket connections per API key (currently 2-3). Each
hotpath process (`hotpath live --league mlb`, `--sport soccer`,
`--league cs2`, etc.) currently opens its own BoltOdds WS connection via
`ws_boltodds.rs`. With 4+ sports running concurrently, this exceeds the
connection budget.

BoltOdds does not require separate connections per sport — a single
livescores WS connection accepts game labels from any sport in one
subscription. The proxy exploits this: one WS connection serves all hotpath
processes.

**Timeline:** Required before LoL/Dota2 hotpath launch, since BoltOdds is
significantly faster than V1 for those esports (map completion signals
arrive 30-245s earlier in captured data). CS2 launches V1-only; the proxy
enables BoltOdds multiplexing for all sports simultaneously.

---

## Architecture

```
                          ┌──────────────────────┐
                          │   BoltOdds Cloud     │
                          │  livescores WS API   │
                          └──────────┬───────────┘
                                     │ one WS connection
                                     │
                          ┌──────────▼───────────┐
                          │   boltodds-proxy      │
                          │   (Rust binary)       │
                          │                       │
                          │ ┌───────────────────┐ │
                          │ │ FxHashMap:         │ │
                          │ │ game_label → fd   │ │
                          │ │ (routing table)   │ │
                          │ └───────────────────┘ │
                          │                       │
                          └──┬──────┬──────┬──────┘
                             │      │      │
                    Unix sock│      │      │Unix socket
                             │      │      │
                    ┌────────▼──┐ ┌─▼────────┐ ┌──▼────────┐
                    │ hotpath   │ │ hotpath   │ │ hotpath   │
                    │ mlb       │ │ soccer    │ │ cs2/lol   │
                    │ (Rust)    │ │ (Rust)    │ │ (Rust)    │
                    └───────────┘ └───────────┘ └───────────┘
```

### Components

**1. `boltodds-proxy` — Rust binary**

A standalone async Rust process. Single-threaded tokio runtime is sufficient
(the workload is I/O-bound frame routing, not CPU-bound evaluation).

Responsibilities:
- Maintain one authenticated WS connection to BoltOdds livescores
- Listen on a local Unix domain socket for hotpath process connections
- Accept registration messages (game labels + process identity)
- Manage the BoltOdds subscription (aggregate all registered labels)
- Route incoming BoltOdds frames to the correct hotpath process(es)
- Handle reconnection with 30s+ backoff (BoltOdds 12 conn/min rate limit)
- Handle hotpath process disconnect (remove labels, resubscribe)

**2. Python orchestrator changes**

The existing `commands_hotpath_runtime.py` orchestrator:
- Instead of passing BoltOdds config to the Rust `NativeHotPathRuntime`
  (which spawns `ws_boltodds.rs`), pass the proxy's Unix socket path
- The hotpath Rust runtime connects to the local socket instead of BoltOdds
- Registration: on startup, send the game labels to the proxy; on
  incremental refresh, send updated labels

**3. Rust hotpath changes**

Minimal changes to `ws_multiplexed.rs` / `ws_boltodds.rs`:
- Replace `try_connect_boltodds()` (WS to BoltOdds cloud) with
  `connect_boltodds_proxy()` (Unix socket to local proxy)
- The frame format is identical — raw JSON bytes, same as what BoltOdds
  sends over WS
- `fast_extract_boltodds` / `process_boltodds_frame_sync` unchanged
- No changes to evaluation logic, dispatch, or presign pool

---

## Protocol

### Proxy ↔ Hotpath (Unix domain socket)

Simple newline-delimited JSON messages. No custom binary protocol — the
frame volume is low enough (~10-100 frames/sec across all sports) that
JSON overhead is negligible.

**Hotpath → Proxy (registration):**

```json
{"action": "register", "process_id": "mlb_01", "game_labels": ["Team A vs Team B, 2026-06-01, 07", ...]}
```

**Hotpath → Proxy (update labels, e.g., after incremental refresh):**

```json
{"action": "update_labels", "process_id": "mlb_01", "game_labels": ["...", ...]}
```

**Hotpath → Proxy (unregister on shutdown):**

```json
{"action": "unregister", "process_id": "mlb_01"}
```

**Proxy → Hotpath (forwarded BoltOdds frame):**

```json
{"source": "boltodds", "frame": <raw BoltOdds JSON>}
```

The `frame` field contains the exact BoltOdds payload (e.g.,
`{"action": "match_update", "game": "...", "state": {...}}`). The hotpath
process strips the wrapper and feeds `frame` to `fast_extract_boltodds` /
`fast_extract_boltodds_baseball` — identical to what it does today with
frames from the direct WS.

### Proxy ↔ BoltOdds (WS)

Standard BoltOdds livescores protocol (unchanged):
- Connect to `wss://spro.agency/api/livescores?key=TOKEN`
- Wait for `socket_connected` handshake
- Subscribe with aggregated game labels from all registered processes
- Receive `match_update` / `new_play` frames
- For esports, use labels from `/api/playbyplay/esports` (the proxy fetches
  these at startup and on periodic refresh)

---

## Routing

When a BoltOdds frame arrives, the proxy extracts the game label:
- `frame.get("game")` for `match_update` (baseball/soccer)
- `frame.get("event")` for `new_play` (esports)

The routing table maps `game_label → Vec<process_fd>`. A label typically
maps to one process, but could map to multiple if the same game appears in
multiple hotpath processes (e.g., during a handoff). The proxy writes the
frame to all matching process sockets.

**Lookup cost:** One `FxHashMap::get` per frame — same as the current
per-sport `game_id_to_idx` lookup in the Rust engine. Sub-microsecond.

**Unrecognized labels:** Frames for games not in the routing table are
silently dropped. This handles games that BoltOdds streams but no hotpath
process is interested in (e.g., subscribed to a broad filter).

---

## Subscription Management

The proxy maintains the **union** of all registered game labels across all
processes. When labels change (process registers, updates, or disconnects),
the proxy resubscribes to BoltOdds with the new aggregate list.

```
Process A registers: ["game1", "game2", "game3"]
Process B registers: ["game4", "game5"]
→ BoltOdds subscription: ["game1", "game2", "game3", "game4", "game5"]

Process A updates: ["game1", "game2"]  (dropped game3)
→ BoltOdds subscription: ["game1", "game2", "game4", "game5"]

Process A disconnects:
→ BoltOdds subscription: ["game4", "game5"]
```

BoltOdds supports resubscription on an existing connection — send a new
`subscribe` message and the filters are replaced (not appended).

**Esports label resolution:** The proxy handles the
`/api/playbyplay/esports` label fetch internally. Hotpath processes send
their game labels as-is (already correct from `provider_games` after the
BoltOdds sync fix). The proxy does not need to translate labels.

---

## Lifecycle

### Startup order

The proxy does **not** require hotpath processes to be running. It starts
independently and waits for connections:

1. Start `boltodds-proxy` → connects to BoltOdds, subscribes to empty
   list (or stays idle until first registration)
2. Start `hotpath live --league mlb` → connects to proxy Unix socket,
   registers MLB game labels
3. Start `hotpath live --league cs2` → same, registers CS2 labels
4. Start/stop processes at any time → proxy dynamically updates

### Shutdown

- Hotpath process exits → proxy detects socket close, removes that
  process's labels, resubscribes
- Proxy exits → all hotpath processes lose BoltOdds data but continue
  running on V1 (fail-safe — BoltOdds is a supplementary source, not
  the sole provider for any sport except potentially LoL/Dota2 in the
  future)

### Reconnection

BoltOdds WS drops → proxy reconnects with 30s backoff (respecting the
12 conn/min rate limit). On reconnect:
1. Regenerate auth (not applicable — BoltOdds uses API key, no HMAC)
2. Resubscribe with the current aggregate label set
3. Hotpath processes are unaware of the reconnection — they just stop
   receiving frames during the gap and resume when the proxy reconnects

---

## Configuration

```python
# In HOTPATH_RUNTIME_POLICY or a new config section:
BOLTODDS_PROXY = {
    "socket_path": "/tmp/boltodds_proxy.sock",  # Unix domain socket
    "enabled": True,                              # toggle proxy vs direct
}
```

When `enabled = True`, the hotpath Rust runtime connects to `socket_path`
instead of spawning a direct BoltOdds WS worker. When `False`, falls back
to the current per-process direct connection (for development/testing).

---

## Implementation Phases

### Phase 1: Proxy binary (Rust)

- Standalone `boltodds-proxy` binary in `native/boltodds_proxy/`
- BoltOdds WS connection with reconnection
- Unix socket listener for hotpath processes
- Registration/unregistration protocol
- Frame routing by game label
- Esports label handling

### Phase 2: Hotpath integration

- New `ws_boltodds_proxy.rs` — Unix socket client that reads frames from
  the proxy instead of a direct WS. Same frame format, same dispatch to
  `process_boltodds_frame_sync`.
- `runtime.rs` — when proxy config is present, spawn the proxy reader
  instead of `ws_boltodds.rs`
- Python orchestrator — pass proxy socket path instead of BoltOdds
  credentials to the Rust runtime

### Phase 3: Orchestrator lifecycle

- Launch/stop the proxy from the CLI or as a systemd service
- Health monitoring — proxy exposes a health endpoint or file for the
  orchestrator to check
- Graceful shutdown — proxy drains in-flight frames before closing

---

## Latency Budget

| Hop | Expected latency |
|-----|-----------------|
| BoltOdds → proxy (WS recv) | 0 (same as current direct WS) |
| Proxy routing (HashMap lookup) | < 1µs |
| Proxy → hotpath (Unix socket write) | < 5µs |
| **Total overhead vs direct WS** | **< 10µs** |

For comparison, the BoltOdds frame-to-evaluation path currently takes
~100ns (fast_extract) + ~10ns (evaluation) + ~100ns (presign pop). The
proxy adds < 10µs — negligible relative to the 3-16 second latency
advantage that BoltOdds provides over V1 for LoL/Dota2 map completions.

---

## Alternatives Considered

**1. Ask BoltOdds for more connections:** Costs significantly more per
month. The proxy is a one-time engineering cost that scales to any number
of sports without additional API fees.

**2. Python proxy:** Lower development effort but adds Python GIL and
asyncio overhead to the frame delivery path. Measured at ~50-200µs per
frame forwarding vs < 10µs in Rust. Acceptable for capture scripts but
not for the latency-sensitive hotpath.

**3. Single multi-sport hotpath process:** Run all sports in one Rust
process with one BoltOdds connection. Would require significant
refactoring of the engine dispatch (currently one `SportEngine` enum per
process) and eliminates process-level isolation between sports. Higher
blast radius if one sport's evaluation panics.
