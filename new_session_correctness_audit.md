# Correctness Audit: Recent Architectural Changes

Date: 2026-05-16

---

## 1. Concurrent Providers: Multi-Provider Plan Compilation

### Files audited
- `src/polybot2/hotpath/compiler.py`
- `src/polybot2/hotpath/contracts.py`
- `src/polybot2/hotpath/native_engine.py`
- `src/polybot2/linking/service.py`

### Findings

- **OK:** `compile_multi_league_plan()` correctly skips leagues with `scope_blocked` errors (line 862-864) and raises only if ALL leagues fail (line 867). No crash on empty-scope leagues.

- **OK:** Deduplication by `provider_game_id` (line 858) correctly prevents duplicate games in the merged plan. `seen_game_ids` set tracks primary IDs.

- **OK:** Plan hash is recomputed for the merged plan (lines 873-887) using a sorted canonical payload of game IDs and condition IDs.

- **OK:** `serialize_compiled_plan()` in `native_engine.py` (lines 65-69) correctly serializes `alternate_provider_game_ids` as `[{"provider": p, "game_id": pid}, ...]`. Matches Rust deserialization.

- **OK:** Opta `"Name|ID"` stripping in `linking/service.py` (lines 203-206) uses `rsplit("|", 1)` which correctly handles both pipe-containing and pipe-free strings.

- **EDGE CASE:** Alternate ID lookup key uses `(home_team, away_team, start_ts_utc)` at `compiler.py:701-704`. If `start_ts_utc` is NULL for an alt-row, it creates a `(team, team, None)` key. Plan games always have non-NULL kickoff (due to horizon filter at line 428-442), so plan games never look up NULL keys — no incorrect match occurs in practice. If the horizon filter were bypassed (e.g., `plan_horizon_hours=None`), two games with the same teams and both NULL timestamps could share alternates incorrectly.

- **EDGE CASE:** `compile_multi_league_plan` hardcodes `league="soccer"` and `provider="multi"` in the returned `CompiledPlan` (line 889-892). Acceptable since this function is only called from the soccer multi-league path.

---

## 2. Dual Game ID Mapping in Engine

### Files audited
- `native/polybot2_native/src/soccer/engine.rs`
- `native/polybot2_native/src/baseball/engine.rs`

### Findings

- **OK:** Both engines insert alternate IDs with `contains_key` guard (`soccer/engine.rs:166`, `baseball/engine.rs:573`). First insertion wins; no overwriting.

- **OK:** `game_ids` vector (canonical log output) is left unchanged in both engines. Only `game_id_to_idx` HashMap gets extra entries.

- **OK:** Soccer `merge_plan` (line 695-747) supports BOTH existing games (adds alternate IDs + new targets) AND new games (creates full per-game state vectors). Correct for V2 resolution which hot-patches new games.

- **OK:** Baseball `merge_plan` (line 566) only processes existing games (`let Some(&gidx) = self.game_id_to_idx.get(uid) else { continue; }`). This is correct by design — V2 resolution is soccer-only, and baseball incremental refresh only adds targets to existing events.

- **EDGE CASE:** If two canonical games share an alternate ID (provider ID collision), only the first game's entry exists in `game_id_to_idx`. The second game's alternate is silently dropped (no warning, no eprintln). Frames for the colliding ID route to the first game only. Low probability in practice (would require same provider ID assigned to different canonical games), but diagnostic logging would help detect it. Location: `soccer/engine.rs:166`, `baseball/engine.rs:573`.

---

## 3. Multiplexed WS Worker

### Files audited
- `native/polybot2_native/src/ws_multiplexed.rs`
- `native/polybot2_native/src/kalstrop_v2_sio.rs`
- `native/polybot2_native/src/kalstrop_v2_types.rs`

### Findings

- **BUG:** V2 subscription failures are silently marked as active. At `ws_multiplexed.rs:270-283`, when `SetCandidateSubscriptions` arrives, new V2 fixture IDs are subscribed one-by-one. If any `kalstrop_v2_sio::subscribe()` call fails (line 278-279), the error is logged but processing continues. At line 282, `v2_active_subs = new_v2` is set unconditionally — including the failed IDs. On the next subscription update, the failed IDs won't appear in `v2_new_ids` (they're already in `v2_active_subs`), so they're never retried. The game is silently lost to V2 for the remainder of the session. Fix: only add successfully-subscribed IDs to `v2_active_subs`, or mark the connection dead on subscribe failure to trigger reconnect.

- **BUG:** BoltOdds provider is configured in `runtime.rs:313-321` (pushed to `mux_providers` with a valid config), but silently dropped by the multiplexed worker at `ws_multiplexed.rs:127-129` (prints a WARN and ignores it). If a league is configured with `provider: ["boltodds", "kalstrop_v1"]`, BoltOdds frames will never be processed. Currently this doesn't hit in production (EPL uses BoltOdds as a single provider, not multiplexed), but the config path allows constructing it, and no validation rejects it at the Python level.

- **OK:** `partition_subscriptions()` (line 43-53) correctly splits by format: dashes → V1 UUID, no dashes → V2 numeric. V1 always uses UUIDs (`d3f41158-...`), V2 always uses numeric fixture IDs (`7490587`). Format is guaranteed by the provider catalog — no ambiguity in practice.

- **OK:** `std::future::pending()` correctly disables dead connection branches in `select!` (lines 311-314, 351-354). When `v1_ws.is_none()`, the async block awaits forever, so `select!` never picks that branch.

- **OK:** V1 `send_kalstrop_resubscribe_async` failure correctly marks V1 as dead: `v1_ws = None`, `v1_active_subs.clear()`, `reconn_v1 = true` (lines 261-263). Triggers reconnect on next outer loop iteration.

- **OK:** Backoff is exponential with cap: `2000 * (1 << max_count)` clamped to 30,000ms (line 218-219). Resets on successful connection (lines 179, 200). Reasonable for production.

- **OK:** `classify_frame()` in `kalstrop_v2_sio.rs` strips the Socket.IO envelope (`42["genius_update", {...}]`) and returns only the inner JSON payload. `fast_extract_v2()` then checks for `"betGeniusFixtureId"` in that payload — not the outer wrapper. Flow is consistent.

- **EDGE CASE:** V2 pong at line 372 awaits `send_pong(conn)` inside the V2 branch of `select!`. If V1 has a frame ready, it must wait for the pong send to complete. In practice, pong sends are sub-millisecond (one WS frame), so latency impact is negligible.

---

## 4. Multi-League Orchestrator

### Files audited
- `src/polybot2/_cli/commands_hotpath_runtime.py`
- `src/polybot2/_cli/args.py`

### Findings

- **BUG:** V2 resolution credentials not loaded for multi-league with non-V2 primary. At `commands_hotpath_runtime.py:232`, `is_v2_league = (provider_name == "kalstrop_v2")` uses the PRIMARY league's provider. At lines 369-373, V2 credentials are only loaded when `is_v2_league` is True. For `--league epl laliga` (epl=BoltOdds primary, laliga=V2 secondary), `is_v2_league` is False, so `v2_client_id` and `v2_shared_secret_raw` remain empty strings. At line 403, `try_resolve_games(due, client_id="", shared_secret_raw="")` fails with auth error. The exception is caught at line 480 (logged as warning, continues), so V2 games are never resolved for the session. Fix: load V2 credentials whenever `_v2_leagues` is non-empty, regardless of primary provider.

- **OK:** `--sport soccer` correctly filters by both `sport_family` and `live_betting_leagues` membership (lines 162-166).

- **OK:** `_primary_provider_for_league()` (lines 39-44) correctly handles both string and list `provider` values — returns first element for list, string directly otherwise.

- **OK:** `compile_multi_league_plan` catches `HotPathPlanError` with `scope_blocked` code (line 863) and continues to next league. Only raises if all fail (line 867). The caller at line 515-517 logs and returns 1.

- **OK:** V2 resolution correctly uses per-league provider (lines 394, 422-424): `_primary_provider_for_league(_v2_cfg)` is called per V2 league in the iteration.

- **OK:** `resolved.pending.league` is always populated — set by `build_pending_games(db, league=_v2_lk, ...)` which passes the league into the `V2PendingGame` constructor.

- **OK:** Multiplexed provider config (lines 270-297) correctly collects from ALL leagues via `_all_providers` set (lines 188-198). All providers get config entries.

- **EDGE CASE:** Order policy is per-primary-league only. At line 218-220, `_hotpath_order_policy_for_league(league_key=league_key)` uses `league_key = league_keys[0]` (the primary). All template generation (line 354, 444, 469) uses this single policy. If secondary leagues need different order sizing (e.g., smaller bets on exact score markets in La Liga vs EPL), this wouldn't be respected. Acceptable for now since all soccer leagues use the same execution policy.

---

## 5. Dual-Order Presign

### Files audited
- `native/polybot2_native/src/dispatch/types.rs`
- `native/polybot2_native/src/dispatch/flow.rs`
- `native/polybot2_native/src/dispatch/presign_pool.rs`
- `src/polybot2/hotpath/order_policy.py`
- `src/polybot2/_cli/common.py`
- `src/polybot2/hotpath/native_service.py`

### Findings

- **OK:** `warm_presign_startup_into` (presign_pool.rs:133-235) correctly handles dual-order. The outer loop (lines 160-162) creates one work item PER TEMPLATE (not per token). For a dual-order token with 2 templates, 2 work items are created. Each calls `sign_order_batch(&c, &s, &tpl, 1)` which returns a `Vec<SdkSignedOrder>` of length 1. Line 208 (`signed_orders.into_iter().next()`) extracts that single element and pushes to `pool[idx]`. After both work items complete, `pool[idx]` has 2 entries. Correct.

- **OK:** Verification at line 217-232 checks `pool[idx].is_empty()`. This is a final sanity check — if any signing call fails, the `?` at line 206 returns early with an error, aborting the entire warmup. By the time verification runs, all successful signs have been pushed.

- **OK:** `pop_for_target` (flow.rs:31-58) uses `std::mem::take(slot)` which moves the entire `SmallVec` contents (1-2 orders) out and leaves the slot empty. The caller receives all pre-signed orders for the target's token in one shot. Correct.

- **OK:** `_build_hotpath_template_orders` (common.py:113-172) correctly generates both primary (lines 123-146) and secondary (lines 147-171) template orders per token when `policy.has_secondary` is True. Each uses the same `token_id` with different amounts/prices/TIF. Dedup via `seen` set with position index.

- **OK:** `apply_incremental_refresh` (native_service.py:305-358) correctly emits secondary templates (lines 334-342) when `p.has_secondary` is True. Two entries per token are appended to the templates list.

- **OK:** Backward compatibility preserved. Without `secondary_*` fields, `OrderPolicy.has_secondary` returns False, only 1 template per token is generated, pool has depth 1 per token, and `pop_for_target` returns a SmallVec with 1 entry. Identical to pre-dual-order behavior.

- **EDGE CASE:** `SubmitBatch` capacity is `SmallVec<[(...); 32]>`. With dual-order, worst case is 16+ intents × 2 = 32+ entries per frame, which would spill to heap. A full exact-score goal frame (up to 20 intents × 2 = 40 entries) exceeds inline capacity. SmallVec gracefully spills — no correctness issue, just a heap allocation on those frames. Typical frames (2-4 intents) stay inline.

---

## 6. Hotpath Patch Plan (Hot-Patch Path)

### Files audited
- `native/polybot2_native/src/runtime.rs`
- `native/polybot2_native/src/ws.rs` (`apply_pending_patches`)
- `native/polybot2_native/src/dispatch/presign_pool.rs` (`extend_for_patch`)
- `native/polybot2_native/src/lib.rs`

### Findings

- **OK:** `patch_plan()` in runtime.rs (lines 509-611) correctly handles dual-order signing. The template map is built at lines 536-543 using `.entry().or_insert_with().push()` — correctly accumulates multiple templates per token. The signing loop (lines 561-592) flattens into `(token_id, request)` pairs (one per template), spawns each independently, and accumulates results via `.entry(token_id).or_insert_with(SmallVec::new).push(order)`. For dual-order: 2 templates → 2 work items → 2 signed orders → both pushed to the same SmallVec entry. Correct.

- **OK:** `extend_for_patch` (presign_pool.rs:98-123) correctly resizes both `presign_templates` and `presign_pool` to match the new registry length (lines 106-107). For new token indices (old_len..new_len), it installs templates from `new_templates` and loops over ALL signed orders from `new_presigned` (lines 115-121: `for signed in signed_orders`). Correctly handles 1 or 2 orders per token.

- **OK:** `PatchPayload` type (`lib.rs`) uses `HashMap<String, SmallVec<[SdkSignedOrder; 2]>>` for `new_presigned` and `HashMap<String, SmallVec<[OrderRequestData; 2]>>` for `new_templates`. Matches both producer (runtime.rs) and consumer (presign_pool.rs).

- **OK:** No race between patch application and frame drain. `apply_pending_patches()` is called at the start of each event loop iteration in the WS worker (ws_multiplexed.rs:166, 293), BEFORE the frame drain loop begins. Both patch application and frame processing happen on the same thread — they're serialized. `extend_for_patch` resizes and populates in one call (no window where pool is resized but empty).

- **OK:** Registry update order in `apply_pending_patches` (ws.rs): `extend_for_patch` → build new `Arc<TargetRegistry>` → `set_registry` on engine → `replace_registry` on dispatch handle → `ArcSwap::store` for submitter thread visibility. Pool is populated before registry is visible, so the submitter never sees a target without its corresponding pool entry.

---

## Summary

| Area | Finding | Type | Severity | Location |
|------|---------|------|----------|----------|
| 3 | V2 subscription failures marked as active, never retried | BUG | Medium | `ws_multiplexed.rs:282` |
| 3 | BoltOdds configured but silently dropped in multiplexed worker | BUG | Low | `ws_multiplexed.rs:127-129` |
| 4 | V2 credentials not loaded when primary league isn't V2 | BUG | High | `commands_hotpath_runtime.py:232,369-373` |
| 1 | NULL kickoff alternate ID key collision (horizon bypass) | EDGE CASE | Low | `compiler.py:701-704,743-746` |
| 2 | Alternate ID collision silently drops second (no warning) | EDGE CASE | Low | `soccer/engine.rs:166` |
| 4 | Order policy is per-primary-league for all leagues | EDGE CASE | Low | `commands_hotpath_runtime.py:218-220` |
| 5 | SubmitBatch spills to heap on 16+ dual-order intents | EDGE CASE | Low | `dispatch/types.rs` |

### Bugs requiring fix before deployment

1. **V2 credentials in multi-league mode** (`commands_hotpath_runtime.py:369-373`): Load V2 credentials whenever `_v2_leagues` is non-empty, not just when `is_v2_league`. Without this fix, `--league epl laliga` silently fails V2 resolution for La Liga.

2. **V2 subscription retry** (`ws_multiplexed.rs:282`): Only mark IDs as active if subscribe succeeded. Or track failed IDs and retry on next `SetCandidateSubscriptions`, or mark connection dead to trigger full reconnect.

3. **BoltOdds in multiplexed** (`ws_multiplexed.rs:127-129`): Either implement BoltOdds support in the multiplexed worker, or reject the config at the Python level with an error (don't silently drop it).
