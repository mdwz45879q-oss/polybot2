# Polybot2 Session Handoff (Updated 2026-04-23)

## 1) What `polybot2` Is

`polybot2` is a sports-trading bot package for Polymarket with:
1. A Rust-native hotpath (`polybot2_native`) for low-latency ingest -> decision -> dispatch.
2. A Python control plane for CLI orchestration, data sync, linking/review, policy wiring, and runtime launch.
3. Operator workflows for `hotpath run`, `hotpath replay`, and `hotpath observe`.

Primary package purpose:
1. Build and maintain tradable mapping/link state.
2. Run low-latency live/paper hotpath for MLB (current v1 focus).
3. Keep a practical operator loop (launch gate, observe, replay correctness checks).

## 2) Current Package State

High-level status:
1. Hotpath architecture is simplified to one Rust-first runtime path (benchmark command removed; replay retained for correctness).
2. Rust dispatch is split into focused modules (`types`, `sdk_exec`, `presign_pool`, `flow`, `events`) and uses SDK-native submit/cancel/get flow.
3. `market_family` was removed from active linking/hotpath/native contracts; `sports_market_type` is the only classification source.
4. Native crate tests are green locally after recent cleanup/refactor passes.

Recent material changes:
1. Rustls startup panic was fixed by enforcing a single process-level rustls provider (`aws-lc-rs`) and installing it at runtime start.
2. Native dead-code cleanup removed remaining unused sync paths and stale fields; native module rebuilt via `maturin develop`.
3. In paper mode (`dispatch=noop`), startup presign warm gate is currently bypassed to avoid unnecessary startup failure.

Current critical problems:
1. **Live presign startup warmup bug remains unresolved:** warm gate can timeout at depth `4/8` across many tokens (`presign_startup_warm_timeout`) due to current refill/warm behavior under startup load.
2. **Observe mode is still operationally broken:** scoreboard can remain empty (`tracked=0`, `live=0`) despite active WS/exec connections.
3. **Event emission/telemetry pipeline needs redesign:** current output is insufficiently reliable/useful for operators, and observe derivation is not robust.
4. Operator UX remains weak for runtime health clarity, failure diagnosis, and actionable status.

Latest runtime note (2026-04-23):
1. Paper-mode startup can run without the warm-timeout shutdown, but this is not a full live-readiness fix.
2. Live-readiness still requires resolving the presign warm gate bug and observe/telemetry correctness.

## 3) Near-Future Priority Tasks

### Task 0: Comprehensive Audit/Review of Entire `polybot2` Package (Mandatory First Step)

Requirement:
1. Before implementing fixes, run a comprehensive audit and review across Python + Rust.
2. Cover architecture, contracts, runtime flows, failure modes, stale code paths, operator workflows, and test gaps.

Deliverable:
1. A concrete audit report with prioritized findings, root causes, and an implementation sequence.
2. Explicit callouts for presign warmup/live readiness and observe/telemetry correctness.

### Task A: Fix Live Presign Startup Warmup Bug (Do Not Mask in Paper-Only Logic)

Problem:
1. Startup warm gate can timeout (`target_depth=8`, widespread `:4` depth) under larger active token sets.
2. Current behavior is acceptable for paper startup continuity, but does not satisfy live-readiness goals.

Goal:
1. Make presign warmup deterministic and reliable for live startup.
2. Preserve fail-closed behavior while eliminating avoidable warm-timeout failures.

Expected outcome:
1. Live startup reaches configured presign depth within bounded time or fails with precise actionable diagnostics.
2. No hidden dependence on paper-mode bypass for correctness.

### Task B: Overhaul Event Emission, Telemetry, and Observe Mode

Problem:
1. Observe currently fails to represent real runtime state in common live runs.
2. High-value event set exists, but end-to-end observe derivation is incomplete and not operator-grade.

Goal:
1. Redesign event -> telemetry -> observe pipeline so scoreboard/logs are trustworthy.
2. Ensure tracked/live/final/upcoming counters reflect runtime truth from active subscriptions and game state.

Expected outcome:
1. Observe mode becomes operationally useful and stable during live sessions.
2. Event emission remains non-blocking and does not violate trigger/submit ordering constraints.

### Task C: Continue Speed/Clarity Cleanup of Rust Hotpath

Goal:
1. Keep one fast path, minimal branch/load on trigger path, and strict fail-closed semantics.
2. Remove remaining low-value complexity once audit findings are integrated.

Targets:
1. No new fallbacks or telemetry spam.
2. Preserve trigger-path invariants and execution contract strictness.
3. Keep module boundaries coherent and maintainable.

## 4) Important Context and Constraints

1. Deployment target is Linux EC2 (`eu-west-1`), latency-sensitive runtime.
2. Current operational scope remains MLB + Kalstrop in v1.
3. Rust native module is mandatory for hotpath runtime.
4. Fail-closed posture is preferred for live trading; fallback complexity should not be reintroduced.
5. **Hotpath ordering invariant (must keep):**
   - When a match update is received, hotpath must not emit any telemetry (high-value observe) event until after decision work is complete and, if applicable, after order submission attempt is complete.
   - Telemetry and any non-essential logic must never block, delay, or interfere with decision-making or submit execution.
6. Live-readiness bar:
   - Paper-mode continuity is not sufficient proof.
   - Critical startup/runtime bugs must be fixed for live mode explicitly.

## 5) Suggested First Moves Next Session

1. Run the comprehensive package audit first (mandatory):
   - map all active runtime flows and contract surfaces,
   - identify root causes for live presign warm timeout and observe emptiness,
   - produce a prioritized implementation backlog with explicit acceptance checks.
2. Implement live presign warmup fix immediately after audit:
   - address refill/warm startup behavior under many active tokens,
   - validate with live-like token counts and startup timing gates.
3. Execute telemetry/event/observe overhaul:
   - redesign state derivation for scoreboard counters,
   - enforce event quality and sequencing guarantees,
   - validate with real run traces.
4. Finish with operator UX hardening:
   - make startup/runtime/failure messages actionable,
   - expose clear health, subscription, and execution visibility.
