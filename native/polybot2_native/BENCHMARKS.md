# polybot2_native Benchmarks

## What This Measures

In-crate benchmark tests that measure the Rust hotpath without modifying production functions. No `Instant::now()` or counters are added to `ws.rs`, `engine.rs`, `frame_pipeline.rs`, dispatch, or submitter code.

| Benchmark | What it measures | What it does NOT measure |
|-----------|-----------------|------------------------|
| `bench_layout_sizes` | `size_of` for hot-path types | — |
| `bench_mlb_parse_only` | `serde_json::from_str::<KalstropFrame>` on 542 captured V1 MLB frames | Engine eval, dispatch, logging |
| `bench_mlb_frame_pipeline_noop` | Full `process_decoded_frame_sync` with noop dispatch over 542 frames | Presign pop, channel send, HTTP |
| `bench_mlb_material_eval_noop` | Engine eval on a synthetic frame that triggers a totals-over intent (noop dispatch) | Presign pop, channel send, HTTP |
| `bench_mlb_material_pop_miss` | Engine eval + presign pop-miss error path (Http dispatch, empty pool) | Channel send (batch is empty after pop fails), HTTP |
| `bench_dispatch_channel_send` | `SmallVec` batch construction + `tokio::sync::mpsc::UnboundedSender::send` with 1 and 4 dummy signed orders | Engine eval, presign pop, HTTP, submitter |

This is NOT a full WS-to-wire benchmark. It does not place orders or hit the network.

## How to Run

**macOS (requires Python dylib on library path):**
```bash
DYLD_LIBRARY_PATH=/Users/reda/miniconda3/lib cargo test \
  --manifest-path native/polybot2_native/Cargo.toml \
  --no-default-features --features bench-support --release \
  -- bench_ --nocapture
```

**Linux / EC2 (Python is normally on the default library path):**
```bash
cargo test \
  --manifest-path native/polybot2_native/Cargo.toml \
  --no-default-features --features bench-support --release \
  -- bench_ --nocapture
```

**Run a single benchmark:**
```bash
DYLD_LIBRARY_PATH=/Users/reda/miniconda3/lib cargo test \
  --manifest-path native/polybot2_native/Cargo.toml \
  --no-default-features --features bench-support --release \
  -- bench_mlb_parse_only --nocapture
```

**Normal tests (unchanged, uses default `python-extension` feature):**
```bash
cargo test --manifest-path native/polybot2_native/Cargo.toml
```

### Why DYLD_LIBRARY_PATH?

Even with `--no-default-features --features bench-support` (which disables `pyo3/extension-module`), PyO3 types (`PyAny`, `PyDict`) are still compiled into the crate and the test binary links against `libpython3.12.dylib`. On macOS, the dynamic linker needs to find it via `DYLD_LIBRARY_PATH`. On Linux with a system Python install, this is usually not needed.

### Build profile

Benchmarks use `--release` which applies the `[profile.release]` settings: `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`. For production-representative numbers, also set:
```bash
RUSTFLAGS="-C target-cpu=native"
```

## Baseline (macOS ARM, local, not production EC2)

Captured 2026-05-02, release mode, Apple M-series. These are local reference numbers — production EC2 (x86, Linux) will differ.

### Timing

| Benchmark | p50 | mean | notes |
|-----------|-----|------|-------|
| `mlb_parse_only` | ~1.6 µs/frame | ~2.2 µs/frame | 542 captured frames, includes duplicates |
| `mlb_frame_pipeline_noop` | ~1.7 µs/frame | ~2.4 µs/frame | parse + engine + noop dispatch |
| `mlb_material_eval_noop` | 1.5 µs | 2.5 µs | setup + trigger pair (score crosses over 8.5) |
| `mlb_material_pop_miss` | 1.7 µs | 2.0 µs | setup + trigger pair (pop fails, error logged) |
| `dispatch_channel_send` (1 intent) | 125 ns | 542 ns | batch build + unbounded mpsc send |
| `dispatch_channel_send` (4 intents) | 125 ns | 647 ns | batch build + unbounded mpsc send |

### Layout sizes

| Type | Bytes |
|------|------:|
| `SdkSignedOrder` | 432 |
| `SubmitWork` | 1776 |
| `SubmitBatch` (SmallVec inline) | 1768 |
| `(TargetIdx, SdkSignedOrder)` | 440 |
| SmallVec inline capacity (4×) | 1760 |
| `LiveTickResult` | 152 |
| `GameState` | 120 |
| `KalstropFrame` | 88 |

### Interpretation

- **JSON parse dominates** the captured-frame path. Most frames are duplicate state (531/542 are repeat `(score, freeText)` tuples), so the engine dedup check returns early after the parse.
- **The material eval path is ~1.5 µs p50** for a setup+trigger pair including parse, engine eval, and intent emission — well within the sub-µs-after-parse budget.
- **Channel send is ~125 ns p50** for both 1 and 4 intents. The SmallVec inline capacity (1.8 KB) doesn't noticeably affect send cost — the unbounded mpsc dominates. This confirms Tokio mpsc is fast enough for the current workload.
- **`SdkSignedOrder` is 432 bytes.** The `SmallVec<[(TargetIdx, SdkSignedOrder); 4]>` inline capacity is ~1.8 KB on the stack. This is a known cost — a future optimization could use `Box` or pool indices for signed orders.
- The harness is useful for **pre/post regression checks** when adding soccer or refactoring the engine.

## Fixture Data

Benchmarks use `captures/whitesox_angels/v1_raw.jsonl` (550 lines: 8 `connection_ack`, 542 `next` frames, ~1.3 KB average). The `.frame` JSON is extracted and reserialized via `serde_json::Value::to_string()` — whitespace may differ slightly from live WS frames.
