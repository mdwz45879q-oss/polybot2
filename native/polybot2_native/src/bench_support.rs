//! Benchmark-only helpers. Feature-gated behind `bench-support`.
//! Not part of the public API.

use crate::baseball::frame_pipeline::process_decoded_frame_sync;
use crate::baseball::types::*;
use crate::dispatch::{DispatchHandle, SubmitWork};
use crate::fast_extract::fast_extract_v1;
use crate::kalstrop_types::KalstropFrame;
use crate::log_writer::LogWriter;
use crate::*;
use std::sync::{Arc, Mutex};

/// Opaque wrapper around engine + dispatch state for benchmarks.
pub struct BenchEngine {
    engine: NativeMlbEngine,
    dispatch: DispatchHandle,
    log: Arc<Mutex<LogWriter>>,
    _rx: Option<rtrb::Consumer<SubmitWork>>,
}

fn shared_registry_for(registry: Arc<crate::TargetRegistry>) -> crate::dispatch::SharedRegistry {
    Arc::new(arc_swap::ArcSwap::new(registry))
}

impl BenchEngine {
    /// Reset engine game state between benchmark iterations.
    pub fn reset(&mut self) {
        self.engine.reset_runtime_state();
    }
}

/// Parse a single Kalstrop V1 frame string. Returns true if parsing succeeded.
pub fn bench_parse_frame(frame: &str) -> bool {
    serde_json::from_str::<KalstropFrame<'_>>(frame).is_ok()
}

/// Fast byte-level extract of a V1 frame. Returns true if extraction succeeded.
pub fn bench_fast_extract_frame(frame: &str) -> bool {
    fast_extract_v1(frame).is_some()
}

/// Build a bench engine from a plan JSON in noop dispatch mode.
/// No channel, no presign pool — evaluates but doesn't dispatch.
pub fn build_bench_engine_noop(plan_json: &str) -> Result<BenchEngine, String> {
    let mut engine = NativeMlbEngine::new();
    engine.load_plan_from_json(plan_json)?;
    let registry = engine
        .clone_registry()
        .ok_or_else(|| "registry_not_built".to_string())?;
    let cfg = DispatchConfig::default(); // Noop mode
    let shared_registry = shared_registry_for(Arc::clone(&registry));
    let dispatch = DispatchHandle::new(cfg, registry, shared_registry);
    let log = LogWriter::open("/dev/null").map_err(|e| format!("log_open:{}", e))?;
    Ok(BenchEngine {
        engine,
        dispatch,
        log: Arc::new(Mutex::new(log)),
        _rx: None,
    })
}

/// Build a bench engine with a real channel (receiver kept alive but unpolled).
/// Presign pool is empty — `pop_for_target` will fail closed, but the batch
/// build + channel send path is exercised.
pub fn build_bench_engine_with_channel(plan_json: &str) -> Result<BenchEngine, String> {
    let mut engine = NativeMlbEngine::new();
    engine.load_plan_from_json(plan_json)?;
    let registry = engine
        .clone_registry()
        .ok_or_else(|| "registry_not_built".to_string())?;
    let mut cfg = DispatchConfig::default();
    cfg.mode = DispatchMode::Http;
    let shared_registry = shared_registry_for(Arc::clone(&registry));
    let mut dispatch = DispatchHandle::new(cfg, registry, shared_registry);
    let (tx, rx) = rtrb::RingBuffer::<SubmitWork>::new(64);
    dispatch.install_submit_tx(tx);
    let log = LogWriter::open("/dev/null").map_err(|e| format!("log_open:{}", e))?;
    Ok(BenchEngine {
        engine,
        dispatch,
        log: Arc::new(Mutex::new(log)),
        _rx: Some(rx),
    })
}

/// Process a frame through the noop pipeline. Returns true if material.
pub fn bench_process_frame_noop(bench: &mut BenchEngine, frame: &str, mono_ns: i64) {
    process_decoded_frame_sync(
        &mut bench.engine,
        frame,
        mono_ns,
        &mut bench.dispatch,
        &bench.log,
    );
}

/// Process a frame through the Http dispatch path with an empty presign pool.
/// `pop_for_target` will fail closed (presign miss) — this measures parse +
/// engine eval + pop-miss error logging. It does NOT measure channel send
/// because the batch is empty after the pop failure.
pub fn bench_process_frame_pop_miss(bench: &mut BenchEngine, frame: &str, mono_ns: i64) {
    process_decoded_frame_sync(
        &mut bench.engine,
        frame,
        mono_ns,
        &mut bench.dispatch,
        &bench.log,
    );
}

/// Print layout sizes of hot-path types.
pub fn layout_sizes() {
    use crate::dispatch::PreparedOrderPayload;
    use std::mem::size_of;
    println!("=== Layout Sizes ===");
    println!(
        "PreparedPayload:   {:>4} bytes",
        size_of::<PreparedOrderPayload>()
    );
    println!("SubmitWork:        {:>4} bytes", size_of::<SubmitWork>());
    println!(
        "SubmitBatch:       {:>4} bytes",
        size_of::<crate::dispatch::SubmitBatch>()
    );
    println!(
        "(TargetIdx, Box<Prepared>): {:>3} bytes",
        size_of::<(TargetIdx, Box<PreparedOrderPayload>)>()
    );
    println!(
        "SmallVec inline:   {:>4} bytes (4 × {})",
        4 * size_of::<(TargetIdx, Box<PreparedOrderPayload>)>(),
        size_of::<(TargetIdx, Box<PreparedOrderPayload>)>(),
    );
    println!(
        "LiveTickResult:    {:>4} bytes",
        size_of::<LiveTickResult>()
    );
    println!("GameState:         {:>4} bytes", size_of::<GameState>());
    println!(
        "KalstropFrame:     {:>4} bytes",
        size_of::<KalstropFrame<'_>>()
    );
    println!("====================");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    const CAPTURE_PATH: &str = "../../captures/2026_05_04/rays_jays/v1_raw.jsonl";

    const BENCH_PLAN: &str = r#"{
        "provider": "kalstrop",
        "league": "mlb",
        "run_id": 1,
        "games": [{
            "provider_game_id": "9e32357e-1005-4b1f-bad2-71cc8422091b",
            "kickoff_ts_utc": null,
            "canonical_home_team": "LAA",
            "canonical_away_team": "CWS",
            "markets": [
                {"sports_market_type": "totals", "line": 8.5, "targets": [
                    {"token_id": "tok_over_8_5", "condition_id": "c1", "strategy_key": "g:TOTAL:OVER:8.5", "outcome_semantic": "over"},
                    {"token_id": "tok_under_8_5", "condition_id": "c1", "strategy_key": "g:TOTAL:UNDER:8.5", "outcome_semantic": "under"}
                ]},
                {"sports_market_type": "moneyline", "line": null, "targets": [
                    {"token_id": "tok_ml_h", "condition_id": "c2", "strategy_key": "g:MONEYLINE:HOME", "outcome_semantic": "home"},
                    {"token_id": "tok_ml_a", "condition_id": "c2", "strategy_key": "g:MONEYLINE:AWAY", "outcome_semantic": "away"}
                ]}
            ]
        }]
    }"#;

    fn load_capture_next_frames() -> Vec<String> {
        let path = std::path::Path::new(CAPTURE_PATH);
        if !path.exists() {
            return vec![];
        }
        let content = std::fs::read_to_string(path).unwrap();
        let mut next_frames = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
                let frame = &val["frame"];
                if frame.get("type").and_then(|t| t.as_str()) == Some("next") {
                    next_frames.push(frame.to_string());
                }
            }
        }
        next_frames
    }

    fn run_timed(name: &str, iterations: usize, mut f: impl FnMut()) {
        // Warmup
        for _ in 0..3 {
            f();
        }
        let mut times = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            f();
            times.push(start.elapsed());
        }
        times.sort();
        let sum: Duration = times.iter().sum();
        let mean = sum / iterations as u32;
        let p50 = times[iterations / 2];
        let p90 = times[iterations * 9 / 10];
        let p99 = times[iterations * 99 / 100];
        let min = times[0];
        let max = times[iterations - 1];
        println!(
            "[{}] n={} mean={:?} p50={:?} p90={:?} p99={:?} min={:?} max={:?}",
            name, iterations, mean, p50, p90, p99, min, max
        );
    }

    #[test]
    fn bench_layout_sizes() {
        layout_sizes();
    }

    #[test]
    fn bench_mlb_parse_only() {
        let frames = load_capture_next_frames();
        if frames.is_empty() {
            println!("SKIP: capture file not found at {}", CAPTURE_PATH);
            return;
        }
        println!("Loaded {} next frames from capture", frames.len());
        run_timed("mlb_parse_only (all frames)", 100, || {
            for frame in &frames {
                std::hint::black_box(bench_parse_frame(frame));
            }
        });
        let per_frame_ns = {
            let start = Instant::now();
            let iters = 1000;
            for _ in 0..iters {
                for frame in &frames {
                    std::hint::black_box(bench_parse_frame(frame));
                }
            }
            let total = start.elapsed();
            total.as_nanos() / (iters * frames.len()) as u128
        };
        println!("  ~{} ns/frame average", per_frame_ns);
    }

    #[test]
    fn bench_mlb_frame_pipeline_noop() {
        let frames = load_capture_next_frames();
        if frames.is_empty() {
            println!("SKIP: capture file not found at {}", CAPTURE_PATH);
            return;
        }
        let mut bench = build_bench_engine_noop(BENCH_PLAN).unwrap();
        run_timed("mlb_frame_pipeline_noop (all frames)", 100, || {
            bench.reset();
            let mut mono = 1_000_000_000i64;
            for frame in &frames {
                bench_process_frame_noop(&mut bench, frame, mono);
                mono += 100_000_000;
            }
        });
    }

    #[test]
    fn bench_mlb_material_eval_noop() {
        // Synthetic frames that trigger an intent
        let setup = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"9e32357e-1005-4b1f-bad2-71cc8422091b","matchSummary":{"homeScore":"5","awayScore":"0","matchStatusDisplay":[{"freeText":"3rd inning top"}]}}}}}"#;
        let trigger = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"9e32357e-1005-4b1f-bad2-71cc8422091b","matchSummary":{"homeScore":"6","awayScore":"3","matchStatusDisplay":[{"freeText":"4th inning top"}]}}}}}"#;

        let mut bench = build_bench_engine_noop(BENCH_PLAN).unwrap();
        run_timed("mlb_material_eval_noop (setup+trigger)", 1000, || {
            bench.reset();
            bench_process_frame_noop(&mut bench, setup, 1000);
            bench_process_frame_noop(&mut bench, trigger, 2000);
        });
    }

    #[test]
    fn bench_mlb_material_pop_miss() {
        let setup = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"9e32357e-1005-4b1f-bad2-71cc8422091b","matchSummary":{"homeScore":"5","awayScore":"0","matchStatusDisplay":[{"freeText":"3rd inning top"}]}}}}}"#;
        let trigger = r#"{"id":"v1_sub","type":"next","payload":{"data":{"sportsMatchStateUpdatedV2":{"fixtureId":"9e32357e-1005-4b1f-bad2-71cc8422091b","matchSummary":{"homeScore":"6","awayScore":"3","matchStatusDisplay":[{"freeText":"4th inning top"}]}}}}}"#;

        let mut bench = build_bench_engine_with_channel(BENCH_PLAN).unwrap();
        // Presign pool is empty → pop_for_target fails closed → batch stays empty.
        // Measures: parse → engine eval → pop-miss error log. Does NOT measure channel send.
        run_timed("mlb_material_pop_miss (setup+trigger)", 1000, || {
            bench.reset();
            bench_process_frame_pop_miss(&mut bench, setup, 1000);
            bench_process_frame_pop_miss(&mut bench, trigger, 2000);
        });
    }

    #[test]
    fn bench_dispatch_channel_send() {
        use alloy::primitives::{Signature, U256};
        use polymarket_client_sdk_v2::auth::ApiKey;
        use polymarket_client_sdk_v2::clob::types::{
            OrderPayload, OrderType, SignedOrder as SdkSignedOrder,
        };

        // Construct a dummy SignedOrder via bon builder (struct is #[non_exhaustive]).
        fn make_dummy_signed_order() -> SdkSignedOrder {
            SdkSignedOrder::builder()
                .payload(OrderPayload::default())
                .signature(Signature::new(U256::ZERO, U256::ZERO, false))
                .order_type(OrderType::GTC)
                .owner(ApiKey::nil())
                .build()
        }

        let (mut tx, mut _rx) = rtrb::RingBuffer::<crate::dispatch::SubmitWork>::new(16384);

        run_timed("dispatch_channel_send (1 intent)", 10000, || {
            let signed = make_dummy_signed_order();
            let mut batch = crate::dispatch::SubmitBatch::new();
            let prepared = crate::dispatch::prepare_payload_from_signed(signed)
                .expect("serialize signed order");
            batch.push((TargetIdx(0), Box::new(prepared)));
            let work = crate::dispatch::SubmitWork::Batch(batch);
            let _ = tx.push(work);
            while _rx.pop().is_ok() {}
        });

        run_timed("dispatch_channel_send (4 intents)", 10000, || {
            let mut batch = crate::dispatch::SubmitBatch::new();
            for i in 0..4u16 {
                let prepared =
                    crate::dispatch::prepare_payload_from_signed(make_dummy_signed_order())
                        .expect("serialize signed order");
                batch.push((TargetIdx(i), Box::new(prepared)));
            }
            let work = crate::dispatch::SubmitWork::Batch(batch);
            let _ = tx.push(work);
            while _rx.pop().is_ok() {}
        });
    }

    #[test]
    fn bench_submitter_chunk_parallel_synthetic() {
        let delay = Duration::from_millis(2);
        let iters = 30u64;
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let serial_start = Instant::now();
        for _ in 0..iters {
            let _ = tokio_rt.block_on(crate::dispatch::simulate_chunk_parallelism_for_test(
                30, 15, 1, delay,
            ));
        }
        let serial = serial_start.elapsed();

        let parallel_start = Instant::now();
        for _ in 0..iters {
            let _ = tokio_rt.block_on(crate::dispatch::simulate_chunk_parallelism_for_test(
                30, 15, 3, delay,
            ));
        }
        let parallel = parallel_start.elapsed();

        let serial_ms = serial.as_secs_f64() * 1000.0 / iters as f64;
        let parallel_ms = parallel.as_secs_f64() * 1000.0 / iters as f64;
        println!(
            "[submitter_chunk_parallel_synthetic] serial_avg_ms={:.3} parallel_avg_ms={:.3} speedup={:.2}x",
            serial_ms,
            parallel_ms,
            serial_ms / parallel_ms.max(0.000_001)
        );
        assert!(
            parallel < serial,
            "expected parallel < serial, serial={:?}, parallel={:?}",
            serial,
            parallel
        );
    }

    #[test]
    fn bench_submitter_spawn_overhead_proxy() {
        let iters = 20_000usize;
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let (spawn_elapsed, inline_elapsed) = tokio_rt
            .block_on(crate::dispatch::simulate_submitter_spawn_vs_inline_overhead_for_test(iters));
        let spawn_us = spawn_elapsed.as_secs_f64() * 1_000_000.0 / iters as f64;
        let inline_us = inline_elapsed.as_secs_f64() * 1_000_000.0 / iters as f64;
        println!(
            "[submitter_spawn_overhead_proxy] spawn_avg_us={:.3} inline_avg_us={:.3} ratio={:.2}x",
            spawn_us,
            inline_us,
            spawn_us / inline_us.max(0.000_001)
        );
        assert!(spawn_elapsed > inline_elapsed);
    }

    #[test]
    fn bench_submitter_small_batch_single_call_synthetic() {
        let delay = Duration::from_millis(2);
        let iters = 40u64;
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let start = Instant::now();
        for _ in 0..iters {
            let (_elapsed, max_inflight) = tokio_rt.block_on(
                crate::dispatch::simulate_chunk_parallelism_for_test(8, 15, 3, delay),
            );
            assert_eq!(max_inflight, 1);
        }
        let total = start.elapsed();
        let avg_ms = total.as_secs_f64() * 1000.0 / iters as f64;
        println!(
            "[submitter_small_batch_single_call_synthetic] avg_ms={:.3}",
            avg_ms
        );
    }

    #[test]
    fn bench_mlb_fast_extract_only() {
        let frames = load_capture_next_frames();
        if frames.is_empty() {
            println!("SKIP: capture file not found at {}", CAPTURE_PATH);
            return;
        }
        println!("Loaded {} next frames from capture", frames.len());

        let serde_ns = {
            let start = Instant::now();
            let iters = 1000u64;
            for _ in 0..iters {
                for frame in &frames {
                    std::hint::black_box(bench_parse_frame(frame));
                }
            }
            start.elapsed().as_nanos() / (iters as u128 * frames.len() as u128)
        };
        println!("  serde_json parse: ~{} ns/frame", serde_ns);

        let fast_ns = {
            let start = Instant::now();
            let iters = 1000u64;
            for _ in 0..iters {
                for frame in &frames {
                    std::hint::black_box(bench_fast_extract_frame(frame));
                }
            }
            start.elapsed().as_nanos() / (iters as u128 * frames.len() as u128)
        };
        println!("  fast_extract:     ~{} ns/frame", fast_ns);
        if fast_ns > 0 {
            println!("  speedup: {:.1}x", serde_ns as f64 / fast_ns as f64);
        }
    }

    #[test]
    fn test_fast_extract_matches_serde() {
        let frames = load_capture_next_frames();
        if frames.is_empty() {
            println!("SKIP: capture file not found at {}", CAPTURE_PATH);
            return;
        }
        let mut checked = 0;
        for (i, frame) in frames.iter().enumerate() {
            let serde_result = serde_json::from_str::<KalstropFrame<'_>>(frame).ok();
            let fast_result = fast_extract_v1(frame);

            if let Some(sf) = &serde_result {
                if sf.msg_type == "next" {
                    let update = sf
                        .payload
                        .as_ref()
                        .and_then(|p| p.data.as_ref())
                        .and_then(|d| d.update.as_ref());
                    if let (Some(u), Some(ff)) = (update, &fast_result) {
                        let summary = u.match_summary.as_ref();
                        let serde_home = summary.and_then(|s| s.home_score).unwrap_or("");
                        let serde_away = summary.and_then(|s| s.away_score).unwrap_or("");
                        let serde_ft = summary.and_then(|s| s.first_free_text).unwrap_or("");
                        assert_eq!(u.fixture_id, ff.fixture_id, "frame {}: fixture_id", i);
                        assert_eq!(serde_home, ff.home_score, "frame {}: home_score", i);
                        assert_eq!(serde_away, ff.away_score, "frame {}: away_score", i);
                        assert_eq!(serde_ft, ff.free_text, "frame {}: free_text", i);
                        checked += 1;
                    }
                }
            }
        }
        println!(
            "Verified {} frames match between serde and fast_extract",
            checked
        );
        assert!(checked > 0, "no frames were checked");
    }
}
