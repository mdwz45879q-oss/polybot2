use super::*;
use crate::dispatch::DispatchRuntime;
use crate::engine::process_kalstrop_frame;
use crate::log_writer::LogWriter;

pub(crate) async fn process_decoded_frame_async(
    engine: &mut NativeMlbEngine,
    frame_text: &str,
    recv_monotonic_ns: i64,
    dispatch_runtime: &mut DispatchRuntime,
    log: &mut LogWriter,
) {
    let results = process_kalstrop_frame(engine, frame_text, recv_monotonic_ns);

    for result in &results {
        if !result.material {
            continue;
        }

        // Dispatch all intents, collecting outcomes
        let mut order_outcomes: Vec<(&str, &str, Result<String, String>)> = Vec::new();
        for intent in &result.intents {
            let outcome = dispatch_runtime.dispatch_order(&intent.token_id).await;
            order_outcomes.push((&intent.strategy_key, &intent.token_id, outcome));
        }

        // Log tick (after dispatch, off critical path)
        log.log_tick(
            &result.game_id,
            result.state.home,
            result.state.away,
            result.state.inning_number,
            result.state.inning_half,
            result.state.game_state,
        );

        // Log order outcomes
        for (sk, tok, outcome) in &order_outcomes {
            match outcome {
                Ok(eid) => log.log_order_ok(sk, tok, eid),
                Err(err) => log.log_order_err(sk, tok, err),
            }
        }
    }
}
