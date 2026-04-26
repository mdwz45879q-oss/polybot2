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

    let mut all_intents: Vec<(&str, &str)> = Vec::new();
    for result in &results {
        if !result.material {
            continue;
        }
        for intent in &result.intents {
            all_intents.push((&intent.strategy_key, &intent.token_id));
        }
    }

    let order_outcomes: Vec<(&str, &str, Result<String, String>)> = if all_intents.len() <= 1 {
        let mut outcomes = Vec::new();
        for &(sk, tok) in &all_intents {
            let outcome = dispatch_runtime.dispatch_order(tok).await;
            outcomes.push((sk, tok, outcome));
        }
        outcomes
    } else {
        let token_ids: Vec<&str> = all_intents.iter().map(|&(_, tok)| tok).collect();
        let batch_results = dispatch_runtime.dispatch_orders_batch(&token_ids).await;
        all_intents
            .iter()
            .zip(batch_results)
            .map(|(&(sk, tok), result)| (sk, tok, result))
            .collect()
    };

    for result in &results {
        if !result.material {
            continue;
        }
        log.log_tick(
            &result.game_id,
            result.state.home,
            result.state.away,
            result.state.inning_number,
            result.state.inning_half,
            result.state.game_state,
        );
    }

    for (sk, tok, outcome) in &order_outcomes {
        match outcome {
            Ok(eid) => log.log_order_ok(sk, tok, eid),
            Err(err) => log.log_order_err(sk, tok, err),
        }
    }
}
