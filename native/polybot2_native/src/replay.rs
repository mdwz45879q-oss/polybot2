use super::*;
use crate::dispatch::DispatchRuntime;
use crate::engine::process_score_frame_value;
use crate::telemetry::TelemetryEmitter;

#[derive(Default)]
pub(crate) struct TelemetryRuntimeState {
    chain_seq: u64,
}

impl TelemetryRuntimeState {
    fn next_chain_id(&mut self, game_id: &str, recv_monotonic_ns: i64) -> String {
        self.chain_seq = self.chain_seq.saturating_add(1);
        format!(
            "{}:{}:{}",
            game_id,
            recv_monotonic_ns.max(0),
            self.chain_seq
        )
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct FrameProcessStats {
    pub messages_action: i64,
    pub messages_no_action: i64,
    pub frame_events_in: i64,
    pub route_tasks_executed: i64,
    pub route_errors: i64,
    pub dispatch_errors: i64,
    pub drops_cooldown: i64,
    pub drops_debounce: i64,
    pub drops_one_shot: i64,
    pub decision_non_material: i64,
    pub decision_no_action: i64,
}

fn emit_observe_signals(
    telemetry: Option<&TelemetryEmitter>,
    telemetry_state: &mut TelemetryRuntimeState,
    recv_monotonic_ns: i64,
    observe_signals: &[ObserveSignal],
) {
    let Some(emitter) = telemetry else { return };
    for signal in observe_signals.iter() {
        let event_type = signal.event_type.as_str();
        if event_type != "score_changed" && event_type != "game_state_changed" {
            continue;
        }
        let game_id = signal.game_id.as_str();
        let chain_id = telemetry_state.next_chain_id(game_id, recv_monotonic_ns);
        emitter.emit(
            event_type,
            game_id,
            chain_id.as_str(),
            "",
            "",
            "",
            "",
            signal.payload.clone(),
        );
    }
}

pub(crate) async fn process_decoded_frame_async<F>(
    engine: &mut NativeMlbEngine,
    frame: &Value,
    recv_monotonic_ns: i64,
    source_recv_monotonic_ns: i64,
    dispatch_runtime: &mut DispatchRuntime,
    telemetry: Option<&TelemetryEmitter>,
    telemetry_state: &mut TelemetryRuntimeState,
    mut on_dispatch_error: F,
) -> FrameProcessStats
where
    F: FnMut(String),
{
    let mut stats = FrameProcessStats::default();

    let (mut out, events_in) =
        process_score_frame_value(engine, frame, recv_monotonic_ns, source_recv_monotonic_ns);
    stats.frame_events_in = events_in.max(0);
    stats.drops_cooldown = out.drops_cooldown;
    stats.drops_debounce = out.drops_debounce;
    stats.drops_one_shot = out.drops_one_shot;
    stats.decision_non_material = out.decision_non_material;
    stats.decision_no_action = out.decision_no_action;

    if events_in <= 0 || out.intents.is_empty() {
        stats.messages_no_action = 1;
        emit_observe_signals(
            telemetry,
            telemetry_state,
            recv_monotonic_ns,
            out.observe_signals.as_slice(),
        );
        return stats;
    }

    stats.messages_action = 1;
    for intent in out.intents.iter_mut() {
        let game_id = intent.source_universal_id.as_str();
        let chain_id = telemetry_state.next_chain_id(game_id, recv_monotonic_ns);
        intent.chain_id = chain_id.clone();

        if let Err(err) = dispatch_runtime.dispatch_intent_async(intent).await {
            stats.dispatch_errors += 1;
            stats.route_errors += 1;
            if let Some(emitter) = telemetry {
                emitter.emit(
                    "exec_error",
                    game_id,
                    chain_id.as_str(),
                    intent.strategy_key.as_str(),
                    "",
                    "",
                    err.as_str(),
                    json!({"source": "dispatch_intent_async"}),
                );
            }
            on_dispatch_error(err);
        }
        stats.route_tasks_executed += 1;
    }

    emit_observe_signals(
        telemetry,
        telemetry_state,
        recv_monotonic_ns,
        out.observe_signals.as_slice(),
    );

    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Builder as TokioBuilder;

    #[test]
    fn shared_pipeline_records_no_action_message() {
        let mut engine = NativeMlbEngine::new(2.0, 0.5, 0.1, 5.0, 0.52, "GTC".to_string());
        let frame = json!({
            "type":"next",
            "payload":{
                "data":{
                    "sportsMatchStateUpdatedV2":{
                        "fixtureId":"g1",
                        "matchSummary":{"eventState":"live","homeScore":0,"awayScore":0}
                    }
                }
            }
        });
        let mut dispatch_runtime = DispatchRuntime::new(DispatchConfig::default(), None);
        let mut telemetry_state = TelemetryRuntimeState::default();

        let rt = TokioBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let stats = rt.block_on(process_decoded_frame_async(
            &mut engine,
            &frame,
            1_000,
            1_000,
            &mut dispatch_runtime,
            None,
            &mut telemetry_state,
            |_err| {},
        ));

        assert_eq!(stats.messages_no_action, 1);
        assert_eq!(stats.messages_action, 0);
    }
}
