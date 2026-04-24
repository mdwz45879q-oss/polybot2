use super::*;

pub(super) fn normalize_status(status: &str) -> String {
    let lowered = status.trim().to_lowercase();
    if lowered.is_empty() {
        return "submitted".to_string();
    }
    if lowered == "live" {
        return "open".to_string();
    }
    lowered
}

impl DispatchRuntime {
    pub(super) fn emit_event(
        &self,
        event_type: &str,
        game_id: &str,
        chain_id: &str,
        strategy_key: &str,
        order_client_id: &str,
        order_exchange_id: &str,
        reason_code: &str,
        payload: Value,
    ) {
        if let Some(emitter) = self.telemetry.as_ref() {
            emitter.emit(
                event_type,
                game_id,
                chain_id,
                strategy_key,
                order_client_id,
                order_exchange_id,
                reason_code,
                payload,
            );
        }
    }

    #[inline]
    pub(super) fn emit_event_lazy<F>(
        &self,
        event_type: &str,
        game_id: &str,
        chain_id: &str,
        strategy_key: &str,
        order_client_id: &str,
        order_exchange_id: &str,
        reason_code: &str,
        payload: F,
    ) where
        F: FnOnce() -> Value,
    {
        if let Some(emitter) = self.telemetry.as_ref() {
            emitter.emit(
                event_type,
                game_id,
                chain_id,
                strategy_key,
                order_client_id,
                order_exchange_id,
                reason_code,
                payload(),
            );
        }
    }

    fn status_to_lifecycle_event(status: &str) -> Option<&'static str> {
        match status.trim().to_lowercase().as_str() {
            "accepted" | "acknowledged" => Some("order_acknowledged"),
            "open" | "resting" | "live" => Some("order_resting"),
            "partially_filled" | "partiallyfilled" | "partial" => Some("order_partially_filled"),
            "filled" => Some("order_filled"),
            "canceled" | "cancelled" | "expired" => Some("order_canceled"),
            "rejected" => Some("order_rejected"),
            "failed" | "error" => Some("order_failed"),
            _ => None,
        }
    }

    pub(super) fn emit_lifecycle_transition(
        &self,
        strategy_key: &str,
        prev_status: &str,
        state: &OrderStateData,
        source_universal_id: &str,
        chain_id: &str,
    ) {
        if prev_status
            .trim()
            .eq_ignore_ascii_case(state.status.as_str())
        {
            return;
        }
        let Some(event_type) = Self::status_to_lifecycle_event(state.status.as_str()) else {
            return;
        };
        self.emit_event(
            event_type,
            source_universal_id,
            chain_id,
            strategy_key,
            state.client_order_id.as_str(),
            state.exchange_order_id.as_str(),
            state.reason.as_str(),
            json!({
                "status": state.status,
                "side": state.side,
                "time_in_force": state.time_in_force,
                "limit_price": state.limit_price,
                "requested_amount_usdc": state.requested_amount_usdc,
                "filled_amount_usdc": state.filled_amount_usdc,
                "error_code": state.error_code,
            }),
        );
    }
}
