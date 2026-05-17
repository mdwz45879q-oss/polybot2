use super::*;
use crate::log_writer::LogWriter;
use std::sync::{Arc, Mutex};

impl DispatchHandle {
    pub(crate) fn mode_label(&self) -> &'static str {
        if matches!(self.cfg.mode, DispatchMode::Http) {
            "http"
        } else {
            "noop"
        }
    }

    /// Resolve `(strategy_key, token_id)` strings from the registry. Used for
    /// inline log lines (noop, fail-closed errors). Returns `("_", "_")` if
    /// the `target_idx` is out of bounds (should never happen).
    pub(crate) fn resolve_strings(&self, target_idx: crate::TargetIdx) -> (&str, &str) {
        let Some(target) = self.registry.targets.get(target_idx.0 as usize) else {
            return ("_", "_");
        };
        let Some(token) = self.registry.tokens.get(target.token_idx.0 as usize) else {
            return (&*target.strategy_key, "_");
        };
        (&*target.strategy_key, &*token.token_id)
    }

    /// Pop a presigned order for the given target from the per-token pool.
    /// Returns all pre-signed orders for this target's token (1-2 orders),
    /// or an error string if the pool slot is empty. Drains the slot completely.
    /// Synchronous; runs on the WS thread.
    pub(crate) fn pop_for_target(
        &mut self,
        target_idx: crate::TargetIdx,
    ) -> Result<smallvec::SmallVec<[Box<PreparedOrderPayload>; 2]>, String> {
        let target = self
            .registry
            .targets
            .get(target_idx.0 as usize)
            .ok_or_else(|| "dispatch_invalid_target_idx".to_string())?;
        let token_idx = target.token_idx.0 as usize;
        let slot = self
            .presign_pool
            .get_mut(token_idx)
            .ok_or_else(|| "dispatch_invalid_token_idx".to_string())?;
        if slot.is_empty() {
            let token_id = self
                .registry
                .tokens
                .get(token_idx)
                .map(|t| &*t.token_id)
                .unwrap_or("_");
            return Err(format!(
                "submit_presigned_miss:token_id={}",
                redact_token_id(token_id)
            ));
        }
        Ok(std::mem::take(slot))
    }

    /// Send a frame-batch to the submitter via the lock-free SPSC ring.
    /// On failure (ring full or not installed), logs an error per item.
    pub(crate) fn send_batch(&mut self, batch: SubmitBatch, log: &Arc<Mutex<LogWriter>>) {
        if batch.is_empty() {
            return;
        }
        let Some(tx) = self.submit_tx.as_mut() else {
            for (target_idx, _) in &batch {
                let (sk, tok) = self.resolve_strings(*target_idx);
                if let Ok(mut g) = log.lock() {
                    g.log_order_err(sk, tok, "submit_channel_uninitialized");
                }
            }
            return;
        };
        match tx.push(SubmitWork::Batch(batch)) {
            Ok(()) => {}
            Err(rtrb::PushError::Full(work)) => {
                let SubmitWork::Batch(returned) = work else {
                    return;
                };
                for (target_idx, _) in returned {
                    let (sk, tok) = self.resolve_strings(target_idx);
                    if let Ok(mut g) = log.lock() {
                        g.log_order_err(sk, tok, "submit_ring_full");
                    }
                }
            }
        }
    }
}

pub(crate) fn dispatch_intents(
    intents: &[crate::Intent],
    handle: &mut DispatchHandle,
    log: &Arc<Mutex<LogWriter>>,
) {
    if matches!(handle.cfg.mode, DispatchMode::Noop) {
        for intent in intents {
            let (sk, tok) = handle.resolve_strings(intent.target_idx);
            if let Ok(mut g) = log.lock() {
                g.log_order_ok(sk, tok, "noop");
            }
        }
    } else {
        let mut batch: SubmitBatch = SubmitBatch::new();
        for intent in intents {
            match handle.pop_for_target(intent.target_idx) {
                Ok(orders) => {
                    for signed in orders {
                        batch.push((intent.target_idx, signed));
                    }
                }
                Err(err) => {
                    let (sk, tok) = handle.resolve_strings(intent.target_idx);
                    if let Ok(mut g) = log.lock() {
                        g.log_order_err(sk, tok, &err);
                    }
                }
            }
        }
        if !batch.is_empty() {
            handle.send_batch(batch, log);
        }
    }
}
