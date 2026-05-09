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
    /// Returns the signed order on success, or an error string for the caller
    /// to log. Synchronous; runs on the WS thread.
    pub(crate) fn pop_for_target(
        &mut self,
        target_idx: crate::TargetIdx,
    ) -> Result<Box<SdkSignedOrder>, String> {
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
        match slot.take() {
            Some(boxed) => Ok(boxed),
            None => {
                let token_id = self
                    .registry
                    .tokens
                    .get(token_idx)
                    .map(|t| &*t.token_id)
                    .unwrap_or("_");
                Err(format!(
                    "submit_presigned_miss:token_id={}",
                    redact_token_id(token_id)
                ))
            }
        }
    }

    /// Send a frame-batch to the submitter. On failure (no channel installed
    /// or receiver dropped), logs an error per item via the registry.
    pub(crate) fn send_batch(
        &self,
        batch: SubmitBatch,
        log: &Arc<Mutex<LogWriter>>,
    ) {
        if batch.is_empty() {
            return;
        }
        let Some(tx) = self.submit_tx.as_ref() else {
            for (target_idx, _) in &batch {
                let (sk, tok) = self.resolve_strings(*target_idx);
                if let Ok(mut g) = log.lock() {
                    g.log_order_err(sk, tok, "submit_channel_uninitialized");
                }
            }
            return;
        };
        // Send first — zero allocation on the success path. On failure,
        // recover the batch from SendError for diagnostics.
        if let Err(flume::SendError(work)) = tx.send(SubmitWork::Batch(batch)) {
            let SubmitWork::Batch(returned) = work else { return };
            for (target_idx, _) in returned {
                let (sk, tok) = self.resolve_strings(target_idx);
                if let Ok(mut g) = log.lock() {
                    g.log_order_err(sk, tok, "submit_channel_closed");
                }
            }
        }
    }

    pub(crate) fn send_registry_update(&self, new_registry: Arc<crate::TargetRegistry>) {
        if let Some(tx) = self.submit_tx.as_ref() {
            let _ = tx.send(SubmitWork::UpdateRegistry(new_registry));
        }
    }
}
